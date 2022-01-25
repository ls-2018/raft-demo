package raft

import (
	"container/list"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"io"
	"sync/atomic"
	"time"
)

const (
	minCheckInterval = 10 * time.Millisecond
)

var (
	keyCurrentTerm  = []byte("CurrentTerm")
	keyLastVoteTerm = []byte("LastVoteTerm")
	keyLastVoteCand = []byte("LastVoteCand")
)

// commitTuple is used to send an index that was committed,
// with an optional associated future that should be invoked.
type commitTuple struct {
	log    *Log
	future *logFuture
}

// leaderState 是我们做领导时使用的状态。
type leaderState struct {
	leadershipTransferInProgress int32 // 表示领导权转移正在进行中。
	commitCh                     chan struct{}
	commitment                   *commitment
	inflight                     *list.List                        // 按日志索引顺序排列的logFuture列表
	replState                    map[ServerID]*followerReplication // 逻辑ID：follower
	notify                       map[*verifyFuture]struct{}
	stepDown                     chan struct{}
}

// ------------------------------------ over ------------------------------------

// requestConfigChange
func (r *Raft) requestConfigChange(req configurationChangeRequest, timeout time.Duration) IndexFuture {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}
	future := &configurationChangeFuture{
		req: req,
	}
	future.init()
	select {
	case <-timer:
		return errorFuture{ErrEnqueueTimeout}
	case r.configurationChangeCh <- future:
		return future
	case <-r.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	}
}

// startStopReplication 将设置状态并开始向新的follower进行异步复制，并停止向被删除的node进行复制。
// 在移除一个node之前，它将指示复制程序尝试复制到当前的 索引。这必须只在主线程中调用。
func (r *Raft) startStopReplication() {
	inConfig := make(map[ServerID]bool, len(r.configurations.latest.Servers))
	lastIdx := r.getLastIndex()
	for _, server := range r.configurations.latest.Servers {
		if server.ID == r.localID {
			continue
		}

		inConfig[server.ID] = true
		//r.leaderState.replState 就是一个空map,运行到这里的时候
		s, ok := r.leaderState.replState[server.ID]
		if !ok {
			r.logger.Info("添加目标节点，开始复制", "peer", server.ID)
			s = &followerReplication{
				peer:                server,                   // follower机电基本信息
				commitment:          r.leaderState.commitment, // leader节点已确认的日志数据
				stopCh:              make(chan uint64, 1),     // 用于停止follower
				triggerCh:           make(chan struct{}, 1),   // 每当有新的条目被添加到日志中时，就会得到通知。
				triggerDeferErrorCh: make(chan *deferError, 1),
				currentTerm:         r.getCurrentTerm(), // leader当前任期
				nextIndex:           lastIdx + 1,        // 下调日志的索引
				lastContact:         time.Now(),
				notify:              make(map[*verifyFuture]struct{}), //
				notifyCh:            make(chan struct{}, 1),           // todo 等下再解释
				stepDown:            r.leaderState.stepDown,           // 不再是 leader
			}

			r.leaderState.replState[server.ID] = s
			r.goFunc(func() { r.replicate(s) })
			_ = r.electSelf // 触发一下,第一次也没数据
			asyncNotifyCh(s.triggerCh)
		} else if ok {
			// 在变成非leader 以及 重新变成leader后、或者节点配置变更，走这里
			s.peerLock.RLock()
			peer := s.peer
			s.peerLock.RUnlock()

			if peer.Address != server.Address {
				r.logger.Info("updating peer", "peer", server.ID)
				s.peerLock.Lock()
				s.peer = server
				s.peerLock.Unlock()
			}
		}
	}
	//
	// 停止需要停止的复制goroutines；  如果说启动时 inConfig、replState肯定是一样的
	for serverID, repl := range r.leaderState.replState {
		if inConfig[serverID] {
			continue
		}
		// TODO 什么时候会走到这里
		// 复制到最新就停止
		r.logger.Info("removed peer, stopping replication", "peer", serverID, "last-index", lastIdx)
		repl.stopCh <- lastIdx
		close(repl.stopCh)
		delete(r.leaderState.replState, serverID)
	}

}

func (r *Raft) configurationChangeChIfStable() chan *configurationChangeFuture {
	if r.configurations.latestIndex == r.configurations.committedIndex &&
		r.getCommitIndex() >= r.leaderState.commitment.startIndex {
		return r.configurationChangeCh
	}
	return nil
}

func (r *Raft) processLogs(index uint64, futures map[uint64]*logFuture) {
	// 拒绝我们已经申请的日志
	lastApplied := r.getLastApplied()
	if index <= lastApplied {
		r.logger.Warn("跳过旧数据", "index", index)
		return
	}

	applyBatch := func(batch []*commitTuple) {
		select {
		case r.fsmMutateCh <- batch:
		case <-r.shutdownCh:
			for _, cl := range batch {
				if cl.future != nil {
					cl.future.respond(ErrRaftShutdown)
				}
			}
		}
	}

	// Store maxAppendEntries for this call in case it ever becomes reloadable. We
	// need to use the same value for all lines here to get the expected result.
	maxAppendEntries := r.config().MaxAppendEntries

	batch := make([]*commitTuple, 0, maxAppendEntries)

	// Apply all the preceding logs
	for idx := lastApplied + 1; idx <= index; idx++ {
		var preparedLog *commitTuple
		// Get the log, either from the future or from our log store
		future, futureOk := futures[idx]
		if futureOk {
			preparedLog = r.prepareLog(&future.log, future)
		} else {
			l := new(Log)
			if err := r.logs.GetLog(idx, l); err != nil {
				r.logger.Error("failed to get log", "index", idx, "error", err)
				panic(err)
			}
			preparedLog = r.prepareLog(l, nil)
		}

		switch {
		case preparedLog != nil:
			// If we have a log ready to send to the FSM add it to the batch.
			// The FSM thread will respond to the future.
			batch = append(batch, preparedLog)

			// If we have filled up a batch, send it to the FSM
			if len(batch) >= maxAppendEntries {
				applyBatch(batch)
				batch = make([]*commitTuple, 0, maxAppendEntries)
			}

		case futureOk:
			// Invoke the future if given.
			future.respond(nil)
		}
	}

	// If there are any remaining logs in the batch apply them
	if len(batch) != 0 {
		applyBatch(batch)
	}

	// Update the lastApplied index and term
	r.setLastApplied(index)
}

// 调用processLog来处理单个已提交日志条目的应用程序。
func (r *Raft) prepareLog(l *Log, future *logFuture) *commitTuple {
	switch l.Type {
	case LogBarrier:
		// Barrier由FSM处理
		fallthrough

	case LogCommand:
		return &commitTuple{l, future}

	case LogConfiguration:
		// Only support this with the v2 configuration format
		if r.protocolVersion > 2 {
			return &commitTuple{l, future}
		}
	case LogAddPeerDeprecated:
	case LogNoop:
		// Ignore the no-op

	default:
		panic(fmt.Errorf("unrecognized log type: %#v", l))
	}

	return nil
}

// appendConfigurationEntry 更改配置并在日志中添加一个新的配置条目。这必须只从主线程调用。
func (r *Raft) appendConfigurationEntry(future *configurationChangeFuture) {
	configuration, err := nextConfiguration(r.configurations.latest, r.configurations.latestIndex, future.req)
	if err != nil {
		future.respond(err)
		return
	}

	r.logger.Info("更新配置",
		"command", future.req.command,
		"server-id", future.req.serverID,
		"server-addr", future.req.serverAddress,
		"servers", hclog.Fmt("%+v", configuration.Servers))

	future.log = Log{
		Type: LogConfiguration,
		Data: EncodeConfiguration(configuration),
	}

	r.dispatchLogs([]*logFuture{&future.logFuture})
	index := future.Index()
	r.setLatestConfiguration(configuration, index)
	r.leaderState.commitment.setConfiguration(configuration)
	r.startStopReplication()
}

// run 是一个运行Raft FSM的长期运行的goroutine。
func (r *Raft) run() {
	for {
		select {
		case <-r.shutdownCh:
			// 清除领导，防止转发
			r.setLeader("")
			return
		default:
		}

		// 进入一个子FSM
		switch r.getState() {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			r.runLeader()
		}
	}
}

// getRPCHeader returns an initialized RPCHeader struct for the given
// Raft instance. This structure is sent along with RPC requests and
// responses.
func (r *Raft) getRPCHeader() RPCHeader {
	return RPCHeader{
		ProtocolVersion: r.config().ProtocolVersion,
	}
}

// checkRPCHeader  raft处理rpc消息的主逻辑
func (r *Raft) checkRPCHeader(rpc RPC) error {
	// 获取rpc消息的头信息
	wh, ok := rpc.Command.(WithRPCHeader)
	if !ok {
		return fmt.Errorf("RPC没有头信息")
	}
	header := wh.GetRPCHeader()

	// 首先检查协议版本  0，1，2，3
	if header.ProtocolVersion < ProtocolVersionMin || header.ProtocolVersion > ProtocolVersionMax {
		return ErrUnsupportedProtocol
	}

	// 第二个检查是，考虑到我们当前配置运行的协议，我们是否应该支持这个消息。
	// 这将放弃对协议版本0的支持，从协议版本2开始，这是目前我们想要的，一般情况下，支持一个版本后。
	// 我们可能需要重新审视这个策略，这取决于未来协议的 的变化，我们可能需要重新审视这个政策。
	// 请求的协议版本，不能低于当前版本-1
	if header.ProtocolVersion < r.config().ProtocolVersion-1 {
		return ErrUnsupportedProtocol
	}

	return nil
}

// OK
func getSnapshotVersion(protocolVersion ProtocolVersion) SnapshotVersion {
	return 1
}

// pickServer 从众多的参与投票的follower 中选出一个日志最新的节点
func (r *Raft) pickServer() *Server {
	var pick *Server
	var current uint64
	for _, server := range r.configurations.latest.Servers {
		if server.ID == r.localID || server.Suffrage != Voter {
			continue
		}
		state, ok := r.leaderState.replState[server.ID]
		if !ok {
			continue
		}
		nextIdx := atomic.LoadUint64(&state.nextIndex)
		if nextIdx > current {
			current = nextIdx
			tmp := server
			pick = &tmp
		}
	}
	return pick
}

// timeoutNow 当leader收到一个超时信号
func (r *Raft) timeoutNow(rpc RPC, req *TimeoutNowRequest) {
	r.setLeader("")
	r.setState(Candidate)
	r.candidateFromLeadershipTransfer = true
	rpc.Respond(&TimeoutNowResponse{}, nil)
}

// dispatchLog 在leader层被调用，以推送日志到磁盘。 标记它 并开始对其进行复制。不会并发调用
func (r *Raft) dispatchLogs(applyLogs []*logFuture) {
	now := time.Now()

	term := r.getCurrentTerm()    // 当前任期
	lastIndex := r.getLastIndex() // 最新的日志索引

	n := len(applyLogs)
	logs := make([]*Log, n)

	for idx, applyLog := range applyLogs {
		// 对即将写入磁盘的LogFuture结构体设置一些信息
		applyLog.dispatch = now
		lastIndex++
		applyLog.log.Index = lastIndex
		applyLog.log.Term = term
		applyLog.log.AppendedAt = now
		logs[idx] = &applyLog.log
		r.leaderState.inflight.PushBack(applyLog)
	}

	// 在本地写入日志条目
	if err := r.logs.StoreLogs(logs); err != nil {
		r.logger.Error("提交日志失败", "error", err)
		// 设置LogFuture的响应
		for _, applyLog := range applyLogs {
			applyLog.respond(err)
		}
		// 更改状态
		r.setState(Follower)
		return
	}
	r.leaderState.commitment.match(r.localID, lastIndex)

	// 更新最后的日志，它现在已经在磁盘上了,但并没有commit
	r.setLastLog(lastIndex, term)

	// 将新的日志通知给复制者
	for _, f := range r.leaderState.replState {
		asyncNotifyCh(f.triggerCh)
	}
}

// 尝试设置一个初始的集群的初始配置；   只有在跟随者状态下才有意义。
func (r *Raft) liveBootstrap(configuration Configuration) error {
	// 使用初始化前API进行静态更新。
	cfg := r.config()
	err := BootstrapCluster(&cfg, r.logs, r.stable, r.snapshots, r.trans, configuration) // 只是往log db 追加了配置项
	if err != nil {
		return err
	}

	// 启用配置。
	var entry Log
	if err := r.logs.GetLog(1, &entry); err != nil {
		panic(err)
	}
	r.setCurrentTerm(1)
	r.setLastLog(entry.Index, entry.Term)
	return r.processConfigurationLogEntry(&entry) // 好像也没干啥，就是更新了一下索引
}

// quorumSize 用来返回竞选者一半的大小。 //2 + 1
func (r *Raft) quorumSize() int {
	voters := 0
	for _, server := range r.configurations.latest.Servers {
		if server.Suffrage == Voter {
			voters++
		}
	}
	return voters/2 + 1
}

// restoreUserSnapshot 重新存储一个快照，并将数据存储到FSM
func (r *Raft) restoreUserSnapshot(meta *SnapshotMeta, reader io.Reader) error {
	version := meta.Version
	if version < SnapshotVersionMin || version > SnapshotVersionMax {
		return fmt.Errorf("快照版本不支持 %d", version)
	}

	// 当配置更改未完成时，我们不支持快照，因为快照没有表示这种状态的方法。
	committedIndex := r.configurations.committedIndex
	latestIndex := r.configurations.latestIndex
	if committedIndex != latestIndex {
		return fmt.Errorf("无法恢复快照, 等待配置项更新 【latestIndex:%v】[committedIndex:%v]", latestIndex, committedIndex)
	}

	// 取消所有未提交的请求
	for {
		e := r.leaderState.inflight.Front()
		if e == nil {
			break
		}
		e.Value.(*logFuture).respond(ErrAbortedByRestore)
		r.leaderState.inflight.Remove(e)
	}
	term := r.getCurrentTerm()
	lastIndex := r.getLastIndex()
	if meta.Index > lastIndex {
		lastIndex = meta.Index
	}
	lastIndex++
	sink, err := r.snapshots.Create(version, lastIndex, term, r.configurations.latest, r.configurations.latestIndex, r.trans)
	if err != nil {
		return fmt.Errorf("创建快照失败: %v", err)
	}
	n, err := io.Copy(sink, reader)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("写快照失败: %v", err)
	}
	if n != meta.Size {
		sink.Cancel()
		return fmt.Errorf("写快照失败, size didn't match (%d != %d)", n, meta.Size)
	}
	if err := sink.Close(); err != nil {
		return fmt.Errorf("关闭快照失败: %v", err)
	}
	r.logger.Info("拷贝数据到本地快照", "bytes", n)

	// 恢复快照到FSM如果失败了，我们就会处于一个糟糕的状态，所以我们会恐慌地把自己干掉。
	// 将数据保存到了快照，然后更新到用户的FSM
	fsm := &restoreFuture{ID: sink.ID()}
	fsm.ShutdownCh = r.shutdownCh
	fsm.init()
	select {
	case r.fsmMutateCh <- fsm:
	case <-r.shutdownCh:
		return ErrRaftShutdown
	}
	if err := fsm.Error(); err != nil {
		panic(fmt.Errorf("重新存储用户快照失败: %v", err))
	}
	r.setLastLog(lastIndex, term)
	r.setLastApplied(lastIndex)
	r.setLastSnapshot(lastIndex, term)

	r.logger.Info("重新存储 snapshot", "index", latestIndex)
	return nil
}

// persistVote 用来确保安全的投票,写db      任期、竞选者
func (r *Raft) persistVote(term uint64, candidate []byte) error {
	if err := r.stable.SetUint64(keyLastVoteTerm, term); err != nil {
		return err
	}
	if err := r.stable.Set(keyLastVoteCand, candidate); err != nil {
		return err
	}
	return nil
}

// setCurrentTerm 是用来以持久的方式设置当前的任期。
func (r *Raft) setCurrentTerm(t uint64) {
	// 先保存到磁盘上
	if err := r.stable.SetUint64(keyCurrentTerm, t); err != nil {
		panic(fmt.Errorf("保存到磁盘上: %v", err))
	}
	r.raftState.setCurrentTerm(t)
}

// setState 是用来更新当前状态。任何状态转换都会导致已知的领导者被清空。这意味着只有在更新状态后才应设置领导者。
func (r *Raft) setState(state State) {
	r.setLeader("")
	r.raftState.setState(state)
}

// setLatestConfiguration 储存最新的配置并更新其副本。
func (r *Raft) setLatestConfiguration(c Configuration, i uint64) {
	r.configurations.latest = c
	r.configurations.latestIndex = i
	r.latestConfiguration.Store(c.Clone())
}

// setCommittedConfiguration 存储已提交的配置。
func (r *Raft) setCommittedConfiguration(c Configuration, i uint64) {
	r.configurations.committed = c
	r.configurations.committedIndex = i
}

// getLatestConfiguration 从主配置的副本中读取配置，这意味着它可以独立于主循环访问。
func (r *Raft) getLatestConfiguration() Configuration {
	switch c := r.latestConfiguration.Load().(type) {
	case Configuration:
		return c
	default:
		return Configuration{}
	}
}

// setLastContact 设置上一次与其他节点通信的时间
func (r *Raft) setLastContact() {
	r.lastContactLock.Lock()
	r.lastContact = time.Now()
	r.lastContactLock.Unlock()
}
