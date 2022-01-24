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
	minCheckInterval       = 10 * time.Millisecond
	oldestLogGaugeInterval = 10 * time.Second
)

var (
	keyCurrentTerm  = []byte("CurrentTerm")
	keyLastVoteTerm = []byte("LastVoteTerm")
	keyLastVoteCand = []byte("LastVoteCand")
)

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

// getSnapshotVersion returns the snapshot version that should be used when
// creating snapshots, given the protocol version in use.
func getSnapshotVersion(protocolVersion ProtocolVersion) SnapshotVersion {
	// Right now we only have two versions and they are backwards compatible
	// so we don't need to look at the protocol version.
	return 1
}

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

// requestConfigChange is a helper for the above functions that make
// configuration change requests. 'req' describes the change. For timeout,
// see AddVoter.
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

// liveBootstrap    尝试发送一个初始的集群的初始配置；   only makes sense in the follower state.
func (r *Raft) liveBootstrap(configuration Configuration) error {
	// Use the pre-init API to make the static updates.
	cfg := r.config()
	err := BootstrapCluster(&cfg, r.logs, r.stable, r.snapshots,
		r.trans, configuration)
	if err != nil {
		return err
	}

	// Make the configuration live.
	var entry Log
	if err := r.logs.GetLog(1, &entry); err != nil {
		panic(err)
	}
	r.setCurrentTerm(1)
	r.setLastLog(entry.Index, entry.Term)
	return r.processConfigurationLogEntry(&entry)
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
			r.observe(PeerObservation{Peer: server, Removed: false})
		} else if ok {
			// 在变成非leader 以及 重新变成leader后，走这里
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
	// Stop replication goroutines that need stopping
	for serverID, repl := range r.leaderState.replState {
		if inConfig[serverID] {
			continue
		}
		// Replicate up to lastIdx and stop
		r.logger.Info("removed peer, stopping replication", "peer", serverID, "last-index", lastIdx)
		repl.stopCh <- lastIdx
		close(repl.stopCh)
		delete(r.leaderState.replState, serverID)
		r.observe(PeerObservation{Peer: repl.peer, Removed: true})
	}

}

// configurationChangeChIfStable returns r.configurationChangeCh if it's safe
// to process requests from it, or nil otherwise. This must only be called
// from the main thread.
//
// Note that if the conditions here were to change outside of leaderLoop to take
// this from nil to non-nil, we would need leaderLoop to be kicked.
func (r *Raft) configurationChangeChIfStable() chan *configurationChangeFuture {
	// Have to wait until:
	// 1. The latest configuration is committed, and
	// 2. This leader has committed some entry (the noop) in this term
	//    https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J
	if r.configurations.latestIndex == r.configurations.committedIndex &&
		r.getCommitIndex() >= r.leaderState.commitment.startIndex {
		return r.configurationChangeCh
	}
	return nil
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

// restoreUserSnapshot is used to manually consume an external snapshot, such
// as if restoring from a backup. We will use the current Raft configuration,
// not the one from the snapshot, so that we can restore into a new cluster. We
// will also use the higher of the index of the snapshot, or the current index,
// and then add 1 to that, so we force a new state with a hole in the Raft log,
// so that the snapshot will be sent to followers and used for any new joiners.
// This can only be run on the leader, and returns a future that can be used to
// block until complete.
func (r *Raft) restoreUserSnapshot(meta *SnapshotMeta, reader io.Reader) error {

	// Sanity check the version.
	version := meta.Version
	if version < SnapshotVersionMin || version > SnapshotVersionMax {
		return fmt.Errorf("unsupported snapshot version %d", version)
	}

	// We don't support snapshots while there's a config change
	// outstanding since the snapshot doesn't have a means to
	// represent this state.
	committedIndex := r.configurations.committedIndex
	latestIndex := r.configurations.latestIndex
	if committedIndex != latestIndex {
		return fmt.Errorf("cannot restore snapshot now, wait until the configuration entry at %v has been applied (have applied %v)",
			latestIndex, committedIndex)
	}

	// Cancel any inflight requests.
	for {
		e := r.leaderState.inflight.Front()
		if e == nil {
			break
		}
		e.Value.(*logFuture).respond(ErrAbortedByRestore)
		r.leaderState.inflight.Remove(e)
	}

	// We will overwrite the snapshot metadata with the current term,
	// an index that's greater than the current index, or the last
	// index in the snapshot. It's important that we leave a hole in
	// the index so we know there's nothing in the Raft log there and
	// replication will fault and send the snapshot.
	term := r.getCurrentTerm()
	lastIndex := r.getLastIndex()
	if meta.Index > lastIndex {
		lastIndex = meta.Index
	}
	lastIndex++

	// Dump the snapshot. Note that we use the latest configuration,
	// not the one that came with the snapshot.
	sink, err := r.snapshots.Create(version, lastIndex, term,
		r.configurations.latest, r.configurations.latestIndex, r.trans)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %v", err)
	}
	n, err := io.Copy(sink, reader)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to write snapshot: %v", err)
	}
	if n != meta.Size {
		sink.Cancel()
		return fmt.Errorf("failed to write snapshot, size didn't match (%d != %d)", n, meta.Size)
	}
	if err := sink.Close(); err != nil {
		return fmt.Errorf("failed to close snapshot: %v", err)
	}
	r.logger.Info("copied to local snapshot", "bytes", n)

	// Restore the snapshot into the FSM. If this fails we are in a
	// bad state so we panic to take ourselves out.
	fsm := &restoreFuture{ID: sink.ID()}
	fsm.ShutdownCh = r.shutdownCh
	fsm.init()
	select {
	case r.fsmMutateCh <- fsm:
	case <-r.shutdownCh:
		return ErrRaftShutdown
	}
	if err := fsm.Error(); err != nil {
		panic(fmt.Errorf("failed to restore snapshot: %v", err))
	}

	// We set the last log so it looks like we've stored the empty
	// index we burned. The last applied is set because we made the
	// FSM take the snapshot state, and we store the last snapshot
	// in the stable store since we created a snapshot as part of
	// this process.
	r.setLastLog(lastIndex, term)
	r.setLastApplied(lastIndex)
	r.setLastSnapshot(lastIndex, term)

	r.logger.Info("restored user snapshot", "index", latestIndex)
	return nil
}

// appendConfigurationEntry changes the configuration and adds a new
// configuration entry to the log. This must only be called from the
// main thread.
func (r *Raft) appendConfigurationEntry(future *configurationChangeFuture) {
	configuration, err := nextConfiguration(r.configurations.latest, r.configurations.latestIndex, future.req)
	if err != nil {
		future.respond(err)
		return
	}

	r.logger.Info("updating configuration",
		"command", future.req.command,
		"server-id", future.req.serverID,
		"server-addr", future.req.serverAddress,
		"servers", hclog.Fmt("%+v", configuration.Servers))

	// In pre-ID compatibility mode we translate all configuration changes
	// in to an old remove peer message, which can handle all supported
	// cases for peer changes in the pre-ID world (adding and removing
	// voters). Both add peer and remove peer log entries are handled
	// similarly on old Raft servers, but remove peer does extra checks to
	// see if a leader needs to step down. Since they both assert the full
	// configuration, then we can safely call remove peer for everything.
	if r.protocolVersion < 2 {
		future.log = Log{
			Type: LogRemovePeerDeprecated,
			Data: encodePeers(configuration, r.trans),
		}
	} else {
		future.log = Log{
			Type: LogConfiguration,
			Data: EncodeConfiguration(configuration),
		}
	}

	r.dispatchLogs([]*logFuture{&future.logFuture})
	index := future.Index()
	r.setLatestConfiguration(configuration, index)
	r.leaderState.commitment.setConfiguration(configuration)
	r.startStopReplication()
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

// processLogs is used to apply all the committed entries that haven't been
// applied up to the given index limit.
// This can be called from both leaders and followers.
// Followers call this from AppendEntries, for n entries at a time, and always
// pass futures=nil.
// Leaders call this when entries are committed. They pass the futures from any
// inflight logs.
//是用来应用所有尚未应用到给定索引限制的已承诺条目。领导者和追随者都可以调用这个功能。
//跟随者从AppendEntries中调用这个，每次调用n个条目，并且总是传递futures=nil。
//领导者在条目被提交时调用此功能。他们从任何飞行日志中传递期货。
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

// processLog is invoked to process the application of a single committed log entry.
func (r *Raft) prepareLog(l *Log, future *logFuture) *commitTuple {
	switch l.Type {
	case LogBarrier:
		// Barrier is handled by the FSM
		fallthrough

	case LogCommand:
		return &commitTuple{l, future}

	case LogConfiguration:
		// Only support this with the v2 configuration format
		if r.protocolVersion > 2 {
			return &commitTuple{l, future}
		}
	case LogAddPeerDeprecated:
	case LogRemovePeerDeprecated:
	case LogNoop:
		// Ignore the no-op

	default:
		panic(fmt.Errorf("unrecognized log type: %#v", l))
	}

	return nil
}

// setLastContact 设置上一次与其他节点通信的时间
func (r *Raft) setLastContact() {
	r.lastContactLock.Lock()
	r.lastContact = time.Now()
	r.lastContactLock.Unlock()
}

type voteResult struct {
	RequestVoteResponse
	voterID ServerID // 选民的逻辑ID
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
func (r *Raft) setState(state RaftState) {
	r.setLeader("")
	oldState := r.raftState.getState()
	r.raftState.setState(state)
	if oldState != state {
		r.observe(state)
	}
}

// pickServer returns the follower that is most up to date and participating in quorum.
// Because it accesses leaderstate, it should only be called from the leaderloop.
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

// timeoutNow is what happens when a server receives a TimeoutNowRequest.
func (r *Raft) timeoutNow(rpc RPC, req *TimeoutNowRequest) {
	r.setLeader("")
	r.setState(Candidate)
	r.candidateFromLeadershipTransfer = true
	rpc.Respond(&TimeoutNowResponse{}, nil)
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
