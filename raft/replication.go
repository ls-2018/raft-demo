package raft

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxFailureScale = 12
	failureWait     = 10 * time.Millisecond // 失败重试间隔
)

var (
	// ErrLogNotFound 表示一个给定的日志条目是不可用的。
	ErrLogNotFound = errors.New("log not found")

	// ErrPipelineReplicationNotSupported 表示一般不支持管道复制，并且不应该产生错误信息。
	ErrPipelineReplicationNotSupported = errors.New("不支持管道复制")
)

// followerReplication 负责在这个特定的期限内将这个leader的快照和日志条目发送给一个远程的follower。
type followerReplication struct {
	// 32位平台的为了原子操作 需要 64bit对齐
	// currentTerm leader当前的任期
	currentTerm uint64
	// nextIndex 下一条要发送给follower的日志索引   xx[nextIndex:?]   数据一致性时   nextIndex 应该与latestIndex+1 相等
	nextIndex uint64 // 选主以后，从leader开始时的的下一条日志开始复制

	// peer 包含远程follower的网络地址和ID。
	peer     Server
	peerLock sync.RWMutex

	// commitment 跟踪被follower承认的条目，以便领导者的commit index可以增加。它在成功的AppendEntries响应时被更新。
	commitment *commitment

	// 当leader变换 发送通知
	// follower从集群移除->close. 携带一个日志索引，应该尽最大努力通过该索引来尝试复制。  在退出之前，
	stopCh chan uint64 // 最外层循环、管道模式、非管道模式

	// triggerCh 节点刚成为leader；每当有新的条目被添加到日志中时，就会得到通知。
	triggerCh chan struct{}

	// triggerDeferErrorCh  是用来提供一个返回通道的。通过发送deferErr，发送方可以在复制完成后得到通知。
	triggerDeferErrorCh chan *deferError

	// lastContact 上一次与该节点进行通信的时间、无论成功与否             ,用来检查leader是否应该转移
	lastContact     time.Time
	lastContactLock sync.RWMutex

	// failures 统计自上次成功RPC请求之后的失败次数，用于计算backoff
	failures uint64

	// notifyCh 需要发送心跳的channel，用来检查本节点是不是leader
	notifyCh chan struct{} // 收到消息了
	// notify  在收到确认信息后，需要解决的回应
	notify     map[*verifyFuture]struct{} // heartbeat 成功,会对这里的值设置
	notifyLock sync.Mutex

	// stepDown 是用来向leader表明，我们应该根据follower的信息下台。
	stepDown chan struct{}

	// allowPipeline 用来决定何时对AppendEntries RPC进行流水线处理。它对这个复制程序来说是私有的。
	allowPipeline bool
}

// TODO 什么时候打的快照呢？还有什么其他情况

// sendLatestSnapshot 用来发送我们最新的快照到Follower
func (r *Raft) sendLatestSnapshot(f *followerReplication) (bool, error) {
	snapshots, err := r.snapshots.List()
	if err != nil {
		r.logger.Error("获取快照列表失败", "error", err)
		return false, err
	}

	if len(snapshots) == 0 {
		return false, fmt.Errorf("no snapshots found")
	}

	// 打开最新的快照
	snapID := snapshots[0].ID
	meta, snapshot, err := r.snapshots.Open(snapID)
	if err != nil {
		r.logger.Error("failed to open snapshot", "id", snapID, "error", err)
		return false, err
	}
	defer snapshot.Close()

	req := InstallSnapshotRequest{
		RPCHeader:          r.getRPCHeader(),
		SnapshotVersion:    meta.Version,
		Term:               f.currentTerm,
		Leader:             r.trans.EncodePeer(r.localID, r.localAddr),
		LastLogIndex:       meta.Index,
		LastLogTerm:        meta.Term,
		Peers:              meta.Peers,
		Size:               meta.Size,
		Configuration:      EncodeConfiguration(meta.Configuration),
		ConfigurationIndex: meta.ConfigurationIndex,
	}

	f.peerLock.RLock()
	peer := f.peer
	f.peerLock.RUnlock()

	var resp InstallSnapshotResponse
	_ = r.processRPC
	if err := r.trans.InstallSnapshot(peer.ID, peer.Address, &req, &resp, snapshot); err != nil {
		r.logger.Error("安装快照失败", "id", snapID, "error", err)
		f.failures++
		return false, err
	}

	// 检查follower任期
	if resp.Term > req.Term {
		r.handleStaleTerm(f)
		return true, nil
	}

	f.setLastContact()

	if resp.Success {
		atomic.StoreUint64(&f.nextIndex, meta.Index+1) // 设置follower下一次发送日志的索引
		f.commitment.match(peer.ID, meta.Index)
		f.failures = 0
		f.notifyAll(true)
	} else {
		f.failures++
		r.logger.Warn("拒绝installSnapshot ", "peer", peer)
	}
	return false, nil
}

// ----------------------------------OVER--------------------------------------------

// replicateTo 复制到最新的索引；   用于更新follower副本的日志索引,同一个节点不是并发调用的
// 非管道模式
func (r *Raft) replicateTo(f *followerReplication, lastIndex uint64) (shouldStop bool) {
	var req AppendEntriesRequest
	var resp AppendEntriesResponse
	var peer Server

START:
	// 防止错误时重试率过高，等一会儿
	if f.failures > 0 {
		select {
		case <-time.After(backoff(failureWait, f.failures, maxFailureScale)):
		case <-r.shutdownCh:
		}
	}

	f.peerLock.RLock()
	peer = f.peer
	f.peerLock.RUnlock()
	err := r.setupAppendEntries(f, &req, atomic.LoadUint64(&f.nextIndex), lastIndex)
	// 填充req需要携带的内容
	// todo 那么在查找A---> B的日志的时候，就会报 ErrLogNotFound

	if err == ErrLogNotFound { // 在读取日志时没有查到
		goto SendSnap // 只有这一种情况，会触发sendLatestSnapshot
	} else if err != nil {
		// 其他错误
		return
	}
	// 发起请求调用
	if err := r.trans.AppendEntries(peer.ID, peer.Address, &req, &resp); err != nil {
		// 失败的情况,
		// Follower 数据落后于本节点
		r.logger.Error("appendEntries失败", "peer", peer, "error", err)
		f.failures++
		return
	}
	// 开始新的任期，停止运行
	if resp.Term > req.Term {
		r.handleStaleTerm(f)
		return true
	}

	// 更新与follower的通信时间
	f.setLastContact()

	if resp.Success {
		// 更新副本状态
		updateLastAppended(f, &req)
		// 清理失败计数、允许管道传输
		f.failures = 0
		f.allowPipeline = true
	} else {
		atomic.StoreUint64(&f.nextIndex, max(min(f.nextIndex-1, resp.LastLog+1), 1))
		if resp.NoRetryBackoff {
			f.failures = 0
		} else {
			f.failures++
		}
		r.logger.Warn("拒绝appendEntries，发送旧的日志", "peer", peer, "next", atomic.LoadUint64(&f.nextIndex))
	}

CheckMore:
	select {
	case <-f.stopCh:
		return true
	default:
	}

	// 检查是否有更多的日志要复制到follower
	if atomic.LoadUint64(&f.nextIndex) <= lastIndex {
		goto START
	}
	return

	//	 当获取日志失败,通常是因为follower落后太多，我们必须用快照来代替。
	//	 比方说leader的db删除了，但是还有快照
SendSnap:
	if stop, err := r.sendLatestSnapshot(f); stop {
		return true
	} else if err != nil {
		r.logger.Error("发送快照失败", "peer", peer, "error", err)
		return
	}

	// 检查是否有更多需要复制的内容
	goto CheckMore
}

// OK
func (r *Raft) setPreviousLog(req *AppendEntriesRequest, nextIndex uint64) error {
	// nextIndex 下一条要发送给follower的日志索引
	lastSnapIdx, lastSnapTerm := r.getLastSnapshot()
	if nextIndex == 1 {
		req.PrevLogEntry = 0
		req.PrevLogTerm = 0
	} else if (nextIndex - 1) == lastSnapIdx { // 下一条-1==最新的快照索引; 之前的日志所有索引就是 最新的日志索引
		req.PrevLogEntry = lastSnapIdx
		req.PrevLogTerm = lastSnapTerm
	} else { // 有新日志产生，且没有写入快照中
		var l Log
		if err := r.logs.GetLog(nextIndex-1, &l); err != nil { //获取当前要发送给follower的日志数据
			r.logger.Error("获取日志失败", "index", nextIndex-1, "error", err)
			return err
		}
		// 设置之前的日志索引、任期
		req.PrevLogEntry = l.Index
		req.PrevLogTerm = l.Term
	}
	return nil
}

// setNewLogs 从db读取指定索引段的日志，并将其添加到req
func (r *Raft) setNewLogs(req *AppendEntriesRequest, nextIndex, lastIndex uint64) error {
	// nextIndex 下一条要发送给follower的日志索引
	// 我们需要在下面几行中使用一个一致的maxAppendEntries的值，以备它可以重新加载。
	maxAppendEntries := r.config().MaxAppendEntries // 日志允许的最大一次性可追加条数,默认64
	req.Entries = make([]*Log, 0, maxAppendEntries)

	maxIndex := min(nextIndex+uint64(maxAppendEntries)-1, lastIndex) // 日志最大索引 要发送哪
	for i := nextIndex; i <= maxIndex; i++ {
		oldLog := new(Log)
		if err := r.logs.GetLog(i, oldLog); err != nil {
			r.logger.Error("获取日志失败", "index", i, "error", err)
			return err
		}
		req.Entries = append(req.Entries, oldLog)
	}
	return nil
}

// notifyAll 是用来通知所有在此follower等待中的verifyFuture。如果Follower认为我们仍然是领导者。
func (f *followerReplication) notifyAll(leader bool) {
	// 清除等待通知，尽量减少锁定时间
	f.notifyLock.Lock()
	n := f.notify
	f.notify = make(map[*verifyFuture]struct{})
	f.notifyLock.Unlock()
	_ = (&Raft{}).verifyLeader // 会将future放入到每一个follower的 notify
	// 确认我们的选举权
	for v := range n {
		v.vote(leader)
	}
}

// cleanNotify 用于删除待验证的请求
func (f *followerReplication) cleanNotify(v *verifyFuture) {
	f.notifyLock.Lock()
	delete(f.notify, v)
	f.notifyLock.Unlock()
}

// LastContact 返回最新的通信时间
func (f *followerReplication) LastContact() time.Time {
	f.lastContactLock.RLock()
	last := f.lastContact
	f.lastContactLock.RUnlock()
	return last
}

// setLastContact 设置与follower最新的通信时间
func (f *followerReplication) setLastContact() {
	f.lastContactLock.Lock()
	f.lastContact = time.Now()
	f.lastContactLock.Unlock()
}

// Ph 打印心跳的信息
func (r *Raft) Ph(req *AppendEntriesRequest, resp *AppendEntriesResponse) {
	return
	type A struct {
		RPCHeader
		Term              uint64
		Leader            string
		PrevLogEntry      uint64
		PrevLogTerm       uint64
		LeaderCommitIndex uint64
	}
	a := A{
		RPCHeader:         req.RPCHeader,
		Leader:            string(req.Leader),
		PrevLogEntry:      req.PrevLogEntry,
		PrevLogTerm:       req.PrevLogTerm,
		LeaderCommitIndex: req.LeaderCommitIndex,
		Term:              req.Term,
	}
	r.logger.Info("心跳请求", "=:", fmt.Sprintf("%+v", a))
	r.logger.Info("心跳响应", "=:", fmt.Sprintf("%+v", resp))

}

// heartbeat 用于定期调用对等体上的AppendEntries，以确保它们不会超时。这是与replicate()异步进行的，因为该例程有可能在磁盘IO上被阻塞。
func (r *Raft) heartbeat(f *followerReplication, stopCh chan struct{}) {
	var failures uint64
	req := AppendEntriesRequest{
		RPCHeader: r.getRPCHeader(),                           // 当前协议版本
		Term:      f.currentTerm,                              // leader 任期
		Leader:    r.trans.EncodePeer(r.localID, r.localAddr), // 这里就是把localAddr string  变成了[]byte
	}
	var resp AppendEntriesResponse
	for {
		select {
		case <-f.notifyCh: // 心跳主动通知
			_ = r.verifyLeader
		case <-randomTimeout(r.config().HeartbeatTimeout / 10): //定时  100ms
		//在100ms内 每循环一次,就会产生一个timer,如果循环太多，可能导致gc飙升
		case <-stopCh:
			return
		}
		f.peerLock.RLock()
		peer := f.peer
		f.peerLock.RUnlock()
		// 发送追加日志请求
		if err := r.trans.AppendEntries(peer.ID, peer.Address, &req, &resp); err != nil {
			r.logger.Error("心跳失败", "peer", peer.Address, "error", err)
			failures++
			select {
			case <-time.After(backoff(failureWait, failures, maxFailureScale)):
			case <-stopCh:
			}
		} else {
			r.Ph(&req, &resp) // 打印信息
			f.setLastContact()
			failures = 0
			// 重复的信息。保留是为了向后兼容。
			f.notifyAll(resp.Success)
		}
	}
}

// replicate goroutine长期运行，它将日志条目复制到一个特定的follower。
// 对每个follower启动一个replicate  ，承担心跳、日志同步的职责
// stopCh|triggerDeferErrorCh|triggerCh|CommitTimeout 都会触发replicateTo
func (r *Raft) replicate(f *followerReplication) {
	// 开启异步心跳请求
	stopHeartbeat := make(chan struct{})
	defer close(stopHeartbeat)
	r.goFunc(func() { r.heartbeat(f, stopHeartbeat) }) // 心跳检测,并对verifyFuture.vote++
	// 与日志同步是分开调用的
	//	 以下是日志复制
RPC:
	shouldStop := false // 应该停止
	for !shouldStop {
		select {
		case maxIndex := <-f.stopCh:
			// 尽最大努力复制到这个索引
			if maxIndex > 0 {
				r.replicateTo(f, maxIndex)
			}
			return
		case deferErr := <-f.triggerDeferErrorCh:
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.replicateTo(f, lastLogIdx)
			if !shouldStop {
				deferErr.respond(nil)
			} else {
				deferErr.respond(fmt.Errorf("复制失败"))
			}
		case <-f.triggerCh: // 有新的日志产生 ✅
			_ = r.dispatchLogs
			// 获取当前日志库的最新索引
			lastLogIdx, _ := r.getLastLog() // 之前存储了一条Type:LogConfiguration数据  ----> 1
			shouldStop = r.replicateTo(f, lastLogIdx)
		// 这不是我们的心跳机制，而是为了确保在筏式提交停止自然流动时，跟随者能迅速了解领导者的提交索引。
		// 实际的心跳不能做到这一点，以保持它们不被跟随者的磁盘IO所阻挡。
		// See https://github.com/hashicorp/raft/issues/282.
		case <-randomTimeout(r.config().CommitTimeout):
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.replicateTo(f, lastLogIdx)
		}

		// 如果事情看起来很健康，就切换到管道模式
		if !shouldStop && f.allowPipeline { // 走到这的时候是不支持
			goto PIPELINE
		}
	}
	return

PIPELINE:
	// 禁用,直到重新启用
	f.allowPipeline = false
	f.peerLock.RLock()
	peer := f.peer
	f.peerLock.RUnlock()
	// 使用管道进行复制以获得高性能。这种方法不能优雅地从错误中恢复，所以我们在失败时退回到标准模式。
	if err := r.pipelineReplicate(f); err != nil { // 会阻塞的
		if err != ErrPipelineReplicationNotSupported {
			r.logger.Error("不能开启管道复制数据到", "peer", peer, "error", err)
		}
	} else {
		// 记录管道的启动和停止
		r.logger.Info("流水线复制已开启", "follower node", peer)
	}
	goto RPC
}

// pipelineDecode 解码响应, 更新状态、commit index、检查是不是leader
func (r *Raft) pipelineDecode(f *followerReplication, p AppendPipeline, stopCh, finishCh chan struct{}) {
	defer close(finishCh)
	respCh := p.Consumer()
	for {
		select {
		case ready := <-respCh:
			f.peerLock.RLock()
			f.peerLock.RUnlock()
			// 请求参数、响应
			req, resp := ready.Request(), ready.Response()

			// 检查是否有新的任期
			if resp.Term > req.Term {
				r.handleStaleTerm(f) // 设置当前角色为follower ,取消所有verifyFuture
				return
			}

			// 更新最新的通信时间
			f.setLastContact()

			// 退出管道模式
			if !resp.Success {
				return
			}

			// 更新复制状态
			updateLastAppended(f, req)
		case <-stopCh:
			return
		}
	}
}

// setupAppendEntries 是用来设置一个追加条目的请求。管道模式、非管道模式都会调用这个函数
func (r *Raft) setupAppendEntries(f *followerReplication, req *AppendEntriesRequest, nextIndex, lastIndex uint64) error {
	// nextIndex 下一条要发送给follower的日志索引
	req.RPCHeader = r.getRPCHeader()
	req.Term = f.currentTerm                                 // leader 的任期
	req.Leader = r.trans.EncodePeer(r.localID, r.localAddr)  // localAddr
	req.LeaderCommitIndex = r.getCommitIndex()               // leader最新的提交索引
	if err := r.setPreviousLog(req, nextIndex); err != nil { // 设置leader已有、follower未有的日志索引
		return err
	}
	if err := r.setNewLogs(req, nextIndex, lastIndex); err != nil { //从db读取指定索引段的日志，并将其添加到req
		return err
	}
	return nil
}

// updateLastAppended 用于在 AppendEntries RPC 成功后更新跟随者的复制状态。
func updateLastAppended(f *followerReplication, req *AppendEntriesRequest) {
	// Mark any inflight logs as committed
	// 将任何机上记录标记为已提交
	logs := req.Entries
	if len(logs) > 0 {
		last := logs[len(logs)-1]
		atomic.StoreUint64(&f.nextIndex, last.Index+1)
		f.commitment.match(f.peer.ID, last.Index)
	}
	// 通知所有Future，依然是leader
	f.notifyAll(true)
}

// handleStaleTerm  当follower的任期大于当前leader节点的任期
func (r *Raft) handleStaleTerm(f *followerReplication) {
	r.logger.Error("对端有一个新的任期，停止复制", "peer", f.peer)
	f.notifyAll(false) // 不再是leader
	asyncNotifyCh(f.stepDown)
}

// pipelineReplicate
// 是在我们已经与Follower同步了我们的状态，并且想切换到更高性能的管道复制模式时使用的。
// 我们只对AppendEntries命令进行流水线处理，如果我们遇到错误，我们就回到标准的复制模式，这样可以处理更复杂的情况。
// stopCh|triggerDeferErrorCh|triggerCh|CommitTimeout 都会触发replicateTo
func (r *Raft) pipelineReplicate(f *followerReplication) error {
	fmt.Println("[***pipelineReplicate***]")
	f.peerLock.RLock()
	peer := f.peer // follower节点
	f.peerLock.RUnlock()
	// 开启了一个处理pipeline响应的goroutine raft/net_transport.go:682
	pipeline, err := r.trans.AppendEntriesPipeline(peer.ID, peer.Address)
	_ = pipeline.(*netPipeline).decodeResponses
	if err != nil {
		return err
	}
	defer pipeline.Close()

	// 创建停止、结束 channel
	stopCh := make(chan struct{})   // 2种情况： 退出管道模式;finishCh关闭
	finishCh := make(chan struct{}) // 3种情况: follower任期>当前任期;管道传输出错;stopCh关闭

	// 启动一个专门的解码器
	r.goFunc(func() { r.pipelineDecode(f, pipeline, stopCh, finishCh) })

	nextIndex := atomic.LoadUint64(&f.nextIndex)

	shouldStop := false
SEND:
	for !shouldStop {
		select {
		case <-finishCh:
			break SEND
		case maxIndex := <-f.stopCh: // 节点移除
			// 尽最大努力复制到这个索引
			if maxIndex > 0 {
				r.pipelineSend(f, pipeline, &nextIndex, maxIndex)
			}
			break SEND
		case deferErr := <-f.triggerDeferErrorCh:
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.pipelineSend(f, pipeline, &nextIndex, lastLogIdx)
			if !shouldStop {
				deferErr.respond(nil)
			} else {
				deferErr.respond(fmt.Errorf("复制出错"))
			}
		case <-f.triggerCh:
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.pipelineSend(f, pipeline, &nextIndex, lastLogIdx)
		case <-randomTimeout(r.config().CommitTimeout):
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.pipelineSend(f, pipeline, &nextIndex, lastLogIdx)
		}
	}

	// 停止解码器，等待结束
	close(stopCh)
	select {
	case <-finishCh:
	case <-r.shutdownCh:
	}
	return nil
}

// pipelineSend 发送数据
func (r *Raft) pipelineSend(f *followerReplication, p AppendPipeline, nextIdx *uint64, lastIndex uint64) (shouldStop bool) {
	// 非管道模式  r.replicateTo(s, lastLogIdx)
	// nextIndex := atomic.LoadUint64(&s.nextIndex)
	req := new(AppendEntriesRequest)
	if err := r.setupAppendEntries(f, req, *nextIdx, lastIndex); err != nil {
		return true
	}

	// 对追加条目进行流水线处理
	if _, err := p.AppendEntries(req, new(AppendEntriesResponse)); err != nil {
		r.logger.Error("失败 pipeline appendEntries", "peer", f.peer, "error", err)
		return true
	}

	// 增加下次发送日志以避免重新发送旧日志
	if n := len(req.Entries); n > 0 {
		last := req.Entries[n-1]
		atomic.StoreUint64(nextIdx, last.Index+1)
	}
	return false
}
