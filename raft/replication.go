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
	failureWait     = 10 * time.Millisecond
)

var (
	// ErrLogNotFound indicates a given log entry is not available.
	ErrLogNotFound = errors.New("log not found")

	// ErrPipelineReplicationNotSupported can be returned by the transport to
	// signal that pipeline replication is not supported in general, and that
	// no error message should be produced.
	ErrPipelineReplicationNotSupported = errors.New("pipeline replication not supported")
)

// followerReplication 负责在这个特定的期限内将这个leader的快照和日志条目发送给一个远程的follower。
type followerReplication struct {
	// 32位平台的为了原子操作 需要 64bit对齐
	// currentTerm leader当前的任期
	currentTerm uint64
	// nextIndex 吓一跳要发送给follower的日志
	nextIndex uint64

	// peer 包含远程follower的网络地址和ID。
	peer     Server
	peerLock sync.RWMutex

	// commitment 跟踪被follower承认的条目，以便领导者的commit index可以增加。它在成功的AppendEntries响应时被更新。
	commitment *commitment

	// 当leader变换 发送通知
	// follower从集群移除->close. 携带一个日志索引，应该尽最大努力通过该索引来尝试复制。  在退出之前，
	stopCh chan uint64

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
	notifyCh chan struct{}
	// notify  在收到确认信息后，需要解决的回应
	notify     map[*verifyFuture]struct{}
	notifyLock sync.Mutex

	// stepDown 是用来向leader表明，我们应该根据follower的信息下台。
	stepDown chan struct{}

	// allowPipeline 用来决定何时对AppendEntries RPC进行流水线处理。它对这个复制程序来说是私有的。
	allowPipeline bool
}

// notifyAll is used to notify all the waiting verify futures
// if the follower believes we are still the leader.
func (s *followerReplication) notifyAll(leader bool) {
	// Clear the waiting notifies minimizing lock time
	s.notifyLock.Lock()
	n := s.notify
	s.notify = make(map[*verifyFuture]struct{})
	s.notifyLock.Unlock()

	// Submit our votes
	for v := range n {
		v.vote(leader)
	}
}

// cleanNotify is used to delete notify, .
func (s *followerReplication) cleanNotify(v *verifyFuture) {
	s.notifyLock.Lock()
	delete(s.notify, v)
	s.notifyLock.Unlock()
}

// LastContact returns the time of last contact.
func (s *followerReplication) LastContact() time.Time {
	s.lastContactLock.RLock()
	last := s.lastContact
	s.lastContactLock.RUnlock()
	return last
}

// setLastContact 设置与follower最新的通信时间
func (s *followerReplication) setLastContact() {
	s.lastContactLock.Lock()
	s.lastContact = time.Now()
	s.lastContactLock.Unlock()
}

// replicate goroutine长期运行，它将日志条目复制到一个特定的follower。
func (r *Raft) replicate(s *followerReplication) {
	// 开启异步心跳请求
	stopHeartbeat := make(chan struct{})
	defer close(stopHeartbeat)
	r.goFunc(func() { r.heartbeat(s, stopHeartbeat) })

RPC:
	shouldStop := false
	for !shouldStop {
		select {
		case maxIndex := <-s.stopCh:
			// Make a best effort to replicate up to this index
			if maxIndex > 0 {
				r.replicateTo(s, maxIndex)
			}
			return
		case deferErr := <-s.triggerDeferErrorCh:
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.replicateTo(s, lastLogIdx)
			if !shouldStop {
				deferErr.respond(nil)
			} else {
				deferErr.respond(fmt.Errorf("replication failed"))
			}
		case <-s.triggerCh:
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.replicateTo(s, lastLogIdx)
		// This is _not_ our heartbeat mechanism but is to ensure
		// followers quickly learn the leader's commit index when
		// raft commits stop flowing naturally. The actual heartbeats
		// can't do this to keep them unblocked by disk IO on the
		// follower. See https://github.com/hashicorp/raft/issues/282.
		case <-randomTimeout(r.config().CommitTimeout):
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.replicateTo(s, lastLogIdx)
		}

		// If things looks healthy, switch to pipeline mode
		if !shouldStop && s.allowPipeline {
			goto PIPELINE
		}
	}
	return

PIPELINE:
	// Disable until re-enabled
	s.allowPipeline = false

	// Replicates using a pipeline for high performance. This method
	// is not able to gracefully recover from errors, and so we fall back
	// to standard mode on failure.
	if err := r.pipelineReplicate(s); err != nil {
		if err != ErrPipelineReplicationNotSupported {
			s.peerLock.RLock()
			peer := s.peer
			s.peerLock.RUnlock()
			r.logger.Error("failed to start pipeline replication to", "peer", peer, "error", err)
		}
	}
	goto RPC
}

// replicateTo is a helper to replicate(), used to replicate the logs up to a
// given last index.
// If the follower log is behind, we take care to bring them up to date.
func (r *Raft) replicateTo(s *followerReplication, lastIndex uint64) (shouldStop bool) {
	// Create the base request
	var req AppendEntriesRequest
	var resp AppendEntriesResponse
	var start time.Time
	var peer Server

START:
	// Prevent an excessive retry rate on errors
	if s.failures > 0 {
		select {
		case <-time.After(backoff(failureWait, s.failures, maxFailureScale)):
		case <-r.shutdownCh:
		}
	}

	s.peerLock.RLock()
	peer = s.peer
	s.peerLock.RUnlock()

	// Setup the request
	if err := r.setupAppendEntries(s, &req, atomic.LoadUint64(&s.nextIndex), lastIndex); err == ErrLogNotFound {
		goto SEND_SNAP
	} else if err != nil {
		return
	}

	// Make the RPC call
	start = time.Now()
	if err := r.trans.AppendEntries(peer.ID, peer.Address, &req, &resp); err != nil {
		r.logger.Error("failed to appendEntries to", "peer", peer, "error", err)
		s.failures++
		return
	}
	appendStats(string(peer.ID), start, float32(len(req.Entries)))

	// Check for a newer term, stop running
	if resp.Term > req.Term {
		r.handleStaleTerm(s)
		return true
	}

	// Update the last contact
	s.setLastContact()

	// Update s based on success
	if resp.Success {
		// Update our replication state
		updateLastAppended(s, &req)

		// Clear any failures, allow pipelining
		s.failures = 0
		s.allowPipeline = true
	} else {
		atomic.StoreUint64(&s.nextIndex, max(min(s.nextIndex-1, resp.LastLog+1), 1))
		if resp.NoRetryBackoff {
			s.failures = 0
		} else {
			s.failures++
		}
		r.logger.Warn("appendEntries rejected, sending older logs", "peer", peer, "next", atomic.LoadUint64(&s.nextIndex))
	}

CHECK_MORE:
	// Poll the stop channel here in case we are looping and have been asked
	// to stop, or have stepped down as leader. Even for the best effort case
	// where we are asked to replicate to a given index and then shutdown,
	// it's better to not loop in here to send lots of entries to a straggler
	// that's leaving the cluster anyways.
	select {
	case <-s.stopCh:
		return true
	default:
	}

	// Check if there are more logs to replicate
	if atomic.LoadUint64(&s.nextIndex) <= lastIndex {
		goto START
	}
	return

	// SEND_SNAP is used when we fail to get a log, usually because the follower
	// is too far behind, and we must ship a snapshot down instead
SEND_SNAP:
	if stop, err := r.sendLatestSnapshot(s); stop {
		return true
	} else if err != nil {
		r.logger.Error("failed to send snapshot to", "peer", peer, "error", err)
		return
	}

	// Check if there is more to replicate
	goto CHECK_MORE
}

// sendLatestSnapshot is used to send the latest snapshot we have
// down to our follower.
func (r *Raft) sendLatestSnapshot(s *followerReplication) (bool, error) {
	// Get the snapshots
	snapshots, err := r.snapshots.List()
	if err != nil {
		r.logger.Error("failed to list snapshots", "error", err)
		return false, err
	}

	// Check we have at least a single snapshot
	if len(snapshots) == 0 {
		return false, fmt.Errorf("no snapshots found")
	}

	// Open the most recent snapshot
	snapID := snapshots[0].ID
	meta, snapshot, err := r.snapshots.Open(snapID)
	if err != nil {
		r.logger.Error("failed to open snapshot", "id", snapID, "error", err)
		return false, err
	}
	defer snapshot.Close()

	// Setup the request
	req := InstallSnapshotRequest{
		RPCHeader:          r.getRPCHeader(),
		SnapshotVersion:    meta.Version,
		Term:               s.currentTerm,
		Leader:             r.trans.EncodePeer(r.localID, r.localAddr),
		LastLogIndex:       meta.Index,
		LastLogTerm:        meta.Term,
		Peers:              meta.Peers,
		Size:               meta.Size,
		Configuration:      EncodeConfiguration(meta.Configuration),
		ConfigurationIndex: meta.ConfigurationIndex,
	}

	s.peerLock.RLock()
	peer := s.peer
	s.peerLock.RUnlock()

	// Make the call
	var resp InstallSnapshotResponse
	if err := r.trans.InstallSnapshot(peer.ID, peer.Address, &req, &resp, snapshot); err != nil {
		r.logger.Error("failed to install snapshot", "id", snapID, "error", err)
		s.failures++
		return false, err
	}

	// Check for a newer term, stop running
	if resp.Term > req.Term {
		r.handleStaleTerm(s)
		return true, nil
	}

	// Update the last contact
	s.setLastContact()

	// Check for success
	if resp.Success {
		// Update the indexes
		atomic.StoreUint64(&s.nextIndex, meta.Index+1)
		s.commitment.match(peer.ID, meta.Index)

		// Clear any failures
		s.failures = 0

		// Notify we are still leader
		s.notifyAll(true)
	} else {
		s.failures++
		r.logger.Warn("installSnapshot rejected to", "peer", peer)
	}
	return false, nil
}

// heartbeat 用于定期调用对等体上的AppendEntries，以确保它们不会超时。这是与replicate()异步进行的，因为该例程有可能在磁盘IO上被阻塞。
func (r *Raft) heartbeat(s *followerReplication, stopCh chan struct{}) {
	var failures uint64
	req := AppendEntriesRequest{
		RPCHeader: r.getRPCHeader(),                           // 当前协议版本
		Term:      s.currentTerm,                              // leader 任期
		Leader:    r.trans.EncodePeer(r.localID, r.localAddr), // 这里就是把localAddr string  变成了[]byte
	}
	var resp AppendEntriesResponse
	for {
		select {
		case <-s.notifyCh: // 心跳主动通知
		case <-randomTimeout(r.config().HeartbeatTimeout / 10): //定时
		case <-stopCh:
			return
		}

		s.peerLock.RLock()
		peer := s.peer
		s.peerLock.RUnlock()
		// 发送追加日志请求
		if err := r.trans.AppendEntries(peer.ID, peer.Address, &req, &resp); err != nil {
			r.logger.Error("failed to heartbeat to", "peer", peer.Address, "error", err)
			r.observe(FailedHeartbeatObservation{PeerID: peer.ID, LastContact: s.LastContact()})
			failures++
			select {
			case <-time.After(backoff(failureWait, failures, maxFailureScale)):
			case <-stopCh:
			}
		} else {
			if failures > 0 {
				r.observe(ResumedHeartbeatObservation{PeerID: peer.ID})
			}
			s.setLastContact()
			failures = 0
			// Duplicated information. Kept for backward compatibility.
			s.notifyAll(resp.Success)
		}
	}
}

// pipelineReplicate is used when we have synchronized our state with the follower,
// and want to switch to a higher performance pipeline mode of replication.
// We only pipeline AppendEntries commands, and if we ever hit an error, we fall
// back to the standard replication which can handle more complex situations.
func (r *Raft) pipelineReplicate(s *followerReplication) error {
	s.peerLock.RLock()
	peer := s.peer // follower节点
	s.peerLock.RUnlock()

	// Create a new pipeline
	pipeline, err := r.trans.AppendEntriesPipeline(peer.ID, peer.Address)
	if err != nil {
		return err
	}
	defer pipeline.Close()

	// 记录管道的启动和停止
	r.logger.Info("流水线复制", "follower node", peer)
	defer r.logger.Info("中止流水线复制", "follower node", peer)

	// 创建停止、结束 channel
	stopCh := make(chan struct{})
	finishCh := make(chan struct{})

	// 启动一个专门的解码器
	r.goFunc(func() { r.pipelineDecode(s, pipeline, stopCh, finishCh) })

	// 在最后一个好的NextIndex处开始 管道发送
	nextIndex := atomic.LoadUint64(&s.nextIndex)

	shouldStop := false
SEND:
	for !shouldStop {
		select {
		case <-finishCh:
			break SEND
		case maxIndex := <-s.stopCh:
			// 尽最大努力复制到这个索引
			if maxIndex > 0 {
				r.pipelineSend(s, pipeline, &nextIndex, maxIndex)
			}
			break SEND
		case deferErr := <-s.triggerDeferErrorCh:
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.pipelineSend(s, pipeline, &nextIndex, lastLogIdx)
			if !shouldStop {
				deferErr.respond(nil)
			} else {
				deferErr.respond(fmt.Errorf("复制出错"))
			}
		case <-s.triggerCh:
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.pipelineSend(s, pipeline, &nextIndex, lastLogIdx)
		case <-randomTimeout(r.config().CommitTimeout):
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.pipelineSend(s, pipeline, &nextIndex, lastLogIdx)
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

// pipelineSend is used to send data over a pipeline. It is a helper to
// pipelineReplicate.
func (r *Raft) pipelineSend(s *followerReplication, p AppendPipeline, nextIdx *uint64, lastIndex uint64) (shouldStop bool) {
	// Create a new append request
	req := new(AppendEntriesRequest)
	if err := r.setupAppendEntries(s, req, *nextIdx, lastIndex); err != nil {
		return true
	}

	// Pipeline the append entries
	if _, err := p.AppendEntries(req, new(AppendEntriesResponse)); err != nil {
		r.logger.Error("failed to pipeline appendEntries", "peer", s.peer, "error", err)
		return true
	}

	// Increase the next send log to avoid re-sending old logs
	if n := len(req.Entries); n > 0 {
		last := req.Entries[n-1]
		atomic.StoreUint64(nextIdx, last.Index+1)
	}
	return false
}

// pipelineDecode is used to decode the responses of pipelined requests.
func (r *Raft) pipelineDecode(s *followerReplication, p AppendPipeline, stopCh, finishCh chan struct{}) {
	defer close(finishCh)
	respCh := p.Consumer()
	for {
		select {
		case ready := <-respCh:
			s.peerLock.RLock()
			peer := s.peer
			s.peerLock.RUnlock()

			req, resp := ready.Request(), ready.Response()
			appendStats(string(peer.ID), ready.Start(), float32(len(req.Entries)))

			// Check for a newer term, stop running
			if resp.Term > req.Term {
				r.handleStaleTerm(s)
				return
			}

			// Update the last contact
			s.setLastContact()

			// Abort pipeline if not successful
			if !resp.Success {
				return
			}

			// Update our replication state
			updateLastAppended(s, req)
		case <-stopCh:
			return
		}
	}
}

// setupAppendEntries is used to setup an append entries request.
func (r *Raft) setupAppendEntries(s *followerReplication, req *AppendEntriesRequest, nextIndex, lastIndex uint64) error {
	req.RPCHeader = r.getRPCHeader()
	req.Term = s.currentTerm
	req.Leader = r.trans.EncodePeer(r.localID, r.localAddr)
	req.LeaderCommitIndex = r.getCommitIndex()
	if err := r.setPreviousLog(req, nextIndex); err != nil {
		return err
	}
	if err := r.setNewLogs(req, nextIndex, lastIndex); err != nil {
		return err
	}
	return nil
}

// setPreviousLog is used to setup the PrevLogEntry and PrevLogTerm for an
// AppendEntriesRequest given the next index to replicate.
func (r *Raft) setPreviousLog(req *AppendEntriesRequest, nextIndex uint64) error {
	// Guard for the first index, since there is no 0 log entry
	// Guard against the previous index being a snapshot as well
	lastSnapIdx, lastSnapTerm := r.getLastSnapshot()
	if nextIndex == 1 {
		req.PrevLogEntry = 0
		req.PrevLogTerm = 0

	} else if (nextIndex - 1) == lastSnapIdx {
		req.PrevLogEntry = lastSnapIdx
		req.PrevLogTerm = lastSnapTerm

	} else {
		var l Log
		if err := r.logs.GetLog(nextIndex-1, &l); err != nil {
			r.logger.Error("failed to get log", "index", nextIndex-1, "error", err)
			return err
		}

		// Set the previous index and term (0 if nextIndex is 1)
		req.PrevLogEntry = l.Index
		req.PrevLogTerm = l.Term
	}
	return nil
}

// setNewLogs is used to setup the logs which should be appended for a request.
func (r *Raft) setNewLogs(req *AppendEntriesRequest, nextIndex, lastIndex uint64) error {
	// Append up to MaxAppendEntries or up to the lastIndex. we need to use a
	// consistent value for maxAppendEntries in the lines below in case it ever
	// becomes reloadable.
	maxAppendEntries := r.config().MaxAppendEntries
	req.Entries = make([]*Log, 0, maxAppendEntries)
	maxIndex := min(nextIndex+uint64(maxAppendEntries)-1, lastIndex)
	for i := nextIndex; i <= maxIndex; i++ {
		oldLog := new(Log)
		if err := r.logs.GetLog(i, oldLog); err != nil {
			r.logger.Error("failed to get log", "index", i, "error", err)
			return err
		}
		req.Entries = append(req.Entries, oldLog)
	}
	return nil
}

// appendStats is used to emit stats about an AppendEntries invocation.
func appendStats(peer string, start time.Time, logs float32) {
}

// handleStaleTerm is used when a follower indicates that we have a stale term.
func (r *Raft) handleStaleTerm(s *followerReplication) {
	r.logger.Error("peer has newer term, stopping replication", "peer", s.peer)
	s.notifyAll(false) // No longer leader
	asyncNotifyCh(s.stepDown)
}

// updateLastAppended is used to update follower replication state after a
// successful AppendEntries RPC.
// TODO: This isn't used during InstallSnapshot, but the code there is similar.
func updateLastAppended(s *followerReplication, req *AppendEntriesRequest) {
	// Mark any inflight logs as committed
	if logs := req.Entries; len(logs) > 0 {
		last := logs[len(logs)-1]
		atomic.StoreUint64(&s.nextIndex, last.Index+1)
		s.commitment.match(s.peer.ID, last.Index)
	}

	// Notify still leader
	s.notifyAll(true)
}
