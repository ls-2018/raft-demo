package raft

import (
	"bytes"
	"container/list"
	"fmt"
	"io"
	"io/ioutil"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	. "raft-demo/raft-boltdb/var"
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
	inflight                     *list.List // 按日志索引顺序排列的logFuture列表
	replState                    map[ServerID]*followerReplication
	notify                       map[*verifyFuture]struct{}
	stepDown                     chan struct{}
}

// setLeader 是用来修改集群的当前领导者的
func (r *Raft) setLeader(leader ServerAddress) {
	r.leaderLock.Lock()
	oldLeader := r.leader
	r.leader = leader
	r.leaderLock.Unlock()
	if oldLeader != leader {
		// 之前有，现在没有;之前没有现在没有;之前没有 现在有,但是不一样
		r.observe(LeaderObservation{Leader: leader})
	}
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

// runFollower 为一个flower运行FSM。
func (r *Raft) runFollower() {
	didWarn := false
	r.logger.Info("进入 follower state", "follower", r, "leader", r.Leader())
	heartbeatTimer := randomTimeout(r.config().HeartbeatTimeout) // 定时器

	for r.getState() == Follower {
		select {
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)
		//-----------------------------------
		// 拒绝任何操作，当不是leader
		case c := <-r.configurationChangeCh:
			c.respond(ErrNotLeader)
		case a := <-r.applyCh:
			a.respond(ErrNotLeader)
		case v := <-r.verifyCh:
			v.respond(ErrNotLeader)
		case r := <-r.userRestoreCh:
			r.respond(ErrNotLeader)
		case r := <-r.leadershipTransferCh:
			r.respond(ErrNotLeader)
		//-----------------------------------
		case c := <-r.configurationsCh:
			c.configurations = r.configurations.Clone()
			c.respond(nil)

		case b := <-r.bootstrapCh:
			b.respond(r.liveBootstrap(b.configuration))

		case <-heartbeatTimer:
			// node 启动后，默认等待超时，然后进入这里
			// 重新启动心跳定时器
			hbTimeout := r.config().HeartbeatTimeout
			heartbeatTimer = randomTimeout(hbTimeout)

			// 检查我们是否有成功的联系
			lastContact := r.LastContact()
			if time.Now().Sub(lastContact) < hbTimeout {
				// 与上次通信时间之差  小于 心跳超时时间
				continue
			}

			// 心跳检查失败，进入候选者状态
			lastLeader := r.Leader()
			r.setLeader("")

			if r.configurations.latestIndex == 0 {
				if !didWarn {
					r.logger.Warn("没有已知的peers，中止选举")
					didWarn = true
				}
				//	最新日志==已提交日志 && 本节点的逻辑ID 不是一个选民
			} else if r.configurations.latestIndex == r.configurations.committedIndex && !hasVote(r.configurations.latest, r.localID) {
				if !didWarn {
					r.logger.Warn("不是稳定配置的一部分，中止选举")
					didWarn = true
				}
			} else {
				// 是一个选民
				if hasVote(r.configurations.latest, r.localID) {
					r.logger.Warn("心跳超时，开始选举", "上一次的leader", lastLeader) // 上一次的leader
					r.setState(Candidate)
					return
				} else if !didWarn {
					r.logger.Warn("达到心跳超时，不是稳定配置的一部分或非投票者，没有触发领导者选举")
					didWarn = true
				}
			}

		case <-r.shutdownCh:
			return
		}
	}
}

// liveBootstrap attempts to seed an initial configuration for the cluster. See
// the Raft object's member BootstrapCluster for more details. This must only be
// called on the main thread, and only makes sense in the follower state.
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

// runCandidate 为一个candidate运行FSM
func (r *Raft) runCandidate() {

	// 首先投自己一票，并设置选举超时
	voteCh := r.electSelf()

	// Make sure the leadership transfer flag is reset after each run. Having this
	// flag will set the field LeadershipTransfer in a RequestVoteRequst to true,
	// which will make other servers vote even though they have a leader already.
	// It is important to reset that flag, because this priviledge could be abused
	// otherwise.
	defer func() { r.candidateFromLeadershipTransfer = false }()

	electionTimer := randomTimeout(r.config().ElectionTimeout)

	// Tally the votes, need a simple majority
	grantedVotes := 0
	votesNeeded := r.quorumSize()
	r.logger.Debug("votes", "needed", votesNeeded)

	for r.getState() == Candidate {
		select {
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)

		case vote := <-voteCh:
			// Check if the term is greater than ours, bail
			if vote.Term > r.getCurrentTerm() {
				r.logger.Debug("newer term discovered, fallback to follower")
				r.setState(Follower)
				r.setCurrentTerm(vote.Term)
				return
			}

			// Check if the vote is granted
			if vote.Granted {
				grantedVotes++
				r.logger.Debug("vote granted", "from", vote.voterID, "term", vote.Term, "tally", grantedVotes)
			}

			// Check if we've become the leader
			if grantedVotes >= votesNeeded {
				r.logger.Info("election won", "tally", grantedVotes)
				r.setState(Leader)
				r.setLeader(r.localAddr)
				return
			}
		//-----------------------------------
		// 拒绝任何操作，当不是leader
		case c := <-r.configurationChangeCh:
			c.respond(ErrNotLeader)
		case a := <-r.applyCh:
			a.respond(ErrNotLeader)
		case v := <-r.verifyCh:
			v.respond(ErrNotLeader)
		case r := <-r.userRestoreCh:
			r.respond(ErrNotLeader)
		case r := <-r.leadershipTransferCh:
			r.respond(ErrNotLeader)
		//-----------------------------------
		case c := <-r.configurationsCh:
			c.configurations = r.configurations.Clone()
			c.respond(nil)

		case b := <-r.bootstrapCh:
			b.respond(ErrCantBootstrap)

		case <-electionTimer:
			// Election failed! Restart the election. We simply return,
			// which will kick us back into runCandidate
			r.logger.Warn("Election timeout reached, restarting election")
			return

		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) setLeadershipTransferInProgress(v bool) {
	if v {
		atomic.StoreInt32(&r.leaderState.leadershipTransferInProgress, 1)
	} else {
		atomic.StoreInt32(&r.leaderState.leadershipTransferInProgress, 0)
	}
}

func (r *Raft) getLeadershipTransferInProgress() bool {
	v := atomic.LoadInt32(&r.leaderState.leadershipTransferInProgress)
	return v == 1
}

func (r *Raft) setupLeaderState() {
	r.leaderState.commitCh = make(chan struct{}, 1)
	r.leaderState.commitment = newCommitment(r.leaderState.commitCh,
		r.configurations.latest,
		r.getLastIndex()+1 /* first index that may be committed in this term */)
	r.leaderState.inflight = list.New()
	r.leaderState.replState = make(map[ServerID]*followerReplication)
	r.leaderState.notify = make(map[*verifyFuture]struct{})
	r.leaderState.stepDown = make(chan struct{}, 1)
}

// runLeader runs the FSM for a leader. Do the setup here and drop into
// the leaderLoop for the hot loop.
func (r *Raft) runLeader() {
	r.logger.Info("entering leader state", "leader", r)

	// Notify that we are the leader
	overrideNotifyBool(r.leaderCh, true)

	// Store the notify chan. It's not reloadable so shouldn't change before the
	// defer below runs, but this makes sure we always notify the same chan if
	// ever for both gaining and loosing leadership.
	notify := r.config().NotifyCh

	// Push to the notify channel if given
	if notify != nil {
		select {
		case notify <- true:
		case <-r.shutdownCh:
		}
	}

	// setup leader state. This is only supposed to be accessed within the
	// leaderloop.
	r.setupLeaderState()

	// Run a background go-routine to emit metrics on log age
	stopCh := make(chan struct{})
	go emitLogStoreMetrics(r.logs, []string{"raft", "leader"}, oldestLogGaugeInterval, stopCh)

	// Cleanup state on step down
	defer func() {
		close(stopCh)

		// Since we were the leader previously, we update our
		// last contact time when we step down, so that we are not
		// reporting a last contact time from before we were the
		// leader. Otherwise, to a client it would seem our data
		// is extremely stale.
		r.setLastContact()

		// Stop replication
		for _, p := range r.leaderState.replState {
			close(p.stopCh)
		}

		// Respond to all inflight operations
		for e := r.leaderState.inflight.Front(); e != nil; e = e.Next() {
			e.Value.(*logFuture).respond(ErrLeadershipLost)
		}

		// Respond to any pending verify requests
		for future := range r.leaderState.notify {
			future.respond(ErrLeadershipLost)
		}

		// Clear all the state
		r.leaderState.commitCh = nil
		r.leaderState.commitment = nil
		r.leaderState.inflight = nil
		r.leaderState.replState = nil
		r.leaderState.notify = nil
		r.leaderState.stepDown = nil

		// If we are stepping down for some reason, no known leader.
		// We may have stepped down due to an RPC call, which would
		// provide the leader, so we cannot always blank this out.
		r.leaderLock.Lock()
		if r.leader == r.localAddr {
			r.leader = ""
		}
		r.leaderLock.Unlock()

		// Notify that we are not the leader
		overrideNotifyBool(r.leaderCh, false)

		// Push to the notify channel if given
		if notify != nil {
			select {
			case notify <- false:
			case <-r.shutdownCh:
				// On shutdown, make a best effort but do not block
				select {
				case notify <- false:
				default:
				}
			}
		}
	}()

	// Start a replication routine for each peer
	r.startStopReplication()

	// Dispatch a no-op log entry first. This gets this leader up to the latest
	// possible commit index, even in the absence of client commands. This used
	// to append a configuration entry instead of a noop. However, that permits
	// an unbounded number of uncommitted configurations in the log. We now
	// maintain that there exists at most one uncommitted configuration entry in
	// any log, so we have to do proper no-ops here.
	noop := &logFuture{
		log: Log{
			Type: LogNoop,
		},
	}
	r.dispatchLogs([]*logFuture{noop})

	// Sit in the leader loop until we step down
	r.leaderLoop()
}

// startStopReplication will set up state and start asynchronous replication to
// new peers, and stop replication to removed peers. Before removing a peer,
// it'll instruct the replication routines to try to replicate to the current
// index. This must only be called from the main thread.
func (r *Raft) startStopReplication() {
	inConfig := make(map[ServerID]bool, len(r.configurations.latest.Servers))
	lastIdx := r.getLastIndex()

	// Start replication goroutines that need starting
	for _, server := range r.configurations.latest.Servers {
		if server.ID == r.localID {
			continue
		}

		inConfig[server.ID] = true

		s, ok := r.leaderState.replState[server.ID]
		if !ok {
			r.logger.Info("added peer, starting replication", "peer", server.ID)
			s = &followerReplication{
				peer:                server,
				commitment:          r.leaderState.commitment,
				stopCh:              make(chan uint64, 1),
				triggerCh:           make(chan struct{}, 1),
				triggerDeferErrorCh: make(chan *deferError, 1),
				currentTerm:         r.getCurrentTerm(),
				nextIndex:           lastIdx + 1,
				lastContact:         time.Now(),
				notify:              make(map[*verifyFuture]struct{}),
				notifyCh:            make(chan struct{}, 1),
				stepDown:            r.leaderState.stepDown,
			}

			r.leaderState.replState[server.ID] = s
			r.goFunc(func() { r.replicate(s) })
			asyncNotifyCh(s.triggerCh)
			r.observe(PeerObservation{Peer: server, Removed: false})
		} else if ok {

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

// leaderLoop is the hot loop for a leader. It is invoked
// after all the various leader setup is done.
func (r *Raft) leaderLoop() {
	// stepDown is used to track if there is an inflight log that
	// would cause us to lose leadership (specifically a RemovePeer of
	// ourselves). If this is the case, we must not allow any logs to
	// be processed in parallel, otherwise we are basing commit on
	// only a single peer (ourself) and replicating to an undefined set
	// of peers.
	stepDown := false
	// This is only used for the first lease check, we reload lease below
	// based on the current config value.
	lease := time.After(r.config().LeaderLeaseTimeout)

	for r.getState() == Leader {
		select {
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)

		case <-r.leaderState.stepDown:
			r.setState(Follower)

		case future := <-r.leadershipTransferCh:
			if r.getLeadershipTransferInProgress() {
				r.logger.Debug(ErrLeadershipTransferInProgress.Error())
				future.respond(ErrLeadershipTransferInProgress)
				continue
			}

			r.logger.Debug("starting leadership transfer", "id", future.ID, "address", future.Address)

			// When we are leaving leaderLoop, we are no longer
			// leader, so we should stop transferring.
			leftLeaderLoop := make(chan struct{})
			defer func() { close(leftLeaderLoop) }()

			stopCh := make(chan struct{})
			doneCh := make(chan error, 1)

			// This is intentionally being setup outside of the
			// leadershipTransfer function. Because the TimeoutNow
			// call is blocking and there is no way to abort that
			// in case eg the timer expires.
			// The leadershipTransfer function is controlled with
			// the stopCh and doneCh.
			go func() {
				select {
				case <-time.After(r.config().ElectionTimeout):
					close(stopCh)
					err := fmt.Errorf("leadership transfer timeout")
					r.logger.Debug(err.Error())
					future.respond(err)
					<-doneCh
				case <-leftLeaderLoop:
					close(stopCh)
					err := fmt.Errorf("lost leadership during transfer (expected)")
					r.logger.Debug(err.Error())
					future.respond(nil)
					<-doneCh
				case err := <-doneCh:
					if err != nil {
						r.logger.Debug(err.Error())
					}
					future.respond(err)
				}
			}()

			// leaderState.replState is accessed here before
			// starting leadership transfer asynchronously because
			// leaderState is only supposed to be accessed in the
			// leaderloop.
			id := future.ID
			address := future.Address
			if id == nil {
				s := r.pickServer()
				if s != nil {
					id = &s.ID
					address = &s.Address
				} else {
					doneCh <- fmt.Errorf("cannot find peer")
					continue
				}
			}
			state, ok := r.leaderState.replState[*id]
			if !ok {
				doneCh <- fmt.Errorf("cannot find replication state for %v", id)
				continue
			}

			go r.leadershipTransfer(*id, *address, state, stopCh, doneCh)

		case <-r.leaderState.commitCh:
			// Process the newly committed entries
			oldCommitIndex := r.getCommitIndex()
			commitIndex := r.leaderState.commitment.getCommitIndex()
			r.setCommitIndex(commitIndex)

			// New configration has been committed, set it as the committed
			// value.
			if r.configurations.latestIndex > oldCommitIndex &&
				r.configurations.latestIndex <= commitIndex {
				r.setCommittedConfiguration(r.configurations.latest, r.configurations.latestIndex)
				if !hasVote(r.configurations.committed, r.localID) {
					stepDown = true
				}
			}

			var groupReady []*list.Element
			var groupFutures = make(map[uint64]*logFuture)
			var lastIdxInGroup uint64

			// Pull all inflight logs that are committed off the queue.
			for e := r.leaderState.inflight.Front(); e != nil; e = e.Next() {
				commitLog := e.Value.(*logFuture)
				idx := commitLog.log.Index
				if idx > commitIndex {
					// Don't go past the committed index
					break
				}

				// Measure the commit time
				groupReady = append(groupReady, e)
				groupFutures[idx] = commitLog
				lastIdxInGroup = idx
			}

			// Process the group
			if len(groupReady) != 0 {
				r.processLogs(lastIdxInGroup, groupFutures)

				for _, e := range groupReady {
					r.leaderState.inflight.Remove(e)
				}
			}

			if stepDown {
				if r.config().ShutdownOnRemove {
					r.logger.Info("removed ourself, shutting down")
					r.Shutdown()
				} else {
					r.logger.Info("removed ourself, transitioning to follower")
					r.setState(Follower)
				}
			}

		case v := <-r.verifyCh:
			if v.quorumSize == 0 {
				// Just dispatched, start the verification
				r.verifyLeader(v)

			} else if v.votes < v.quorumSize {
				// Early return, means there must be a new leader
				r.logger.Warn("new leader elected, stepping down")
				r.setState(Follower)
				delete(r.leaderState.notify, v)
				for _, repl := range r.leaderState.replState {
					repl.cleanNotify(v)
				}
				v.respond(ErrNotLeader)

			} else {
				// Quorum of members agree, we are still leader
				delete(r.leaderState.notify, v)
				for _, repl := range r.leaderState.replState {
					repl.cleanNotify(v)
				}
				v.respond(nil)
			}

		case future := <-r.userRestoreCh:
			if r.getLeadershipTransferInProgress() {
				r.logger.Debug(ErrLeadershipTransferInProgress.Error())
				future.respond(ErrLeadershipTransferInProgress)
				continue
			}
			err := r.restoreUserSnapshot(future.meta, future.reader)
			future.respond(err)

		case future := <-r.configurationsCh:
			if r.getLeadershipTransferInProgress() {
				r.logger.Debug(ErrLeadershipTransferInProgress.Error())
				future.respond(ErrLeadershipTransferInProgress)
				continue
			}
			future.configurations = r.configurations.Clone()
			future.respond(nil)

		case future := <-r.configurationChangeChIfStable():
			if r.getLeadershipTransferInProgress() {
				r.logger.Debug(ErrLeadershipTransferInProgress.Error())
				future.respond(ErrLeadershipTransferInProgress)
				continue
			}
			r.appendConfigurationEntry(future)

		case b := <-r.bootstrapCh:
			b.respond(ErrCantBootstrap)

		case newLog := <-r.applyCh:
			if r.getLeadershipTransferInProgress() { // 判断是不是在转移leader
				r.logger.Debug(ErrLeadershipTransferInProgress.Error())
				newLog.respond(ErrLeadershipTransferInProgress)
				continue
			}
			// 集体提交，收集所有准备好的提交
			ready := []*logFuture{newLog}
		GROUP_COMMIT_LOOP:
			for i := 0; i < r.config().MaxAppendEntries; i++ {
				select {
				case newLog := <-r.applyCh:
					ready = append(ready, newLog)
				default:
					break GROUP_COMMIT_LOOP
				}
			}

			// 发送日志
			if stepDown {
				// 我们正在卸任领导职务，不要处理任何新的数据。
				for i := range ready {
					ready[i].respond(ErrNotLeader)
				}
			} else {
				r.dispatchLogs(ready)
			}

		case <-lease:
			// Check if we've exceeded the lease, potentially stepping down
			maxDiff := r.checkLeaderLease()

			// Next check interval should adjust for the last node we've
			// contacted, without going negative
			checkInterval := r.config().LeaderLeaseTimeout - maxDiff
			if checkInterval < minCheckInterval {
				checkInterval = minCheckInterval
			}

			// Renew the lease timer
			lease = time.After(checkInterval)

		case <-r.shutdownCh:
			return
		}
	}
}

// verifyLeader must be called from the main thread for safety.
// Causes the followers to attempt an immediate heartbeat.
func (r *Raft) verifyLeader(v *verifyFuture) {
	// Current leader always votes for self
	v.votes = 1

	// Set the quorum size, hot-path for single node
	v.quorumSize = r.quorumSize()
	if v.quorumSize == 1 {
		v.respond(nil)
		return
	}

	// Track this request
	v.notifyCh = r.verifyCh
	r.leaderState.notify[v] = struct{}{}

	// Trigger immediate heartbeats
	for _, repl := range r.leaderState.replState {
		repl.notifyLock.Lock()
		repl.notify[v] = struct{}{}
		repl.notifyLock.Unlock()
		asyncNotifyCh(repl.notifyCh)
	}
}

// leadershipTransfer is doing the heavy lifting for the leadership transfer.
func (r *Raft) leadershipTransfer(id ServerID, address ServerAddress, repl *followerReplication, stopCh chan struct{}, doneCh chan error) {

	// make sure we are not already stopped
	select {
	case <-stopCh:
		doneCh <- nil
		return
	default:
	}

	// Step 1: set this field which stops this leader from responding to any client requests.
	r.setLeadershipTransferInProgress(true)
	defer func() { r.setLeadershipTransferInProgress(false) }()

	for atomic.LoadUint64(&repl.nextIndex) <= r.getLastIndex() {
		err := &deferError{}
		err.init()
		repl.triggerDeferErrorCh <- err
		select {
		case err := <-err.errCh:
			if err != nil {
				doneCh <- err
				return
			}
		case <-stopCh:
			doneCh <- nil
			return
		}
	}

	// Step ?: the thesis describes in chap 6.4.1: Using clocks to reduce
	// messaging for read-only queries. If this is implemented, the lease
	// has to be reset as well, in case leadership is transferred. This
	// implementation also has a lease, but it serves another purpose and
	// doesn't need to be reset. The lease mechanism in our raft lib, is
	// setup in a similar way to the one in the thesis, but in practice
	// it's a timer that just tells the leader how often to check
	// heartbeats are still coming in.

	// Step 3: send TimeoutNow message to target server.
	err := r.trans.TimeoutNow(id, address, &TimeoutNowRequest{RPCHeader: r.getRPCHeader()}, &TimeoutNowResponse{})
	if err != nil {
		err = fmt.Errorf("failed to make TimeoutNow RPC to %v: %v", id, err)
	}
	doneCh <- err
}

// checkLeaderLease is used to check if we can contact a quorum of nodes
// within the last leader lease interval. If not, we need to step down,
// as we may have lost connectivity. Returns the maximum duration without
// contact. This must only be called from the main thread.
func (r *Raft) checkLeaderLease() time.Duration {
	// Track contacted nodes, we can always contact ourself
	contacted := 0

	// Store lease timeout for this one check invocation as we need to refer to it
	// in the loop and would be confusing if it ever becomes reloadable and
	// changes between iterations below.
	leaseTimeout := r.config().LeaderLeaseTimeout

	// Check each follower
	var maxDiff time.Duration
	now := time.Now()
	for _, server := range r.configurations.latest.Servers {
		if server.Suffrage == Voter {
			if server.ID == r.localID {
				contacted++
				continue
			}
			f := r.leaderState.replState[server.ID]
			diff := now.Sub(f.LastContact())
			if diff <= leaseTimeout {
				contacted++
				if diff > maxDiff {
					maxDiff = diff
				}
			} else {
				// Log at least once at high value, then debug. Otherwise it gets very verbose.
				if diff <= 3*leaseTimeout {
					r.logger.Warn("failed to contact", "server-id", server.ID, "time", diff)
				} else {
					r.logger.Debug("failed to contact", "server-id", server.ID, "time", diff)
				}
			}
		}
	}

	// Verify we can contact a quorum
	quorum := r.quorumSize()
	if contacted < quorum {
		r.logger.Warn("failed to contact quorum of nodes, stepping down")
		r.setState(Follower)
	}
	return maxDiff
}

// quorumSize is used to return the quorum size. This must only be called on
// the main thread.
// TODO: revisit usage
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

// dispatchLog在领导层被调用，以推送日志到磁盘。 标记它 并开始对其进行复制。不会并发调用
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

	// 更新最后的日志，因为它现在已经在磁盘上了   ,并没有commit
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
func (r *Raft) processLogs(index uint64, futures map[uint64]*logFuture) {
	// Reject logs we've applied already
	lastApplied := r.getLastApplied()
	if index <= lastApplied {
		r.logger.Warn("skipping application of old log", "index", index)
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

// processRPC 处理rpc请求
func (r *Raft) processRPC(rpc RPC) {
	// 版本检查
	if err := r.checkRPCHeader(rpc); err != nil {
		rpc.Respond(nil, err)
		return
	}

	switch cmd := rpc.Command.(type) {
	case *AppendEntriesRequest:
		r.appendEntries(rpc, cmd)
	case *RequestVoteRequest:
		r.requestVote(rpc, cmd)
	case *InstallSnapshotRequest:
		r.installSnapshot(rpc, cmd)
	case *TimeoutNowRequest:
		r.timeoutNow(rpc, cmd)
	default:
		r.logger.Error("异常的命令类型", "command", hclog.Fmt("%#v", rpc.Command))
		rpc.Respond(nil, fmt.Errorf("unexpected command"))
	}
}

// processHeartbeat 是一个专门用于心跳请求的特殊处理程序，以便在传输支持的情况下可以快速处理它们。它只能从主线程中调用。
func (r *Raft) processHeartbeat(rpc RPC) {

	select {
	case <-r.shutdownCh:
		return
	default:
	}

	// 确保我们只处理心跳的问题
	switch cmd := rpc.Command.(type) {
	case *AppendEntriesRequest:
		r.appendEntries(rpc, cmd)
	default:
		r.logger.Error("预期的心跳, got", "command", hclog.Fmt("%#v", rpc.Command))
		rpc.Respond(nil, fmt.Errorf("unexpected command"))
	}
}

// appendEntries 当我们得到一个追加条目的RPC调用时被调用。这必须只在主线程中调用。
func (r *Raft) appendEntries(rpc RPC, a *AppendEntriesRequest) {
	// Setup a response
	resp := &AppendEntriesResponse{
		RPCHeader:      r.getRPCHeader(),
		Term:           r.getCurrentTerm(),
		LastLog:        r.getLastIndex(),
		Success:        false,
		NoRetryBackoff: false,
	}
	var rpcErr error
	defer func() {
		rpc.Respond(resp, rpcErr)
	}()

	// Ignore an older term
	if a.Term < r.getCurrentTerm() {
		return
	}

	// Increase the term if we see a newer one, also transition to follower
	// if we ever get an appendEntries call
	if a.Term > r.getCurrentTerm() || r.getState() != Follower {
		// Ensure transition to follower
		r.setState(Follower)
		r.setCurrentTerm(a.Term)
		resp.Term = a.Term
	}

	// Save the current leader
	r.setLeader(r.trans.DecodePeer(a.Leader))

	// Verify the last log entry
	if a.PrevLogEntry > 0 {
		lastIdx, lastTerm := r.getLastEntry()

		var prevLogTerm uint64
		if a.PrevLogEntry == lastIdx {
			prevLogTerm = lastTerm

		} else {
			var prevLog Log
			if err := r.logs.GetLog(a.PrevLogEntry, &prevLog); err != nil {
				r.logger.Warn("failed to get previous log",
					"previous-index", a.PrevLogEntry,
					"last-index", lastIdx,
					"error", err)
				resp.NoRetryBackoff = true
				return
			}
			prevLogTerm = prevLog.Term
		}

		if a.PrevLogTerm != prevLogTerm {
			r.logger.Warn("previous log term mis-match",
				"ours", prevLogTerm,
				"remote", a.PrevLogTerm)
			resp.NoRetryBackoff = true
			return
		}
	}

	// Process any new entries
	if len(a.Entries) > 0 {

		// Delete any conflicting entries, skip any duplicates
		lastLogIdx, _ := r.getLastLog()
		var newEntries []*Log
		for i, entry := range a.Entries {
			if entry.Index > lastLogIdx {
				newEntries = a.Entries[i:]
				break
			}
			var storeEntry Log
			if err := r.logs.GetLog(entry.Index, &storeEntry); err != nil {
				r.logger.Warn("failed to get log entry",
					"index", entry.Index,
					"error", err)
				return
			}
			if entry.Term != storeEntry.Term {
				r.logger.Warn("clearing log suffix",
					"from", entry.Index,
					"to", lastLogIdx)
				if err := r.logs.DeleteRange(entry.Index, lastLogIdx); err != nil {
					r.logger.Error("failed to clear log suffix", "error", err)
					return
				}
				if entry.Index <= r.configurations.latestIndex {
					r.setLatestConfiguration(r.configurations.committed, r.configurations.committedIndex)
				}
				newEntries = a.Entries[i:]
				break
			}
		}

		if n := len(newEntries); n > 0 {
			// Append the new entries
			if err := r.logs.StoreLogs(newEntries); err != nil {
				r.logger.Error("failed to append to logs", "error", err)
				// TODO: leaving r.getLastLog() in the wrong
				// state if there was a truncation above
				return
			}

			// Handle any new configuration changes
			for _, newEntry := range newEntries {
				if err := r.processConfigurationLogEntry(newEntry); err != nil {
					r.logger.Warn("failed to append entry",
						"index", newEntry.Index,
						"error", err)
					rpcErr = err
					return
				}
			}

			// Update the lastLog
			last := newEntries[n-1]
			r.setLastLog(last.Index, last.Term)
		}

	}

	// Update the commit index
	if a.LeaderCommitIndex > 0 && a.LeaderCommitIndex > r.getCommitIndex() {
		idx := min(a.LeaderCommitIndex, r.getLastIndex())
		r.setCommitIndex(idx)
		if r.configurations.latestIndex <= idx {
			r.setCommittedConfiguration(r.configurations.latest, r.configurations.latestIndex)
		}
		r.processLogs(idx, nil)
	}

	// Everything went well, set success
	resp.Success = true
	r.setLastContact()
	return
}

// processConfigurationLogEntry
// 从logState中获取快照中没有的数据,然后对每一个log 调用此函数
func (r *Raft) processConfigurationLogEntry(entry *Log) error {
	fmt.Printf("======>  %+v\n", *entry)
	switch entry.Type {
	case LogConfiguration: //
		r.setCommittedConfiguration(r.configurations.latest, r.configurations.latestIndex)
		r.setLatestConfiguration(DecodeConfiguration(entry.Data), entry.Index)
		//r.configurations.committed = r.configurations.latest
		//r.configurations.committedIndex = r.configurations.latestIndex
		//r.configurations.latest = DecodeConfiguration(entry.Data)
		//r.configurations.latestIndex = entry.Index
		//r.latestConfiguration.Store(DecodeConfiguration(entry.Data).Clone())

	case LogAddPeerDeprecated, LogRemovePeerDeprecated:
		r.setCommittedConfiguration(r.configurations.latest, r.configurations.latestIndex)
		conf, err := decodePeers(entry.Data, r.trans)
		if err != nil {
			return err
		}
		r.setLatestConfiguration(conf, entry.Index)
	}
	return nil
}

// requestVote 当接收到远程的rpc 投票请求 会调用此函数
func (r *Raft) requestVote(rpc RPC, req *RequestVoteRequest) {
	r.observe(*req)

	// 构建响应
	resp := &RequestVoteResponse{
		RPCHeader: r.getRPCHeader(),
		Term:      r.getCurrentTerm(), // 当前的任期
		Granted:   false,              // 不投票
	}
	var rpcErr error
	defer func() {
		rpc.Respond(resp, rpcErr)
	}()

	// Version 0 servers will panic unless the peers is present. It's only  used on them to produce a warning message.
	// 版本0，服务器会panic，除非节点存在。它只用在他们身上，以产生一个警告信息。
	if r.protocolVersion < 2 {
		// 请求协议版本为0，1
		// TODO 现在协议版本都设置为了3 ，不会走到这里
		resp.Peers = encodePeers(r.configurations.latest, r.trans)
	}

	candidate := r.trans.DecodePeer(req.Candidate) // 候选者地址
	// 不是当前节点的leader，且没有发生领导者转移
	if leader := r.Leader(); leader != "" && leader != candidate && !req.LeadershipTransfer {
		r.logger.Warn("拒绝投票请求，因为我们有一个领导者", "from", candidate, "leader", leader)
		return
	}

	// 小于当前任期
	if req.Term < r.getCurrentTerm() {
		return
	}

	// 大于当前任期
	if req.Term > r.getCurrentTerm() {
		// 确保过渡到追随者
		r.logger.Debug("失去了领导权  因为收到了具有新任期的requestVote请求")
		r.setState(Follower)
		r.setCurrentTerm(req.Term)
		resp.Term = req.Term
	}

	// 检查我们是否已经为自己投票  r.persistVote(req.Term, req.Candidate)
	lastVoteTerm, err := r.stable.GetUint64(keyLastVoteTerm) // 最新的竞选任期
	if err != nil && err != ErrKeyNotFound {
		r.logger.Error("获取当前任期失败", "error", err)
		return
	}
	lastVoteCandBytes, err := r.stable.Get(keyLastVoteCand) // 本机地址
	if err != nil && err != ErrKeyNotFound {
		r.logger.Error("获取最新的候选任期失败", "error", err)
		return
	}

	// 检查我们是否曾经在这次选举中投票
	if lastVoteTerm == req.Term && lastVoteCandBytes != nil {
		r.logger.Info("对同一任期的重复请求投票", "term", req.Term)
		// 如果已经任期相同，但不是之前存储的竞选者ID，不投票
		// 1、集群之初，给A投票了，B请求来了,就不给B投票了   									不投票   ✅
		// 2、集群重启，任期相同,lastVoteCandBytes 之前的leader== 现在的竞选者 ，				投票     ✅
		// 3、集群重启，任期相同，请求来源不是之前的leader，需要判断 来源的任期、日志索引数
		//	      请求任期 < 当期  														不投票   ✅
		//	      请求任期 > 当期  														投票     ✅
		//	      请求任期 = 当期  && 请求的日志索引 <  当前日志索引  						不投票   ✅
		//	      请求任期 = 当期  && 请求的日志索引 >= 当前日志索引  						投票     ✅
		//
		if bytes.Compare(lastVoteCandBytes, req.Candidate) == 0 {
			// 同一任期、同一来源
			r.logger.Warn("重复的候选者地址", "candidate", candidate)
			resp.Granted = true
		}
		return
	}

	// 如果请求的任期小于当前的任期，则拒绝
	lastIdx, lastTerm := r.getLastEntry()
	if lastTerm > req.LastLogTerm {
		r.logger.Warn("拒绝投票请求，因为我们的本机任期更大",
			"candidate", candidate, "本机任期", lastTerm, "投票申请的任期", req.LastLogTerm)
		return
	}

	if lastTerm == req.LastLogTerm && lastIdx > req.LastLogIndex {
		r.logger.Warn("拒绝投票请求，因为我们的本机索引更大",
			"candidate", candidate, "本机日志索引", lastIdx, "投票申请的日志索引", req.LastLogIndex)
		return
	}

	// 安全存储任期, 如果是集群运行之初,都相等,这里就会设置第一个到达的任期、竞选者ID
	if err := r.persistVote(req.Term, req.Candidate); err != nil {
		r.logger.Error("存储任期失败", "error", err)
		return
	}
	// 如果不断的都可以走到这，那就疯了
	// 这里没有进行限制，都投票了  例如term、index 都一样的情况
	resp.Granted = true
	r.setLastContact()
	return
}

// installSnapshot is invoked when we get a InstallSnapshot RPC call.
// We must be in the follower state for this, since it means we are
// too far behind a leader for log replay. This must only be called
// from the main thread.
func (r *Raft) installSnapshot(rpc RPC, req *InstallSnapshotRequest) {
	// Setup a response
	resp := &InstallSnapshotResponse{
		Term:    r.getCurrentTerm(),
		Success: false,
	}
	var rpcErr error
	defer func() {
		io.Copy(ioutil.Discard, rpc.Reader) // ensure we always consume all the snapshot data from the stream [see issue #212]
		rpc.Respond(resp, rpcErr)
	}()

	// Sanity check the version
	if req.SnapshotVersion < SnapshotVersionMin ||
		req.SnapshotVersion > SnapshotVersionMax {
		rpcErr = fmt.Errorf("unsupported snapshot version %d", req.SnapshotVersion)
		return
	}

	// Ignore an older term
	if req.Term < r.getCurrentTerm() {
		r.logger.Info("ignoring installSnapshot request with older term than current term",
			"request-term", req.Term,
			"current-term", r.getCurrentTerm())
		return
	}

	// Increase the term if we see a newer one
	if req.Term > r.getCurrentTerm() {
		// Ensure transition to follower
		r.setState(Follower)
		r.setCurrentTerm(req.Term)
		resp.Term = req.Term
	}

	// Save the current leader
	r.setLeader(r.trans.DecodePeer(req.Leader))

	// Create a new snapshot
	var reqConfiguration Configuration
	var reqConfigurationIndex uint64
	if req.SnapshotVersion > 0 {
		reqConfiguration = DecodeConfiguration(req.Configuration)
		reqConfigurationIndex = req.ConfigurationIndex
	} else {
		reqConfiguration, rpcErr = decodePeers(req.Peers, r.trans)
		if rpcErr != nil {
			r.logger.Error("failed to install snapshot", "error", rpcErr)
			return
		}
		reqConfigurationIndex = req.LastLogIndex
	}
	version := getSnapshotVersion(r.protocolVersion)
	sink, err := r.snapshots.Create(version, req.LastLogIndex, req.LastLogTerm,
		reqConfiguration, reqConfigurationIndex, r.trans)
	if err != nil {
		r.logger.Error("failed to create snapshot to install", "error", err)
		rpcErr = fmt.Errorf("failed to create snapshot: %v", err)
		return
	}

	// Spill the remote snapshot to disk
	n, err := io.Copy(sink, rpc.Reader)
	if err != nil {
		sink.Cancel()
		r.logger.Error("failed to copy snapshot", "error", err)
		rpcErr = err
		return
	}

	// Check that we received it all
	if n != req.Size {
		sink.Cancel()
		r.logger.Error("failed to receive whole snapshot",
			"received", hclog.Fmt("%d / %d", n, req.Size))
		rpcErr = fmt.Errorf("short read")
		return
	}

	// Finalize the snapshot
	if err := sink.Close(); err != nil {
		r.logger.Error("failed to finalize snapshot", "error", err)
		rpcErr = err
		return
	}
	r.logger.Info("copied to local snapshot", "bytes", n)

	// Restore snapshot
	future := &restoreFuture{ID: sink.ID()}
	future.ShutdownCh = r.shutdownCh
	future.init()
	select {
	case r.fsmMutateCh <- future:
	case <-r.shutdownCh:
		future.respond(ErrRaftShutdown)
		return
	}

	// Wait for the restore to happen
	if err := future.Error(); err != nil {
		r.logger.Error("failed to restore snapshot", "error", err)
		rpcErr = err
		return
	}

	// Update the lastApplied so we don't replay old logs
	r.setLastApplied(req.LastLogIndex)

	// Update the last stable snapshot info
	r.setLastSnapshot(req.LastLogIndex, req.LastLogTerm)

	// Restore the peer set
	r.setLatestConfiguration(reqConfiguration, reqConfigurationIndex)
	r.setCommittedConfiguration(reqConfiguration, reqConfigurationIndex)

	// Compact logs, continue even if this fails
	if err := r.compactLogs(req.LastLogIndex); err != nil {
		r.logger.Error("failed to compact logs", "error", err)
	}

	r.logger.Info("Installed remote snapshot")
	resp.Success = true
	r.setLastContact()
	return
}

// setLastContact 设置与 leader 心跳通信的时间
func (r *Raft) setLastContact() {
	r.lastContactLock.Lock()
	r.lastContact = time.Now()
	r.lastContactLock.Unlock()
}

type voteResult struct {
	RequestVoteResponse
	voterID ServerID // 选民的逻辑ID
}

// electSelf 是用来向所有同伴发送一个RequestVote RPC，并为我们自己投票。
// 这有一个副作用，就是增加了当前的任期。返回的响应通道被用来等待所有的响应（包括对我们自己的投票）。这必须只在主线程中调用。
func (r *Raft) electSelf() <-chan *voteResult {
	r.logger.Info("进入选举状态", "node", r, "term", r.getCurrentTerm()+1)
	// 构建响应channel      ,最多会有 这么多投票
	respCh := make(chan *voteResult, len(r.configurations.latest.Servers))

	// 设置任期
	r.setCurrentTerm(r.getCurrentTerm() + 1)

	// 构建请求
	lastIdx, lastTerm := r.getLastEntry()
	req := &RequestVoteRequest{
		RPCHeader:          r.getRPCHeader(),   // 只有协议版本
		Term:               r.getCurrentTerm(), // 当前任期
		Candidate:          r.trans.EncodePeer(r.localID, r.localAddr),
		LastLogIndex:       lastIdx,
		LastLogTerm:        lastTerm,
		LeadershipTransfer: r.candidateFromLeadershipTransfer, // 在这为false
	}

	// 构建一个请求投票的函数
	askPeer := func(peer Server) {
		r.goFunc(func() {
			resp := &voteResult{voterID: peer.ID}
			// 发送请求并将结果写入到&resp.RequestVoteResponse
			err := r.trans.RequestVote(peer.ID, peer.Address, req, &resp.RequestVoteResponse)
			if err != nil {
				r.logger.Error("调用请求投票 RPC失败", "target", peer, "error", err)
				resp.Term = req.Term
				resp.Granted = false
			}
			//	 raft/raft.go:1411
			respCh <- resp // 可以获取到远端节点的任期
		})
	}

	// 对每一个节点，进行投票请求
	// TODO 最初 r.configurations.latest 是从快照里读取到的？ 那快照的数据  是怎么写进去的
	for _, server := range r.configurations.latest.Servers {
		if server.Suffrage == Voter {
			if server.ID == r.localID {
				// 自己
				// 坚持为自己投票,写db   任期、候选者
				if err := r.persistVote(req.Term, req.Candidate); err != nil {
					r.logger.Error("为自己投票失败", "error", err)
					return nil
				}
				// 包括我们自己的投票
				respCh <- &voteResult{
					RequestVoteResponse: RequestVoteResponse{
						RPCHeader: r.getRPCHeader(),
						Term:      req.Term,
						Granted:   true,
					},
					voterID: r.localID,
				}
			} else {
				askPeer(server)
			}
		}
	}

	return respCh
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

// initiateLeadershipTransfer starts the leadership on the leader side, by
// sending a message to the leadershipTransferCh, to make sure it runs in the
// mainloop.
func (r *Raft) initiateLeadershipTransfer(id *ServerID, address *ServerAddress) LeadershipTransferFuture {
	future := &leadershipTransferFuture{ID: id, Address: address}
	future.init()

	if id != nil && *id == r.localID {
		err := fmt.Errorf("cannot transfer leadership to itself")
		r.logger.Info(err.Error())
		future.respond(err)
		return future
	}

	select {
	case r.leadershipTransferCh <- future:
		return future
	case <-r.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	default:
		return errorFuture{ErrEnqueueTimeout}
	}
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
