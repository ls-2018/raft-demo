package raft

import (
	"container/list"
	"fmt"
	"sync/atomic"
	"time"
)

// runLeader 在这里设置让leader运行FSM，并将其放入leaderLoop
func (r *Raft) runLeader() {
	r.logger.Info("进入 leader state", "leader", r)

	// 通知所有人，只是将true 放入了leaderCh
	overrideNotifyBool(r.leaderCh, true)

	// 存储通知通道。它是不可重载的，所以在下面的defer运行之前不应该改变，但这可以确保我们在获得和失去领导权的情况下总是通知同一个chan。
	notify := r.config().NotifyCh // nil 因为没有赋值的地方

	// 如果给定的话，推送到通知通道
	if notify != nil {
		select {
		case notify <- true:
		case <-r.shutdownCh:
		}
	}

	// 设置领导状态。这应该只在leaderLoop中被访问。
	r.setupLeaderState()

	// 运行一个后台go-routine来排放日志年龄的指标
	stopCh := make(chan struct{})

	// todo 降级时的清理状态
	defer func() {
		close(stopCh)

		// 由于我们之前是领导，所以当我们不是时，我们会更新我们的最后联系时间，
		// 这样我们就不会报告我们是领导之前的最后联系时间了。否则，对客户来说，我们的数据会显得非常陈旧。
		r.setLastContact()

		// 停止复制
		for _, p := range r.leaderState.replState {
			close(p.stopCh)
		}

		// 响应所有飞行行动
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

	// 对每一个节点启动一个复制 go routine  日志复制、心跳检测
	r.startStopReplication()
	// 先分发一个无操作的日志条目。即使在没有客户端命令的情况下，这也会使leader达到最新的可能提交索引。
	// 这用于追加配置项而不是noop。
	// 但是，这允许日志中有无数个未提交的配置。现在，我们认为在任何日志中最多只存在一个未提交的配置条目，因此我们必须在这里进行适当的无操作。
	noop := &logFuture{
		log: Log{
			Type: LogNoop,
		},
	}
	r.dispatchLogs([]*logFuture{noop})
	r.leaderLoop()
}

// leaderLoop 它在所有不同的leader设置完成后被调用。
func (r *Raft) leaderLoop() {
	// stepDown用来追踪是否有飞行日志会导致我们失去领导能力(特别是我们自己的一个RemovePeer)。
	// 如果是这种情况，我们不能允许任何日志并行处理，否则我们将只基于单个follower(我们自己)提交，并复制到一组未定义的对等体。
	stepDown := false
	// 这只用于第一次租约检查，我们在下面根据当前配置值重新加载租约。
	lease := time.After(r.config().LeaderLeaseTimeout)

	for r.getState() == Leader {
		select {
		case rpc := <-r.rpcCh: // over
			r.processRPC(rpc)
		case <-r.leaderState.stepDown:
			r.setState(Follower)
		case future := <-r.leadershipTransferCh: // runLeader 监听leader转移
			if r.getLeadershipTransferInProgress() {
				// 如果已经在转移中了
				r.logger.Debug(ErrLeadershipTransferInProgress.Error())
				future.respond(ErrLeadershipTransferInProgress)
				continue
			}
			r.logger.Debug("leader开始转移到", "id", future.ID, "address", future.Address)
			// 当离开 leaderLoop,不再是leader, 需要关闭leftLeaderLoop
			leftLeaderLoop := make(chan struct{})
			defer func() { close(leftLeaderLoop) }()
			stopCh := make(chan struct{})
			doneCh := make(chan error, 1)

			// 这是在领导权转移函数之外故意设置的。
			// 因为TimeoutNow调用是阻塞的，而且没有办法在定时器过期的情况下中止它。
			// leadershipTransfer函数是由stopCh和doneCh控制的。
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
				// 刚发送，开始核查
				r.verifyLeader(v)

			} else if v.votes < v.quorumSize {
				// 提早回来，意味着必须有一个新的领袖
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
		case b := <-r.bootstrapCh: // over 🈲
			b.respond(ErrCantBootstrap)
		case newLog := <-r.applyCh: // over
			if r.getLeadershipTransferInProgress() { // 判断是不是在转移leader
				r.logger.Debug(ErrLeadershipTransferInProgress.Error())
				newLog.respond(ErrLeadershipTransferInProgress)
				continue
			}
			// 集体提交，收集所有准备好的提交
			ready := []*logFuture{newLog}
		GroupCommitLoop:
			for i := 0; i < r.config().MaxAppendEntries; i++ {
				select {
				case newLog := <-r.applyCh:
					ready = append(ready, newLog)
				default:
					break GroupCommitLoop
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
		case <-lease: // over

			// 看看我们是否超过了租约，可能会辞职
			// 调整下一次检查各节点通信节点的时间
			maxDiff := r.checkLeaderLease()

			// 下次检查的间隔应该调整为我们联系的最后一个节点，而不是负数
			checkInterval := r.config().LeaderLeaseTimeout - maxDiff
			if checkInterval < minCheckInterval {
				checkInterval = minCheckInterval
			}
			lease = time.After(checkInterval)
		case <-r.shutdownCh:
			return
		}
	}
}

// verifyLeader
func (r *Raft) verifyLeader(v *verifyFuture) {
	// 现任领导人总是为自己投票
	v.votes = 1

	// 设置获胜需要的票数
	v.quorumSize = r.quorumSize()
	if v.quorumSize == 1 {
		v.respond(nil)
		return
	}

	// 追踪请求
	v.notifyCh = r.verifyCh
	r.leaderState.notify[v] = struct{}{}

	// 立即引发心跳
	for _, repl := range r.leaderState.replState {
		repl.notifyLock.Lock()
		repl.notify[v] = struct{}{}
		repl.notifyLock.Unlock()
		asyncNotifyCh(repl.notifyCh)
	}
}

func (r *Raft) leadershipTransfer(id ServerID, address ServerAddress, repl *followerReplication, stopCh chan struct{}, doneCh chan error) {

	// 确保我们没有被阻止
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

func (r *Raft) setupLeaderState() {
	r.leaderState.commitCh = make(chan struct{}, 1)
	r.leaderState.commitment = newCommitment(r.leaderState.commitCh,
		r.configurations.latest,
		r.getLastIndex()+1, // 这个任期内，最早提交的index
	)
	r.leaderState.inflight = list.New()
	r.leaderState.replState = make(map[ServerID]*followerReplication)
	r.leaderState.notify = make(map[*verifyFuture]struct{})
	r.leaderState.stepDown = make(chan struct{}, 1)
}

// ------------------------------------ over ------------------------------------

// setLeader 是用来修改集群的当前领导者的
func (r *Raft) setLeader(leader ServerAddress) {
	r.leaderLock.Lock()
	r.leader = leader
	r.leaderLock.Unlock()
}

// initiateLeadershipTransfer  初始化leader转移到 id 的事件，等待未来返回结果
func (r *Raft) initiateLeadershipTransfer(id *ServerID, address *ServerAddress) LeadershipTransferFuture {
	future := &leadershipTransferFuture{ID: id, Address: address}
	future.init()

	if id != nil && *id == r.localID {
		err := fmt.Errorf("不能把leader转移到自己身上")
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

// OK
func (r *Raft) setLeadershipTransferInProgress(v bool) {
	if v {
		atomic.StoreInt32(&r.leaderState.leadershipTransferInProgress, 1)
	} else {
		atomic.StoreInt32(&r.leaderState.leadershipTransferInProgress, 0)
	}
}

// OK
func (r *Raft) getLeadershipTransferInProgress() bool {
	v := atomic.LoadInt32(&r.leaderState.leadershipTransferInProgress)
	return v == 1
}

// checkLeaderLease 检查是否仍与大多数通信在规定时间内;返回没有通信的最长时间。
func (r *Raft) checkLeaderLease() time.Duration {
	// 跟踪可以通信的节点，包括自己
	contacted := 0

	// 存储这个检查调用的租约超时，因为我们需要在循环中引用它，如果它变成可更改的，并在下面的循环之间更改，将会令人困惑。
	leaseTimeout := r.config().LeaderLeaseTimeout // 500ms

	var maxDiff time.Duration // 通信间隔最大的时间
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
				r.logger.Warn("failed to contact", "server-id", server.ID, "time", diff)
			}
		}
	}

	quorum := r.quorumSize()
	if contacted < quorum {
		r.logger.Warn("无法与大多数节点通信，设置为Follower")
		r.setState(Follower)
	}
	return maxDiff
}
