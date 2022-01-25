package raft

import "time"

// runFollower 为一个follower运行FSM。
func (r *Raft) runFollower() {
	didWarn := false
	r.logger.Info("进入 follower state", "follower", r, "leader", r.Leader())
	heartbeatTimer := randomTimeout(r.config().HeartbeatTimeout) // 定时器

	for r.getState() == Follower {
		select {
		case rpc := <-r.rpcCh: //  ✅
			r.processRPC(rpc)
		//-----------------------------------
		// 拒绝任何操作，当不是leader
		//case c := <-r.configurationChangeCh:
		//	c.respond(ErrNotLeader)
		//case a := <-r.applyCh:
		//	a.respond(ErrNotLeader)
		//case v := <-r.verifyCh:
		//	v.respond(ErrNotLeader)
		//case r := <-r.userRestoreCh:
		//	r.respond(ErrNotLeader)
		//case r := <-r.leadershipTransferCh:
		//	r.respond(ErrNotLeader)
		//-----------------------------------
		case c := <-r.configurationsCh: // over
			// 配置获取请求，读取当前配置、然后设置
			c.configurations = r.configurations.Clone()
			c.respond(nil)
		case b := <-r.bootstrapCh: // over ✅
			_ = r.BootstrapCluster // 由它触发
			b.respond(r.liveBootstrap(b.configuration))

		case <-heartbeatTimer: // over
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

			if r.configurations.latestIndex == 0 { // 默认0 ,引导以后会变成1
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
				// 自己是一个选民
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
