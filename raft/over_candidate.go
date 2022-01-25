package raft

type voteResult struct {
	RequestVoteResponse
	voterID ServerID // 选民的逻辑ID
}

// runCandidate 为一个candidate运行FSM
func (r *Raft) runCandidate() {

	// 首先投自己一票，并设置选举超时
	voteCh := r.electSelf() // voteCh 投票响应channel

	// 确保每次运行后领导权转移flag被重置。
	// 有这个flag, 将RequestVoteRequst中的LeadershipTransfer字段设置为 "true"，这将使其他服务器投票
	// 即使他们已经有一个领导者。
	// 重置这个标志很重要，因为这个特权可能会被滥用。
	defer func() { r.candidateFromLeadershipTransfer = false }() // 执行 defer前 candidateFromLeadershipTransfer=FALSE

	electionTimer := randomTimeout(r.config().ElectionTimeout) // 选举超时

	// 统计票数，需要简单多数
	grantedVotes := 0
	votesNeeded := r.quorumSize() // 需要有多少投票才会变成leader
	r.logger.Debug("竞选", "needed", votesNeeded)

	for r.getState() == Candidate {
		select {
		case rpc := <-r.rpcCh: // runCandidate
			r.processRPC(rpc)

		case vote := <-voteCh: // runCandidate
			// 检查目标主机是否大于当前任期
			if vote.Term > r.getCurrentTerm() {
				r.logger.Debug("发现新的任期、退化为follower")
				r.setState(Follower)
				r.setCurrentTerm(vote.Term)
				return
			}

			// 检查是否投票
			if vote.Granted {
				grantedVotes++
				r.logger.Debug("获得投票", "from", vote.voterID, "term", vote.Term, "tally", grantedVotes)
			}

			// 检查是否可以成为leader
			if grantedVotes >= votesNeeded {
				r.logger.Info("竞选获胜", "选票数", grantedVotes)
				r.setState(Leader)
				r.setLeader(r.localAddr)
				return
			}
		//-----------------------------------
		// 拒绝任何操作，当不是leader
		case c := <-r.configurationChangeCh: // runCandidate🈲
			c.respond(ErrNotLeader)
		case a := <-r.applyCh: // runCandidate🈲
			a.respond(ErrNotLeader)
		case v := <-r.verifyCh: // runCandidate🈲
			v.respond(ErrNotLeader)
		case r := <-r.userRestoreCh: // runCandidate 🈲
			r.respond(ErrNotLeader)
		case r := <-r.leadershipTransferCh: // runCandidate🈲
			r.respond(ErrNotLeader)
		//-----------------------------------
		// 拒绝任何操作，当不是follower
		case b := <-r.bootstrapCh: // runCandidate 🈲
			b.respond(ErrCantBootstrap)

		case c := <-r.configurationsCh: // runCandidate
			c.configurations = r.configurations.Clone()
			c.respond(nil)

		case <-electionTimer: // runCandidate
			r.logger.Warn("选举已超时，重新开始选举")
			return

		case <-r.shutdownCh:
			return
		}
	}
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
		RPCHeader:          r.getRPCHeader(),                           // 只有协议版本
		Term:               r.getCurrentTerm(),                         // 当前任期
		Candidate:          r.trans.EncodePeer(r.localID, r.localAddr), // localAddr
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
				// 坚持为自己投票,写db 任期、候选者
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
