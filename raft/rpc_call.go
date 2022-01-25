package raft

import (
	"bytes"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"io"
	"io/ioutil"
)

// processRPC 处理rpc请求 日志追加、心跳、投票、快照、超时请求
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

// appendEntries 当我们得到一个追加条目的RPC调用时被调用。心跳也是走的这。
func (r *Raft) appendEntries(rpc RPC, a *AppendEntriesRequest) {
	// 设置响应
	resp := &AppendEntriesResponse{
		RPCHeader:      r.getRPCHeader(),
		Term:           r.getCurrentTerm(),
		LastLog:        r.getLastIndex(), // follower节点最新的日志索引
		Success:        false,
		NoRetryBackoff: false,
	}
	var rpcErr error
	defer func() {
		rpc.Respond(resp, rpcErr)
	}()

	// 不会接受小于自己任期的数据追加
	if a.Term < r.getCurrentTerm() {
		return
	}

	// 请求的任期 大于当前任期 || 当前的状态不是Follower
	if a.Term > r.getCurrentTerm() || r.getState() != Follower {
		r.setState(Follower)
		r.setCurrentTerm(a.Term)
		resp.Term = a.Term
	}
	// a:leader b:leader
	// a heartbeat -> b ; 因为b是leader了;设置b为follower
	// 如果 在这之间 b heartbeat -> a ;因为a是leader了;设置a为follower
	// 重新进入选举流程

	// 设置当前节点的leader
	r.setLeader(r.trans.DecodePeer(a.Leader))

	// 验证最新的日志  同步过来的一批的日志的第一个日志索引
	if a.PrevLogEntry > 0 {
		lastIdx, lastTerm := r.getLastEntry() // 获取follower最新的日志索引、任期

		var prevLogTerm uint64 // follower 的任期
		if a.PrevLogEntry == lastIdx {
			prevLogTerm = lastTerm // 日志索引一样、任期应该一致
			//	 TODO 一会儿找不一致的情况，😁    ？？不同集群的节点，
		} else {
			// 分两种
			// PrevLogEntry < lastIdx   可以查找，不会报错
			// PrevLogEntry > lastIdx	查不到，会报错
			var prevLog Log
			if err := r.logs.GetLog(a.PrevLogEntry, &prevLog); err != nil {
				r.logger.Warn("获取最新日志失败", "请求的索引", a.PrevLogEntry, "最新的索引", lastIdx, "error", err)
				resp.NoRetryBackoff = true
				return
			}
			// 走到这，就是可以找到
			prevLogTerm = prevLog.Term
		}

		if a.PrevLogTerm != prevLogTerm {
			r.logger.Warn("与之前的任期不一样", "ours", prevLogTerm, "remote", a.PrevLogTerm)
			resp.NoRetryBackoff = true
			return
		}
	}

	// 处理每一个日志
	if len(a.Entries) > 0 {
		// 删除任何冲突的条目，跳过任何重复的条目
		lastLogIdx, _ := r.getLastLog()
		var newEntries []*Log
		for i, entry := range a.Entries {
			if entry.Index > lastLogIdx {
				newEntries = a.Entries[i:]
				break
			}
			var storeEntry Log
			if err := r.logs.GetLog(entry.Index, &storeEntry); err != nil {
				r.logger.Warn("获取日志条目失败", "index", entry.Index, "error", err)
				return
			}
			if entry.Term != storeEntry.Term {
				r.logger.Warn("清理日志前缀", "from", entry.Index, "to", lastLogIdx)
				if err := r.logs.DeleteRange(entry.Index, lastLogIdx); err != nil {
					r.logger.Error("清理日志前缀失败", "error", err)
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

	// 更新follower的commit index
	if a.LeaderCommitIndex > 0 && a.LeaderCommitIndex > r.getCommitIndex() {
		idx := min(a.LeaderCommitIndex, r.getLastIndex())
		r.setCommitIndex(idx)
		if r.configurations.latestIndex <= idx {
			r.setCommittedConfiguration(r.configurations.latest, r.configurations.latestIndex)
		}
		r.processLogs(idx, nil)
	}

	resp.Success = true
	r.setLastContact()
	return
}

// processConfigurationLogEntry
// 从logState中获取快照中没有的数据,然后对每一个log 调用此函数
func (r *Raft) processConfigurationLogEntry(entry *Log) error {
	switch entry.Type {
	case LogConfiguration: //
		r.setCommittedConfiguration(r.configurations.latest, r.configurations.latestIndex)
		r.setLatestConfiguration(DecodeConfiguration(entry.Data), entry.Index)
		//r.configurations.committed = r.configurations.latest
		//r.configurations.committedIndex = r.configurations.latestIndex
		//r.configurations.latest = DecodeConfiguration(entry.Data)
		//r.configurations.latestIndex = entry.Index
		//r.latestConfiguration.Store(DecodeConfiguration(entry.Data).Clone())

	case LogAddPeerDeprecated:
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

// installSnapshot Follower状态下，日志落后leader太多
func (r *Raft) installSnapshot(rpc RPC, req *InstallSnapshotRequest) {
	_ = (&NetworkTransport{}).InstallSnapshot
	resp := &InstallSnapshotResponse{
		Term:    r.getCurrentTerm(),
		Success: false,
	}
	var rpcErr error
	defer func() {
		io.Copy(ioutil.Discard, rpc.Reader)
		// 确保我们总是使用来自流的所有快照数据(参见问题#212)
		rpc.Respond(resp, rpcErr)
	}()

	if req.SnapshotVersion < SnapshotVersionMin || req.SnapshotVersion > SnapshotVersionMax {
		rpcErr = fmt.Errorf("不支持的快照版本 %d", req.SnapshotVersion)
		return
	}
	// req.Term leader端存储的follower任期
	if req.Term < r.getCurrentTerm() {
		r.logger.Info("忽略比当前期限更早的installSnapshot请求", "request-term", req.Term, "current-term", r.getCurrentTerm())
		return
	}

	if req.Term > r.getCurrentTerm() {
		r.setState(Follower)
		r.setCurrentTerm(req.Term)
		resp.Term = req.Term
	}

	r.setLeader(r.trans.DecodePeer(req.Leader))

	var reqConfiguration Configuration
	var reqConfigurationIndex uint64
	if req.SnapshotVersion > 0 {
		reqConfiguration = DecodeConfiguration(req.Configuration)
		reqConfigurationIndex = req.ConfigurationIndex
	} else {
		reqConfiguration, rpcErr = decodePeers(req.Peers, r.trans)
		if rpcErr != nil {
			r.logger.Error("安装快照失败", "error", rpcErr)
			return
		}
		reqConfigurationIndex = req.LastLogIndex
	}
	version := getSnapshotVersion(r.protocolVersion) // 1
	sink, err := r.snapshots.Create(version, req.LastLogIndex, req.LastLogTerm, reqConfiguration, reqConfigurationIndex, r.trans)
	if err != nil {
		r.logger.Error("创建快照失败", "error", err)
		rpcErr = fmt.Errorf("创建快照失败: %v", err)
		return
	}

	// 将远程快照存储到磁盘
	n, err := io.Copy(sink, rpc.Reader)
	if err != nil {
		sink.Cancel() // 关闭、清除文件
		r.logger.Error("拷贝快照失败", "error", err)
		rpcErr = err
		return
	}

	//检查一下我们是否都收到了
	if n != req.Size {
		sink.Cancel() // 关闭、清除文件
		r.logger.Error("接收完整快照失败", "received", hclog.Fmt("%d / %d", n, req.Size))
		rpcErr = fmt.Errorf("数据不够")
		return
	}

	// 完成快照
	if err := sink.Close(); err != nil {
		r.logger.Error("完成快照失败", "error", err)
		rpcErr = err
		return
	}
	r.logger.Info("拷贝到了本地快照", "bytes", n)

	// 重置快照
	future := &restoreFuture{ID: sink.ID()}
	future.ShutdownCh = r.shutdownCh
	future.init()
	select {
	case r.fsmMutateCh <- future:
		_ = r.runFSM
	case <-r.shutdownCh:
		future.respond(ErrRaftShutdown)
		return
	}

	// 等待恢复发生
	if err := future.Error(); err != nil { // 阻塞
		r.logger.Error("恢复快照失败", "error", err)
		rpcErr = err
		return
	}

	// 更新lastApplied，这样我们就不会重播旧日志了
	r.setLastApplied(req.LastLogIndex)

	// 更新最新的快照
	r.setLastSnapshot(req.LastLogIndex, req.LastLogTerm)

	r.setLatestConfiguration(reqConfiguration, reqConfigurationIndex)
	r.setCommittedConfiguration(reqConfiguration, reqConfigurationIndex)

	// 压缩日志，即使失败也要继续
	if err := r.compactLogs(req.LastLogIndex); err != nil {
		r.logger.Error("压缩日志失败", "error", err)
	}

	r.logger.Info("已安装远程的日志")
	resp.Success = true
	r.setLastContact()
	return
}
