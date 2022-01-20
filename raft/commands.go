package raft

// RPCHeader 是一个公共的子结构，用于传递协议版本和其他关于集群的信息。
// 对于加入版本控制之前的旧版Raft实现，当被新版Raft读取时，它将默认为一个零值结构。
type RPCHeader struct {
	ProtocolVersion ProtocolVersion
}

// WithRPCHeader 暴露rpc头信息
type WithRPCHeader interface {
	GetRPCHeader() RPCHeader
}

// AppendEntriesRequest 是用于向复制的日志追加条目的命令。
type AppendEntriesRequest struct {
	RPCHeader

	// 提供当前的任期和leader
	Term   uint64
	Leader []byte

	// 提供以前的条目进行完整性检查
	PrevLogEntry uint64
	PrevLogTerm  uint64

	// 新提交的日志条目
	Entries []*Log

	// leader commit的索引
	LeaderCommitIndex uint64
}

// GetRPCHeader - 返回RPCHeader
func (r *AppendEntriesRequest) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// AppendEntriesResponse is the response returned from an
// AppendEntriesRequest.
type AppendEntriesResponse struct {
	RPCHeader

	// Newer term if leader is out of date
	Term uint64

	// Last Log is a hint to help accelerate rebuilding slow nodes
	LastLog uint64

	// We may not succeed if we have a conflicting entry
	Success bool

	// There are scenarios where this request didn't succeed
	// but there's no need to wait/back-off the next attempt.
	NoRetryBackoff bool
}

// GetRPCHeader - See WithRPCHeader.
func (r *AppendEntriesResponse) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// RequestVoteRequest is the command used by a candidate to ask a Raft peer
// for a vote in an election.
type RequestVoteRequest struct {
	RPCHeader

	// Provide the term and our id
	Term      uint64
	Candidate []byte

	// Used to ensure safety
	LastLogIndex uint64
	LastLogTerm  uint64

	// Used to indicate to peers if this vote was triggered by a leadership
	// transfer. It is required for leadership transfer to work, because servers
	// wouldn't vote otherwise if they are aware of an existing leader.
	LeadershipTransfer bool
}

// GetRPCHeader - See WithRPCHeader.
func (r *RequestVoteRequest) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// RequestVoteResponse   RequestVoteRequest的响应
type RequestVoteResponse struct {
	RPCHeader

	//新的任期 如果leader过期
	Term uint64

	// Peers 已被废弃，但只理解协议版本0的服务器需要它。 在协议版本2及以后的版本中，它不被填充。
	Peers []byte

	// 是否批准投票。
	Granted bool
}

// GetRPCHeader - See WithRPCHeader.
func (r *RequestVoteResponse) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// InstallSnapshotRequest is the command sent to a Raft peer to bootstrap its
// log (and state machine) from a snapshot on another peer.
type InstallSnapshotRequest struct {
	RPCHeader
	SnapshotVersion SnapshotVersion

	Term   uint64
	Leader []byte

	// These are the last index/term included in the snapshot
	LastLogIndex uint64
	LastLogTerm  uint64

	// Peer Set in the snapshot. This is deprecated in favor of Configuration
	// but remains here in case we receive an InstallSnapshot from a leader
	// that's running old code.
	Peers []byte

	// Cluster membership.
	Configuration []byte
	// Log index where 'Configuration' entry was originally written.
	ConfigurationIndex uint64

	// Size of the snapshot
	Size int64
}

// GetRPCHeader - See WithRPCHeader.
func (r *InstallSnapshotRequest) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// InstallSnapshotResponse is the response returned from an
// InstallSnapshotRequest.
type InstallSnapshotResponse struct {
	RPCHeader

	Term    uint64
	Success bool
}

// GetRPCHeader - See WithRPCHeader.
func (r *InstallSnapshotResponse) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// TimeoutNowRequest is the command used by a leader to signal another server to
// start an election.
type TimeoutNowRequest struct {
	RPCHeader
}

// GetRPCHeader - See WithRPCHeader.
func (r *TimeoutNowRequest) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// TimeoutNowResponse is the response to TimeoutNowRequest.
type TimeoutNowResponse struct {
	RPCHeader
}

// GetRPCHeader - See WithRPCHeader.
func (r *TimeoutNowResponse) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}
