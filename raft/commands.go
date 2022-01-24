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

	// 从leader角度出发,从follower看,xx日志已写入leader，但是follower没有，xx对follower来说就是pre
	PrevLogEntry uint64 // 同步过来的一批的日志的第一个日志索引
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

type AppendEntriesResponse struct {
	RPCHeader
	Term uint64
	// 一个提示，帮助缓慢的节点 加速重建
	LastLog uint64
	// 如果我们有一个冲突的条目，我们可能不会成功
	Success bool
	// 有些情况下，这个请求没有成功，但没有必要等待/回避下一次尝试。
	NoRetryBackoff bool
}

// GetRPCHeader - See WithRPCHeader.
func (r *AppendEntriesResponse) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

// RequestVoteRequest 是候选人在选举中向raft中的节点请求投票的req。
type RequestVoteRequest struct {
	RPCHeader

	// 提供当前的任期、竞选者逻辑ID
	Term      uint64 // 发送者的当前任期
	Candidate []byte // localAddr

	LastLogIndex uint64
	LastLogTerm  uint64

	// 特权模式
	// 如果是true ，在请求投票时，及时对方有leader ,也会走判断term、index的逻辑
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

type InstallSnapshotRequest struct {
	RPCHeader
	SnapshotVersion SnapshotVersion

	Term   uint64
	Leader []byte

	// 快照的索引、任期
	LastLogIndex uint64
	LastLogTerm  uint64

	//快照中包含的节点信息；这已被废弃，转而使用 "Configuration"，但仍保留在这里，以备我们从运行旧代码的领导者那里收到InstallSnapshot。
	Peers []byte

	// 集群成员
	Configuration []byte
	// 最初写入'Configuration'条目的索引。
	ConfigurationIndex uint64

	// Size 快照大小
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

// TimeoutNowRequest 是由leader发出信号给另一台服务器开始选举的命令。
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
