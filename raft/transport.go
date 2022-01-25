package raft

import (
	"io"
	"time"
)

// RPCResponse 捕捉到了一个响应和一个错误。
type RPCResponse struct {
	Response interface{}
	Error    error
}

// RPC 有一个命令，并提供一个响应机制。
type RPC struct {
	Command  interface{}
	Reader   io.Reader // 只有在 InstallSnapshot时设置
	RespChan chan<- RPCResponse
}

// Respond is used to respond with a response, error or both
func (r *RPC) Respond(resp interface{}, err error) {
	r.RespChan <- RPCResponse{resp, err}
}

// Transport 提供了一个网络传输的接口 以使Raft能够与其他节点进行通信。
type Transport interface {
	// Consumer 返回一个可用于消费和响应RPC请求的通道。
	Consumer() <-chan RPC

	// LocalAddr is used to return our local address to distinguish from our peers.
	LocalAddr() ServerAddress

	// AppendEntriesPipeline  批量AppendEntries接口
	AppendEntriesPipeline(id ServerID, target ServerAddress) (AppendPipeline, error)

	// AppendEntries 追加日志请求  raft/rpc_call.go:55
	AppendEntries(id ServerID, target ServerAddress, args *AppendEntriesRequest, resp *AppendEntriesResponse) error

	// RequestVote 发送投票请求到目标节点 raft/rpc_call.go:
	RequestVote(id ServerID, target ServerAddress, args *RequestVoteRequest, resp *RequestVoteResponse) error

	// InstallSnapshot 用于将快照下推给follower。从ReadCloser读取数据并推到客户端。
	InstallSnapshot(id ServerID, target ServerAddress, args *InstallSnapshotRequest, resp *InstallSnapshotResponse, data io.Reader) error

	// EncodePeer 是用来序列化一个节点的地址。
	EncodePeer(id ServerID, addr ServerAddress) []byte

	// DecodePeer is used to deserialize a peer's address.
	DecodePeer([]byte) ServerAddress

	// SetHeartbeatHandler 是用来设置一个心跳处理程序作为一个快速通道。这是为了避免磁盘IO的线头阻塞。如果Transport不支持这一点，它可以简单地忽略调用，并将心跳推到消费者通道上。
	SetHeartbeatHandler(cb func(rpc RPC))

	// TimeoutNow is used to start a leadership transfer to the target node.
	TimeoutNow(id ServerID, target ServerAddress, args *TimeoutNowRequest, resp *TimeoutNowResponse) error
}

// WithClose is an interface that a transport may provide which
// allows a transport to be shut down cleanly when a Raft instance
// shuts down.
//
// It is defined separately from Transport as unfortunately it wasn't in the
// original interface specification.
type WithClose interface {
	// Close permanently closes a transport, stopping
	// any associated goroutines and freeing other resources.
	Close() error
}

// LoopbackTransport is an interface that provides a loopback transport suitable for testing
// e.g. InmemTransport. It's there so we don't have to rewrite tests.
type LoopbackTransport interface {
	Transport // Embedded transport reference
	WithPeers // Embedded peer management
	WithClose // with a close routine
}

// WithPeers is an interface that a transport may provide which allows for connection and
// disconnection. Unless the transport is a loopback transport, the transport specified to
// "Connect" is likely to be nil.
type WithPeers interface {
	Connect(peer ServerAddress, t Transport) // Connect a peer
	Disconnect(peer ServerAddress)           // Disconnect a given peer
	DisconnectAll()                          // Disconnect all peers, possibly to reconnect them later
}

// AppendPipeline 用于管道AppendEntries请求。它通过屏蔽延迟和更好地利用带宽来提高复制吞吐量。
type AppendPipeline interface {
	// AppendEntries 用于向管道中添加另一个请求。发送可能会阻塞，这是一种有效的背压形式。
	AppendEntries(args *AppendEntriesRequest, resp *AppendEntriesResponse) (AppendFuture, error)

	// Consumer 返回用于消费的 就绪的response futures  channel
	Consumer() <-chan AppendFuture

	// Close 关闭管道并取消所有通信中的RPCs
	Close() error
}

// AppendFuture 用于返回管道传输对象的信息
type AppendFuture interface {
	Future

	// Start 返回追加请求开始的时间。调用这个方法总是ok的。
	Start() time.Time

	// Request 保存AppendEntries调用的参数。调用这个方法总是OK的。
	Request() *AppendEntriesRequest

	// Response 保存AppendEntries调用的结果。此方法只能在Error方法返回后调用，并且只有在成功时才有效。
	Response() *AppendEntriesResponse
}
