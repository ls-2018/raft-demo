package raft

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-msgpack/codec"
)

const (
	rpcAppendEntries   uint8 = iota // 日志追加
	rpcRequestVote                  // 请求投票
	rpcInstallSnapshot              // 打快照
	rpcTimeoutNow

	// DefaultTimeoutScale is the default TimeoutScale in a NetworkTransport.
	DefaultTimeoutScale = 256 * 1024 // 256KB

	// rpcMaxPipeline AppendEntries RPC calls.一次性最大发送数
	rpcMaxPipeline = 128

	// connReceiveBufferSize 是我们将用于接收RPC请求数据的缓冲区的大小， ->follower。
	connReceiveBufferSize = 256 * 1024 // 256KB

	// connSendBufferSize 是我们将用于发送RPC请求数据的缓冲区的大小，从leader到follower。
	connSendBufferSize = 256 * 1024 // 256KB
)

var (
	// ErrTransportShutdown is returned when operations on a transport are
	// invoked after it's been terminated.
	ErrTransportShutdown = errors.New("transport shutdown")

	// ErrPipelineShutdown is returned when the pipeline is closed.
	ErrPipelineShutdown = errors.New("append pipeline closed")
)

/*

NetworkTransport provides a network based transport that can be
used to communicate with Raft on remote machines. It requires
an underlying stream layer to provide a stream abstraction, which can
be simple TCP, TLS, etc.

This transport is very simple and lightweight. Each RPC request is
framed by sending a byte that indicates the message type, followed
by the MsgPack encoded request.

The response is an error string followed by the response object,
both are encoded using MsgPack.

InstallSnapshot is special, in that after the RPC request we stream
the entire state. That socket is not re-used as the connection state
is not known if there is an error.

*/
type NetworkTransport struct {
	connPool     map[ServerAddress][]*netConn
	connPoolLock sync.Mutex

	consumeCh chan RPC // rpc请求消息

	heartbeatFn     func(RPC)
	heartbeatFnLock sync.Mutex

	logger hclog.Logger

	maxPool int

	serverAddressProvider ServerAddressProvider // 默认为nil,再测试的时候 会添加内容

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	stream StreamLayer

	// streamCtx 是用来取消现有的连接处理程序。
	streamCtx     context.Context
	streamCancel  context.CancelFunc
	streamCtxLock sync.RWMutex

	timeout      time.Duration
	TimeoutScale int
}

var _ Transport = &NetworkTransport{} // 既可以发送命令、也可以接收命令
// NetworkTransportConfig 封装了网络传输层的配置。
type NetworkTransportConfig struct {
	// ServerAddressProvider 用于在建立连接以调用RPC时覆盖目标地址。
	ServerAddressProvider ServerAddressProvider

	Logger hclog.Logger

	// Dialer
	Stream StreamLayer

	// MaxPool 控制我们将存储多少个连接
	MaxPool int

	// Timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
	// the timeout by (SnapshotSize / TimeoutScale).
	Timeout time.Duration
}

// ServerAddressProvider is a target address to which we invoke an RPC when establishing a connection
type ServerAddressProvider interface {
	ServerAddr(id ServerID) (ServerAddress, error)
}

// StreamLayer is used with the NetworkTransport to provide
// the low level stream abstraction.
type StreamLayer interface {
	net.Listener

	// Dial is used to create a new outgoing connection
	Dial(address ServerAddress, timeout time.Duration) (net.Conn, error)
}

// 封装了net.Conn
type netConn struct {
	target ServerAddress
	conn   net.Conn
	w      *bufio.Writer
	dec    *codec.Decoder
	enc    *codec.Encoder
}

func (n *netConn) Release() error {
	return n.conn.Close()
}

type netPipeline struct {
	conn  *netConn
	trans *NetworkTransport

	doneCh       chan AppendFuture
	inprogressCh chan *appendFuture

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

// NewNetworkTransportWithConfig 用给定的配置结构创建一个新的网络传输。
func NewNetworkTransportWithConfig(config *NetworkTransportConfig) *NetworkTransport {
	if config.Logger == nil {
		config.Logger = hclog.New(&hclog.LoggerOptions{
			Name:   "raft-net",
			Output: hclog.DefaultOutput, //  os.Stderr
			Level:  hclog.DefaultLevel,  // Info
		})
	}
	trans := &NetworkTransport{
		connPool:              make(map[ServerAddress][]*netConn), // string= []*netConn{}
		consumeCh:             make(chan RPC),                     // 阻塞式传递
		logger:                config.Logger,
		maxPool:               config.MaxPool,
		shutdownCh:            make(chan struct{}),
		stream:                config.Stream,
		timeout:               config.Timeout,
		TimeoutScale:          DefaultTimeoutScale,
		serverAddressProvider: config.ServerAddressProvider, // nil
	}

	trans.setupStreamContext()
	go trans.listen()

	return trans
}

// NewNetworkTransport 用给定的拨号器和监听器创建一个新的网络传输
func NewNetworkTransport(stream StreamLayer, maxPool int, timeout time.Duration, logOutput io.Writer) *NetworkTransport {
	if logOutput == nil {
		logOutput = os.Stderr
	}
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "raft-net",
		Output: logOutput,
		Level:  hclog.DefaultLevel,
	})
	config := &NetworkTransportConfig{Stream: stream, MaxPool: maxPool, Timeout: timeout, Logger: logger}
	return NewNetworkTransportWithConfig(config)
}

// NewNetworkTransportWithLogger  用给定的拨号器和监听器创建一个新的网络传输
func NewNetworkTransportWithLogger(stream StreamLayer, maxPool int, timeout time.Duration, logger hclog.Logger) *NetworkTransport {
	config := &NetworkTransportConfig{Stream: stream, MaxPool: maxPool, Timeout: timeout, Logger: logger}
	return NewNetworkTransportWithConfig(config)
}

// setupStreamContext 是用来创建一个StreamContext。在调用该功能时，应持有流锁。
func (n *NetworkTransport) setupStreamContext() {
	ctx, cancel := context.WithCancel(context.Background())
	n.streamCtx = ctx
	n.streamCancel = cancel
}

// getStreamContext 是用来检索当前流的上下文。
func (n *NetworkTransport) getStreamContext() context.Context {
	n.streamCtxLock.RLock()
	defer n.streamCtxLock.RUnlock()
	return n.streamCtx
}

// SetHeartbeatHandler 是用来设置一个心跳处理程序作为一个快速通道。这是为了避免磁盘IO的线头阻塞。
func (n *NetworkTransport) SetHeartbeatHandler(cb func(rpc RPC)) {
	n.heartbeatFnLock.Lock()
	defer n.heartbeatFnLock.Unlock()
	n.heartbeatFn = cb
}

// CloseStreams closes the current streams.
func (n *NetworkTransport) CloseStreams() {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	// Close all the connections in the connection pool and then remove their
	// entry.
	for k, e := range n.connPool {
		for _, conn := range e {
			conn.Release()
		}

		delete(n.connPool, k)
	}

	// Cancel the existing connections and create a new context. Both these
	// operations must always be done with the lock held otherwise we can create
	// connection handlers that are holding a context that will never be
	// cancelable.
	n.streamCtxLock.Lock()
	n.streamCancel()
	n.setupStreamContext()
	n.streamCtxLock.Unlock()
}

// Close is used to stop the network transport.
func (n *NetworkTransport) Close() error {
	n.shutdownLock.Lock()
	defer n.shutdownLock.Unlock()

	if !n.shutdown {
		close(n.shutdownCh)
		n.stream.Close()
		n.shutdown = true
	}
	return nil
}

// Consumer 获取可以消费的命令事件
func (n *NetworkTransport) Consumer() <-chan RPC {
	return n.consumeCh
}

// LocalAddr implements the Transport interface.
func (n *NetworkTransport) LocalAddr() ServerAddress {
	return ServerAddress(n.stream.Addr().String())
}

// IsShutdown is used to check if the transport is shutdown.
func (n *NetworkTransport) IsShutdown() bool {
	select {
	case <-n.shutdownCh:
		return true
	default:
		return false
	}
}

// getExistingConn is used to grab a pooled connection.
func (n *NetworkTransport) getPooledConn(target ServerAddress) *netConn {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	conns, ok := n.connPool[target]
	if !ok || len(conns) == 0 {
		return nil
	}

	var conn *netConn
	num := len(conns)
	conn, conns[num-1] = conns[num-1], nil
	n.connPool[target] = conns[:num-1]
	return conn
}

// getConnFromAddressProvider 根据地址获取链接
func (n *NetworkTransport) getConnFromAddressProvider(id ServerID, target ServerAddress) (*netConn, error) {
	address := n.getProviderAddressOrFallback(id, target)
	return n.getConn(address)
}

// 获得服务地址
func (n *NetworkTransport) getProviderAddressOrFallback(id ServerID, target ServerAddress) ServerAddress {
	if n.serverAddressProvider != nil {
		serverAddressOverride, err := n.serverAddressProvider.ServerAddr(id)
		if err != nil {
			n.logger.Warn("无法获得服务器地址，使用备用地址", "id", id, "fallback", target, "error", err)
		} else {
			return serverAddressOverride
		}
	}
	return target
}

// getConn 从连接池中获取一个节点链接
func (n *NetworkTransport) getConn(target ServerAddress) (*netConn, error) {
	if conn := n.getPooledConn(target); conn != nil {
		return conn, nil
	}
	conn, err := n.stream.Dial(target, n.timeout)
	if err != nil {
		return nil, err
	}
	netConn := &netConn{
		target: target,
		conn:   conn,
		dec:    codec.NewDecoder(bufio.NewReader(conn), &codec.MsgpackHandle{}),
		w:      bufio.NewWriterSize(conn, connSendBufferSize), // 256KB
	}

	netConn.enc = codec.NewEncoder(netConn.w, &codec.MsgpackHandle{})

	return netConn, nil
}

// returnConn returns a connection back to the pool.
func (n *NetworkTransport) returnConn(conn *netConn) {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	key := conn.target
	conns, _ := n.connPool[key]

	if !n.IsShutdown() && len(conns) < n.maxPool {
		n.connPool[key] = append(conns, conn)
	} else {
		conn.Release()
	}
}

// AppendEntriesPipeline  批量AppendEntries
func (n *NetworkTransport) AppendEntriesPipeline(id ServerID, target ServerAddress) (AppendPipeline, error) {
	conn, err := n.getConnFromAddressProvider(id, target)
	if err != nil {
		return nil, err
	}

	// 创建管道
	return newNetPipeline(n, conn), nil
}

// AppendEntries 追加日志请求
func (n *NetworkTransport) AppendEntries(id ServerID, target ServerAddress, args *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	return n.genericRPC(id, target, rpcAppendEntries, args, resp)
}

// RequestVote 发送投票请求到目标节点
func (n *NetworkTransport) RequestVote(id ServerID, target ServerAddress, args *RequestVoteRequest, resp *RequestVoteResponse) error {
	return n.genericRPC(id, target, rpcRequestVote, args, resp)
}

// genericRPC 通用的RPC处理函数
func (n *NetworkTransport) genericRPC(id ServerID, target ServerAddress, rpcType uint8, args interface{}, resp interface{}) error {
	conn, err := n.getConnFromAddressProvider(id, target)
	if err != nil {
		return err
	}

	// 设置超时时间
	if n.timeout > 0 {
		conn.conn.SetDeadline(time.Now().Add(n.timeout))
	}

	// 发送RPC请求
	if err = sendRPC(conn, rpcType, args); err != nil {
		return err
	}

	// Decode the response
	canReturn, err := decodeResponse(conn, resp)
	if canReturn {
		n.returnConn(conn)
	}
	return err
}

// InstallSnapshot 用于将快照下推给follower。从ReadCloser读取数据并推到客户端。
func (n *NetworkTransport) InstallSnapshot(id ServerID, target ServerAddress, args *InstallSnapshotRequest, resp *InstallSnapshotResponse, data io.Reader) error {
	conn, err := n.getConnFromAddressProvider(id, target)
	if err != nil {
		return err
	}
	defer conn.Release()

	// 设定一个截止日期，根据请求的大小进行调整
	if n.timeout > 0 {
		timeout := n.timeout * time.Duration(args.Size/int64(n.TimeoutScale))
		if timeout < n.timeout {
			timeout = n.timeout
		}
		conn.conn.SetDeadline(time.Now().Add(timeout))
	}

	if err = sendRPC(conn, rpcInstallSnapshot, args); err != nil {
		return err
	}

	if _, err = io.Copy(conn.w, data); err != nil {
		return err
	}

	// Flush
	if err = conn.w.Flush(); err != nil {
		return err
	}

	// 解码响应
	_, err = decodeResponse(conn, resp)
	return err
}

// EncodePeer 是用来序列化一个节点的地址。
func (n *NetworkTransport) EncodePeer(id ServerID, p ServerAddress) []byte {
	address := n.getProviderAddressOrFallback(id, p)
	return []byte(address)
}

// DecodePeer implements the Transport interface.
func (n *NetworkTransport) DecodePeer(buf []byte) ServerAddress {
	return ServerAddress(buf)
}

// TimeoutNow implements the Transport interface.
func (n *NetworkTransport) TimeoutNow(id ServerID, target ServerAddress, args *TimeoutNowRequest, resp *TimeoutNowResponse) error {
	return n.genericRPC(id, target, rpcTimeoutNow, args, resp)
}

// listen 是用来处理传入的连接。
func (n *NetworkTransport) listen() {
	const baseDelay = 5 * time.Millisecond
	const maxDelay = 1 * time.Second

	var loopDelay time.Duration
	for {
		// 处理到来的链接
		conn, err := n.stream.Accept()
		if err != nil {
			if loopDelay == 0 {
				loopDelay = baseDelay
			} else {
				loopDelay *= 2
			}

			if loopDelay > maxDelay {
				loopDelay = maxDelay
			}

			if !n.IsShutdown() {
				n.logger.Error("failed to accept connection", "error", err)
			}

			select {
			case <-n.shutdownCh:
				return
			case <-time.After(loopDelay):
				continue
			}
		}
		// 无错误，重置循环延迟
		loopDelay = 0

		n.logger.Debug("接收链接", "local-address", n.LocalAddr(), "remote-address", conn.RemoteAddr().String())

		// 在单独的go routine中处理连接
		go n.handleConn(n.getStreamContext(), conn)
	}
}

// handleConn 处理入栈请求,
func (n *NetworkTransport) handleConn(connCtx context.Context, conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReaderSize(conn, connReceiveBufferSize) // 256Kb
	w := bufio.NewWriter(conn)
	dec := codec.NewDecoder(r, &codec.MsgpackHandle{})
	enc := codec.NewEncoder(w, &codec.MsgpackHandle{})

	for {
		select {
		case <-connCtx.Done():
			n.logger.Debug("传输层已关闭")
			return
		default:
		}

		if err := n.handleCommand(r, dec, enc); err != nil {
			if err != io.EOF {
				n.logger.Error("解码传输到来的命令失败", "error", err)
			}
			return
		}
		if err := w.Flush(); err != nil {
			n.logger.Error("刷盘 res 失败", "error", err)
			return
		}
	}
}

// handleCommand 解码、处理命令
func (n *NetworkTransport) handleCommand(r *bufio.Reader, dec *codec.Decoder, enc *codec.Encoder) error {

	rpcType, err := r.ReadByte() // 读取一个字节
	if err != nil {
		return err
	}

	// measuring the time to get the first byte separately because the heartbeat conn will hang out here
	// for a good while waiting for a heartbeat whereas the append entries/rpc conn should not.
	// Create the RPC object
	// 分别测量获得第一个字节的时间，因为心跳CONN会在这里停留很长时间等待心跳，而追加日志/RPC CONN不会
	respCh := make(chan RPCResponse, 1)
	rpc := RPC{
		RespChan: respCh,
	}

	// 解析命令
	isHeartbeat := false
	switch rpcType {
	case rpcAppendEntries: // 日志追加
		var req AppendEntriesRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req

		// 检查这是否是心跳
		if req.Term != 0 && req.Leader != nil &&
			req.PrevLogEntry == 0 && req.PrevLogTerm == 0 &&
			len(req.Entries) == 0 && req.LeaderCommitIndex == 0 {
			isHeartbeat = true
		}

	case rpcRequestVote: // 申请投票请求
		var req RequestVoteRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
	case rpcInstallSnapshot:
		var req InstallSnapshotRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
		rpc.Reader = io.LimitReader(r, req.Size)
	case rpcTimeoutNow:
		var req TimeoutNowRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
	default:
		return fmt.Errorf("未知的rpc类型 %d", rpcType)
	}

	// 检查心脏跳动的快速途径
	if isHeartbeat {
		n.heartbeatFnLock.Lock()
		fn := n.heartbeatFn
		n.heartbeatFnLock.Unlock()
		if fn != nil {
			fn(rpc)
			goto RESP
		}
	}

	// 分发rpc请求
	select {
	case n.consumeCh <- rpc: // 保证只有一个命令在处理  size=0
	case <-n.shutdownCh:
		return ErrTransportShutdown
	}

	// 等待响应
RESP:
	// we will differentiate the heartbeat fast path from normal RPCs with labels
	// 我们将用标签来区分心跳和普通的RPC
	select {
	case resp := <-respCh: //  n.consumeCh <- rpc 放进去以后，要等代处理完成
		// 会交给  r.runFollower \ r.runCandidate() \ r.runLeader() 来从consumeCh 读取,最终由r.processRPC(rpc) 处理
		respErr := ""
		if resp.Error != nil {
			respErr = resp.Error.Error()
		}
		if err := enc.Encode(respErr); err != nil {
			return err
		}

		// 发送响应
		if err := enc.Encode(resp.Response); err != nil {
			return err
		}
	case <-n.shutdownCh:
		return ErrTransportShutdown
	}
	return nil
}

// decodeResponse 用于解码RPC响应，并报告连接是否可以重用。
func decodeResponse(conn *netConn, resp interface{}) (bool, error) {
	// 解码错误(如果有的话)
	var rpcError string
	if err := conn.dec.Decode(&rpcError); err != nil {
		conn.Release()
		return false, err
	}

	// 解码响应
	if err := conn.dec.Decode(resp); err != nil {
		conn.Release()
		return false, err
	}

	// Format an error if any
	if rpcError != "" {
		return true, fmt.Errorf(rpcError)
	}
	return true, nil
}

// sendRPC 发送请求
func sendRPC(conn *netConn, rpcType uint8, args interface{}) error {
	// 先发送请求类型
	if err := conn.w.WriteByte(rpcType); err != nil {
		conn.Release()
		return err
	}

	// 编码请求体
	// raft/net_transport.go:332
	// 将编码好的数据，写入到了enc   ;  enc 是封装的w
	if err := conn.enc.Encode(args); err != nil {
		conn.Release()
		return err
	}

	// Flush  w也是封装的conn
	if err := conn.w.Flush(); err != nil {
		conn.Release()
		return err
	}
	return nil
}

// newNetPipeline 构建管道
func newNetPipeline(trans *NetworkTransport, conn *netConn) *netPipeline {
	n := &netPipeline{
		conn:         conn,
		trans:        trans,
		doneCh:       make(chan AppendFuture, rpcMaxPipeline),
		inprogressCh: make(chan *appendFuture, rpcMaxPipeline),
		shutdownCh:   make(chan struct{}),
	}
	go n.decodeResponses()
	return n
}

// decodeResponses 解析远程调用的响应
func (n *netPipeline) decodeResponses() {
	timeout := n.trans.timeout
	for {
		select {
		case future := <-n.inprogressCh:
			if timeout > 0 {
				n.conn.conn.SetReadDeadline(time.Now().Add(timeout))
			}

			_, err := decodeResponse(n.conn, future.resp)
			future.respond(err)
			select {
			case n.doneCh <- future:
			case <-n.shutdownCh:
				_ = n.Close
				return
			}
		case <-n.shutdownCh:
			_ = n.Close
			return
		}
	}
}

// AppendEntries is used to pipeline a new append entries request.
func (n *netPipeline) AppendEntries(args *AppendEntriesRequest, resp *AppendEntriesResponse) (AppendFuture, error) {
	future := &appendFuture{
		start: time.Now(),
		args:  args,
		resp:  resp,
	}
	future.init()

	if timeout := n.trans.timeout; timeout > 0 {
		n.conn.conn.SetWriteDeadline(time.Now().Add(timeout))
	}

	if err := sendRPC(n.conn, rpcAppendEntries, future.args); err != nil {
		return nil, err
	}

	// Hand-off for decoding, this can also cause back-pressure
	// to prevent too many inflight requests
	select {
	case n.inprogressCh <- future:
		return future, nil
	case <-n.shutdownCh:
		_ = n.Close
		return nil, ErrPipelineShutdown
	}
}

// Consumer returns a channel that can be used to consume complete futures.
func (n *netPipeline) Consumer() <-chan AppendFuture {
	return n.doneCh
}

// Close 用于关闭所有管道链接is used to shutdown the pipeline connection.
func (n *netPipeline) Close() error {
	n.shutdownLock.Lock()
	defer n.shutdownLock.Unlock()
	if n.shutdown {
		// 已关闭
		return nil
	}

	// 释放连接
	n.conn.Release()

	n.shutdown = true
	close(n.shutdownCh)
	return nil
}
