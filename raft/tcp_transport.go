package raft

import (
	"errors"
	"github.com/hashicorp/go-hclog"
	"io"
	"log"
	"net"
	"time"
)

var (
	errNotAdvertisable = errors.New("本地绑定的地址不能被公布")
	errNotTCP          = errors.New("本地地址不是TCP地址")
)

// TCPStreamLayer 实现了普通TCP的StreamLayer接口。
type TCPStreamLayer struct {
	advertise net.Addr
	listener  *net.TCPListener
}

// NewTCPTransport 返回一个NetworkTransport，它是建立在 TCP流媒体传输层之上。
func NewTCPTransport(bindAddr string, advertise net.Addr, maxPool int, timeout time.Duration, logOutput io.Writer) (*NetworkTransport, error) {
	return newTCPTransport(bindAddr, advertise, func(stream StreamLayer) *NetworkTransport {
		return NewNetworkTransport(stream, maxPool, timeout, logOutput)
	})
}

// NewTCPTransportWithLogger 返回一个NetworkTransport，这个NetworkTransport是建立在TCP流传输层之上的。 日志输出将被发送到提供的Logger
func NewTCPTransportWithLogger(bindAddr string, advertise net.Addr, maxPool int, timeout time.Duration, logger hclog.Logger) (*NetworkTransport, error) {
	return newTCPTransport(bindAddr, advertise, func(stream StreamLayer) *NetworkTransport {
		return NewNetworkTransportWithLogger(stream, maxPool, timeout, logger)
	})
}

// NewTCPTransportWithConfig 返回一个NetworkTransport，这个NetworkTransport是建立在TCP流传输层之上的。 使用提供的配置项
func NewTCPTransportWithConfig(bindAddr string, advertise net.Addr, config *NetworkTransportConfig) (*NetworkTransport, error) {
	return newTCPTransport(bindAddr, advertise, func(stream StreamLayer) *NetworkTransport {
		config.Stream = stream
		return NewNetworkTransportWithConfig(config)
	})
}

// 创建TCPStreamLayer，检验地址，调用transportCreator
func newTCPTransport(bindAddr string, advertise net.Addr, transportCreator func(stream StreamLayer) *NetworkTransport) (*NetworkTransport, error) {
	list, err := net.Listen("tcp", bindAddr) // 返回的是接口
	if err != nil {
		return nil, err
	}

	stream := &TCPStreamLayer{
		advertise: advertise,
		listener:  list.(*net.TCPListener), // 具体实现
	}

	// 验证我们是否有一个可用的地址
	addr, ok := stream.Addr().(*net.TCPAddr)
	if !ok {
		list.Close()
		return nil, errNotTCP
	}
	if addr.IP == nil || addr.IP.IsUnspecified() { // 不是能0.0.0.0 或 ::
		list.Close()
		log.Println("--------", addr)
		return nil, errNotAdvertisable
	}

	return transportCreator(stream), nil
}

// Dial 与ServerAddress建立连接
func (t *TCPStreamLayer) Dial(address ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}

// Accept 接收链接
func (t *TCPStreamLayer) Accept() (c net.Conn, err error) {
	return t.listener.Accept()
}

// Close 关闭listener
func (t *TCPStreamLayer) Close() (err error) {
	return t.listener.Close()
}

// Addr 返回程序绑定的地址
func (t *TCPStreamLayer) Addr() net.Addr {
	if t.advertise != nil {
		return t.advertise
	}
	return t.listener.Addr()
}
