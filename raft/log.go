package raft

import (
	"fmt"
	"time"
)

// LogType 描述了各种类型的日志条目。
type LogType uint8

const (
	// LogCommand 应用于用户的FSM  最终状态机
	LogCommand LogType = iota

	// LogNoop 是用来宣示领导地位的。
	LogNoop

	// LogAddPeerDeprecated
	// 用来添加一个新的node。这应该只用于旧的协议版本，旨在与未版本的Raft服务器兼容。详见config.go中的注释。
	LogAddPeerDeprecated

	// LogRemovePeerDeprecated
	//用来删除一个现有的node。这应该只用于较早的协议版本，旨在与没有版本的Raft服务器兼容。详见config.go中的注释。
	LogRemovePeerDeprecated

	// LogBarrier 是用来确保所有前面的操作都被应用到FSM中。它类似于LogNoop，但是它不是一旦提交就返回，
	//而是只在FSM管理器捕获它时返回。否则就有可能存在已提交但尚未应用到FSM的操作。
	LogBarrier

	// LogConfiguration
	// 建立一个成员变更配置。它是在一个服务器被添加、移除、晋升等情况下创建的。
	// 只在使用协议1或更高版本时使用。当协议版本1或更高版本正在使用时。

	LogConfiguration
)

// String
// 返回人类可读的日志类型
func (lt LogType) String() string {
	switch lt {
	case LogCommand:
		return "LogCommand"
	case LogNoop:
		return "LogNoop"
	case LogAddPeerDeprecated:
		return "LogAddPeerDeprecated"
	case LogRemovePeerDeprecated:
		return "LogRemovePeerDeprecated"
	case LogBarrier:
		return "LogBarrier"
	case LogConfiguration:
		return "LogConfiguration"
	default:
		return fmt.Sprintf("%d", lt)
	}
}

// Log 条目被复制到Raft集群的所有成员中,构成了复制状态机的核心。
type Log struct {
	// Index 日志条目的索引
	Index uint64

	// Term 当前日志所处的任期
	Term uint64

	// Type 日志类型
	Type LogType

	// Data 持有日志条目的特定类型的数据。
	Data []byte

	// Extensions
	// 持有一个不透明的字节切片，用于中间件的信息。
	// 这库的客户端在添加、删除layer 时对其进行适当的修改。
	// 这个值是日志的一部分，所以非常大的值可能会导致时间问题。
	Extensions []byte
	AppendedAt time.Time
}

// LogStore
// 以持久化的方式存储并检索日志 接口
type LogStore interface {
	// FirstIndex 返回第一个写入的索引。0表示没有条目。
	FirstIndex() (uint64, error)

	// LastIndex 返回最新写入的索引。0表示没有条目。
	LastIndex() (uint64, error)

	// GetLog 获得一个给定索引的日志条目。
	GetLog(index uint64, log *Log) error

	// StoreLog 存储日志条目
	StoreLog(log *Log) error

	// StoreLogs 存储多个日志条目
	StoreLogs(logs []*Log) error

	// DeleteRange 删除一个范围的日志条目。该范围是包括在内的。
	DeleteRange(min, max uint64) error
}

// StableStore 是用来为关键配置提供稳定的存储，以确保安全。
type StableStore interface {
	Set(key []byte, val []byte) error
	// Get 如果没找到返回空切片
	Get(key []byte) ([]byte, error)
	SetUint64(key []byte, val uint64) error
	// GetUint64 如果没找到则返回0
	GetUint64(key []byte) (uint64, error)
}
