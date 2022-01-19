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

	// N.B. It is _up to the client_ to handle upgrade paths. For instance if
	// using this with go-raftchunking, the client should ensure that all Raft
	// peers are using a version that can handle that extension before ever
	// actually triggering chunking behavior. It is sometimes sufficient to
	// ensure that non-leaders are upgraded first, then the current leader is
	// upgraded, but a leader changeover during this process could lead to
	// trouble, so gating extension behavior via some flag in the client
	// program is also a good idea.
	Extensions []byte

	// AppendedAt stores the time the leader first appended this log to it's
	// LogStore. Followers will observe the leader's time. It is not used for
	// coordination or as part of the replication protocol at all. It exists only
	// to provide operational information for example how many seconds worth of
	// logs are present on the leader which might impact follower's ability to
	// catch up after restoring a large snapshot. We should never rely on this
	// being in the past when appending on a follower or reading a log back since
	// the clock skew can mean a follower could see a log with a future timestamp.
	// In general too the leader is not required to persist the log before
	// delivering to followers although the current implementation happens to do
	// this.
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

func oldestLog(s LogStore) (Log, error) {
	var l Log

	// We might get unlucky and have a truncate right between getting first log
	// index and fetching it so keep trying until we succeed or hard fail.
	var lastFailIdx uint64
	var lastErr error
	for {
		firstIdx, err := s.FirstIndex()
		if err != nil {
			return l, err
		}
		if firstIdx == 0 {
			return l, ErrLogNotFound
		}
		if firstIdx == lastFailIdx {
			// Got same index as last time around which errored, don't bother trying
			// to fetch it again just return the error.
			return l, lastErr
		}
		err = s.GetLog(firstIdx, &l)
		if err == nil {
			// We found the oldest log, break the loop
			break
		}
		// We failed, keep trying to see if there is a new firstIndex
		lastFailIdx = firstIdx
		lastErr = err
	}
	return l, nil
}

// 发送日志存储指标
func emitLogStoreMetrics(s LogStore, prefix []string, interval time.Duration, stopCh <-chan struct{}) {
	for {
		select {
		case <-time.After(interval):
			// 在错误情况下，发出0作为年龄
			oldestLog(s)
		case <-stopCh:
			return
		}
	}
}
