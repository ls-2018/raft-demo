package raft

import (
	"fmt"
	"time"

	metrics "github.com/armon/go-metrics"
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

	// LogConfiguration establishes a membership change configuration. It is
	// created when a server is added, removed, promoted, etc. Only used
	// when protocol version 1 or greater is in use.
	LogConfiguration
)

// String returns LogType as a human readable string.
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

// Log entries are replicated to all members of the Raft cluster
// and form the heart of the replicated state machine.
type Log struct {
	// Index holds the index of the log entry.
	Index uint64

	// Term holds the election term of the log entry.
	Term uint64

	// Type holds the type of the log entry.
	Type LogType

	// Data holds the log entry's type-specific data.
	Data []byte

	// Extensions holds an opaque byte slice of information for middleware. It
	// is up to the client of the library to properly modify this as it adds
	// layers and remove those layers when appropriate. This value is a part of
	// the log, so very large values could cause timing issues.
	//
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

// LogStore is used to provide an interface for storing
// and retrieving logs in a durable fashion.
type LogStore interface {
	// FirstIndex returns the first index written. 0 for no entries.
	FirstIndex() (uint64, error)

	// LastIndex returns the last index written. 0 for no entries.
	LastIndex() (uint64, error)

	// GetLog gets a log entry at a given index.
	GetLog(index uint64, log *Log) error

	// StoreLog stores a log entry.
	StoreLog(log *Log) error

	// StoreLogs stores multiple log entries.
	StoreLogs(logs []*Log) error

	// DeleteRange deletes a range of log entries. The range is inclusive.
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

func emitLogStoreMetrics(s LogStore, prefix []string, interval time.Duration, stopCh <-chan struct{}) {
	for {
		select {
		case <-time.After(interval):
			// In error case emit 0 as the age
			ageMs := float32(0.0)
			l, err := oldestLog(s)
			if err == nil && !l.AppendedAt.IsZero() {
				ageMs = float32(time.Since(l.AppendedAt).Milliseconds())
			}
			metrics.SetGauge(append(prefix, "oldestLogAge"), ageMs)
		case <-stopCh:
			return
		}
	}
}
