package raft

import (
	"fmt"
	"io"
)

// SnapshotMeta is for metadata of a snapshot.
type SnapshotMeta struct {
	// Version is the version number of the snapshot metadata. This does not cover
	// the application's data in the snapshot, that should be versioned
	// separately.
	Version SnapshotVersion

	// ID is opaque to the store, and is used for opening.
	ID string

	// Index and Term store when the snapshot was taken.
	Index uint64
	Term  uint64

	// Peers is deprecated and used to support version 0 snapshots, but will
	// be populated in version 1 snapshots as well to help with upgrades.
	Peers []byte

	// Configuration and ConfigurationIndex are present in version 1
	// snapshots and later.
	Configuration      Configuration
	ConfigurationIndex uint64

	// Size is the size of the snapshot in bytes.
	Size int64
}

// SnapshotStore interface is used to allow for flexible implementations
// of snapshot storage and retrieval. For example, a client could implement
// a shared state store such as S3, allowing new nodes to restore snapshots
// without streaming from the leader.
type SnapshotStore interface {
	// Create is used to begin a snapshot at a given index and term, and with
	// the given committed configuration. The version parameter controls
	// which snapshot version to create.
	Create(version SnapshotVersion, index, term uint64, configuration Configuration,
		configurationIndex uint64, trans Transport) (SnapshotSink, error)

	// List 是用来列出商店中的可用快照。它应该按降序返回，以最高的索引为先。
	List() ([]*SnapshotMeta, error)

	// Open 接受一个快照ID并返回一个ReadCloser。一旦关闭被调用，就认为不再需要该快照了。
	Open(id string) (*SnapshotMeta, io.ReadCloser, error)
}

// SnapshotSink is returned by StartSnapshot. The FSM will Write state
// to the sink and call Close on completion. On error, Cancel will be invoked.
type SnapshotSink interface {
	io.WriteCloser
	ID() string
	Cancel() error
}

// runSnapshots 管理FSM打快照;它与FSM和主程序并行运行，因此快照不会阻碍正常操作。
func (r *Raft) runSnapshots() {
	for {
		select {
		case <-randomTimeout(r.config().SnapshotInterval):
			// 检查是否应该打快照
			if !r.shouldSnapshot() {
				continue
			}

			// 开始打快照
			if _, err := r.TakeSnapshot(); err != nil {
				r.logger.Error("打快照失败", "error", err)
			}

		case future := <-r.userSnapshotCh:
			id, err := r.TakeSnapshot()
			if err != nil {
				r.logger.Error("打快照失败", "error", err)
			} else {
				future.opener = func() (*SnapshotMeta, io.ReadCloser, error) {
					return r.snapshots.Open(id)
				}
			}
			future.respond(err)

		case <-r.shutdownCh:
			return
		}
	}
}

// shouldSnapshot checks if we meet the conditions to take
// a new snapshot.
func (r *Raft) shouldSnapshot() bool {
	// Check the last snapshot index
	lastSnap, _ := r.getLastSnapshot()

	// Check the last log index
	lastIdx, err := r.logs.LastIndex()
	if err != nil {
		r.logger.Error("failed to get last log index", "error", err)
		return false
	}

	// Compare the delta to the threshold
	delta := lastIdx - lastSnap
	return delta >= r.config().SnapshotThreshold
}

// TakeSnapshot 被用来生成一个新的快照。这必须只在快照线程中调用，而不是在主线程中。它返回新快照的ID，以及一个错误。
func (r *Raft) TakeSnapshot() (string, error) {

	// 为FSM创建一个执行快照的请求。
	snapReq := &reqSnapshotFuture{}
	snapReq.init()

	// 等待调度或关闭。
	select {
	case r.fsmSnapshotCh <- snapReq: // 容量为0,因此会阻塞
	case <-r.shutdownCh: // raft 停止
		return "", ErrRaftShutdown
	}

	// 等待，直到我们得到回应
	if err := snapReq.Error(); err != nil {
		if err != ErrNothingNewToSnapshot {
			err = fmt.Errorf("打快照失败: %v", err)
		}
		return "", err
	}
	defer snapReq.snapshot.Release()

	// 对配置提出请求，并提取承诺的信息。我们必须在这里使用future来安全地获得这些信息，因为它是由主线程拥有的。
	configReq := &configurationsFuture{}
	configReq.ShutdownCh = r.shutdownCh
	configReq.init()
	select {
	case r.configurationsCh <- configReq: // 获取配置，
	case <-r.shutdownCh: // raft 停止
		return "", ErrRaftShutdown
	}
	// 等待 xx 执行完，往errCh扔nil
	if err := configReq.Error(); err != nil {
		return "", err
	}
	committed := configReq.configurations.committed
	committedIndex := configReq.configurations.committedIndex

	// We don't support snapshots while there's a config change outstanding
	// since the snapshot doesn't have a means to represent this state. This
	// is a little weird because we need the FSM to apply an index that's
	// past the configuration change, even though the FSM itself doesn't see
	// the configuration changes. It should be ok in practice with normal
	// application traffic flowing through the FSM. If there's none of that
	// then it's not crucial that we snapshot, since there's not much going
	// on Raft-wise.
	if snapReq.index < committedIndex {
		return "", fmt.Errorf("cannot take snapshot now, wait until the configuration entry at %v has been applied (have applied %v)",
			committedIndex, snapReq.index)
	}

	// Create a new snapshot.
	r.logger.Info("starting snapshot up to", "index", snapReq.index)
	version := getSnapshotVersion(r.protocolVersion)
	sink, err := r.snapshots.Create(version, snapReq.index, snapReq.term, committed, committedIndex, r.trans)
	if err != nil {
		return "", fmt.Errorf("failed to create snapshot: %v", err)
	}

	// Try to persist the snapshot.
	if err := snapReq.snapshot.Persist(sink); err != nil {
		sink.Cancel()
		return "", fmt.Errorf("failed to persist snapshot: %v", err)
	}

	// Close and check for error.
	if err := sink.Close(); err != nil {
		return "", fmt.Errorf("failed to close snapshot: %v", err)
	}

	// Update the last stable snapshot info.
	r.setLastSnapshot(snapReq.index, snapReq.term)

	// Compact the logs.
	if err := r.compactLogs(snapReq.index); err != nil {
		return "", err
	}

	r.logger.Info("snapshot complete up to", "index", snapReq.index)
	return sink.ID(), nil
}

// compactLogs takes the last inclusive index of a snapshot
// and trims the logs that are no longer needed.
func (r *Raft) compactLogs(snapIdx uint64) error {
	// Determine log ranges to compact
	minLog, err := r.logs.FirstIndex()
	if err != nil {
		return fmt.Errorf("failed to get first log index: %v", err)
	}

	// Check if we have enough logs to truncate
	lastLogIdx, _ := r.getLastLog()

	// Use a consistent value for trailingLogs for the duration of this method
	// call to avoid surprising behaviour.
	trailingLogs := r.config().TrailingLogs
	if lastLogIdx <= trailingLogs {
		return nil
	}

	// Truncate up to the end of the snapshot, or `TrailingLogs`
	// back from the head, which ever is further back. This ensures
	// at least `TrailingLogs` entries, but does not allow logs
	// after the snapshot to be removed.
	maxLog := min(snapIdx, lastLogIdx-trailingLogs)

	if minLog > maxLog {
		r.logger.Info("no logs to truncate")
		return nil
	}

	r.logger.Info("compacting logs", "from", minLog, "to", maxLog)

	// Compact the logs
	if err := r.logs.DeleteRange(minLog, maxLog); err != nil {
		return fmt.Errorf("log compaction failed: %v", err)
	}
	return nil
}
