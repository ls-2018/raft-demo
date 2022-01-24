package raft

import (
	"fmt"
	"io"
)

// SnapshotMeta 快照元数据
type SnapshotMeta struct {
	Version SnapshotVersion

	// ID 快照的唯一标识、文件夹的名字
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

	// Size 快照有多少字节
	Size int64
}

// SnapshotStore 接口用于灵活实现快照存储和检索。
// 例如，客户端可以实现一个共享状态存储(如S3)，允许新节点恢复快照，而无需从leader流。
type SnapshotStore interface {
	Create(version SnapshotVersion, index, term uint64, configuration Configuration, configurationIndex uint64, trans Transport) (SnapshotSink, error)

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

// compactLogs  获取快照的最后一个日志索引，并删除不再需要的日志。
// 快照恢复、打快照时 会调用此函数
func (r *Raft) compactLogs(snapIdx uint64) error {
	// Index 日志当前存储到的位置
	// 确定要压缩的日志范围
	minLog, err := r.logs.FirstIndex() // bolt log db
	if err != nil {
		return fmt.Errorf("获取第一个日志索引失败: %v", err)
	}

	//检查我们是否有足够的日志来截断
	lastLogIdx, _ := r.getLastLog() // 200

	// 落后的日志
	// TODO
	trailingLogs := r.config().TrailingLogs // 100
	if lastLogIdx <= trailingLogs {
		return nil
	}
	// 快照索引是100
	// 但是blot db存储的日志可能是200

	maxLog := min(snapIdx, lastLogIdx-trailingLogs)

	if minLog > maxLog {
		r.logger.Info("没有要截断的日志")
		return nil
	}

	r.logger.Info("压缩日志", "from", minLog, "to", maxLog)

	// 压缩日志
	if err := r.logs.DeleteRange(minLog, maxLog); err != nil {
		return fmt.Errorf("日志压缩失败: %v", err)
	}
	return nil
}
