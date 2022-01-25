package raft

import (
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/go-hclog"
)

type ProtocolVersion int

const (
	// ProtocolVersionMin 最小的协议版本
	ProtocolVersionMin ProtocolVersion = 0
	// ProtocolVersionMax 最大的协议版本
	ProtocolVersionMax = 3
)

type SnapshotVersion int

const (
	// SnapshotVersionMin 快照最小版本
	SnapshotVersionMin SnapshotVersion = 0
	// SnapshotVersionMax 快照最大版本
	SnapshotVersionMax = 1
)

// Config 为Raft服务器提供任何必要的配置。
type Config struct {
	// ProtocolVersion 允许Raft服务器与运行旧版本代码的Raft服务器进行互操作。
	// 这被用来对电报协议以及服务器在与其他服务器对话时使用的Raft特定的日志条目进行版本管理。
	// 目前没有自动协商的版本，所以所有服务器必须手动配置兼容的版本。
	// 参见ProtocolVersionMin和 ProtocolVersionMax，了解该服务器可以理解的协议版本。
	ProtocolVersion ProtocolVersion

	// HeartbeatTimeout 指定在没有领导者的情况下处于跟随者状态的时间
	// 在我们试图进行选举之前，处于没有领导者的追随者状态的时间。
	// follower <> candidate <> leader
	HeartbeatTimeout time.Duration

	// ElectionTimeout 规定了在我们试图进行选举之后，没有领导者的候选时间。
	ElectionTimeout time.Duration

	// CommitTimeout 控制在我们心跳之前没有Apply()操作的时间，以确保及时提交。由于随机交错，可能会延迟到这个值的2倍之多。
	CommitTimeout time.Duration

	// MaxAppendEntries 控制一次性发送的最大追加日志数。我们想在效率和避免浪费之间取得平衡，如果follower要因为不一致的日志而拒绝的话。
	MaxAppendEntries int

	// BatchApplyCh 表示我们是否应该将applyCh缓冲到MaxAppendEntries大小。
	// 这可以实现批量的日志承诺，但会破坏Apply的超时保证。
	// 具体来说，一个日志可以被添加到applyCh的缓冲区，但实际上直到指定的超时后才会被处理。
	BatchApplyCh bool

	// If we are a member of a cluster, and RemovePeer is invoked for the
	// local node, then we forget all peers and transition into the follower state.
	// If ShutdownOnRemove is set, we additional shutdown Raft. Otherwise,
	// we can become a leader of a cluster containing only this node.
	ShutdownOnRemove bool

	// TrailingLogs 控制快照后留下多少日志。这样我们就可以在一个Follower上快速重放日志，而不是被迫发送一个完整的快照。
	// 这里传递的值是使用的初始设置。这可以在操作期间使用ReloadConfig进行调优。
	TrailingLogs uint64

	// SnapshotInterval
	// 控制我们多长时间检查一次是否应该执行快照。我们在这个值和2倍这个值之间随机错开，以避免整个集群一次执行快照。
	// 这里传递的值是使用的初始设置。在操作过程中，可以用 ReloadConfig。
	SnapshotInterval time.Duration

	// SnapshotThreshold
	// 控制在执行快照之前必须有多少未完成的日志。这是为了通过重放一小组日志来防止过度的快照。
	// 这里传递的值是使用的初始设置。这可以在操作期间使用ReloadConfig进行调优。
	SnapshotThreshold uint64 // 8192 条数据

	// LeaderLeaseTimeout
	// 用于控制作为leader而不能联系节点的quorum的“租约”持续多长时间。如果我们到达这段时间没有联系，我们将辞去领导职务。
	LeaderLeaseTimeout time.Duration

	// LocalID 是该服务器在所有时间内的唯一ID。当使用ProtocolVersion < 3运行时，你必须将其设置为与传输的网络地址相同。
	LocalID ServerID

	// NotifyCh 是用来提供一个通知领导层变化的通道。Raft将阻止对该通道的写入，所以它应该被缓冲或积极地消耗。
	NotifyCh chan<- bool

	// LogOutput 默认 os.Stderr.
	LogOutput io.Writer

	// LogLevel 日志登记  hclog.NoLevel
	LogLevel string

	// Logger
	Logger hclog.Logger

	// NoSnapshotRestoreOnStart
	// 控制raft是否会在启动时恢复快照到FSM。如果你的FSM是从raft快照以外的其他机制中恢复的，这就很有用。
	// 快照元数据仍将被用于初始化raft的配置和索引值。
	NoSnapshotRestoreOnStart bool

	// skipStartup 允许NewRaft()绕过所有的后台工作程序。
	skipStartup bool
}

// ReloadableConfig
// 是Config的子集，可以在运行时使用raft.ReloadConfig进行重新配置。
// 我们选择重复字段，而不是嵌入或接受一个Config，但只使用特定的字段来保持API的清晰。
// 重新配置一些字段有潜在的危险，所以我们应该只在允许的字段中选择性地启用它。
type ReloadableConfig struct {
	// TrailingLogs 控制我们在快照后留下多少日志。这是为了让我们能够快速地在跟follower上重放日志，而不是被迫发送整个快照。
	// 这里传递的值会在运行时更新设置，一旦下一个快照完成并发生截断，就会生效。
	TrailingLogs uint64

	// 控制我们多长时间检查一次是否应该执行快照。我们在这个值和2倍这个值之间随机错开，以避免整个集群一次执行快照。
	SnapshotInterval time.Duration

	// SnapshotThreshold 控制在我们执行快照之前必须有多少未完成的日志。这是为了防止在我们只需重放一小部分日志的情况下出现过多的快照。
	SnapshotThreshold uint64
}

// apply 将传递的Config上的可重载字段设置为`ReloadableConfig`中的值。它返回一个Config的副本，其中包含ReloadableConfig的字段。
func (rc *ReloadableConfig) apply(old Config) Config {
	old.TrailingLogs = rc.TrailingLogs
	old.SnapshotInterval = rc.SnapshotInterval
	old.SnapshotThreshold = rc.SnapshotThreshold
	return old
}

// fromConfig 从传递的配置中复制可重载的字段。
func (rc *ReloadableConfig) fromConfig(from Config) {
	rc.TrailingLogs = from.TrailingLogs
	rc.SnapshotInterval = from.SnapshotInterval
	rc.SnapshotThreshold = from.SnapshotThreshold
}

// DefaultConfig 返回一个带有可用默认值的配置。
func DefaultConfig() *Config {
	return &Config{
		ProtocolVersion:    ProtocolVersionMax,      // 3
		HeartbeatTimeout:   1000 * time.Millisecond, // 1s
		ElectionTimeout:    1000 * time.Millisecond, // 1s
		CommitTimeout:      1000 * time.Second,   // 50ms   in order to test
		MaxAppendEntries:   64,
		ShutdownOnRemove:   true,
		TrailingLogs:       10240,
		SnapshotInterval:   120 * time.Second,
		SnapshotThreshold:  8192,
		LeaderLeaseTimeout: 500 * time.Millisecond, //500ms  租约超时
		LogLevel:           "DEBUG",
	}
}

// ValidateConfig 是用来验证一个合理的配置
func ValidateConfig(config *Config) error {
	// 我们实际上不再支持在库中以协议版本0的形式运行，但我们确实理解它。
	protocolMin := ProtocolVersionMin
	if protocolMin == 0 {
		protocolMin = 1
	}
	if config.ProtocolVersion < protocolMin || config.ProtocolVersion > ProtocolVersionMax {
		return fmt.Errorf("协议版本 %d must be >= %d and <= %d",
			config.ProtocolVersion, protocolMin, ProtocolVersionMax)
	}
	if len(config.LocalID) == 0 {
		return fmt.Errorf("LocalID cannot be empty")
	}
	// follower 进入候选的时间间隔
	if config.HeartbeatTimeout < 5*time.Millisecond {
		return fmt.Errorf("follower 进入候选的时间间隔 is too low")
	}
	// 候选超时
	if config.ElectionTimeout < 5*time.Millisecond {
		return fmt.Errorf("候选超时 is too low")
	}
	// 日志提交超时
	if config.CommitTimeout < time.Millisecond {
		return fmt.Errorf("日志提交超时 is too low")
	}
	// 最大的日志条目数
	if config.MaxAppendEntries <= 0 {
		return fmt.Errorf("最大的日志条目数必须>0")
	}
	if config.MaxAppendEntries > 1024 {
		return fmt.Errorf("最大的日志条目数必须 <1024")
	}
	// 快照检查间隔
	if config.SnapshotInterval < 5*time.Millisecond {
		return fmt.Errorf("快照检查间隔 is too low")
	}
	// leader 发送心跳的超时时间
	if config.LeaderLeaseTimeout < 5*time.Millisecond {
		return fmt.Errorf("LeaderLeaseTimeout is too low")
	}
	if config.LeaderLeaseTimeout > config.HeartbeatTimeout {
		return fmt.Errorf("LeaderLeaseTimeout不能大于心跳超时。")
	}
	// 选举超时
	if config.ElectionTimeout < config.HeartbeatTimeout {
		return fmt.Errorf("ElectionTimeout必须等于或大于Heartbeat Timeout")
	}
	return nil
}
