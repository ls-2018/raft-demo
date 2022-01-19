package raft

import (
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/go-hclog"
)

// ProtocolVersion
// 是该服务器能够_理解的协议版本（包括RPC消息以及Raft特定的日志条目）。
// 使用配置对象的ProtocolVersion成员来控制与其他服务器对话时要使用的协议版本。
// 请注意，根据所使用的协议版本，一些本来可以理解的RPC消息 可能会被拒绝。
// 关于这个逻辑的细节，请参见dispositionRPC。
//
// 在下面的版本描述中，有关于升级方式的说明。如果你正在启动一个新的集群，
// 那么没有理由不直接跳到最新的协议版本。如果你需要与旧的、0版本的Raft服务器进行互操作，你将需要通过不同的版本依次驱动集群。
//
// 版本的细节很复杂，但这里总结了从0版本的集群到3版本所需的内容。
// 1. 在你的应用程序的N个版本中，开始使用新的Raft库，并采用版本管理，将ProtocolVersion设置为1。
// 2. 使你的应用程序的N+1版本需要N版本作为先决条件（所有服务器必须升级）。对于你的应用程序的N+1版本，将ProtocolVersion设置为2。
// 3. 同样，使你的应用程序的N+2版本需要N+1版本作为先决条件。对于你的应用程序的N+2版本，将ProtocolVersion设置为3。
//
// 在这个升级过程中，老的集群成员仍然会有等于其网络地址的服务器ID。要升级一个老成员并给它一个ID，它需要离开集群并重新进入。
//
// 1. 使用RemoveServer将服务器从集群中移除，使用其网络地址作为其ServerID。
// 2. 更新服务器的配置，使用UUID或其他不与机器绑定的东西作为ServerID（重启服务器）。
// 3. 用AddVoter将服务器添加回集群，使用它的新ID。
//
// 你可以在你的应用程序从N+1到N+2的滚动升级过程中这样做，也可以在升级后的任何时候作为一个滚动变化。
//
// Version History
//
// 0: Original Raft library before versioning was added. Servers running this
//    version of the Raft library use AddPeerDeprecated/RemovePeerDeprecated
//    for all configuration changes, and have no support for LogConfiguration.
// 1: First versioned protocol, used to interoperate with old servers, and begin
//    the migration path to newer versions of the protocol. Under this version
//    all configuration changes are propagated using the now-deprecated
//    RemovePeerDeprecated Raft log entry. This means that server IDs are always
//    set to be the same as the server addresses (since the old log entry type
//    cannot transmit an ID), and only AddPeer/RemovePeer APIs are supported.
//    Servers running this version of the protocol can understand the new
//    LogConfiguration Raft log entry but will never generate one so they can
//    remain compatible with version 0 Raft servers in the cluster.
// 2: Transitional protocol used when migrating an existing cluster to the new
//    server ID system. Server IDs are still set to be the same as server
//    addresses, but all configuration changes are propagated using the new
//    LogConfiguration Raft log entry type, which can carry full ID information.
//    This version supports the old AddPeer/RemovePeer APIs as well as the new
//    ID-based AddVoter/RemoveServer APIs which should be used when adding
//    version 3 servers to the cluster later. This version sheds all
//    interoperability with version 0 servers, but can interoperate with newer
//    Raft servers running with protocol version 1 since they can understand the
//    new LogConfiguration Raft log entry, and this version can still understand
//    their RemovePeerDeprecated Raft log entries. We need this protocol version
//    as an intermediate step between 1 and 3 so that servers will propagate the
//    ID information that will come from newly-added (or -rolled) servers using
//    protocol version 3, but since they are still using their address-based IDs
//    from the previous step they will still be able to track commitments and
//    their own voting status properly. If we skipped this step, servers would
//    be started with their new IDs, but they wouldn't see themselves in the old
//    address-based configuration, so none of the servers would think they had a
//    vote.
// 3: Protocol adding full support for server IDs and new ID-based server APIs
//    (AddVoter, AddNonvoter, etc.), old AddPeer/RemovePeer APIs are no longer
//    supported. Version 2 servers should be swapped out by removing them from
//    the cluster one-by-one and re-adding them with updated configuration for
//    this protocol version, along with their server ID. The remove/add cycle
//    is required to populate their server ID. Note that removing must be done
//    by ID, which will be the old server's address.
type ProtocolVersion int

const (
	// ProtocolVersionMin 最小的协议版本
	ProtocolVersionMin ProtocolVersion = 0
	// ProtocolVersionMax 最大的协议版本
	ProtocolVersionMax = 3
)

// SnapshotVersion is the version of snapshots that this server can understand.
// Currently, it is always assumed that the server generates the latest version,
// though this may be changed in the future to include a configurable version.
//
// Version History
//
// 0: Original Raft library before versioning was added. The peers portion of
//    these snapshots is encoded in the legacy format which requires decodePeers
//    to parse. This version of snapshots should only be produced by the
//    unversioned Raft library.
// 1: New format which adds support for a full configuration structure and its
//    associated log index, with support for server IDs and non-voting server
//    modes. To ease upgrades, this also includes the legacy peers structure but
//    that will never be used by servers that understand version 1 snapshots.
//    Since the original Raft library didn't enforce any versioning, we must
//    include the legacy peers structure for this version, but we can deprecate
//    it in the next snapshot version.
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
	// flower <> candidate <> leader
	HeartbeatTimeout time.Duration

	// ElectionTimeout 规定了在我们试图进行选举之后，没有领导者的候选时间。
	ElectionTimeout time.Duration

	// CommitTimeout 控制在我们心跳之前没有Apply()操作的时间，以确保及时提交。由于随机交错，可能会延迟到这个值的2倍之多。
	CommitTimeout time.Duration

	// MaxAppendEntries controls the maximum number of append entries
	// to send at once. We want to strike a balance between efficiency
	// and avoiding waste if the follower is going to reject because of
	// an inconsistent log.
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

	// TrailingLogs controls how many logs we leave after a snapshot. This is used
	// so that we can quickly replay logs on a follower instead of being forced to
	// send an entire snapshot. The value passed here is the initial setting used.
	// This can be tuned during operation using ReloadConfig.
	TrailingLogs uint64

	// SnapshotInterval
	// 控制我们多长时间检查一次是否应该执行快照。我们在这个值和2倍这个值之间随机错开，以避免整个集群一次执行快照。
	// 这里传递的值是使用的初始设置。在操作过程中，可以用 ReloadConfig。
	SnapshotInterval time.Duration

	// SnapshotThreshold controls how many outstanding logs there must be before
	// we perform a snapshot. This is to prevent excessive snapshotting by
	// replaying a small set of logs instead. The value passed here is the initial
	// setting used. This can be tuned during operation using ReloadConfig.
	SnapshotThreshold uint64

	// LeaderLeaseTimeout is used to control how long the "lease" lasts
	// for being the leader without being able to contact a quorum
	// of nodes. If we reach this interval without contact, we will
	// step down as leader.
	LeaderLeaseTimeout time.Duration

	// LocalID 是该服务器在所有时间内的唯一ID。当使用ProtocolVersion < 3运行时，你必须将其设置为与传输的网络地址相同。
	LocalID ServerID

	// NotifyCh is used to provide a channel that will be notified of leadership
	// changes. Raft will block writing to this channel, so it should either be
	// buffered or aggressively consumed.
	NotifyCh chan<- bool

	// LogOutput 默认 os.Stderr.
	LogOutput io.Writer

	// LogLevel 日志登记  hclog.NoLevel
	LogLevel string

	// Logger
	Logger hclog.Logger

	// NoSnapshotRestoreOnStart
	// 控制raft是否会在启动时恢复快照到FSM。如果你的FSM是从raft快照以外的其他机制中恢复的，这就很有用。
	// 快照元数据仍将被用于初始化筏的配置和索引值。

	NoSnapshotRestoreOnStart bool

	// skipStartup 允许NewRaft()绕过所有的后台工作程序。
	skipStartup bool
}

// ReloadableConfig
// 是Config的子集，可以在运行时使用raft.ReloadConfig进行重新配置。
// 我们选择重复字段，而不是嵌入或接受一个Config，但只使用特定的字段来保持API的清晰。
// 重新配置一些字段有潜在的危险，所以我们应该只在允许的字段中选择性地启用它。
type ReloadableConfig struct {
	// TrailingLogs 控制我们在快照后留下多少日志。这是为了让我们能够快速地在跟flower上重放日志，而不是被迫发送整个快照。
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
		CommitTimeout:      50 * time.Millisecond,   // 50ms
		MaxAppendEntries:   64,
		ShutdownOnRemove:   true,
		TrailingLogs:       10240,
		SnapshotInterval:   120 * time.Second,
		SnapshotThreshold:  8192,
		LeaderLeaseTimeout: 500 * time.Millisecond, //50ms  租约超时
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
	// flower 进入候选的时间间隔
	if config.HeartbeatTimeout < 5*time.Millisecond {
		return fmt.Errorf("flower 进入候选的时间间隔 is too low")
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
