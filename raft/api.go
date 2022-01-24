package raft

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	hclog "github.com/hashicorp/go-hclog"
)

const (
	// SuggestedMaxDataSize
	// 这是目前建议的raft日志条目中数据的最大值。这是基于当前的架构、默认时间等。
	// 客户可以忽略这个值，如果他们想的话，因为库内没有实际的硬检查。
	// 随着库的增强，这个值可能会随着时间的推移而改变，以反映当前建议的最大值。
	//
	// 如果增加到超过这个数值，就有可能使RPC IO耗时过长，并妨碍及时发出心跳信号，
	// 而这些信号在目前的传输系统中是以串行方式发送的，有可能造成leader的不稳定。
	SuggestedMaxDataSize = 512 * 1024
)

var (
	// ErrLeader is returned when an operation can't be completed on a
	// leader node.
	ErrLeader = errors.New("node is the leader")

	// ErrNotLeader is returned when an operation can't be completed on a
	// follower or candidate node.
	ErrNotLeader = errors.New("node is not the leader")

	// ErrLeadershipLost is returned when a leader fails to commit a log entry
	// because it's been deposed in the process.
	ErrLeadershipLost = errors.New("leadership lost while committing log")

	// ErrAbortedByRestore is returned when a leader fails to commit a log
	// entry because it's been superseded by a user snapshot restore.
	ErrAbortedByRestore = errors.New("snapshot restored while committing log")

	// ErrRaftShutdown 当请求对一个不活动的raft进行操作时，将返回。
	ErrRaftShutdown = errors.New("raft已经关闭了")

	// ErrEnqueueTimeout is returned when a command fails due to a timeout.
	ErrEnqueueTimeout = errors.New("timed out enqueuing operation")

	// ErrNothingNewToSnapshot 当试图创建一个快照时返回，但自从我们开始以来，没有任何新的东西被输入到FSM。
	ErrNothingNewToSnapshot = errors.New("没有新的数据进行打快照")

	// ErrUnsupportedProtocol is returned when an operation is attempted
	// that's not supported by the current protocol version.
	ErrUnsupportedProtocol = errors.New("operation not supported with current protocol version")

	// ErrCantBootstrap is returned when attempt is made to bootstrap a
	// cluster that already has state present.
	ErrCantBootstrap = errors.New("bootstrap only works on new clusters")

	// ErrLeadershipTransferInProgress is returned when the leader is rejecting
	// client requests because it is attempting to transfer leadership.
	ErrLeadershipTransferInProgress = errors.New("leadership transfer in progress")
)

// Raft raft 节点
type Raft struct {
	raftState
	// stable 是一个用于持久状态的StableStore实现。它为raftState中的许多字段提供稳定的存储。 k/v数据库
	stable StableStore
	// protocolVersion raft节点之间通信的协议版本
	protocolVersion ProtocolVersion

	// applyCh 是用来异步发送日志到主线程，以便被提交并应用到FSM中。
	applyCh chan *logFuture // raft 节点间同步channel

	// conf 存储当前要使用的配置。这是所提供的最新的配置。所有对配置值的读取都应该使用config()辅助方法来安全读取。
	conf atomic.Value

	// confReloadMu 确保同时只有一个工作线程可以重新加载congfig
	confReloadMu sync.Mutex

	// FSM 客户端状态机应该实现的接口
	fsm FSM

	// fsmMutateCh 用来向FSM发送状态变化的更新。
	// 在应用日志时，它接收指向commitTuple结构的指针;
	// 在恢复快照时接收指向restoreFuture结构的指针。
	fsmMutateCh chan interface{}
	// r.installSnapshot

	// fsmSnapshotCh 用来触发新的快照拍摄。
	fsmSnapshotCh chan *reqSnapshotFuture

	// lastContact 是我们最后一次与leader节点通信的时间。这可以用来衡量呆滞性。
	lastContact     time.Time // 默认是零值，即0001-01-01 00:00:00 +0000
	lastContactLock sync.RWMutex

	// Leader 当前的集群leader地址
	leader     ServerAddress
	leaderLock sync.RWMutex

	// leaderCh 用来通知leader的变化
	leaderCh chan bool

	// leaderState 只有 leader 是有此值
	leaderState leaderState

	// candidateFromLeadershipTransfer 表示  leader -> candidate。这个标志在RequestVoteRequest中被用来表示正在进行leader的转移。进行中。
	candidateFromLeadershipTransfer bool

	// 服务器的逻辑ID 存储我们的本地服务器ID，用于避免向我们自己发送RPC。
	localID ServerID

	// 本机地址  ip:port
	localAddr ServerAddress

	logger hclog.Logger

	// LogStore 日志存储空间
	logs LogStore

	// 用来记录leader 进行配置修改。
	configurationChangeCh chan *configurationChangeFuture

	// 追踪日志/快照中的最新配置和最新提交的配置。
	configurations configurations

	// 保存一份最新配置的副本，可以从主循环中独立读取。
	latestConfiguration atomic.Value

	// RPC chan来自于传输层
	rpcCh <-chan RPC

	// Shutdown 退出的通道，保护以防止同时退出
	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	// snapshots 存储和恢复快照数据
	snapshots SnapshotStore

	// userSnapshotCh // 用户可以主动触发打快照操作
	userSnapshotCh chan *userSnapshotFuture

	// userRestoreCh 用于用户触发的快照
	userRestoreCh chan *userRestoreFuture

	// 使用的传输层
	trans Transport

	// verifyCh  用于向主线程异步发送验证请求，以验证我们仍然是leader者。
	verifyCh chan *verifyFuture

	// configurationsCh 安全的从主线程之外获取配置
	configurationsCh chan *configurationsFuture

	// bootstrapCh 是用来尝试从主线程之外进行初始引导
	bootstrapCh chan *bootstrapFuture

	// leadershipTransferCh 是用来从主线程之外启动leader转移的。
	leadershipTransferCh chan *leadershipTransferFuture
}

// BootstrapCluster initializes a server's storage with the given cluster
// configuration. This should only be called at the beginning of time for the
// cluster with an identical configuration listing all Voter servers. There is
// no need to bootstrap Nonvoter and Staging servers.
//
// A cluster can only be bootstrapped once from a single participating Voter
// server. Any further attempts to bootstrap will return an error that can be
// safely ignored.
//
// One approach is to bootstrap a single server with a configuration
// listing just itself as a Voter, then invoke AddVoter() on it to add other
// servers to the cluster.
func BootstrapCluster(conf *Config, logs LogStore, stable StableStore, snaps SnapshotStore, trans Transport, configuration Configuration) error {
	// Validate the Raft server config.
	if err := ValidateConfig(conf); err != nil {
		return err
	}

	// Sanity check the Raft peer configuration.
	if err := checkConfiguration(configuration); err != nil {
		return err
	}

	// Make sure the cluster is in a clean state.
	hasState, err := HasExistingState(logs, stable, snaps)
	if err != nil {
		return fmt.Errorf("failed to check for existing state: %v", err)
	}
	if hasState {
		return ErrCantBootstrap
	}

	// Set current term to 1.
	if err := stable.SetUint64(keyCurrentTerm, 1); err != nil {
		return fmt.Errorf("failed to save current term: %v", err)
	}

	// Append configuration entry to log.
	entry := &Log{
		Index: 1,
		Term:  1,
	}
	if conf.ProtocolVersion < 3 {
		entry.Type = LogRemovePeerDeprecated
		entry.Data = encodePeers(configuration, trans)
	} else {
		entry.Type = LogConfiguration
		entry.Data = EncodeConfiguration(configuration)
	}
	if err := logs.StoreLog(entry); err != nil {
		return fmt.Errorf("failed to append configuration entry to log: %v", err)
	}

	return nil
}

// RecoverCluster is used to manually force a new configuration in order to
// recover from a loss of quorum where the current configuration cannot be
// restored, such as when several servers die at the same time. This works by
// reading all the current state for this server, creating a snapshot with the
// supplied configuration, and then truncating the Raft log. This is the only
// safe way to force a given configuration without actually altering the log to
// insert any new entries, which could cause conflicts with other servers with
// different state.
//
// WARNING! This operation implicitly commits all entries in the Raft log, so
// in general this is an extremely unsafe operation. If you've lost your other
// servers and are performing a manual recovery, then you've also lost the
// commit information, so this is likely the best you can do, but you should be
// aware that calling this can cause Raft log entries that were in the process
// of being replicated but not yet be committed to be committed.
//
// Note the FSM passed here is used for the snapshot operations and will be
// left in a state that should not be used by the application. Be sure to
// discard this FSM and any associated state and provide a fresh one when
// calling NewRaft later.
//
// A typical way to recover the cluster is to shut down all servers and then
// run RecoverCluster on every server using an identical configuration. When
// the cluster is then restarted, and election should occur and then Raft will
// resume normal operation. If it's desired to make a particular server the
// leader, this can be used to inject a new configuration with that server as
// the sole voter, and then join up other new clean-state peer servers using
// the usual APIs in order to bring the cluster back into a known state.
func RecoverCluster(conf *Config, fsm FSM, logs LogStore, stable StableStore,
	snaps SnapshotStore, trans Transport, configuration Configuration) error {
	// Validate the Raft server config.
	if err := ValidateConfig(conf); err != nil {
		return err
	}

	// Sanity check the Raft peer configuration.
	if err := checkConfiguration(configuration); err != nil {
		return err
	}

	// Refuse to recover if there's no existing state. This would be safe to
	// do, but it is likely an indication of an operator error where they
	// expect data to be there and it's not. By refusing, we force them
	// to show intent to start a cluster fresh by explicitly doing a
	// bootstrap, rather than quietly fire up a fresh cluster here.
	if hasState, err := HasExistingState(logs, stable, snaps); err != nil {
		return fmt.Errorf("failed to check for existing state: %v", err)
	} else if !hasState {
		return fmt.Errorf("refused to recover cluster with no initial state, this is probably an operator error")
	}

	// Attempt to restore any snapshots we find, newest to oldest.
	var (
		snapshotIndex  uint64
		snapshotTerm   uint64
		snapshots, err = snaps.List()
	)
	if err != nil {
		return fmt.Errorf("failed to list snapshots: %v", err)
	}
	for _, snapshot := range snapshots {
		var source io.ReadCloser
		_, source, err = snaps.Open(snapshot.ID)
		if err != nil {
			// Skip this one and try the next. We will detect if we
			// couldn't open any snapshots.
			continue
		}

		// Note this is the one place we call fsm.Restore without the
		// fsmRestoreAndMeasure wrapper since this function should only be called to
		// reset state on disk and the FSM passed will not be used for a running
		// server instance. If the same process will eventually become a Raft peer
		// then it will call NewRaft and restore again from disk then which will
		// report metrics.
		err = fsm.Restore(source)
		// Close the source after the restore has completed
		source.Close()
		if err != nil {
			// Same here, skip and try the next one.
			continue
		}

		snapshotIndex = snapshot.Index
		snapshotTerm = snapshot.Term
		break
	}
	if len(snapshots) > 0 && (snapshotIndex == 0 || snapshotTerm == 0) {
		return fmt.Errorf("failed to restore any of the available snapshots")
	}

	// The snapshot information is the best known end point for the data
	// until we play back the Raft log entries.
	lastIndex := snapshotIndex
	lastTerm := snapshotTerm

	// Apply any Raft log entries past the snapshot.
	lastLogIndex, err := logs.LastIndex()
	if err != nil {
		return fmt.Errorf("failed to find last log: %v", err)
	}
	for index := snapshotIndex + 1; index <= lastLogIndex; index++ {
		var entry Log
		if err = logs.GetLog(index, &entry); err != nil {
			return fmt.Errorf("failed to get log at index %d: %v", index, err)
		}
		if entry.Type == LogCommand {
			_ = fsm.Apply(&entry)
		}
		lastIndex = entry.Index
		lastTerm = entry.Term
	}

	// Create a new snapshot, placing the configuration in as if it was
	// committed at index 1.
	snapshot, err := fsm.Snapshot()
	if err != nil {
		return fmt.Errorf("failed to snapshot FSM: %v", err)
	}
	version := getSnapshotVersion(conf.ProtocolVersion)
	sink, err := snaps.Create(version, lastIndex, lastTerm, configuration, 1, trans)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %v", err)
	}
	if err = snapshot.Persist(sink); err != nil {
		return fmt.Errorf("failed to persist snapshot: %v", err)
	}
	if err = sink.Close(); err != nil {
		return fmt.Errorf("failed to finalize snapshot: %v", err)
	}

	// Compact the log so that we don't get bad interference from any
	// configuration change log entries that might be there.
	firstLogIndex, err := logs.FirstIndex()
	if err != nil {
		return fmt.Errorf("failed to get first log index: %v", err)
	}
	if err := logs.DeleteRange(firstLogIndex, lastLogIndex); err != nil {
		return fmt.Errorf("log compaction failed: %v", err)
	}

	return nil
}

// GetConfiguration returns the persisted configuration of the Raft cluster
// without starting a Raft instance or connecting to the cluster. This function
// has identical behavior to Raft.GetConfiguration.
func GetConfiguration(conf *Config, fsm FSM, logs LogStore, stable StableStore, snaps SnapshotStore, trans Transport) (Configuration, error) {
	conf.skipStartup = true
	r, err := NewRaft(conf, fsm, logs, stable, snaps, trans)
	if err != nil {
		return Configuration{}, err
	}
	future := r.GetConfiguration()
	if err = future.Error(); err != nil {
		return Configuration{}, err
	}
	return future.Configuration(), nil
}

// HasExistingState returns true if the server has any existing state (logs,
// knowledge of a current term, or any snapshots).
func HasExistingState(logs LogStore, stable StableStore, snaps SnapshotStore) (bool, error) {
	// Make sure we don't have a current term.
	currentTerm, err := stable.GetUint64(keyCurrentTerm)
	if err == nil {
		if currentTerm > 0 {
			return true, nil
		}
	} else {
		if err.Error() != "not found" {
			return false, fmt.Errorf("failed to read current term: %v", err)
		}
	}

	// Make sure we have an empty log.
	lastIndex, err := logs.LastIndex()
	if err != nil {
		return false, fmt.Errorf("failed to get last log index: %v", err)
	}
	if lastIndex > 0 {
		return true, nil
	}

	// Make sure we have no snapshots
	snapshots, err := snaps.List()
	if err != nil {
		return false, fmt.Errorf("failed to list snapshots: %v", err)
	}
	if len(snapshots) > 0 {
		return true, nil
	}

	return false, nil
}

// NewRaft 用于构建一个新的Raft节点。它需要一个配置，以及所需的各种接口的实现。
// 如果我们有任何旧的状态，如快照、日志、对等体等，所有这些将在创建Raft节点时被恢复。
func NewRaft(conf *Config, fsm FSM, logs LogStore, stable StableStore, snaps SnapshotStore, trans Transport) (*Raft, error) {
	// 验证配置
	if err := ValidateConfig(conf); err != nil {
		return nil, err
	}

	// Ensure we have a LogOutput.
	var logger hclog.Logger
	if conf.Logger != nil {
		logger = conf.Logger
	} else {
		if conf.LogOutput == nil {
			conf.LogOutput = os.Stderr
		}

		logger = hclog.New(&hclog.LoggerOptions{
			Name:   "raft",
			Level:  hclog.LevelFromString(conf.LogLevel),
			Output: conf.LogOutput,
		})
	}

	// 尝试重置当前任期
	currentTerm, err := stable.GetUint64(keyCurrentTerm)
	if err != nil && err != ErrKeyNotFound {
		return nil, fmt.Errorf("获取当前任期失败: %v", err)
	}

	// 读取最后一条日志记录的索引。
	lastIndex, err := logs.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("读取最后一条日志记录的索引。: %v", err)
	}

	// 获取最新的日志条目
	var lastLog Log // logState最新的数据
	if lastIndex > 0 {
		if err = logs.GetLog(lastIndex, &lastLog); err != nil {
			return nil, fmt.Errorf("获取最新的日志条目失败 at index %d: %v", lastIndex, err)
		}
	}
	// 确保我们有一个有效的服务器地址和ID。
	protocolVersion := conf.ProtocolVersion
	localAddr := trans.LocalAddr() // 127.0.0.1:10000
	localID := conf.LocalID        // 逻辑的服务器ID  main.go:30

	if protocolVersion < 3 && string(localID) != string(localAddr) {
		return nil, fmt.Errorf("当协议版本<3，逻辑ID应与实际的ip:port相等")
	}

	// 如果选项被启用，缓冲区适用于MaxAppendEntries。
	applyCh := make(chan *logFuture) // raft 节点间同步channel
	if conf.BatchApplyCh {           // 默认为False
		applyCh = make(chan *logFuture, conf.MaxAppendEntries)
	}

	r := &Raft{
		protocolVersion:       protocolVersion,
		applyCh:               applyCh,
		fsm:                   fsm,
		fsmMutateCh:           make(chan interface{}, 128),   // fsm 变更 ch
		fsmSnapshotCh:         make(chan *reqSnapshotFuture), // fsm 快照 ch
		leaderCh:              make(chan bool, 1),            // leader 变更 ch
		localID:               localID,                       // 逻辑ID
		localAddr:             localAddr,                     // 实际的IP:PORT
		logger:                logger,
		logs:                  logs,                                  // bolt 数据存储
		configurationChangeCh: make(chan *configurationChangeFuture), //配置变更 ch
		configurations:        configurations{},
		rpcCh:                 trans.Consumer(),
		snapshots:             snaps,                          // 快照存储
		userSnapshotCh:        make(chan *userSnapshotFuture), // 用户主动打快照 ch
		userRestoreCh:         make(chan *userRestoreFuture),
		shutdownCh:            make(chan struct{}),          // 停止 ch
		stable:                stable,                       // 任期 bolt 数据库
		trans:                 trans,                        //
		verifyCh:              make(chan *verifyFuture, 64), // 校验自己是不是leader
		configurationsCh:      make(chan *configurationsFuture, 8),
		bootstrapCh:           make(chan *bootstrapFuture),
		leadershipTransferCh:  make(chan *leadershipTransferFuture, 1),
	}

	r.conf.Store(*conf) // 默认配置，用户手动设置的值

	// 初始化为Follower
	r.setState(Follower)

	// 重新存储当前任期和日志索引
	r.setCurrentTerm(currentTerm)             // db + memory
	r.setLastLog(lastLog.Index, lastLog.Term) // memory

	// 如果有的话，尝试恢复快照。只对最新的快照进行恢复   【全量快照】
	if err := r.restoreSnapshot(); err != nil {
		return nil, err
	}

	// 扫描日志中的任何配置更改条目。
	snapshotIndex, _ := r.getLastSnapshot()
	//  如果快照的数据< 当前日志的数据
	for index := snapshotIndex + 1; index <= lastLog.Index; index++ {
		var entry Log
		// 从logState中获取快照中没有的数据
		if err := r.logs.GetLog(index, &entry); err != nil {
			r.logger.Error("failed to get log", "index", index, "error", err)
			panic(err)
		}
		// 更新已提交、未提交的一些信息
		if err := r.processConfigurationLogEntry(&entry); err != nil {
			return nil, err
		}
	}
	r.logger.Info("初始化配置",
		"index", r.configurations.latestIndex, "servers", hclog.Fmt("%+v", r.configurations.latest.Servers))

	// 设置一个心跳快速路径，尽可能避免线头阻塞。这必须是安全的，因为它可以与一个阻塞的RPC同时调用。
	trans.SetHeartbeatHandler(r.processHeartbeat)
	// 默认为false
	if conf.skipStartup {
		return r, nil
	}
	// 启动后台worker
	r.goFunc(r.run)
	r.goFunc(r.runFSM)
	r.goFunc(r.runSnapshots)// 打快照
	return r, nil
}

// restoreSnapshot 试图恢复最新的快照，如果没有一个能被恢复就会失败。
// 这是在初始化时调用的，在其他任何时候调用都是不安全的。
func (r *Raft) restoreSnapshot() error {
	snapshots, err := r.snapshots.List()
	if err != nil {
		r.logger.Error("获取快照列表失败", "error", err)
		return err
	}

	// 尝试按照从新到旧的顺序加载
	for _, snapshot := range snapshots {
		if !r.config().NoSnapshotRestoreOnStart {
			// 默认会走到这里
			_, source, err := r.snapshots.Open(snapshot.ID)
			if err != nil {
				r.logger.Error("打开快照失败", "id", snapshot.ID, "error", err)
				continue
			}

			if err := fsmRestoreAndMeasure(r.fsm, source); err != nil {
				source.Close()
				r.logger.Error("重置快照失败", "id", snapshot.ID, "error", err)
				continue
			}
			source.Close()

			r.logger.Info("已恢复快照", "id", snapshot.ID)
		}

		// 更新lastApplied，这样我们就不会重放旧的日志了。
		r.setLastApplied(snapshot.Index)

		// 更新最后的快照信息
		r.setLastSnapshot(snapshot.Index, snapshot.Term)

		// 更新配置
		var conf Configuration
		var index uint64
		if snapshot.Version > 0 {
			conf = snapshot.Configuration       // 集群信息
			index = snapshot.ConfigurationIndex // 1
		} else {
			var err error
			if conf, err = decodePeers(snapshot.Peers, r.trans); err != nil {
				return err
			}
			index = snapshot.Index
		}
		r.setCommittedConfiguration(conf, index)
		r.setLatestConfiguration(conf, index)

		// Success!
		return nil
	}

	// 如果我们有快照，但未能加载它们，就会出现错误
	if len(snapshots) > 0 {
		return fmt.Errorf("failed to load any existing snapshots")
	}
	return nil
}

func (r *Raft) config() Config {
	return r.conf.Load().(Config)
}

// ReloadConfig updates the configuration of a running raft node. If the new
// configuration is invalid an error is returned and no changes made to the
// instance. All fields will be copied from rc into the new configuration, even
// if they are zero valued.
func (r *Raft) ReloadConfig(rc ReloadableConfig) error {
	r.confReloadMu.Lock()
	defer r.confReloadMu.Unlock()

	// Load the current config (note we are under a lock so it can't be changed
	// between this read and a later Store).
	oldCfg := r.config()

	// Set the reloadable fields
	newCfg := rc.apply(oldCfg)

	if err := ValidateConfig(&newCfg); err != nil {
		return err
	}
	r.conf.Store(newCfg)
	return nil
}

// ReloadableConfig returns the current state of the reloadable fields in Raft's
// configuration. This is useful for programs to discover the current state for
// reporting to users or tests. It is safe to call from any goroutine. It is
// intended for reporting and testing purposes primarily; external
// synchronization would be required to safely use this in a read-modify-write
// pattern for reloadable configuration options.
func (r *Raft) ReloadableConfig() ReloadableConfig {
	cfg := r.config()
	var rc ReloadableConfig
	rc.fromConfig(cfg)
	return rc
}

// BootstrapCluster is equivalent to non-member BootstrapCluster but can be
// called on an un-bootstrapped Raft instance after it has been created. This
// should only be called at the beginning of time for the cluster with an
// identical configuration listing all Voter servers. There is no need to
// bootstrap Nonvoter and Staging servers.
//
// A cluster can only be bootstrapped once from a single participating Voter
// server. Any further attempts to bootstrap will return an error that can be
// safely ignored.
//
// One sane approach is to bootstrap a single server with a configuration
// listing just itself as a Voter, then invoke AddVoter() on it to add other
// servers to the cluster.
func (r *Raft) BootstrapCluster(configuration Configuration) Future {
	bootstrapReq := &bootstrapFuture{}
	bootstrapReq.init()
	bootstrapReq.configuration = configuration
	select {
	case <-r.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	case r.bootstrapCh <- bootstrapReq:
		return bootstrapReq
	}
}

// Leader 用来返回集群的当前leader。如果没有当前的leader未知，它可能返回空字符串。
func (r *Raft) Leader() ServerAddress {
	r.leaderLock.RLock()
	leader := r.leader
	r.leaderLock.RUnlock()
	return leader
}

// Apply 是用来以高度一致的方式将命令应用于FSM。这将返回一个可以用来等待应用程序的Future。
// 可以提供一个可选的超时来限制我们等待命令启动的时间。这必须在leader上运行，否则会失败。
func (r *Raft) Apply(cmd []byte, timeout time.Duration) ApplyFuture {
	// 将二进制数据封装成 Log
	return r.ApplyLog(Log{Data: cmd}, timeout)
}

// ApplyLog 执行Apply，但直接接收Log。目前从提交的日志中获取的唯一数值是数据和扩展。
func (r *Raft) ApplyLog(log Log, timeout time.Duration) ApplyFuture {

	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}

	// 创建一个logFuture，还没有索引和任期
	logFuture := &logFuture{
		log: Log{
			Type:       LogCommand,
			Data:       log.Data,
			Extensions: log.Extensions,
		},
	}
	logFuture.init()

	select {
	case <-timer: // 发送超时
		return errorFuture{ErrEnqueueTimeout}
	case <-r.shutdownCh: // raft关闭
		return errorFuture{ErrRaftShutdown}
	case r.applyCh <- logFuture:
		return logFuture
	}
}

// Barrier is used to issue a command that blocks until all preceeding
// operations have been applied to the FSM. It can be used to ensure the
// FSM reflects all queued writes. An optional timeout can be provided to
// limit the amount of time we wait for the command to be started. This
// must be run on the leader or it will fail.
func (r *Raft) Barrier(timeout time.Duration) Future {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}

	// Create a log future, no index or term yet
	logFuture := &logFuture{
		log: Log{
			Type: LogBarrier,
		},
	}
	logFuture.init()

	select {
	case <-timer:
		return errorFuture{ErrEnqueueTimeout}
	case <-r.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	case r.applyCh <- logFuture:
		return logFuture
	}
}

// VerifyLeader  是用来确保当前节点仍然是领导者。
func (r *Raft) VerifyLeader() Future {
	verifyFuture := &verifyFuture{}
	verifyFuture.init()
	select {
	case <-r.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	case r.verifyCh <- verifyFuture:
		return verifyFuture
	}
}

// GetConfiguration 返回最新的配置。这可能还没有被提交。主循环可以直接访问这个。
func (r *Raft) GetConfiguration() ConfigurationFuture {
	// 这里没有进行什么实际逻辑，只是将已有的配置封装到了configReq里
	configReq := &configurationsFuture{}
	configReq.init()
	configReq.configurations = configurations{latest: r.getLatestConfiguration()}
	configReq.respond(nil)
	return configReq
}

// AddPeer (deprecated) is used to add a new peer into the cluster. This must be
// run on the leader or it will fail. Use AddVoter/AddNonvoter instead.
func (r *Raft) AddPeer(peer ServerAddress) Future {
	if r.protocolVersion > 2 {
		return errorFuture{ErrUnsupportedProtocol}
	}

	return r.requestConfigChange(configurationChangeRequest{
		command:       AddStaging,
		serverID:      ServerID(peer),
		serverAddress: peer,
		prevIndex:     0,
	}, 0)
}

// RemovePeer (deprecated) is used to remove a peer from the cluster. If the
// current leader is being removed, it will cause a new election
// to occur. This must be run on the leader or it will fail.
// Use RemoveServer instead.
func (r *Raft) RemovePeer(peer ServerAddress) Future {
	if r.protocolVersion > 2 {
		return errorFuture{ErrUnsupportedProtocol}
	}

	return r.requestConfigChange(configurationChangeRequest{
		command:   RemoveServer,
		serverID:  ServerID(peer),
		prevIndex: 0,
	}, 0)
}

// AddVoter will add the given server to the cluster as a staging server. If the
// server is already in the cluster as a voter, this updates the server's address.
// This must be run on the leader or it will fail. The leader will promote the
// staging server to a voter once that server is ready. If nonzero, prevIndex is
// the index of the only configuration upon which this change may be applied; if
// another configuration entry has been added in the meantime, this request will
// fail. If nonzero, timeout is how long this server should wait before the
// configuration change log entry is appended.
func (r *Raft) AddVoter(id ServerID, address ServerAddress, prevIndex uint64, timeout time.Duration) IndexFuture {
	if r.protocolVersion < 2 {
		return errorFuture{ErrUnsupportedProtocol}
	}

	return r.requestConfigChange(configurationChangeRequest{
		command:       AddStaging,
		serverID:      id,
		serverAddress: address,
		prevIndex:     prevIndex,
	}, timeout)
}

// AddNonvoter will add the given server to the cluster but won't assign it a
// vote. The server will receive log entries, but it won't participate in
// elections or log entry commitment. If the server is already in the cluster,
// this updates the server's address. This must be run on the leader or it will
// fail. For prevIndex and timeout, see AddVoter.
func (r *Raft) AddNonvoter(id ServerID, address ServerAddress, prevIndex uint64, timeout time.Duration) IndexFuture {
	if r.protocolVersion < 3 {
		return errorFuture{ErrUnsupportedProtocol}
	}

	return r.requestConfigChange(configurationChangeRequest{
		command:       AddNonvoter,
		serverID:      id,
		serverAddress: address,
		prevIndex:     prevIndex,
	}, timeout)
}

// RemoveServer will remove the given server from the cluster. If the current
// leader is being removed, it will cause a new election to occur. This must be
// run on the leader or it will fail. For prevIndex and timeout, see AddVoter.
func (r *Raft) RemoveServer(id ServerID, prevIndex uint64, timeout time.Duration) IndexFuture {
	if r.protocolVersion < 2 {
		return errorFuture{ErrUnsupportedProtocol}
	}

	return r.requestConfigChange(configurationChangeRequest{
		command:   RemoveServer,
		serverID:  id,
		prevIndex: prevIndex,
	}, timeout)
}

// DemoteVoter will take away a server's vote, if it has one. If present, the
// server will continue to receive log entries, but it won't participate in
// elections or log entry commitment. If the server is not in the cluster, this
// does nothing. This must be run on the leader or it will fail. For prevIndex
// and timeout, see AddVoter.
func (r *Raft) DemoteVoter(id ServerID, prevIndex uint64, timeout time.Duration) IndexFuture {
	if r.protocolVersion < 3 {
		return errorFuture{ErrUnsupportedProtocol}
	}

	return r.requestConfigChange(configurationChangeRequest{
		command:   DemoteVoter,
		serverID:  id,
		prevIndex: prevIndex,
	}, timeout)
}

// Shutdown is used to stop the Raft background routines.
// This is not a graceful operation. Provides a future that
// can be used to block until all background routines have exited.
func (r *Raft) Shutdown() Future {
	r.shutdownLock.Lock()
	defer r.shutdownLock.Unlock()

	if !r.shutdown {
		close(r.shutdownCh)
		r.shutdown = true
		r.setState(Shutdown)
		return &shutdownFuture{r}
	}

	// avoid closing transport twice
	return &shutdownFuture{nil}
}

// Snapshot 是用来手动强制Raft进行快照的。返回一个可以用来阻断直到完成的SnapshotFuture，并且包含一个可以用来打开快照的函数。
func (r *Raft) Snapshot() SnapshotFuture {
	future := &userSnapshotFuture{}
	future.init()
	select {
	case r.userSnapshotCh <- future:
		return future
	case <-r.shutdownCh:
		future.respond(ErrRaftShutdown)
		return future
	}
}

// Restore is used to manually force Raft to consume an external snapshot, such
// as if restoring from a backup. We will use the current Raft configuration,
// not the one from the snapshot, so that we can restore into a new cluster. We
// will also use the higher of the index of the snapshot, or the current index,
// and then add 1 to that, so we force a new state with a hole in the Raft log,
// so that the snapshot will be sent to followers and used for any new joiners.
// This can only be run on the leader, and blocks until the restore is complete
// or an error occurs.
//
// WARNING! This operation has the leader take on the state of the snapshot and
// then sets itself up so that it replicates that to its followers though the
// install snapshot process. This involves a potentially dangerous period where
// the leader commits ahead of its followers, so should only be used for disaster
// recovery into a fresh cluster, and should not be used in normal operations.
func (r *Raft) Restore(meta *SnapshotMeta, reader io.Reader, timeout time.Duration) error {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}

	restore := &userRestoreFuture{
		meta:   meta,
		reader: reader,
	}
	restore.init()
	select {
	case <-timer:
		return ErrEnqueueTimeout
	case <-r.shutdownCh:
		return ErrRaftShutdown
	case r.userRestoreCh <- restore:
		// If the restore is ingested then wait for it to complete.
		if err := restore.Error(); err != nil {
			return err
		}
	}

	// Apply a no-op log entry. Waiting for this allows us to wait until the
	// followers have gotten the restore and replicated at least this new
	// entry, which shows that we've also faulted and installed the
	// snapshot with the contents of the restore.
	noop := &logFuture{
		log: Log{
			Type: LogNoop,
		},
	}
	noop.init()
	select {
	case <-timer:
		return ErrEnqueueTimeout
	case <-r.shutdownCh:
		return ErrRaftShutdown
	case r.applyCh <- noop:
		return noop.Error()
	}
}

// State is used to return the current raft state.
func (r *Raft) State() RaftState {
	return r.getState()
}

// LeaderCh 是用来获得一个通道，该通道传递关于获得或失去leader的信号。
// 如果我们成为leader，它就会发出真信号，如果我们失去leader，就会发出假信号。
//
// 只有在leader交接的情况下，接收者才会收到通知。
//
// 如果receiver没有为信号做好准备，信号可能会丢失，只有最新的leader转移。
// 例如，如果一个接收者收到后续的 "true "值，他们可能会推断出leader的丧失和重新获得，同时 接收者正在处理第一个leader转换。
func (r *Raft) LeaderCh() <-chan bool {
	return r.leaderCh
}

// String returns a string representation of this Raft node.
func (r *Raft) String() string {
	return fmt.Sprintf("Node at %s [%v]", r.localAddr, r.getState())
}

// LastContact 返回与leader最后一次通信的时间。这只有在我们目前是follower的情况下才有意义。
func (r *Raft) LastContact() time.Time {
	r.lastContactLock.RLock()
	last := r.lastContact
	r.lastContactLock.RUnlock()
	return last
}

// Stats is used to return a map of various internal stats. This
// should only be used for informative purposes or debugging.
//
// Keys are: "state", "term", "last_log_index", "last_log_term",
// "commit_index", "applied_index", "fsm_pending",
// "last_snapshot_index", "last_snapshot_term",
// "latest_configuration", "last_contact", and "num_peers".
//
// The value of "state" is a numeric constant representing one of
// the possible leadership states the node is in at any given time.
// the possible states are: "Follower", "Candidate", "Leader", "Shutdown".
//
// The value of "latest_configuration" is a string which contains
// the id of each server, its suffrage status, and its address.
//
// The value of "last_contact" is either "never" if there
// has been no contact with a leader, "0" if the node is in the
// leader state, or the time since last contact with a leader
// formatted as a string.
//
// The value of "num_peers" is the number of other voting servers in the
// cluster, not including this node. If this node isn't part of the
// configuration then this will be "0".
//
// All other values are uint64s, formatted as strings.
func (r *Raft) Stats() map[string]string {
	toString := func(v uint64) string {
		return strconv.FormatUint(v, 10)
	}
	lastLogIndex, lastLogTerm := r.getLastLog()
	lastSnapIndex, lastSnapTerm := r.getLastSnapshot()
	s := map[string]string{
		"state":                r.getState().String(),
		"term":                 toString(r.getCurrentTerm()),
		"last_log_index":       toString(lastLogIndex),
		"last_log_term":        toString(lastLogTerm),
		"commit_index":         toString(r.getCommitIndex()),
		"applied_index":        toString(r.getLastApplied()),
		"fsm_pending":          toString(uint64(len(r.fsmMutateCh))),
		"last_snapshot_index":  toString(lastSnapIndex),
		"last_snapshot_term":   toString(lastSnapTerm),
		"protocol_version":     toString(uint64(r.protocolVersion)),
		"protocol_version_min": toString(uint64(ProtocolVersionMin)),
		"protocol_version_max": toString(uint64(ProtocolVersionMax)),
		"snapshot_version_min": toString(uint64(SnapshotVersionMin)),
		"snapshot_version_max": toString(uint64(SnapshotVersionMax)),
	}

	future := r.GetConfiguration()
	if err := future.Error(); err != nil {
		r.logger.Warn("could not get configuration for stats", "error", err)
	} else {
		configuration := future.Configuration()
		s["latest_configuration_index"] = toString(future.Index())
		s["latest_configuration"] = fmt.Sprintf("%+v", configuration.Servers)

		// This is a legacy metric that we've seen people use in the wild.
		hasUs := false
		numPeers := 0
		for _, server := range configuration.Servers {
			if server.Suffrage == Voter {
				if server.ID == r.localID {
					hasUs = true
				} else {
					numPeers++
				}
			}
		}
		if !hasUs {
			numPeers = 0
		}
		s["num_peers"] = toString(uint64(numPeers))
	}

	last := r.LastContact()
	if r.getState() == Leader {
		s["last_contact"] = "0"
	} else if last.IsZero() {
		s["last_contact"] = "never"
	} else {
		s["last_contact"] = fmt.Sprintf("%v", time.Now().Sub(last))
	}
	return s
}

// LastIndex returns the last index in stable storage,
// either from the last log or from the last snapshot.
func (r *Raft) LastIndex() uint64 {
	return r.getLastIndex()
}

// AppliedIndex returns the last index applied to the FSM. This is generally
// lagging behind the last index, especially for indexes that are persisted but
// have not yet been considered committed by the leader. NOTE - this reflects
// the last index that was sent to the application's FSM over the apply channel
// but DOES NOT mean that the application's FSM has yet consumed it and applied
// it to its internal state. Thus, the application's state may lag behind this
// index.
func (r *Raft) AppliedIndex() uint64 {
	return r.getLastApplied()
}

// LeadershipTransfer will transfer leadership to a server in the cluster.
// This can only be called from the leader, or it will fail. The leader will
// stop accepting client requests, make sure the target server is up to date
// and starts the transfer with a TimeoutNow message. This message has the same
// effect as if the election timeout on the on the target server fires. Since
// it is unlikely that another server is starting an election, it is very
// likely that the target server is able to win the election.  Note that raft
// protocol version 3 is not sufficient to use LeadershipTransfer. A recent
// version of that library has to be used that includes this feature.  Using
// transfer leadership is safe however in a cluster where not every node has
// the latest version. If a follower cannot be promoted, it will fail
// gracefully.
func (r *Raft) LeadershipTransfer() Future {
	if r.protocolVersion < 3 {
		return errorFuture{ErrUnsupportedProtocol}
	}

	return r.initiateLeadershipTransfer(nil, nil)
}

// LeadershipTransferToServer does the same as LeadershipTransfer but takes a
// server in the arguments in case a leadership should be transitioned to a
// specific server in the cluster.  Note that raft protocol version 3 is not
// sufficient to use LeadershipTransfer. A recent version of that library has
// to be used that includes this feature. Using transfer leadership is safe
// however in a cluster where not every node has the latest version. If a
// follower cannot be promoted, it will fail gracefully.
func (r *Raft) LeadershipTransferToServer(id ServerID, address ServerAddress) Future {
	if r.protocolVersion < 3 {
		return errorFuture{ErrUnsupportedProtocol}
	}

	return r.initiateLeadershipTransfer(&id, &address)
}
