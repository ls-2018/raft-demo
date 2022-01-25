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

	// ErrCantBootstrap 当尝试引导一个已经有状态的集群时返回。
	ErrCantBootstrap = errors.New("bootstrap only works on new clusters")

	// ErrLeadershipTransferInProgress 当领导因试图转移领导而拒绝客户请求时返回。
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

	// conf 存储当前要使用的配置。这是所提供的最新的配置。
	// 所有对配置值的读取都应该使用config()辅助方法来安全读取。 按照当前的逻辑来看，在运行中是没有更改的
	conf atomic.Value

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
	// leader让此节点变成candidate
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
	bootstrapCh chan *bootstrapFuture // 只能在follower的时候运行

	// leadershipTransferCh 是用来从主线程之外启动leader转移监听的。
	leadershipTransferCh chan *leadershipTransferFuture
}

// ReloadableConfig 返回Raft配置中可重载字段的当前状态。这对于程序发现向用户或测试报告的当前状态非常有用。
// 从任何goroutine调用都是安全的。它主要用于报告和测试目的;对于可重新加载的配置选项，需要外部同步才能以读-修改-写模式安全地使用它。
func (r *Raft) ReloadableConfig() ReloadableConfig {
	cfg := r.config()
	var rc ReloadableConfig
	rc.fromConfig(cfg)
	return rc
}

// Restore 重新保存快照、应用
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

// GetConfiguration 返回最新的配置。这可能还没有被提交。主循环可以直接访问这个。
func (r *Raft) GetConfiguration() ConfigurationFuture {
	// 这里没有进行什么实际逻辑，只是将已有的配置封装到了configReq里
	configReq := &configurationsFuture{}
	configReq.init()
	configReq.configurations = configurations{latest: r.getLatestConfiguration()}
	configReq.respond(nil)
	return configReq
}

// Stats 用于返回各种内部统计的映射。这应该只用于提供信息或调试。
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
		r.logger.Warn("获取配置失败", "error", err)
	} else {
		configuration := future.Configuration()
		s["latest_configuration_index"] = toString(future.Index())
		s["latest_configuration"] = fmt.Sprintf("%+v", configuration.Servers)
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

// LeadershipTransfer 对用户暴露的leader转移
func (r *Raft) LeadershipTransfer() Future {
	if r.protocolVersion < 3 {
		return errorFuture{ErrUnsupportedProtocol}
	}

	return r.initiateLeadershipTransfer(nil, nil)
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

// HasExistingState 如果服务器有任何现有状态，则返回true  (logs,knowledge of a current term, or any snapshots).
func HasExistingState(logs LogStore, stable StableStore, snaps SnapshotStore) (bool, error) {
	// 确保当前没有任期
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

	// 确保有一个空日志
	lastIndex, err := logs.LastIndex()
	if err != nil {
		return false, fmt.Errorf("failed to get last log index: %v", err)
	}
	if lastIndex > 0 {
		return true, nil
	}

	// 确保没有快照
	snapshots, err := snaps.List()
	if err != nil {
		return false, fmt.Errorf("failed to list snapshots: %v", err)
	}
	if len(snapshots) > 0 {
		return true, nil
	}

	return false, nil
}

func (r *Raft) config() Config {
	return r.conf.Load().(Config)
}

// String 当前节点的地址、状态
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

// LastIndex 返回最新的日志索引
func (r *Raft) LastIndex() uint64 {
	return r.getLastIndex()
}

// AppliedIndex 返回应用于FSM的最后一个索引。
func (r *Raft) AppliedIndex() uint64 {
	return r.getLastApplied()
}

// Shutdown 用于优雅停止raft
func (r *Raft) Shutdown() Future {
	r.shutdownLock.Lock()
	defer r.shutdownLock.Unlock()

	if !r.shutdown {
		// 没有关闭
		close(r.shutdownCh)
		r.shutdown = true
		r.setState(Shutdown)
		return &shutdownFuture{r}
	}

	// 避免关闭两次transport
	return &shutdownFuture{nil}
}

// State 返回当前的raft状态
func (r *Raft) State() State {
	return r.getState()
}

// BootstrapCluster 等价于非成员BootstrapCluster，但是可以在一个非bootstrap的Raft实例创建后被调用。
// 对于列出所有Voter服务器的相同配置的集群，应该只在一开始调用它。不需要引导Nonvoter和Staging服务器。
// 集群只能从一个参与的投票者服务器启动一次。任何进一步的引导尝试都将返回一个可以安全地忽略的错误。
// 一种明智的方法是使用一个配置清单引导单个服务器，将其本身作为一个投票者，然后在其上调用AddVoter()将其他服务器添加到集群中。
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
	r.goFunc(r.run) // 状态变更 over
	r.goFunc(r.runFSM)
	r.goFunc(r.runSnapshots) // 打快照 over
	return r, nil
}

// BootstrapCluster
// 通过提供的集群配置初始化服务器存储,对于列出所有Voter服务器的相同配置的集群，应该只在一开始调用它。不需要引导Nonvoter、Staging 服务器。
// 集群只能从一个参与的投票者服务器启动一次。在leader启动
// 任何进一步的引导尝试都将返回一个可以安全地忽略的错误。
// 一种方法是将configuration作为一个Voter引导单个服务器，然后调用它的AddVoter()将其他服务器添加到集群中。
func BootstrapCluster(conf *Config, logs LogStore, stable StableStore, snaps SnapshotStore, trans Transport, configuration Configuration) error {
	if err := ValidateConfig(conf); err != nil {
		return err
	}

	// 完整性检查Raft node 配置。
	if err := checkConfiguration(configuration); err != nil {
		return err
	}

	// 确保集群有一个干净的状态
	hasState, err := HasExistingState(logs, stable, snaps)
	if err != nil {
		return fmt.Errorf("检查已有状态失败: %v", err)
	}
	if hasState {
		return ErrCantBootstrap
	}
	// 以下是集群初始化的时候需要写点数据
	// 设置当前任期为1
	if err := stable.SetUint64(keyCurrentTerm, 1); err != nil {
		return fmt.Errorf("failed to save current term: %v", err)
	}

	// 追加配置项
	entry := &Log{
		Index: 1,
		Term:  1,
	}
	entry.Type = LogConfiguration
	entry.Data = EncodeConfiguration(configuration)

	if err := logs.StoreLog(entry); err != nil {
		return fmt.Errorf("追加配置项失败: %v", err)
	}

	return nil
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
