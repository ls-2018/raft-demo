package raft

import (
	"sync"
	"sync/atomic"
)

// RaftState 捕获一个Raft节点的状态。跟随者、候选者、领导者或关闭。
type RaftState uint32

const (
	Follower RaftState = iota
	Candidate
	Leader
	Shutdown
)

func (s RaftState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Shutdown:
		return "Shutdown"
	default:
		return "Unknown"
	}
}

// raftState 用于维护各种状态变量，并提供了一个接口，以便以线程安全的方式设置/获取这些变量
type raftState struct {
	// 当前任期
	currentTerm uint64
	// commit的  最高的日志条目
	commitIndex uint64
	// 上一次apply to fsm 的index
	lastApplied uint64

	// protects 4 next fields
	lastLock sync.Mutex

	// 缓存最新快照的  index\term
	lastSnapshotIndex uint64
	lastSnapshotTerm  uint64

	// 缓存 LogStore 最新的日志索引数据
	lastLogIndex uint64
	lastLogTerm  uint64

	// 运行goroutines的轨迹
	routinesGroup sync.WaitGroup

	// 当前状态
	state RaftState
}

// ok
func (r *raftState) getState() RaftState {
	// 初始化时，因为值为0 ,本身就是follower
	stateAddr := (*uint32)(&r.state)
	return RaftState(atomic.LoadUint32(stateAddr))
}

// OK
func (r *raftState) setState(s RaftState) {
	stateAddr := (*uint32)(&r.state)
	atomic.StoreUint32(stateAddr, uint32(s))
}

// OK
func (r *raftState) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&r.currentTerm)
}

// OK
func (r *raftState) setCurrentTerm(term uint64) {
	atomic.StoreUint64(&r.currentTerm, term)
}

// ok
func (r *raftState) getLastLog() (index, term uint64) {
	r.lastLock.Lock()
	index = r.lastLogIndex
	term = r.lastLogTerm
	r.lastLock.Unlock()
	return
}

// ok
func (r *raftState) setLastLog(index, term uint64) {
	r.lastLock.Lock()
	r.lastLogIndex = index
	r.lastLogTerm = term
	r.lastLock.Unlock()
}

func (r *raftState) getLastSnapshot() (index, term uint64) {
	r.lastLock.Lock()
	index = r.lastSnapshotIndex
	term = r.lastSnapshotTerm
	r.lastLock.Unlock()
	return
}
// 设置最新的快照的 索引、任期
func (r *raftState) setLastSnapshot(index, term uint64) {
	r.lastLock.Lock()
	r.lastSnapshotIndex = index
	r.lastSnapshotTerm = term
	r.lastLock.Unlock()
}

func (r *raftState) getCommitIndex() uint64 {
	return atomic.LoadUint64(&r.commitIndex)
}

func (r *raftState) setCommitIndex(index uint64) {
	atomic.StoreUint64(&r.commitIndex, index)
}

func (r *raftState) getLastApplied() uint64 {
	return atomic.LoadUint64(&r.lastApplied)
}

func (r *raftState) setLastApplied(index uint64) {
	atomic.StoreUint64(&r.lastApplied, index)
}

// 启动一个goroutine，并正确处理goroutine启动和增量，以及退出和减量之间的竞赛。
func (r *raftState) goFunc(f func()) {
	r.routinesGroup.Add(1)
	go func() {
		defer r.routinesGroup.Done()
		f()
	}()
}

func (r *raftState) waitShutdown() {
	r.routinesGroup.Wait()
}

// getLastIndex  获取快照、日志中最大的索引
func (r *raftState) getLastIndex() uint64 {
	r.lastLock.Lock()
	defer r.lastLock.Unlock()
	return max(r.lastLogIndex, r.lastSnapshotIndex)
}

// getLastEntry 返回最新的日志索引 、任期
func (r *raftState) getLastEntry() (uint64, uint64) {
	r.lastLock.Lock()
	defer r.lastLock.Unlock()
	if r.lastLogIndex >= r.lastSnapshotIndex {
		return r.lastLogIndex, r.lastLogTerm
	}
	return r.lastSnapshotIndex, r.lastSnapshotTerm
}
