package raft

import (
	"sync/atomic"
	"time"
)

// Observation 在事件发生时沿着给定的通道发送给观察员。
type Observation struct {
	// Raft holds the Raft instance generating the observation.
	Raft *Raft
	// Data holds observation-specific data. Possible types are
	// *RequestVoteRequest
	// RaftState
	// PeerObservation
	// LeaderObservation
	Data interface{}
}

// LeaderObservation 是用于leader变化时的数据。
type LeaderObservation struct {
	Leader ServerAddress
}

// PeerObservation is sent to observers when peers change.
type PeerObservation struct {
	Removed bool
	Peer    Server
}

// FailedHeartbeatObservation is sent when a node fails to heartbeat with the leader
type FailedHeartbeatObservation struct {
	PeerID      ServerID
	LastContact time.Time
}

// ResumedHeartbeatObservation is sent when a node resumes to heartbeat with the leader following failures
type ResumedHeartbeatObservation struct {
	PeerID ServerID
}

// nextObserverId is used to provide a unique ID for each observer to aid in
// deregistration.
var nextObserverID uint64

// FilterFn 是一个可以注册的函数，以便过滤观察结果。该函数报告观察值是否应该被包括在内--如果它返回false，观察值将被过滤掉。
type FilterFn func(o *Observation) bool

// Observer 描述了如何处理一个给定的观察结果。
type Observer struct {
	// numObserved and numDropped 是 该观察者的性能计数器。
	// 64位的类型必须是64位对齐的，以用于原子操作的
	// 32位平台，所以把它们放在结构的顶部。
	numObserved uint64 // 接收到的事件
	numDropped  uint64 // 因阻塞而drop掉的事件

	// channel 接收观察结果
	channel chan Observation

	// blocking, 如果为真，将导致Raft在发送观察结果到这个观察者时阻塞 ，会导致Raft阻塞。这通常应该被设置为false。
	blocking bool

	// filter 将被调用，以确定是否应将观察结果发送到通道。
	filter FilterFn

	// id 是该观察者在Raft map 中的ID。
	id uint64
}

// NewObserver
// 创建一个新的观察者，可以被注册来对raft实例进行观察。如果观察结果满足给定的过滤器，就会在给定的通道上发送。
// 如果阻塞为真，观察者将在无法在通道上发送时阻塞，否则它可能会丢弃事件。
func NewObserver(channel chan Observation, blocking bool, filter FilterFn) *Observer {
	// 每个节点都会有一个观察者  n*n
	return &Observer{
		channel:  channel,
		blocking: blocking,
		filter:   filter,
		id:       atomic.AddUint64(&nextObserverID, 1),
	}
}

// GetNumObserved returns the number of observations.
func (or *Observer) GetNumObserved() uint64 {
	return atomic.LoadUint64(&or.numObserved)
}

// GetNumDropped returns the number of dropped observations due to blocking.
func (or *Observer) GetNumDropped() uint64 {
	return atomic.LoadUint64(&or.numDropped)
}

// RegisterObserver 注册一个观察者
func (r *Raft) RegisterObserver(or *Observer) {
	r.observersLock.Lock()
	defer r.observersLock.Unlock()
	r.observers[or.id] = or
}

// DeregisterObserver 取消一个观察者
func (r *Raft) DeregisterObserver(or *Observer) {
	r.observersLock.Lock()
	defer r.observersLock.Unlock()
	delete(r.observers, or.id)
}

// observe 向每个node发送了一个观察结果。
func (r *Raft) observe(o interface{}) {
	// 一般来说，观察员不应该阻止。但在任何情况下，这都不是灾难性的，因为我们只持有一个读锁，这只是防止观察者的注册/取消注册。
	r.observersLock.RLock()
	defer r.observersLock.RUnlock()
	for _, or := range r.observers {
		// 在循环中这样做是很浪费的，但对于没有观察者的常见情况，我们不会创建任何对象。
		ob := Observation{Raft: r, Data: o}
		if or.filter != nil && !or.filter(&ob) {
			// 过滤函数不为空,     如果它返回false，观察值将被过滤掉。
			continue
		}
		if or.channel == nil {
			// 接收事件的通道
			continue
		}
		if or.blocking {
			// 阻塞式发送
			or.channel <- ob
			atomic.AddUint64(&or.numObserved, 1)
		} else {
			// 非阻塞式发送
			select {
			case or.channel <- ob:
				atomic.AddUint64(&or.numObserved, 1)
			default:
				atomic.AddUint64(&or.numDropped, 1)
			}
		}
	}
}
