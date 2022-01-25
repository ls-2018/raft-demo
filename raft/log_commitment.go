package raft

import (
	"sort"
	"sync"
)

// Commitment 用来推进领导者的commit索引。
// 1、leader和复制程序用Match()报告新写入的条目，
// 2、在commitCh获取事件
type commitment struct {
	// 保护 matchIndexes 、 commitIndex
	sync.Mutex
	// 当 commitIndex 增加时通知
	commitCh chan struct{}
	// 选民的日志索引,服务器通过日志条目存储起来
	matchIndexes map[ServerID]uint64 // node logical id = the latest log index
	// a quorum stores up through this log entry. monotonically increases.
	// 一个法定人数通过这个日志条目存储起来，单调地增加。
	commitIndex uint64
	// 这个leader任期的第一个索引：在这个leader可以标记任何已承诺的事情之前，这需要复制到集群的大多数。(根据Raft的承诺规则)
	startIndex uint64
}

// newCommitment returns a commitment struct that notifies the provided
// channel when log entries have been committed. A new commitment struct is
// created each time this server becomes leader for a particular term.
// 'configuration' is the servers in the cluster.
// 'startIndex' is the first index created in this term (see
// its description above).
// 返回一个newCommitment结构体，当日志条目被提交时通知所提供的通道。每次这个服务器成为某个特定术语的领导者时，就会创建一个新的承诺结构。
// '配置'是集群中的服务器。'startIndex'是这个术语中创建的第一个索引（见上面的描述）。
func newCommitment(commitCh chan struct{}, configuration Configuration, startIndex uint64) *commitment {
	matchIndexes := make(map[ServerID]uint64)
	for _, server := range configuration.Servers {
		if server.Suffrage == Voter {
			matchIndexes[server.ID] = 0
		}
	}
	return &commitment{
		commitCh:     commitCh,
		matchIndexes: matchIndexes,
		commitIndex:  0,
		startIndex:   startIndex,
	}
}

// Called when a new cluster membership configuration is created: it will be
// used to determine commitment from now on. 'configuration' is the servers in
// the cluster.
func (c *commitment) setConfiguration(configuration Configuration) {
	c.Lock()
	defer c.Unlock()
	oldMatchIndexes := c.matchIndexes
	c.matchIndexes = make(map[ServerID]uint64)
	for _, server := range configuration.Servers {
		if server.Suffrage == Voter {
			c.matchIndexes[server.ID] = oldMatchIndexes[server.ID] // defaults to 0
		}
	}
	c.recalculate()
}

// 领导者在接到 commitCh 的通知后调用
func (c *commitment) getCommitIndex() uint64 {
	c.Lock()
	defer c.Unlock()
	return c.commitIndex
}

// 一旦服务器完成了向磁盘写入条目的工作，就会调用Match：
// 要么是leader写入了新条目；
// 要么是follower回复了AppendEntries RPC。给定的服务器的磁盘与该服务器的日志在给定的索引前一致。
func (c *commitment) match(server ServerID, matchIndex uint64) {
	c.Lock()
	defer c.Unlock()
	// 当前选民的日志索引 存在，且当前的日志索引大于之前的存储的
	if prev, hasVote := c.matchIndexes[server]; hasVote && matchIndex > prev {
		c.matchIndexes[server] = matchIndex
		c.recalculate()
	}
}

// 内部帮助程序，从matchIndexes计算新的commitIndex。必须在锁定的情况下调用。
func (c *commitment) recalculate() {
	// 选民的日志索引
	if len(c.matchIndexes) == 0 {
		return
	}

	matched := make([]uint64, 0, len(c.matchIndexes))
	for _, idx := range c.matchIndexes {
		matched = append(matched, idx)
	}
	sort.Sort(uint64Slice(matched))
	quorumMatchIndex := matched[(len(matched)-1)/2]
	// 日志索引的中间数

	// 提交数小于中间数 且 中间数>= leader任期开始时的索引数
	// TODO 说明：一个日志落后的node成为了leader
	if quorumMatchIndex > c.commitIndex && quorumMatchIndex >= c.startIndex {
		c.commitIndex = quorumMatchIndex
		asyncNotifyCh(c.commitCh)
	}
}
