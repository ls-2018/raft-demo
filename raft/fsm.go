package raft

import (
	"fmt"
	"io"
)

// FSM 客户端应该实现这个接口
type FSM interface {
	// Apply
	//如果Raft.Apply方法在与FSM相同的Raft节点上被调用，则该方法将在ApplyFuture中使用。
	Apply(*Log) interface{}
	// Snapshot
	//是用来支持日志压缩的。这个调用应该返回一个FSMSnapshot，可以用来保存FSM的时间点快照。
	//Apply和Snapshot不应在多线程中调用，但Apply将通过Persist并发调用。
	//这意味着FSM的实现方式应该是允许在快照发生时进行并发更新。
	Snapshot() (FSMSnapshot, error)
	// Restore 是用来从快照中恢复FSM的。它不能与任何其他命令同时调用。FSM必须丢弃所有以前的 状态。
	Restore(io.ReadCloser) error
}

// BatchingFSM  允许客户端批量提交数据 ,一个批次最多MaxAppendEntries
type BatchingFSM interface {
	ApplyBatch([]*Log) []interface{}
	FSM
}

// FSMSnapshot 快照接口，需实现
type FSMSnapshot interface {
	Persist(sink SnapshotSink) error // 打快照

	// Release 是在我们完成快照后调用的。// 回调函数
	Release()
}

// runFSM 是一个长期运行的Goroutine，负责将日志应用到FSM中。这是与其他日志同步进行的，因为我们不希望FSM阻塞我们的内部操作。
func (r *Raft) runFSM() {
	//最新索引、最新任期
	var lastIndex, lastTerm uint64

	batchingFSM, batchingEnabled := r.fsm.(BatchingFSM)

	commitSingle := func(req *commitTuple) {
		// 如果一个命令或配置发生变化，应用日志
		var resp interface{}
		defer func() {
			if req.future != nil {
				req.future.response = resp
				req.future.respond(nil)
			}
		}()

		switch req.log.Type {
		case LogCommand:
			resp = r.fsm.Apply(req.log)

		case LogConfiguration:
			return
		}

		// Update the indexes
		lastIndex = req.log.Index
		lastTerm = req.log.Term
	}

	commitBatch := func(reqs []*commitTuple) {
		if !batchingEnabled {
			for _, ct := range reqs {
				commitSingle(ct)
			}
			return
		}

		// Only send LogCommand and LogConfiguration log types. LogBarrier types
		// will not be sent to the FSM.
		shouldSend := func(l *Log) bool {
			switch l.Type {
			case LogCommand, LogConfiguration:
				return true
			}
			return false
		}

		var lastBatchIndex, lastBatchTerm uint64
		sendLogs := make([]*Log, 0, len(reqs))
		for _, req := range reqs {
			if shouldSend(req.log) {
				sendLogs = append(sendLogs, req.log)
			}
			lastBatchIndex = req.log.Index
			lastBatchTerm = req.log.Term
		}

		var responses []interface{}
		if len(sendLogs) > 0 {
			responses = batchingFSM.ApplyBatch(sendLogs)

			// Ensure we get the expected responses
			if len(sendLogs) != len(responses) {
				panic("invalid number of responses")
			}
		}

		// Update the indexes
		lastIndex = lastBatchIndex
		lastTerm = lastBatchTerm

		var i int
		for _, req := range reqs {
			var resp interface{}
			// If the log was sent to the FSM, retrieve the response.
			if shouldSend(req.log) {
				resp = responses[i]
				i++
			}

			if req.future != nil {
				req.future.response = resp
				req.future.respond(nil)
			}
		}
	}

	restore := func(req *restoreFuture) {
		// 打开一个快照
		meta, source, err := r.snapshots.Open(req.ID)
		if err != nil {
			req.respond(fmt.Errorf("打开快照失败 %v: %v", req.ID, err))
			return
		}
		defer source.Close()

		// 试图恢复
		if err := fsmRestoreAndMeasure(r.fsm, source); err != nil {
			req.respond(fmt.Errorf("恢复快照失败 %v: %v", req.ID, err))
			return
		}

		// 更新最新的索引、任期
		lastIndex = meta.Index
		lastTerm = meta.Term
		req.respond(nil)
	}

	snapshot := func(req *reqSnapshotFuture) {
		// Is there something to snapshot?
		if lastIndex == 0 {
			req.respond(ErrNothingNewToSnapshot)
			return
		}

		// Start a snapshot
		snap, err := r.fsm.Snapshot()

		// Respond to the request
		req.index = lastIndex
		req.term = lastTerm
		req.snapshot = snap
		req.respond(err)
	}

	for {
		select {
		case ptr := <-r.fsmMutateCh: // 用来向FSM发送状态变化的更新,恢复快照
			switch req := ptr.(type) {
			case []*commitTuple:
				// 在应用日志时，它接收指向commitTuple结构的指针;
				commitBatch(req)
			case *restoreFuture:
				// 在恢复快照时接收指向restoreFuture结构的指针。
				restore(req)
			default:
				panic(fmt.Errorf("错误的类型传递 fsmMutateCh: %#v", ptr))
			}

		case req := <-r.fsmSnapshotCh: // 快照信号
			snapshot(req)

		case <-r.shutdownCh: // raft停止
			return
		}
	}
}

// fsmRestoreAndMeasure 包裹FSM上的Restore调用， 在所有情况下，调用者仍然负责在源上调用关闭。
func fsmRestoreAndMeasure(fsm FSM, source io.ReadCloser) error {
	if err := fsm.Restore(source); err != nil {
		return err
	}
	return nil
}
