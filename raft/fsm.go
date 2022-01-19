package raft

import (
	"fmt"
	"io"
)

// FSM provides an interface that can be implemented by
// clients to make use of the replicated log.
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

// BatchingFSM extends the FSM interface to add an ApplyBatch function. This can
// optionally be implemented by clients to enable multiple logs to be applied to
// the FSM in batches. Up to MaxAppendEntries could be sent in a batch.
type BatchingFSM interface {
	// ApplyBatch is invoked once a batch of log entries has been committed and
	// are ready to be applied to the FSM. ApplyBatch will take in an array of
	// log entries. These log entries will be in the order they were committed,
	// will not have gaps, and could be of a few log types. Clients should check
	// the log type prior to attempting to decode the data attached. Presently
	// the LogCommand and LogConfiguration types will be sent.
	//
	// The returned slice must be the same length as the input and each response
	// should correlate to the log at the same index of the input. The returned
	// values will be made available in the ApplyFuture returned by Raft.Apply
	// method if that method was called on the same Raft node as the FSM.
	ApplyBatch([]*Log) []interface{}

	FSM
}

// FSMSnapshot is returned by an FSM in response to a Snapshot
// It must be safe to invoke FSMSnapshot methods with concurrent
// calls to Apply.
type FSMSnapshot interface {
	// Persist should dump all necessary state to the WriteCloser 'sink',
	// and call sink.Close() when finished or call sink.Cancel() on error.
	Persist(sink SnapshotSink) error

	// Release 是在我们完成快照后调用的。
	Release()
}

// runFSM 是一个长期运行的Goroutine，负责将日志应用到FSM中。这是与其他日志同步进行的，因为我们不希望FSM阻塞我们的内部操作。
func (r *Raft) runFSM() {
	//最新索引、最新任期
	var lastIndex, lastTerm uint64

	batchingFSM, batchingEnabled := r.fsm.(BatchingFSM)
	configStore, configStoreEnabled := r.fsm.(ConfigurationStore)

	commitSingle := func(req *commitTuple) {
		// Apply the log if a command or config change
		var resp interface{}
		// Make sure we send a response
		defer func() {
			// Invoke the future if given
			if req.future != nil {
				req.future.response = resp
				req.future.respond(nil)
			}
		}()

		switch req.log.Type {
		case LogCommand:
			resp = r.fsm.Apply(req.log)

		case LogConfiguration:
			if !configStoreEnabled {
				// Return early to avoid incrementing the index and term for
				// an unimplemented operation.
				return
			}

			configStore.StoreConfiguration(req.log.Index, DecodeConfiguration(req.log.Data))
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
		// Open the snapshot
		meta, source, err := r.snapshots.Open(req.ID)
		if err != nil {
			req.respond(fmt.Errorf("failed to open snapshot %v: %v", req.ID, err))
			return
		}
		defer source.Close()

		// Attempt to restore
		if err := fsmRestoreAndMeasure(r.fsm, source); err != nil {
			req.respond(fmt.Errorf("failed to restore snapshot %v: %v", req.ID, err))
			return
		}

		// Update the last index and term
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
		case ptr := <-r.fsmMutateCh:
			switch req := ptr.(type) {
			case []*commitTuple:
				commitBatch(req)

			case *restoreFuture:
				restore(req)

			default:
				panic(fmt.Errorf("bad type passed to fsmMutateCh: %#v", ptr))
			}

		case req := <-r.fsmSnapshotCh:
			snapshot(req)

		case <-r.shutdownCh:
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
