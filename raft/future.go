package raft

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

// Future is used to represent an action that may occur in the future.
type Future interface {
	// Error blocks until the future arrives and then returns the error status
	// of the future. This may be called any number of times - all calls will
	// return the same value, however is not OK to call this method twice
	// concurrently on the same Future instance.
	// Error will only return generic errors related to raft, such
	// as ErrLeadershipLost, or ErrRaftShutdown. Some operations, such as
	// ApplyLog, may also return errors from other methods.
	Error() error
}

// IndexFuture is used for future actions that can result in a raft log entry
// being created.
type IndexFuture interface {
	Future

	// Index holds the index of the newly applied log entry.
	// This must not be called until after the Error method has returned.
	Index() uint64
}

// ApplyFuture is used for Apply and can return the FSM response.
type ApplyFuture interface {
	IndexFuture

	// Response returns the FSM response as returned by the FSM.Apply method. This
	// must not be called until after the Error method has returned.
	// Note that if FSM.Apply returns an error, it will be returned by Response,
	// and not by the Error method, so it is always important to check Response
	// for errors from the FSM.
	Response() interface{}
}

// ConfigurationFuture is used for GetConfiguration and can return the
// latest configuration in use by Raft.
type ConfigurationFuture interface {
	IndexFuture

	// Configuration contains the latest configuration. This must
	// not be called until after the Error method has returned.
	Configuration() Configuration
}

// SnapshotFuture is used for waiting on a user-triggered snapshot to complete.
type SnapshotFuture interface {
	Future

	// Open is a function you can call to access the underlying snapshot and
	// its metadata. This must not be called until after the Error method
	// has returned.
	Open() (*SnapshotMeta, io.ReadCloser, error)
}

// LeadershipTransferFuture is used for waiting on a user-triggered leadership
// transfer to complete.
type LeadershipTransferFuture interface {
	Future
}

// errorFuture is used to return a static error.
type errorFuture struct {
	err error
}

func (e errorFuture) Error() error {
	return e.err
}

func (e errorFuture) Response() interface{} {
	return nil
}

func (e errorFuture) Index() uint64 {
	return 0
}

// deferError  可以被嵌入以允许 提供一个错误
type deferError struct {
	err        error
	errCh      chan error
	responded  bool
	ShutdownCh chan struct{}
}

func (d *deferError) init() {
	d.errCh = make(chan error, 1)
}

func (d *deferError) Error() error {
	if d.err != nil {
		return d.err
	}
	if d.errCh == nil {
		panic("在一个空channel上等待响应")
	}
	select {
	case d.err = <-d.errCh:
	case <-d.ShutdownCh:
		d.err = ErrRaftShutdown
	}
	return d.err
}

func (d *deferError) respond(err error) {
	if d.errCh == nil {
		return
	}
	if d.responded {
		return
	}
	d.errCh <- err
	close(d.errCh)
	d.responded = true
}

// There are several types of requests that cause a configuration entry to
// be appended to the log. These are encoded here for leaderLoop() to process.
// This is internal to a single server.
type configurationChangeFuture struct {
	logFuture
	req configurationChangeRequest
}

// bootstrapFuture is used to attempt a live bootstrap of the cluster. See the
// Raft object's BootstrapCluster member function for more details.
type bootstrapFuture struct {
	deferError

	// configuration is the proposed bootstrap configuration to apply.
	configuration Configuration
}

// logFuture is used to apply a log entry and waits until
// the log is considered committed.
type logFuture struct {
	deferError
	log      Log
	response interface{}
	dispatch time.Time
}

func (l *logFuture) Response() interface{} {
	return l.response
}

func (l *logFuture) Index() uint64 {
	return l.log.Index
}

type shutdownFuture struct {
	raft *Raft
}

func (s *shutdownFuture) Error() error {
	if s.raft == nil {
		return nil
	}
	s.raft.waitShutdown()
	if closeable, ok := s.raft.trans.(WithClose); ok {
		closeable.Close()
	}
	return nil
}

// userSnapshotFuture
type userSnapshotFuture struct {
	deferError

	// opener 提供了一个用来打开指定快照的函数
	opener func() (*SnapshotMeta, io.ReadCloser, error)
}

// Open 提供了一个用来打开指定快照的函数
func (u *userSnapshotFuture) Open() (*SnapshotMeta, io.ReadCloser, error) {
	if u.opener == nil {
		return nil, nil, fmt.Errorf("no snapshot available")
	}
	// Invalidate the opener so it can't get called multiple times,
	// which isn't generally safe.
	defer func() {
		u.opener = nil
	}()
	return u.opener()
}

// userRestoreFuture is used for waiting on a user-triggered restore of an
// external snapshot to complete.
type userRestoreFuture struct {
	deferError

	// meta is the metadata that belongs with the snapshot.
	meta *SnapshotMeta

	// reader is the interface to read the snapshot contents from.
	reader io.Reader
}

// reqSnapshotFuture is used for requesting a snapshot start.
// It is only used internally.
type reqSnapshotFuture struct {
	deferError

	// snapshot details provided by the FSM runner before responding
	index    uint64
	term     uint64
	snapshot FSMSnapshot
}

// restoreFuture 请求FSM进行快照恢复。仅在内部使用。
type restoreFuture struct {
	deferError
	ID string // 快照的ID
}

// verifyFuture   是用来验证当前节点是否仍然是领导者。
type verifyFuture struct {
	deferError
	notifyCh   chan *verifyFuture
	quorumSize int // 竞选获胜的数量
	votes      int // 投票数
	voteLock   sync.Mutex
}

// leadershipTransferFuture is used to track the progress of a leadership
// transfer internally.
type leadershipTransferFuture struct {
	deferError

	ID      *ServerID
	Address *ServerAddress
}

// configurationsFuture 是用来检索当前的配置的。这是 用来允许在主线程之外安全地访问这些信息。
type configurationsFuture struct {
	deferError
	configurations configurations
}

// Configuration returns the latest configuration in use by Raft.
func (c *configurationsFuture) Configuration() Configuration {
	return c.configurations.latest
}

// Index returns the index of the latest configuration in use by Raft.
func (c *configurationsFuture) Index() uint64 {
	return c.configurations.latestIndex
}

// vote 设置响应, 是不是leader
func (v *verifyFuture) vote(leader bool) {
	v.voteLock.Lock()
	defer v.voteLock.Unlock()

	// 避免已经通知
	if v.notifyCh == nil {
		return
	}

	if leader {
		// 为自己投票
		v.votes++
		if v.votes >= v.quorumSize {
			// 单节点 不会走到这里，因为replicate对自身是不会复制数据的
			// raft/raft.go:152
			fmt.Println("✈️ ✈️ ✈️", v.votes)
			v.notifyCh <- v
			v.notifyCh = nil
		}
	} else {
		v.notifyCh <- v
		v.notifyCh = nil
	}
}

// appendFuture 用于等待一个流水线上的追加条目RPC。
type appendFuture struct {
	deferError
	start time.Time
	args  *AppendEntriesRequest
	resp  *AppendEntriesResponse
}

func (a *appendFuture) Start() time.Time {
	return a.start
}

func (a *appendFuture) Request() *AppendEntriesRequest {
	return a.args
}

func (a *appendFuture) Response() *AppendEntriesResponse {
	return a.resp
}
func TestDeferFutureError(t *testing.T) {
	want := errors.New("x")
	var f deferError
	f.init()
	f.respond(want)
	if got := f.Error(); got != want {
		t.Fatalf("unexpected error result; got %#v want %#v", got, want)
	}
	if got := f.Error(); got != want {
		t.Fatalf("unexpected error result; got %#v want %#v", got, want)
	}
}
