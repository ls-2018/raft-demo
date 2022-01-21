package raftboltdb

import (
	"fmt"
	"github.com/boltdb/bolt"
	"raft-demo/raft"
	. "raft-demo/raft-boltdb/var"
)

const (
	// 在db文件上使用的权限。这只在数据库文件不存在而需要创建时使用。
	dbFileMode = 0600
)

var _ raft.LogStore = &BoltStore{}
var _ raft.StableStore = &BoltStore{}

// BoltStore 为Raft提供对BoltDB的访问，以存储和检索日志条目。它还提供了键/值存储，并可作为LogStore和StableStore使用。
type BoltStore struct {
	// conn 是数据库的底层句柄。
	conn *bolt.DB

	// bolt数据库文件的路径
	path string
}

// Options 包含用于打开BoltDB的所有配置。
type Options struct {
	// Path 是要使用的BoltDB的文件路径
	Path string

	// BoltOptions 包含任何你可能想要指定的BoltDB选项  [例如：OPEN超时]。
	BoltOptions *bolt.Options

	// NoSync 导致数据库在每次写入日志后跳过fsync调用。这是不安全的，所以应该谨慎使用。
	NoSync bool
}

// readOnly 如果包含的bolt选项说要在只读模式下打开DB [这对想要检查日志的工具很有用] 。
func (o *Options) readOnly() bool {
	return o != nil && o.BoltOptions != nil && o.BoltOptions.ReadOnly
}

// NewBoltStore 接收一个文件路径并返回一个连接的Raft后端。
func NewBoltStore(path string) (*BoltStore, error) {
	return New(Options{Path: path})
}

// New 使用所提供的选项来打开BoltDB，并准备将其用作raft的后端。
func New(options Options) (*BoltStore, error) {
	handle, err := bolt.Open(options.Path, dbFileMode, options.BoltOptions)
	if err != nil {
		return nil, err
	}
	handle.NoSync = options.NoSync

	store := &BoltStore{
		conn: handle,
		path: options.Path,
	}

	// 如果store是以只读方式打开的，就不要试图创建水桶。
	if !options.readOnly() {
		// 不是只读模式
		// 设置我们的bucket
		if err := store.initialize(); err != nil {
			store.Close()
			return nil, err
		}
	}
	return store, nil
}

// initialize 用来设置所有的bucket
func (b *BoltStore) initialize() error {
	// 开始一个可写的事务
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// logs
	if _, err := tx.CreateBucketIfNotExists(DbLogs); err != nil {
		return err
	}
	// conf
	if _, err := tx.CreateBucketIfNotExists(DbConf); err != nil {
		return err
	}

	return tx.Commit()
}

// Close 优雅的关闭数据库连接
func (b *BoltStore) Close() error {
	return b.conn.Close()
}

// FirstIndex 从raft log 中返回第一个记录的value
// 如果第一个log日志值不符合条件，类似 a ；会产生panic
func (b *BoltStore) FirstIndex() (uint64, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(DbLogs).Cursor()
	if first, _ := curs.First(); first == nil {
		return 0, nil
	} else {
		return bytesToUint64(first), nil
	}
}

// LastIndex 从raft log 中返回最后一个记录的value
// 如果第一个log日志值不符合条件，类似 a ；会产生panic
func (b *BoltStore) LastIndex() (uint64, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(DbLogs).Cursor()
	if last, _ := curs.Last(); last == nil {
		return 0, nil
	} else {
		return bytesToUint64(last), nil
	}
}

// GetLog 是用来从BoltDB检索指定索引的日志。在bolt层面来说，即为指定key的数据
func (b *BoltStore) GetLog(idx uint64, log *raft.Log) error {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(DbLogs)
	val := bucket.Get(uint64ToBytes(idx))

	if val == nil {
		return raft.ErrLogNotFound
	}
	return decodeMsgPack(val, log)
}

// StoreLog 存储单条raft日志
func (b *BoltStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

// StoreLogs 存储多条raft日志
func (b *BoltStore) StoreLogs(logs []*raft.Log) error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, log := range logs {
		fmt.Printf("------------->  log  存储数据 key: %+v,value :%+v\n", log.Index, raft.TransLog(log))
		key := uint64ToBytes(log.Index)
		val, err := encodeMsgPack(log)
		if err != nil {
			return err
		}
		bucket := tx.Bucket(DbLogs)
		if err := bucket.Put(key, val.Bytes()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// DeleteRange 是用来删除一个给定范围内的日志，包括在内。
func (b *BoltStore) DeleteRange(min, max uint64) error {
	minKey := uint64ToBytes(min)

	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	curs := tx.Bucket(DbLogs).Cursor()
	for k, _ := curs.Seek(minKey); k != nil; k, _ = curs.Next() {
		// 处理超出范围的日志索引
		if bytesToUint64(k) > max {
			break
		}

		// 删除范围内的日志索引
		if err := curs.Delete(); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// Set 是用来在raft日志之外设置一个键/值集的。
func (b *BoltStore) Set(k, v []byte) error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	fmt.Printf("+++++++++++++> state 存储数据 key: %s,value :%s\n", string(k), string(v))

	bucket := tx.Bucket(DbConf)
	if err := bucket.Put(k, v); err != nil {
		return err
	}

	return tx.Commit()
}

// Get 获取键值数据
func (b *BoltStore) Get(k []byte) ([]byte, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(DbConf)
	val := bucket.Get(k)

	if val == nil {
		return nil, ErrKeyNotFound
	}
	return append([]byte(nil), val...), nil
}

// SetUint64 设置，但value是uint64
func (b *BoltStore) SetUint64(key []byte, val uint64) error {
	return b.Set(key, uint64ToBytes(val))
}

// GetUint64 返回uint64类型的数据
func (b *BoltStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}

// Sync 在数据库文件句柄上执行fsync。这在正常操作下是没有必要的，除非启用NoSync，在这种情况下，数据库文件会强制与磁盘同步。
func (b *BoltStore) Sync() error {
	return b.conn.Sync()
}
