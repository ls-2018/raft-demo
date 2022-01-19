package raft

// StableStore 是用来为关键配置提供稳定的存储，以确保安全。
type StableStore interface {
	Set(key []byte, val []byte) error
	// Get 如果没找到返回空切片
	Get(key []byte) ([]byte, error)
	SetUint64(key []byte, val uint64) error
	// GetUint64 如果没找到则返回0
	GetUint64(key []byte) (uint64, error)
}
