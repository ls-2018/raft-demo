package _var

import "errors"

var (
	// 我们执行事务的的Bucket名称
	DbLogs = []byte("logs")
	DbConf = []byte("conf")
	// ErrKeyNotFound 一个错误表明一个给定的键不存在
	ErrKeyNotFound = errors.New("not found")
)
