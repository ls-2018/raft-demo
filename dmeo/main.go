package main

import (
	"encoding/binary"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/hashicorp/go-hclog"
	"os"
	"sync"
	"time"
)

var dbConf = []byte("MyBucket")

func main() {
	var a = sync.WaitGroup{}
}
func mai2n() {
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "raft-net",
		Output: os.Stdout,
		Level:  hclog.DefaultLevel,
	})
	logger.Info("asd")
	//	2022-01-19T10:54:08.611+0800 [INFO]  raft-net: asd
	db, _ := bolt.Open("./node/my.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
	defer db.Close()
	logger.Info("", d(db))
	tx, _ := db.Begin(true)
	bucket := tx.Bucket(dbConf)
	if err := bucket.Put([]byte("a"), []byte("ab1")); err != nil {
	}
	if err := bucket.Put([]byte("a"), []byte("ab5")); err != nil {
	}
	tx.Commit()

	tx, _ = db.Begin(true)
	bucket = tx.Bucket(dbConf)
	fmt.Println(string(bucket.Get([]byte("a"))))
	tx.Rollback()

	//currentTerm, err := db.GetUint64(keyCurrentTerm)
	//fmt.Println(f(db))
}

func d(db *bolt.DB) error {
	// Start a writable transaction.
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.CreateBucket(dbConf)
	if err != nil {
		return err
	}

	// Commit the transaction and check for error.
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func f(db *bolt.DB) (uint64, error) {
	// FirstIndex 从raft log 中返回一个一个index
	tx, err := db.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbConf).Cursor()
	if first, _ := curs.First(); first == nil {
		return 0, nil
	} else {
		return bytesToUint64(first), nil
	}
}

// 将数据转换成uint64值
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// 将uint64数据转换成8字节数据
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}
