package raft

import (
	"bytes"
	crand "crypto/rand"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
)

func init() {
	// 确保我们为伪随机发生器使用一个高熵值的种子
	rand.Seed(newSeed())
}

// 从加密的随机源返回一个int64。可用于为math/rand的种子源。
func newSeed() int64 {
	r, err := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		panic(fmt.Errorf("读取随机字节失败: %v", err))
	}
	return r.Int64()
}

// randomTimeout 返回一个介于x与2x之间随机时间
func randomTimeout(minVal time.Duration) <-chan time.Time {
	if minVal == 0 {
		return nil
	}
	extra := time.Duration(rand.Int63()) % minVal
	return time.After(minVal + extra)
}

// min returns the minimum.
func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

// max returns the maximum.
func max(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}

// generateUUID is used to generate a random UUID.
func generateUUID() string {
	buf := make([]byte, 16)
	if _, err := crand.Read(buf); err != nil {
		panic(fmt.Errorf("failed to read random bytes: %v", err))
	}

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		buf[0:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:16])
}

// asyncNotifyCh 是用来做一个异步的通道，发送到一个单一的通道而不阻塞。
func asyncNotifyCh(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// drainNotifyCh 清空一个channel，而不发生阻塞。并返回它是否收到任何东西。
func drainNotifyCh(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// asyncNotifyBool is used to do an async notification
// on a bool channel.
func asyncNotifyBool(ch chan bool, v bool) {
	select {
	case ch <- v:
	default:
	}
}

// overrideNotifyBool
// 用于通知一个bool通道，但如果值存在，则覆盖现有的值，ch必须是1项缓冲通道。该方法不支持多个并发的调用。
func overrideNotifyBool(ch chan bool, v bool) {
	select {
	case ch <- v: // 如果channel 没值，走这
		// 值已发送，完成
	case <-ch:
		// 如果channel 有值，需要排除旧的，添加新的
		select {
		case ch <- v:
		default:
			panic("race: channel 并发调用了")
		}
	}
}

// Decode reverses the encode operation on a byte slice input.
func decodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

// Encode writes an encoded object to a new bytes buffer.
func encodeMsgPack(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}

// backoff is used to compute an exponential backoff
// duration. Base time is scaled by the current round,
// up to some maximum scale factor.
func backoff(base time.Duration, round, limit uint64) time.Duration {
	power := min(round, limit)
	for power > 2 {
		base *= 2
		power--
	}
	return base
}

// 需要进行排序[]uint64，用于确定commit。
type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type Temp struct {
	Index      uint64
	Term       uint64
	Type       LogType
	Data       Configuration
	Extensions string
	AppendedAt time.Time
}

var xxxx Transport

func SetTrans(x Transport) {
	xxxx = x
}
func TransLog(log *Log) (temp Temp) {
	c, _ := decodePeers(log.Data, xxxx)
	temp = Temp{
		Index:      log.Index,
		Term:       log.Term,
		Type:       log.Type,
		Data:       c,
		Extensions: string(log.Extensions),
		AppendedAt: log.AppendedAt,
	}
	return
}
