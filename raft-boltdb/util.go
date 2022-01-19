package raftboltdb

import (
	"bytes"
	"encoding/binary"
	"github.com/hashicorp/go-msgpack/codec"
)

// 解码消息体。
func decodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

// 编码消息体
func encodeMsgPack(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
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
