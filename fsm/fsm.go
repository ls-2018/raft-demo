package fsm

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"raft-demo/raft"
	"strings"
	"sync"
)

type Fsm struct {
	DataBase database
}

func NewFsm() *Fsm {
	fsm := &Fsm{
		DataBase: NewDatabase(),
	}
	return fsm
}

func (f *Fsm) Apply(l *raft.Log) interface{} {
	fmt.Println("apply data:", string(l.Data))
	data := strings.Split(string(l.Data), ",")
	op := data[0]
	if op == "set" {
		key := data[1]
		value := data[2]
		f.DataBase.Set(key, value)
	}
	return nil
}

func (f *Fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &f.DataBase, nil
}

// Restore 集群重启时，需要从快照中恢复数据
func (f *Fsm) Restore(old io.ReadCloser) error {
	read := bufio.NewReader(old)
	for {
		c, _, err := read.ReadLine()
		if c == nil {
			return nil
		}
		var a [2]string
		err = json.Unmarshal(c, &a)
		if err != nil {
			return err
		}
		_, ok := f.DataBase.Data[a[0]]
		if !ok {
			f.DataBase.Data[a[0]] = a[1]
		}
		if err == io.EOF {
			return nil
		}
		if err != nil && err != io.EOF {
			return err
		}
	}
}

type database struct {
	Data map[string]string
	mu   sync.Mutex
}

func NewDatabase() database {
	return database{
		Data: make(map[string]string),
	}
}
func (d *database) Persist(sink raft.SnapshotSink) error {
	d.mu.Lock()
	start := true
	for k, v := range d.Data {
		temp := [2]string{k, v}
		data, err := json.Marshal(temp)
		if err != nil {
			return err
		}
		if start {
			sink.Write(data)
			start = false
		} else {
			sink.Write([]byte{'\n'})
			sink.Write(data)
		}
	}
	d.mu.Unlock()
	sink.Close()
	return nil
}

func (d *database) Get(key string) string {
	d.mu.Lock()
	value := d.Data[key]
	d.mu.Unlock()
	return value
}

func (d *database) Set(key, value string) {
	d.mu.Lock()
	d.Data[key] = value
	d.mu.Unlock()
}

// Release 打完快照后要做的事情
func (d *database) Release() {

}
