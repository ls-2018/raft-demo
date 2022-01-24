package myraft

import (
	"log"
	"net"
	"os"
	"path/filepath"
	fsm "raft-demo/fsm"
	"raft-demo/raft"
	"strings"
	"time"
)

func NewMyRaft(raftAddr, raftId, raftDir string) (*raft.Raft, *fsm.Fsm, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(raftId)           // 类型转换
	addr, err := net.ResolveTCPAddr("tcp", raftAddr) // string -> TCPAddr{}
	if err != nil {
		log.Fatal(err)
	}
	// 传输端点
	var transport raft.Transport
	transport, err = raft.NewTCPTransport(raftAddr, addr, 2, 5*time.Second, os.Stderr)
	if err != nil {
		log.Fatal(err)
	}
	raft.SetTrans(transport)

	// 日志存储
	var logStore *raft.BoltStore
	logStore, err = raft.NewBoltStore(filepath.Join(raftDir, "raft-log.db"))
	if err != nil {
		log.Fatal(err)
	}
	// 表存储
	var stableStore *raft.BoltStore
	stableStore, err = raft.NewBoltStore(filepath.Join(raftDir, "raft-stable.db"))
	if err != nil {
		log.Fatal(err)
	}
	// 利用复制的日志
	fm := fsm.NewFsm()
	// 快照存储
	var snapshots *raft.FileSnapshotStore
	snapshots, err = raft.NewFileSnapshotStore(raftDir, 2, os.Stderr)
	if err != nil {
		log.Fatal(err)
	}
	var rf *raft.Raft
	rf, err = raft.NewRaft(config, fm, logStore, stableStore, snapshots, transport)
	if err != nil {
		log.Fatal(err)
	}
	return rf, fm, nil
}

// Bootstrap 集群节点信息引导
func Bootstrap(rf *raft.Raft, raftId, raftAddr, raftCluster string) {
	servers := rf.GetConfiguration().Configuration().Servers
	if len(servers) > 0 {
		return
	}
	peerArray := strings.Split(raftCluster, ",")
	if len(peerArray) == 0 {
		return
	}

	var configuration raft.Configuration
	for _, peerInfo := range peerArray {
		peer := strings.Split(peerInfo, "/")
		id := peer[0]
		addr := peer[1]
		server := raft.Server{
			ID:      raft.ServerID(id),
			Address: raft.ServerAddress(addr),
		}
		configuration.Servers = append(configuration.Servers, server)
	}

	rf.BootstrapCluster(configuration)
	return
}
