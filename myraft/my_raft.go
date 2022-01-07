package myraft

import (
	"fmt"
	"raft-demo/raft"

	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	fsm "raft-demo/fsm"

	raftboltdb "raft-demo/raft-boltdb"
)

func NewMyRaft(raftAddr, raftId, raftDir string) (*raft.Raft, *fsm.Fsm, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(raftId)

	addr, _ := net.ResolveTCPAddr("tcp", raftAddr)
	// 传输端点
	transport, _ := raft.NewTCPTransport(raftAddr, addr, 2, 5*time.Second, os.Stderr)
	// 日志存储
	logStore, _ := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft-log.db"))
	// 表存储
	stableStore, _ := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft-stable.db"))
	// 利用复制的日志
	fm := fsm.NewFsm()
	// 快照存储
	snapshots, _ := raft.NewFileSnapshotStore(raftDir, 2, os.Stderr)
	rf, _ := raft.NewRaft(config, fm, logStore, stableStore, snapshots, transport)
	fmt.Println(raft.HasExistingState(logStore, stableStore, snapshots))
	return rf, fm, nil
}

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
