package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"raft-demo/fsm"
	"raft-demo/myraft"
	"raft-demo/raft"
	"sync/atomic"
	"time"
)

var (
	httpAddr    string
	raftAddr    string
	raftId      string
	raftCluster string
	raftDir     string
)

var (
	isLeader int64
)

func init() {
	flag.StringVar(&httpAddr, "http_addr", "127.0.0.1:10001", "http listen addr")
	flag.StringVar(&raftAddr, "raft_addr", "127.0.0.1:10000", "raft listen addr")
	flag.StringVar(&raftId, "raft_id", "1", "raft id")
	flag.StringVar(&raftCluster, "raft_cluster", "1/127.0.0.1:10000,2/127.0.0.1:20000", "cluster info")
	//flag.StringVar(&raftCluster, "raft_cluster", "1/127.0.0.1:10000", "cluster info")
}

// go build -mod vendor
// ./raft-demo --http_addr=127.0.0.1:10001 --raft_addr=127.0.0.1:10000 --raft_id=1 --raft_cluster=1/127.0.0.1:10000,2/127.0.0.1:20000,3/127.0.0.1:30000
// ./raft-demo --http_addr=127.0.0.1:20001 --raft_addr=127.0.0.1:20000 --raft_id=2 --raft_cluster=1/127.0.0.1:10000,2/127.0.0.1:20000
// ./raft-demo --http_addr=127.0.0.1:30001 --raft_addr=127.0.0.1:30000 --raft_id=3 --raft_cluster=1/127.0.0.1:10000,2/127.0.0.1:20000,3/127.0.0.1:30000
// curl http://127.0.0.1:10001/set?key=test_key&value=test_value
// curl http://127.0.0.1:10001/get?key=test_key

func main() {
	flag.Parse()
	// 初始化配置
	raftDir := "node/raft_" + raftId
	os.MkdirAll(raftDir, 0700)

	// 初始化raft
	myRaft, fm, err := myraft.NewMyRaft(raftAddr, raftId, raftDir)
	if err != nil {
		fmt.Println("NewMyRaft error ", err)
		os.Exit(1)
		return
	}

	// 启动raft
	myraft.Bootstrap(myRaft, raftId, raftAddr, raftCluster)

	// 监听leader变化
	go func() {
		for leader := range myRaft.LeaderCh() { // 只有此一处，用于监听leader变化
			if leader {
				atomic.StoreInt64(&isLeader, 1)
			} else {
				atomic.StoreInt64(&isLeader, 0)
			}
		}
	}()

	// 启动http server
	httpServer := HttpServer{
		ctx: myRaft,
		fsm: fm,
	}

	http.HandleFunc("/set", httpServer.Set)
	http.HandleFunc("/get", httpServer.Get)
	go func() {
		time.Sleep(time.Second * 5)
		for i := 0; i < 200; i++ {
			data := "set" + "," + fmt.Sprintf("a+%d", i) + "," + fmt.Sprintf("%d", i)

			value := httpServer.fsm.DataBase.Get(fmt.Sprintf("a+%d", i))
			if value != "" {
				fmt.Printf("GET %s---->%s\n", fmt.Sprintf("a+%d", i), value)
			} else {
				future := httpServer.ctx.Apply([]byte(data), 5*time.Second)
				if err := future.Error(); err != nil {
				}
			}

		}
		snapshot, err := httpServer.ctx.TakeSnapshot()
		if err != nil {
			return
		}
		fmt.Println("-------->", snapshot)

	}()
	http.ListenAndServe(httpAddr, nil)

	// 关闭raft
	shutdownFuture := myRaft.Shutdown()
	if err := shutdownFuture.Error(); err != nil {
		fmt.Printf("shutdown raft error:%v \n", err)
	}

	// 退出http server
	fmt.Println("shutdown kv http server")
}

type HttpServer struct {
	ctx *raft.Raft
	fsm *fsm.Fsm
}

func (h HttpServer) Set(w http.ResponseWriter, r *http.Request) {
	if atomic.LoadInt64(&isLeader) == 0 {
		fmt.Fprintf(w, "not leader")
		return
	}
	vars := r.URL.Query()
	key := vars.Get("key")
	value := vars.Get("value")
	if key == "" || value == "" {
		fmt.Fprintf(w, "error key or value")
		return
	}
	data := "set" + "," + key + "," + value
	fmt.Println("------------------------------------------ 1.应用层kv server收到请求，提交到raft层，开始复制，需要复制的data:", data)
	future := h.ctx.Apply([]byte(data), 5*time.Second)
	if err := future.Error(); err != nil {
		fmt.Fprintf(w, "error:"+err.Error())
		fmt.Println("应用层kv server提交失败，data:", data)
		return
	}
	fmt.Println("------------------------------------------ 7.应用层kv server复制数据且提交成功返回给客户端请求处理成功，处理的data:", data)
	fmt.Fprintf(w, "ok")
	return
}

func (h HttpServer) Get(w http.ResponseWriter, r *http.Request) {
	vars := r.URL.Query()
	key := vars.Get("key")
	if key == "" {
		fmt.Fprintf(w, "error key")
		return
	}
	value := h.fsm.DataBase.Get(key)
	fmt.Fprintf(w, value)
	return
}
