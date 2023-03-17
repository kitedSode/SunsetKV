package main

import (
	"SunsetKV/common"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"unsafe"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *Raft
	applyCh chan ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap             map[string]string
	clerkSeqId        map[int64]int         // 记录每个clerk最新执行的seqId
	waitChans         map[int]chan Response // 任务完成时告知server
	lastIncludedIndex int                   // 最后执行快照的index
	commitIndex       int                   // 记录commit的index

	data map[int]string
}

type Response struct {
	errMsg common.Err
	value  string
}

func (kvs *KVServer) Ping(args common.GetArgs, reply *common.GetReply) error {
	return nil
}

func StartKVServer(id int) {
	kvs := new(KVServer)
	kvs.me = id
	kvs.maxraftstate = 1024
	kvs.applyCh = make(chan ApplyMsg)

	// 读取配置文件，得到每一个KVServer以及RaftServer的IP
	file, err := os.Open("config.json")
	if err != nil {
		log.Fatalln(err)
	}
	buf, err := io.ReadAll(file)
	if err != nil {
		log.Fatalln(err)
	}

	// 解析配置文件
	type ss struct {
		ServerPort string
		RaftPort   string
	}
	var servers []ss
	err = json.Unmarshal(buf, &servers)
	if err != nil {
		log.Fatalln(err)
	}

	raftServersIP := make([]string, len(servers))
	pre := "localhost:"
	var kvserverAddr string
	for i := 0; i < len(servers); i++ {
		if i == kvs.me {
			kvsIP := make([]byte, 0, 14)
			kvsIP = append(kvsIP, *(*[]byte)(unsafe.Pointer(&pre))...)
			kvsIP = append(kvsIP, *(*[]byte)(unsafe.Pointer(&servers[i].ServerPort))...)
			kvserverAddr = *(*string)(unsafe.Pointer(&kvsIP))
		}
		raftIP := make([]byte, 0, 14)
		raftIP = append(raftIP, *(*[]byte)(unsafe.Pointer(&pre))...)
		raftIP = append(raftIP, *(*[]byte)(unsafe.Pointer(&servers[i].RaftPort))...)
		raftServersIP[i] = *(*string)(unsafe.Pointer(&raftIP))
	}
	//TODO 暂时先临时创建一个后期再考虑持久化
	ps := MakePersister()

	kvs.rf = MakeRaftServer(raftServersIP, kvs.me, ps, kvs.applyCh)

	// 为KVServer注册rpc服务并启动
	err = rpc.Register(kvs)
	if err != nil {
		log.Fatalln(err)
	}
	conn, err := net.Listen("tcp", kvserverAddr)
	if err != nil {
		log.Fatalln(err)
	}
	go func() {
		for {
			client, err := conn.Accept()
			if err != nil {
				log.Fatalln(err)
			}

			go rpc.ServeConn(client)
		}
	}()

	fmt.Printf("KVServer [%d] is running!\n", id)

}

func main() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGHUP,
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	id, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalln(err)
	}

	StartKVServer(id)
	<-sig
}
