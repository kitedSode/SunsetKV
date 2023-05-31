package main

import (
	"SunsetKV/common"
	"bytes"
	"encoding/gob"
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
	"time"
	"unsafe"
)

type KVServer struct {
	mu      sync.Mutex
	me      int           // 记录当前节点的id
	rf      *Raft         // raft服务
	applyCh chan ApplyMsg // 接收raft服务提交的日志

	maxraftstate int //若raft服务存储的日志数据大小超过了该数值，则
	//使用快照代替。快照中存储是KVServer中最新的Key-Value
	kvMap             map[string]string     // 记录Key-Value
	clerkSeqId        map[int64]int         // 记录每个clerk最新执行的seqId
	waitChans         map[int]chan Response // 任务完成时告知server
	lastIncludedIndex int                   // 最后执行快照的index
	commitIndex       int                   // 记录commit的index
}

type Response struct {
	errMsg common.Err
	value  string
}

type Op struct {
	SeqId   int
	ClerkId int64
	Key     string
	Value   string
	Api     string
}

func (kv *KVServer) Get(args common.GetArgs, reply *common.GetReply) error {

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = common.ErrWrongLeader
		return nil
	}

	//fmt.Println("receive Get")

	//kv.mu.Lock()
	// 将请求进行封装
	command := Op{
		SeqId:   args.SeqId,
		ClerkId: args.ClerkId,
		Key:     args.Key,
		Api:     "Get",
	}
	//fmt.Println("client command:", command)
	index, _, isLeader := kv.rf.Start(command)
	//fmt.Println("server start a command...")
	//kv.mu.Unlock()

	kv.mu.Lock()
	// 如果raft节点不是leader则拒绝处理
	if !isLeader {
		reply.Err = common.ErrWrongLeader
		kv.mu.Unlock()
		return nil
	}

	// 获取结果返回通道
	ch := kv.getWaitChan(index)
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChans, index)
		kv.mu.Unlock()
		//delete(kv.waitChans, index)
	}()

	select {
	case resp := <-ch:
		reply.Err = resp.errMsg
		reply.Value = resp.value
	case <-time.After(100 * time.Millisecond): // 超时则拒绝处理
		reply.Err = common.ErrWrongLeader
		//fmt.Println("outTime...")
	}

	return nil
}

func (kv *KVServer) PutAppend(args common.PutAppendArgs, reply *common.PutAppendReply) error {
	//fmt.Println("server exec putAppend")
	kv.mu.Lock()
	if kv.isExpired(args.ClerkId, args.SeqId) {
		reply.Err = common.OK
		//fmt.Println("expired...")
		kv.mu.Unlock()
		return nil
	}
	kv.mu.Unlock()

	if _, isLeader := kv.rf.GetState(); !isLeader {
		//fmt.Println("I'm not leader, leader is:", kv.rf.votedFor)
		reply.Err = common.ErrWrongLeader
		return nil
	}

	//fmt.Println("receive Put")
	command := Op{
		SeqId:   args.SeqId,
		ClerkId: args.ClerkId,
		Key:     args.Key,
		Value:   args.Value,
		Api:     args.Op,
	}
	//fmt.Println("client command:", command)
	index, _, ok := kv.rf.Start(command)
	//fmt.Println("server start a command...")

	kv.mu.Lock()
	if !ok {
		reply.Err = common.ErrWrongLeader
		kv.mu.Unlock()
		return nil
	}

	ch := kv.getWaitChan(index)
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChans, index)
		kv.mu.Unlock()
	}()

	select {
	case resp := <-ch:
		reply.Err = resp.errMsg
	case <-time.After(100 * time.Millisecond):
		//fmt.Println("timeout...")
		reply.Err = common.ErrWrongLeader
	}

	return nil
}

func (kv *KVServer) getWaitChan(index int) chan Response {
	if ch, exist := kv.waitChans[index]; exist {
		return ch
	}

	kv.waitChans[index] = make(chan Response, 1)
	return kv.waitChans[index]
}

func (kv *KVServer) isExpired(clerkId int64, seqId int) bool {
	return seqId <= kv.clerkSeqId[clerkId]
}

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		//fmt.Println("applier receive msg:", msg.Command)
		if msg.CommandValid {
			kv.mu.Lock()
			if msg.CommandIndex <= kv.lastIncludedIndex {
				kv.mu.Unlock()
				//r := fmt.Sprintf("server[%d] msg: %d, kv: %d, rf: %d\n",kv.me, msg.CommandIndex, kv.lastIncludedIndex, kv.rf.GetCommitIndex())
				//panic(r)
				return
			}

			//if msg.CommandIndex != kv.lastIncludedIndex + 1{
			//	fmt.Printf("newest index is %d, but kv.commitIndex is %d\n", msg.CommandIndex, kv.lastIncludedIndex)
			//}

			//if msg.CommandIndex != kv.commitIndex + 1{
			//	fmt.Printf("newest index is %d, but kv.commitIndex is %d\n", msg.CommandIndex, kv.lastIncludedIndex)
			//}

			//kv.commitIndex ++

			op := msg.Command.(Op)
			//fmt.Println("apply command:", op)
			//fmt.Println("commandIndex:",  msg.CommandIndex)

			// 如果该条命令不是Get并且符合线性一致性的话则给予执行
			resp := Response{errMsg: common.OK}
			if op.Api == "Get" {
				if v, exist := kv.kvMap[op.Key]; exist {
					resp.value = v
				} else {
					resp.errMsg = common.ErrNoKey
				}
			} else if !kv.isExpired(op.ClerkId, op.SeqId) {
				switch op.Api {
				case "Put":
					kv.kvMap[op.Key] = op.Value
					//fmt.Printf("Server: %d [Key: %s, Value: %s]\n", kv.me, op.Key, kv.kvMap[op.Key])
				case "Append":
					kv.kvMap[op.Key] += op.Value
					//fmt.Printf("Server: %d [Key: %s, Value: %s]\n", kv.me, op.Key, kv.kvMap[op.Key])
					//fmt.Printf("Server: %d key: %s append: %s\n", kv.me, op.Key, op.Value)
				}
				kv.clerkSeqId[op.ClerkId] = op.SeqId
			} else {
				resp.errMsg = common.ErrExpired
				//fmt.Printf("seqId[%d], key: %s, value: %s is Expired!\n", op.SeqId, op.Key, op.Value)
			}

			kv.lastIncludedIndex = msg.CommandIndex
			// 如果raft存储的日志数据超过了server给定的maxraftstate的话向raft发送快照
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
				kv.rf.Snapshot(kv.lastIncludedIndex, kv.getPersistData())
			}
			// 如果对应的raft服务器不是leader的话则不用发送
			if _, ok := kv.rf.GetState(); !ok {
				kv.mu.Unlock()
				continue
			}
			ch := kv.getWaitChan(msg.CommandIndex)
			kv.mu.Unlock()

			ch <- resp
		}

		if msg.SnapshotValid {

			kv.mu.Lock()
			if msg.SnapshotIndex <= kv.lastIncludedIndex {
				kv.mu.Unlock()
				//r := fmt.Sprintf("server[%d] msg: %d, kv: %d, rf: %d\n",kv.me, msg.SnapshotIndex, kv.lastIncludedIndex, kv.rf.GetCommitIndex())
				//panic(r)
				return
			}

			kv.readPersist(msg.Snapshot)
			kv.lastIncludedIndex = msg.SnapshotIndex
			kv.mu.Unlock()
		}
	}
}

// 将kvMap以及clerkSeqId做持久化操作
func (kv *KVServer) getPersistData() []byte {
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)
	encoder.Encode(kv.kvMap)
	encoder.Encode(kv.clerkSeqId)
	return w.Bytes()
}

// 读取由raft传来的快照来恢复之前的数据
func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	kvMap := make(map[string]string)
	clerkSeqId := make(map[int64]int)

	err := d.Decode(&kvMap)
	if err != nil {
		panic(err)
	}

	err = d.Decode(&clerkSeqId)
	if err != nil {
		panic(err)
	}

	//if d.Decode(kvMap) != nil d.Decode(clerkSeqId) != nil{
	//	//panic("decode err!")
	//}else{
	kv.kvMap = kvMap
	kv.clerkSeqId = clerkSeqId
	//}
}

func (kvs *KVServer) Ping(args common.PingArgs, reply *common.PingReply) error {
	//fmt.Printf("reveive from client[%d]'s ping![%s]\n", args.ClerkId, args.Msg)
	_, isLeader := kvs.rf.GetState()
	if isLeader {
		//reply.Msg = fmt.Sprintf("server for [%d]:yes, I'm leader!", kvs.me)
		reply.IsLeader = true
	} else {
		//reply.Msg = fmt.Sprintf("server for [%d]:sorry, I'm not leader.Leader is %d", kvs.me, kvs.rf.votedFor)
		reply.IsLeader = false
	}

	return nil
}

func StartKVServer(id int) *KVServer {
	gob.Register(Op{})
	kvs := new(KVServer)
	kvs.me = id
	kvs.maxraftstate = 2048
	kvs.applyCh = make(chan ApplyMsg)
	kvs.waitChans = make(map[int]chan Response)
	kvs.clerkSeqId = make(map[int64]int)
	kvs.kvMap = make(map[string]string)

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
	go kvs.applier()
	return kvs
}

func (kvs *KVServer) kill() {
	kvs.rf.Kill()
	kvs.mu.Lock()
	for ch := range kvs.waitChans {
		close(kvs.waitChans[ch])
	}
	kvs.waitChans = nil
	kvs.kvMap = nil
	kvs.clerkSeqId = nil
	kvs.mu.Unlock()

}

func main() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGHUP,
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	id, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalln(err)
	}

	kvs := StartKVServer(id)
	defer kvs.kill()
	<-sig

}
