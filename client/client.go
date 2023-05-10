package main

import (
	"SunsetKV/common"
	"bufio"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	rand2 "math/rand"
	"net/rpc"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
	"unsafe"
)

type Clerk struct {
	serversIP []string
	clerkId   int64
	seqId     int // 用以记录每一次请求的id
	leaderId  int
	mu        *sync.Mutex
	rpcServer *rpc.Client
}

func MakeClerk(serversIP []string) *Clerk {
	ck := new(Clerk)
	ck.serversIP = serversIP

	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	ck.clerkId = bigx.Int64()
	ck.leaderId = rand2.Intn(len(serversIP))
	ck.mu = new(sync.Mutex)
	ck.serversIP = serversIP

	ck.getNewRpcServer(false)
	return ck
}

func main() {
	file, err := os.Open("config.json")
	if err != nil {
		log.Fatalln(err)
	}
	buf, err := io.ReadAll(file)
	if err != nil {
		log.Fatalln(err)
	}
	file.Close()

	type ss struct {
		ServerPort string
		RaftPort   string
	}
	var servers []ss
	err = json.Unmarshal(buf, &servers)
	if err != nil {
		log.Fatalln(err)
	}

	serversIP := make([]string, len(servers))
	pre := "localhost:"
	for i := 0; i < len(servers); i++ {
		serverIP := make([]byte, 0, 14)
		serverIP = append(serverIP, *(*[]byte)(unsafe.Pointer(&pre))...)
		serverIP = append(serverIP, *(*[]byte)(unsafe.Pointer(&servers[i].ServerPort))...)
		serversIP[i] = *(*string)(unsafe.Pointer(&serverIP))
	}

	clerk := MakeClerk(serversIP)
	defer clerk.rpcServer.Close()

	time.Sleep(time.Second)
	var flag bool
	if runtime.GOOS == "windows" {
		flag = true
	}
	rd := bufio.NewReader(os.Stdin)
	for true {
		fmt.Print("user>")

		msg, _ := rd.ReadString('\n')
		if flag {
			msg = msg[:len(msg)-2]
		} else {
			msg = msg[:len(msg)-1]
		}
		words := strings.Fields(msg)
		if len(words) == 0 {
			fmt.Println("len(command) can't be 0 ")
			continue
		}

		switch words[0] {
		case "get":
			if len(words) != 2 {
				commandErr()
				continue
			}
			value, getErr := clerk.Get(words[1])
			if getErr != nil {
				fmt.Println(getErr)
				continue
			}
			fmt.Println(value)
		case "put":
			if len(words) != 3 {
				commandErr()
				continue
			}
			putErr := clerk.Put(words[1], words[2])
			if putErr != nil {
				fmt.Println(putErr)
				continue
			}
			fmt.Printf("key[%s]'s value is %s\n", words[1], words[2])
		case "quit":
			return
		case "help":
			fmt.Println("[Command: get(to get the value)]get key\n" +
				"[Command: put(to put the value in the key)]put key value\n" +
				"[Command: quit]quit")
			continue
		default:
			commandErr()
			continue
		}
	}
}

func commandErr() {
	fmt.Println("unknown command, You can use the help to see how the command works")
}

func (ck *Clerk) Get(key string) (string, error) {

	// You will have to modify this function.

	ck.mu.Lock()
	ck.seqId++
	args := common.GetArgs{Key: key, SeqId: ck.seqId, ClerkId: ck.clerkId}
	ck.mu.Unlock()
	reply := common.GetReply{}
	//fmt.Printf("client[%d] send:[SeqId: %d][key: %s], api: Get\n",ck.clerkId, args.SeqId, key)
	for true {
		err := ck.rpcServer.Call("KVServer.Get", args, &reply)
		if err != nil {
			ck.mu.Lock()
			fmt.Println("connect err, change rpc")
			ck.getNewRpcServer(false)
			ck.mu.Unlock()
		} else {
			switch reply.Err {
			case common.OK:
				//fmt.Printf("client[%d] get from [server: %d] get key: %s, value: %s\n", ck.clerkId, ck.leaderId, key, reply.Value)
				return reply.Value, nil
			case common.ErrNoKey:
				//fmt.Println("get key error")
				return "", errors.New(common.ErrNoKey)
			case common.ErrWrongLeader:
				ck.mu.Lock()
				fmt.Println("leader err, change rpc")
				ck.getNewRpcServer(true)
				ck.mu.Unlock()
			}
		}
	}

	return "", nil
}

func (ck *Clerk) PutAppend(key string, value string, op string) error {
	// You will have to modify this function.
	ck.mu.Lock()
	ck.seqId++
	args := common.PutAppendArgs{SeqId: ck.seqId, ClerkId: ck.clerkId, Key: key, Value: value, Op: op}
	ck.mu.Unlock()
	//fmt.Printf("client[%d] send:[SeqId: %d][key: %s, value: %s], api: %s\n",ck.clerkId, args.SeqId, key, value, op)
	reply := common.GetReply{}
	for true {
		err := ck.rpcServer.Call("KVServer.PutAppend", args, &reply)
		if err != nil {
			ck.mu.Lock()
			fmt.Println("connect err, change rpc.", err)
			ck.getNewRpcServer(false)
			ck.mu.Unlock()
		} else {
			switch reply.Err {
			case common.OK:
				return nil
			case common.ErrExpired:
				errMsg := fmt.Sprintf("client[%d]' [SeqId: %d][key: %s, value: %s] is Expired, api: %s\n", ck.clerkId, args.SeqId, key, value, op)
				return errors.New(errMsg)
			case common.ErrWrongLeader:
				ck.mu.Lock()
				fmt.Println("leader err, change rpc.", reply.Err)
				ck.getNewRpcServer(true)
				ck.mu.Unlock()
			}
		}
	}

	return nil
}

func (ck *Clerk) Put(key string, value string) error {
	return ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// 获取最新的leaderServer
func (clerk *Clerk) getNewRpcServer(isConnected bool) {

	// 如果连接还可以使用则询问当前的leaderID
	if isConnected {
		args := common.PingArgs{
			ClerkId: clerk.clerkId,
			Msg:     "hi",
		}

		reply := new(common.PingReply)
		err := clerk.rpcServer.Call("KVServer.Ping", args, reply)
		if err != nil {
			clerk.rpcServer.Close()
		} else {
			if clerk.leaderId == reply.LeaderId {
				return
			}
			clerk.leaderId = reply.LeaderId - 1
			clerk.rpcServer.Close()
		}
	}

	err := errors.New("connect error")
	var rs *rpc.Client
	for err != nil {
		clerk.leaderId = (clerk.leaderId + 1) % len(clerk.serversIP)
		rs, err = rpc.Dial("tcp", clerk.serversIP[clerk.leaderId])

		args := common.PingArgs{
			ClerkId: clerk.clerkId,
			Msg:     "hi",
		}

		reply := new(common.PingReply)
		err = rs.Call("KVServer.Ping", args, reply)
		if err != nil {
			rs.Close()
		} else {
			if clerk.leaderId == reply.LeaderId {
				fmt.Printf("Get correct leader: %d\n", reply.LeaderId)
				clerk.rpcServer = rs
				return
			} else {
				clerk.leaderId = reply.LeaderId - 1
				rs.Close()
				err = errors.New("error leader")
			}
		}
	}
}
