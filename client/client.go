package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	rand2 "math/rand"
	"os"
	"sync"
	"unsafe"
)

type Clerk struct {
	serversIP []string
	// You will have to modify this struct.
	clerkId  int64
	seqId    int // 用以记录每一次请求的id
	leaderId int
	mu       sync.Mutex
}

func MakeClerk(serversIP []string) *Clerk {
	ck := new(Clerk)
	ck.serversIP = serversIP

	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	ck.clerkId = bigx.Int64()
	ck.leaderId = rand2.Intn(len(serversIP))
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
	fmt.Println(serversIP)
}
