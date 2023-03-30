package main

import (
	"fmt"
	"net/rpc"
	"sync"
	"sync/atomic"
)

const (
	PeerTryConnecting int32 = iota // 连接关闭
	PeerClosed
	PeerConnected
)

type Peer struct {
	id        int
	adder     string
	rpcServer *rpc.Client
	state     int32
	mu        sync.Mutex
}

func MakePeer(adders []string, me int) []*Peer {
	peers := make([]*Peer, len(adders))
	wg := &sync.WaitGroup{}

	for i := 0; i < len(adders); i++ {
		if i == me {
			continue
		}

		wg.Add(1)

		peer := &Peer{
			id:        i,
			adder:     adders[i],
			rpcServer: nil,
			state:     PeerTryConnecting,
			mu:        sync.Mutex{},
		}
		peers[i] = peer

		go func() {
			rpcServer, err := rpc.Dial("tcp", peer.adder)
			wg.Done()

			//如果连接失败则继续重试
			if err != nil {
				peer.state = PeerClosed
				peer.ReConnect()
			} else {
				peer.rpcServer = rpcServer
				peer.state = PeerConnected
			}

			fmt.Println("connect finish:", peer.adder)
		}()

	}

	wg.Wait()
	for i := 0; i < len(peers); i++ {
		if i == me {
			continue
		}
	}
	return peers
}

func PeersCreateConnect(peers []*Peer, me int) {
	//fmt.Println("peers:", peers)
	wg := new(sync.WaitGroup)
	for i := 0; i < len(peers); i++ {
		if i == me {
			continue
		}

		wg.Add(1)
		args := ConnArgs{
			ID: me,
		}

		//fmt.Printf("send connect[%v]\n", args)
		go func(n int) {
			peers[n].Call("Raft.Connect", args, new(ConnReply))
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func (p *Peer) ReConnect() {
	ps := atomic.LoadInt32(&p.state)
	switch ps {
	case PeerTryConnecting:
		return
	case PeerConnected:
		// 如果Ping失败则执行重连操作
		p.mu.Lock()
		reply := new(byte)
		if err := p.rpcServer.Call("Raft.Ping", 0, reply); err == nil {
			p.mu.Unlock()
			return
		}

		p.state = PeerTryConnecting
		p.rpcServer.Close()
		go func() {
			for true {
				rpcServer, err := rpc.Dial("tcp", p.adder)
				if err == nil {
					p.rpcServer = rpcServer
					p.state = PeerConnected
					fmt.Printf("connect to rpcServer[%s] successfully!\n", p.adder)
					break
				}
			}
			p.mu.Unlock()
		}()
	case PeerClosed:
		p.mu.Lock()
		p.state = PeerTryConnecting
		go func() {
			for true {
				rpcServer, err := rpc.Dial("tcp", p.adder)
				if err == nil {
					p.rpcServer = rpcServer
					p.state = PeerConnected
					fmt.Printf("connect to rpcServer[%s] successfully!\n", p.adder)
					break
				}
			}
			p.mu.Unlock()
		}()
	}
}

func (p *Peer) Call(method string, args any, reply any) bool {
	if atomic.LoadInt32(&p.state) != PeerConnected {
		return false
	}
	err := p.rpcServer.Call(method, args, reply)
	if err != nil {
		p.ReConnect()
		//fmt.Printf("RPC[%s] ERROR and ERROR is %v\n", p.adder, err)
		return false
	}

	return true
}
