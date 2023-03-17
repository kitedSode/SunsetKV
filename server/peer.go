package main

import (
	"log"
	"net/rpc"
)

type Peer struct {
	addr      string
	rpcServer *rpc.Client
}

func MakePeer(addrs []string, me int) []*Peer {
	peers := make([]*Peer, len(addrs))
	for i := 0; i < len(addrs); i++ {
		if i == me {
			continue
		}
		rpcServer, err := rpc.Dial("tcp", addrs[i])
		if err != nil {
			log.Fatalln(err)
		}

		peers[i] = &Peer{
			addr:      addrs[i],
			rpcServer: rpcServer,
		}
	}

	return peers
}

func (p *Peer) Call(method string, args any, reply any) bool {
	err := p.rpcServer.Call(method, args, reply)
	if err != nil {
		//fmt.Printf("RPC[%s] ERROR and ERROR is %v\n", p.addr, err)
		return false
	}

	return true
}
