package datanode

import (
	"io"
	"log"
	"strings"

	. "github.com/michaelmaltese/golang-distributed-filesystem/common"
)

func sendBlock(dn *DataNodeState, blockID BlockID, peers []string) {
	if err := dn.Manager.LockRead(blockID); err != nil {
		log.Println("Couldn't lock", blockID)
		return
	}
	defer dn.Manager.UnlockRead(blockID)

	var peer *RPCClient
	var forwardTo []string
	var err error
	// Find an online peer
	for i, addr := range peers {
		peer, err = NewRPCClient(addr)
		if err == nil {
			defer peer.Close()
			forwardTo = append(peers[:i], peers[i+1:]...)
			break
		}
	}
	// TODO: Wrong. Can't compare interface to nil
	if peer == nil {
		log.Println("Couldn't forward block",
			blockID,
			"to any DataNodes in:",
			strings.Join(peers, " "))
		return
	}

	size, err := dn.Store.BlockSize(blockID)
	if err != nil {
		log.Fatal("Stat error: ", err)
	}

	err = peer.Call("Forward",
		&ForwardBlock{blockID, forwardTo, size},
		nil)
	if err != nil {
		log.Fatal("Forward error: ", err)
	}

	reader, err := dn.Store.OpenBlock(blockID)
	if err != nil {
		log.Fatal("Open block error: ", err)
	}
	_, err = io.Copy(peer, reader)
	if err != nil {
		log.Fatalln("Copy error:", err)
	}

	hash, err := dn.Store.ReadChecksum(blockID)
	if err != nil {
		log.Fatalln("Reading checksum:", err)
	}
	err = peer.Call("Confirm", hash, nil)
	if err != nil {
		log.Fatal("Confirm error: ", err)
	}
}

func (self *DataNodeState) BlockForwarder() {
	for {
		f := <-self.forwardingBlocks
		sendBlock(self, f.BlockID, f.Nodes)
	}
}
