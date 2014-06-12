package datanode

import (
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"
	"time"

	. "github.com/michaelmaltese/golang-distributed-filesystem/comm"
	"github.com/michaelmaltese/golang-distributed-filesystem/util"
)

type DataNodeState struct {
	mutex sync.Mutex
	newBlocks []BlockID
	deadBlocks []BlockID
	forwardingBlocks chan ForwardBlock
	NodeID NodeID
	Store BlockStore
	Manager BlockIntents
	heartbeatInterval time.Duration
}

func (self *DataNodeState) HaveBlocks(blockIDs []BlockID) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.newBlocks = append(self.newBlocks, blockIDs...)
}

func (self *DataNodeState) DontHaveBlocks(blockIDs []BlockID) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.deadBlocks = append(self.deadBlocks, blockIDs...)
}

func (self *DataNodeState) RemoveBlock(block BlockID) {
	log.Println("Removing block '" + block + "'")
	err := self.Store.DeleteBlock(block)
	if err != nil {
		log.Fatalln("Deleting block", block, "->", err)
	}
	self.DontHaveBlocks([]BlockID{block})
}

func (self *DataNodeState) DrainNewBlocks() []BlockID {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	newBlocks := self.newBlocks
	self.newBlocks = []BlockID{}
	return newBlocks
}

func (self *DataNodeState) DrainDeadBlocks() []BlockID {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	deadBlocks := self.deadBlocks
	self.deadBlocks = []BlockID{}
	return deadBlocks
}

func heartbeat() {
	for {
		tick()
		time.Sleep(State.heartbeatInterval)
	}
}

func tick() {
	conn, err := net.Dial("tcp", "[::1]:5051")
	if err != nil {
		// MetaDataNode offline
		log.Println("Couldn't connect to leader")
		State.NodeID = ""
		return
	}
	codec := jsonrpc.NewClientCodec(conn)
	if Debug {
		codec = util.LoggingClientCodec(
			conn.RemoteAddr().String(),
			codec)
	}
	client := rpc.NewClientWithCodec(codec)
	defer client.Close()

	log.Println("Heartbeat...")
	if len(State.NodeID) == 0 {
		log.Println("Re-reading blocklist")
		blocks, err := State.Store.ReadBlockList()
		if err != nil {
			log.Fatalln("Getting blocklist:", err)
		}
		err = client.Call("PeerSession.Register", &RegistrationMsg{Port, blocks}, &State.NodeID)
		if err != nil {
			log.Println("Registration error:", err)
			return
		}
		log.Println("Registered with ID:", State.NodeID)
		return
	}

	// Could be cached so we don't have to hit the filesystem
	blocks, err := State.Store.ReadBlockList()
	if err != nil {
		log.Fatalln("Getting utilization:", err)
	}
	spaceUsed := len(blocks)
	newBlocks := State.DrainNewBlocks()
	deadBlocks := State.DrainDeadBlocks()
	var resp HeartbeatResponse

	err = client.Call("PeerSession.Heartbeat",
		HeartbeatMsg{State.NodeID, spaceUsed, newBlocks, deadBlocks},
		&resp)
	if err != nil {
		log.Println("Heartbeat error:", err)
		State.HaveBlocks(newBlocks)
		State.DontHaveBlocks(deadBlocks)
		return
	}
	if resp.NeedToRegister {
		log.Println("Re-registering with leader...")
		State.NodeID = ""
		State.HaveBlocks(newBlocks) // Try again next heartbeat
		State.DontHaveBlocks(deadBlocks)
		return
	}
	for _, blockID := range resp.InvalidateBlocks {
		State.RemoveBlock(blockID)
	}
	go func() {
		for _, fwd := range resp.ToReplicate {
			log.Println("Will replicate '" + string(fwd.BlockID) + "' to", fwd.Nodes)
			State.forwardingBlocks <- fwd
		}
	}()
}