package datanode

import (
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"
	"time"

	. "github.com/michaelmaltese/golang-distributed-filesystem/common"
)

type DataNodeState struct {
	mutex             sync.Mutex
	newBlocks         []BlockID
	forwardingBlocks  chan ForwardBlock
	NodeID            NodeID
	Store             BlockStore
	Manager           BlockIntents
	heartbeatInterval time.Duration

	blocksToDelete chan BlockID
	deadBlocks     []BlockID
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
	self.Manager.LockDelete(block)
	defer self.Manager.CommitDelete(block)
	log.Println("Removing block '" + block + "'")
	if err := self.Store.DeleteBlock(block); err != nil {
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

func (self *DataNodeState) Heartbeat() {
	for {
		tick()
		time.Sleep(self.heartbeatInterval)
	}
}

func (self *DataNodeState) BlockForwarder() {
	for {
		f := <-self.forwardingBlocks
		sendBlock(f.BlockID, f.Nodes)
	}
}

func (self *DataNodeState) IntegrityChecker() {
	for {
		time.Sleep(5 * time.Second)
		log.Println("Checking block integrity...")
		files, err := self.Store.ReadBlockList()
		if err != nil {
			log.Fatal("Reading directory '"+DataDir+"': ", err)
		}
		for _, f := range files {
			if err := self.Manager.LockRead(f); err != nil {
				// Being uploaded or deleted
				// May or may not actually exist now/in the future
				// Does not imply it actually exists!
				continue
			}
			storedChecksum, err := self.Store.ReadChecksum(f)
			if err != nil {
				go State.RemoveBlock(BlockID(f))
			}
			localChecksum, err := self.Store.LocalChecksum(f)
			if err != nil {
				go State.RemoveBlock(BlockID(f))
			}

			if storedChecksum != localChecksum {
				log.Println("Checksum doesn't match block:", f)
				log.Println(storedChecksum, localChecksum)
				go self.RemoveBlock(BlockID(f))
			}
			self.Manager.UnlockRead(f)
		}
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
		codec = LoggingClientCodec(
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
		for _, b := range blocks {
			// Seems hacky
			State.Manager.exists[b] = true
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
			log.Println("Will replicate '"+string(fwd.BlockID)+"' to", fwd.Nodes)
			State.forwardingBlocks <- fwd
		}
	}()
}
