package datanode

import (
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"sync"
	"time"

	. "github.com/michaelmaltese/golang-distributed-filesystem/common"
)

var (
	Debug   bool
)

type DataNodeState struct {
	mutex             sync.Mutex
	newBlocks         []BlockID
	forwardingBlocks  chan ForwardBlock
	NodeID            NodeID
	Store             BlockStore
	Manager           BlockIntents
	heartbeatInterval time.Duration
	Addr string
	LeaderAddress string

	blocksToDelete chan BlockID
	deadBlocks     []BlockID
}

func Create(conf Config) (*DataNodeState, error) {
	var dn DataNodeState

	dn.forwardingBlocks = make(chan ForwardBlock)
	dn.Manager.using = map[BlockID]*sync.WaitGroup{}
	dn.Manager.receiving = map[BlockID]bool{}
	dn.Manager.willDelete = map[BlockID]bool{}
	dn.Manager.exists = map[BlockID]bool{}

	Debug = conf.Debug

	dn.Store.DataDir = conf.DataDir
	dn.Addr = conf.Listener.Addr().String()
	dn.heartbeatInterval = conf.HeartbeatInterval
	dn.LeaderAddress = conf.LeaderAddress

	log.Print("Block storage in directory '" + dn.Store.BlocksDirectory() + "'")
	if err := os.MkdirAll(dn.Store.BlocksDirectory(), 0777); err != nil {
		log.Fatal("Making directory:", err)
	}

	log.Print("Meta storage in directory '" + dn.Store.MetaDirectory() + "'")
	if err := os.MkdirAll(dn.Store.MetaDirectory(), 0777); err != nil {
		log.Fatal("Making directory:", err)
	}

	go dn.RPCServer(conf.Listener)
	go dn.Heartbeat()
	go dn.IntegrityChecker()
	go dn.BlockForwarder()

	return &dn, nil
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
		log.Println("Deleting block", block, "->", err)
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
		tick(self)
		time.Sleep(self.heartbeatInterval)
	}
}

func (self *DataNodeState) BlockForwarder() {
	for {
		f := <-self.forwardingBlocks
		sendBlock(self, f.BlockID, f.Nodes)
	}
}

func (self *DataNodeState) IntegrityChecker() {
	for {
		time.Sleep(5 * time.Second)
		log.Println("Checking block integrity...")
		files, err := self.Store.ReadBlockList()
		if err != nil {
			log.Fatal("Reading directory '"+self.Store.BlocksDirectory()+"': ", err)
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
				go self.RemoveBlock(BlockID(f))
			}
			localChecksum, err := self.Store.LocalChecksum(f)
			if err != nil {
				go self.RemoveBlock(BlockID(f))
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

func tick(dn *DataNodeState) {
	conn, err := net.Dial("tcp", dn.LeaderAddress)
	if err != nil {
		log.Println("Couldn't connect to leader at", dn.LeaderAddress)
		dn.NodeID = ""
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
	if len(dn.NodeID) == 0 {
		log.Println("Re-reading blocklist")
		blocks, err := dn.Store.ReadBlockList()
		if err != nil {
			log.Fatalln("Getting blocklist:", err)
		}
		for _, b := range blocks {
			// Seems hacky
			dn.Manager.exists[b] = true
		}
		err = client.Call("Register", &RegistrationMsg{dn.Addr, blocks}, &dn.NodeID)
		if err != nil {
			log.Println("Registration error:", err)
			return
		}
		log.Println("Registered with ID:", dn.NodeID)
		return
	}

	// Could be cached so we don't have to hit the filesystem
	blocks, err := dn.Store.ReadBlockList()
	if err != nil {
		log.Fatalln("Getting utilization:", err)
	}
	spaceUsed := len(blocks)
	newBlocks := dn.DrainNewBlocks()
	deadBlocks := dn.DrainDeadBlocks()
	var resp HeartbeatResponse

	err = client.Call("Heartbeat",
		HeartbeatMsg{dn.NodeID, spaceUsed, newBlocks, deadBlocks},
		&resp)
	if err != nil {
		log.Println("Heartbeat error:", err)
		dn.HaveBlocks(newBlocks)
		dn.DontHaveBlocks(deadBlocks)
		return
	}
	if resp.NeedToRegister {
		log.Println("Re-registering with leader...")
		dn.NodeID = ""
		dn.HaveBlocks(newBlocks) // Try again next heartbeat
		dn.DontHaveBlocks(deadBlocks)
		return
	}
	for _, blockID := range resp.InvalidateBlocks {
		dn.RemoveBlock(blockID)
	}
	go func() {
		for _, fwd := range resp.ToReplicate {
			log.Println("Will replicate '"+string(fwd.BlockID)+"' to", fwd.Nodes)
			dn.forwardingBlocks <- fwd
		}
	}()
}
