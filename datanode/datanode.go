package datanode

import (
	"log"
	"os"
	"sync"
	"time"

	. "github.com/michaelmaltese/golang-distributed-filesystem/common"
)

var (
	Debug bool
)

type DataNodeState struct {
	mutex             sync.Mutex
	forwardingBlocks  chan ForwardBlock
	NodeID            NodeID
	Store             BlockStore
	Manager           BlockIntents
	heartbeatInterval time.Duration
	Addr              string
	LeaderAddress     string

	blocksToDelete chan BlockID

	newBlocks  TransactionalQueue
	deadBlocks TransactionalQueue
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

func (self *DataNodeState) RemoveBlock(block BlockID) {
	self.Manager.LockDelete(block)
	defer self.Manager.CommitDelete(block)
	log.Println("Removing block '" + block + "'")
	if err := self.Store.DeleteBlock(block); err != nil {
		log.Println("Deleting block", block, "->", err)
	}
	self.deadBlocks.Push([]BlockID{block})
}
