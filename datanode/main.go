// Stores blocks for the cluster.
package datanode

import (
	"log"
	"os"
	"sync"
	"time"

	. "github.com/michaelmaltese/golang-distributed-filesystem/common"
)

var (
	DataDir string
	Debug   bool
	Port    string
	State   DataNodeState
)

type Config struct {
	DataDir string
	Debug bool
	Port string
	HeartbeatInterval time.Duration
}

func Create(conf Config) (*DataNodeState, error) {
	State.forwardingBlocks = make(chan ForwardBlock)
	State.Manager.using = map[BlockID]*sync.WaitGroup{}
	State.Manager.receiving = map[BlockID]bool{}
	State.Manager.willDelete = map[BlockID]bool{}
	State.Manager.exists = map[BlockID]bool{}

	DataDir = conf.DataDir
	Debug = conf.Debug
	port := conf.Port
	State.heartbeatInterval = conf.HeartbeatInterval

	log.Print("Block storage in directory '" + State.Store.BlocksDirectory() + "'")
	if err := os.MkdirAll(State.Store.BlocksDirectory(), 0777); err != nil {
		log.Fatal("Making directory:", err)
	}

	log.Print("Meta storage in directory '" + State.Store.MetaDirectory() + "'")
	if err := os.MkdirAll(State.Store.MetaDirectory(), 0777); err != nil {
		log.Fatal("Making directory:", err)
	}

	go State.RPCServer(port)
	go State.Heartbeat()
	go State.IntegrityChecker()
	go State.BlockForwarder()

	return &State, nil
}