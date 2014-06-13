// Stores blocks for the cluster.
package datanode

import (
	"flag"
	"log"
	"os"
	"sync"
	"time"

	. "github.com/michaelmaltese/golang-distributed-filesystem/comm"
)

var (
	DataDir string
	Debug bool
	Port string
	State DataNodeState
)

func init() {
	State.forwardingBlocks = make(chan ForwardBlock)
	State.Manager.using = map[BlockID]*sync.WaitGroup{}
	State.Manager.receiving = map[BlockID]bool{}
	State.Manager.willDelete = map[BlockID]bool{}
	State.Manager.exists = map[BlockID]bool{}
}

func DataNode() {
	port := flag.String("port", "0", "port to listen on (0=random)")
	flag.StringVar(&DataDir, "dataDir", "_data", "directory to store data")
	flag.BoolVar(&Debug, "debug", false, "Show RPC conversations")
	flag.DurationVar(&State.heartbeatInterval, "heartbeatInterval", 3 * time.Second, "")
	flag.Parse()

	log.Print("Block storage in directory '" + State.Store.BlocksDirectory() + "'")
	if err := os.MkdirAll(State.Store.BlocksDirectory(), 0777); err != nil {
		log.Fatal("Making directory:", err)
	}

	log.Print("Meta storage in directory '" + State.Store.MetaDirectory() + "'")
	if err := os.MkdirAll(State.Store.MetaDirectory(), 0777); err != nil {
		log.Fatal("Making directory:", err)
	}

	go State.RPCServer(*port)
	go State.Heartbeat()
	go State.IntegrityChecker()
	go State.BlockForwarder()

	<- make(chan bool)
}