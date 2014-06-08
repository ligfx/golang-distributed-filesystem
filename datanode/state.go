package datanode

import (
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"
	"time"

	"github.com/michaelmaltese/golang-distributed-filesystem/comm"
	"github.com/michaelmaltese/golang-distributed-filesystem/util"
)

type DataNodeState struct {
	mutex sync.Mutex
	staleBlocks []string
	NodeID string
	heartbeatInterval time.Duration
}

func (self *DataNodeState) HaveBlock(blockID string) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.staleBlocks = append(self.staleBlocks, blockID)
}

func (self *DataNodeState) DrainStaleBlocks() []string {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	staleBlocks := self.staleBlocks
	self.staleBlocks = []string{}
	return staleBlocks
}

func heartbeat() {
	for {
		tick()
		time.Sleep(State.heartbeatInterval)
	}
}

func tick() {
	if len(State.NodeID) == 0 {
		register()
	}
	if len(State.NodeID) == 0 {
		// Still offline :/
		return
	}

	log.Println("Heartbeat...")
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

	err = client.Call("PeerSession.Heartbeat", State.NodeID, nil)
	if err != nil {
		log.Fatalln("Heartbeat error:", err)
	}

	staleBlocks := State.DrainStaleBlocks()
	for _, blockID := range staleBlocks {
		err = client.Call("PeerSession.HaveBlock", &comm.HaveBlock{blockID, State.NodeID}, nil)
		if err != nil {
			log.Fatalln("HaveBlock error:", err)
		}
	}
}

func register() {
	log.Println("Registering with leader...")
	conn, err := net.Dial("tcp", "[::1]:5051")
	if err != nil {
		log.Println("Couldn't connect to leader")
		// MetaDataNode offline
		return
	}
	defer conn.Close()

	codec := jsonrpc.NewClientCodec(conn)
	if Debug {
		codec = util.LoggingClientCodec(
			conn.RemoteAddr().String(),
			codec)
	}
	client := rpc.NewClientWithCodec(codec)

	err = client.Call("PeerSession.Register", Port, &State.NodeID)
	if err != nil {
		log.Fatal("Register error:", err)
	}
	log.Println("Registered with ID:", State.NodeID)

	log.Println("Re-reading blocklist")
	go func() {
		files, err := ioutil.ReadDir(DataDir)
		if err != nil {
			log.Fatal("Reading directory '" + DataDir + "': ", err)
		}
		for _, f := range files {
			State.HaveBlock(f.Name())
		}
	}()
}