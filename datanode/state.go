package datanode

import (
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"path"
	"sync"
	"time"

	"github.com/michaelmaltese/golang-distributed-filesystem/comm"
	"github.com/michaelmaltese/golang-distributed-filesystem/util"
)

type DataNodeState struct {
	mutex sync.Mutex
	newBlocks []string
	deadBlocks []string
	NodeID string
	heartbeatInterval time.Duration
}

func (self *DataNodeState) HaveBlocks(blockIDs []string) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.newBlocks = append(self.newBlocks, blockIDs...)
}

func (self *DataNodeState) DontHaveBlocks(blockIDs []string) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.deadBlocks = append(self.deadBlocks, blockIDs...)
}

func (self *DataNodeState) DrainNewBlocks() []string {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	newBlocks := self.newBlocks
	self.newBlocks = []string{}
	return newBlocks
}

func (self *DataNodeState) DrainDeadBlocks() []string {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	deadBlocks := self.deadBlocks
	self.deadBlocks = []string{}
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

	if len(State.NodeID) == 0 {
		register(client)
	}
	if len(State.NodeID) == 0 {
		// Still offline :/
		return
	}

	log.Println("Heartbeat...")

	// Could be cached
	files, err := ioutil.ReadDir(DataDir)
	if err != nil {
		log.Fatal("Reading directory '" + DataDir + "': ", err)
	}
	spaceUsed := len(files)

	newBlocks := State.DrainNewBlocks()
	deadBlocks := State.DrainDeadBlocks()
	var resp comm.HeartbeatResponse
	err = client.Call("PeerSession.Heartbeat",
		comm.HeartbeatMsg{State.NodeID, spaceUsed, newBlocks, deadBlocks},
		&resp)
	if err != nil {
		log.Fatalln("Heartbeat error:", err)
	}
	if resp.NeedToRegister {
		log.Println("Re-registering with leader...")
		State.HaveBlocks(newBlocks) // Try again next heartbeat
		register(client)
		return
	}
	for _, blockID := range resp.InvalidateBlocks {
		log.Println("Removing block '" + blockID + "'")
		err = os.Remove(path.Join(DataDir, blockID))
		if err != nil {
			log.Fatalln("Error removing '" + blockID + "':", err)
		}
	}
	State.DontHaveBlocks(resp.InvalidateBlocks)
}

func register(client *rpc.Client) {
	err := client.Call("PeerSession.Register", Port, &State.NodeID)
	if err != nil {
		log.Fatalln("Registration error:", err)
	}
	log.Println("Registered with ID:", State.NodeID)

	go func() {
		log.Println("Re-reading blocklist")
		files, err := ioutil.ReadDir(DataDir)
		if err != nil {
			log.Fatal("Reading directory '" + DataDir + "': ", err)
		}
		var names []string
		for _, f := range files {
			names = append(names, f.Name())
		}
		State.HaveBlocks(names)
	}()
}