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
	staleBlocks []string
	NodeID string
	heartbeatInterval time.Duration
}

func (self *DataNodeState) HaveBlocks(blockIDs []string) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.staleBlocks = append(self.staleBlocks, blockIDs...)
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

	staleBlocks := State.DrainStaleBlocks()
	var resp comm.HeartbeatResponse
	err = client.Call("PeerSession.Heartbeat",
		comm.HeartbeatMsg{State.NodeID, spaceUsed, staleBlocks},
		&resp)
	if err != nil {
		log.Fatalln("Heartbeat error:", err)
	}
	if resp.NeedToRegister {
		log.Println("Re-registering with leader...")
		State.HaveBlocks(staleBlocks) // Try again next heartbeat
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