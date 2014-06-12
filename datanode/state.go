package datanode

import (
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	. "github.com/michaelmaltese/golang-distributed-filesystem/comm"
	"github.com/michaelmaltese/golang-distributed-filesystem/util"
)

type DataNodeConfig struct {}

func (self *DataNodeConfig) BlockSize(block BlockID) (int64, error) {
	fileInfo, err := os.Stat(self.BlockFilename(block))
	if err != nil {
		return -1, err
	}
	return fileInfo.Size(), nil
}

func (self *DataNodeConfig) ReadBlock(block BlockID, size int64, w io.Writer) error {
	file, err := os.Open(self.BlockFilename(block))
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.CopyN(w, file, size)
	if err != nil {
		return err
	}
	return nil
}

func (self *DataNodeConfig) WriteBlock(block BlockID, size int64, r io.Reader) (uint32, error) {
	file, err := os.Create(self.BlockFilename(block))
	if err != nil {
		return 0, err
	}
	defer file.Close()

	hash := crc32.NewIEEE()
	_, err = io.CopyN(file, io.TeeReader(r, hash), size)
	if err != nil {
		return 0, err
	}
	return hash.Sum32(), nil
}

func (self *DataNodeConfig) ReadBlockList() ([]BlockID, error) {
	files, err := ioutil.ReadDir(self.BlocksDirectory())
	if err != nil {
		return nil, err
	}
	var names []BlockID
	for _, f := range files {
		names = append(names, BlockID(f.Name()))
	}
	return names, nil
}

func (self *DataNodeConfig) BlocksDirectory() string {
	return path.Join(DataDir, "blocks")
}

func (self *DataNodeConfig) ChecksumFromString(s string) (uint32, error) {
	checksum, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	} else {
		return uint32(checksum), nil
	}
}

func (self *DataNodeConfig) ReadChecksum(block BlockID) (string, error) {
	b, err := ioutil.ReadFile(self.ChecksumFilename(block))
	if err != nil {
		return "", err
	}
	return string(b), nil
}
func (self *DataNodeConfig) WriteChecksum(block BlockID, s string) error {
	return ioutil.WriteFile(self.ChecksumFilename(block), []byte(s), 0777)
}

func (self *DataNodeConfig) MetaDirectory() string {
	return path.Join(DataDir, "meta")
}

func (self *DataNodeConfig) BlockFilename(block BlockID) string {
	return path.Join(self.BlocksDirectory(), string(block))
}

func (self *DataNodeConfig) ChecksumFilename(block BlockID) string {
	return path.Join(self.MetaDirectory(), string(block) + ".crc32")
}

func (self *DataNodeConfig) DeleteBlock(block BlockID) error {
	err := os.Remove(self.BlockFilename(block))
	if err != nil {
		return err
	}
	err = os.Remove(self.ChecksumFilename(block))
	return err
}

type DataNodeState struct {
	mutex sync.Mutex
	newBlocks []BlockID
	deadBlocks []BlockID
	forwardingBlocks chan ForwardBlock
	NodeID NodeID
	Config DataNodeConfig
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
	err := self.Config.DeleteBlock(block)
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
		blocks, err := State.Config.ReadBlockList()
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
	blocks, err := State.Config.ReadBlockList()
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