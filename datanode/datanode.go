// Stores blocks for the cluster.
package datanode

import (
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"strconv"
	"strings"
	"time"

	. "github.com/michaelmaltese/golang-distributed-filesystem/comm"
	"github.com/michaelmaltese/golang-distributed-filesystem/util"
)

type ClientSessionState int
const (
	Start ClientSessionState = iota
	Done
	Receiving
	Sending
	ReadyToConfirm
)
type ClientSession struct {
	state ClientSessionState
	connection net.Conn
	blockId BlockID
	forwardTo []string
	size int64
	hash uint32
}

func (self *ClientSession) GetBlock(blockID *BlockID, size *int64) error {
	if self.state != Start {
		return errors.New("Not allowed in current session state")
	}

	fileInfo, err := os.Stat(path.Join(DataDir, string(*blockID)))
	if err != nil {
		log.Fatal("Stat error: ", err)
	}
	self.size = fileInfo.Size()
	*size = self.size

	self.blockId = *blockID
	self.forwardTo = nil
	self.state = Sending

	return nil
}

func (self *ClientSession) ForwardBlock(blockMsg *ForwardBlock, _ *int) error {
	if self.state != Start {
		return errors.New("Not allowed in current session state")
	}

	if blockMsg.Size <= 0 {
		return errors.New("Bad size")
	}

	self.blockId = blockMsg.BlockID
	self.forwardTo = blockMsg.Nodes
	self.size = blockMsg.Size
	self.state = Receiving

	return nil
}

func (self *ClientSession) Confirm(crc *string, _ *int) error {
	if self.state != ReadyToConfirm {
		return errors.New("Not allowed in current session state")
	}

	hash, err := strconv.ParseInt(*crc, 10, 64)
	if err != nil {
		log.Fatal("Error parsing hash")
	}
	if uint32(hash) != self.hash {
		os.Remove(path.Join(DataDir, string(self.blockId)))
		fmt.Println("Hash doesn't match for", self.blockId)
		return errors.New("Hash doesn't match!")
	}

	file, err := os.Create(path.Join(DataDir, "meta", string(self.blockId) + ".crc32"))
	if err != nil {
		log.Fatal("Create file '" + path.Join(DataDir, "meta", string(self.blockId) + ".crc32") + "' error:", err)
	}
	defer file.Close()
	fmt.Fprint(file, *crc)

	// Pipeline!
	if len(self.forwardTo) > 0 {
		State.forwardingBlocks <- ForwardBlock{self.blockId, self.forwardTo, -1}
	}

	State.HaveBlocks([]BlockID{self.blockId})

	self.blockId = ""
	self.size = -1
	self.state = Done
	self.hash = 0
	return nil	
}

func sendBlock(blockID BlockID, peers []string) {
	var peerConn net.Conn
	var forwardTo []string
	var err error
	// Find an online peer
	for i, addr := range peers {
		peerConn, err = net.Dial("tcp", addr)
		if err == nil {
			forwardTo = append(peers[:i], peers[i+1:]...)
			break
		}
	}
	if peerConn == nil {
		log.Println("Couldn't forward block",
			blockID,
			"to any DataNodes in:",
			strings.Join(peers, " "))
		return
	}
	peerCodec := jsonrpc.NewClientCodec(peerConn)
	if Debug {
		peerCodec = util.LoggingClientCodec(
			peerConn.RemoteAddr().String(),
			peerCodec)
	}
	peer := rpc.NewClientWithCodec(peerCodec)
	defer peer.Close()

	fileName := path.Join(DataDir, "blocks", string(blockID))
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		log.Fatal("Stat error: ", err)
	}
	size := fileInfo.Size()
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalln("Open file '" + blockID + "' error:", err)
	}

	err = peer.Call("ClientSession.ForwardBlock",
		&ForwardBlock{blockID, forwardTo, size},
		nil)
	if err != nil {
		log.Fatal("ForwardBlock error: ", err)
	}

	_, err = io.CopyN(peerConn, file, size)
	file.Close()
	if err != nil {
		log.Fatal("Copying error: ", err)
	}
	
	hash, err := ioutil.ReadFile(path.Join(DataDir, "meta", string(blockID) + ".crc32"))
	if err != nil {
		log.Fatalln("Read file '" + path.Join(DataDir, "meta", string(blockID) + ".crc32") + "' error:", err)
	}
	err = peer.Call("ClientSession.Confirm", string(hash), nil)
	if err != nil {
		log.Fatal("Confirm error: ", err)
	}
}

func handleRequest(c net.Conn) {
	server := rpc.NewServer()
	session := &ClientSession{Start, c, "", nil, -1, 0}
	server.Register(session)
	codec := jsonrpc.NewServerCodec(c)
	if Debug {
		codec = util.LoggingServerCodec(
			c.RemoteAddr().String(),
			codec)
	}
	for {
		switch session.state {
		default:
			server.ServeRequest(codec)

		case Done:
			return

		case Receiving:
			var err error

			file, err := os.Create(path.Join(DataDir, "blocks", string(session.blockId)))
			if err != nil {
				log.Fatal("Create file '" + session.blockId + "' error:", err)
			}
			defer file.Close()

			hash := crc32.NewIEEE()
			_, err = io.CopyN(file, io.TeeReader(c, hash), session.size)
			if err != nil {
				log.Fatal("Copying error: ", err)
			}
			session.hash = hash.Sum32()

			log.Println("Received block '" + string(session.blockId) + "' from " + c.RemoteAddr().String())
			
			session.state = ReadyToConfirm

		case Sending:
			file, err := os.Open(path.Join(DataDir, "blocks", string(session.blockId)))
			if err != nil {
				log.Fatal("Open file '" + session.blockId + "' error:", err)
			}
			fmt.Println("Copying")
			_, err = io.CopyN(c, file, session.size)
			file.Close()
			fmt.Println("Done copying")
			if err != nil {
				log.Fatal("Copying error: ", err)
			}
			session.state = Start
		}
	}

}

var (
	DataDir string
	Debug bool
	Port string
	State DataNodeState
)

func init() {
	State.forwardingBlocks = make(chan ForwardBlock)
}

func DataNode() {
	port := flag.String("port", "0", "port to listen on (0=random)")
	flag.StringVar(&DataDir, "dataDir", "_data", "directory to store data")
	flag.BoolVar(&Debug, "debug", false, "Show RPC conversations")
	flag.DurationVar(&State.heartbeatInterval, "heartbeatInterval", 3 * time.Second, "")
	flag.Parse()

	addr, socket := util.Listen(*port)
	log.Print("Accepting connections on " + addr)
	_, realPort, err := net.SplitHostPort(addr)
	Port = realPort
	if err != nil {
		log.Fatalln("SplitHostPort error:", err)
	}

	err = os.MkdirAll(path.Join(DataDir, "blocks"), 0777)
	if err != nil {
		log.Fatal("Making directory '" + path.Join(DataDir, "blocks") + "': ", err)
	}
	log.Print("Block storage in directory '" + path.Join(DataDir, "blocks") + "'")
	err = os.MkdirAll(path.Join(DataDir, "meta"), 0777)
	if err != nil {
		log.Fatal("Making directory '" + path.Join(DataDir, "meta") + "': ", err)
	}
	log.Print("Meta storage in directory '" + path.Join(DataDir, "meta") + "'")

	// Heartbeat and registration
	go heartbeat()

	go func() {
		for {
			f := <- State.forwardingBlocks
			sendBlock(f.BlockID, f.Nodes)
		}
	}()

	// Server
	for {
		conn := <- socket
		go handleRequest(conn)
	}
}