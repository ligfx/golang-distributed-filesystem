// Stores blocks for the cluster.
package datanode

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"time"

	"github.com/michaelmaltese/golang-distributed-filesystem/comm"
	"github.com/michaelmaltese/golang-distributed-filesystem/util"
)



type ClientSessionState int
const (
	Start ClientSessionState = iota
	SizeNegotiation
	Done
	Receiving
	Sending
	ReadyToConfirm
)
type ClientSession struct {
	state ClientSessionState
	connection net.Conn
	blockId string
	forwardTo []string
	size int64
}

const MaxSize = int64(128 * 1024 * 1024)

func (self *ClientSession) GetBlock(blockID *string, size *int64) error {
	if self.state != Start {
		return errors.New("Not allowed in current session state")
	}

	fileInfo, err := os.Stat(path.Join(DataDir, *blockID))
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

func (self *ClientSession) ForwardBlock(blockMsg *comm.ForwardBlock, maxSize *int64) error {
	if self.state != Start {
		return errors.New("Not allowed in current session state")
	}

	self.blockId = blockMsg.BlockId
	*maxSize = MaxSize
	self.state = SizeNegotiation
	self.forwardTo = blockMsg.Nodes
	return nil
}

func (self *ClientSession) Size(size *int64, _ *int) error {
	if self.state != SizeNegotiation {
		return errors.New("Not allowed in current session state")
	}

	if *size > MaxSize || *size <= 0 {
		return errors.New("Bad size")
	}

	self.state = Receiving
	self.size = *size
	return nil
}

func (self *ClientSession) Confirm(_ *int, _ *int) error {
	if self.state != ReadyToConfirm {
		return errors.New("Not allowed in current session state")
	}

	State.HaveBlock(self.blockId)

	self.blockId = ""
	self.size = -1
	self.state = Done
	return nil	
}

func sendBlock(blockID string, peers []string) {

	fileName := path.Join(DataDir, blockID)
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		log.Fatal("Stat error: ", err)
	}
	size := fileInfo.Size()
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalln("Open file '" + blockID + "' error:", err)
	}

	nextNode := peers[0]
	peerConn, err := net.Dial("tcp", nextNode)
	if err != nil {
		log.Fatalln("Dial error:", err)
	}
	peerCodec := jsonrpc.NewClientCodec(peerConn)
	if Debug {
		peerCodec = util.LoggingClientCodec(
			peerConn.RemoteAddr().String(),
			peerCodec)
	}
	peer := rpc.NewClientWithCodec(peerCodec)
	defer peer.Close()

	// Ignore maxsize because we assume it's the same thing
	// Could be wrong. Maybe datanodes shouldn't worry about that.
	err = peer.Call("ClientSession.ForwardBlock",
		&comm.ForwardBlock{blockID, peers[1:]},
		nil)
	if err != nil {
		log.Fatal("ForwardBlock error: ", err)
	}
	err = peer.Call("ClientSession.Size", &size, nil)
	if err != nil {
		log.Fatal("Size error: ", err)
	}

	_, err = io.CopyN(peerConn, file, size)
	file.Close()
	if err != nil {
		log.Fatal("Copying error: ", err)
	}
	
	err = peer.Call("ClientSession.Confirm", nil, nil)
	if err != nil {
		log.Fatal("Confirm error: ", err)
	}
}

func handleRequest(c net.Conn) {
	server := rpc.NewServer()
	session := &ClientSession{Start, c, "", nil, -1}
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

			file, err := os.Create(path.Join(DataDir, session.blockId))
			if err != nil {
				log.Fatal("Create file '" + session.blockId + "' error:", err)
			}
			_, err = io.CopyN(file, c, session.size)
			file.Close()
			if err != nil {
				log.Fatal("Copying error: ", err)
			}

			// Pipeline!
			if len(session.forwardTo) > 0 {
				go sendBlock(session.blockId, session.forwardTo)
			}

			log.Println("Received block '" + session.blockId + "' from " + c.RemoteAddr().String())
			
			session.state = ReadyToConfirm

		case Sending:
			file, err := os.Open(path.Join(DataDir, session.blockId))
			if err != nil {
				log.Fatal("Open file '" + session.blockId + "' error:", err)
			}
			fmt.Println("Copying")
			fmt.Fprint(c, "hi there")
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

func DataNode() {
	port := flag.String("port", "0", "port to listen on (0=random)")
	flag.StringVar(&DataDir, "dataDir", "_blocks", "directory to store blocks")
	flag.BoolVar(&Debug, "debug", false, "Show RPC conversations")
	flag.DurationVar(&State.heartbeatInterval, "heartbeatInterval", 1000 * time.Millisecond, "")
	flag.Parse()

	addr, socket := util.Listen(*port)
	log.Print("Accepting connections on " + addr)
	_, realPort, err := net.SplitHostPort(addr)
	Port = realPort
	if err != nil {
		log.Fatalln("SplitHostPort error:", err)
	}

	err = os.MkdirAll(DataDir, 0777)
	if err != nil {
		log.Fatal("Making directory '" + DataDir + "': ", err)
	}
	log.Print("Block storage in directory '" + DataDir + "'")

	// Heartbeat and registration
	go heartbeat()

	// Server
	for {
		conn := <- socket
		go handleRequest(conn)
	}
}