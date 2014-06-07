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

	metaDataNodeConn, err := net.Dial("tcp", "[::1]:5051")
	if err != nil {
		log.Fatalln("Dial error:", err)
	}

	metaDataNodeCodec := jsonrpc.NewClientCodec(metaDataNodeConn)
	if Debug {
		metaDataNodeCodec = util.LoggingClientCodec(
			metaDataNodeConn.RemoteAddr().String(),
			metaDataNodeCodec)
	}
	metaDataNode := rpc.NewClientWithCodec(metaDataNodeCodec)
	defer metaDataNode.Close()

	err = metaDataNode.Call("PeerSession.HaveBlock", &comm.HaveBlock{self.blockId, NodeID}, nil)
	if err != nil {
		log.Fatalln("HaveBlock error:", err)
	}

	self.blockId = ""
	self.size = -1
	self.state = Done
	return nil	
}

func handleRequest(c net.Conn) {
	server := rpc.NewServer()
	session := &ClientSession{Start, c, "", -1}
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
			file, err := os.Create(path.Join(DataDir, session.blockId))
			if err != nil {
				log.Fatal("Create file '" + session.blockId + "' error:", err)
			}
			_, err = io.CopyN(file, c, session.size)
			file.Close()
			if err != nil {
				log.Fatal("Copying error: ", err)
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

func register() {
	conn, err := net.Dial("tcp", "[::1]:5051")
	if err != nil {
		log.Fatal("Dial error:", err)
	}
	defer conn.Close()

	codec := jsonrpc.NewClientCodec(conn)
	if Debug {
		codec = util.LoggingClientCodec(
			conn.RemoteAddr().String(),
			codec)
	}
	client := rpc.NewClientWithCodec(codec)

	err = client.Call("PeerSession.Register", Port, &NodeID)
	if err != nil {
		log.Fatal("Register error:", err)
	}

	log.Println("Registered with ID '" + NodeID + "'")
}

var (
	DataDir string
	Debug bool
	NodeID string
	Port string
)

func DataNode() {
	port := flag.String("port", "0", "port to listen on (0=random)")
	flag.StringVar(&DataDir, "dataDir", "_blocks", "directory to store blocks")
	flag.BoolVar(&Debug, "debug", false, "Show RPC conversations")
	flag.Parse()

	err := os.MkdirAll(DataDir, 0777)
	if err != nil {
		log.Fatal("Making directory '" + DataDir + "': ", err)
	}
	log.Print("Block storage in directory '" + DataDir + "'")

	addr, socket := util.Listen(*port)
	log.Print("Accepting connections on " + addr)

	_, realPort, err := net.SplitHostPort(addr)
	Port = realPort
	if err != nil {
		log.Fatalln("SplitHostPort error:", err)
	}
	
	go register()

	for {
		conn := <- socket
		go handleRequest(conn)
	}
}
