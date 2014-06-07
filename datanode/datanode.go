// Stores blocks for the cluster.
package datanode

import (
	"errors"
	"flag"
	"io"
	"log"
	"os"
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
	Streaming
	ReadyToConfirm
	Done
)
type ClientSession struct {
	state ClientSessionState
	blockId string
	size int64
}

const MaxSize = int64(128 * 1024 * 1024)

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

	self.state = Streaming
	self.size = *size
	return nil
}

func (self *ClientSession) Confirm(_ *int, _ *int) error {
	if self.state != ReadyToConfirm {
		return errors.New("Not allowed in current session state")
	}

	self.state = Done
	return nil	
}

func handleRequest(c net.Conn, debug bool) {
	server := rpc.NewServer()
	session := &ClientSession{Start, "", -1}
	server.Register(session)
	codec := jsonrpc.NewServerCodec(c)
	if debug {
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

		case Streaming:
			file, err := os.Create(session.blockId)
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
		}
	}

}

func DataNode() {
	var (
		port = flag.String("port", "5052", "port to listen on")
		debug = flag.Bool("debug", false, "Show RPC conversations")
	)
	flag.Parse()

	socket := util.Listen(*port)
	log.Print("Accepting connections on :" + *port)

	for {
		conn := <- socket
		go handleRequest(conn, *debug)
	}
}
