// Keeps tracks of blobs, blocks, and other nodes.
package metadatanode

import (
	"flag"
	"log"
	"errors"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"

	"github.com/nu7hatch/gouuid"

	"github.com/michaelmaltese/golang-distributed-filesystem/comm"
	"github.com/michaelmaltese/golang-distributed-filesystem/util"
)

type ClientSessionState int
const (
	Start ClientSessionState = iota
	Creating
)
type ClientSession struct {
	state ClientSessionState
	server *MetaDataNodeState
	blob_id string
	blocks []string
	remoteAddr string
}
func (self *ClientSession) CreateBlob(_ *int, ret *string) error {
	if self.state != Start {
		return errors.New("Not allowed in current session state")
	}

	u4, err := uuid.NewV4()
	if err != nil {
		log.Fatal(err)
	}
	self.blob_id = u4.String()
	self.blocks = []string{}
	*ret = self.blob_id
	self.state = Creating
	return nil
}
func (self *ClientSession) Append(_ *int, ret *comm.ForwardBlock) error {
	if self.state != Creating {
		return errors.New("Not allowed in current session state")
	}

	u4, err := uuid.NewV4()
	if err != nil {
		log.Fatal(err)
	}
	self.blocks = append(self.blocks, u4.String())

	*ret = comm.ForwardBlock{u4.String(), self.server.GetDataNodes()}
	return nil
}

func (self *ClientSession) Commit(_ *int, _ *int) error {
	if self.state != Creating {
		return errors.New("Not allowed in current session state")
	}

	self.server.CommitBlob(self.blob_id, self.blocks)
	log.Print("Committed blob '" + self.blob_id + "' for " + self.remoteAddr)
	self.blob_id = ""
	self.blocks = nil
	self.state = Start
	return nil
}

func (self *ClientSession) GetBlob(blobId *string, blocks *[]string) error {
	if self.state != Start {
		return errors.New("Not allowed in current session state")
	}

	*blocks = self.server.GetBlob(*blobId)
	return nil
}

func (self *ClientSession) GetBlock(blockId *string, nodes *[]string) error {
	if self.state != Start {
		return errors.New("Not allowed in current session state")
	}

	*nodes = self.server.GetBlock(*blockId)
	return nil
}

type MetaDataNodeState struct {
	mutex sync.Mutex
	dataNodes []string
	blobs map[string][]string
	blocks map[string][]string
}

func NewMetaDataNodeState() *MetaDataNodeState {
	var self MetaDataNodeState
	self.dataNodes = []string{"127.0.0.1:5052"}
	self.blobs = make(map[string][]string)
	return &self
}

func (self *MetaDataNodeState) GetBlob(blob_id string) []string {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	log.Println(self.blobs[blob_id])
	return self.blobs[blob_id]
}

func (self *MetaDataNodeState) GetBlock(block_id string) []string {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return self.blocks[block_id]
}


func (self *MetaDataNodeState) GetDataNodes() []string {
	// Is this lock necessary?
	self.mutex.Lock()
	defer self.mutex.Unlock()
	l := make([]string, len(self.dataNodes))
	copy(l, self.dataNodes)
	return l
}

func (self *MetaDataNodeState) CommitBlob(name string, blocks []string) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.blobs[name] = blocks
}

func handlePeerConnection(c net.Conn, s *MetaDataNodeState) {

}

func MetadataNode() {
	var (
		clientPort = flag.String("clientport", "5050", "port to listen on")
		peerPort = flag.String("peerport", "5051", "port to listen on")
		debug = flag.Bool("debug", false, "Show RPC conversations")
	)
	flag.Parse()

	clientSocket := util.Listen(*clientPort)
	peerSocket := util.Listen(*peerPort)
	log.Print("Accepting client connections on :" + *clientPort)
	log.Print("Accepting peer connections on :" + *peerPort)

	state := NewMetaDataNodeState()

	for {
		select {
		case client := <- clientSocket:
			server := rpc.NewServer()
			server.Register(&ClientSession{Start, state, "", nil, client.RemoteAddr().String()})
			codec := jsonrpc.NewServerCodec(client)
			if *debug {
				codec = util.LoggingServerCodec(client.RemoteAddr().String(),
					codec)
			}
			go server.ServeCodec(codec)
		case peer := <- peerSocket:
			go handlePeerConnection(peer, state)
		}
	}
}
