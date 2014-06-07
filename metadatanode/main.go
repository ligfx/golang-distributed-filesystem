// Building a distributed system in Go, to:
// 1) Learn Go
// 2) Learn more about distributed systems

package metadatanode

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"

	"github.com/nu7hatch/gouuid"

	"github.com/michaelmaltese/golang-distributed-filesystem/comm"
)

type Session struct {
	net.Conn
	comm.EncodeDecoder
	*log.Logger
}

func NewSession(conn net.Conn) *Session {
	logger := log.New(os.Stderr, conn.RemoteAddr().String()+" ", log.LstdFlags)

	ed := comm.NewEncodeDecoder(io.TeeReader(conn, &comm.LoggerWrapper{logger}), conn)
	return &Session{conn,
		ed,
		logger}
}

func handleClientConnection(session *Session, state *MetaDataNodeState) {
	defer session.Close()

	message, err := session.Decode()
	if err != nil {
		session.Print(err)
		return
	}

	switch message[0] {
	case "CREATE":
		blob_id, err := uuid.NewV4()
		if err != nil {
			session.Print(err)
			return
		}
		session.Encode("OK", "BLOB", blob_id.String())
		
		blocks := make([]string, 0)

		for {
			message, err := session.Decode()
			if err != nil {
				session.Print(err)
				return
			}

			switch message[0] {
			case "APPEND":
				block_id, err := uuid.NewV4()
				if err != nil {
					session.Print(err)
					return
				}
				blocks = append(blocks, block_id.String())
				// Get list of datanodes
				session.Encode(append([]string{"OK", "BLOCK", block_id.String()}, state.GetDataNodes()...)...)
			case "OK":
				state.CommitBlob(blob_id.String(), blocks)
			default:
				session.Printf("Unknown message_type: %#v", message[0])
				return
			}
		}

	case "GETBLOB":
		blob_id := message[1]
		fmt.Fprintf(session, "%v\n", state.GetBlob(blob_id))

	case "GETBLOCK":
		block_id := message[1]
		fmt.Fprintf(session, "%v\n", state.GetBlock(block_id))

	default:
		session.Printf("Unknown message_type: %#v", message[0])
	}
}

type MetaDataNodeState struct {
	mutex sync.Mutex
	dataNodes []string
	blobs map[string][]string
	blocks map[string][]string
}

func NewMetaDataNodeState() *MetaDataNodeState {
	var self MetaDataNodeState
	self.dataNodes = make([]string, 0)
	self.blobs = make(map[string][]string)
	return &self
}

func (self *MetaDataNodeState) GetBlob(blob_id string) []string {
	self.mutex.Lock()
	defer self.mutex.Unlock()

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

func handlePeerConnection(c net.Conn) {

}

func MetadataNode() bool {
	var (
		clientPort = flag.String("clientport", "5050", "port to listen on")
		peerPort = flag.String("peerport", "5051", "port to listen on")
	)
	flag.Parse()

	clientSocket, err := net.Listen("tcp", ":"+*clientPort)
	if err != nil {
		log.Fatal(err)
	}
	peerSocket, err := net.Listen("tcp", ":"+*peerPort)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("Accepting client connections on :" + *clientPort)
	log.Print("Accepting peer connections on :" + *peerPort)

	state := NewMetaDataNodeState()

	go func(){
		
		for {
			conn, err := clientSocket.Accept()
			log.Print("Connection from client", conn.RemoteAddr())
			if err != nil {
				log.Print(err)
				continue
			}
			session := NewSession(conn)
			go handleClientConnection(session, state)
		}
	}()

	for {
		conn, err := peerSocket.Accept()
		log.Print("Connection from peer", conn.RemoteAddr())
		if err != nil {
			log.Print(err)
			continue
		}
		go handlePeerConnection(conn)
	}

	return true
}
