package metadatanode

import (
	"errors"
	"log"
	"net"
	"github.com/michaelmaltese/golang-distributed-filesystem/comm"
)

type PeerSession struct {
	state SessionState
	server *MetaDataNodeState
	remoteAddr string
}
func (self *PeerSession) Register (port *string, nodeId *string) error {
	if self.state != Start {
		return errors.New("Not allowed in current session state")
	}
	host, _, err := net.SplitHostPort(self.remoteAddr)
	if err != nil {
		log.Fatalln("SplitHostPort error:", err)
	}
	addr := net.JoinHostPort(host, *port)
	*nodeId = self.server.RegisterDataNode(addr)
	log.Println("DataNode '" + *nodeId + "' registered at " + addr)
	return nil
}

func (self *PeerSession) HaveBlock (msg *comm.HaveBlock, _ *int) error {
	if self.state != Start {
		return errors.New("Not allowed in current session state")
	}
	self.server.HasBlock(msg.NodeId, msg.BlockId)
	log.Println("Block '" + msg.BlockId + "' registered to " + msg.NodeId)
	return nil
}

