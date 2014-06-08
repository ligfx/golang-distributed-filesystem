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
func (self *PeerSession) Heartbeat (msg *comm.HeartbeatMsg, resp *comm.HeartbeatResponse) error {
	if self.state != Start {
		return errors.New("Not allowed in current session state")
	}
	resp.NeedToRegister = ! self.server.HeartbeatFrom(msg.NodeID, msg.SpaceUsed)
	if ! resp.NeedToRegister {
		log.Println("Heartbeat from '" + msg.NodeID + "', space used", msg.SpaceUsed)
		self.server.HasBlocks(msg.NodeID, msg.NewBlocks)
		for _, blockID := range msg.NewBlocks {
			log.Println("Block '" + blockID + "' registered to " + msg.NodeID)
			// Pathological MDN
			// resp.InvalidateBlocks = append(resp.InvalidateBlocks, blockID)
		}
		self.server.DoesntHaveBlocks(msg.NodeID, msg.DeadBlocks)
		for _, blockID := range msg.DeadBlocks {
			log.Println("Block '" + blockID + "' de-registered from " + msg.NodeID)
		}
	}
	return nil
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