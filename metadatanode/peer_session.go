package metadatanode

import (
	"errors"
	"log"
	"net"

	. "github.com/michaelmaltese/golang-distributed-filesystem/common"
)

type PeerSession struct {
	state      SessionState
	server     *MetaDataNodeState
	remoteAddr string
}

func (self *PeerSession) Heartbeat(msg *HeartbeatMsg, resp *HeartbeatResponse) error {
	if self.state != Start {
		return errors.New("Not allowed in current session state")
	}
	resp.NeedToRegister = !self.server.HeartbeatFrom(msg.NodeID, msg.SpaceUsed)
	if resp.NeedToRegister {
		return nil
	}

	log.Println("Heartbeat from '"+msg.NodeID+"', space used", msg.SpaceUsed)
	self.server.HasBlocks(msg.NodeID, msg.NewBlocks)
	for _, blockID := range msg.NewBlocks {
		log.Println("Block '" + string(blockID) + "' registered to " + string(msg.NodeID))
	}
	self.server.DoesntHaveBlocks(msg.NodeID, msg.DeadBlocks)
	for _, blockID := range msg.DeadBlocks {
		log.Println("Block '" + string(blockID) + "' de-registered from " + string(msg.NodeID))
	}
	resp.InvalidateBlocks = self.server.deletionIntents.Get(msg.NodeID)
	for block, nodes := range self.server.replicationIntents.Get(msg.NodeID) {
		var addrs []string
		for _, n := range nodes {
			addrs = append(addrs, self.server.dataNodes[n])
		}
		resp.ToReplicate = append(resp.ToReplicate, ForwardBlock{block, addrs, -1})
	}
	return nil
}

func (self *PeerSession) Register(req *RegistrationMsg, nodeId *NodeID) error {
	if self.state != Start {
		return errors.New("Not allowed in current session state")
	}
	host, _, err := net.SplitHostPort(self.remoteAddr)
	if err != nil {
		log.Fatalln("SplitHostPort error:", err)
	}
	addr := net.JoinHostPort(host, req.Port)
	*nodeId = self.server.RegisterDataNode(addr, req.Blocks)
	log.Println("DataNode '"+string(*nodeId)+"' with", len(req.Blocks), "blocks registered at", addr)
	return nil
}
