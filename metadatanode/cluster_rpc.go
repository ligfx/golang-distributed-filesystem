package metadatanode

import (
	"log"
	"net"

	. "github.com/michaelmaltese/golang-distributed-filesystem/common"
)

func runClusterRPC(c net.Conn, mdn *MetaDataNodeState) {
	server := NewRPCServer(c)
	defer c.Close()

	var method string
	method, err := server.ReadHeader()
	if err != nil {
		log.Println(err)
		return
	}
	switch method {
	case "Register":
		var reg RegistrationMsg
		if err := server.ReadBody(&reg); err != nil {
			log.Println(err)
			return
		}
		host, _, err := net.SplitHostPort(c.RemoteAddr().String())
		if err != nil {
			log.Fatalln("SplitHostPort error:", err)
		}
		addr := net.JoinHostPort(host, reg.Port)
		nodeID := mdn.RegisterDataNode(addr, reg.Blocks)
		server.Send(&nodeID)
		log.Println("DataNode '"+string(nodeID)+"' with", len(reg.Blocks), "blocks registered at", addr)

	case "Heartbeat":
		var msg HeartbeatMsg
		if err := server.ReadBody(&msg); err != nil {
			log.Println(err)
			return
		}
		var resp HeartbeatResponse
		resp.NeedToRegister = !mdn.HeartbeatFrom(msg.NodeID, msg.SpaceUsed)
		if resp.NeedToRegister {
			server.Send(&resp)
			return
		}
		log.Println("Heartbeat from '"+msg.NodeID+"', space used", msg.SpaceUsed)
		mdn.HasBlocks(msg.NodeID, msg.NewBlocks)
		mdn.DoesntHaveBlocks(msg.NodeID, msg.DeadBlocks)
		for _, blockID := range msg.NewBlocks {
			log.Println("Block '" + string(blockID) + "' registered to " + string(msg.NodeID))
		}
		for _, blockID := range msg.DeadBlocks {
			log.Println("Block '" + string(blockID) + "' de-registered from " + string(msg.NodeID))
		}
		resp.InvalidateBlocks = mdn.deletionIntents.Get(msg.NodeID)
		for block, nodes := range mdn.replicationIntents.Get(msg.NodeID) {
			var addrs []string
			for _, n := range nodes {
				addrs = append(addrs, mdn.dataNodes[n])
			}
			resp.ToReplicate = append(resp.ToReplicate, ForwardBlock{block, addrs, -1})
		}
		server.Send(&resp)

	default:
		server.Unacceptable()
	}
}

func (self *MetaDataNodeState) ClusterRPCServer(port string) {
	peerSock, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Accepting peer connections on", peerSock.Addr())
	for {
		peer, err := peerSock.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go runClusterRPC(peer, self)
	}
}