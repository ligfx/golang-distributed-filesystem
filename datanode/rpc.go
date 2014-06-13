package datanode

import (
	"errors"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"strings"

	. "github.com/michaelmaltese/golang-distributed-filesystem/comm"
	"github.com/michaelmaltese/golang-distributed-filesystem/util"
)

type RPCState int
const (
	Start RPCState = iota
	Done
	Receiving
	Giving
	ReadyToConfirm
)
type RPC struct {
	state RPCState
	DataNode DataNodeState
	connection net.Conn
	blockId BlockID
	forwardTo []string
	size int64
	checksum string
}

func (self *RPC) GetBlock(blockID *BlockID, size *int64) error {
	if self.state != Start {
		return errors.New("Not allowed in current state")
	}

	self.blockId = *blockID
	self.forwardTo = nil
	self.state = Giving

	// Should be fine if reading,
	// fail if uploading or deleting
	self.DataNode.Manager.LockRead(*blockID)

	return nil
}

func (self *RPC) ForwardBlock(blockMsg *ForwardBlock, _ *int) error {
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

	// Should blow up if block already exists?
	self.DataNode.Manager.LockReceive(self.blockId)
	return nil
}

func (self *RPC) Confirm(checksum *string, _ *int) error {
	if self.state != ReadyToConfirm {
		return errors.New("Not allowed in current session state")
	}
	if *checksum != self.checksum {
		self.DataNode.Manager.AbortReceive(self.blockId)
		_ = self.DataNode.Store.DeleteBlock(self.blockId)
		log.Println("Checksum doesn't match for", self.blockId)
		return errors.New("Checksum doesn't match!")
	}

	err := self.DataNode.Store.WriteChecksum(self.blockId, *checksum)
	if err != nil {
		// TODO: Remove block
		self.DataNode.Manager.AbortReceive(self.blockId)
		_ = self.DataNode.Store.DeleteBlock(self.blockId)
		log.Fatalln("Couldn't write checksum:", err)
		return errors.New("Couldn't write checksum")
	}

	self.DataNode.Manager.CommitReceive(self.blockId)
	// Combine into Block Manager?
	State.HaveBlocks([]BlockID{self.blockId})

	// Pipeline!
	if len(self.forwardTo) > 0 {
		State.forwardingBlocks <- ForwardBlock{self.blockId, self.forwardTo, -1}
	}

	self.blockId = ""
	self.size = -1
	self.state = Done
	self.checksum = ""
	return nil	
}

func sendBlock(blockID BlockID, peers []string) {
	if err := State.Manager.LockRead(blockID); err != nil {
		log.Println("Couldn't lock", blockID)
		return
	}
	defer State.Manager.UnlockRead(blockID)

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

	size, err := State.Store.BlockSize(blockID)
	if err != nil {
		log.Fatal("Stat error: ", err)
	}

	err = peer.Call("RPC.ForwardBlock",
		&ForwardBlock{blockID, forwardTo, size},
		nil)
	if err != nil {
		log.Fatal("ForwardBlock error: ", err)
	}

	err = State.Store.ReadBlock(blockID, size, peerConn)
	if err != nil {
		log.Fatal("Copying error: ", err)
	}
	
	hash, err := State.Store.ReadChecksum(blockID)
	if err != nil {
		log.Fatalln("Reading checksum:", err)
	}
	err = peer.Call("RPC.Confirm", hash, nil)
	if err != nil {
		log.Fatal("Confirm error: ", err)
	}
}

func (self *RPC) receiveBlock() {
	checksum, err := self.DataNode.Store.WriteBlock(self.blockId, self.size, self.connection)
	if err != nil {
		log.Fatal("Writing block:", err)
	}
	log.Println("Received block '" + string(self.blockId) + "' from", self.connection.RemoteAddr())
	self.checksum = checksum
	self.state = ReadyToConfirm
}

func (self *RPC) giveBlock() {
	size, err := self.DataNode.Store.BlockSize(self.blockId)
	if err != nil {
		log.Fatalln("Getting block size:", err)
	}
	err = self.DataNode.Store.ReadBlock(self.blockId, size, self.connection)
	if err != nil {
		log.Fatalln("Copying error: ", err)
	}
	self.DataNode.Manager.UnlockRead(self.blockId)
	self.state = Done
}

func RunRPC(c net.Conn, dn DataNodeState) {
	server := rpc.NewServer()
	session := &RPC{Start, State, c, "", nil, -1, ""}
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
			session.receiveBlock()
		case Giving:
			session.giveBlock()
		}
	}
}