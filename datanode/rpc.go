package datanode

import (
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"strings"

	. "github.com/michaelmaltese/golang-distributed-filesystem/common"
)

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
		peerCodec = LoggingClientCodec(
			peerConn.RemoteAddr().String(),
			peerCodec)
	}
	peer := rpc.NewClientWithCodec(peerCodec)
	defer peer.Close()

	size, err := State.Store.BlockSize(blockID)
	if err != nil {
		log.Fatal("Stat error: ", err)
	}

	err = peer.Call("Forward",
		&ForwardBlock{blockID, forwardTo, size},
		nil)
	if err != nil {
		log.Fatal("Forward error: ", err)
	}

	err = State.Store.ReadBlock(blockID, peerConn)
	if err != nil {
		log.Fatal("Copying error: ", err)
	}

	hash, err := State.Store.ReadChecksum(blockID)
	if err != nil {
		log.Fatalln("Reading checksum:", err)
	}
	err = peer.Call("Confirm", hash, nil)
	if err != nil {
		log.Fatal("Confirm error: ", err)
	}
}

func RunRPC(c net.Conn, dn DataNodeState) {
	server := NewRPCServer(c)
	defer c.Close()

	var method string
	method, err := server.ReadHeader()
	if err != nil {
		log.Println(err)
		return
	}
	switch method {
	case "Forward":
		var blockMsg ForwardBlock
		if err := server.ReadBody(&blockMsg); err != nil {
			log.Println(err)
			return
		}
		blockID := blockMsg.BlockID
		size := blockMsg.Size
		forwardTo := blockMsg.Nodes
		if size <= 0 {
			server.Error("Size must be >0")
			return
		}
		dn.Manager.LockReceive(blockID)
		server.SendOkay()

		localChecksum, err := dn.Store.WriteBlock(
			blockID,
			size,
			c)
		if err != nil {
			log.Println("Writing block:", err)
			server.Error("Writing block")
			return
		}
		log.Println("Received block '"+string(blockID)+"' from", c.RemoteAddr())

		method, err = server.ReadHeader(); if err != nil {
			log.Println(err)
			dn.Manager.AbortReceive(blockID)
			dn.Store.DeleteBlock(blockID)
			return
		}
		if method != "Confirm" {
			server.Unacceptable()
			dn.Manager.AbortReceive(blockID)
			dn.Store.DeleteBlock(blockID)
			return
		}
		var remoteChecksum string
		if err := server.ReadBody(&remoteChecksum); err != nil {
			dn.Manager.AbortReceive(blockID)
			dn.Store.DeleteBlock(blockID)
			return
		}
		if remoteChecksum != localChecksum {
			dn.Manager.AbortReceive(blockID)
			dn.Store.DeleteBlock(blockID)
			log.Println("Checksum doesn't match for", blockID)
			server.Error("Checksum doesn't match")
			return
		}
		if err := dn.Store.WriteChecksum(blockID, remoteChecksum); err != nil {
			dn.Manager.AbortReceive(blockID)
			dn.Store.DeleteBlock(blockID)
			log.Fatalln("Couldn't write checksum:", err)
			server.Error("Couldn't write checksum")
			return
		}
		server.SendOkay()
		dn.Manager.CommitReceive(blockID)
		// Combine into Block Manager?
		State.HaveBlocks([]BlockID{blockID})
		// Pipeline!
		if len(forwardTo) > 0 {
			State.forwardingBlocks <- ForwardBlock{blockID, forwardTo, -1}
		}


	case "Get":
		var blockID BlockID
		if err := server.ReadBody(&blockID); err != nil {
			log.Println(err)
			return
		}
		if err := dn.Manager.LockRead(blockID); err != nil {
			server.Error("Couldn't get read lock")
			return
		}
		defer dn.Manager.UnlockRead(blockID)
		server.SendOkay()
		if err := dn.Store.ReadBlock(blockID, c); err != nil {
			log.Fatalln("Copying error: ", err)
		}

	default:
		server.Unacceptable()
	}
}

func (self *DataNodeState) RPCServer(port string) {
	sock, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalln(err)
	}
	log.Print("Accepting connections on " + sock.Addr().String())
	_, realPort, err := net.SplitHostPort(sock.Addr().String())
	if err != nil {
		log.Fatalln("SplitHostPort error:", err)
	}
	// Weird race condition with heartbeat, do this first
	Port = realPort

	for {
		conn, err := sock.Accept()
		if err != nil {
			log.Fatalln(err)
		}
		go RunRPC(conn, State)
	}
}