package datanode

import (
	"io"
	"log"
	"net"

	. "github.com/michaelmaltese/golang-distributed-filesystem/common"
)

func RunRPC(c net.Conn, dn *DataNodeState) {
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
		defer dn.Manager.AbortAndDeleteIfNotCommitted(blockID)
		server.SendOkay()

		writer, err := dn.Store.CreateBlock(blockID)
		if err != nil {
			log.Println("Opening block:", err)
			server.Error("Opening block")
			return
		}
		_, err = io.CopyN(writer, c, size)
		if err != nil {
			log.Println("Writing block:", err)
			server.Error("Writing block")
			return
		}
		localChecksum := writer.Checksum()
		log.Println("Received block '"+string(blockID)+"' from", c.RemoteAddr())

		method, err = server.ReadHeader()
		if err != nil {
			log.Println(err)
			return
		}
		if method != "Confirm" {
			server.Unacceptable()
			return
		}
		var remoteChecksum string
		if err := server.ReadBody(&remoteChecksum); err != nil {
			return
		}
		if remoteChecksum != localChecksum {
			log.Println("Checksum doesn't match for", blockID)
			server.Error("Checksum doesn't match")
			return
		}
		if err := dn.Store.WriteChecksum(blockID, remoteChecksum); err != nil {
			log.Fatalln("Couldn't write checksum:", err)
			server.Error("Couldn't write checksum")
			return
		}
		server.SendOkay()

		// Combine into Block Manager?
		dn.Manager.CommitReceive(blockID)
		dn.newBlocks.Push([]BlockID{blockID})
		// Pipeline!
		go func() {
			if len(forwardTo) > 0 {
				dn.forwardingBlocks <- ForwardBlock{blockID, forwardTo, -1}
			}
		}()

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
		reader, err := dn.Store.OpenBlock(blockID)
		if err != nil {
			log.Fatalln("Open error:", err)
		}
		_, err = io.Copy(c, reader)
		if err != nil {
			log.Fatalln("Copy error:", err)
		}

	default:
		server.Unacceptable()
	}
}

func (self *DataNodeState) RPCServer(sock net.Listener) {
	log.Print("Accepting connections on " + sock.Addr().String())
	for {
		conn, err := sock.Accept()
		if err != nil {
			log.Fatalln(err)
		}
		go RunRPC(conn, self)
	}
}
