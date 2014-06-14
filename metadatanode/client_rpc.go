package metadatanode

import (
	"log"
	"net"

	. "github.com/michaelmaltese/golang-distributed-filesystem/common"
)

func runClientRPC(c net.Conn, mdn *MetaDataNodeState) {
	server := NewRPCServer(c)
	defer c.Close()

	var method string
	method, err := server.ReadHeader()
	if err != nil {
		log.Println(err)
		return
	}
	switch method {
	case "CreateBlob":
		if err := server.ReadBody(nil); err != nil {
			// TODO: Fatal bc I want to see if passing nil fails
			log.Fatalln(err)
			return
		}
		blobID := mdn.GenerateBlobId()
		server.Send(&blobID)
		var blocks []BlockID

		for {
			method, err = server.ReadHeader()
			if err != nil {
				// TODO: Handle this better: remove blob, blocks?
				log.Fatalln(err)
				return
			}
			switch method {
			case "Append":
				forwardBlock := mdn.GenerateBlock(blobID)
				blocks = append(blocks, forwardBlock.BlockID)
				server.Send(&forwardBlock)

			case "Commit":
				mdn.CommitBlob(blobID, blocks)
				log.Println("Committed blob '" + blobID + "' for", c.RemoteAddr())
				server.SendOkay()
				return

			default:
				server.Unacceptable()
			}
		}

	case "GetBlob":
		var blobID string
		if err := server.ReadBody(&blobID); err != nil {
			log.Println(err)
			return
		}
		blocks := mdn.GetBlob(blobID)
		server.Send(&blocks)

	case "GetBlock":
		var blockID BlockID
		if err := server.ReadBody(&blockID); err != nil {
			log.Println(err)
			return
		}
		nodes := mdn.GetBlock(blockID)
		server.Send(&nodes)

	default:
		server.Unacceptable()
	}
}


func (self *MetaDataNodeState) ClientRPCServer(port string) {
	clientSock, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Accepting client connections on", clientSock.Addr())
	for {
		client, err := clientSock.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go runClientRPC(client, self)
	}
}