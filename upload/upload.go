// Command-line tool to upload files to cluster.
package upload

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"net/rpc"
	"net/rpc/jsonrpc"

	"github.com/michaelmaltese/golang-distributed-filesystem/comm"
	"github.com/michaelmaltese/golang-distributed-filesystem/util"
)

func Upload() {
	var err error

	if len(os.Args) != 2 {
		fmt.Println("Usage: upload FILE")
		os.Exit(1)
	}
	localFileName := os.Args[1]
	localFileInfo, err := os.Stat(localFileName)
	if err != nil {
		panic(err)
	}
	localFileSize := localFileInfo.Size()

	conn, err := net.Dial("tcp", "localhost:5050")
	if err != nil {
		log.Fatal("Dial error:", err)
	}
	defer conn.Close()

	codec := util.LoggingClientCodec(
		conn.RemoteAddr().String(),
		jsonrpc.NewClientCodec(conn))
	client := rpc.NewClientWithCodec(codec)

	var blobId string
	err = client.Call("ClientSession.CreateBlob", nil, &blobId)
	if err != nil {
		log.Fatal("CreateBlob error:", err)
	}
	fmt.Println("Blob ID:", blobId)

	bytesLeft := localFileSize
	for bytesLeft > 0 {
		var nodesMsg comm.ForwardBlock
		err = client.Call("ClientSession.Append", nil, &nodesMsg)
		if err != nil {
			log.Fatal("Append error:", err)
		}

		dataNode, err := net.Dial("tcp", nodesMsg.Nodes[0])
		if err != nil {
			log.Fatal("DataNode dial error:", err)
		}
		defer dataNode.Close()

		dataNodeCodec := util.LoggingClientCodec(
			dataNode.RemoteAddr().String(),
			jsonrpc.NewClientCodec(dataNode))
		dataNodeClient := rpc.NewClientWithCodec(dataNodeCodec)

		var maxSize int64
		err = dataNodeClient.Call("ClientSession.ForwardBlock",
			&comm.ForwardBlock{nodesMsg.BlockId, []string{}},
			&maxSize)
		if err != nil {
			log.Fatal("ForwardBlock error: ", err)
		}

		var size int64
		if maxSize > bytesLeft {
			size = bytesLeft
			bytesLeft = 0
		} else {
			size = maxSize
			bytesLeft = bytesLeft - maxSize
		}

		err = dataNodeClient.Call("ClientSession.Size",
			&size, nil)
		if err != nil {
			log.Fatal("Size error: ", err)
		}

		file, err := os.Open(localFileName)
		if err != nil {
			log.Fatal("Open error: ", err)
		}
		defer file.Close()

		io.CopyN(dataNode, file, size)

		err = dataNodeClient.Call("ClientSession.Confirm", nil, nil)
		if err != nil {
			log.Fatal("Confirm error: ", err)
		}
	}
	/*
	
	var blobMsg comm.BlobName
	err = decoder.Decode(&blobMsg)
	if err != nil {
		panic(err)
	}
	fmt.Println(blobMsg.BlobId)

	bytesLeft := localFileSize
	for bytesLeft > 0 {
		encoder.Encode(comm.Append{})
		var nodesMsg comm.ForwardBlock
		err := decoder.Decode(&nodesMsg)
		if err != nil {
			panic(err)
		}
		dataNode, err := net.Dial("tcp", nodesMsg.Nodes[0])
		if err != nil {
			panic(err)
		}
		defer dataNode.Close()
		
		dataNodeDecoder := util.NewDecoder(io.TeeReader(dataNode, os.Stdout))
		dataNodeEncoder := util.NewEncoder(dataNode)

		dataNodeEncoder.Encode(comm.ForwardBlock{nodesMsg.BlockId, nil})

		var maxSizeMsg comm.MaxSize
		err = dataNodeDecoder.Decode(&maxSizeMsg)
		if err != nil {
			panic(err)
		}

		maxSize := maxSizeMsg.Size
		var size int64
		if maxSize > bytesLeft {
			size = bytesLeft
			bytesLeft = 0
		} else {
			size = maxSize
			bytesLeft = bytesLeft - maxSize
		}

		dataNodeEncoder.Encode(comm.Size{size})

		var okMsg comm.Ok
		err = dataNodeDecoder.Decode(&okMsg)
		if err != nil {
			panic(err)
		}

		// TODO: absolute paths
		file, err := os.Open(localFileName)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		io.CopyN(dataNode, file, size)

		err = dataNodeDecoder.Decode(&okMsg)
		if err != nil {
			panic(err)
		}
	}

	encoder.Encode("OK")

	*/
}
