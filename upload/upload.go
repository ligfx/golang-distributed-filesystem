// Command-line tool to upload files to cluster.
package upload

import (
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"os"
	"net/rpc"
	"net/rpc/jsonrpc"
	"strings"

	. "github.com/michaelmaltese/golang-distributed-filesystem/comm"
	"github.com/michaelmaltese/golang-distributed-filesystem/util"
)

func Upload() {
	var err error
	var (
		localFileName = flag.String("file", "", "File to upload")
		debug = flag.Bool("debug", false, "Show RPC conversation")
	)
	flag.Parse()

	*localFileName = strings.TrimSpace(*localFileName)
	if len(flag.Args()) != 0 || len(*localFileName) == 0 {
		fmt.Println("Usage of " + os.Args[0] + ":")
		flag.PrintDefaults()
		os.Exit(2)
	}

	localFileInfo, err := os.Stat(*localFileName)
	if err != nil {
		log.Fatal("Stat error: ", err)
	}
	localFileSize := localFileInfo.Size()

	conn, err := net.Dial("tcp", "[::1]:5050")
	if err != nil {
		log.Fatal("Dial error:", err)
	}
	defer conn.Close()

	codec := jsonrpc.NewClientCodec(conn)
	if *debug {
		codec = util.LoggingClientCodec(
			conn.RemoteAddr().String(),
			codec)
	}
	client := rpc.NewClientWithCodec(codec)

	var blobId string
	err = client.Call("ClientSession.CreateBlob", nil, &blobId)
	if err != nil {
		log.Fatal("CreateBlob error:", err)
	}

	file, err := os.Open(*localFileName)
	if err != nil {
		log.Fatal("Open error: ", err)
	}
	defer file.Close()

	bytesLeft := localFileSize
	for bytesLeft > 0 {
		var nodesMsg ForwardBlock
		err = client.Call("ClientSession.Append", nil, &nodesMsg)
		if err != nil {
			log.Fatal("Append error:", err)
		}
		blockSize := nodesMsg.Size

		var dataNode net.Conn
		var forwardTo []string
		// Find a DataNode
		for i, addr := range nodesMsg.Nodes {
			dataNode, err = net.Dial("tcp", addr)
			if err == nil {
				forwardTo = append(nodesMsg.Nodes[:i], nodesMsg.Nodes[i+1:]...)
				break
			}
			dataNode = nil
		}
		if dataNode == nil {
			log.Fatalln("Couldn't connect to any DataNodes in:", strings.Join(nodesMsg.Nodes, " "))
		}
		defer dataNode.Close()

		dataNodeCodec := jsonrpc.NewClientCodec(dataNode)
		if *debug {
			dataNodeCodec = util.LoggingClientCodec(
				dataNode.RemoteAddr().String(),
				dataNodeCodec)
		}
		dataNodeClient := rpc.NewClientWithCodec(dataNodeCodec)

		var size int64
		if blockSize > bytesLeft {
			size = bytesLeft
			bytesLeft = 0
		} else {
			size = blockSize
			bytesLeft = bytesLeft - blockSize
		}

		err = dataNodeClient.Call("ClientSession.ForwardBlock",
			&ForwardBlock{nodesMsg.BlockID, forwardTo, size},
			nil)
		if err != nil {
			log.Fatal("ForwardBlock error: ", err)
		}

		hash := crc32.NewIEEE()
		io.CopyN(dataNode, io.TeeReader(file, hash), size)

		log.Println("Uploading block with checksum", fmt.Sprint(hash.Sum32()))
		err = dataNodeClient.Call("ClientSession.Confirm", fmt.Sprint(hash.Sum32()), nil)
		if err != nil {
			log.Fatal("Confirm error: ", err)
		}
	}
	
	err = client.Call("ClientSession.Commit", nil, nil)
	if err != nil {
		log.Fatal("Commit error:", err)
	}
	fmt.Println("Blob ID:", blobId)
}
