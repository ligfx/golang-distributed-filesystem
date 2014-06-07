// Command-line tool to upload files to cluster.
package upload

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"net/rpc"
	"net/rpc/jsonrpc"
	"strings"

	"github.com/michaelmaltese/golang-distributed-filesystem/comm"
	"github.com/michaelmaltese/golang-distributed-filesystem/util"
)

func Upload() {
	var err error

	fmt.Println(strings.Join(os.Args, " "))

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


		dataNodeCodec := jsonrpc.NewClientCodec(dataNode)
		if *debug {
			dataNodeCodec = util.LoggingClientCodec(
				dataNode.RemoteAddr().String(),
				dataNodeCodec)
		}
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

		file, err := os.Open(*localFileName)
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
	
	err = client.Call("ClientSession.Commit", nil, nil)
	if err != nil {
		log.Fatal("Commit error:", err)
	}
	fmt.Println("Blob ID:", blobId)
}
