// Command-line tool to upload files to cluster.
package upload

import (
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"strings"

	. "github.com/michaelmaltese/golang-distributed-filesystem/common"
)

func Upload(file *os.File, debug bool) {
	localFileInfo, err := file.Stat()
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
	if debug {
		codec = LoggingClientCodec(
			conn.RemoteAddr().String(),
			codec)
	}
	client := rpc.NewClientWithCodec(codec)

	var blobId string
	err = client.Call("CreateBlob", nil, &blobId)
	if err != nil {
		log.Fatal("CreateBlob error:", err)
	}

	bytesLeft := localFileSize
	for bytesLeft > 0 {
		var nodesMsg ForwardBlock
		err = client.Call("Append", nil, &nodesMsg)
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
		if debug {
			dataNodeCodec = LoggingClientCodec(
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

		err = dataNodeClient.Call("Forward",
			&ForwardBlock{nodesMsg.BlockID, forwardTo, size},
			nil)
		if err != nil {
			log.Fatal("ForwardBlock error: ", err)
		}

		hash := crc32.NewIEEE()
		io.CopyN(dataNode, io.TeeReader(file, hash), size)

		log.Println("Uploading block with checksum", fmt.Sprint(hash.Sum32()))
		err = dataNodeClient.Call("Confirm", fmt.Sprint(hash.Sum32()), nil)
		if err != nil {
			log.Fatal("Confirm error: ", err)
		}
	}

	err = client.Call("Commit", nil, nil)
	if err != nil {
		log.Fatal("Commit error:", err)
	}
	fmt.Println("Blob ID:", blobId)
}
