package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
)

func Upload() bool {
	if len(os.Args) != 2 {
		return false
	}
	localFileName := os.Args[1]
	localFileInfo, err := os.Stat(localFileName)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	localFileSize := localFileInfo.Size()

	conn, err := net.Dial("tcp", "localhost:5050")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer conn.Close()
	ed := NewEncodeDecoder(io.TeeReader(conn, os.Stdout), conn)

	ed.Encode("CREATE")
	var msg []string
	msg, err = ed.Decode()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if msg[0] != "OK" || msg[1] != "BLOB" || len(msg) != 3 {
		fmt.Printf("Received %#v", msg)
		os.Exit(1)
	}

	// blobId := msg[2]

	bytesLeft := localFileSize
	for bytesLeft > 0 {

		ed.Encode("APPEND")
		msg, err = ed.Decode()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		// TODO: pipelining
		if msg[0] != "OK" || msg[1] != "BLOCK" || len(msg) != 4 {
			fmt.Printf("Received %#v", msg)
			os.Exit(1)
		}
		blockId := msg[2]
		dataNodeAddress := msg[3]

		dataNode, err := net.Dial("tcp", dataNodeAddress)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		defer dataNode.Close()

		dataNodeStream := NewEncodeDecoder(io.TeeReader(dataNode, os.Stdout), dataNode)

		dataNodeStream.Encode("BLOCK", blockId)

		msg, err = dataNodeStream.Decode()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if msg[0] != "OK" || msg[1] != "MAXSIZE" || len(msg) != 3 {
			fmt.Printf("Received %#v", msg)
			os.Exit(1)
		}
		maxsize, err := strconv.ParseInt(msg[2], 10, 64)
		if err != nil {
			fmt.Print(err)
			os.Exit(1)
		}

		var size int64
		if maxsize > bytesLeft {
			size = bytesLeft
			bytesLeft = 0
		} else {
			size = maxsize
			bytesLeft = bytesLeft - maxsize
		}

		dataNodeStream.Encode("SIZE", strconv.FormatInt(size, 10))

		msg, err = dataNodeStream.Decode()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if msg[0] != "OK" || len(msg) != 1 {
			fmt.Printf("Received %#v", msg)
			os.Exit(1)
		}

		// TODO: absolute paths
		file, err := os.Open(localFileName)
		if err != nil {
			fmt.Print(err)
			os.Exit(1)
		}
		defer file.Close()

		io.CopyN(dataNode, file, size)

		msg, err = dataNodeStream.Decode()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if msg[0] != "OK" || len(msg) != 1 {
			fmt.Printf("Received %#v", msg)
			os.Exit(1)
		}

		ed.Encode("OK")
	}

	return true
}
