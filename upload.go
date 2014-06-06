package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

func Upload() bool {
	if len(os.Args) != 1 {
		return false
	}
	// local := os.Args[0]

	conn, err := net.Dial("tcp", "localhost:5050")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	ed := NewEncodeDecoder(conn)

	// send CREATE
	// recv OK
	// send APPEND
	// recv OK $blockid $data_nodes

	ed.Encode("CREATE")
	msg_type, _, err := ed.Decode()
	// check err

	if msg_type != "OK" {
		fmt.Printf("Received %#v", msg_type)
		os.Exit(1)
	}

	ed.Encode("APPEND")
	msg_type, msg, err := ed.Decode()
	// check err
	fmt.Println(msg_type, strings.Join(msg, " "))

	return true
}