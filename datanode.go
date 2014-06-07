package main

import (
	"flag"
	"io"
	"log"
	"net"
	"os"
	"path"
	"strconv"
)

func dnHandleRequest(c net.Conn) {
	logger := log.New(os.Stderr, c.RemoteAddr().String()+" ", log.LstdFlags)
	ed := NewEncodeDecoder(io.TeeReader(c, &LoggerWrapper{logger}), c)

	var msg []string
	var err error
	msg, err = ed.Decode()
	if err != nil {
		logger.Println(err)
		return
	}
	// TODO: Implement pipelining
	if msg[0] != "BLOCK" || len(msg) != 2 {
		return
	}
	id := msg[1]

	maxsize := int64(128 * 1024 * 1024)
	ed.Encode("OK", "MAXSIZE", strconv.FormatInt(maxsize, 10))

	msg, err = ed.Decode()
	if err != nil {
		logger.Println(err)
		return
	}
	if msg[0] != "SIZE" || len(msg) != 2 {
		return
	}
	size, err := strconv.ParseInt(msg[1], 10, 64)
	if size > maxsize || size <= 0 {
		return
	}

	ed.Encode("OK")

	pwd, err := os.Getwd()
	if err != nil {
		logger.Print(err)
		return
	}

	file, err := os.Create(path.Join(pwd, id))
	if err != nil {
		logger.Print(err)
		return
	}
	defer file.Close()

	io.CopyN(file, c, size)

	ed.Encode("OK")
	c.Close()
}

func DataNode() bool {
	var (
		port = flag.String("port", "5051", "port to listen on")
	)
	flag.Parse()

	socket, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("Accepting connections on :" + *port)
	for {
		conn, err := socket.Accept()
		log.Print("Connection from ", conn.RemoteAddr())
		if err != nil {
			log.Print(err)
			continue
		}
		// session := NewSession(conn)
		go dnHandleRequest(conn)
	}

	return true
}
