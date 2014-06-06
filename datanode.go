package main

import (
	"flag"
	"fmt"
	"log"
	"net"
)

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
		go func(c net.Conn){
			fmt.Fprint(c, "OK")
			c.Close()
		}(conn)
	}

	return true
}
