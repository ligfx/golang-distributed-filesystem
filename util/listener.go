package util

import (
	"log"
	"net"
)

func Listen(port string) (string, chan net.Conn) {
	sock, err := net.Listen("tcp", ":" + port)
	if err != nil {
		log.Fatal(err)
	}
	c := make(chan net.Conn)
	go func() {
		for {
			conn, err := sock.Accept()
			if err != nil {
				log.Fatal(err)
			}
			c <- conn
		}
	}()
	return sock.Addr().String(), c
}