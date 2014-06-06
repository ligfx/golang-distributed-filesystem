package main

import (
	"bufio"
	"flag"
	// "fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

type Session struct {
	Reader *bufio.Reader
	Writer io.Writer
	*log.Logger
}

func NewSession(conn net.Conn) *Session {
	return &Session{bufio.NewReader(conn), conn, log.New(os.Stderr, conn.RemoteAddr().String()+" ", log.LstdFlags)}
}

func (s Session) Encode(args ...string) {
	// should check for errors...
	for _, str := range args {
		s.Writer.Write([]byte(str))
	}
	s.Writer.Write([]byte("\n"))
}

func (s Session) Decode() (string, []string, error) {
	line, err := s.Reader.ReadString('\n')
	if err != nil {
		return "", nil, err
	}

	_splits := strings.Split(strings.TrimSpace(line), " ")
	message_type := _splits[0]
	message := _splits[1:]

	s.Print("Message of type: ", message_type)
	s.Print("Message body: ", message)

	return message_type, message, nil
}

func handleCreate(session Session) {
	message_type, message, err := session.Decode()
	if err != nil {
		session.Print(err)
		return
	}

	switch message_type {
	case "APPEND":
		session.Encode("OK")
		handleCreate(session)
		return
	default:
		session.Printf("Unknown message_type: %#v", message_type)
		return
	}
}

func handleConnection(conn net.Conn) {
	session := NewSession(conn)

	message_type, message, err := session.Decode()
	if err != nil {
		session.Print(err)
		return
	}

	switch message_type {
	case "CREATE":
		session.Encode("OK")
		handleCreate(session)
		return
	default:
		session.Printf("Unknown message_type: %#v", message_type)
		return
	}
}

func main() {
	var (
		_ = flag.String("payload", "abc", "payload data")
		_ = flag.Duration("delay", 1*time.Second, "write delay")
	)
	flag.Parse()

	socket, err := net.Listen("tcp", ":5050")
	if err != nil {
		log.Fatal(err)
	}
	log.Print("Accepting connections on :5050")
	for {
		conn, err := socket.Accept()
		log.Print("Connection from ", conn.RemoteAddr())
		if err != nil {
			log.Print(err)
			continue
		}
		go handleConnection(conn)
	}
}
