// Building a distributed system in Go, to:
// 1) Learn Go
// 2) Learn more about distributed systems

package main

import (
	"bufio"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/nu7hatch/gouuid"
)

type Session struct {
	Reader *bufio.Reader
	Writer io.Writer
	*log.Logger
	*DataNodeTracker
}

func NewSession(conn net.Conn, tracker *DataNodeTracker) *Session {
	return &Session{bufio.NewReader(conn),
		conn,
		log.New(os.Stderr, conn.RemoteAddr().String()+" ", log.LstdFlags),
		tracker}
}

func (s Session) Encode(args ...string) {
	// should check for errors...
	s.Writer.Write([]byte(strings.Join(args, " ") + "\n"))
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

func handleCreate(session *Session) {
	for {
		message_type, _, err := session.Decode()
		if err != nil {
			session.Print(err)
			return
		}

		switch message_type {
		case "APPEND":
			u4, err := uuid.NewV4()
			if err != nil {
				session.Print(err)
				return
			}
			// Get list of datanodes
			session.Encode(append([]string{u4.String()}, session.GetDataNodes()...)...)
		default:
			session.Printf("Unknown message_type: %#v", message_type)
			return
		}
	}
}

func handleConnection(session *Session) {
	message_type, _, err := session.Decode()
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

type DataNodeTracker struct {
	dataNodes []string
	mutex sync.Mutex
}

func NewDataNodeTracker() *DataNodeTracker {
	var self DataNodeTracker
	self.dataNodes = make([]string, 0)
	self.dataNodes = append(self.dataNodes, "127.0.0.1:5051")
	return &self
}

func (d *DataNodeTracker) GetDataNodes() []string {
	d.mutex.Lock()
	l := make([]string, len(d.dataNodes))
	copy(l, d.dataNodes)
	d.mutex.Unlock()
	return l
}

func MetadataNode() bool {
	var (
		port = flag.String("port", "5050", "port to listen on")
	)
	flag.Parse()

	socket, err := net.Listen("tcp", ":" + *port)
	if err != nil {
		log.Fatal(err)
	}
	dt := NewDataNodeTracker()
	log.Print("Accepting connections on :" + *port)
	for {
		conn, err := socket.Accept()
		log.Print("Connection from ", conn.RemoteAddr())
		if err != nil {
			log.Print(err)
			continue
		}
		session := NewSession(conn, dt)
		go handleConnection(session)
	}

	return true
}
