// Building a distributed system in Go, to:
// 1) Learn Go
// 2) Learn more about distributed systems

package main

import (
	"flag"
	"io"
	"log"
	"net"
	"os"
	"sync"

	"github.com/nu7hatch/gouuid"
)

type Session struct {
	net.Conn
	EncodeDecoder
	*log.Logger
}

type LoggerWrapper struct {
	*log.Logger
}

func (l *LoggerWrapper) Write(p []byte) (n int, err error) {
	l.Print(string(p))
	return len(p), nil
}

func NewSession(conn net.Conn) *Session {
	logger := log.New(os.Stderr, conn.RemoteAddr().String()+" ", log.LstdFlags)

	ed := NewEncodeDecoder(io.TeeReader(conn, &LoggerWrapper{logger}), conn)
	return &Session{conn,
		ed,
		logger}
}

func handleCreate(session *Session, tracker *DataNodeTracker) {
	for {
		message, err := session.Decode()
		if err != nil {
			session.Print(err)
			return
		}

		switch message[0] {
		case "APPEND":
			u4, err := uuid.NewV4()
			if err != nil {
				session.Print(err)
				return
			}
			// Get list of datanodes
			session.Encode(append([]string{"OK", "BLOCK", u4.String()}, tracker.GetDataNodes()...)...)
		default:
			session.Printf("Unknown message_type: %#v", message[0])
			return
		}
	}
}

func handleConnection(session *Session, tracker *DataNodeTracker) {
	defer session.Close()

	message, err := session.Decode()
	if err != nil {
		session.Print(err)
		return
	}

	switch message[0] {
	case "CREATE":
		u4, err := uuid.NewV4()
		if err != nil {
			session.Print(err)
			return
		}
		session.Encode("OK", "BLOB", u4.String())
		handleCreate(session, tracker)
	default:
		session.Printf("Unknown message_type: %#v", message[0])
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
		session := NewSession(conn)
		go handleConnection(session, dt)
	}

	return true
}
