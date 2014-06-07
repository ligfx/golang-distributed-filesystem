// Keeps tracks of blobs, blocks, and other nodes.
package metadatanode

import (
	"flag"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"

	"github.com/michaelmaltese/golang-distributed-filesystem/util"
)

type SessionState int
const (
	Start SessionState = iota
	Creating
)

func rpcServer(c net.Conn, debug bool, obj interface{}) {
	server := rpc.NewServer()
	server.Register(obj)
	codec := jsonrpc.NewServerCodec(c)
	if debug {
		codec = util.LoggingServerCodec(c.RemoteAddr().String(), codec)
	}
	server.ServeCodec(codec)
}

func MetadataNode() {
	var (
		clientPort = flag.String("clientport", "5050", "port to listen on")
		peerPort = flag.String("peerport", "5051", "port to listen on")
		debug = flag.Bool("debug", false, "Show RPC conversations")
	)
	flag.Parse()

	clientAddr, clientChan := util.Listen(*clientPort)
	peerAddr, peerChan := util.Listen(*peerPort)
	log.Println("Accepting client connections on", clientAddr)
	log.Println("Accepting peer connections on", peerAddr)

	state := NewMetaDataNodeState()

	for {
		select {
		case client := <- clientChan:
			go rpcServer(client,
				*debug,
				&ClientSession{Start, state, "", nil, client.RemoteAddr().String()})

		case peer := <- peerChan:
			go rpcServer(peer,
				*debug,
				&PeerSession{Start, state, peer.RemoteAddr().String()})
		}
	}
}
