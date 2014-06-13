// Keeps tracks of blobs, blocks, and other nodes.
package metadatanode

import (
	"flag"
	"log"
)

type SessionState int

const (
	Start SessionState = iota
	Creating
)

var State *MetaDataNodeState
var Debug bool

func MetadataNode() {
	State = NewMetaDataNodeState()
	var (
		clientPort = flag.String("clientport", "5050", "port to listen on")
		peerPort   = flag.String("peerport", "5051", "port to listen on")
	)
	flag.BoolVar(&Debug, "debug", false, "Show RPC conversations")
	flag.IntVar(&State.ReplicationFactor, "replicationFactor", 2, "")
	flag.Parse()

	log.Println("Replication factor of", State.ReplicationFactor)

	go State.Monitor()
	go State.ClientRPC(*clientPort)
	go State.PeerRPC(*peerPort)

	// Let goroutines run forever
	<-make(chan bool)
}
