// Keeps tracks of blobs, blocks, and other nodes.
package metadatanode

import (
	"flag"
	"log"
	"net"
)

var Debug bool

func MetadataNode() {
	var (
		clientPort = flag.String("clientport", "5050", "port to listen on")
		peerPort   = flag.String("peerport", "5051", "port to listen on")
		replicationFactor = flag.Int("replicationFactor", 2, "")
	)
	flag.BoolVar(&Debug, "debug", false, "Show RPC conversations")
	flag.Parse()

	log.Println("Replication factor of", *replicationFactor)

	clientListener, err := net.Listen("tcp", ":"+*clientPort)
	if err != nil {
		log.Fatal(err)
	}
	clusterListener, err := net.Listen("tcp", ":"+*peerPort)
	if err != nil {
		log.Fatal(err)
	}

	conf := Config{clientListener, clusterListener, *replicationFactor}
	_, _ = Create(conf)
	// Let goroutines run forever
	<- make(chan bool)
}
