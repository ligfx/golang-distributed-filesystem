package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	"github.com/michaelmaltese/golang-distributed-filesystem/pkg/command"

	"github.com/michaelmaltese/golang-distributed-filesystem/datanode"
	"github.com/michaelmaltese/golang-distributed-filesystem/metadatanode"
	"github.com/michaelmaltese/golang-distributed-filesystem/upload"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	var debug bool

	cli := command.App()
	cli.Global(func(flag command.Flags) {
		flag.BoolVar(&debug, "debug", false, "Show debug messages")
	})

	cli.Command("datanode", "Run storage node", func(flag command.Flags) {
		port := flag.Int("port", 0, "")
		dataDir := flag.String("dataDir", "_data", "")
		heartbeatInterval := flag.Duration("heartbeatInterval", 3*time.Second, "")
		flag.Parse()

		conf := datanode.Config{
			*dataDir,
			debug,
			fmt.Sprintf("%d", *port),
			*heartbeatInterval}
		datanode.Create(conf)
		// Wait on goroutines
		<- make(chan bool)
	})

	cli.Command("metadatanode", "Run leader", func(flag command.Flags) {
		clientPort := flag.Int("clientport", 5050, "")
		peerPort   := flag.Int("peerport", 5051, "")
		replicationFactor := flag.Int("replicationFactor", 2, "")
		flag.Parse()

		log.Println("Replication factor of", *replicationFactor)
		clientListener, err := net.Listen("tcp", ":"+fmt.Sprintf("%d", *clientPort)); if err != nil {
			log.Fatal(err) }
		clusterListener, err := net.Listen("tcp", ":"+fmt.Sprintf("%d", *peerPort)); if err != nil {
			log.Fatal(err) }
		conf := metadatanode.Config{
			clientListener,
			clusterListener,
			*replicationFactor,
			"metadata.db"}
		metadatanode.Create(conf)
		// Wait on goroutines
		<- make(chan bool)
	})

	cli.Command("upload", "Upload a file", func(flag command.Flags) {
		filename := flag.String("file", "", "")
		flag.Parse()
		upload.Upload(*filename, debug)
	})
	
	cli.Run()
}
