package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
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
		leaderAddress := flag.String("leaderAddress", "[::1]:5051", "")
		heartbeatInterval := flag.Duration("heartbeatInterval", 3*time.Second, "")
		flag.Parse()

		listener, err := net.Listen("tcp", ":"+fmt.Sprintf("%d", *port)); if err != nil {
			log.Fatal(err) }

		conf := datanode.Config{
			DataDir: *dataDir,
			Debug: debug,
			Listener: listener,
			HeartbeatInterval: *heartbeatInterval,
			LeaderAddress: *leaderAddress}
		datanode.Create(conf)
		// Wait on goroutines
		<- make(chan bool)
	})

	cli.Command("metadatanode", "Run leader", func(flag command.Flags) {
		clientPort := flag.Int("clientListen", 5050, "")
		peerPort   := flag.Int("peerListen", 5051, "")
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
		leaderAddress := flag.String("leaderAddress", "[::1]:5050", "")
		flag.Parse()

		file, err := os.Open(*filename); if err != nil {
			log.Fatal(err)
		}
		upload.Upload(file, debug, *leaderAddress)
	})

	cli.Run()
}
