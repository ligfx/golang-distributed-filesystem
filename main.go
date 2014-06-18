package main

import (
	"log"
	"math/rand"
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
		listener := command.ListenerFlag(flag, "port", 0, "")
		dataDir := flag.String("dataDir", "_data", "")
		leaderAddress := flag.String("leaderAddress", "[::1]:5051", "")
		heartbeatInterval := flag.Duration("heartbeatInterval", 3*time.Second, "")
		flag.Parse()

		conf := datanode.Config{
			DataDir:           *dataDir,
			Debug:             debug,
			Listener:          listener.Get(),
			HeartbeatInterval: *heartbeatInterval,
			LeaderAddress:     *leaderAddress}
		datanode.Create(conf)
		// Wait on goroutines
		<-make(chan bool)
	})

	cli.Command("metadatanode", "Run leader", func(flag command.Flags) {
		clientListener := command.ListenerFlag(flag, "clientPort", 5050, "")
		clusterListener := command.ListenerFlag(flag, "clusterPort", 5051, "")
		replicationFactor := flag.Int("replicationFactor", 2, "")
		flag.Parse()

		log.Println("Replication factor of", *replicationFactor)
		conf := metadatanode.Config{
			clientListener.Get(),
			clusterListener.Get(),
			*replicationFactor,
			"metadata.db"}
		metadatanode.Create(conf)
		// Wait on goroutines
		<-make(chan bool)
	})

	cli.Command("upload", "Upload a file", func(flag command.Flags) {
		file := command.FileFlag(flag, "file", "")
		leaderAddress := flag.String("leaderAddress", "[::1]:5050", "")
		flag.Parse()

		upload.Upload(file.Get(), debug, *leaderAddress)
	})

	cli.Run()
}
