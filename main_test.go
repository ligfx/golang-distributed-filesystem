package main

import (
	"testing"

	"github.com/michaelmaltese/golang-distributed-filesystem/metadatanode"
	"github.com/michaelmaltese/golang-distributed-filesystem/datanode"
	"github.com/michaelmaltese/golang-distributed-filesystem/pkg/chanio"
)

func TestIntegration(*testing.T) {

	network := chanio.NewNetwork()

	mdnClientListener := network.Listen()
	mdnClusterListener := network.Listen()

	_, _ = metadatanode.Create(metadatanode.Config{
		ClientListener: mdnClientListener,
		ClusterListener: mdnClusterListener,
		ReplicationFactor: 2,
		DatabaseFile: ":memory:",
		})

	dnListener := network.Listen()

	_, _ = datanode.Create(datanode.Config{
		Listener: dnListener,
		DataDir: "_data",
		HeartbeatInterval: 1,
		})
}