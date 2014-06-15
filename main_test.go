package main

import (
	"os"
	"testing"
	"time"

	"github.com/michaelmaltese/golang-distributed-filesystem/metadatanode"
	"github.com/michaelmaltese/golang-distributed-filesystem/datanode"
	"github.com/michaelmaltese/golang-distributed-filesystem/upload"
	"github.com/michaelmaltese/golang-distributed-filesystem/pkg/chanio"
)

func TestIntegration(*testing.T) {
	network := chanio.NewNetwork()

	mdnClusterListener := network.Listen()
	mdnClientListener := network.Listen()

	_, _ = metadatanode.Create(metadatanode.Config{
		ClientListener: mdnClientListener,
		ClusterListener: mdnClusterListener,
		ReplicationFactor: 2,
		DatabaseFile: ":memory:",
		})

	dnListener1 := network.Listen()
	_, _ = datanode.Create(datanode.Config{
		Listener: dnListener1,
		LeaderAddress: mdnClusterListener.Addr().String(),
		Network: network,
		DataDir: "_data",
		HeartbeatInterval: 1 * time.Second,
		})

	dnListener2 := network.Listen()
	_, _ = datanode.Create(datanode.Config{
		Listener: dnListener2,
		LeaderAddress: mdnClusterListener.Addr().String(),
		Network: network,
		DataDir: "_data2",
		HeartbeatInterval: 1 * time.Second,
		})

	go func() {
		file, err := os.Open("Godeps"); if err != nil {
			panic(err)
		}
		upload.Upload(file, false, network, mdnClientListener.Addr().String())
	}()

	time.Sleep(10 * time.Second)
}