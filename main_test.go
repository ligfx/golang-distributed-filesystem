package main

import (
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/michaelmaltese/golang-distributed-filesystem/metadatanode"
	"github.com/michaelmaltese/golang-distributed-filesystem/datanode"
	"github.com/michaelmaltese/golang-distributed-filesystem/upload"
)

func TestIntegration(*testing.T) {

	mdnClientListener, err := net.Listen("tcp", "[::1]:0"); if err != nil {
		log.Fatal(err) }
	mdnClusterListener, err := net.Listen("tcp", "[::1]:0"); if err != nil {
		log.Fatal(err) }

	_, _ = metadatanode.Create(metadatanode.Config{
		ClientListener: mdnClientListener,
		ClusterListener: mdnClusterListener,
		ReplicationFactor: 2,
		DatabaseFile: ":memory:",
		})

	log.Println(mdnClusterListener.Addr().String())

	dnListener1, err := net.Listen("tcp", ":0"); if err != nil {
		log.Fatal(err) }
	_, _ = datanode.Create(datanode.Config{
		Listener: dnListener1,
		LeaderAddress: mdnClusterListener.Addr().String(),
		DataDir: "_data",
		HeartbeatInterval: 1 * time.Second,
		})

	dnListener2, err := net.Listen("tcp", ":0"); if err != nil {
		log.Fatal(err) }
	_, _ = datanode.Create(datanode.Config{
		Listener: dnListener2,
		LeaderAddress: mdnClusterListener.Addr().String(),
		DataDir: "_data2",
		HeartbeatInterval: 1 * time.Second,
		})

	file, err := os.Open("Godeps"); if err != nil {
		panic(err)
	}
	upload.Upload(file, false, mdnClientListener.Addr().String())
	
	time.Sleep(5 * time.Second)
	
}