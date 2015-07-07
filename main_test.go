package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	. "golang-distributed-filesystem/common"
	"golang-distributed-filesystem/datanode"
	"golang-distributed-filesystem/metadatanode"
	"golang-distributed-filesystem/upload"
)

type ByRandom []string

func (s ByRandom) Len() int {
	return len(s)
}
func (s ByRandom) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s ByRandom) Less(i, j int) bool {
	return rand.Intn(2) == 0 // 0 or 1
}

// Here's a test. It uploads 18 small blobs onto 2 data nodes, then starts
// 2 additional datanodes, then downloads the blobs.
// TODO:
//   - Decommission nodes
//   - Check that files are the same
//   - Random data
//   - Bigger blobs / more blocks
func TestIntegration(*testing.T) {

	mdnClientListener, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		log.Fatal(err)
	}
	mdnClusterListener, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		log.Fatal(err)
	}

	_, _ = metadatanode.Create(metadatanode.Config{
		ClientListener:    mdnClientListener,
		ClusterListener:   mdnClusterListener,
		ReplicationFactor: 2,
		DatabaseFile:      "metadata.test.db",
	})

	log.Println(mdnClusterListener.Addr().String())

	dnListener1, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	_, _ = datanode.Create(datanode.Config{
		Listener:          dnListener1,
		LeaderAddress:     mdnClusterListener.Addr().String(),
		DataDir:           "_data",
		HeartbeatInterval: 1 * time.Second,
	})

	dnListener2, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	_, _ = datanode.Create(datanode.Config{
		Listener:          dnListener2,
		LeaderAddress:     mdnClusterListener.Addr().String(),
		DataDir:           "_data2",
		HeartbeatInterval: 1 * time.Second,
	})

	wg := new(sync.WaitGroup)
	wg2 := new(sync.WaitGroup)
	doneBalancing := new(sync.WaitGroup)
	for _, _ = range make([]bool, 18) {
		wg.Add(1)
		wg2.Add(1)
		doneBalancing.Add(1)
		go func() {
			file, err := os.Open("Makefile")
			if err != nil {
				panic(err)
			}
			blobID := upload.Upload(file, false, mdnClientListener.Addr().String())

			wg.Done()
			doneBalancing.Wait()

			conn, err := net.Dial("tcp", mdnClientListener.Addr().String())
			if err != nil {
				log.Fatal("Dial error:", err)
			}
			defer conn.Close()
			codec := jsonrpc.NewClientCodec(conn)
			client := rpc.NewClientWithCodec(codec)

			var blocks []BlockID
			fmt.Println(blobID)
			if err := client.Call("GetBlob", blobID, &blocks); err != nil {
				log.Fatal("GetBlob error:", err)
			}
			fmt.Println(blocks)

			for _, b := range blocks {
				for {
					conn, err := net.Dial("tcp", mdnClientListener.Addr().String())
					if err != nil {
						log.Fatal("Dial error:", err)
					}
					defer conn.Close()
					codec := jsonrpc.NewClientCodec(conn)
					client := rpc.NewClientWithCodec(codec)

					var nodes []string
					if err := client.Call("GetBlock", b, &nodes); err != nil {
						log.Fatal("GetBlock error:", err)
					}
					fmt.Println(nodes)
					sort.Sort(ByRandom(nodes))

					conn, err = net.Dial("tcp", nodes[0])
					if err != nil {
						log.Fatalln("Dial error:", err)
					}
					defer conn.Close()
					codec = jsonrpc.NewClientCodec(conn)
					client = rpc.NewClientWithCodec(codec)
					if err := client.Call("Get", b, nil); err != nil {
						if fmt.Sprint(err) == "Couldn't get read lock" {
							time.Sleep(100 * time.Millisecond)
							continue
						}
						log.Fatalln(err)
					}

					if _, err := io.Copy(ioutil.Discard, conn); err != nil {
						log.Fatalln(err)
					}
					break
				}
			}

			wg2.Done()
		}()
	}

	wg.Wait()
	dnListener3, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	_, _ = datanode.Create(datanode.Config{
		Listener:          dnListener3,
		LeaderAddress:     mdnClusterListener.Addr().String(),
		DataDir:           "_data3",
		HeartbeatInterval: 1 * time.Second,
	})
	dnListener4, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	_, _ = datanode.Create(datanode.Config{
		Listener:          dnListener4,
		LeaderAddress:     mdnClusterListener.Addr().String(),
		DataDir:           "_data4",
		HeartbeatInterval: 1 * time.Second,
	})
	time.Sleep(5 * time.Second)
	for _, _ = range make([]bool, 18) {
		doneBalancing.Done()
	}
	wg2.Wait()
}
