// Stores blocks for the cluster.
package datanode

import (
	"flag"
	"log"
	"os"
	"net"
	"sync"
	"time"

	. "github.com/michaelmaltese/golang-distributed-filesystem/comm"
	"github.com/michaelmaltese/golang-distributed-filesystem/util"
)

var (
	DataDir string
	Debug bool
	Port string
	State DataNodeState
)

func init() {
	State.forwardingBlocks = make(chan ForwardBlock)
	State.Manager.using = map[BlockID]*sync.WaitGroup{}
	State.Manager.receiving = map[BlockID]bool{}
	State.Manager.willDelete = map[BlockID]bool{}
	State.Manager.exists = map[BlockID]bool{}
}

func DataNode() {
	port := flag.String("port", "0", "port to listen on (0=random)")
	flag.StringVar(&DataDir, "dataDir", "_data", "directory to store data")
	flag.BoolVar(&Debug, "debug", false, "Show RPC conversations")
	flag.DurationVar(&State.heartbeatInterval, "heartbeatInterval", 3 * time.Second, "")
	flag.Parse()

	addr, socket := util.Listen(*port)
	log.Print("Accepting connections on " + addr)
	_, realPort, err := net.SplitHostPort(addr); if err != nil {
		log.Fatalln("SplitHostPort error:", err)
	}
	Port = realPort

	log.Print("Block storage in directory '" + State.Store.BlocksDirectory() + "'")
	if err = os.MkdirAll(State.Store.BlocksDirectory(), 0777); err != nil {
		log.Fatal("Making directory:", err)
	}

	log.Print("Meta storage in directory '" + State.Store.MetaDirectory() + "'")
	if err = os.MkdirAll(State.Store.MetaDirectory(), 0777); err != nil {
		log.Fatal("Making directory:", err)
	}

	// Heartbeat and registration
	go heartbeat()

	go func() {
		for {
			f := <- State.forwardingBlocks
			sendBlock(f.BlockID, f.Nodes)
		}
	}()

	go func() {
		for {
			time.Sleep(5 * time.Second)
			log.Println("Checking block integrity...")
			files, err := State.Store.ReadBlockList()
			if err != nil {
				log.Fatal("Reading directory '" + DataDir + "': ", err)
			}
			for _, f := range files {
				if err := State.Manager.LockRead(f); err != nil {
					// Being uploaded or deleted
					// May or may not actually exist now/in the future
					// Does not imply it actually exists!
					continue
				}
				storedChecksum, err := State.Store.ReadChecksum(f)
				if err != nil {
					go State.RemoveBlock(BlockID(f))
				}
				localChecksum, err := State.Store.LocalChecksum(f)
				if err != nil {
					go State.RemoveBlock(BlockID(f))
				}

				if storedChecksum != localChecksum {
					log.Println("Checksum doesn't match block:", f)
					log.Println(storedChecksum, localChecksum)
					go State.RemoveBlock(BlockID(f))
				}
				State.Manager.UnlockRead(f)
			}
		}
	}()

	// Server
	for {
		conn := <- socket
		go RunRPC(conn, State)
	}
}