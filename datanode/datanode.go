// Stores blocks for the cluster.
package datanode

import (
	"flag"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"net"
	"strconv"
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
		// Need to keep track of which files we actually have
		// and aren't in the middle of receiving or deleting
		return

		for {
			time.Sleep(5 * time.Second)
			log.Println("Checking block integrity...")
			files, err := ioutil.ReadDir(path.Join(DataDir, "blocks"))
			if err != nil {
				log.Fatal("Reading directory '" + DataDir + "': ", err)
			}
			for _, f := range files {
				name := f.Name()
				hashFile, err := ioutil.ReadFile(path.Join(DataDir, "meta", name + ".crc32"))
				if err != nil {
					log.Println("Reading checksum:", path.Join(DataDir, "meta", name + ".crc32"))
					log.Println("Skipping for now")
					continue
				}

				crc := crc32.NewIEEE()
				block, err := os.Open(path.Join(DataDir, "blocks", name))
				if err != nil {
					log.Println("Opening block:", err)
					log.Println("Skipping for now")
					continue
				}
				_, err = io.CopyN(crc, block, f.Size())
				if err != nil {
					log.Fatalln("Hashing block:", block)
				}
				hash, err := strconv.ParseInt(string(hashFile), 10, 64)
				if err != nil {
					log.Fatal("Error parsing hash")
				}

				if crc.Sum32() != uint32(hash) {
					log.Println("Checksum doesn't match block:", name)
					State.RemoveBlock(BlockID(name))
				}
			}
		}
	}()

	// Server
	for {
		conn := <- socket
		go RunRPC(conn, State)
	}
}