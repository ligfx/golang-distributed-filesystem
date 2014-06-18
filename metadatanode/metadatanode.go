package metadatanode

import (
	"bytes"
	"crypto/sha1"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dotcloud/docker/pkg/namesgenerator"
	"github.com/nu7hatch/gouuid"

	. "github.com/michaelmaltese/golang-distributed-filesystem/common"
)

type MetaDataNodeState struct {
	mutex                sync.RWMutex
	store                *DB
	dataNodes            map[NodeID]string
	dataNodesLastSeen    map[NodeID]time.Time
	dataNodesUtilization map[NodeID]int
	blocks               map[BlockID]map[NodeID]bool
	dataNodesBlocks      map[NodeID]map[BlockID]bool
	replicationIntents   ReplicationIntents
	deletionIntents      DeletionIntents
	ReplicationFactor    int
}

func Create(conf Config) (*MetaDataNodeState, error) {
	self := new(MetaDataNodeState)

	db, err := OpenDB(conf.DatabaseFile)
	log.Println("Persistent storage at", conf.DatabaseFile)
	if err != nil {
		log.Println("Metadata store error:", err)
		return nil, err
	}
	self.store = db

	self.dataNodesLastSeen = map[NodeID]time.Time{}
	self.dataNodes = map[NodeID]string{}
	self.dataNodesUtilization = map[NodeID]int{}
	self.blocks = map[BlockID]map[NodeID]bool{}
	self.dataNodesBlocks = map[NodeID]map[BlockID]bool{}

	self.ReplicationFactor = conf.ReplicationFactor
	go self.Monitor()
	go self.ClientRPCServer(conf.ClientListener)
	go self.ClusterRPCServer(conf.ClusterListener)

	return self, nil
}

func (self *MetaDataNodeState) GenerateBlobId() string {
	u4, err := uuid.NewV4()
	if err != nil {
		log.Fatal(err)
	}
	hash := sha1.Sum([]byte(u4.String()))
	a := []byte{85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85, 85}
	b := []byte{170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170}
	switch {
	case bytes.Compare(hash[:], a) == -1:
		log.Println("Blob in first third of circle")
	case bytes.Compare(hash[:], b) == -1:
		log.Println("Blob in second third of circle")
	default:
		log.Println("Blob in final third of circle")
	}

	return u4.String()
}

func (self *MetaDataNodeState) GenerateBlock(blob string) ForwardBlock {
	u4, err := uuid.NewV4()
	if err != nil {
		log.Fatalln(err)
	}
	block := BlockID(blob + ":" + u4.String())

	nodes := self.LeastUsedNodes()
	var forwardTo []NodeID
	if len(nodes) < self.ReplicationFactor {
		forwardTo = nodes
	} else {
		forwardTo = nodes[0:self.ReplicationFactor]
	}
	var addrs []string
	for _, nodeID := range forwardTo {
		addrs = append(addrs, self.dataNodes[nodeID])
	}

	// Lock?
	self.replicationIntents.Add(block, nil, forwardTo)
	return ForwardBlock{block, addrs, 128 * 1024 * 1024}
}

func (self *MetaDataNodeState) GetBlob(blobID string) []BlockID {
	self.mutex.RLock()
	defer self.mutex.RUnlock()

	names, err := self.store.Get(blobID)
	if err != nil {
		log.Fatalln(err)
	}

	// Is this really necessary?
	var blocks []BlockID
	for _, n := range names {
		blocks = append(blocks, BlockID(n))
	}

	return blocks
}

func (self *MetaDataNodeState) HasBlocks(nodeID NodeID, blocks []BlockID) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	for _, blockID := range blocks {
		self.replicationIntents.Done(nodeID, blockID)
		if self.blocks[blockID] == nil {
			self.blocks[blockID] = map[NodeID]bool{}
		}
		if self.dataNodesBlocks[nodeID] == nil {
			self.dataNodesBlocks[nodeID] = map[BlockID]bool{}
		}
		self.blocks[blockID][nodeID] = true
		self.dataNodesBlocks[nodeID][blockID] = true
	}
}

func (self *MetaDataNodeState) DoesntHaveBlocks(nodeID NodeID, blockIDs []BlockID) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	for _, blockID := range blockIDs {
		self.deletionIntents.Done(nodeID, blockID)
		if self.blocks[blockID] != nil {
			delete(self.blocks[blockID], nodeID)
		}
		if self.dataNodesBlocks[nodeID] != nil {
			delete(self.dataNodesBlocks[nodeID], blockID)
		}
	}
}

func (self *MetaDataNodeState) GetBlock(blockID BlockID) []string {
	self.mutex.RLock()
	defer self.mutex.RUnlock()

	var addrs []string
	for nodeID, _ := range self.blocks[blockID] {
		addrs = append(addrs, self.dataNodes[nodeID])
	}

	return addrs
}

func (self *MetaDataNodeState) RegisterDataNode(addr string, blocks []BlockID) NodeID {
	name := strings.Replace(namesgenerator.GetRandomName(0), "_", "-", -1)
	nodeID := NodeID(name)
	self.HasBlocks(nodeID, blocks)

	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.dataNodes[nodeID] = addr
	self.dataNodesUtilization[nodeID] = len(blocks)
	self.dataNodesLastSeen[nodeID] = time.Now()

	return nodeID
}

func (self *MetaDataNodeState) HeartbeatFrom(nodeID NodeID, utilization int) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if len(self.dataNodes[nodeID]) > 0 {
		self.dataNodesLastSeen[nodeID] = time.Now()
		self.dataNodesUtilization[nodeID] = utilization
		return true
	}
	return false
}

func (self *MetaDataNodeState) Utilization(n NodeID) int {
	return self.dataNodesUtilization[n] + self.replicationIntents.Count(n) - self.deletionIntents.Count(n)
}

// This is not concurrency safe
func (self *MetaDataNodeState) LeastUsedNodes() []NodeID {
	var nodes []NodeID
	for nodeID, _ := range self.dataNodes {
		nodes = append(nodes, nodeID)
	}

	sort.Sort(ByRandom(nodes))
	sort.Stable(ByFunc(self.Utilization, nodes))

	return nodes
}
func (self *MetaDataNodeState) MostUsedNodes() []NodeID {
	var nodes []NodeID
	for nodeID, _ := range self.dataNodes {
		nodes = append(nodes, nodeID)
	}

	sort.Sort(ByRandom(nodes))
	sort.Stable(sort.Reverse(ByFunc(self.Utilization, nodes)))

	return nodes
}

func (self *MetaDataNodeState) CommitBlob(name string, blocks []BlockID) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	for _, b := range blocks {
		self.store.Append(name, string(b))
	}
}
