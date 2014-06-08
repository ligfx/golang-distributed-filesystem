package metadatanode

import (
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/nu7hatch/gouuid"
)

type MetaDataNodeState struct {
	mutex sync.Mutex
	store *DB
	dataNodes map[string]string
	dataNodesLastSeen map[string]time.Time
	dataNodesUtilization map[string]int
	blocks map[string]map[string]bool
	dataNodesBlocks map[string][]string
	ReplicationFactor int
}

func NewMetaDataNodeState() *MetaDataNodeState {
	var self MetaDataNodeState
	db, err := OpenDB("metadata.db")
	log.Println("Persistent storage at", "metadata.db")
	if err != nil {
		log.Fatalln("Metadata store error:", err)
	}
	self.store = db
	self.dataNodesLastSeen = map[string]time.Time{}
	self.dataNodes = map[string]string{}
	self.dataNodesUtilization = map[string]int{}
	self.blocks = map[string]map[string]bool{}
	self.dataNodesBlocks = map[string][]string{}
	return &self
}

func (self *MetaDataNodeState) GenerateBlobId() string {
	u4, err := uuid.NewV4()
	if err != nil {
		log.Fatal(err)
	}
	return u4.String()
}

func (self *MetaDataNodeState) GenerateBlockId() string {
	u4, err := uuid.NewV4()
	if err != nil {
		log.Fatalln(err)
	}
	return u4.String()
}

func (self *MetaDataNodeState) GetBlob(blobID string) []string {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	blocks, err := self.store.Get(blobID)
	if err != nil {
		log.Fatalln(err)
	}
	return blocks
}

func (self *MetaDataNodeState) HasBlock(nodeID string, blockID string) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if self.blocks[blockID] == nil {
		self.blocks[blockID] = map[string]bool{}
	}
	self.blocks[blockID][nodeID] = true
	self.dataNodesBlocks[nodeID] = append(self.dataNodesBlocks[nodeID], blockID)
}

func (self *MetaDataNodeState) GetBlock(blockID string) []string {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	var addrs []string
	for nodeID, _ := range self.blocks[blockID] {
		addrs = append(addrs, self.dataNodes[nodeID])
	}

	return addrs
}

func (self *MetaDataNodeState) RegisterDataNode(addr string) string {
	u4, err := uuid.NewV4()
	if err != nil {
		log.Fatal(err)
	}
	nodeID := u4.String()

	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.dataNodes[nodeID] = addr
	self.dataNodesUtilization[nodeID] = 0
	self.dataNodesLastSeen[nodeID] = time.Now()
	return nodeID
}

func (self *MetaDataNodeState) HeartbeatFrom(nodeID string, utilization int) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.dataNodesLastSeen[nodeID] = time.Now()
	self.dataNodesUtilization[nodeID] = utilization
	return len(self.dataNodes[nodeID]) > 0
}

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

type ByUtilization []string
func (s ByUtilization) Len() int {
    return len(s)
}
func (s ByUtilization) Swap(i, j int) {
    s[i], s[j] = s[j], s[i]
}
func (s ByUtilization) Less(i, j int) bool {
    return State.dataNodesUtilization[s[i]] < State.dataNodesUtilization[s[j]]
}

func (self *MetaDataNodeState) GetDataNodes() []string {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	var nodes []string
	for nodeID, _ := range self.dataNodes {
		nodes = append(nodes, nodeID)
	}

	sort.Sort(ByRandom(nodes))
	sort.Stable(ByUtilization(nodes))
	
	var forwardTo []string
	if len(nodes) < self.ReplicationFactor {
		forwardTo = nodes
	} else {
		forwardTo = nodes[0:self.ReplicationFactor]
	}

	var addrs []string
	for _, nodeID := range forwardTo {
		self.dataNodesUtilization[nodeID] += 1
		addrs = append(addrs, self.dataNodes[nodeID])
	}

	return addrs
}

func (self *MetaDataNodeState) CommitBlob(name string, blocks []string) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	for _, b := range blocks {
		self.store.Append(name, b)
	}
}


func monitor() {
	for {
		log.Println("Monitor checking system..")
		// This sucks. Probably could do a separate lock for DataNodes and file stuff
		State.mutex.Lock()
		for id, lastSeen := range State.dataNodesLastSeen {
			if time.Since(lastSeen) > 20 * time.Second {
				log.Println("Forgetting absent node:", id)
				delete(State.dataNodesLastSeen, id)
				delete(State.dataNodes, id)
				delete(State.dataNodesUtilization, id)
				for _, block := range State.dataNodesBlocks[id] {
					delete(State.blocks[block], id)
				}
				delete(State.dataNodesBlocks, id)
			}
		}
		State.mutex.Unlock()

		time.Sleep(2 * time.Second)
	}
}