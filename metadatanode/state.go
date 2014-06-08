package metadatanode

import (
	"log"
	"sync"

	"github.com/nu7hatch/gouuid"
)

type MetaDataNodeState struct {
	mutex sync.Mutex
	store *DB
	dataNodes map[string]string
	blocks map[string][]string
}

func NewMetaDataNodeState() *MetaDataNodeState {
	var self MetaDataNodeState
	db, err := OpenDB("metadata.db")
	log.Println("Persistent storage at", "metadata.db")
	if err != nil {
		log.Fatalln("Metadata store error:", err)
	}
	self.store = db
	self.dataNodes = make(map[string]string)
	self.blocks = make(map[string][]string)
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

func (self *MetaDataNodeState) HasBlock(addr string, blockId string) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.blocks[blockId] = append(self.blocks[blockId], addr)
}

func (self *MetaDataNodeState) GetBlock(blockID string) []string {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	var addrs []string
	for _, nodeID := range self.blocks[blockID] {
		addrs = append(addrs, self.dataNodes[nodeID])
	}

	return addrs
}

func (self *MetaDataNodeState) RegisterDataNode(addr string) string {
	u4, err := uuid.NewV4()
	if err != nil {
		log.Fatal(err)
	}
	nodeId := u4.String()

	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.dataNodes[nodeId] = addr
	return nodeId
}

func (self *MetaDataNodeState) GetDataNodes() []string {
	// Is this lock necessary?
	self.mutex.Lock()
	defer self.mutex.Unlock()

	var addrs []string
	for _, addr := range self.dataNodes {
		addrs = append(addrs, addr)
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