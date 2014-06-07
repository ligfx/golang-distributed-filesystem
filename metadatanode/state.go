package metadatanode

import (
	"log"
	"sync"

	"github.com/nu7hatch/gouuid"
)

type MetaDataNodeState struct {
	mutex sync.Mutex
	dataNodes []string
	blobs map[string][]string
	blocks map[string][]string
}

func NewMetaDataNodeState() *MetaDataNodeState {
	var self MetaDataNodeState
	self.dataNodes = []string{}
	self.blobs = make(map[string][]string)
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
		log.Fatal(err)
	}
	return u4.String()
}

func (self *MetaDataNodeState) GetBlob(blob_id string) []string {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	log.Println(self.blobs[blob_id])
	return self.blobs[blob_id]
}

func (self *MetaDataNodeState) GetBlock(block_id string) []string {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return self.blocks[block_id]
}

func (self *MetaDataNodeState) RegisterDataNode(addr string) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.dataNodes = append(self.dataNodes, addr)
}

func (self *MetaDataNodeState) GetDataNodes() []string {
	// Is this lock necessary?
	self.mutex.Lock()
	defer self.mutex.Unlock()
	l := make([]string, len(self.dataNodes))
	copy(l, self.dataNodes)
	return l
}

func (self *MetaDataNodeState) CommitBlob(name string, blocks []string) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.blobs[name] = blocks
}