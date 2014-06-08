package metadatanode

import (
	"log"
	"errors"
	. "github.com/michaelmaltese/golang-distributed-filesystem/comm"
)

type ClientSession struct {
	state SessionState
	server *MetaDataNodeState
	blob_id string
	blocks []BlockID
	remoteAddr string
}
func (self *ClientSession) CreateBlob(_ *int, ret *string) error {
	if self.state != Start {
		return errors.New("Not allowed in current session state")
	}

	self.blob_id = self.server.GenerateBlobId()
	self.blocks = []BlockID{}
	*ret = self.blob_id
	self.state = Creating
	return nil
}
func (self *ClientSession) Append(_ *int, ret *ForwardBlock) error {
	if self.state != Creating {
		return errors.New("Not allowed in current session state")
	}

	*ret = self.server.GenerateBlock(self.blob_id)
	self.blocks = append(self.blocks, ret.BlockID)
	return nil
}

func (self *ClientSession) Commit(_ *int, _ *int) error {
	if self.state != Creating {
		return errors.New("Not allowed in current session state")
	}

	self.server.CommitBlob(self.blob_id, self.blocks)
	log.Print("Committed blob '" + self.blob_id + "' for " + self.remoteAddr)
	self.blob_id = ""
	self.blocks = nil
	self.state = Start
	return nil
}

func (self *ClientSession) GetBlob(blobId *string, blocks *[]BlockID) error {
	if self.state != Start {
		return errors.New("Not allowed in current session state")
	}

	*blocks = self.server.GetBlob(*blobId)
	return nil
}

func (self *ClientSession) GetBlock(blockId *BlockID, nodes *[]string) error {
	if self.state != Start {
		return errors.New("Not allowed in current session state")
	}

	*nodes = self.server.GetBlock(*blockId)
	return nil
}