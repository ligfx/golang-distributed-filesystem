package datanode

import (
	"errors"
	"sync"

	. "github.com/michaelmaltese/golang-distributed-filesystem/common"
)

// Linearizes access to blocks
type BlockIntents struct {
	using      map[BlockID]*sync.WaitGroup
	receiving  map[BlockID]bool
	willDelete map[BlockID]bool
	exists     map[BlockID]bool

	lock sync.Mutex
}

// What's up with this?
// - If a block is being received, we shouldn't check it yet,
//   and we shouldn't read it yet. But, maybe we want to queue
//   that up.
// - If a block is being received or read, obviously don't
//   delete it. Doesn't need to be queued up, we're okay with
//   dropping delete commands
// - If a block is being deleted, we shouldn't read it, and we
//   can't queue that up.
// - If a block is being read, don't delete it!

// TODO:

func (self *BlockIntents) LockReceive(block BlockID) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.receiving[block] = true
}

func (self *BlockIntents) CommitReceive(block BlockID) {
	self.lock.Lock()
	defer self.lock.Unlock()

	self.exists[block] = true
	self.receiving[block] = false
}

func (self *BlockIntents) AbortReceive(block BlockID) {
	self.lock.Lock()
	defer self.lock.Unlock()

	delete(self.willDelete, block)
	delete(self.receiving, block)
	delete(self.using, block)
	delete(self.exists, block)
}

func (self *BlockIntents) LockRead(block BlockID) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	if !self.exists[block] || self.receiving[block] || self.willDelete[block] {
		return errors.New("Can't lock")
	}

	if self.using[block] == nil {
		self.using[block] = &sync.WaitGroup{}
	}
	self.using[block].Add(1)
	return nil
}

func (self *BlockIntents) UnlockRead(block BlockID) {
	self.lock.Lock()
	defer self.lock.Unlock()

	self.using[block].Done()
}

func (self *BlockIntents) LockDelete(block BlockID) {
	self.lock.Lock()
	self.willDelete[block] = true
	if self.using[block] == nil {
		self.using[block] = &sync.WaitGroup{}
	}
	m := self.using[block]
	self.lock.Unlock()
	m.Wait()
	return
}

func (self *BlockIntents) CommitDelete(block BlockID) {
	self.lock.Lock()
	defer self.lock.Unlock()

	delete(self.willDelete, block)
	delete(self.receiving, block)
	delete(self.using, block)
	delete(self.exists, block)
}
