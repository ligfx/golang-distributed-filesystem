package metadatanode

import (
	"log"
	"time"

	. "golang-distributed-filesystem/common"
)

type replicationIntent struct {
	startedAt     time.Time
	sentCommand   bool
	block         BlockID
	availableFrom []NodeID
	forwardTo     []NodeID
}

type ReplicationIntents struct {
	intents []*replicationIntent
}

func (self *ReplicationIntents) Add(block BlockID, from []NodeID, to []NodeID) {
	if self.InProgress(block) {
		log.Fatalln("Already replicating block '" + string(block))
	}
	self.intents = append(self.intents, &replicationIntent{time.Now(), false, block, from, to})
}

func (self *ReplicationIntents) Count(node NodeID) int {
	count := 0
	for _, intent := range self.intents {
		for _, n := range intent.forwardTo {
			if n == node {
				if time.Since(intent.startedAt) < 20*time.Second {
					count++
				}
			}
		}
	}
	return count
}

func (self *ReplicationIntents) Get(node NodeID) map[BlockID][]NodeID {
	actions := map[BlockID][]NodeID{}
	for _, intent := range self.intents {
		if intent.sentCommand {
			continue
		}
		for _, n := range intent.availableFrom {
			if n == node {
				actions[intent.block] = intent.forwardTo
				intent.sentCommand = true
				intent.startedAt = time.Now()
			}
		}
	}
	return actions
}

func (self *ReplicationIntents) Done(node NodeID, block BlockID) {
	for i, intent := range self.intents {
		if intent.block != block {
			continue
		}
		for j, n := range intent.forwardTo {
			if n == node {
				intent.forwardTo = append(intent.forwardTo[:j], intent.forwardTo[j+1:]...)
			}
		}
		if len(intent.forwardTo) == 0 {
			self.intents = append(self.intents[:i], self.intents[i+1:]...)
		}
	}
}

func (self *ReplicationIntents) InProgress(block BlockID) bool {
	for i, intent := range self.intents {
		if intent.block == block {
			if time.Since(intent.startedAt) < 20*time.Second {
				return true
			}
			self.intents = append(self.intents[:i], self.intents[i+1:]...)
		}
	}
	return false
}

type deletionIntent struct {
	startedAt   time.Time
	sentCommand bool
	block       BlockID
	node        NodeID
}

type DeletionIntents struct {
	intents []*deletionIntent
}

func (self *DeletionIntents) Add(block BlockID, from []NodeID) {
	for _, node := range from {
		if self.InProgress(block) {
			log.Fatalln("Already deleting block '" + string(block) + "' from '" + string(node) + "'")
		}
		self.intents = append(self.intents, &deletionIntent{time.Now(), false, block, node})
	}
}

func (self *DeletionIntents) Count(node NodeID) int {
	var deletions []BlockID
	for _, intent := range self.intents {
		if intent.node == node {
			if time.Since(intent.startedAt) < 20*time.Second {
				deletions = append(deletions, intent.block)
			}
		}
	}
	return len(deletions)
}

func (self *DeletionIntents) Get(node NodeID) []BlockID {
	var deletions []BlockID
	for _, intent := range self.intents {
		if intent.sentCommand {
			continue
		}
		if intent.node == node {
			deletions = append(deletions, intent.block)
			intent.sentCommand = true
			intent.startedAt = time.Now()
		}
	}
	return deletions
}

func (self *DeletionIntents) Done(node NodeID, block BlockID) {
	for i, intent := range self.intents {
		if intent.node == node && intent.block == block {
			self.intents = append(self.intents[:i], self.intents[i+1:]...)
		}
	}
}

func (self *DeletionIntents) InProgress(block BlockID) bool {
	for i, intent := range self.intents {
		if intent.block == block {
			if time.Since(intent.startedAt) < 20*time.Second {
				return true
			}
			self.intents = append(self.intents[:i], self.intents[i+1:]...)
		}
	}
	return false
}
