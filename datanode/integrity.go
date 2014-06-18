package datanode

import (
	"log"
	"time"

	. "github.com/michaelmaltese/golang-distributed-filesystem/common"
)

func (self *DataNodeState) IntegrityChecker() {
	for {
		time.Sleep(5 * time.Second)
		log.Println("Checking block integrity...")

		blocks := self.Manager.GetExisting()
		for _, b := range blocks {
			if err := self.Manager.LockRead(b); err != nil {
				// Being uploaded or deleted
				// May or may not actually exist now/in the future
				// Does not imply it actually exists!
				continue
			}
			storedChecksum, err := self.Store.ReadChecksum(b)
			if err != nil {
				go self.RemoveBlock(BlockID(b))
			}
			localChecksum, err := self.Store.LocalChecksum(b)
			if err != nil {
				go self.RemoveBlock(BlockID(b))
			}

			if storedChecksum != localChecksum {
				log.Println("Checksum doesn't match block:", b)
				log.Println(storedChecksum, localChecksum)
				go self.RemoveBlock(BlockID(b))
			}
			self.Manager.UnlockRead(b)
		}
	}
}
