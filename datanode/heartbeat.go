package datanode

import (
	"log"
	"time"

	. "github.com/michaelmaltese/golang-distributed-filesystem/common"
)

func (self *DataNodeState) Heartbeat() {
	for {
		tick(self)
		time.Sleep(self.heartbeatInterval)
	}
}

func tick(dn *DataNodeState) {
	client, err := NewRPCClient(dn.LeaderAddress)
	if err != nil {
		log.Println("Couldn't connect to leader at", dn.LeaderAddress)
		dn.NodeID = ""
		return
	}
	defer client.Close()

	log.Println("Heartbeat...")
	if len(dn.NodeID) == 0 {
		log.Println("Re-reading blocklist")
		blocks, err := dn.Store.ReadBlockList()
		if err != nil {
			log.Fatalln("Getting blocklist:", err)
		}
		for _, b := range blocks {
			// Seems hacky
			// Should be something like, ReinitBlocks
			dn.Manager.exists[b] = true
		}
		err = client.Call("Register", &RegistrationMsg{dn.Addr, blocks}, &dn.NodeID)
		if err != nil {
			log.Println("Registration error:", err)
			return
		}
		log.Println("Registered with ID:", dn.NodeID)
		return
	}

	spaceUsed := dn.Manager.GetUtilization()
	newBlocks := dn.newBlocks.StartRead()
	defer newBlocks.AbortIfNotCommitted()
	deadBlocks := dn.newBlocks.StartRead()
	defer deadBlocks.AbortIfNotCommitted()

	var resp HeartbeatResponse
	err = client.Call("Heartbeat",
		HeartbeatMsg{dn.NodeID, spaceUsed, newBlocks.Get(), deadBlocks.Get()},
		&resp)
	if err != nil {
		log.Println("Heartbeat error:", err)
		return
	}
	if resp.NeedToRegister {
		log.Println("Re-registering with leader...")
		dn.NodeID = ""
		return
	} else {
		// We're good! Leader accepted our changes
		newBlocks.CommitRead()
		deadBlocks.CommitRead()

		for _, blockID := range resp.InvalidateBlocks {
			dn.RemoveBlock(blockID)
		}
		go func() {
			for _, fwd := range resp.ToReplicate {
				log.Println("Will replicate '"+string(fwd.BlockID)+"' to", fwd.Nodes)
				dn.forwardingBlocks <- fwd
			}
		}()
	}
}
