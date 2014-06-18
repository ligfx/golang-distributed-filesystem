package metadatanode

import (
	"log"
	"sort"
	"time"

	. "github.com/michaelmaltese/golang-distributed-filesystem/common"
)

func (self *MetaDataNodeState) Monitor() {
	for {
		log.Println("Monitor checking system..")
		// This sucks. Probably could do a separate lock for DataNodes and file stuff
		self.mutex.Lock()
		for id, lastSeen := range self.dataNodesLastSeen {
			if time.Since(lastSeen) > 10*time.Second {
				log.Println("Forgetting absent node:", id)
				delete(self.dataNodesLastSeen, id)
				delete(self.dataNodes, id)
				delete(self.dataNodesUtilization, id)
				for block, _ := range self.dataNodesBlocks[id] {
					delete(self.blocks[block], id)
				}
				delete(self.dataNodesBlocks, id)
			}
		}

		for blockID, nodes := range self.blocks {
			switch {
			default:
				continue

			case self.replicationIntents.InProgress(blockID):
				continue

			case self.deletionIntents.InProgress(blockID):
				continue

			case len(nodes) > self.ReplicationFactor:
				log.Println("Block '" + blockID + "' is over-replicated")
				var deleteFrom []NodeID
				nodesByUtilization := self.MostUsedNodes()
				for _, nodeID := range nodesByUtilization {
					if len(nodes)-len(deleteFrom) <= self.ReplicationFactor {
						break
					}
					if nodes[nodeID] {
						deleteFrom = append(deleteFrom, nodeID)
					}
				}
				log.Printf("Deleting from: %v", deleteFrom)
				self.deletionIntents.Add(blockID, deleteFrom)

			case len(nodes) < self.ReplicationFactor:
				log.Println("Block '" + blockID + "' is under-replicated!")
				var forwardTo []NodeID
				nodesByUtilization := self.LeastUsedNodes()
				for _, nodeID := range nodesByUtilization {
					if len(forwardTo)+len(nodes) >= self.ReplicationFactor {
						break
					}
					if !nodes[nodeID] {
						forwardTo = append(forwardTo, nodeID)
					}
				}
				log.Printf("Replicating to: %v", forwardTo)
				var availableFrom []NodeID
				for n, _ := range nodes {
					availableFrom = append(availableFrom, n)
				}
				self.replicationIntents.Add(blockID, availableFrom, forwardTo)
			}
		}

		if len(self.dataNodes) != 0 {
			totalUtilization := 0
			for _, utilization := range self.dataNodesUtilization {
				totalUtilization += utilization
			}
			avgUtilization := totalUtilization / len(self.dataNodes)

			var lessThanAverage []NodeID
			var moreThanAverage []NodeID
			for node, utilization := range self.dataNodesUtilization {
				switch {
				case utilization < avgUtilization:
					lessThanAverage = append(lessThanAverage, node)
				case utilization > avgUtilization:
					moreThanAverage = append(moreThanAverage, node)
				}
			}

			moveIntents := map[NodeID]int{}
			for len(lessThanAverage) != 0 && len(moreThanAverage) != 0 {
				sort.Sort(ByRandom(lessThanAverage))
				sort.Stable(ByFunc(self.Utilization, lessThanAverage))
				sort.Sort(ByRandom(moreThanAverage))
				sort.Stable(sort.Reverse(ByFunc(self.Utilization, moreThanAverage)))

			Nodes:
				for _, lessNode := range lessThanAverage {
				Blocks:
					for block, _ := range self.dataNodesBlocks[moreThanAverage[0]] {
						for existingBlock, _ := range self.dataNodesBlocks[lessNode] {
							if block == existingBlock {
								continue Blocks
							}
						}

						switch {
						case self.replicationIntents.InProgress(block):
							continue Blocks

						case self.deletionIntents.InProgress(block):
							continue Blocks

						default:
							var nodes []NodeID
							for n, _ := range self.blocks[block] {
								nodes = append(nodes, n)
							}
							log.Println("Move a block from", moreThanAverage[0], "to", lessNode)
							self.replicationIntents.Add(block, nodes, []NodeID{lessNode})
							if self.Utilization(lessThanAverage[0]) >= avgUtilization {
								lessThanAverage = lessThanAverage[1:]
							}
							break Nodes
						}
					}
				}

				// Prevent infinite loop
				moveIntents[moreThanAverage[0]]++
				self.dataNodesUtilization[moreThanAverage[0]]--
				if self.Utilization(moreThanAverage[0]) <= avgUtilization {
					moreThanAverage = moreThanAverage[1:]
				}
			}

			// So I don't have to write another sorter
			for node, offset := range moveIntents {
				self.dataNodesUtilization[node] += offset
			}
		}

		self.mutex.Unlock()
		time.Sleep(3 * time.Second)
	}
}
