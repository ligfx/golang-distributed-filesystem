package metadatanode

import (
	"math/rand"
	"sort"

	. "github.com/michaelmaltese/golang-distributed-filesystem/common"
)

type ByRandom []NodeID

func (s ByRandom) Len() int {
	return len(s)
}
func (s ByRandom) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s ByRandom) Less(i, j int) bool {
	return rand.Intn(2) == 0 // 0 or 1
}

type byFunc struct {
	f    func(NodeID) int
	list []NodeID
}

func (self byFunc) Len() int {
	return len(self.list)
}
func (self byFunc) Swap(i, j int) {
	self.list[i], self.list[j] = self.list[j], self.list[i]
}
func (self byFunc) Less(i, j int) bool {
	return self.f(self.list[i]) < self.f(self.list[j])
}

func ByFunc(f func(NodeID) int, list []NodeID) sort.Interface {
	return byFunc{f, list}
}
