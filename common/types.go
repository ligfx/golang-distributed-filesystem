// Network protocol and other communications issues.
package common

import (
	"net"
)

type BlockID string
type NodeID string

type ForwardBlock struct {
	BlockID BlockID
	Nodes   []string
	Size    int64
}

type RegistrationMsg struct {
	Addr   string
	Blocks []BlockID
}

type HeartbeatMsg struct {
	NodeID     NodeID
	SpaceUsed  int
	NewBlocks  []BlockID
	DeadBlocks []BlockID
}

type HeartbeatResponse struct {
	NeedToRegister   bool
	InvalidateBlocks []BlockID
	ToReplicate      []ForwardBlock
}

//

type NetworkAdapter interface {
	Dial(string) (net.Conn, error)
}

type TCPNetwork struct {}
func (*TCPNetwork) Dial(addr string) (net.Conn, error) {
	return net.Dial("tcp", addr)
}