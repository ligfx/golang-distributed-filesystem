// Network protocol and other communications issues.
package comm

type ForwardBlock struct {
	BlockId string
	Nodes []string
	Size int64
}

type HeartbeatMsg struct {
	NodeID string
	SpaceUsed int
	NewBlocks []string
	DeadBlocks []string
}

type HeartbeatResponse struct {
	NeedToRegister bool
	InvalidateBlocks []string
}