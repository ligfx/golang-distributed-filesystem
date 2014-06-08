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
	BlockIDs []string
}