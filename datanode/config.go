// Stores blocks for the cluster.
package datanode

import (
	"net"
	"time"
)

type Config struct {
	DataDir string
	Debug bool
	Listener net.Listener
	HeartbeatInterval time.Duration
}