// Stores blocks for the cluster.
package datanode

import (
	"net"
	"time"

	"github.com/michaelmaltese/golang-distributed-filesystem/common"
)

type Config struct {
	DataDir string
	Debug bool
	Listener net.Listener
	Network common.NetworkAdapter
	HeartbeatInterval time.Duration
	LeaderAddress string
}