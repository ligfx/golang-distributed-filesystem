// Stores blocks for the cluster.
package datanode

import (
	"time"
)

type Config struct {
	DataDir string
	Debug bool
	Port string
	HeartbeatInterval time.Duration
}