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
	Network NetworkAdapter
	HeartbeatInterval time.Duration
	LeaderAddress string
}

type NetworkAdapter interface {
	Dial(string) (net.Conn, error)
}

type TCPNetwork struct {}
func (*TCPNetwork) Dial(addr string) (net.Conn, error) {
	return net.Dial("tcp", addr)
}