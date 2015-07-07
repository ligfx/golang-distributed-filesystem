package metadatanode

import (
	"net"
)

type Config struct {
	ClientListener    net.Listener
	ClusterListener   net.Listener
	ReplicationFactor int
	DatabaseFile      string
}
