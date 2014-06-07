package metadatanode

import (
	"errors"
	"log"
	"net"
	// "github.com/michaelmaltese/golang-distributed-filesystem/comm"
)

type PeerSession struct {
	state SessionState
	server *MetaDataNodeState
	remoteAddr string
}
func (self *PeerSession) Register (port *string, _ *int) error {
	if self.state != Start {
		return errors.New("Not allowed in current session state")
	}
	host, _, err := net.SplitHostPort(self.remoteAddr)
	if err != nil {
		log.Fatalln("SplitHostPort error:", err)
	}
	addr := net.JoinHostPort(host, *port)
	self.server.RegisterDataNode(addr)
	log.Println("DataNode registered at " + addr)
	return nil
}