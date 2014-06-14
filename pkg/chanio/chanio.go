package chanio

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

type Network struct {
	lock sync.Mutex
	listeners map[string]*ChanListener
	sockID uint64
}

const networkName = "[::chanio]"

func NewNetwork() *Network {
	return &Network{sync.Mutex{}, map[string]*ChanListener{}, 1}
}

func (self *Network) Listen() net.Listener {
	self.lock.Lock()
	defer self.lock.Unlock()
	addr := fmt.Sprintf("%s:%d", networkName, self.sockID)
	self.sockID++
	c := make(chan ChanConn)
	listener := &ChanListener{self, addr, c}
	self.listeners[addr] = listener
	return listener
}

func (self *Network) Dial(addr string) (net.Conn, error) {
	self.lock.Lock()
	defer self.lock.Unlock()
	listener, present := self.listeners[addr]
	if ! present {
		return nil, errors.New("Address not found")
	}
	clientAddr := fmt.Sprintf("%s:%d", networkName, self.sockID)
	self.sockID++
	clientToServer := make(chan []byte)
	ServerToClient := make(chan []byte)
	client := ChanConn{ServerToClient,
		clientToServer,
		bytes.Buffer{},
		ChanAddr{clientAddr},
		ChanAddr{addr}}
	server := ChanConn{clientToServer,
		ServerToClient,
		bytes.Buffer{},
		ChanAddr{addr},
		ChanAddr{clientAddr}}
	go func() {
		listener.incoming <- server
	}()
	return client, nil
}

type ChanListener struct {
	network *Network
	addr string
	incoming chan ChanConn
}

type ChanConn struct {
	read <- chan []byte
	write chan []byte
	readBuf bytes.Buffer
	localAddr ChanAddr
	remoteAddr ChanAddr
}

type ChanAddr struct {
	addr string
}

func (self ChanAddr) Network() string {
	return networkName
}

func (self ChanAddr) String() string {
	return self.addr
}

func (self *ChanListener) Accept() (net.Conn, error) {
	return <- self.incoming, nil
}

func (self *ChanListener) Close() error {
	self.network.lock.Lock()
	defer self.network.lock.Unlock()
	delete(self.network.listeners, self.addr)
	return nil
}

func (self *ChanListener) Addr() net.Addr {
	return &ChanAddr{self.addr}
}

func (self ChanConn) Read(b []byte) (int, error) {
	if self.readBuf.Len() > 0 {
		return self.readBuf.Read(b)
	}

	bytes := <- self.read
	n := copy(b, bytes)
	if n > len(b) {
		self.readBuf.Write(bytes[n:])
	}
	return n, nil
}

func (self ChanConn) Write(b []byte) (int, error) {
	self.write <- b
	return len(b), nil
}

func (self ChanConn) Close() error {
	close(self.write)
	return nil
}

func (self ChanConn) LocalAddr() net.Addr {
	return self.localAddr
}

func (self ChanConn) RemoteAddr() net.Addr {
	return self.remoteAddr
}

func (self ChanConn) SetDeadline(t time.Time) error {
	panic("Not implemented")
}

func (self ChanConn) SetReadDeadline(t time.Time) error {
	panic("Not implemented")
}

func (self ChanConn) SetWriteDeadline(t time.Time) error {
	panic("Not implemented")
}