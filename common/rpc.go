package common

import (
	"io"
	"log"

	"net/rpc"
	"net/rpc/jsonrpc"
)

type RPCClient struct {
}

func (self *RPCClient) Close() {
	panic("Not implemented")
}

func (self *RPCClient) Call(method string, in interface{}, out interface{}) error {
	panic("Not implemented")
}

func (self *RPCClient) Write(p []byte) (int, error) {
	panic("Not implemented")
}

func NewRPCClient(addr string) (*RPCClient, error) {
	panic("Not implemented")
}

type RPCServer struct {
	codec             rpc.ServerCodec
	lastServiceMethod string
	lastSeq           uint64
}

// TODO: Needs to support Debug
func NewRPCServer(sock io.ReadWriteCloser) *RPCServer {
	return &RPCServer{jsonrpc.NewServerCodec(sock), "", 0}
}

func (self *RPCServer) ReadHeader() (string, error) {
	var r rpc.Request
	if err := self.codec.ReadRequestHeader(&r); err != nil {
		return "", err
	}
	self.lastServiceMethod = r.ServiceMethod
	self.lastSeq = r.Seq
	return r.ServiceMethod, nil
}

func (self *RPCServer) ReadBody(obj interface{}) error {
	return self.codec.ReadRequestBody(obj)
}

func (self *RPCServer) Error(s string) error {
	var r rpc.Response
	r.ServiceMethod = self.lastServiceMethod
	r.Seq = self.lastSeq
	r.Error = s
	return self.codec.WriteResponse(&r, nil)
}

func (self *RPCServer) Unacceptable() error {
	log.Println("Unacceptable")
	self.ReadBody(nil)
	return self.Error("Method not accepted")
}

func (self *RPCServer) SendOkay() error {
	var r rpc.Response
	r.ServiceMethod = self.lastServiceMethod
	r.Seq = self.lastSeq
	r.Error = ""
	return self.codec.WriteResponse(&r, "OK")
}

func (self *RPCServer) Send(obj interface{}) error {
	var r rpc.Response
	r.ServiceMethod = self.lastServiceMethod
	r.Seq = self.lastSeq
	r.Error = ""
	return self.codec.WriteResponse(&r, obj)
}
