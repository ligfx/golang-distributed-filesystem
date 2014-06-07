package util

import (
	"log"
	"net/rpc"
)

type loggingServerCodec struct {
	remote string
	rpc.ServerCodec
}

func (self *loggingServerCodec) ReadRequestHeader(req *rpc.Request) error {
	err := self.ServerCodec.ReadRequestHeader(req)
	log.Println(self.remote, "->", req)
	return err
}

func (self *loggingServerCodec) ReadRequestBody(p interface{}) error {
	err := self.ServerCodec.ReadRequestBody(p)
	log.Println(self.remote, "->", p)
	return err
}

func (self *loggingServerCodec) WriteResponse(header *rpc.Response, body interface{}) error {
	log.Println(self.remote, "<-", header)
	log.Println(self.remote, "<-", body)
	return self.ServerCodec.WriteResponse(header, body)
}

func LoggingServerCodec(remote string, parent rpc.ServerCodec) rpc.ServerCodec {
	return &loggingServerCodec{remote, parent}
}

type loggingClientCodec struct {
	remote string
	rpc.ClientCodec
}

func (self *loggingClientCodec) ReadResponseHeader(req *rpc.Response) error {
	err := self.ClientCodec.ReadResponseHeader(req)
	log.Println(self.remote, "->", req)
	return err
}

func (self *loggingClientCodec) ReadResponseBody(p interface{}) error {
	err := self.ClientCodec.ReadResponseBody(p)
	log.Println(self.remote, "->", p)
	return err
}

func (self *loggingClientCodec) WriteRequest(header *rpc.Request, body interface{}) error {
	log.Println(self.remote, "<-", header)
	log.Println(self.remote, "<-", body)
	return self.ClientCodec.WriteRequest(header, body)
}

func LoggingClientCodec(remote string, parent rpc.ClientCodec) rpc.ClientCodec {
	return &loggingClientCodec{remote, parent}
}