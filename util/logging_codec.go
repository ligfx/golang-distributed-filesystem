package util

import (
	"fmt"
	"log"
	"net/rpc"
	"reflect"
)

type loggingServerCodec struct {
	remote string
	rpc.ServerCodec
	lastMethod string
}

func (self *loggingServerCodec) ReadRequestHeader(req *rpc.Request) error {
	err := self.ServerCodec.ReadRequestHeader(req)
	if err == nil {
		self.lastMethod = req.ServiceMethod
	}
	return err
}

func (self *loggingServerCodec) ReadRequestBody(p interface{}) error {
	err := self.ServerCodec.ReadRequestBody(p)
	v := reflect.Indirect(reflect.ValueOf(p))
	switch {
	case err != nil:
		log.Println(self.remote, "->", self.lastMethod, err)
	case v.IsValid():
		log.Println(self.remote, "->", self.lastMethod, fmt.Sprintf("%+v", v.Interface()))
	default:
		
	}
	return err
}

func (self *loggingServerCodec) WriteResponse(header *rpc.Response, body interface{}) error {
	v := reflect.Indirect(reflect.ValueOf(body))
	if v.IsValid() {
		log.Println(self.remote, "<-", fmt.Sprintf("%+v", v.Interface()))
	} else {
		log.Println(self.remote, "<-", "ok")
	}
	return self.ServerCodec.WriteResponse(header, body)
}

func LoggingServerCodec(remote string, parent rpc.ServerCodec) rpc.ServerCodec {
	return &loggingServerCodec{remote, parent, ""}
}

type loggingClientCodec struct {
	remote string
	rpc.ClientCodec
}

type clientRequest struct {
	Method string         `json:"method"`
    Params []interface{} `json:"params"`
}

func (self *loggingClientCodec) ReadResponseBody(p interface{}) error {
	err := self.ClientCodec.ReadResponseBody(p)
	v := reflect.Indirect(reflect.ValueOf(p))
	switch {
	case err != nil:
		log.Println(self.remote, "->", err)
	case v.IsValid():
		log.Println(self.remote, "->", fmt.Sprintf("%+v", v.Interface()))
	default:
		log.Println(self.remote, "->", "ok")
	}
	return err
}

func (self *loggingClientCodec) WriteRequest(r *rpc.Request, param interface{}) error {
	v := reflect.Indirect(reflect.ValueOf(param))
	if v.IsValid() {
		log.Println(self.remote, "<-", r.ServiceMethod, fmt.Sprintf("%+v", v.Interface()))
	} else {
		log.Println(self.remote, "<-", r.ServiceMethod)
	}
	return self.ClientCodec.WriteRequest(r, param)
}

func LoggingClientCodec(remote string, parent rpc.ClientCodec) rpc.ClientCodec {
	return &loggingClientCodec{remote, parent}
}