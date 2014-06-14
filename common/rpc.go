package common

import (
	"io"
	"net/rpc"
	"net/rpc/jsonrpc"
)

type RPCServer struct {
	codec rpc.ServerCodec
	lastServiceMethod string
	lastSeq uint64
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

/*

	serverListen, err := net.Listen("tcp", ":7070")
	if err != nil {
		log.Fatalln(err)
	}

	clientSock, err := net.Dial("tcp", ":7070")
	if err != nil {
		log.Fatalln(err)
	}
	client := jsonrpc.NewClientCodec(clientSock)

	var r rpc.Request
	r.ServiceMethod = "Test"
	r.Seq = 1
	if err := client.WriteRequest(&r, "hi there"); err != nil {
		log.Fatalln(err)
	}

	sock, _ := serverListen.Accept()
	server := jsonrpc.NewServerCodec(sock)

	var r1 rpc.Request
	if err := server.ReadRequestHeader(&r1); err != nil {
		log.Fatalln(err)
	}

	fmt.Println(r1.ServiceMethod)
	var s string
	server.ReadRequestBody(&s)
	fmt.Println(s)

	var resp rpc.Response
	resp.ServiceMethod = r1.ServiceMethod
	resp.Seq = r1.Seq
	resp.Error = ""
	server.WriteResponse(&resp, "okay")

	var r2 rpc.Response
	client.ReadResponseHeader(&r2)
	var s2 string
	client.ReadResponseBody(&s2)
	fmt.Println(s2)
	os.Exit(0)

*/