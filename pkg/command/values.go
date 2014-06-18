package command

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
)

type listenerFlag struct {
	port int
}

func ListenerFlag(flags Flags, name string, value int, usage string) *listenerFlag {
	self := &listenerFlag{port: value}
	flags.Var(self, name, usage)
	return self
}
func (self *listenerFlag) String() string {
	return strconv.Itoa(self.port)
}
func (self *listenerFlag) Set(s string) error {
	port, err := strconv.Atoi(s)
	self.port = port
	if err != nil {
		return err
	}
	return nil
}
func (self *listenerFlag) Get() net.Listener {
	listener, err := net.Listen("tcp", ":"+self.String())
	if err != nil {
		log.Fatalln(err)
	}
	return listener
}

type fileFlag struct {
	name     string
	filename string
	set      bool
}

func FileFlag(flags Flags, name string, usage string) *fileFlag {
	self := &fileFlag{name, "", false}
	flags.Var(self, name, usage)
	return self
}
func (self *fileFlag) String() string {
	return ""
}
func (self *fileFlag) Set(s string) error {
	self.filename = s
	self.set = true
	return nil
}
func (self *fileFlag) Get() *os.File {
	if !self.set {
		fmt.Println("flag must be provided:", "-"+self.name)
		fmt.Println("run with command 'help' for usage information")
		os.Exit(2)
	}
	file, err := os.Open(self.filename)
	if err != nil {
		log.Fatal(err)
	}
	return file
}
