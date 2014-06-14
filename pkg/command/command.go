// Replace with https://github.com/codegangsta/cli and https://github.com/dotcloud/docker/pkg/mflag
package command

import (
	goflag "flag"
	"fmt"
	"os"
	"path"
	"strings"
	"time"
)

type flag struct {
	name string
	value string
	usage string
}

type command struct {
	name string
	description string
	flags []flag
	function func(Flags)
}

type Flags interface {
	BoolVar(*bool, string, bool, string)
	String(string, string, string) *string
	Duration(string, time.Duration, string) *time.Duration
	Parse()
	Int(string, int, string) *int
}

type AppConfig struct {
	whoami string
	help bool
	globalFlags []flag
	global func(Flags)
	commands []command
}

func App() *AppConfig {
	return new(AppConfig)
}

type flagDummy struct {
	list *[]flag
}

func (self *flagDummy) BoolVar(_ *bool, name string, value bool, usage string) {
	flag := flag{name, fmt.Sprintf("%+v", value), usage}
	*self.list = append(*self.list, flag)
}

func (self *flagDummy) Duration(name string, value time.Duration, usage string) *time.Duration {
	flag := flag{name, fmt.Sprintf("%+v", value), usage}
	*self.list = append(*self.list, flag)
	return nil
}

func (self *flagDummy) String(name string, value string, usage string) *string {
	flag := flag{name, fmt.Sprintf("%#v", value), usage}
	*self.list = append(*self.list, flag)
	return nil
}

func (self *flagDummy) Int(name string, value int, usage string) *int {
	flag := flag{name, fmt.Sprintf("%+v", value), usage}
	*self.list = append(*self.list, flag)
	return nil
}

type doneTracing struct {}
func (self *doneTracing) Error() string {
	return "Tracing flags"
}

func (self *flagDummy) Parse() {
	panic(doneTracing{})
}

func (self *AppConfig) Global(f func(Flags)) {
	if self.global != nil {
		panic("Already set")
	}
	self.global = f
	f(&flagDummy{&self.globalFlags})
	(&flagDummy{&self.globalFlags}).BoolVar(nil, "help", false, "")
}

func (self *AppConfig) Command(name string, description string, f func(Flags)) {
	var flags []flag
	defer func() {
		if r := recover(); r != nil {
			switch r.(type) {
			case doneTracing:
			default:
				panic(r)
			}
		}
		c := command{name, description, flags, f}
		self.commands = append(self.commands, c)
	}()

	f(&flagDummy{&flags})	
}

type flagSet struct {
	*goflag.FlagSet
}

func newFlagSet() *flagSet {
	return &flagSet{goflag.NewFlagSet("", goflag.ContinueOnError)}
}

type flagSetFailure struct {
	err error
}

func (self *flagSet) Parse() {
	if err := self.FlagSet.Parse(os.Args); err != nil {
		panic(flagSetFailure{err})
	}
}

func (self *AppConfig) Run() {
	self.whoami = path.Base(os.Args[0])
	endOfGlobalFlags := 0
	for _, a := range os.Args[1:] {
		if strings.HasPrefix(a, "-") {
			endOfGlobalFlags++
		} else {
			break
		}
	}
	commandArgs := os.Args[endOfGlobalFlags+1:]
	os.Args = os.Args[1:endOfGlobalFlags+1]
	set := newFlagSet()
	defer func(){
		if r := recover(); r != nil {
			switch r.(type) {
			case flagSetFailure:
				self.Usage()
			default:
				panic(r)
			}
		}
	}()
	self.global(set)
	var help bool
	set.BoolVar(&help, "help", false, "")
	set.Parse()
	if len(commandArgs) == 0 || help {
		self.Usage()
	}
	command := commandArgs[0]
	os.Args = commandArgs[1:]
	for _, c := range self.commands {
		if c.name == command {
			set := newFlagSet()
			c.function(set)
			os.Exit(0)
		}
	}
	self.Usage()
}

func (self *AppConfig) Usage() {
	fmt.Println("Usage:", self.whoami, "[global flags]", "command", "[flags]")
	fmt.Println("Global:")
	for _, f := range self.globalFlags {
		fmt.Printf("\t-%s=%v\n", f.name, f.value)
	}
	fmt.Println("Commands:")
	for _, c := range self.commands {
		fmt.Printf("\t%s: %s\n", c.name, c.description)
		for _, f := range c.flags {
			fmt.Printf("\t\t-%s=%s\n", f.name, f.value)
		}
	}

	os.Exit(2)
}

/*
type Command struct {
	Name        string
	Description string
	Function    func()
}

func usage(whoami string, commands []Command) {
	fmt.Println("Usage:", whoami, "CMD", "[OPTS...]", "[ARGS...]")
	fmt.Println("Commands:")
	for _, c := range commands {
		fmt.Println("\t", c.Description)
	}
	os.Exit(2)
}

func CommandRun(commands []Command) {
	whoami := path.Base(os.Args[0])
	if len(os.Args) > 1 {
		for _, c := range commands {
			if c.Name == os.Args[1] {
				os.Args = os.Args[1:]
				c.Function()
				return
			}
		}
	}

	usage(whoami, commands)
}
*/