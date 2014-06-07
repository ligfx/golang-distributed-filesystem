package main

import (
	"fmt"
	"os"
	"path"
)

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
