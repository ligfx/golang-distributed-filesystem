package main

import (
	"fmt"
	"os"
	"path"
)

type Command struct {
	Name string
	Description string
	Function func([]string)bool
}

func usage(commands []Command) {
	fmt.Println("USAGE:", path.Base(os.Args[0]), "CMD", "[OPTS...]", "[ARGS...]")
	fmt.Println("COMMANDS:")
	for _, c := range commands {
		fmt.Println("\t", c.Description)
	}
	os.Exit(1)
}

func CommandRun(commands []Command) {
	if len(os.Args) < 2 {
		usage(commands)
	}

	for _, c := range commands {
		if c.Name == os.Args[1] {
			ok := c.Function(os.Args[2:])
			if !ok {
				usage(commands)
			}
			return
		}
	}

	usage(commands)
}