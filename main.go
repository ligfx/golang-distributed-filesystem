/*

	Need to share the communication stuff between the binaries.
	This is also a good place to define specific messages.

*/

package main

func main() {
	CommandRun([]Command{
		{Name: "upload", Description: "upload LOCAL", Function: Upload},
	})
}