/*

	Need to share the communication stuff between the binaries.
	This is also a good place to define specific messages.

*/

package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

func Usage() {
	fmt.Println("USAGE: client upload LOCAL REMOTE")
	os.Exit(1)
}

type Decoder struct {
	reader *bufio.Reader
}

type Encoder struct {
	writer io.Writer
}

type EncoderDecoder struct {
	*Decoder
	*Encoder
}

func NewEncoderDecoder(c net.Conn) *EncoderDecoder {
	return &EncoderDecoder{&Decoder{bufio.NewReader(c)}, &Encoder{c}}
}

func (d Decoder) Decode() (string, []string, error) {
	line, err := d.reader.ReadString('\n')
	if err != nil {
		return "", nil, err
	}

	_splits := strings.Split(strings.TrimSpace(line), " ")
	message_type := _splits[0]
	message := _splits[1:]

	return message_type, message, nil
}

func (e Encoder) Encode(args ...string) {
	// should check for errors...
	e.writer.Write([]byte(strings.Join(args, " ")))
	e.writer.Write([]byte("\n"))
}

func main() {
	if len(os.Args) < 2 {
		Usage()
	}

	command := os.Args[1]

	switch command {
	case "upload":
		if len(os.Args) != 4 {
			Usage()
		}
		// local := os.Args[2]
		remote := os.Args[3]

		conn, err := net.Dial("tcp", "localhost:5050")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		ed := NewEncoderDecoder(conn)

		ed.Encode("CREATE", remote)
		msg_type, _, err := ed.Decode()
		// check err

		if msg_type != "OK" {
			fmt.Printf("Received %#v", msg_type)
			os.Exit(1)
		}

		ed.Encode("APPEND", remote)
		msg_type, msg, err := ed.Decode()
		// check err
		fmt.Println(msg_type, strings.Join(msg, " "))

	default:
		Usage()
	}

}