package main

import (
	"bufio"
	"io"
	"strings"
)

type Decoder struct {
	reader *bufio.Reader
}

type Encoder struct {
	writer io.Writer
}

type EncodeDecoder struct {
	*Decoder
	*Encoder
}

func NewEncodeDecoder(rw io.ReadWriter) *EncodeDecoder {
	return &EncodeDecoder{&Decoder{bufio.NewReader(rw)}, &Encoder{rw}}
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