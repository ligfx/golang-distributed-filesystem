package main

import (
	"bufio"
	"io"
	"strings"
)

type basicDecoder struct {
	reader *bufio.Reader
}

type basicEncoder struct {
	writer io.Writer
}

type basicEncodeDecoder struct {
	basicDecoder
	basicEncoder
}


func (d basicDecoder) Decode() ([]string, error) {
	line, err := d.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	msg := strings.Split(strings.TrimSpace(line), " ")

	return msg, nil
}

func (e basicEncoder) Encode(args ...string) {
	// should check for errors...
	e.writer.Write([]byte(strings.Join(args, " ") + "\n"))
}

type Encoder interface {
	Encode(...string)
}

type Decoder interface {
	Decode()([]string, error)
}

type EncodeDecoder interface {
	Decoder
	Encoder
}

func NewEncodeDecoder(r io.Reader, w io.Writer) EncodeDecoder {
	return &basicEncodeDecoder{basicDecoder{bufio.NewReader(r)}, basicEncoder{w}}
}