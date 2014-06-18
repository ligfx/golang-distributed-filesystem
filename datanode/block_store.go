package datanode

import (
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path"

	. "github.com/michaelmaltese/golang-distributed-filesystem/common"
)

// Deals with filesystem
type BlockStore struct {
	DataDir string
}

func (self *BlockStore) BlockSize(block BlockID) (int64, error) {
	fileInfo, err := os.Stat(self.BlockFilename(block))
	if err != nil {
		return -1, err
	}
	return fileInfo.Size(), nil
}

func (self *BlockStore) LocalChecksum(block BlockID) (string, error) {
	file, err := self.OpenBlock(block)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := crc32.NewIEEE()
	if _, err = io.Copy(hash, file); err != nil {
		return "", err
	}
	return fmt.Sprint(hash.Sum32()), nil
}

func (self *BlockStore) OpenBlock(block BlockID) (io.ReadCloser, error) {
	return os.Open(self.BlockFilename(block))
}

type HashingWriter struct {
	main io.WriteCloser
	hash hash.Hash32
}

func NewHashingWriter(main io.WriteCloser, hash hash.Hash32) *HashingWriter {
	return &HashingWriter{main, hash}
}

func (self *HashingWriter) Write(p []byte) (int, error) {
	return io.MultiWriter(self.main, self.hash).Write(p)
}

func (self *HashingWriter) Close() error {
	return self.main.Close()
}

func (self *HashingWriter) Checksum() string {
	return fmt.Sprint(self.hash.Sum32())
}

// Something that you can get the hash from afterwards?
// A HashingWriterCloser ?
func (self *BlockStore) CreateBlock(block BlockID) (*HashingWriter, error) {
	file, err := os.Create(self.BlockFilename(block))
	if err != nil {
		return nil, err
	}

	hash := crc32.NewIEEE()
	return NewHashingWriter(file, hash), nil
}

func (self *BlockStore) ReadBlockList() ([]BlockID, error) {
	files, err := ioutil.ReadDir(self.BlocksDirectory())
	if err != nil {
		return nil, err
	}
	var names []BlockID
	for _, f := range files {
		names = append(names, BlockID(f.Name()))
	}
	return names, nil
}

func (self *BlockStore) BlocksDirectory() string {
	return path.Join(self.DataDir, "blocks")
}

func (self *BlockStore) ReadChecksum(block BlockID) (string, error) {
	b, err := ioutil.ReadFile(self.ChecksumFilename(block))
	if err != nil {
		return "", err
	}
	return string(b), nil
}
func (self *BlockStore) WriteChecksum(block BlockID, s string) error {
	return ioutil.WriteFile(self.ChecksumFilename(block), []byte(s), 0777)
}

func (self *BlockStore) MetaDirectory() string {
	return path.Join(self.DataDir, "meta")
}

func (self *BlockStore) BlockFilename(block BlockID) string {
	return path.Join(self.BlocksDirectory(), string(block))
}

func (self *BlockStore) ChecksumFilename(block BlockID) string {
	return path.Join(self.MetaDirectory(), string(block)+".crc32")
}

func (self *BlockStore) DeleteBlock(block BlockID) error {
	err := os.Remove(self.BlockFilename(block))
	if err != nil {
		return err
	}
	err = os.Remove(self.ChecksumFilename(block))
	return err
}
