package main

import (
	"math/rand"
	"time"

	"github.com/michaelmaltese/golang-distributed-filesystem/datanode"
	"github.com/michaelmaltese/golang-distributed-filesystem/metadatanode"
	"github.com/michaelmaltese/golang-distributed-filesystem/upload"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	CommandRun([]Command{
		{Name: "datanode", Description: "datanode", Function: datanode.DataNode},
		{Name: "metadatanode", Description: "metadatanode", Function: metadatanode.MetadataNode},
		{Name: "upload", Description: "upload LOCAL", Function: upload.Upload},
	})
}
