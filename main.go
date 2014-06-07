package main

import (
	"github.com/michaelmaltese/golang-distributed-filesystem/datanode"
	"github.com/michaelmaltese/golang-distributed-filesystem/metadatanode"
	"github.com/michaelmaltese/golang-distributed-filesystem/upload"
)

func main() {
	CommandRun([]Command{
		{Name: "datanode", Description: "datanode", Function: datanode.DataNode},
		{Name: "metadatanode", Description: "metadatanode", Function: metadatanode.MetadataNode},
		{Name: "upload", Description: "upload LOCAL", Function: upload.Upload},
	})
}
