package main

import (
	"github.com/michaelmaltese/golang-distributed-filesystem/metadatanode"
)

func main() {
	CommandRun([]Command{
		{Name: "datanode", Description: "datanode", Function: DataNode},
		{Name: "metadatanode", Description: "metadatanode", Function: metadatanode.MetadataNode},
		{Name: "upload", Description: "upload LOCAL", Function: Upload},
	})
}
