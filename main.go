package main

func main() {
	CommandRun([]Command{
		{Name: "datanode", Description: "datanode", Function: DataNode},
		{Name: "metadatanode", Description: "metadatanode", Function: MetadataNode},
		{Name: "upload", Description: "upload LOCAL", Function: Upload},
	})
}
