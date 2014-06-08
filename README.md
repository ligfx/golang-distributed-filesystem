[![GoDoc](https://godoc.org/github.com/michaelmaltese/golang-distributed-filesystem?status.png)](https://godoc.org/github.com/michaelmaltese/golang-distributed-filesystem) [![Build Status](https://travis-ci.org/michaelmaltese/golang-distributed-filesystem.svg?branch=master)](https://travis-ci.org/michaelmaltese/golang-distributed-filesystem)

Writing a HDFS clone in [Go](http://golang.org) to learn more about Go and the nitty-gritty of distributed systems.

## Features/TODO

- [x] MetaDataNode/DataNode handle uploads
- [x] MetaDataNode/DataNode handle downloads
- [x] DataNode dynamically registers with MetaDataNode
- [x] DataNode tells MetaDataNode its blocks on startup
- [x] MetaDataNode persists file->blocklist map
- [x] DataNode pipelines uploads to other DataNodes
- [x] MetaDataNode can restart and DataNode will re-register (heartbeats)
- [ ] Drop DataNodes when they go down (heartbeats)
- [ ] Tell DataNodes to re-register if MetaDataNode doesn't recognize them
- [ ] MetaDataNode obeys replication factor instead of replicating to all DataNodes
- [ ] If a client tries to upload a block and every DataNode in its list is down, it needs to get more from the MetaDataNode.
- [ ] MetaDataNode moves blocks when a fresh DataNode comes online
- [ ] Support multiple MetaDataNodes with a DHT (and consensus algorithm?)

Uses [gpm](https://github.com/pote/gpm) for dependency management. 
