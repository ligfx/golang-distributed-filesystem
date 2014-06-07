[![GoDoc](https://godoc.org/github.com/michaelmaltese/golang-distributed-filesystem?status.png)](https://godoc.org/github.com/michaelmaltese/golang-distributed-filesystem) [![Build Status](https://travis-ci.org/michaelmaltese/golang-distributed-filesystem.svg?branch=master)](https://travis-ci.org/michaelmaltese/golang-distributed-filesystem)

Writing a HDFS clone in [Go](http://golang.org) to learn more about Go and the nitty-gritty of distributed systems.

## Features/TODO

- [x] MetaDataNode/DataNode handle uploads
- [x] MetaDataNode/DataNode handle downloads
- [x] DataNode dynamically registers with MetaDataNode
- [x] DataNode tells MetaDataNode its blocks on startup
- [ ] DataNode persists ID between sessions
- [ ] DataNode imprints on MetaDataNode and won't connect to others
- [ ] DataNode pipelines uploads to other DataNodes
- [ ] MetaDataNode persists file->blocklist map
- [ ] MetaDataNode can restart and DataNode will re-register (heartbeats)
- [ ] MetaDataNode will drop DataNodes when they go down (heartbeats?)
- [ ] MetaDataNode obeys replication factor instead of replicating to all DataNodes
- [ ] MetaDataNode moves blocks when a fresh DataNode comes online
- [ ] Support multiple MetaDataNodes with a DHT (and consensus algorithm?)

Uses [gpm](https://github.com/pote/gpm) for dependency management. 
