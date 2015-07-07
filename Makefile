all: build test

build:
	GOPATH="$(CURDIR)" && go install golang-distributed-filesystem

test: build
	GOPATH="$(CURDIR)" && go test ./src/golang-distributed-filesystem/
