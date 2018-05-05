export GOPATH := $(shell pwd)
export PATH := $(GOPATH)/bin:$(PATH)

all: get build test

get:
	@echo "--> go get..."
	go get github.com/XeLabs/go-mysqlstack/driver
	go get github.com/stretchr/testify/assert
	go get github.com/pierrre/gotestcover

build:
	@echo "--> Building..."
	go build -v -o bin/mydumper src/mydumper/main.go
	go build -v -o bin/myloader src/myloader/main.go
	go build -v -o bin/mystreamer src/mystreamer/main.go
	@chmod 755 bin/*

clean:
	@echo "--> Cleaning..."
	@go clean
	@rm -f bin/*

fmt:
	go fmt ./...
	go vet ./...

test:
	@echo "--> Testing..."
	@$(MAKE) testcommon

testcommon:
	go test -race -v common

# code coverage
COVPKGS =	common
coverage:
	go build -v -o bin/gotestcover \
	src/github.com/pierrre/gotestcover/*.go;
	gotestcover -coverprofile=coverage.out -v $(COVPKGS)
	go tool cover -html=coverage.out
.PHONY: get build clean fmt test coverage
