export PATH := $(GOPATH)/bin:$(PATH)

fmt:
	go fmt ./...
	go vet ./...

test:
	go get github.com/stretchr/testify/assert
	@echo "--> Testing..."
	@$(MAKE) testxlog
	@$(MAKE) testsqlparser
	@$(MAKE) testcommon
	@$(MAKE) testsqldb
	@$(MAKE) testproto
	@$(MAKE) testpacket
	@$(MAKE) testdriver

testxlog:
	go test -v ./xlog
testsqlparser:
	go test -v ./sqlparser/...
testcommon:
	go test -v ./common
testsqldb:
	go test -v ./sqldb
testproto:
	go test -v ./proto
testpacket:
	go test -v ./packet
testdriver:
	go test -v ./driver

COVPKGS = ./sqlparser ./common ./sqldb ./proto ./packet ./driver ./sqlparser/depends/sqltypes
coverage:
	go get github.com/pierrre/gotestcover
	gotestcover -coverprofile=coverage.out -v $(COVPKGS)
	go tool cover -html=coverage.out

.PHONY: fmt testcommon testproto testpacket testdriver coverage
