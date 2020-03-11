PREFIX    :=/usr/local
export GOPATH := $(shell pwd):$(GOPATH)
export PATH := $(GOPATH)/bin:$(PATH)

build: LDFLAGS   += $(shell GOPATH=${GOPATH} src/build/ldflags.sh)
build:
	@echo "--> Building..."
	@mkdir -p bin/
	go build -v -o bin/radon    --ldflags '$(LDFLAGS)' src/radon/radon.go
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
	@$(MAKE) testxbase
	@$(MAKE) testxcontext
	@$(MAKE) testconfig
	@$(MAKE) testrouter
	@$(MAKE) testoptimizer
	@$(MAKE) testplanner
	@$(MAKE) testexecutor
	@$(MAKE) testbackend
	@$(MAKE) testproxy
	@$(MAKE) testaudit
	@$(MAKE) testsyncer
	@$(MAKE) testctl
	@$(MAKE) testmonitor
	@$(MAKE) testplugins
	@$(MAKE) testfuzz

testxbase:
	go test -v -race xbase
	go test -v -race xbase/stats
	go test -v -race xbase/sync2
testxcontext:
	go test -v xcontext
testconfig:
	go test -v config
testrouter:
	go test -v router
testoptimizer:
	go test -v optimizer
testplanner:
	go test -v planner/...
testexecutor:
	go test -v executor/...
testbackend:
	go test -v -race backend
testproxy:
	go test -v -race proxy
testaudit:
	go test -v -race audit
testsyncer:
	go test -v -race syncer
testctl:
	go test -v -race ctl/v1
testpoc:
	go test -v poc
testmonitor:
	go test -v monitor
testplugins:
	go test -v plugins
	go test -v plugins/autoincrement
	go test -v plugins/privilege
	go test -v plugins/shiftmanager
testmysqlstack:
	cd src/vendor/github.com/xelabs/go-mysqlstack&&make test

testfuzz:
	go test -v -race fuzz/sqlparser
testshift:
	cd src/vendor/github.com/radondb/shift&&make test

# code coverage
allpkgs =	xbase\
			ctl/v1/\
			xcontext\
			config\
			router\
			optimizer\
			planner/...\
			executor/...\
			backend\
			proxy\
			audit\
			syncer\
			monitor\
			plugins/...
coverage:
	go build -v -o bin/gotestcover \
	src/vendor/github.com/pierrre/gotestcover/*.go;
	bin/gotestcover -coverprofile=coverage.out -v $(allpkgs)
	go tool cover -html=coverage.out

check:
	go get -v gopkg.in/alecthomas/gometalinter.v2
	bin/gometalinter.v2 -j 4 --disable-all \
	--enable=gofmt \
	--enable=golint \
	--enable=vet \
	--enable=gosimple \
	--enable=unconvert \
	--deadline=10m $(allpkgs) 2>&1 | tee /dev/stderr

.PHONY: build clean install fmt test coverage check
