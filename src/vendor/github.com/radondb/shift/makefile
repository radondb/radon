build: LDFLAGS   += $(shell ./build/ldflags.sh)
build:
	@echo "--> Building..."
	@mkdir -p bin/
	go build -v -o bin/shift --ldflags '$(LDFLAGS)' ./cli/cli.go
	@chmod 755 bin/*

clean:
	@echo "--> Cleaning..."
	@mkdir -p bin/
	@go clean
	@rm -f bin/*

fmt:
	go fmt ./...

test:
	@echo "--> Testing..."
	@$(MAKE) testshift
	@$(MAKE) testxbase
	@$(MAKE) testmysql

testshift:
	go test -v -race ./shift
testxbase:
	go test -v -race ./xbase/sync2

testmysql:
	go test -v ./vendor/github.com/siddontang/go-mysql/...

# code coverage
COVPKGS =	./shift/...\
   			./xbase/...\
			./vendor/github.com/siddontang/go-mysql/...
coverage:
	go build -v -o bin/gotestcover \
	../../pierrre/gotestcover/*.go;
	bin/gotestcover -coverprofile=coverage.out -v $(COVPKGS)
	go tool cover -html=coverage.out
.PHONY: build clean install fmt test coverage .go-version
