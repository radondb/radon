# How to fuzz Radon using go-fuzz #

This document will describe how to use the fuzz-testing library `go-fuzz` on Radon packages.

### Setup and Installation ###

* First, we must get `go-fuzz`:
```
$ go get -u github.com/dvyukov/go-fuzz/...
```

* Next, Build the test program - this produces a <folder name here>-fuzz.zip (archive) file.
```
$ go-fuzz-build fuzz/sqlparser
```

* Now, run `go-fuzz`!!!
```
$ go-fuzz -bin=./sqlparser-fuzz.zip -workdir=./src/fuzz/sqlparser
```
