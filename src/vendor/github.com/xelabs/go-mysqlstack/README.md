[![Build Status](https://travis-ci.org/xelabs/go-mysqlstack.png)](https://travis-ci.org/xelabs/go-mysqlstack) [![Go Report Card](https://goreportcard.com/badge/github.com/xelabs/go-mysqlstack)](https://goreportcard.com/report/github.com/xelabs/go-mysqlstack) [![codecov.io](https://codecov.io/gh/xelabs/go-mysqlstack/graphs/badge.svg)](https://codecov.io/gh/xelabs/go-mysqlstack/branch/master)

# go-mysqlstack

***go-mysqlstack*** is an MySQL protocol library implementing in Go (golang).

Protocol is based on [mysqlproto-go](https://github.com/pubnative/mysqlproto-go) and [go-sql-driver](https://github.com/go-sql-driver/mysql)

## Running Tests

```
$ mkdir src
$ export GOPATH=`pwd`
$ go get -u github.com/xelabs/go-mysqlstack/driver
$ cd src/github.com/xelabs/go-mysqlstack/
$ make test
```

## Examples

1. ***examples/mysqld.go*** mocks a MySQL server by running:

```
$ go run example/mysqld.go
  2018/01/26 16:02:02.304376 mysqld.go:52:     [INFO]    mysqld.server.start.address[:4407]
```

2. ***examples/client.go*** mocks a client and query from the mock MySQL server:

```
$ go run example/client.go
  2018/01/26 16:06:10.779340 client.go:32:    [INFO]    results:[[[10 nice name]]]
```

## Status

go-mysqlstack is production ready.

## License

go-mysqlstack is released under the GPLv3. See LICENSE
