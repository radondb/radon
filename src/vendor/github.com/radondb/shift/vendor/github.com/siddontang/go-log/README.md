## go-log

a golang log lib supports level and multi handlers

## Use

    import "github.com/siddontang/go-log/log"

    //log with different level
    log.Info("hello world")
    log.Error("hello world")

    //create a logger with specified handler
    h := NewStreamHandler(os.Stdout)
    l := log.NewDefault(h)
    l.Info("hello world")

## go-doc

[![GoDoc](https://godoc.org/github.com/siddontang/go-log?status.png)](https://godoc.org/github.com/siddontang/go-log)
