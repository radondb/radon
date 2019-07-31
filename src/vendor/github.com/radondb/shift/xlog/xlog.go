/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package xlog

import (
	"fmt"
	"io"
	"log"
	"log/syslog"
	"os"
)

var (
	defaultlog *Log
)

type LogLevel int

const (
	DEBUG LogLevel = 1 << iota
	INFO
	WARNING
	ERROR
	FATAL
	PANIC
)

var LevelNames = [...]string{
	DEBUG:   "DEBUG",
	INFO:    "INFO",
	WARNING: "WARNING",
	ERROR:   "ERROR",
	FATAL:   "FATAL",
	PANIC:   "PANIC",
}

const (
	D_LOG_FLAGS int = log.LstdFlags | log.Lmicroseconds | log.Lshortfile
)

type Log struct {
	opts *Options
	*log.Logger
}

// syslog
func NewSysLog(opts ...Option) *Log {
	w, err := syslog.New(syslog.LOG_DEBUG, "")
	if err != nil {
		panic(err)
	}
	return NewXLog(w, opts...)
}

func NewStdLog(opts ...Option) *Log {
	return NewXLog(os.Stdout, opts...)
}

func NewXLog(w io.Writer, opts ...Option) *Log {
	options := newOptions(opts...)

	l := &Log{
		opts: options,
	}
	l.Logger = log.New(w, l.opts.Name, D_LOG_FLAGS)
	defaultlog = l
	return l
}

func NewLog(w io.Writer, prefix string, flag int) *Log {
	l := &Log{}
	l.Logger = log.New(w, prefix, flag)
	return l
}

func GetLog() *Log {
	if defaultlog == nil {
		log := NewStdLog(Level(INFO))
		defaultlog = log
	}
	return defaultlog
}

func (t *Log) SetLevel(level string) {
	for i, v := range LevelNames {
		if level == v {
			t.opts.Level = LogLevel(i)
			return
		}
	}
}

func (t *Log) Debug(format string, v ...interface{}) {
	if DEBUG < t.opts.Level {
		return
	}
	t.log("\t  [DEBUG]  \t%s", fmt.Sprintf(format, v...))
}

func (t *Log) Info(format string, v ...interface{}) {
	if INFO < t.opts.Level {
		return
	}
	t.log("\t  [INFO]  \t%s", fmt.Sprintf(format, v...))
}

func (t *Log) Warning(format string, v ...interface{}) {
	if WARNING < t.opts.Level {
		return
	}
	t.log("\t  [WARNING]  \t%s", fmt.Sprintf(format, v...))
}

func (t *Log) Error(format string, v ...interface{}) {
	if ERROR < t.opts.Level {
		return
	}
	t.log("\t  [ERROR]  \t%s", fmt.Sprintf(format, v...))
}

func (t *Log) Fatal(format string, v ...interface{}) {
	if FATAL < t.opts.Level {
		return
	}
	t.log("\t  [FATAL+EXIT]  \t%s", fmt.Sprintf(format, v...))
	os.Exit(0)
}

func (t *Log) Panic(format string, v ...interface{}) {
	if PANIC < t.opts.Level {
		return
	}
	msg := fmt.Sprintf("\t  [PANIC]  \t %s", fmt.Sprintf(format, v...))
	t.log(msg)
	panic(msg)
}

func (t *Log) Close() {
	// nothing
}

func (t *Log) log(format string, v ...interface{}) {
	t.Output(3, fmt.Sprintf(format, v...)+"\n")
}
