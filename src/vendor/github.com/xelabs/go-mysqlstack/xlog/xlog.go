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
	"strings"
)

var (
	defaultlog *Log
)

// LogLevel used for log level.
type LogLevel int

const (
	// DEBUG enum.
	DEBUG LogLevel = 1 << iota
	// INFO enum.
	INFO
	// WARNING enum.
	WARNING
	// ERROR enum.
	ERROR
	// FATAL enum.
	FATAL
	// PANIC enum.
	PANIC
)

// LevelNames represents the string name of all levels.
var LevelNames = [...]string{
	DEBUG:   "DEBUG",
	INFO:    "INFO",
	WARNING: "WARNING",
	ERROR:   "ERROR",
	FATAL:   "FATAL",
	PANIC:   "PANIC",
}

const (
	// D_LOG_FLAGS is the default log flags.
	D_LOG_FLAGS int = log.LstdFlags | log.Lmicroseconds | log.Lshortfile
)

// Log struct.
type Log struct {
	opts *Options
	*log.Logger
}

// NewSysLog creates a new sys log.
func NewSysLog(opts ...Option) *Log {
	w, err := syslog.New(syslog.LOG_DEBUG, "")
	if err != nil {
		panic(err)
	}
	return NewXLog(w, opts...)
}

// NewStdLog creates a new std log.
func NewStdLog(opts ...Option) *Log {
	return NewXLog(os.Stdout, opts...)
}

// NewXLog creates a new xlog.
func NewXLog(w io.Writer, opts ...Option) *Log {
	options := newOptions(opts...)

	l := &Log{
		opts: options,
	}
	l.Logger = log.New(w, l.opts.Name, D_LOG_FLAGS)
	defaultlog = l
	return l
}

// NewLog creates the new log.
func NewLog(w io.Writer, prefix string, flag int) *Log {
	l := &Log{}
	l.Logger = log.New(w, prefix, flag)
	return l
}

// GetLog returns Log.
func GetLog() *Log {
	if defaultlog == nil {
		log := NewStdLog(Level(INFO))
		defaultlog = log
	}
	return defaultlog
}

// SetLevel used to set the log level.
func (t *Log) SetLevel(level string) {
	for i, v := range LevelNames {
		if level == v {
			t.opts.Level = LogLevel(i)
			return
		}
	}
}

// Debug used to log debug msg.
func (t *Log) Debug(format string, v ...interface{}) {
	if DEBUG < t.opts.Level {
		return
	}
	t.log("\t [DEBUG] \t%s", fmt.Sprintf(format, v...))
}

// Info used to log info msg.
func (t *Log) Info(format string, v ...interface{}) {
	if INFO < t.opts.Level {
		return
	}
	t.log("\t [INFO] \t%s", fmt.Sprintf(format, v...))
}

// Warning used to log warning msg.
func (t *Log) Warning(format string, v ...interface{}) {
	if WARNING < t.opts.Level {
		return
	}
	t.log("\t [WARNING] \t%s", fmt.Sprintf(format, v...))
}

// Error used to log error msg.
func (t *Log) Error(format string, v ...interface{}) {
	if ERROR < t.opts.Level {
		return
	}
	t.log("\t [ERROR] \t%s", fmt.Sprintf(format, v...))
}

// Fatal used to log faltal msg.
func (t *Log) Fatal(format string, v ...interface{}) {
	if FATAL < t.opts.Level {
		return
	}
	t.log("\t [FATAL+EXIT] \t%s", fmt.Sprintf(format, v...))
	os.Exit(1)
}

// Panic used to log panic msg.
func (t *Log) Panic(format string, v ...interface{}) {
	if PANIC < t.opts.Level {
		return
	}
	msg := fmt.Sprintf("\t [PANIC] \t%s", fmt.Sprintf(format, v...))
	t.log(msg)
	panic(msg)
}

// Close used to close the log.
func (t *Log) Close() {
	// nothing
}

func (t *Log) log(format string, v ...interface{}) {
	t.Output(3, strings.Repeat(" ", 3)+fmt.Sprintf(format, v...)+"\n")
}
