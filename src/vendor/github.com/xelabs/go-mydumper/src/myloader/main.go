/*
 * go-mydumper
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package main

import (
	"common"
	"flag"
	"fmt"
	"os"

	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	flagOverwriteTables                     bool
	flagPort, flagThreads                   int
	flagUser, flagPasswd, flagHost, flagDir string

	log = xlog.NewStdLog(xlog.Level(xlog.INFO))
)

func init() {
	flag.StringVar(&flagUser, "u", "", "Username with privileges to run the loader")
	flag.StringVar(&flagPasswd, "p", "", "User password")
	flag.StringVar(&flagHost, "h", "", "The host to connect to")
	flag.IntVar(&flagPort, "P", 3306, "TCP/IP port to connect to")
	flag.StringVar(&flagDir, "d", "", "Directory of the dump to import")
	flag.IntVar(&flagThreads, "t", 16, "Number of threads to use")
	flag.BoolVar(&flagOverwriteTables, "o", false, "Drop tables if they already exist")
}

func usage() {
	fmt.Println("Usage: " + os.Args[0] + " -h [HOST] -P [PORT] -u [USER] -p [PASSWORD] -d [DIR] [-o]")
	flag.PrintDefaults()
}

func main() {
	flag.Usage = func() { usage() }
	flag.Parse()

	if flagHost == "" || flagUser == "" || flagPasswd == "" || flagDir == "" {
		usage()
		os.Exit(0)
	}

	args := &common.Args{
		User:            flagUser,
		Password:        flagPasswd,
		Address:         fmt.Sprintf("%s:%d", flagHost, flagPort),
		Outdir:          flagDir,
		Threads:         flagThreads,
		IntervalMs:      10 * 1000,
		OverwriteTables: flagOverwriteTables,
	}
	common.Loader(log, args)
}
