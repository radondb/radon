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
	flagChunksize, flagThreads, flagPort, flagStmtSize         int
	flagUser, flagPasswd, flagHost, flagDb, flagTable, flagDir string

	log = xlog.NewStdLog(xlog.Level(xlog.INFO))
)

func init() {
	flag.StringVar(&flagUser, "u", "", "Username with privileges to run the dump")
	flag.StringVar(&flagPasswd, "p", "", "User password")
	flag.StringVar(&flagHost, "h", "", "The host to connect to")
	flag.IntVar(&flagPort, "P", 3306, "TCP/IP port to connect to")
	flag.StringVar(&flagDb, "db", "", "Database to dump")
	flag.StringVar(&flagTable, "table", "", "Table to dump")
	flag.StringVar(&flagDir, "o", "", "Directory to output files to")
	flag.IntVar(&flagChunksize, "F", 128, "Split tables into chunks of this output file size. This value is in MB")
	flag.IntVar(&flagThreads, "t", 16, "Number of threads to use")
	flag.IntVar(&flagStmtSize, "s", 1000000, "Attempted size of INSERT statement in bytes")
}

func usage() {
	fmt.Println("Usage: " + os.Args[0] + " -h [HOST] -P [PORT] -u [USER] -p [PASSWORD] -db [DATABASE] -o [OUTDIR]")
	flag.PrintDefaults()
}

func main() {
	flag.Usage = func() { usage() }
	flag.Parse()

	if flagHost == "" || flagUser == "" || flagPasswd == "" || flagDb == "" {
		usage()
		os.Exit(0)
	}

	if _, err := os.Stat(flagDir); os.IsNotExist(err) {
		x := os.MkdirAll(flagDir, 0777)
		common.AssertNil(x)
	}

	args := &common.Args{
		User:          flagUser,
		Password:      flagPasswd,
		Address:       fmt.Sprintf("%s:%d", flagHost, flagPort),
		Database:      flagDb,
		Table:         flagTable,
		Outdir:        flagDir,
		ChunksizeInMB: flagChunksize,
		Threads:       flagThreads,
		StmtSize:      flagStmtSize,
		IntervalMs:    10 * 1000,
	}

	common.Dumper(log, args)
}
