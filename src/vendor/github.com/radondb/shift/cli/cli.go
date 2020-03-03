/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package main

import (
	"flag"
	"fmt"
	"runtime"

	"github.com/radondb/shift/build"
	"github.com/radondb/shift/shift"
	"github.com/radondb/shift/xlog"
)

var (
	toFlavor = flag.String("to-flavor", "mysql", "Destination db flavor, like mysql/mariadb/radondb")

	from         = flag.String("from", "", "Source MySQL backend")
	fromUser     = flag.String("from-user", "", "MySQL user, must have replication privilege")
	fromPassword = flag.String("from-password", "", "MySQL user password")
	fromDatabase = flag.String("from-database", "", "Source database")
	fromTable    = flag.String("from-table", "", "Source table")

	to         = flag.String("to", "", "Destination MySQL backend")
	toUser     = flag.String("to-user", "", "MySQL user, must have replication privilege")
	toPassword = flag.String("to-password", "", "MySQL user password")
	toDatabase = flag.String("to-database", "", "Destination database")
	toTable    = flag.String("to-table", "", "Destination table")

	cleanup                = flag.Bool("cleanup", false, "Cleanup the from table after shifted(defaults false)")
	rebalance              = flag.Bool("rebalance", false, "Rebalance means a rebalance operation, which from table need cleanup after shifted(default false)")
	checksum               = flag.Bool("checksum", true, "Checksum the from table and to table after shifted(defaults true)")
	mysqlDump              = flag.String("mysqldump", "mysqldump", "mysqldump path")
	threads                = flag.Int("threads", 16, "shift threads num(defaults 16)")
	behinds                = flag.Int("behinds", 2048, "seconds behinds num(default 2048)")
	radonURL               = flag.String("radonurl", "http://127.0.0.1:8080", "Radon RESTful api(defaults http://127.0.0.1:8080)")
	waitTimeBeforeChecksum = flag.Int("wait-time-before-checksum", 10, "seconds sleep before checksum")

	debug = flag.Bool("debug", false, "Set log to debug mode(defaults false)")
)

func check(log *xlog.Log) {
	if *toFlavor == "" || *from == "" || *fromUser == "" || *fromDatabase == "" || *fromTable == "" ||
		*to == "" || *toUser == "" || *toDatabase == "" || *toTable == "" {
		log.Panic("usage: shift --from=[host:port] --from-database=[database] --from-table=[table] --from-user=[user] --from-password=[password] --to=[host:port] --to-database=[database] --to-table=[table]  --to-user=[user] --to-password=[password] --cleanup=[false|true] --to-flavor=[mysql|radondb|mariadb]")
	}
}

func main() {
	log := xlog.NewStdLog(xlog.Level(xlog.INFO))
	runtime.GOMAXPROCS(runtime.NumCPU())

	build := build.GetInfo()
	fmt.Printf("shift:[%+v]\n", build)

	// flags.
	flag.Parse()

	// log.
	if *debug {
		log = xlog.NewStdLog(xlog.Level(xlog.DEBUG))
	}
	check(log)
	fmt.Println(`
           IMPORTANT: Please check that the shift run completes successfully.
           At the end of a successful shift run prints "shift.completed.OK!".`)

	cfg := &shift.Config{
		ToFlavor:               *toFlavor,
		From:                   *from,
		FromUser:               *fromUser,
		FromPassword:           *fromPassword,
		FromDatabase:           *fromDatabase,
		FromTable:              *fromTable,
		To:                     *to,
		ToUser:                 *toUser,
		ToPassword:             *toPassword,
		ToDatabase:             *toDatabase,
		ToTable:                *toTable,
		Rebalance:              *rebalance,
		Cleanup:                *cleanup,
		MySQLDump:              *mysqlDump,
		Threads:                *threads,
		Behinds:                *behinds,
		RadonURL:               *radonURL,
		Checksum:               *checksum,
		WaitTimeBeforeChecksum: *waitTimeBeforeChecksum,
	}
	log.Info("shift.cfg:%+v", cfg)
	shift := shift.NewShift(log, cfg)
	if err := shift.Start(); err != nil {
		log.Fatal("shift.start.error:%+v", err)
	}

	_ = shift.WaitFinish()
}
