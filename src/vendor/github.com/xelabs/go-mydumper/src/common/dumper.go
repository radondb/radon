/*
 * go-mydumper
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package common

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func writeMetaData(args *Args) {
	file := fmt.Sprintf("%s/metadata", args.Outdir)
	WriteFile(file, "")
}

func dumpDatabaseSchema(log *xlog.Log, conn *Connection, args *Args) {
	err := conn.Execute(fmt.Sprintf("USE `%s`", args.Database))
	AssertNil(err)

	schema := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`;", args.Database)
	file := fmt.Sprintf("%s/%s-schema-create.sql", args.Outdir, args.Database)
	WriteFile(file, schema)
	log.Info("dumping.database[%s].schema...", args.Database)
}

func dumpTableSchema(log *xlog.Log, conn *Connection, args *Args, table string) {
	qr, err := conn.Fetch(fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", args.Database, table))
	AssertNil(err)
	schema := qr.Rows[0][1].String() + ";\n"

	file := fmt.Sprintf("%s/%s.%s-schema.sql", args.Outdir, args.Database, table)
	WriteFile(file, schema)
	log.Info("dumping.table[%s.%s].schema...", args.Database, table)
}

func dumpTable(log *xlog.Log, conn *Connection, args *Args, table string) {
	var allBytes uint64
	var allRows uint64

	_, err := conn.Fetch("set @@SESSION.radon_streaming_fetch='ON'")
	AssertNil(err)

	cursor, err := conn.StreamFetch(fmt.Sprintf("SELECT * FROM `%s`.`%s`", args.Database, table))
	AssertNil(err)

	fields := make([]string, 0, 16)
	flds := cursor.Fields()
	for _, fld := range flds {
		fields = append(fields, fmt.Sprintf("`%s`", fld.Name))
	}

	fileNo := 1
	stmtsize := 0
	chunkbytes := 0
	rows := make([]string, 0, 256)
	inserts := make([]string, 0, 256)
	for cursor.Next() {
		row, err := cursor.RowValues()
		AssertNil(err)

		values := make([]string, 0, 16)
		for _, v := range row {
			if v.Raw() == nil {
				values = append(values, "NULL")
			} else {
				str := v.String()
				switch {
				case v.IsSigned(), v.IsUnsigned(), v.IsFloat(), v.IsIntegral(), v.Type() == querypb.Type_DECIMAL:
					values = append(values, str)
				default:
					values = append(values, fmt.Sprintf("\"%s\"", EscapeBytes(v.Raw())))
				}
			}
		}
		r := "(" + strings.Join(values, ",") + ")"
		rows = append(rows, r)

		allRows++
		stmtsize += len(r)
		chunkbytes += len(r)
		allBytes += uint64(len(r))
		atomic.AddUint64(&args.Allbytes, uint64(len(r)))
		atomic.AddUint64(&args.Allrows, 1)

		if stmtsize >= args.StmtSize {
			insertone := fmt.Sprintf("INSERT INTO `%s`(%s) VALUES\n%s", table, strings.Join(fields, ","), strings.Join(rows, ",\n"))
			inserts = append(inserts, insertone)
			rows = rows[:0]
			stmtsize = 0
		}

		if (chunkbytes / 1024 / 1024) >= args.ChunksizeInMB {
			query := strings.Join(inserts, ";\n") + ";\n"
			file := fmt.Sprintf("%s/%s.%s.%05d.sql", args.Outdir, args.Database, table, fileNo)
			WriteFile(file, query)

			log.Info("dumping.table[%s.%s].rows[%v].bytes[%vMB].part[%v].thread[%d]", args.Database, table, allRows, (allBytes / 1024 / 1024), fileNo, conn.ID)
			inserts = inserts[:0]
			chunkbytes = 0
			fileNo++
		}
	}
	if chunkbytes > 0 {
		insertone := fmt.Sprintf("INSERT INTO `%s`(%s) VALUES\n%s", table, strings.Join(fields, ","), strings.Join(rows, ",\n"))
		inserts = append(inserts, insertone)

		query := strings.Join(inserts, ";\n") + ";\n"
		file := fmt.Sprintf("%s/%s.%s.%05d.sql", args.Outdir, args.Database, table, fileNo)
		WriteFile(file, query)
	}
	err = cursor.Close()
	AssertNil(err)

	log.Info("dumping.table[%s.%s].done.allrows[%v].allbytes[%vMB].thread[%d]...", args.Database, table, allRows, (allBytes / 1024 / 1024), conn.ID)
}

func allTables(log *xlog.Log, conn *Connection, args *Args) []string {
	qr, err := conn.Fetch(fmt.Sprintf("SHOW TABLES FROM `%s`", args.Database))
	AssertNil(err)

	tables := make([]string, 0, 128)
	for _, t := range qr.Rows {
		tables = append(tables, t[0].String())
	}
	return tables
}

// Dumper used to start the dumper worker.
func Dumper(log *xlog.Log, args *Args) {
	pool, err := NewPool(log, args.Threads, args.Address, args.User, args.Password)
	AssertNil(err)
	defer pool.Close()

	// Meta data.
	writeMetaData(args)

	// database.
	conn := pool.Get()
	dumpDatabaseSchema(log, conn, args)

	// tables.
	var wg sync.WaitGroup
	var tables []string
	t := time.Now()
	if args.Table != "" {
		tables = strings.Split(args.Table, ",")
	} else {
		tables = allTables(log, conn, args)
	}
	pool.Put(conn)

	for _, table := range tables {
		conn := pool.Get()
		dumpTableSchema(log, conn, args, table)

		wg.Add(1)
		go func(conn *Connection, table string) {
			defer func() {
				wg.Done()
				pool.Put(conn)
			}()
			log.Info("dumping.table[%s.%s].datas.thread[%d]...", args.Database, table, conn.ID)
			dumpTable(log, conn, args, table)
			log.Info("dumping.table[%s.%s].datas.thread[%d].done...", args.Database, table, conn.ID)
		}(conn, table)
	}

	tick := time.NewTicker(time.Millisecond * time.Duration(args.IntervalMs))
	defer tick.Stop()
	go func() {
		for range tick.C {
			diff := time.Since(t).Seconds()
			allbytesMB := float64(atomic.LoadUint64(&args.Allbytes) / 1024 / 1024)
			allrows := atomic.LoadUint64(&args.Allrows)
			rates := allbytesMB / diff
			log.Info("dumping.allbytes[%vMB].allrows[%v].time[%.2fsec].rates[%.2fMB/sec]...", allbytesMB, allrows, diff, rates)
		}
	}()

	wg.Wait()
	elapsed := time.Since(t).Seconds()
	log.Info("dumping.all.done.cost[%.2fsec].allrows[%v].allbytes[%v].rate[%.2fMB/s]", elapsed, args.Allrows, args.Allbytes, (float64(args.Allbytes/1024/1024) / elapsed))
}
