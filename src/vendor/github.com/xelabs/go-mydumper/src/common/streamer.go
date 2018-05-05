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

	"github.com/xelabs/go-mysqlstack/sqlparser"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func streamDatabaseSchema(log *xlog.Log, db string, todb string, from *Connection, to *Connection) {
	err := from.Execute(fmt.Sprintf("USE `%s`", db))
	AssertNil(err)

	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`;", todb)
	err = to.Execute(query)
	AssertNil(err)
	log.Info("streaming.database[%s].schema...", todb)
}

func streamTableSchema(log *xlog.Log, db string, todb string, toengine string, tbl string, overwrite bool, from *Connection, to *Connection) {
	qr, err := from.Fetch(fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", db, tbl))
	AssertNil(err)
	query := qr.Rows[0][1].String()

	if overwrite {
		dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", todb, tbl)
		err = to.Execute(dropQuery)
		AssertNil(err)
	}

	// Rewrite the table engine.
	if toengine != "" {
		node, err := sqlparser.Parse(query)
		AssertNil(err)
		if ddl, ok := node.(*sqlparser.DDL); ok {
			ddl.TableSpec.Options.Engine = toengine
			query = sqlparser.String(ddl)
			log.Warning("streaming.schema.engine.rewritten:%v", query)
		}
	}
	err = to.Execute(fmt.Sprintf("USE `%s`", todb))
	AssertNil(err)

	err = to.Execute(query)
	AssertNil(err)
	log.Info("streaming.table[%s.%s].schema...", todb, tbl)
}

func streamTable(log *xlog.Log, db string, todb string, tbl string, from *Connection, to *Connection, args *Args) {
	var allRows uint64
	var allBytes uint64

	cursor, err := from.StreamFetch(fmt.Sprintf("SELECT /*backup*/ * FROM `%s`.`%s`", db, tbl))
	AssertNil(err)

	fields := make([]string, 0, 16)
	flds := cursor.Fields()
	for _, fld := range flds {
		fields = append(fields, fmt.Sprintf("`%s`", fld.Name))
	}

	err = to.Execute(fmt.Sprintf("USE `%s`", todb))
	AssertNil(err)

	stmtsize := 0
	rows := make([]string, 0, 256)
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
		allBytes += uint64(len(r))
		atomic.AddUint64(&args.Allbytes, uint64(len(r)))
		atomic.AddUint64(&args.Allrows, 1)

		if stmtsize >= args.StmtSize {
			query := fmt.Sprintf("INSERT INTO `%s`(%s) VALUES\n%s", tbl, strings.Join(fields, ","), strings.Join(rows, ",\n"))
			err = to.Execute(query)
			AssertNil(err)

			rows = rows[:0]
			stmtsize = 0
		}
	}

	if stmtsize > 0 {
		query := fmt.Sprintf("INSERT INTO `%s`(%s) VALUES\n%s", tbl, strings.Join(fields, ","), strings.Join(rows, ",\n"))
		err = to.Execute(query)
		AssertNil(err)
	}

	err = cursor.Close()
	AssertNil(err)
	log.Info("streaming.table[%s.%s].done.allrows[%v].allbytes[%vMB].thread[%d]...", todb, tbl, allRows, (allBytes / 1024 / 1024), from.ID)
}

// Streamer used to start the streamer worker.
func Streamer(log *xlog.Log, args *Args) {
	var tables []string
	var wg sync.WaitGroup

	fromPool, err := NewPool(log, args.Threads, args.Address, args.User, args.Password)
	AssertNil(err)
	defer fromPool.Close()

	toPool, err := NewPool(log, args.Threads, args.ToAddress, args.ToUser, args.ToPassword)
	AssertNil(err)
	defer toPool.Close()

	// database.
	db := args.Database
	todb := args.ToDatabase
	if todb == "" {
		todb = db
	}
	toengine := args.ToEngine

	from := fromPool.Get()
	to := toPool.Get()
	streamDatabaseSchema(log, db, todb, from, to)

	// tables.
	t := time.Now()
	if args.Table != "" {
		tables = strings.Split(args.Table, ",")
	} else {
		tables = allTables(log, from, args)
	}
	fromPool.Put(from)
	toPool.Put(to)

	// datas.
	for _, tbl := range tables {
		from := fromPool.Get()
		to := toPool.Get()
		streamTableSchema(log, db, todb, toengine, tbl, args.OverwriteTables, from, to)

		wg.Add(1)
		go func(db string, tbl string, from *Connection, to *Connection, args *Args) {
			defer func() {
				wg.Done()
				fromPool.Put(from)
				toPool.Put(to)
			}()
			log.Info("streaming.table[%s.%s].datas.thread[%d]...", db, tbl, from.ID)
			streamTable(log, db, todb, tbl, from, to, args)
			log.Info("streaming.table[%s.%s].datas.thread[%d].done...", db, tbl, from.ID)
		}(db, tbl, from, to, args)
	}

	tick := time.NewTicker(time.Millisecond * time.Duration(args.IntervalMs))
	defer tick.Stop()
	go func() {
		for range tick.C {
			diff := time.Since(t).Seconds()
			allbytesMB := float64(atomic.LoadUint64(&args.Allbytes) / 1024 / 1024)
			allrows := atomic.LoadUint64(&args.Allrows)
			rates := allbytesMB / diff
			log.Info("streaming.allbytes[%vMB].allrows[%v].time[%.2fsec].rates[%.2fMB/sec]...", allbytesMB, allrows, diff, rates)
		}
	}()

	wg.Wait()
	elapsed := time.Since(t).Seconds()
	log.Info("streaming.all.done.cost[%.2fsec].allrows[%v].allbytes[%v].rate[%.2fMB/s]", elapsed, args.Allrows, args.Allbytes, (float64(args.Allbytes/1024/1024) / elapsed))
}
