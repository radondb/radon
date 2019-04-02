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
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// Files tuple.
type Files struct {
	databases []string
	schemas   []string
	tables    []string
}

var (
	dbSuffix     = "-schema-create.sql"
	schemaSuffix = "-schema.sql"
	tableSuffix  = ".sql"
)

func loadFiles(log *xlog.Log, dir string) *Files {
	files := &Files{}
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Panicf("loader.file.walk.error:%+v", err)
		}

		if !info.IsDir() {
			switch {
			case strings.HasSuffix(path, dbSuffix):
				files.databases = append(files.databases, path)
			case strings.HasSuffix(path, schemaSuffix):
				files.schemas = append(files.schemas, path)
			default:
				if strings.HasSuffix(path, tableSuffix) {
					files.tables = append(files.tables, path)
				}
			}
		}
		return nil
	}); err != nil {
		log.Panicf("loader.file.walk.error:%+v", err)
	}
	return files
}

func restoreDatabaseSchema(log *xlog.Log, dbs []string, conn *Connection) {
	for _, db := range dbs {
		base := filepath.Base(db)
		name := strings.TrimSuffix(base, dbSuffix)

		data, err := ReadFile(db)
		AssertNil(err)
		sql := common.BytesToString(data)

		err = conn.Execute(sql)
		AssertNil(err)
		log.Info("restoring.database[%s]", name)
	}
}

func restoreTableSchema(log *xlog.Log, overwrite bool, tables []string, conn *Connection) {
	for _, table := range tables {
		// use
		base := filepath.Base(table)
		name := strings.TrimSuffix(base, schemaSuffix)
		db := strings.Split(name, ".")[0]

		err := conn.Execute(fmt.Sprintf("USE `%s`", db))
		AssertNil(err)

		data, err := ReadFile(table)
		AssertNil(err)
		query1 := common.BytesToString(data)
		querys := strings.Split(query1, ";\n")
		for _, query := range querys {
			if !strings.HasPrefix(query, "/*") && query != "" {
				if overwrite {
					dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", db, name)
					err = conn.Execute(dropQuery)
					AssertNil(err)
				}
				err = conn.Execute(query)
				AssertNil(err)
			}
		}
		log.Info("restoring.schema[%s]", name)
	}
}

func restoreTable(log *xlog.Log, table string, conn *Connection) int {
	bytes := 0
	part := "0"
	base := filepath.Base(table)
	name := strings.TrimSuffix(base, tableSuffix)
	splits := strings.Split(name, ".")
	db := splits[0]
	tbl := splits[1]
	if len(splits) > 2 {
		part = splits[2]
	}

	log.Info("restoring.tables[%s].parts[%s].thread[%d]", tbl, part, conn.ID)
	err := conn.Execute(fmt.Sprintf("USE `%s`", db))
	AssertNil(err)

	data, err := ReadFile(table)
	AssertNil(err)
	query1 := common.BytesToString(data)
	querys := strings.Split(query1, ";\n")
	bytes = len(query1)
	for _, query := range querys {
		if !strings.HasPrefix(query, "/*") && query != "" {
			err = conn.Execute(query)
			AssertNil(err)
		}
	}
	log.Info("restoring.tables[%s].parts[%s].thread[%d].done...", tbl, part, conn.ID)
	return bytes
}

// Loader used to start the loader worker.
func Loader(log *xlog.Log, args *Args) {
	pool, err := NewPool(log, args.Threads, args.Address, args.User, args.Password)
	AssertNil(err)
	defer pool.Close()

	files := loadFiles(log, args.Outdir)

	// database.
	conn := pool.Get()
	restoreDatabaseSchema(log, files.databases, conn)
	pool.Put(conn)

	// tables.
	conn = pool.Get()
	restoreTableSchema(log, args.OverwriteTables, files.schemas, conn)
	pool.Put(conn)

	// Shuffle the tables
	for i := range files.tables {
		j := rand.Intn(i + 1)
		files.tables[i], files.tables[j] = files.tables[j], files.tables[i]
	}

	var wg sync.WaitGroup
	var bytes uint64
	t := time.Now()
	for _, table := range files.tables {
		conn := pool.Get()
		wg.Add(1)
		go func(conn *Connection, table string) {
			defer func() {
				wg.Done()
				pool.Put(conn)
			}()
			r := restoreTable(log, table, conn)
			atomic.AddUint64(&bytes, uint64(r))
		}(conn, table)
	}

	tick := time.NewTicker(time.Millisecond * time.Duration(args.IntervalMs))
	defer tick.Stop()
	go func() {
		for range tick.C {
			diff := time.Since(t).Seconds()
			bytes := float64(atomic.LoadUint64(&bytes) / 1024 / 1024)
			rates := bytes / diff
			log.Info("restoring.allbytes[%vMB].time[%.2fsec].rates[%.2fMB/sec]...", bytes, diff, rates)
		}
	}()

	wg.Wait()
	elapsed := time.Since(t).Seconds()
	log.Info("restoring.all.done.cost[%.2fsec].allbytes[%.2fMB].rate[%.2fMB/s]", elapsed, float64(bytes/1024/1024), (float64(bytes/1024/1024) / elapsed))
}
