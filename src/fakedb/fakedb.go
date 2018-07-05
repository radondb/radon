/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package fakedb

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	"config"

	"github.com/xelabs/go-mysqlstack/driver"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	// Result1 result.
	Result1 = &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
			{
				Name: "name",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("11")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("1nice name")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("12")),
				sqltypes.NULL,
			},
		},
	}

	// Result2 result.
	Result2 = &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "id",
				Type: querypb.Type_INT32,
			},
			{
				Name: "name",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("21")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("2nice name")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("22")),
				sqltypes.NULL,
			},
		},
	}

	// Result3 result.
	Result3 = &sqltypes.Result{}
)

// GetTmpDir used to create a test tmp dir
// dir: path specified, can be an empty string
// module: the name of test module
func GetTmpDir(dir, module string, log *xlog.Log) string {
	tmpDir := ""
	var err error
	if dir == "" {
		tmpDir, err = ioutil.TempDir(os.TempDir(), module)
		if err != nil {
			log.Error("%v.test.can't.create.temp.dir.in:[%v]", module, os.TempDir())
		}
	} else {
		tmpDir, err = ioutil.TempDir(dir, module)
		if err != nil {
			log.Error("%v.test.can't.create.temp.dir.in:[%v]", module, dir)
		}
	}
	return tmpDir
}

// DB is a fake database.
type DB struct {
	log          *xlog.Log
	mu           sync.RWMutex
	handler      *driver.TestHandler
	listeners    []*driver.Listener
	backendconfs []*config.BackendConfig
	addrs        []string
}

// New creates a new DB.
func New(log *xlog.Log, n int) *DB {
	th := driver.NewTestHandler(log)
	listeners := make([]*driver.Listener, 0, 8)
	addrs := make([]string, 0, 8)
	backendconfs := make([]*config.BackendConfig, 0, 8)
	for i := 0; i < n; i++ {
		l, err := driver.MockMysqlServer(log, th)
		if err != nil {
			panic(err)
		}
		conf := &config.BackendConfig{
			Name:           fmt.Sprintf("backend%d", i),
			Address:        l.Addr(),
			User:           "mock",
			Password:       "pwd",
			DBName:         "sbtest",
			Charset:        "utf8",
			MaxConnections: 1024,
		}
		backendconfs = append(backendconfs, conf)
		addrs = append(addrs, l.Addr())
		listeners = append(listeners, l)
	}
	db := &DB{
		log:          log,
		handler:      th,
		addrs:        addrs,
		listeners:    listeners,
		backendconfs: backendconfs,
	}
	// Add mock/mock user to mysql.user table.
	db.addMockUser()
	return db
}

// Addrs used to get all address of the server.
func (db *DB) Addrs() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.addrs
}

// BackendConfs used to get all backend configs.
func (db *DB) BackendConfs() []*config.BackendConfig {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.backendconfs
}

// Close used to close all the listeners.
func (db *DB) Close() {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, l := range db.listeners {
		l.Close()
	}
}

// AddQuery used to add a query and the return result expected.
func (db *DB) AddQuery(query string, result *sqltypes.Result) {
	db.handler.AddQuery(query, result)
}

// AddQuerys used to add a query and the return results expected.
func (db *DB) AddQuerys(query string, result ...*sqltypes.Result) {
	db.handler.AddQuerys(query, result...)
}

// AddQueryStream used to add a query and the streamly return result expected.
func (db *DB) AddQueryStream(query string, result *sqltypes.Result) {
	db.handler.AddQueryStream(query, result)
}

// AddQueryDelay used to add query and return by delay.
func (db *DB) AddQueryDelay(query string, result *sqltypes.Result, delayMS int) {
	db.handler.AddQueryDelay(query, result, delayMS)
}

// AddQueryError use to add a query and return the error expected.
func (db *DB) AddQueryError(query string, err error) {
	db.handler.AddQueryError(query, err)
}

// AddQueryPanic used to add the query with panic.
func (db *DB) AddQueryPanic(query string) {
	db.handler.AddQueryPanic(query)
}

// AddQueryPattern used to add an expected result for a set of queries.
func (db *DB) AddQueryPattern(qp string, result *sqltypes.Result) {
	db.handler.AddQueryPattern(qp, result)
}

// AddQueryErrorPattern use to add a query and return the error expected.
func (db *DB) AddQueryErrorPattern(qp string, err error) {
	db.handler.AddQueryErrorPattern(qp, err)
}

// GetQueryCalledNum returns how many times db executes a certain query.
func (db *DB) GetQueryCalledNum(query string) int {
	return db.handler.GetQueryCalledNum(query)
}

// ResetAll will reset all, including: query and query patterns.
func (db *DB) ResetAll() {
	db.handler.ResetAll()
}

// ResetPatternErrors used to reset all the error pattern.
func (db *DB) ResetPatternErrors() {
	db.handler.ResetPatternErrors()
}

// ResetErrors used to reset all the errors.
func (db *DB) ResetErrors() {
	db.handler.ResetErrors()
}

// addMockUser adds mock/mock user to mysql.user table.
func (db *DB) addMockUser() {
	r1 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "authentication_string ",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("*CC86C0D547DE7603129BC1D3B98DB2242E7F744F")),
			},
		},
	}
	db.AddQuery("select authentication_string from mysql.user where user='mock'", r1)
}
