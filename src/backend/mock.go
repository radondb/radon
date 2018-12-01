/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package backend

import (
	"fmt"
	"time"

	"config"
	"fakedb"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	result1 = &sqltypes.Result{
		RowsAffected: 2,
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
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("12nice name")),
			},
		},
	}
	result2 = &sqltypes.Result{
		RowsAffected: 2,
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
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("22nice name")),
			},
		},
	}
)

// MockBackendConfigDefault mocks new backend config.
func MockBackendConfigDefault(name, addr string) *config.BackendConfig {
	return &config.BackendConfig{
		Name:           name,
		Address:        addr,
		User:           "mock",
		Password:       "pwd",
		DBName:         "sbtest",
		Charset:        "utf8",
		MaxConnections: 1024,
	}
}

// MockScatterDefault mocks new xacheck config.
func MockScatterDefault(log *xlog.Log) *config.ScatterConfig {
	dir := fakedb.GetTmpDir("/tmp", "xacheck", log)
	return &config.ScatterConfig{
		XaCheckInterval: 1,
		XaCheckDir:      dir,
	}
}

// MockScatterDefault2 mocks new xacheck config with dir.
func MockScatterDefault2(dir string) *config.ScatterConfig {
	return &config.ScatterConfig{
		XaCheckInterval: 1,
		XaCheckDir:      dir,
	}
}

// MockScatter used to mock a scatter.
func MockScatter(log *xlog.Log, n int) (*Scatter, *fakedb.DB, func()) {
	scatter := NewScatter(log, "")
	fakedb := fakedb.New(log, n)
	backends := make(map[string]*Pool)
	addrs := fakedb.Addrs()
	for i, addr := range addrs {
		name := fmt.Sprintf("backend%d", i)
		conf := MockBackendConfigDefault(name, addr)
		pool := NewPool(log, conf)
		backends[name] = pool
	}
	scatter.backends = backends

	return scatter, fakedb, func() {
		fakedb.Close()
		scatter.Close()
	}
}

// MockClient mocks a client connection.
func MockClient(log *xlog.Log, addr string) (Connection, func()) {
	return MockClientWithConfig(log, MockBackendConfigDefault("", addr))
}

// MockClientWithConfig mocks a client with backendconfig.
func MockClientWithConfig(log *xlog.Log, conf *config.BackendConfig) (Connection, func()) {
	pool := NewPool(log, conf)
	conn := NewConnection(log, pool)
	if err := conn.Dial(); err != nil {
		log.Panic("mock.conn.with.config.error:%+v", err)
	}
	return conn, func() {
		pool.Close()
	}
}

// MockTxnMgr mocks txn manager.
func MockTxnMgr(log *xlog.Log, n int) (*fakedb.DB, *TxnManager, map[string]*Pool, []string, func()) {
	fakedb := fakedb.New(log, n+1)
	backends := make(map[string]*Pool)
	addrs := fakedb.Addrs()
	for i := 0; i < len(addrs)-1; i++ {
		addr := addrs[i]
		conf := MockBackendConfigDefault(addr, addr)
		pool := NewPool(log, conf)
		backends[addr] = pool
	}

	txnMgr := NewTxnManager(log)
	return fakedb, txnMgr, backends, addrs, func() {
		time.Sleep(time.Millisecond * 10)
		for _, v := range backends {
			v.Close()
		}
		fakedb.Close()
	}
}

// MockTxnMgrScatter used to mock a txnMgr and a scatter.
// commit err and rollback err will WriteXaCommitErrLog, need the scatter
func MockTxnMgrScatter(log *xlog.Log, n int) (*fakedb.DB, *TxnManager, map[string]*Pool, []string, *Scatter, func()) {
	scatter := NewScatter(log, "")
	fakedb := fakedb.New(log, n)
	backends := make(map[string]*Pool)
	addrs := fakedb.Addrs()
	for _, addr := range addrs {
		conf := MockBackendConfigDefault(addr, addr)
		pool := NewPool(log, conf)
		backends[addr] = pool
	}
	scatter.backends = backends

	txnMgr := scatter.txnMgr
	return fakedb, txnMgr, backends, addrs, scatter, func() {
		fakedb.Close()
		scatter.Close()
	}
}
