/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"fmt"
	"math/rand"
	"os"
	"path"
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

	autocommitResult1 = &sqltypes.Result{
		RowsAffected: 5,
		Fields: []*querypb.Field{
			{
				Name: "@@autocommit",
				Type: querypb.Type_INT64,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte("1")),
			},
		},
	}
)

func randomPort(min int, max int) int {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	d, delta := min, (max - min)
	if delta > 0 {
		d += rand.Intn(int(delta))
	}
	return d
}

// MockDefaultConfig mocks the default config.
func MockDefaultConfig() *config.Config {
	conf := &config.Config{
		Proxy:   config.DefaultProxyConfig(),
		Audit:   config.DefaultAuditConfig(),
		Router:  config.DefaultRouterConfig(),
		Log:     config.DefaultLogConfig(),
		Scatter: config.DefaultScatterConfig(),
	}
	return conf
}

// MockConfigMax16 mocks the config with MaxConnections=16.
func MockConfigMax16() *config.Config {
	conf := MockDefaultConfig()
	conf.Proxy.IPS = []string{"127.0.0.2"}
	conf.Proxy.MetaDir = "/tmp/test_radonmeta"
	conf.Proxy.TwopcEnable = false
	conf.Proxy.Endpoint = "127.0.0.1:3306"
	conf.Proxy.MaxConnections = 16
	conf.Proxy.MaxResultSize = 1024 * 1024 * 1024 // 1GB
	conf.Proxy.DDLTimeout = 10 * 3600 * 1000      // 10 hours
	conf.Proxy.QueryTimeout = 5 * 60 * 1000       // 5 minutes
	conf.Log = &config.LogConfig{
		Level: "ERROR",
	}
	return conf
}

// MockProxy mocks a proxy.
func MockProxy(log *xlog.Log) (*fakedb.DB, *Proxy, func()) {
	return MockProxy1(log, MockDefaultConfig())
}

// MockProxy1 mocks the proxy with config.
func MockProxy1(log *xlog.Log, conf *config.Config) (*fakedb.DB, *Proxy, func()) {
	tmpDir := fakedb.GetTmpDir("", "radon_mock_", log)

	// set Blocks 128
	conf.Router.Blocks = 128
	// Fake backends.
	fakedbs := fakedb.New(log, 5)

	port := randomPort(15000, 20000)
	addr := fmt.Sprintf(":%d", port)

	conf.Proxy.Endpoint = addr

	fileFormat := "20060102150405.000"
	t := time.Now().UTC()
	timestamp := t.Format(fileFormat)
	metaDir := tmpDir + "/test_radonmeta_" + timestamp
	conf.Proxy.MetaDir = metaDir

	if x := os.MkdirAll(metaDir, 0777); x != nil {
		log.Panic("%+v", x)
	}

	backendsConf := &config.BackendsConfig{Backends: fakedbs.BackendConfs()}
	if err := config.WriteConfig(path.Join(conf.Proxy.MetaDir, "backend.json"), backendsConf); err != nil {
		log.Panic("mock.proxy.write.backends.config.error:%+v", err)
	}

	// Proxy.
	mockJSON := tmpDir + "/radon_mock.json"
	proxy := NewProxy(log, mockJSON, conf)
	proxy.Start()
	return fakedbs, proxy, func() {
		proxy.Stop()
		fakedbs.Close()
		os.RemoveAll(tmpDir)
	}
}
