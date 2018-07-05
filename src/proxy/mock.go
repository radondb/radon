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

	"github.com/xelabs/go-mysqlstack/xlog"
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
		Proxy:  config.DefaultProxyConfig(),
		Audit:  config.DefaultAuditConfig(),
		Router: config.DefaultRouterConfig(),
		Binlog: config.DefaultBinlogConfig(),
		Log:    config.DefaultLogConfig(),
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

// MockProxyWithBackup mocks the proxy with backup.
func MockProxyWithBackup(log *xlog.Log) (*fakedb.DB, *Proxy, func()) {
	conf := MockDefaultConfig()
	conf.Binlog.RelayWaitMs = 100
	conf.Binlog.EnableBinlog = true
	conf.Binlog.EnableRelay = true
	os.RemoveAll(conf.Binlog.LogDir)
	return MockProxy2(log, conf)
}

// MockProxy1 mocks the proxy with config.
func MockProxy1(log *xlog.Log, conf *config.Config) (*fakedb.DB, *Proxy, func()) {
	tmpDir := fakedb.GetTmpDir("", "radon_mock_", log)

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
	mockJson := tmpDir + "/radon_mock.json"
	proxy := NewProxy(log, mockJson, conf)
	proxy.Start()
	return fakedbs, proxy, func() {
		proxy.Stop()
		fakedbs.Close()
		os.RemoveAll(tmpDir)
	}
}

// MockProxy2 mocks the proxy with the conf.
func MockProxy2(log *xlog.Log, conf *config.Config) (*fakedb.DB, *Proxy, func()) {
	tmpDir := fakedb.GetTmpDir("", "radon_mock_", log)
	// Fake backends.
	fakedbs := fakedb.New(log, 5)

	port := randomPort(15000, 20000)
	addr := fmt.Sprintf(":%d", port)

	conf.Proxy.Endpoint = addr
	metaDir := tmpDir + "/test_radonmeta_"
	conf.Proxy.MetaDir = metaDir

	os.RemoveAll(metaDir)
	if x := os.MkdirAll(metaDir, 0777); x != nil {
		log.Panic("%+v", x)
	}

	backends := fakedbs.BackendConfs()
	backendLen := len(backends)
	backendsConf := &config.BackendsConfig{
		Backends: backends[0 : backendLen-1],
		Backup:   backends[backendLen-1],
	}
	if err := config.WriteConfig(path.Join(conf.Proxy.MetaDir, "backend.json"), backendsConf); err != nil {
		log.Panic("mock.proxy.write.backends.config.error:%+v", err)
	}

	// Proxy.
	mockJson := tmpDir + "/radon_mock.json"
	proxy := NewProxy(log, mockJson, conf)
	proxy.Start()
	return fakedbs, proxy, func() {
		proxy.Stop()
		fakedbs.Close()
		os.RemoveAll(tmpDir)
	}
}
