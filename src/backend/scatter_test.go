/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package backend

import (
	"os"
	"testing"

	"fakedb"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestScatterAddRemove(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_backend_", log)
	defer os.RemoveAll(tmpDir)

	scatter := NewScatter(log, tmpDir)
	fakedb := fakedb.New(log, 2)
	defer fakedb.Close()
	addrs := fakedb.Addrs()
	config1 := MockBackendConfigDefault("node1", addrs[0])

	// add
	{
		err := scatter.Add(config1)
		assert.Nil(t, err)
	}

	// duplicate
	{
		err := scatter.Add(config1)
		assert.NotNil(t, err)
	}

	// duplicate address
	{
		config2 := MockBackendConfigDefault("node2", addrs[0])
		err := scatter.Add(config2)
		assert.NotNil(t, err)
	}

	// remove
	{
		err := scatter.Remove(config1)
		assert.Nil(t, err)
	}

	// remove again
	{
		err := scatter.Remove(config1)
		assert.NotNil(t, err)
	}

	// flush config
	{
		err := scatter.FlushConfig()
		assert.Nil(t, err)
	}

	// load config
	{
		err := scatter.LoadConfig()
		assert.Nil(t, err)
	}
}

func TestScatterLoadConfig(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_backend_", log)
	defer os.RemoveAll(tmpDir)

	scatter := NewScatter(log, tmpDir)
	fakedb := fakedb.New(log, 2)
	defer fakedb.Close()
	addrs := fakedb.Addrs()
	config1 := MockBackendConfigDefault("node1", addrs[0])
	config2 := MockBackendConfigDefault("node2", addrs[1])

	// add config1.
	{
		err := scatter.Add(config1)
		assert.Nil(t, err)
	}

	// add config2.
	{
		err := scatter.Add(config2)
		assert.Nil(t, err)
	}

	// flush config.
	{
		err := scatter.FlushConfig()
		assert.Nil(t, err)
	}

	// load config.
	{
		want := scatter.backends["node1"].conf
		err := scatter.LoadConfig()
		assert.Nil(t, err)
		got := scatter.backends["node1"].conf
		assert.Equal(t, want, got)
	}

	// load config again.
	{
		want := scatter.backends["node2"].conf
		err := scatter.LoadConfig()
		assert.Nil(t, err)
		got := scatter.backends["node2"].conf
		assert.Equal(t, want, got)
	}
}

func TestScatter(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_backend_", log)
	defer os.RemoveAll(tmpDir)

	scatter := NewScatter(log, tmpDir)
	defer scatter.Close()

	fakedb := fakedb.New(log, 2)
	defer fakedb.Close()
	addrs := fakedb.Addrs()

	// add
	{
		config1 := MockBackendConfigDefault("node1", addrs[0])
		err := scatter.add(config1)
		assert.Nil(t, err)
	}
	// backends
	{
		backends := scatter.Backends()
		assert.Equal(t, "node1", backends[0])
	}

	// CheckBackend
	{
		isExist := scatter.CheckBackend("node1")
		assert.Equal(t, true, isExist)

		isExist = scatter.CheckBackend("node0")
		assert.Equal(t, false, isExist)
	}

	// pool clone.
	{
		clone := scatter.PoolzClone()
		assert.Equal(t, clone["node1"], scatter.backends["node1"])
	}

	// backends config clone.
	{
		clone := scatter.BackendConfigsClone()
		assert.Equal(t, clone[0], scatter.backends["node1"].conf)
	}

	// create txn.
	{
		_, err := scatter.CreateTransaction()
		assert.Nil(t, err)
	}
}

func TestScatterLoadNotExists(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_backend_", log)
	defer os.RemoveAll(tmpDir)

	scatter := NewScatter(log, tmpDir)
	err := scatter.LoadConfig()
	assert.Nil(t, err)
}

func TestScatterNormalBackends(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_backend_", log)
	defer os.RemoveAll(tmpDir)

	scatter := NewScatter(log, tmpDir)
	defer scatter.Close()

	fakedb := fakedb.New(log, 2)
	defer fakedb.Close()
	addrs := fakedb.Addrs()

	// add
	{
		config1 := MockBackendConfigDefault("node1", addrs[0])
		err := scatter.add(config1)
		assert.Nil(t, err)
	}
	// backends
	{
		backends := scatter.Backends()
		assert.Equal(t, "node1", backends[0])
	}

	// add
	{
		config1 := MockBackendConfigAttach("node2", addrs[1])
		err := scatter.add(config1)
		assert.Nil(t, err)
	}
	// backends
	{
		backends := scatter.Backends()
		assert.Equal(t, "node1", backends[0])
	}
}
