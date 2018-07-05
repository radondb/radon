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

func TestScatterAddRemoveBackup(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := fakedb.GetTmpDir("", "radon_backend_", log)
	defer os.RemoveAll(tmpDir)

	scatter := NewScatter(log, tmpDir)
	fakedb := fakedb.New(log, 2)
	defer fakedb.Close()
	addrs := fakedb.Addrs()

	config1 := MockBackendConfigDefault("node1", addrs[0])
	backup1 := MockBackendConfigDefault("backup", addrs[1])

	// add normal.
	{
		err := scatter.Add(config1)
		assert.Nil(t, err)
		assert.Equal(t, false, scatter.HasBackup())
	}

	// add backup.
	{
		err := scatter.AddBackup(backup1)
		assert.Nil(t, err)
	}

	// add backup again.
	{
		err := scatter.AddBackup(backup1)
		assert.NotNil(t, err)
	}

	// backup name.
	{
		got := scatter.BackupBackend()
		want := "backup"
		assert.Equal(t, want, got)
	}

	// backup pool.
	{
		got := scatter.BackupPool()
		want := scatter.backup
		assert.Equal(t, want, got)
	}

	// backup config.
	{
		got := scatter.BackupConfig()
		want := scatter.backup.conf
		assert.Equal(t, want, got)
	}

	// flush config
	{
		err := scatter.FlushConfig()
		assert.Nil(t, err)
	}

	// remove backup.
	{
		err := scatter.RemoveBackup(backup1)
		assert.Nil(t, err)
	}

	// remove backup again.
	{
		err := scatter.RemoveBackup(backup1)
		assert.NotNil(t, err)
	}

	// remove config1.
	{
		err := scatter.Remove(config1)
		assert.Nil(t, err)
	}

	// load config
	{
		err := scatter.LoadConfig()
		assert.Nil(t, err)
		assert.Equal(t, backup1, scatter.backup.conf)
		assert.Equal(t, true, scatter.HasBackup())
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

	// pool clone.
	{
		clone := scatter.PoolClone()
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
