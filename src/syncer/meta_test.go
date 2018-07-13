/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package syncer

import (
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"config"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

const (
	testMetadir = "_test_syncer/radon_syncer_meta_test"
	testDirRm   = "_test_syncer"
)

func testRemoveMetadir() {
	os.RemoveAll(testDirRm)
}

func TestMeta(t *testing.T) {
	defer testRemoveMetadir()

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	syncer := NewSyncer(log, testMetadir, "", nil, nil)
	assert.NotNil(t, syncer)

	err := syncer.Init()
	assert.Nil(t, err)

	meta := &Meta{
		Metas: make(map[string]string),
	}

	// Rebuild.
	{
		meta.Metas["backends.json"] = ("backends.json")
		meta.Metas["version.json"] = ("12345")
		meta.Metas["sbtest/t1.json"] = ("t1.json")
		meta.Metas["sbtest/t2.json"] = ("t2.json")
		meta.Metas["sbtest/t3.json"] = ("t2.json")
		syncer.MetaRebuild(meta)

		hasBackup := false
		checkFiles := func(p string, f os.FileInfo, err error) (e error) {
			if strings.HasPrefix(f.Name(), "_backup_radon") {
				hasBackup = true
				return
			}
			return
		}
		filepath.Walk(path.Dir(testMetadir), checkFiles)
		assert.True(t, hasBackup)
	}

	// MetaJson.
	{
		got, err := syncer.MetaJSON()
		assert.Nil(t, err)
		assert.Equal(t, meta, got)
	}

	// MetaVersion.
	{
		ver := syncer.MetaVersion()
		assert.True(t, ver == 0)
	}
}

func TestMetaError(t *testing.T) {
	defer testRemoveMetadir()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	syncer := NewSyncer(log, testMetadir, "", nil, nil)
	assert.NotNil(t, syncer)

	// MetaJson.
	{
		_, err := syncer.MetaJSON()
		assert.NotNil(t, err)
	}
}

func TestMetaFileError(t *testing.T) {
	defer testRemoveMetadir()
	file := "/xx/radon_syncer_meta_test.xx"
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	{
		_, err := readFile(log, file)
		assert.NotNil(t, err)
	}

	{
		err := writeFile(log, file, "")
		assert.NotNil(t, err)
	}
}

func TestMetaVersionCheck(t *testing.T) {
	defer testRemoveMetadir()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	syncers, cleanup := mockSyncer(log, 3)
	assert.NotNil(t, syncers)
	defer cleanup()

	syncer0 := syncers[0]
	syncer2 := syncers[2]
	config.UpdateVersion(syncer2.metadir)
	checked, _ := syncer0.MetaVersionCheck()
	assert.False(t, checked)

	time.Sleep(time.Second * 2)
	checked, _ = syncer0.MetaVersionCheck()
	assert.True(t, checked)
}
