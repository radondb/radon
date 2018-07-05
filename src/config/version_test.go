/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"xbase"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// avoid that import cycle with fakedb
// getTmpDir used to create and get a test tmp dir
// dir: path specified, can be an empty string
// module: the name of test module
func getTmpDir(dir, module string, log *xlog.Log) string {
	tmpDir := ""
	var err error
	if dir == "" {
		tmpDir, err = ioutil.TempDir(os.TempDir(), module)
		if err != nil {
			log.Error("%v.test.can't.create.temp.dir.in:%v", module, os.TempDir())
		}
	} else {
		tmpDir, err = ioutil.TempDir(dir, module)
		if err != nil {
			log.Error("%v.test.can't.create.temp.dir.in:%v", module, dir)
		}
	}
	return tmpDir
}

// Generate version file
func genVersion(metadir string, ts int64) error {
	if err := os.MkdirAll(metadir, os.ModePerm); err != nil {
		return err
	}
	name := path.Join(metadir, versionJSONFile)
	version := &Version{
		Ts: ts,
	}
	b, err := json.Marshal(version)
	if err != nil {
		return err
	}
	return xbase.WriteFile(name, b)
}

func TestVersion(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := getTmpDir("", "radon_config_", log)
	defer os.RemoveAll(tmpDir)

	// Generate version file
	ts := time.Now().UnixNano()
	if err := genVersion(tmpDir, ts); err != nil {
		log.Error("config.version_test.genVersion.error:%+v", err)
	}

	// Read version.
	{
		ver := ReadVersion(tmpDir)
		assert.Equal(t, ver, ts)
	}

	// Update version.
	{
		err := UpdateVersion(tmpDir)
		assert.Nil(t, err)
	}

	// Read version.
	{
		ver := ReadVersion(tmpDir)
		assert.True(t, ver > ts)

		tsNow := time.Now().UnixNano()
		assert.True(t, ver < tsNow)
	}
}

func TestVersionError(t *testing.T) {
	// Update version.
	{
		err := UpdateVersion("xxx")
		assert.NotNil(t, err)
	}

	// Read version.
	{
		ver := ReadVersion("xxx")
		assert.Equal(t, int64(0), ver)
	}
}
