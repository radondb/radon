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
	"os"
	"path"
	"testing"
	"time"
	"xbase"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

const (
	testMetadir = "_test_config"
	//versionJSONFile = "version.json"
)

func testRemoveMetadir() {
	os.RemoveAll(testMetadir)
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
	defer testRemoveMetadir()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	// Generate version file
	ts := time.Now().UnixNano()
	if err := genVersion(testMetadir, ts); err != nil {
		log.Error("config.version_test.genVersion.error:%+v", err)
	}

	// Read version.
	{
		ver := ReadVersion(testMetadir)
		assert.Equal(t, ver, ts)
	}

	// Update version.
	{
		err := UpdateVersion(testMetadir)
		assert.Nil(t, err)
	}

	// Read version.
	{
		ver := ReadVersion(testMetadir)
		assert.True(t, ver > ts)

		tsNow := time.Now().UnixNano()
		assert.True(t, ver < tsNow)
	}
}

func TestVersionError(t *testing.T) {
	defer testRemoveMetadir()

	// Update version.
	{
		err := UpdateVersion(testMetadir)
		assert.NotNil(t, err)
	}

	// Read version.
	{
		ver := ReadVersion(testMetadir)
		assert.Equal(t, int64(0), ver)
	}
}
