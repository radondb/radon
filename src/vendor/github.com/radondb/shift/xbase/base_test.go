/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package xbase

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

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

func TestXbaseWriteFile(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := getTmpDir("", "radon_backend_", log)
	defer os.RemoveAll(tmpDir)
	file := path.Join(tmpDir, "xbase.test")

	// Write OK.
	{
		err := WriteFile(file, []byte{0xfd})
		assert.Nil(t, err)
	}

	// Write Error.
	{
		badFile := "/xx/xbase.test"
		err := WriteFile(badFile, []byte{0xfd})
		assert.NotNil(t, err)
	}
}

func TestXbaseTruncateQuery(t *testing.T) {
	var testCases = []struct {
		in, out string
	}{{
		in:  "",
		out: "",
	}, {
		in:  "12345",
		out: "12345",
	}, {
		in:  "123456",
		out: "12345 [TRUNCATED]",
	}}
	for _, testCase := range testCases {
		got := TruncateQuery(testCase.in, 5)
		assert.Equal(t, testCase.out, got)
	}
}
