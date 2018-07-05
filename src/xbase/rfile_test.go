/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package xbase

import (
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

const (
	mockPrefix    = "xfiletest-"
	mockExtension = ".testlog"
)

func TestFileGetOldLogInfos4(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := getTmpDir("", "radon_xbase_", log)
	defer os.RemoveAll(tmpDir)

	xfile := NewRotateFile(tmpDir, mockPrefix, mockExtension, 1024*512)
	defer xfile.Close()

	for i := 0; i < 1024*64; i++ {
		datas := []byte("rotate.me....rotate.me....please...")
		n, err := xfile.Write(datas)
		assert.Nil(t, err)
		assert.Equal(t, len(datas), n)
	}
	err := xfile.Sync()
	assert.Nil(t, err)

	logInfos, err := xfile.GetOldLogInfos()
	assert.Nil(t, err)
	assert.Equal(t, 4, len(logInfos))

	// check the old files not contains the current file.
	xfile1 := xfile.(*rotateFile)
	curName := xfile1.name.Get()
	for _, info := range logInfos {
		assert.False(t, strings.Contains(curName, info.Name))
	}

	list := make([]string, 0, 10)
	filepath.Walk(tmpDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) == ".testlog" {
			list = append(list, path)
			os.Remove(path)
		}
		return nil
	})

	want := 5
	got := len(list)
	assert.Equal(t, want, got)
}

func TestFileGetOldLogInfos0(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := getTmpDir("", "radon_xbase_", log)
	defer os.RemoveAll(tmpDir)

	xfile := NewRotateFile(tmpDir, mockPrefix, mockExtension, 1024*512)
	defer xfile.Close()

	for i := 0; i < 1024; i++ {
		datas := []byte("rotate.me....rotate.me....please...")
		n, err := xfile.Write(datas)
		assert.Nil(t, err)
		assert.Equal(t, len(datas), n)
	}

	logInfos, err := xfile.GetOldLogInfos()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(logInfos))

	list := make([]string, 0, 10)
	filepath.Walk(tmpDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) == ".testlog" {
			list = append(list, path)
			os.Remove(path)
		}
		return nil
	})

	want := 1
	got := len(list)
	assert.Equal(t, want, got)
}

func TestFileGetCurrLogInfo(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := getTmpDir("", "radon_xbase_", log)
	defer os.RemoveAll(tmpDir)

	files := []string{
		"xfiletest-20171226140847.773.testlog",
		"xfiletest-20171226140846.772.testlog",
		"xfiletest-20171226140848.773.testlog",
		"xfiletest-20171226140846.770.testlog",
	}

	for _, file := range files {
		name := path.Join(tmpDir, file)
		f, err := os.OpenFile(name, os.O_RDONLY|os.O_CREATE, 0666)
		assert.Nil(t, err)
		f.Close()
	}

	xfile := NewRotateFile(tmpDir, mockPrefix, mockExtension, 1024*512)
	defer xfile.Close()

	info, err := xfile.GetCurrLogInfo(time.Now().UnixNano())
	assert.Nil(t, err)
	assert.Equal(t, files[2], info.Name)

	xfile1 := xfile.(*rotateFile)
	infos, err := xfile1.logInfos()
	assert.Nil(t, err)

	// sort by ts desc.
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].Ts > infos[j].Ts
	})

	// timestamp == info.Ts = "20171226140846.772",
	ts := infos[2].Ts
	info, err = xfile.GetCurrLogInfo(ts)
	assert.Nil(t, err)
	assert.Equal(t, infos[2].Name, info.Name)

	// timestamp == (info.Ts + 2seconds) = "20171226140848.772"
	ts = time.Unix(0, infos[2].Ts).Add(time.Second * 2).UnixNano()
	info, err = xfile.GetCurrLogInfo(ts)
	assert.Nil(t, err)
	assert.Equal(t, files[0], info.Name)

	// timestamp == (info.Ts - 2seconds) = "20171226140844.772"
	ts = time.Unix(0, infos[2].Ts).Add(time.Second * -2).UnixNano()
	info, err = xfile.GetCurrLogInfo(ts)
	assert.Nil(t, err)
	assert.Equal(t, files[3], info.Name)
}

func TestFileGetNextLogInfo(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := getTmpDir("", "radon_xbase_", log)
	defer os.RemoveAll(tmpDir)

	files := []string{
		"xfiletest-20171226140847.773.testlog",
		"xfiletest-20171226140846.772.testlog",
		"xfiletest-20171226140848.773.testlog",
		"xfiletest-20171226140846.770.testlog",
	}

	for _, file := range files {
		name := path.Join(tmpDir, file)
		f, err := os.OpenFile(name, os.O_RDONLY|os.O_CREATE, 0666)
		assert.Nil(t, err)
		f.Close()
	}

	xfile := NewRotateFile(tmpDir, mockPrefix, mockExtension, 1024*512)
	defer xfile.Close()

	// Next should be "xfiletest-20171226140846.772.testlog".
	info, err := xfile.GetNextLogInfo("xfiletest-20171226140846.770.testlog")
	assert.Nil(t, err)
	assert.Equal(t, files[1], info.Name)

	// Next should be "xfiletest-20171226140846.770.testlog".
	info, err = xfile.GetNextLogInfo("xfiletest-20171226140844.770.testlog")
	assert.Nil(t, err)
	assert.Equal(t, files[3], info.Name)

	// Next should be "xfiletest-20171226140846.770.testlog".
	info, err = xfile.GetNextLogInfo("/tmp/logtest/xfiletest-20171226140844.770.testlog")
	assert.Nil(t, err)
	assert.Equal(t, files[3], info.Name)

	// Next should be "".
	info, err = xfile.GetNextLogInfo("xfiletest-20171226140848.775.testlog")
	assert.Nil(t, err)
	assert.Equal(t, "", info.Name)

	// Next should be the first.
	info, err = xfile.GetNextLogInfo("")
	assert.Nil(t, err)
	assert.Equal(t, files[3], info.Name)

	// Parse the time error.
	info, err = xfile.GetNextLogInfo("xfiletest-20171226140848.775.test")
	assert.NotNil(t, err)
}

func TestFileGetNextLogInfoWithEmpty(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	tmpDir := getTmpDir("", "radon_xbase_", log)
	defer os.RemoveAll(tmpDir)

	xfile := NewRotateFile(tmpDir, mockPrefix, mockExtension, 1024*512)
	defer xfile.Close()

	// Next should be "".
	info, err := xfile.GetNextLogInfo("")
	assert.Nil(t, err)
	assert.Equal(t, "", info.Name)
	xfile.Name()
}
