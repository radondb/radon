/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package xbase

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"xbase/sync2"
)

const (
	fileFormat = "20060102150405.000"
)

var (
	_ RotateFile = &rotateFile{}
)

// RotateFile interface.
type RotateFile interface {
	Write(b []byte) (int, error)
	Sync() error
	Close()
	Name() string
	GetOldLogInfos() ([]LogInfo, error)
	GetNextLogInfo(logName string) (LogInfo, error)
	GetCurrLogInfo(ts int64) (LogInfo, error)
}

type rotateFile struct {
	size      int
	max       int
	file      *os.File
	name      sync2.AtomicString
	dir       string
	prefix    string
	extension string
}

// NewRotateFile creates a new rotateFile.
func NewRotateFile(dir string, prefix string, extension string, maxSize int) RotateFile {
	return &rotateFile{
		max:       maxSize,
		dir:       dir,
		prefix:    prefix,
		extension: extension,
	}
}

func (f *rotateFile) openNew() error {
	t := time.Now().UTC()
	timestamp := t.Format(fileFormat)
	next := filepath.Join(f.dir, fmt.Sprintf("%s%s%s", f.prefix, timestamp, f.extension))
	f.name.Set(next)

	cur, err := os.OpenFile(next, os.O_CREATE|os.O_WRONLY, os.FileMode(0644))
	if err != nil {
		return err
	}
	f.file = cur
	f.size = 0
	return nil
}

func (f *rotateFile) rotate() error {
	if err := f.file.Sync(); err != nil {
		return err
	}
	if err := f.file.Close(); err != nil {
		return err
	}
	return f.openNew()
}

// Name returns the current writing file base name.
func (f *rotateFile) Name() string {
	return path.Base(f.name.Get())
}

// Write used to writes datas to file.
func (f *rotateFile) Write(b []byte) (int, error) {
	if f.file == nil {
		f.openNew()
	}
	n, err := f.file.Write(b)
	if err != nil {
		return n, err
	}
	f.size += n

	if f.size > f.max {
		if err := f.rotate(); err != nil {
			return n, err
		}
	}
	return n, nil
}

// Sync used to sync the file.
func (f *rotateFile) Sync() error {
	return f.file.Sync()
}

// Close used to close the file.
func (f *rotateFile) Close() {
	if f.file != nil {
		f.file.Close()
		f.file = nil
	}
}

// LogInfo tuple.
type LogInfo struct {
	Name string
	// Ts is the timestamp with UTC().UnixNano.
	Ts int64
}

func (f *rotateFile) logInfos() ([]LogInfo, error) {
	infos := make([]LogInfo, 0, 64)
	files, err := ioutil.ReadDir(f.dir)
	if err != nil {
		return infos, err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if filepath.Ext(file.Name()) == f.extension {
			name := strings.TrimSuffix(strings.TrimPrefix(file.Name(), f.prefix), f.extension)
			t, err := time.Parse(fileFormat, name)
			if err != nil {
				continue
			}
			infos = append(infos, LogInfo{
				Name: file.Name(),
				Ts:   t.UnixNano(),
			})
		}
	}
	return infos, nil
}

// GetOldLogInfos returns all the files except the current writing file.
func (f *rotateFile) GetOldLogInfos() ([]LogInfo, error) {
	infos, err := f.logInfos()
	if err != nil {
		return nil, err
	}

	// sort by ts asc.
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].Ts < infos[j].Ts
	})

	if len(infos) > 0 {
		return infos[:len(infos)-1], nil
	}
	return infos, nil
}

// GetCurrLogInfo returns the last log file which ts >= log.Ts.
// If when ts < log.Ts, returns the last LogInfo.
// ts is the UTC().UnixNano() tiemstamp.
func (f *rotateFile) GetCurrLogInfo(ts int64) (LogInfo, error) {
	info := LogInfo{}
	infos, err := f.logInfos()
	if err != nil {
		return info, err
	}

	// sort by ts desc.
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].Ts > infos[j].Ts
	})

	i := sort.Search(len(infos), func(i int) bool { return infos[i].Ts <= ts })
	if len(infos) != i {
		info = infos[i]
	}

	// Return the last log if ts < last.Ts.
	if info.Name == "" && len(infos) > 0 {
		lastIdx := len(infos) - 1
		last := infos[lastIdx]
		if last.Ts > ts {
			return last, nil
		}
	}
	return info, nil
}

// GetNextLogInfo return the first log file which LogInfo.Ts > ts.
func (f *rotateFile) GetNextLogInfo(logName string) (LogInfo, error) {
	info := LogInfo{}

	infos, err := f.logInfos()
	if err != nil {
		return info, err
	}

	// sort by ts asc.
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].Ts < infos[j].Ts
	})

	// logName is "".
	if logName == "" && len(infos) > 0 {
		return infos[0], nil
	}

	// no logs.
	logName = path.Base(logName)
	if logName == "." {
		return info, nil
	}

	// Get the ts from the logname.
	name := strings.TrimSuffix(strings.TrimPrefix(logName, f.prefix), f.extension)
	t, err := time.Parse(fileFormat, name)
	if err != nil {
		return info, err
	}
	ts := t.UnixNano()
	i := sort.Search(len(infos), func(i int) bool { return infos[i].Ts > ts })
	if len(infos) != i {
		info = infos[i]
	}
	return info, nil
}
