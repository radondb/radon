/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package xbase

import (
	"syscall"
)

// DiskStatus tuple.
type DiskStatus struct {
	All  uint64
	Used uint64
	Free uint64
}

// DiskUsage returns the disk info of the path.
func DiskUsage(path string) (*DiskStatus, error) {
	disk := &DiskStatus{}
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &fs)
	if err != nil {
		return nil, err
	}
	disk.All = fs.Blocks * uint64(fs.Bsize)
	disk.Free = fs.Bavail * uint64(fs.Bsize)
	disk.Used = disk.All - disk.Free
	return disk, nil
}
