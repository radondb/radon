/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package xbase

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDiskUsage(t *testing.T) {
	disk, err := DiskUsage("/")
	assert.Nil(t, err)

	assert.True(t, disk.All > 0)
	assert.True(t, disk.Used > 0)
	assert.True(t, disk.Free > 0)
}
