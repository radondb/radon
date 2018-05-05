/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestDiskCheck(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	dc := NewDiskCheck(log, "/tmp/")
	err := dc.Init()
	assert.Nil(t, err)
	defer dc.Close()

	dc.doCheck()
	high := dc.HighWater()
	assert.False(t, high)
}
