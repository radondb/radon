/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package router

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestSingle(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	single := NewSingle(log, MockTableSConfig())
	{
		err := single.Build()
		assert.Nil(t, err)
		assert.Equal(t, string(single.Type()), methodTypeSingle)
	}

	{
		parts, err := single.Lookup(nil, nil)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(parts))
	}
}
