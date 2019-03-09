/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package plugins

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestPlugins(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))

	plugin := NewPlugin(log, nil, nil, nil)
	err := plugin.Init()
	assert.Nil(t, err)
	defer plugin.Close()

	autoincPlug := plugin.PlugAutoIncrement()
	assert.NotNil(t, autoincPlug)
}
