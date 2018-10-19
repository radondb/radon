/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package cmd

import (
	"testing"
	"time"

	"ctl"
	"proxy"

	"github.com/stretchr/testify/assert"
)

func TestCmdReadOnly(t *testing.T) {
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	admin := ctl.NewAdmin(log, proxy)
	admin.Start()
	defer admin.Stop()
	time.Sleep(100 * time.Nanosecond)

	// enable.
	{
		cmd := NewReadonlyCommand()
		_, err := executeCommand(cmd, "enable")
		assert.Nil(t, err)
		_, err = executeCommand(cmd, "enable", "--radon-host", "127.0.0.1")
		assert.Nil(t, err)
	}

	// disable.
	{
		cmd := NewReadonlyCommand()
		_, err := executeCommand(cmd, "disable")
		assert.Nil(t, err)
		_, err = executeCommand(cmd, "disable", "--radon-host", "127.0.0.1")
		assert.Nil(t, err)
	}
}
