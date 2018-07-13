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

func TestCmdTwopc(t *testing.T) {
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	admin := ctl.NewAdmin(log, proxy)
	admin.Start()
	defer admin.Stop()
	time.Sleep(100)

	// enable.
	{
		cmd := NewTwopcCommand()
		_, err := executeCommand(cmd, "enable")
		assert.Nil(t, err)
	}
	// disable.
	{
		cmd := NewTwopcCommand()
		_, err := executeCommand(cmd, "disable")
		assert.Nil(t, err)
	}
}
