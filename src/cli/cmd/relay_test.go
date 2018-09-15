/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package cmd

import (
	"fmt"
	"testing"
	"time"

	"ctl"
	"proxy"

	"github.com/stretchr/testify/assert"
)

func TestCmdRelayStatus(t *testing.T) {
	_, proxy, cleanup := proxy.MockProxyWithBackup(log)
	defer cleanup()

	admin := ctl.NewAdmin(log, proxy)
	admin.Start()
	defer admin.Stop()
	time.Sleep(200)

	{
		cmd := NewRelayCommand()
		_, err := executeCommand(cmd, "status")
		assert.Nil(t, err)
		_, err = executeCommand(cmd, "status", "--radon-host", "127.0.0.1")
		assert.Nil(t, err)
	}
}

func TestCmdRelayInfos(t *testing.T) {
	_, proxy, cleanup := proxy.MockProxyWithBackup(log)
	defer cleanup()

	admin := ctl.NewAdmin(log, proxy)
	admin.Start()
	defer admin.Stop()
	time.Sleep(200)

	{
		cmd := NewRelayCommand()
		_, err := executeCommand(cmd, "infos")
		assert.Nil(t, err)
		_, err = executeCommand(cmd, "infos", "--radon-host", "127.0.0.1")
		assert.Nil(t, err)
	}
}

func TestCmdRelayStart(t *testing.T) {
	_, proxy, cleanup := proxy.MockProxyWithBackup(log)
	defer cleanup()

	admin := ctl.NewAdmin(log, proxy)
	admin.Start()
	defer admin.Stop()
	time.Sleep(200)

	{
		cmd := NewRelayCommand()
		_, err := executeCommand(cmd, "start")
		assert.Nil(t, err)
		_, err = executeCommand(cmd, "start", "--radon-host", "127.0.0.1")
		assert.Nil(t, err)
	}
}

func TestCmdRelayStop(t *testing.T) {
	_, proxy, cleanup := proxy.MockProxyWithBackup(log)
	defer cleanup()

	admin := ctl.NewAdmin(log, proxy)
	admin.Start()
	defer admin.Stop()
	time.Sleep(200)

	{
		cmd := NewRelayCommand()
		_, err := executeCommand(cmd, "stop")
		assert.Nil(t, err)
		_, err = executeCommand(cmd, "stop", "--radon-host", "127.0.0.1")
		assert.Nil(t, err)
	}
}

func TestCmdRelayParallelType(t *testing.T) {
	_, proxy, cleanup := proxy.MockProxyWithBackup(log)
	defer cleanup()

	admin := ctl.NewAdmin(log, proxy)
	admin.Start()
	defer admin.Stop()
	time.Sleep(200)

	{
		cmd := NewRelayCommand()
		for i := 0; i < 50; i++ {
			_, err := executeCommand(cmd, "paralleltype", "--type", fmt.Sprintf("%d", (i%5)))
			assert.Nil(t, err)
			_, err = executeCommand(cmd, "paralleltype", "--radon-host", "127.0.0.1", "--type", fmt.Sprintf("%d", (i%5)))
			assert.Nil(t, err)
		}
	}
}

func TestCmdRelayReset(t *testing.T) {
	_, proxy, cleanup := proxy.MockProxyWithBackup(log)
	defer cleanup()

	admin := ctl.NewAdmin(log, proxy)
	admin.Start()
	defer admin.Stop()
	time.Sleep(200)

	{
		cmd := NewRelayCommand()
		_, err := executeCommand(cmd, "stop")
		assert.Nil(t, err)
		_, err = executeCommand(cmd, "reset", "--gtid", "1514254947594569595")
		assert.Nil(t, err)
		_, err = executeCommand(cmd, "stop", "--radon-host", "127.0.0.1")
		assert.Nil(t, err)
		_, err = executeCommand(cmd, "reset", "--radon-host", "127.0.0.1", "--gtid", "1514254947594569595")
		assert.Nil(t, err)
	}
}

func TestCmdRelayResetToNow(t *testing.T) {
	_, proxy, cleanup := proxy.MockProxyWithBackup(log)
	defer cleanup()

	admin := ctl.NewAdmin(log, proxy)
	admin.Start()
	defer admin.Stop()
	time.Sleep(200)

	{
		cmd := NewRelayCommand()
		_, err := executeCommand(cmd, "stop")
		assert.Nil(t, err)
		_, err = executeCommand(cmd, "resettonow")
		assert.Nil(t, err)
		_, err = executeCommand(cmd, "stop", "--radon-host", "127.0.0.1")
		assert.Nil(t, err)
		_, err = executeCommand(cmd, "resettonow", "--radon-host", "127.0.0.1")
		assert.Nil(t, err)
	}
}

func TestCmdRelayMaxWorkers(t *testing.T) {
	_, proxy, cleanup := proxy.MockProxyWithBackup(log)
	defer cleanup()

	admin := ctl.NewAdmin(log, proxy)
	admin.Start()
	defer admin.Stop()
	time.Sleep(200)

	{
		cmd := NewRelayCommand()
		_, err := executeCommand(cmd, "workers", "--max", "111")
		assert.Nil(t, err)
		_, err = executeCommand(cmd, "workers", "--radon-host", "127.0.0.1", "--max", "111")
		assert.Nil(t, err)
	}
	time.Sleep(200)
	{
		cmd := NewRelayCommand()
		_, err := executeCommand(cmd, "workers", "--max", "1")
		assert.Nil(t, err)
		_, err = executeCommand(cmd, "workers", "--radon-host", "127.0.0.1", "--max", "1")
		assert.Nil(t, err)
	}
}
