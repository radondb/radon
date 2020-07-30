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
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestProxyIptables(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()
	iptable := proxy.IPTable()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
	}

	// OK.
	{
		_, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
	}

	// Add.
	{
		err := iptable.Add("127.0.0.1")
		assert.Nil(t, err)
	}

	// OK.
	{
		_, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
	}

	// Remove.
	{
		iptable.Remove("127.0.0.1")
		err := iptable.Add("127.0.0.2")
		assert.Nil(t, err)
	}

	// Check.
	{
		got := iptable.Check("128.0.0.2")
		assert.False(t, got)
	}

	// Add.
	{
		err := iptable.Add("*")
		assert.Nil(t, err)
	}

	// Add err, err regexp format.
	{
		err := iptable.Add("172.10.[0-9")
		assert.NotNil(t, err)
	}

	// Check.
	{
		got := iptable.Check("128.0.0.2")
		assert.True(t, got)
	}

	// Remove.
	{
		iptable.Remove("127.0.0.2")
		iptable.Remove("*")
	}

	// Add.
	{
		err := iptable.Add("172.16.[0-9]+.[0-9]+")
		assert.Nil(t, err)
	}

	// Check.
	{
		got := iptable.Check("128.0.0.2")
		assert.False(t, got)
		got = iptable.Check("172.16.1.1")
		assert.True(t, got)
		got = iptable.Check("172.1.1.1")
		assert.False(t, got)
	}

	// Refresh.
	{
		proxy.SetAllowIP([]string{"x", "y"})
		err := iptable.Refresh()
		assert.Nil(t, err)
		proxy.SetAllowIP([]string{"192.168.0.1", "10.1.*"})
		err = iptable.Refresh()
		assert.Nil(t, err)
	}

	// Check.
	{
		got := iptable.Check("192.168.0.1")
		assert.True(t, got)
		got = iptable.Check("10.1.1.1")
		assert.True(t, got)
		got = iptable.Check("10.2.1.1")
		assert.False(t, got)
	}

	// Refresh err, wrong regexp format
	{
		proxy.SetAllowIP([]string{"172.10.[0-9"})
		err := iptable.Refresh()
		assert.NotNil(t, err)
	}
}
