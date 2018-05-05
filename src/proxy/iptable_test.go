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
		iptable.Add("127.0.0.1")
	}

	// OK.
	{
		_, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
	}

	// Remove.
	{
		iptable.Remove("127.0.0.1")
		iptable.Add("127.0.0.2")
	}

	// Check.
	{
		got := iptable.Check("128.0.0.2")
		assert.False(t, got)
	}

	// Refresh.
	{
		proxy.SetAllowIP([]string{"x", "y"})
		iptable.Refresh()
	}
}
