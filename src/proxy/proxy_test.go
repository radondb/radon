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

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestProxy1(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := MockProxy(log)
	defer cleanup()

	assert.NotNil(t, proxy.IPTable())
	assert.NotNil(t, proxy.Scatter())
	assert.NotNil(t, proxy.Router())
	assert.NotNil(t, proxy.Sessions())

	// SetMaxConnections
	{
		proxy.SetMaxConnections(6666)
		assert.Equal(t, 6666, proxy.conf.Proxy.MaxConnections)
	}

	// SetMaxResultSize
	{
		proxy.SetMaxResultSize(6666)
		assert.Equal(t, 6666, proxy.conf.Proxy.MaxResultSize)
	}

	// SetMaxJoinRows
	{
		proxy.SetMaxJoinRows(6666)
		assert.Equal(t, 6666, proxy.conf.Proxy.MaxJoinRows)
	}

	// SetDDLTimeout
	{
		proxy.SetDDLTimeout(6666)
		assert.Equal(t, 6666, proxy.conf.Proxy.DDLTimeout)
	}

	// SetQueryTimeout
	{
		proxy.SetQueryTimeout(6666)
		assert.Equal(t, 6666, proxy.conf.Proxy.QueryTimeout)
	}

	// SetTwoPC
	{
		proxy.SetTwoPC(true)
		assert.Equal(t, true, proxy.conf.Proxy.TwopcEnable)
	}

	// SetAllowIP
	{
		ips := []string{"x", "y"}
		proxy.SetAllowIP(ips)
		assert.Equal(t, ips, proxy.conf.Proxy.IPS)
	}

	// SetAuditMode
	{
		proxy.SetAuditMode("A")
		assert.Equal(t, "A", proxy.conf.Audit.Mode)
	}

	// SetBlocks.
	{
		proxy.SetBlocks(256)
		assert.Equal(t, 256, proxy.conf.Router.Blocks)
	}

	// SetThrottle
	{
		proxy.SetThrottle(100)
		assert.Equal(t, 100, proxy.throttle.Limits())
	}

	// SetReadOnly
	{
		assert.Equal(t, false, proxy.spanner.ReadOnly())
		proxy.SetReadOnly(true)
		assert.Equal(t, true, proxy.spanner.ReadOnly())
		proxy.SetReadOnly(false)
		assert.Equal(t, false, proxy.spanner.ReadOnly())
	}

	// SetLoadBalance
	{
		proxy.SetLoadBalance(1)
		assert.Equal(t, 1, proxy.conf.Proxy.LoadBalance)
		proxy.SetLoadBalance(0)
		assert.Equal(t, 0, proxy.conf.Proxy.LoadBalance)
	}

	// SetLowerCaseTableNames.
	{
		proxy.SetLowerCaseTableNames(true)
		assert.Equal(t, true, proxy.conf.Proxy.LowerCaseTableNames)
		proxy.SetLowerCaseTableNames(false)
		assert.Equal(t, false, proxy.conf.Proxy.LowerCaseTableNames)
	}

	// FlushConfig.
	{
		err := proxy.FlushConfig()
		assert.Nil(t, err)
	}

	// Config
	{
		config := proxy.Config()
		assert.NotNil(t, config)
	}

	// Syncer
	{
		syncer := proxy.Syncer()
		assert.NotNil(t, syncer)
	}

	// Plugins.
	{
		plugins := proxy.Plugins()
		assert.NotNil(t, plugins)
	}

	// PeerAddress
	{
		addr := proxy.PeerAddress()
		assert.NotNil(t, addr)
	}
}
