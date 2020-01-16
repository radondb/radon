/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestErrorParams(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// attach.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "radon attach('attach1', '127.0.0.1:6000', 'root', '123456', 'xxxx')"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	// detach.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "radon detach('attach1','127')"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	// reshard.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "radon reshard db.tb to db2.t2"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	// progress.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "radon progress db.tb"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}
}
