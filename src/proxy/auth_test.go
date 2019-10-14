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
	"github.com/xelabs/go-mysqlstack/xlog"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

var (
	resultVersion57 = &sqltypes.Result{
		RowsAffected: 2,
		Fields: []*querypb.Field{
			{
				Name: "version",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("5.7")),
			},
		},
	}

	resultVersion56 = &sqltypes.Result{
		RowsAffected: 2,
		Fields: []*querypb.Field{
			{
				Name: "version",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("5.6")),
			},
		},
	}

	resultVersion560 = &sqltypes.Result{
		RowsAffected: 2,
		Fields: []*querypb.Field{
			{
				Name: "version",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("5.6.0")),
			},
		},
	}
)

func TestProxyAuthSessionCheck(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy1(log, MockConfigMax16())
	defer cleanup()
	address := proxy.Address()
	iptable := proxy.IPTable()

	fakedbs.AddQuery("select version() as version", resultVersion57)

	// IPTables.
	{
		iptable.Add("192.168.0.255")
	}

	// max connection.
	{
		var clients []driver.Conn
		for i := 0; i < 16; i++ {
			client, err := driver.NewConn("mock", "mock", address, "", "utf8")
			assert.Nil(t, err)
			clients = append(clients, client)
		}
		{
			_, err := driver.NewConn("mock", "mock", address, "", "utf8")
			want := "Too many connections(max: 16) (errno 1040) (sqlstate 08004)"
			got := err.Error()
			assert.Equal(t, want, got)
		}
		_ = clients
	}
}

func TestProxyAuth(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	fakedbs.AddQuery("select version() as version", resultVersion57)

	// Select mysql.user error.
	{
		_, err := driver.NewConn("mockx", "mockx", address, "", "utf8")
		want := "Access denied for user 'mockx' (errno 1045) (sqlstate 28000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// User not exists.
	{
		fakedbs.AddQuery("select authentication_string from mysql.user where user='mocknull'", &sqltypes.Result{})
		_, err := driver.NewConn("mocknull", "mockx", address, "", "utf8")
		want := "Access denied for user 'mocknull' (errno 1045) (sqlstate 28000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// Auth password error.
	{
		_, err := driver.NewConn("mock", "mockx", address, "", "utf8")
		want := "Access denied for user 'mock' (errno 1045) (sqlstate 28000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// Auth OK.
	{
		_, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
	}
}

func TestProxyAuthLocalPassby(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	{
		_, err := driver.NewConn("root", "", address, "", "utf8")
		assert.Nil(t, err)
	}
}

func TestProxyAuthMySQLVersion(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	fakedbs.ResetAll()
	fakedbs.AddQuery("select version() as version", resultVersion56)

	// Select mysql.user error.
	{
		_, err := driver.NewConn("mockx", "mockx", address, "", "utf8")
		want := "Access denied for user 'mockx' (errno 1045) (sqlstate 28000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	{
		fakedbs.ResetAll()
		fakedbs.AddQuery("select version() as version", &sqltypes.Result{})
		_, err := driver.NewConn("mockx", "mockx", address, "", "utf8")
		assert.NotNil(t, err)
	}
}

func TestProxyAuthMySQLVersionToLower(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	fakedbs.ResetAll()
	fakedbs.AddQuery("select version() as version", resultVersion560)

	// Select mysql.user error.
	{
		_, err := driver.NewConn("mockx", "mockx", address, "", "utf8")
		want := "Access denied for user 'mockx' (errno 1045) (sqlstate 28000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}
