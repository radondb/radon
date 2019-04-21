/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestKillIdleTxn(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	fakedbs, proxy, cleanup := MockProxy1(log, MockConfigIdleTxnTimeout1())
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("XA .*", result1)
	}

	// create database.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create database test"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// begin.
	{
		proxy.SetTwoPC(true)
		client1, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "begin;"
		_, err = client1.FetchAll(query, -1)
		assert.Nil(t, err)
		defer client1.Close()
	}

	// show processlist.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "show processlist;"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		log.Warning("show processlist: %v", qr.Rows)
	}

	time.Sleep(2 * time.Second)

	// show processlist.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "show processlist;"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		log.Warning("show processlist: %v", qr.Rows)
	}
}

func TestKillIdleTxnInTxnLongQuery(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	fakedbs, proxy, cleanup := MockProxy1(log, MockConfigIdleTxnTimeout1())
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("XA .*", result1)
		fakedbs.AddQueryPattern("select * .*", &sqltypes.Result{})
		fakedbs.AddQueryDelay("select * from test.t1_0002 as t1", &sqltypes.Result{}, 3000)
	}

	// create database.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create database test"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		query := "create table t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
		client.Quit()
	}

	// long query
	var wg sync.WaitGroup
	proxy.SetTwoPC(true)
	client1, err := driver.NewConn("mock", "mock", address, "test", "utf8")
	{
		assert.Nil(t, err)
		// long query success
		{
			wg.Add(1)
			go func(c driver.Conn) {
				defer wg.Done()
				query := "begin;"
				_, err = c.FetchAll(query, -1)
				query = "select * from t1"
				_, err = c.FetchAll(query, -1)
				log.Warning("get the long query.")
			}(client1)
		}
		//defer client.Close()
	}

	// show processlist.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "show processlist;"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		log.Warning("show processlist: %v", qr.Rows)
	}

	time.Sleep(2 * time.Second)

	// show processlist, the long query in the transaction will not be killed.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "show processlist;"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		log.Warning("show processlist: %v", qr.Rows)
		rs := false
		for _, row := range qr.Rows {
			if (len(row[6].Raw()) > 0) && (len(row[7].Raw()) > 0) {
				if strings.EqualFold(sessionStateInTransaction, string(row[6].Raw())) &&
					strings.EqualFold("select * from t1", string(row[7].Raw())) {
					rs = true
				}
			}
		}
		assert.EqualValues(t, true, rs)
	}

	wg.Wait()
	client1.Close()
}

func TestKillIdleTxnLongQueryInExecuteSingle(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	fakedbs, proxy, cleanup := MockProxy1(log, MockConfigIdleTxnTimeout1())
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		//fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		//fakedbs.AddQueryPattern("XA .*", result1)
		fakedbs.AddQueryPattern("select * .*", &sqltypes.Result{})
		fakedbs.AddQueryDelay("select 1 from dual", &sqltypes.Result{}, 3000)
	}

	// long query
	var wg sync.WaitGroup
	//proxy.SetTwoPC(true)
	client1, err := driver.NewConn("mock", "mock", address, "", "utf8")
	{
		assert.Nil(t, err)
		// long query success
		{
			wg.Add(1)
			go func(c driver.Conn) {
				defer wg.Done()
				query := "select 1 from dual"
				_, err = c.FetchAll(query, -1)
				log.Warning("get the long query.")
			}(client1)
		}
	}

	// show processlist.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "show processlist;"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		log.Warning("show processlist: %v", qr.Rows)
	}

	time.Sleep(2 * time.Second)

	// show processlist
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "show processlist;"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		log.Warning("show processlist: %v", qr.Rows)
	}

	wg.Wait()
	client1.Close()
}
