/*
 * go-mydumper
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package common

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestPool(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.INFO))
	fakedbs := driver.NewTestHandler(log)
	server, err := driver.MockMysqlServer(log, fakedbs)
	assert.Nil(t, err)
	defer server.Close()
	address := server.Addr()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("select .*", &sqltypes.Result{})
	}

	pool, err := NewPool(log, 8, address, "mock", "mock")
	assert.Nil(t, err)

	var wg sync.WaitGroup
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	{
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ch1:
					return
				default:
					conn := pool.Get()
					err := conn.Execute("select 1")
					assert.Nil(t, err)

					_, err = conn.Fetch("select 1")
					assert.Nil(t, err)

					_, err = conn.StreamFetch("select 1")
					assert.Nil(t, err)

					pool.Put(conn)
				}
			}
		}()
	}

	{
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ch2:
					return
				default:
					conn := pool.Get()
					conn.Execute("select 2")
					assert.Nil(t, err)

					conn.Fetch("select 2")
					assert.Nil(t, err)

					_, err = conn.StreamFetch("select 1")
					assert.Nil(t, err)

					pool.Put(conn)
				}
			}
		}()
	}

	time.Sleep(time.Second)
	close(ch1)
	close(ch2)
	pool.Close()

	wg.Wait()
}
