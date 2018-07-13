/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package backend

import (
	"testing"
	"time"

	"fakedb"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestQueryz(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	// MySQL Server starts...
	fakedb := fakedb.New(log, 1)
	defer fakedb.Close()
	addr := fakedb.Addrs()[0]
	conf := MockBackendConfigDefault(addr, addr)
	pool := NewPool(log, conf)

	querys := []string{
		"SELECT1",
		"SELECT2",
	}

	// conn1
	conn1 := NewConnection(log, pool)
	err := conn1.Dial()
	assert.Nil(t, err)

	// conn2
	conn2 := NewConnection(log, pool)
	err = conn2.Dial()
	assert.Nil(t, err)

	// set conds
	fakedb.AddQueryDelay(querys[0], result1, 200)
	fakedb.AddQueryDelay(querys[1], result1, 205)

	// QueryRows
	{
		e1 := func(q string) {
			conn1.Execute(q)
		}

		e2 := func(q string) {
			conn2.Execute(q)
		}
		go e1(querys[0])
		time.Sleep(100 * time.Millisecond)
		go e2(querys[1])

		time.Sleep(50 * time.Millisecond)
		rows := qz.GetQueryzRows()
		assert.Equal(t, querys[0], rows[0].Query)
		assert.Equal(t, querys[1], rows[1].Query)
	}
}
