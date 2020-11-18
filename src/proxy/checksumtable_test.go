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
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	checksumTableResult1 = &sqltypes.Result{
		RowsAffected: 1,
		Fields: []*querypb.Field{
			{
				Name: "Table",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Checksum",
				Type: querypb.Type_INT64,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("a_0000")),
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte("2000038982")),
			},
		},
	}

	checksumTableResult2 = &sqltypes.Result{
		RowsAffected: 1,
		Fields: []*querypb.Field{
			{
				Name: "Table",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Checksum",
				Type: querypb.Type_INT64,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("a_0000")),
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte("NULL")),
			},
		},
	}

	checksumTableResult3 = &sqltypes.Result{
		RowsAffected: 2,
		Fields: []*querypb.Field{
			{
				Name: "Table",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Checksum",
				Type: querypb.Type_INT64,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("a_0000")),
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte("2000038982")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("a_0000")),
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte("NULL")),
			},
		},
	}
)

func TestProxyChecksumTable(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("checksum table .*", checksumTableResult1)
		fakedbs.AddQueryPattern("checksum table xx.*", checksumTableResult2)
		fakedbs.AddQueryPattern("checksum table t1, mock.t1", checksumTableResult3)
		fakedbs.AddQueryPattern("checksum table t quick", checksumTableResult2)
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
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// explain not support.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "explain checksum table t1"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	// checksum with table exist.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "checksum table t1"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)

		var want uint32
		// 30 is the partition tables number
		for i := 0; i < 30; i++ {
			want += 2000038982
		}
		got := uint32(qr.Rows[0][1].ToNative().(int64))
		assert.Equal(t, want, got)
	}

	// checksum with table or db not exist.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "checksum table xx.t1"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		want := "xx.t1"
		got := qr.Rows[0][0].String()
		assert.Equal(t, want, got)
		want = "NULL"
		got = qr.Rows[0][1].String()
		assert.Equal(t, want, got)
	}

	// checksum with table not exist.
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "checksum table mock.t1"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		want := "mock.t1"
		got := qr.Rows[0][0].String()
		assert.Equal(t, want, got)
		want = "NULL"
		got = qr.Rows[0][1].String()
		assert.Equal(t, want, got)
	}

	// checksum with multi tables
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "checksum tables t1, mock.t1"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		{
			var want uint32
			// 30 is the partition tables number
			for i := 0; i < 30; i++ {
				want += 2000038982
			}
			got := uint32(qr.Rows[0][1].ToNative().(int64))
			assert.Equal(t, want, got)
		}
		{
			want := "mock.t1"
			got := qr.Rows[1][0].String()
			assert.Equal(t, want, got)
			want = "NULL"
			got = qr.Rows[1][1].String()
			assert.Equal(t, want, got)
		}
	}

	// checksum with quick option
	{
		client, err := driver.NewConn("mock", "mock", address, "test", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "checksum table t quick"
		qr, err := client.FetchAll(query, -1)
		assert.Nil(t, err)
		want := "test.t"
		got := qr.Rows[0][0].String()
		assert.Equal(t, want, got)
		want = "NULL"
		got = qr.Rows[0][1].String()
		assert.Equal(t, want, got)
	}
}
