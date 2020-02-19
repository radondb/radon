/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package v1

import (
	"router"
	"strings"
	"testing"

	"proxy"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/ant0ine/go-json-rest/rest/test"
	"github.com/radondb/shift/shift"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestCtlV1Shardz(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
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

	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Get("/v1/shard/shardz", ShardzHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("GET", "http://localhost/v1/shard/shardz", nil))
		recorded.CodeIs(200)

		want := "{\"Schemas\":[{\"DB\":\"test\",\"Tables\":[{\"Name\":\"t1\",\"ShardKey\":\"id\",\"Partition\":{\"Segments\":[{\"Table\":\"t1_0000\",\"Backend\":\"backend0\",\"Range\":{\"Start\":0,\"End\":128}},{\"Table\":\"t1_0001\",\"Backend\":\"backend0\",\"Range\":{\"Start\":128,\"End\":256}},{\"Table\":\"t1_0002\",\"Backend\":\"backend0\",\"Range\":{\"Start\":256,\"End\":384}},{\"Table\":\"t1_0003\",\"Backend\":\"backend0\",\"Range\":{\"Start\":384,\"End\":512}},{\"Table\":\"t1_0004\",\"Backend\":\"backend0\",\"Range\":{\"Start\":512,\"End\":640}},{\"Table\":\"t1_0005\",\"Backend\":\"backend0\",\"Range\":{\"Start\":640,\"End\":819}},{\"Table\":\"t1_0006\",\"Backend\":\"backend1\",\"Range\":{\"Start\":819,\"End\":947}},{\"Table\":\"t1_0007\",\"Backend\":\"backend1\",\"Range\":{\"Start\":947,\"End\":1075}},{\"Table\":\"t1_0008\",\"Backend\":\"backend1\",\"Range\":{\"Start\":1075,\"End\":1203}},{\"Table\":\"t1_0009\",\"Backend\":\"backend1\",\"Range\":{\"Start\":1203,\"End\":1331}},{\"Table\":\"t1_0010\",\"Backend\":\"backend1\",\"Range\":{\"Start\":1331,\"End\":1459}},{\"Table\":\"t1_0011\",\"Backend\":\"backend1\",\"Range\":{\"Start\":1459,\"End\":1638}},{\"Table\":\"t1_0012\",\"Backend\":\"backend2\",\"Range\":{\"Start\":1638,\"End\":1766}},{\"Table\":\"t1_0013\",\"Backend\":\"backend2\",\"Range\":{\"Start\":1766,\"End\":1894}},{\"Table\":\"t1_0014\",\"Backend\":\"backend2\",\"Range\":{\"Start\":1894,\"End\":2022}},{\"Table\":\"t1_0015\",\"Backend\":\"backend2\",\"Range\":{\"Start\":2022,\"End\":2150}},{\"Table\":\"t1_0016\",\"Backend\":\"backend2\",\"Range\":{\"Start\":2150,\"End\":2278}},{\"Table\":\"t1_0017\",\"Backend\":\"backend2\",\"Range\":{\"Start\":2278,\"End\":2457}},{\"Table\":\"t1_0018\",\"Backend\":\"backend3\",\"Range\":{\"Start\":2457,\"End\":2585}},{\"Table\":\"t1_0019\",\"Backend\":\"backend3\",\"Range\":{\"Start\":2585,\"End\":2713}},{\"Table\":\"t1_0020\",\"Backend\":\"backend3\",\"Range\":{\"Start\":2713,\"End\":2841}},{\"Table\":\"t1_0021\",\"Backend\":\"backend3\",\"Range\":{\"Start\":2841,\"End\":2969}},{\"Table\":\"t1_0022\",\"Backend\":\"backend3\",\"Range\":{\"Start\":2969,\"End\":3097}},{\"Table\":\"t1_0023\",\"Backend\":\"backend3\",\"Range\":{\"Start\":3097,\"End\":3276}},{\"Table\":\"t1_0024\",\"Backend\":\"backend4\",\"Range\":{\"Start\":3276,\"End\":3404}},{\"Table\":\"t1_0025\",\"Backend\":\"backend4\",\"Range\":{\"Start\":3404,\"End\":3532}},{\"Table\":\"t1_0026\",\"Backend\":\"backend4\",\"Range\":{\"Start\":3532,\"End\":3660}},{\"Table\":\"t1_0027\",\"Backend\":\"backend4\",\"Range\":{\"Start\":3660,\"End\":3788}},{\"Table\":\"t1_0028\",\"Backend\":\"backend4\",\"Range\":{\"Start\":3788,\"End\":3916}},{\"Table\":\"t1_0029\",\"Backend\":\"backend4\",\"Range\":{\"Start\":3916,\"End\":4096}}]}}]}]}"
		got := recorded.Recorder.Body.String()
		log.Debug(got)
		assert.Equal(t, want, got)
	}
}

func TestCtlV1ShardBalanceAdvice1(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	rdbs := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Databases",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("information_schema")),
			},
		},
	}

	r10 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "SizeInMB",
				Type: querypb.Type_DECIMAL,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("8192")),
			},
		},
	}

	r11 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "SizeInMB",
				Type: querypb.Type_DECIMAL,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("3072")),
			},
		},
	}

	r2 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "table_schema",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "table_name",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "sizeMB",
				Type: querypb.Type_DECIMAL,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1_0001")),
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("6144")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1_0002")),
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("2048")),
			},
		},
	}

	// fakedbs.
	{
		fakedbs.AddQuery("show databases", rdbs)
		fakedbs.AddQuery("create database if not exists `test`", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQuerys("select round((sum(data_length) + sum(index_length)) / 1024/ 1024, 0)  as sizeinmb from information_schema.tables", r10, r11)
		fakedbs.AddQuery("SELECT table_schema, table_name, ROUND((SUM(data_length+index_length)) / 1024/ 1024, 0) AS sizeMB FROM information_schema.TABLES GROUP BY table_name HAVING SUM(data_length + index_length)>10485760 ORDER BY (data_length + index_length) DESC", r2)
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

	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Get("/v1/shard/balanceadvice", ShardBalanceAdviceHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("GET", "http://localhost/v1/shard/balanceadvice", nil))
		recorded.CodeIs(200)

		got := recorded.Recorder.Body.String()
		log.Debug(got)
		assert.True(t, strings.Contains(got, `"to-datasize":3072,"to-user":"mock","to-password":"pwd","database":"test","table":"t1_0002","tablesize":2048`))
	}
}

func TestCtlV1ShardBalanceAdviceGlobal(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()
	proxy.Router().AddForTest("sbtest", router.MockTableGConfig())

	rdbs := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Databases",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("sbtest")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("information_schema")),
			},
		},
	}

	r10 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "SizeInMB",
				Type: querypb.Type_DECIMAL,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("8192")),
			},
		},
	}

	r11 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "SizeInMB",
				Type: querypb.Type_DECIMAL,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("3072")),
			},
		},
	}

	r2 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "table_schema",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "table_name",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "sizeMB",
				Type: querypb.Type_DECIMAL,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("sbtest")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1_0002")),
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("6144")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("sbtest")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("G")),
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("2048")),
			},
		},
	}

	// fakedbs.
	{
		fakedbs.AddQuery("show databases", rdbs)
		fakedbs.AddQuery("create database if not exists `sbtest`", &sqltypes.Result{})
		fakedbs.AddQuerys("select round((sum(data_length) + sum(index_length)) / 1024/ 1024, 0)  as sizeinmb from information_schema.tables", r10, r11)
		fakedbs.AddQuery("SELECT table_schema, table_name, ROUND((SUM(data_length+index_length)) / 1024/ 1024, 0) AS sizeMB FROM information_schema.TABLES GROUP BY table_name HAVING SUM(data_length + index_length)>10485760 ORDER BY (data_length + index_length) DESC", r2)
	}

	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Get("/v1/shard/balanceadvice", ShardBalanceAdviceHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("GET", "http://localhost/v1/shard/balanceadvice", nil))
		recorded.CodeIs(200)

		got := recorded.Recorder.Body.String()
		assert.Equal(t, "null", got)
	}
}

func TestCtlV1ShardBalanceAdviceSingle(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()
	proxy.Router().AddForTest("sbtest", router.MockTableSConfig())

	rdbs := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Databases",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("sbtest")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("information_schema")),
			},
		},
	}

	r10 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "SizeInMB",
				Type: querypb.Type_DECIMAL,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("8192")),
			},
		},
	}

	r11 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "SizeInMB",
				Type: querypb.Type_DECIMAL,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("3072")),
			},
		},
	}

	r2 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "table_schema",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "table_name",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "sizeMB",
				Type: querypb.Type_DECIMAL,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("sbtest")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1_00002")),
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("6144")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("sbtest")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("S")),
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("2048")),
			},
		},
	}

	// fakedbs.
	{
		fakedbs.AddQuery("show databases", rdbs)
		fakedbs.AddQuery("create database if not exists `sbtest`", &sqltypes.Result{})
		fakedbs.AddQuerys("select round((sum(data_length) + sum(index_length)) / 1024/ 1024, 0)  as sizeinmb from information_schema.tables", r10, r11)
		fakedbs.AddQuery("SELECT table_schema, table_name, ROUND((SUM(data_length+index_length)) / 1024/ 1024, 0) AS sizeMB FROM information_schema.TABLES GROUP BY table_name HAVING SUM(data_length + index_length)>10485760 ORDER BY (data_length + index_length) DESC", r2)
	}

	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Get("/v1/shard/balanceadvice", ShardBalanceAdviceHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("GET", "http://localhost/v1/shard/balanceadvice", nil))
		recorded.CodeIs(200)

		got := recorded.Recorder.Body.String()
		assert.Equal(t, "null", got)
	}
}

func TestCtlV1ShardBalanceAdviceList(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()
	proxy.Router().AddForTest("sbtest", router.MockTableListConfig())

	rdbs := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Databases",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("sbtest")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("information_schema")),
			},
		},
	}

	r10 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "SizeInMB",
				Type: querypb.Type_DECIMAL,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("8192")),
			},
		},
	}

	r11 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "SizeInMB",
				Type: querypb.Type_DECIMAL,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("3072")),
			},
		},
	}

	r2 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "table_schema",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "table_name",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "sizeMB",
				Type: querypb.Type_DECIMAL,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("sbtest")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1_00002")),
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("6144")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("sbtest")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("L_0000")),
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("2048")),
			},
		},
	}

	// fakedbs.
	{
		fakedbs.AddQuery("show databases", rdbs)
		fakedbs.AddQuery("create database if not exists `sbtest`", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQuerys("select round((sum(data_length) + sum(index_length)) / 1024/ 1024, 0)  as sizeinmb from information_schema.tables", r10, r11)
		fakedbs.AddQuery("SELECT table_schema, table_name, ROUND((SUM(data_length+index_length)) / 1024/ 1024, 0) AS sizeMB FROM information_schema.TABLES GROUP BY table_name HAVING SUM(data_length + index_length)>10485760 ORDER BY (data_length + index_length) DESC", r2)
	}

	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Get("/v1/shard/balanceadvice", ShardBalanceAdviceHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("GET", "http://localhost/v1/shard/balanceadvice", nil))
		recorded.CodeIs(200)

		got := recorded.Recorder.Body.String()
		assert.Equal(t, "null", got)
	}
}

func TestCtlV1ShardBalanceAdviceNoBestDifferTooSmall(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	rdbs := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Databases",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("information_schema")),
			},
		},
	}

	r10 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "SizeInMB",
				Type: querypb.Type_DECIMAL,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("0")),
			},
		},
	}

	r11 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "SizeInMB",
				Type: querypb.Type_DECIMAL,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("255")),
			},
		},
	}

	// fakedbs.
	{
		fakedbs.AddQuery("show databases", rdbs)
		fakedbs.AddQuery("create database if not exists `test`", &sqltypes.Result{})
		fakedbs.AddQuerys("select round((sum(data_length) + sum(index_length)) / 1024/ 1024, 0)  as sizeinmb from information_schema.tables", r10, r11)
	}

	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Get("/v1/shard/balanceadvice", ShardBalanceAdviceHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("GET", "http://localhost/v1/shard/balanceadvice", nil))
		recorded.CodeIs(200)

		got := recorded.Recorder.Body.String()
		assert.Equal(t, "null", got)
	}
}

func TestCtlV1ShardBalanceAdviceNoBest(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	rdbs := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Databases",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("information_schema")),
			},
		},
	}

	r10 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "SizeInMB",
				Type: querypb.Type_DECIMAL,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("8192")),
			},
		},
	}

	r11 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "SizeInMB",
				Type: querypb.Type_DECIMAL,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("4096")),
			},
		},
	}

	r2 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "table_schema",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "table_name",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "sizeMB",
				Type: querypb.Type_DECIMAL,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1_00001")),
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("6144")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1_00002")),
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("2048")),
			},
		},
	}

	// fakedbs.
	{
		fakedbs.AddQuery("show databases", rdbs)
		fakedbs.AddQuery("create database if not exists `test`", &sqltypes.Result{})
		fakedbs.AddQuerys("select round((sum(data_length) + sum(index_length)) / 1024/ 1024, 0)  as sizeinmb from information_schema.tables", r10, r11)
		fakedbs.AddQuery("SELECT table_schema, table_name, ROUND((SUM(data_length+index_length)) / 1024/ 1024, 0) AS sizeMB FROM information_schema.TABLES GROUP BY table_name HAVING SUM(data_length + index_length)>10485760 ORDER BY (data_length + index_length) DESC", r2)
	}

	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Get("/v1/shard/balanceadvice", ShardBalanceAdviceHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("GET", "http://localhost/v1/shard/balanceadvice", nil))
		recorded.CodeIs(200)

		got := recorded.Recorder.Body.String()
		assert.Equal(t, "null", got)
	}
}

func TestCtlV1ShardRuleShift(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()
	address := proxy.Address()
	scatter := proxy.Scatter()
	routei := proxy.Router()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
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

	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Post("/v1/shard/shift", ShardRuleShiftHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		var from, to string
		backends := scatter.BackendConfigsClone()
		for _, backend := range backends {
			if backend.Name == "backend0" {
				from = backend.Address
			} else if backend.Name == "backend1" {
				to = backend.Address
			}
		}

		p := &ruleParams{
			Database:    "test",
			Table:       "t1_0000",
			FromAddress: from,
			ToAddress:   to,
		}
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/shard/shift", p))
		recorded.CodeIs(200)
		want := `{
	"Schemas": {
		"test": {
			"DB": "test",
			"Tables": {
				"t1": {
					"Name": "t1",
					"ShardKey": "id",
					"Partition": {
						"Segments": [
							{
								"Table": "t1_0000",
								"Backend": "backend1",
								"Range": {
									"Start": 0,
									"End": 128
								}
							},
							{
								"Table": "t1_0001",
								"Backend": "backend0",
								"Range": {
									"Start": 128,
									"End": 256
								}
							},
							{
								"Table": "t1_0002",
								"Backend": "backend0",
								"Range": {
									"Start": 256,
									"End": 384
								}
							},
							{
								"Table": "t1_0003",
								"Backend": "backend0",
								"Range": {
									"Start": 384,
									"End": 512
								}
							},
							{
								"Table": "t1_0004",
								"Backend": "backend0",
								"Range": {
									"Start": 512,
									"End": 640
								}
							},
							{
								"Table": "t1_0005",
								"Backend": "backend0",
								"Range": {
									"Start": 640,
									"End": 819
								}
							},
							{
								"Table": "t1_0006",
								"Backend": "backend1",
								"Range": {
									"Start": 819,
									"End": 947
								}
							},
							{
								"Table": "t1_0007",
								"Backend": "backend1",
								"Range": {
									"Start": 947,
									"End": 1075
								}
							},
							{
								"Table": "t1_0008",
								"Backend": "backend1",
								"Range": {
									"Start": 1075,
									"End": 1203
								}
							},
							{
								"Table": "t1_0009",
								"Backend": "backend1",
								"Range": {
									"Start": 1203,
									"End": 1331
								}
							},
							{
								"Table": "t1_0010",
								"Backend": "backend1",
								"Range": {
									"Start": 1331,
									"End": 1459
								}
							},
							{
								"Table": "t1_0011",
								"Backend": "backend1",
								"Range": {
									"Start": 1459,
									"End": 1638
								}
							},
							{
								"Table": "t1_0012",
								"Backend": "backend2",
								"Range": {
									"Start": 1638,
									"End": 1766
								}
							},
							{
								"Table": "t1_0013",
								"Backend": "backend2",
								"Range": {
									"Start": 1766,
									"End": 1894
								}
							},
							{
								"Table": "t1_0014",
								"Backend": "backend2",
								"Range": {
									"Start": 1894,
									"End": 2022
								}
							},
							{
								"Table": "t1_0015",
								"Backend": "backend2",
								"Range": {
									"Start": 2022,
									"End": 2150
								}
							},
							{
								"Table": "t1_0016",
								"Backend": "backend2",
								"Range": {
									"Start": 2150,
									"End": 2278
								}
							},
							{
								"Table": "t1_0017",
								"Backend": "backend2",
								"Range": {
									"Start": 2278,
									"End": 2457
								}
							},
							{
								"Table": "t1_0018",
								"Backend": "backend3",
								"Range": {
									"Start": 2457,
									"End": 2585
								}
							},
							{
								"Table": "t1_0019",
								"Backend": "backend3",
								"Range": {
									"Start": 2585,
									"End": 2713
								}
							},
							{
								"Table": "t1_0020",
								"Backend": "backend3",
								"Range": {
									"Start": 2713,
									"End": 2841
								}
							},
							{
								"Table": "t1_0021",
								"Backend": "backend3",
								"Range": {
									"Start": 2841,
									"End": 2969
								}
							},
							{
								"Table": "t1_0022",
								"Backend": "backend3",
								"Range": {
									"Start": 2969,
									"End": 3097
								}
							},
							{
								"Table": "t1_0023",
								"Backend": "backend3",
								"Range": {
									"Start": 3097,
									"End": 3276
								}
							},
							{
								"Table": "t1_0024",
								"Backend": "backend4",
								"Range": {
									"Start": 3276,
									"End": 3404
								}
							},
							{
								"Table": "t1_0025",
								"Backend": "backend4",
								"Range": {
									"Start": 3404,
									"End": 3532
								}
							},
							{
								"Table": "t1_0026",
								"Backend": "backend4",
								"Range": {
									"Start": 3532,
									"End": 3660
								}
							},
							{
								"Table": "t1_0027",
								"Backend": "backend4",
								"Range": {
									"Start": 3660,
									"End": 3788
								}
							},
							{
								"Table": "t1_0028",
								"Backend": "backend4",
								"Range": {
									"Start": 3788,
									"End": 3916
								}
							},
							{
								"Table": "t1_0029",
								"Backend": "backend4",
								"Range": {
									"Start": 3916,
									"End": 4096
								}
							}
						]
					}
				}
			}
		}
	}
}`
		got := routei.JSON()
		assert.Equal(t, want, got)
	}
}

func TestCtlV1ShardRuleShiftError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()
	address := proxy.Address()
	scatter := proxy.Scatter()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
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

	// database is NULL.
	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Post("/v1/shard/shift", ShardRuleShiftHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		p := &ruleParams{
			Database:    "",
			Table:       "t1_0000",
			FromAddress: "",
			ToAddress:   "",
		}
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/shard/shift", p))
		recorded.CodeIs(500)

		want := "{\"Error\":\"api.v1.shard.rule.request.database.or.table.is.null\"}"
		got := recorded.Recorder.Body.String()
		assert.Equal(t, want, got)
	}

	// database is system.
	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Post("/v1/shard/shift", ShardRuleShiftHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		p := &ruleParams{
			Database:    "mysql",
			Table:       "t1_0000",
			FromAddress: "",
			ToAddress:   "",
		}
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/shard/shift", p))
		recorded.CodeIs(500)

		want := "{\"Error\":\"api.v1.shard.rule.database.can't.be.system.database\"}"
		got := recorded.Recorder.Body.String()
		assert.Equal(t, want, got)
	}

	// from is NULL.
	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Post("/v1/shard/shift", ShardRuleShiftHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		p := &ruleParams{
			Database:    "test",
			Table:       "t1_0000",
			FromAddress: "",
			ToAddress:   "",
		}
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/shard/shift", p))
		recorded.CodeIs(500)

		want := "{\"Error\":\"api.v1.shard.rule.backend.NULL\"}"
		got := recorded.Recorder.Body.String()
		assert.Equal(t, want, got)
	}

	// from equals to.
	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Post("/v1/shard/shift", ShardRuleShiftHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		var from string
		backends := scatter.BackendConfigsClone()
		for _, backend := range backends {
			if backend.Name == "backend0" {
				from = backend.Address
				break
			}
		}

		p := &ruleParams{
			Database:    "test",
			Table:       "t1_0000",
			FromAddress: from,
			ToAddress:   from,
		}
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/shard/shift", p))
		recorded.CodeIs(500)

		want := "{\"Error\":\"api.v1.shard.rule.backend.NULL\"}"
		got := recorded.Recorder.Body.String()
		assert.Equal(t, want, got)
	}

	// Tables cant find.
	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Post("/v1/shard/shift", ShardRuleShiftHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		var from, to string
		backends := scatter.BackendConfigsClone()
		for _, backend := range backends {
			if backend.Name == "backend0" {
				from = backend.Address
			} else if backend.Name == "backend1" {
				to = backend.Address
			}
		}

		p := &ruleParams{
			Database:    "test",
			Table:       "t1_000x",
			FromAddress: from,
			ToAddress:   to,
		}
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/shard/shift", p))
		recorded.CodeIs(500)

		want := "{\"Error\":\"router.find.table.config.cant.found.backend[backend0]+table:[t1_000x]\"}"
		got := recorded.Recorder.Body.String()
		assert.Equal(t, want, got)
	}
}

func TestCtlV1ShardReLoad(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
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

	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Post("/v1/shard/reload", ShardReLoadHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/shard/reload", nil))
		recorded.CodeIs(200)
	}
}

func TestCtlV1ShardReLoadError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
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

	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Post("/v1/shard/reload", ShardReLoadHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("GET", "http://localhost/v1/shard/reload", nil))
		recorded.CodeIs(405)
	}
}

func TestCtlV1Globals(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	{
		err := proxy.Router().AddForTest("sbtest", router.MockTableMConfig())
		assert.Nil(t, err)
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Get("/v1/shard/globals", GlobalsHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("GET", "http://localhost/v1/shard/globals", nil))
		recorded.CodeIs(200)

		got := recorded.Recorder.Body.String()
		log.Debug(got)
		want := "null"
		assert.Equal(t, want, got)
	}

	{
		err := proxy.Router().AddForTest("sbtest", router.MockTableGConfig(), router.MockTableSConfig())
		assert.Nil(t, err)
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Get("/v1/shard/globals", GlobalsHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("GET", "http://localhost/v1/shard/globals", nil))
		recorded.CodeIs(200)

		got := recorded.Recorder.Body.String()
		log.Debug(got)
		want := "{\"schemas\":[{\"database\":\"sbtest\",\"tables\":[\"G\"]}]}"
		assert.Equal(t, want, got)
	}
}

func TestSubTableToTable(t *testing.T) {
	testCases := []struct {
		in, out string
	}{
		{"t", ""},
		{"t1_0001", "t1"},
		{"t2_000", ""},
		{"t3_0000_0001", "t3_0000"},
	}

	for _, test := range testCases {
		isSub, table := SubTableToTable(test.in)
		if isSub || test.out != "" {
			assert.Equal(t, test.out, table)
		}
	}
}

var (
	showBinlogFormat = &sqltypes.Result{
		RowsAffected: 1,
		Fields: []*querypb.Field{
			{
				Name: "Variable_name",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Value",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("binlog_format")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("ROW")),
			},
		},
	}
	selectResult = &sqltypes.Result{
		RowsAffected: 2,
		Fields: []*querypb.Field{
			{
				Name: "Tables_in_test",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("b")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("A")),
			},
		},
	}
	showCreateTableResult = &sqltypes.Result{
		RowsAffected: 2,
		Fields: []*querypb.Field{
			{
				Name: "Table",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Create Table",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("a")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR,
					[]byte("CREATE TABLE `a` (`i` int(11) NOT NULL, PRIMARY KEY (`i`)) ENGINE=InnoDB DEFAULT CHARSET=utf8")),
			},
		},
	}
	emptyResult = &sqltypes.Result{}
)

func TestCtlV1ShardMigrateErr(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()
	address := proxy.Address()
	scatter := proxy.Scatter()

	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Post("/v1/shard/migrate", ShardMigrateHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	var from, fromUsr, fromPasswd, to, toUsr, toPasswd string
	backends := scatter.BackendConfigsClone()
	for _, backend := range backends {
		if backend.Name == "backend0" {
			from = backend.Address
			fromUsr = backend.User
			fromPasswd = backend.Password
		} else if backend.Name == "backend1" {
			to = backend.Address
			toUsr = backend.User
			toPasswd = backend.Password
		}
	}
	p := &migrateParams{
		ToFlavor:               "mysql",
		From:                   "127.0.0.1" + from,
		FromUser:               fromUsr,
		FromPassword:           fromPasswd,
		FromDatabase:           "test",
		FromTable:              "a",
		To:                     "127.0.0.1" + to,
		ToUser:                 toUsr,
		ToPassword:             toPasswd,
		ToDatabase:             "test",
		ToTable:                "a",
		RadonURL:               "http://" + proxy.Config().Proxy.PeerAddress,
		Cleanup:                false,
		MySQLDump:              "mysqldump",
		Threads:                16,
		Behinds:                2048,
		Checksum:               true,
		WaitTimeBeforeChecksum: 10,
	}

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", emptyResult)
		fakedbs.AddQueryPattern("show .*", showCreateTableResult)
		fakedbs.AddQuery("SHOW GLOBAL VARIABLES LIKE \"binlog_format\"", showBinlogFormat)
		fakedbs.AddQueryPattern("FLUSH .*", emptyResult)
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
		query := "create table test.a(i int primary key) single"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// DecodeJsonPayload err.
	{
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/shard/migrate", nil))
		recorded.CodeIs(500)
	}

	// shift.Start() err.
	{
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/shard/migrate", p))
		recorded.CodeIs(500)
	}

	// shift.WaitFinish() err.
	{
		fakedbs.AddQueryPattern("select .*", selectResult)
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/shard/migrate", p))
		recorded.CodeIs(500)
	}

	// check args empty.
	{
		p.ToTable = ""
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/shard/migrate", p))
		recorded.CodeIs(204)
	}
}

func TestCtlV1ShardStatus(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()
	address := proxy.Address()
	scatter := proxy.Scatter()
	routei := proxy.Router()

	var from, to string
	backends := scatter.BackendConfigsClone()
	for _, backend := range backends {
		if backend.Name == "backend0" {
			from = backend.Address
		} else if backend.Name == "backend1" {
			to = backend.Address
		}
	}

	testcases := []statusParams{
		// migrating, cleanup is true.
		{
			Database:    "test",
			Table:       "t1_0000",
			FromAddress: from,
			ToAddress:   to,
			Status:      shift.MIGRATING,
			Cleanup:     true,
		},
		// migrate success, cleanup is true.
		{
			Database:    "test",
			Table:       "t1_0000",
			FromAddress: to,
			ToAddress:   from,
			Status:      shift.SUCCESS,
			Cleanup:     true,
		},
		// migrating, cleanup is false.
		{
			Database:    "test",
			Table:       "t1_0000",
			FromAddress: from,
			ToAddress:   to,
			Status:      shift.MIGRATING,
			Cleanup:     false,
		},
		// migrate failure, cleanup is false.
		{
			Database:    "test",
			Table:       "t1_0000",
			FromAddress: from,
			ToAddress:   to,
			Status:      shift.FAILURE,
			Cleanup:     false,
		},
	}

	wants := []struct {
		status  string
		cleanup string
	}{
		{
			status:  "migrating",
			cleanup: "backend1",
		},
		{
			status:  "",
			cleanup: "",
		},
		{
			status:  "migrating",
			cleanup: "backend1",
		},
		{
			status:  "",
			cleanup: "backend1",
		},
	}
	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
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

	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Put("/v1/shard/status", ShardStatusHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		for i, p := range testcases {
			recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("PUT", "http://localhost/v1/shard/status", &p))
			recorded.CodeIs(200)
			tbInf, err := routei.TableConfig("test", "t1")
			assert.Nil(t, err)
			assert.Equal(t, wants[i].status, tbInf.Partitions[0].Status)
			assert.Equal(t, wants[i].cleanup, tbInf.Partitions[0].Cleanup)
		}
	}
}

func TestCtlV1ShardStatusErr(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()
	address := proxy.Address()
	scatter := proxy.Scatter()

	var from, to string
	backends := scatter.BackendConfigsClone()
	for _, backend := range backends {
		if backend.Name == "backend0" {
			from = backend.Address
		} else if backend.Name == "backend1" {
			to = backend.Address
		}
	}

	testcases := []statusParams{
		// database is NULL.
		{
			Database:    "",
			Table:       "t1_0000",
			FromAddress: from,
			ToAddress:   to,
			Status:      shift.MIGRATING,
			Cleanup:     true,
		},
		// database is system.
		{
			Database:    "mysql",
			Table:       "user",
			FromAddress: from,
			ToAddress:   to,
			Status:      shift.MIGRATING,
			Cleanup:     true,
		},
		// from is NULL.
		{
			Database:    "test",
			Table:       "t1_0000",
			FromAddress: "",
			ToAddress:   to,
			Status:      shift.MIGRATING,
			Cleanup:     true,
		},
		// from equals to.
		{
			Database:    "test",
			Table:       "t1_0000",
			FromAddress: to,
			ToAddress:   to,
			Status:      shift.MIGRATING,
			Cleanup:     true,
		},
		// Table cant find.
		{
			Database:    "test",
			Table:       "t2",
			FromAddress: from,
			ToAddress:   to,
			Status:      shift.MIGRATING,
			Cleanup:     true,
		},
	}

	wants := []struct {
		code int
		err  string
	}{
		{
			code: 500,
			err:  "{\"Error\":\"database or table is null\"}",
		},
		{
			code: 500,
			err:  "{\"Error\":\"database can't be system database\"}",
		},
		{
			code: 500,
			err:  "{\"Error\":\"from-address or to-address is invalid\"}",
		},
		{
			code: 500,
			err:  "{\"Error\":\"from-address or to-address is invalid\"}",
		},
		{
			code: 500,
			err:  "{\"Error\":\"router.find.table.config.cant.found.backend[backend0]+table:[t2]\"}",
		},
	}
	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
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

	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Put("/v1/shard/status", ShardStatusHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		// DecodeJsonPayload err.
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("PUT", "http://localhost/v1/shard/status", nil))
		recorded.CodeIs(500)
		got := recorded.Recorder.Body.String()
		assert.Equal(t, "{\"Error\":\"JSON payload is empty\"}", got)

		for i, p := range testcases {
			recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("PUT", "http://localhost/v1/shard/status", &p))
			recorded.CodeIs(wants[i].code)

			got := recorded.Recorder.Body.String()
			assert.Equal(t, wants[i].err, got)
		}
	}
}
