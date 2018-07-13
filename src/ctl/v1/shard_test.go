/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package v1

import (
	"strings"
	"testing"

	"proxy"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/ant0ine/go-json-rest/rest/test"
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
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
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
		log.Debug(got)
		assert.True(t, strings.Contains(got, `"to-datasize":3072,"to-user":"mock","to-password":"pwd","database":"test","table":"t1_00002","tablesize":2048`))
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
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
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
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
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

		want := "{\"Error\":\"router.rule.change.cant.found.backend[backend0]+table:[t1_000x]\"}"
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
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
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
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
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
