/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package router

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestRouter(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)
}

func TestRouteradd(t *testing.T) {
	results := []string{`{
	"Schemas": {
		"sbtest": {
			"DB": "sbtest",
			"Tables": {
				"A": {
					"Name": "A",
					"ShardKey": "id",
					"Partition": {
						"Segments": [
							{
								"Table": "A0",
								"Backend": "backend0",
								"Range": {
									"Start": 0,
									"End": 2
								}
							},
							{
								"Table": "A2",
								"Backend": "backend2",
								"Range": {
									"Start": 2,
									"End": 4
								}
							},
							{
								"Table": "A4",
								"Backend": "backend4",
								"Range": {
									"Start": 4,
									"End": 8
								}
							},
							{
								"Table": "A8",
								"Backend": "backend8",
								"Range": {
									"Start": 8,
									"End": 4096
								}
							}
						]
					}
				}
			}
		}
	}
}`}
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)

	// router
	{
		err := router.add("sbtest", MockTableAConfig())
		assert.Nil(t, err)
		want := results[0]
		got := router.JSON()
		log.Debug(got)
		assert.Equal(t, want, got)
	}

	// add same routers
	{
		err := router.add("sbtest", MockTableAConfig())
		want := "router.add.db[sbtest].table[A].exists"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// unsupport shardtype
	{
		err := router.add("sbtest", MockTableE1Config())
		want := "router.unsupport.shardtype:[Range]"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestRouteraddGlobal(t *testing.T) {
	results := []string{`{
	"Schemas": {
		"sbtest": {
			"DB": "sbtest",
			"Tables": {
				"G": {
					"Name": "G",
					"Partition": {
						"Segments": [
							{
								"Table": "G",
								"Backend": "backend1",
								"Range": {}
							},
							{
								"Table": "G",
								"Backend": "backend2",
								"Range": {}
							}
						]
					}
				}
			}
		}
	}
}`}
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)

	// router
	{
		err := router.add("sbtest", MockTableGConfig())
		assert.Nil(t, err)
		want := results[0]
		got := router.JSON()
		log.Debug(got)
		assert.Equal(t, want, got)
	}
}

func TestRouterremove(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)

	// router
	{
		err := router.remove("sbtest", MockTableBConfig().Name)
		want := "router.can.not.find.db[sbtest]"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// add router of sbtest.A
	{
		err := router.add("sbtest", MockTableMConfig())
		assert.Nil(t, err)

		strVal := sqlparser.NewStrVal([]byte("shardkey"))
		_, err = router.Lookup("sbtest", "A", strVal, strVal)
		assert.Nil(t, err)
	}

	// remove router of xx.A
	{
		err := router.remove("xx", MockTableAConfig().Name)
		want := "router.can.not.find.db[xx]"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// remove router of sbtest.E1(invalid router)
	{
		err := router.remove("sbtest", MockTableE1Config().Name)
		want := "router.can.not.find.table[E1]"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// remove router of sbtest.A
	{
		err := router.remove("sbtest", MockTableAConfig().Name)
		assert.Nil(t, err)

		strVal := sqlparser.NewStrVal([]byte("shardkey"))
		_, err = router.Lookup("sbtest", "A", strVal, strVal)
		want := "Table 'A' doesn't exist (errno 1146) (sqlstate 42S02)"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestRouterLookup(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)

	// add router of sbtest.A
	{
		err := router.add("sbtest", MockTableAConfig())
		assert.Nil(t, err)

		strVal := sqlparser.NewStrVal([]byte("shardkey"))
		_, err = router.Lookup("sbtest", "A", strVal, strVal)
		assert.Nil(t, err)
	}
}

func TestRouterLookupError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)

	// add router of sbtest.A
	{
		err := router.add("sbtest", MockTableAConfig())
		assert.Nil(t, err)

		// database error
		{
			strVal := sqlparser.NewStrVal([]byte("shardkey"))
			_, err = router.Lookup("xx", "A", strVal, strVal)
			want := "Unknown database 'xx' (errno 1049) (sqlstate 42000)"
			got := err.Error()
			assert.Equal(t, want, got)
		}

		// database is NULL
		{
			strVal := sqlparser.NewStrVal([]byte("shardkey"))
			_, err = router.Lookup("", "A", strVal, strVal)
			want := "No database selected (errno 1046) (sqlstate 3D000)"
			got := err.Error()
			assert.Equal(t, want, got)
		}
	}
}

func TestRouterShardKey(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)

	// add router of sbtest.A
	{
		err := router.add("sbtest", MockTableAConfig())
		assert.Nil(t, err)

		shardKey, err := router.ShardKey("sbtest", "A")
		assert.Nil(t, err)
		assert.Equal(t, "id", shardKey)
	}
}

func TestRouterShardKeyError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)

	// add router of sbtest.A
	{
		err := router.add("sbtest", MockTableAConfig())
		assert.Nil(t, err)

		// database error
		{
			_, err = router.ShardKey("xx", "A")
			want := "Table 'xx.A' doesn't exist (errno 1146) (sqlstate 42S02)"
			got := err.Error()
			assert.Equal(t, want, got)
		}

		// table error
		{
			_, err = router.ShardKey("sbtest", "x")
			want := "Table 'x' doesn't exist (errno 1146) (sqlstate 42S02)"
			got := err.Error()
			assert.Equal(t, want, got)
		}

		// database is NULL
		{
			_, err = router.ShardKey("", "A")
			want := "No database selected (errno 1046) (sqlstate 3D000)"
			got := err.Error()
			assert.Equal(t, want, got)
		}
	}
}

func TestRouterDatabaseACL(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)

	// Not ok.
	{
		sysDB := []string{"SYS", "MYSQL", "performance_schema", "information_schema"}
		for _, sys := range sysDB {
			err := router.DatabaseACL(sys)
			assert.NotNil(t, err)
		}
	}

	// OK.
	{
		sysDB := []string{"SYS1", "MYSQL1", "performance_schema1", "information_schema1"}
		for _, sys := range sysDB {
			err := router.DatabaseACL(sys)
			assert.Nil(t, err)
		}
	}
}

func TestRouterIsSystemDB(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)

	// true.
	{
		sysDB := []string{"SYS", "MYSQL", "performance_schema", "information_schema"}
		for _, sys := range sysDB {
			is := router.IsSystemDB(sys)
			assert.Equal(t, is, true)
		}
	}

	// false.
	{
		sysDB := []string{"SYS1", "MYSQL1", "performance_schema1", "information_schema1"}
		for _, sys := range sysDB {
			is := router.IsSystemDB(sys)
			assert.Equal(t, is, false)
		}
	}
}

func TestRouterTableConfig(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)

	// add router of sbtest.A
	{
		err := router.add("sbtest", MockTableAConfig())
		assert.Nil(t, err)

		tConf, err := router.TableConfig("sbtest", "A")
		assert.Nil(t, err)
		assert.NotNil(t, tConf)
	}
}
