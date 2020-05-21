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

func TestRouterAdd(t *testing.T) {
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
	err := router.CreateDatabase("sbtest")
	assert.Nil(t, err)

	// router
	{
		err := router.addTable("sbtest", MockTableAConfig())
		assert.Nil(t, err)
		want := results[0]
		got := router.JSON()
		log.Debug(got)
		assert.Equal(t, want, got)
	}

	// add same routers
	{
		err := router.addTable("sbtest", MockTableAConfig())
		want := "router.add.db[sbtest].table[A].exists"
		got := err.Error()
		assert.Equal(t, want, got)

		err = router.addTable("sbtest", MockTableDeadLockConfig())
		assert.NotNil(t, err)
	}

	// unsupport shardtype
	{
		err := router.addTable("sbtest", MockTableE1Config())
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
	err := router.CreateDatabase("sbtest")
	assert.Nil(t, err)

	// router
	{
		err := router.addTable("sbtest", MockTableGConfig())
		assert.Nil(t, err)
		want := results[0]
		got := router.JSON()
		log.Debug(got)
		assert.Equal(t, want, got)
	}
}

func TestRouteraddSingle(t *testing.T) {
	results := []string{`{
	"Schemas": {
		"sbtest": {
			"DB": "sbtest",
			"Tables": {
				"S": {
					"Name": "S",
					"Partition": {
						"Segments": [
							{
								"Table": "S",
								"Backend": "backend1",
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
	err := router.CreateDatabase("sbtest")
	assert.Nil(t, err)

	// router
	{
		err := router.addTable("sbtest", MockTableSConfig())
		assert.Nil(t, err)
		want := results[0]
		got := router.JSON()
		log.Debug(got)
		assert.Equal(t, want, got)
	}
}

func TestRouterRemove(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)

	// router
	{
		err := router.removeTable("sbtest", MockTableBConfig().Name)
		want := "router.can.not.find.db[sbtest]"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	err := router.CreateDatabase("sbtest")
	assert.Nil(t, err)
	// router
	{
		err := router.removeTable("", MockTableCConfig().Name)
		want := "router.can.not.find.db[]"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// add router of sbtest.A
	{
		err := router.addTable("sbtest", MockTableMConfig())
		assert.Nil(t, err)

		strVal := sqlparser.NewStrVal([]byte("shardkey"))
		_, err = router.Lookup("sbtest", "A", strVal, strVal)
		assert.Nil(t, err)
	}

	// remove router of xx.A
	{
		err := router.removeTable("xx", MockTableAConfig().Name)
		want := "router.can.not.find.db[xx]"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// remove router of sbtest.E1(invalid router)
	{
		err := router.removeTable("sbtest", MockTableE1Config().Name)
		want := "router.can.not.find.table[E1]"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// remove router of sbtest.A
	{
		err := router.removeTable("sbtest", MockTableAConfig().Name)
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
	err := router.CreateDatabase("sbtest")
	assert.Nil(t, err)

	// add router of sbtest.A
	{
		err := router.addTable("sbtest", MockTableAConfig())
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
	err := router.CreateDatabase("sbtest")
	assert.Nil(t, err)

	// add router of sbtest.A
	{
		err := router.addTable("sbtest", MockTableAConfig())
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
	err := router.CreateDatabase("sbtest")
	assert.Nil(t, err)

	// add router of sbtest.A
	{
		err := router.addTable("sbtest", MockTableAConfig())
		assert.Nil(t, err)

		shardKey, err := router.ShardKey("sbtest", "A")
		assert.Nil(t, err)
		assert.Equal(t, "id", shardKey)
	}
}

func TestRouterPartitionType(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)
	err := router.CreateDatabase("sbtest")
	assert.Nil(t, err)

	{
		// add router of sbtest.A
		err := router.addTable("sbtest", MockTableAConfig())
		assert.Nil(t, err)

		_, err = router.PartitionType("sbtest", "B")
		assert.NotNil(t, err)

		partitionType, err := router.PartitionType("sbtest", "A")
		assert.Nil(t, err)
		assert.EqualValues(t, methodTypeHash, partitionType)

		isHash := router.IsPartitionHash(methodTypeHash)
		assert.Equal(t, true, isHash)
	}
}

func TestRouterShardKeyError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)
	err := router.CreateDatabase("sbtest")
	assert.Nil(t, err)

	// add router of sbtest.A
	{
		err := router.addTable("sbtest", MockTableAConfig())
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
	err := router.CreateDatabase("sbtest")
	assert.Nil(t, err)

	// add router of sbtest.A
	{
		err := router.addTable("sbtest", MockTableAConfig())
		assert.Nil(t, err)

		tConf, err := router.TableConfig("sbtest", "A")
		assert.Nil(t, err)
		assert.NotNil(t, tConf)
	}
}

func TestRouterGetIndex(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)
	err := router.CreateDatabase("sbtest")
	assert.Nil(t, err)
	err = router.AddForTest("sbtest", MockTableGConfig(), MockTableMConfig(), MockTableSConfig())
	assert.Nil(t, err)
	// hash.
	{
		intVal := sqlparser.NewIntVal([]byte("1"))
		idx, err := router.GetIndex("sbtest", "A", intVal)
		assert.Nil(t, err)
		assert.Equal(t, 2323, idx)
	}
	//global.
	{
		intVal := sqlparser.NewIntVal([]byte("1"))
		idx, err := router.GetIndex("sbtest", "G", intVal)
		assert.Nil(t, err)
		assert.Equal(t, -1, idx)
	}
	//single.
	{
		intVal := sqlparser.NewIntVal([]byte("1"))
		idx, err := router.GetIndex("sbtest", "S", intVal)
		assert.Nil(t, err)
		assert.Equal(t, 0, idx)
	}
}

func TestRouterGetIndexError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)
	err := router.CreateDatabase("sbtest")
	assert.Nil(t, err)
	err = router.AddForTest("sbtest", MockTableMConfig())
	assert.Nil(t, err)
	// no database.
	{
		intVal := sqlparser.NewIntVal([]byte("1"))
		idx, err := router.GetIndex("", "A", intVal)
		assert.Equal(t, -1, idx)
		want := "No database selected (errno 1046) (sqlstate 3D000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}
	// table not exists.
	{
		intVal := sqlparser.NewIntVal([]byte("1"))
		idx, err := router.GetIndex("sbtest", "B", intVal)
		assert.Equal(t, -1, idx)
		want := "Table 'B' doesn't exist (errno 1146) (sqlstate 42S02)"
		got := err.Error()
		assert.Equal(t, want, got)
	}
	// hash unsupport key type.
	{
		hexVal := sqlparser.NewHexNum([]byte("3.1415926"))
		idx, err := router.GetIndex("sbtest", "A", hexVal)
		assert.Equal(t, -1, idx)
		want := "hash.unsupported.key.type:[3]"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestRouterGetSegments(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)
	err := router.CreateDatabase("sbtest")
	assert.Nil(t, err)
	err = router.AddForTest("sbtest", MockTableGConfig(), MockTableMConfig(), MockTableSConfig(), MockTableListConfig())
	assert.Nil(t, err)
	// hash.
	{
		segments, err := router.GetSegments("sbtest", "A", []int{1})
		assert.Nil(t, err)
		assert.Equal(t, 1, len(segments))
	}
	// hash repeat segments.
	{
		segments, err := router.GetSegments("sbtest", "A", []int{0, 1})
		assert.Nil(t, err)
		assert.Equal(t, 1, len(segments))
	}
	// hash all segments.
	{
		segments, err := router.GetSegments("sbtest", "A", []int{})
		assert.Nil(t, err)
		assert.Equal(t, 6, len(segments))
	}
	//global.
	{
		segments, err := router.GetSegments("sbtest", "G", []int{1})
		assert.Nil(t, err)
		assert.Equal(t, 1, len(segments))
	}
	//global all segments.
	{
		segments, err := router.GetSegments("sbtest", "G", []int{})
		assert.Nil(t, err)
		assert.Equal(t, 2, len(segments))
	}
	//single.
	{
		segments, err := router.GetSegments("sbtest", "S", []int{0})
		assert.Nil(t, err)
		assert.Equal(t, 1, len(segments))
	}
	//single all segments.
	{
		segments, err := router.GetSegments("sbtest", "S", []int{})
		assert.Nil(t, err)
		assert.Equal(t, 1, len(segments))
	}
	//list.
	{
		segments, err := router.GetSegments("sbtest", "L", []int{0})
		assert.Nil(t, err)
		assert.Equal(t, 1, len(segments))
	}
	//list all segments.
	{
		segments, err := router.GetSegments("sbtest", "L", []int{})
		assert.Nil(t, err)
		assert.Equal(t, 3, len(segments))
	}
}

func TestRouterGetSegmentsError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)
	err := router.CreateDatabase("sbtest")
	assert.Nil(t, err)
	err = router.AddForTest("sbtest", MockTableGConfig(), MockTableMConfig(), MockTableSConfig())
	assert.Nil(t, err)
	// no database.
	{
		segments, err := router.GetSegments("", "A", []int{1})
		assert.Nil(t, segments)
		want := "No database selected (errno 1046) (sqlstate 3D000)"
		got := err.Error()
		assert.Equal(t, want, got)
	}
	// table not exists.
	{
		segments, err := router.GetSegments("sbtest", "B", []int{1})
		assert.Nil(t, segments)
		want := "Table 'B' doesn't exist (errno 1146) (sqlstate 42S02)"
		got := err.Error()
		assert.Equal(t, want, got)
	}
	// hash out of range.
	{
		segments, err := router.GetSegments("sbtest", "A", []int{4096})
		assert.Nil(t, segments)
		want := "hash.getsegment.index.[4096].out.of.range"
		got := err.Error()
		assert.Equal(t, want, got)
	}
	// global out of range.
	{
		segments, err := router.GetSegments("sbtest", "G", []int{2})
		assert.Nil(t, segments)
		want := "global.getsegment.index.[2].out.of.range"
		got := err.Error()
		assert.Equal(t, want, got)
	}
	// single out of range.
	{
		segments, err := router.GetSegments("sbtest", "S", []int{1})
		assert.Nil(t, segments)
		want := "single.getsegment.index.[1].out.of.range"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestRouterTables(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	err := router.CreateDatabase("sbtest")
	assert.Nil(t, err)

	// sbtest with tables.
	err = router.AddForTest("sbtest", MockTableMConfig())
	assert.Nil(t, err)

	// tables is empty.
	router.CreateDatabase("nulldatabase")
	assert.Nil(t, err)

	want := make(map[string][]string)
	want["sbtest"] = []string{"A"}
	want["nulldatabase"] = []string{}
	got := router.Tables()
	assert.Equal(t, want, got)
}

func TestRouterGetRenameTableConfig(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	err := router.CreateDatabase("sbtest")
	assert.Nil(t, err)

	// sbtest with tables.
	err = router.AddForTest("sbtest", MockTableMConfig())
	assert.Nil(t, err)

	_, err = router.getRenameTableConfig("sbtest", "A", "B")
	assert.Nil(t, err)

	_, err = router.getRenameTableConfig("sbtest1", "A", "B")
	assert.NotNil(t, err)

	_, err = router.getRenameTableConfig("sbtest", "B", "B")
	assert.NotNil(t, err)

	_, err = router.getRenameTableConfig("sbtest", "A", "A")
	assert.NotNil(t, err)
}

func TestRouterIsPartitionHash(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	err := router.CreateDatabase("sbtest")
	assert.Nil(t, err)

	// sbtest with tables.
	err = router.AddForTest("sbtest", MockTableMConfig())
	assert.Nil(t, err)

	isHash := router.IsPartitionHash(methodTypeHash)
	assert.Equal(t, true, isHash)
}
