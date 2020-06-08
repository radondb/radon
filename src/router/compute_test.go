/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package router

import (
	"fmt"
	"testing"

	"config"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestRouterCompute(t *testing.T) {
	datas := `{
	"name": "t1",
	"slots-readonly": 4096,
	"blocks-readonly": 128,
	"shardkey": "id",
	"shardtype": "HASH",
	"shardkey": "id",
	"partitions": [
		{
			"table": "t1_0000",
			"segment": "0-128",
			"backend": "192.168.0.1"
		},
		{
			"table": "t1_0001",
			"segment": "128-256",
			"backend": "192.168.0.1"
		},
		{
			"table": "t1_0002",
			"segment": "256-384",
			"backend": "192.168.0.1"
		},
		{
			"table": "t1_0003",
			"segment": "384-512",
			"backend": "192.168.0.1"
		},
		{
			"table": "t1_0004",
			"segment": "512-640",
			"backend": "192.168.0.1"
		},
		{
			"table": "t1_0005",
			"segment": "640-819",
			"backend": "192.168.0.1"
		},
		{
			"table": "t1_0006",
			"segment": "819-947",
			"backend": "192.168.0.2"
		},
		{
			"table": "t1_0007",
			"segment": "947-1075",
			"backend": "192.168.0.2"
		},
		{
			"table": "t1_0008",
			"segment": "1075-1203",
			"backend": "192.168.0.2"
		},
		{
			"table": "t1_0009",
			"segment": "1203-1331",
			"backend": "192.168.0.2"
		},
		{
			"table": "t1_0010",
			"segment": "1331-1459",
			"backend": "192.168.0.2"
		},
		{
			"table": "t1_0011",
			"segment": "1459-1638",
			"backend": "192.168.0.2"
		},
		{
			"table": "t1_0012",
			"segment": "1638-1766",
			"backend": "192.168.0.3"
		},
		{
			"table": "t1_0013",
			"segment": "1766-1894",
			"backend": "192.168.0.3"
		},
		{
			"table": "t1_0014",
			"segment": "1894-2022",
			"backend": "192.168.0.3"
		},
		{
			"table": "t1_0015",
			"segment": "2022-2150",
			"backend": "192.168.0.3"
		},
		{
			"table": "t1_0016",
			"segment": "2150-2278",
			"backend": "192.168.0.3"
		},
		{
			"table": "t1_0017",
			"segment": "2278-2457",
			"backend": "192.168.0.3"
		},
		{
			"table": "t1_0018",
			"segment": "2457-2585",
			"backend": "192.168.0.4"
		},
		{
			"table": "t1_0019",
			"segment": "2585-2713",
			"backend": "192.168.0.4"
		},
		{
			"table": "t1_0020",
			"segment": "2713-2841",
			"backend": "192.168.0.4"
		},
		{
			"table": "t1_0021",
			"segment": "2841-2969",
			"backend": "192.168.0.4"
		},
		{
			"table": "t1_0022",
			"segment": "2969-3097",
			"backend": "192.168.0.4"
		},
		{
			"table": "t1_0023",
			"segment": "3097-3276",
			"backend": "192.168.0.4"
		},
		{
			"table": "t1_0024",
			"segment": "3276-3404",
			"backend": "192.168.0.5"
		},
		{
			"table": "t1_0025",
			"segment": "3404-3532",
			"backend": "192.168.0.5"
		},
		{
			"table": "t1_0026",
			"segment": "3532-3660",
			"backend": "192.168.0.5"
		},
		{
			"table": "t1_0027",
			"segment": "3660-3788",
			"backend": "192.168.0.5"
		},
		{
			"table": "t1_0028",
			"segment": "3788-3916",
			"backend": "192.168.0.5"
		},
		{
			"table": "t1_0029",
			"segment": "3916-4096",
			"backend": "192.168.0.5"
		}
	]
}`
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)

	backends := []string{
		"192.168.0.1",
		"192.168.0.2",
		"192.168.0.3",
		"192.168.0.4",
		"192.168.0.5",
	}
	got, err := router.HashUniform("t1", "id", backends, sqlparser.NewIntVal([]byte("32")))
	assert.Nil(t, err)
	//config.WriteConfig("/tmp/c.json", got)
	want, err := config.ReadTableConfig(datas)
	assert.Nil(t, err)
	assert.Equal(t, want, got)
}

func TestRouterComputeHashError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	// backends is NULL.
	{
		assert.NotNil(t, router)
		backends := []string{}
		_, err := router.HashUniform("t1", "id", backends, nil)
		assert.NotNil(t, err)
	}
	// backends is too manys.
	{
		assert.NotNil(t, router)
		backends := []string{}
		for i := 0; i < router.conf.Slots; i++ {
			backends = append(backends, fmt.Sprintf("%d", i))
		}
		_, err := router.HashUniform("t1", "id", backends, nil)
		assert.NotNil(t, err)
	}
}

func TestRouterComputeHashError1(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	// Table is null.
	{
		assert.NotNil(t, router)
		backends := []string{"backend1"}
		_, err := router.HashUniform("", "id", backends, nil)
		assert.NotNil(t, err)
	}

	// Shardkey is null.
	{
		assert.NotNil(t, router)
		backends := []string{"backend1"}
		_, err := router.HashUniform("t1", "", backends, nil)
		assert.NotNil(t, err)
	}

	// PartitionNum invaild.
	{
		assert.NotNil(t, router)
		backends := []string{"backend1"}
		_, err := router.HashUniform("t1", "id", backends, sqlparser.NewIntVal([]byte("4")))
		assert.NotNil(t, err)
	}

	// PartitionNum type invaild.
	{
		assert.NotNil(t, router)
		backends := []string{"backend1"}
		_, err := router.HashUniform("t1", "id", backends, sqlparser.NewIntVal([]byte("1.2")))
		assert.NotNil(t, err)
	}
}

func TestRouterComputeGlobalError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	// backends is NULL.
	{
		assert.NotNil(t, router)
		backends := []string{}
		_, err := router.GlobalUniform("t1", backends)
		assert.NotNil(t, err)
	}

	// Table is null.
	{
		assert.NotNil(t, router)
		backends := []string{"backend1"}
		_, err := router.GlobalUniform("", backends)
		assert.NotNil(t, err)
	}
}

func TestRouterComputeGlobal(t *testing.T) {
	datas := `{
	"name": "t1",
	"shardtype": "GLOBAL",
	"shardkey": "",
	"partitions": [
		{
			"table": "t1",
			"segment": "",
			"backend": "192.168.0.1"
		},
		{
			"table": "t1",
			"segment": "",
			"backend": "192.168.0.2"
		},
		{
			"table": "t1",
			"segment": "",
			"backend": "192.168.0.3"
		}
	]
}`
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)

	backends := []string{
		"192.168.0.1",
		"192.168.0.2",
		"192.168.0.3",
	}
	got, err := router.GlobalUniform("t1", backends)
	assert.Nil(t, err)
	want, err := config.ReadTableConfig(datas)
	assert.Nil(t, err)
	assert.Equal(t, want, got)
}

func TestRouterComputeSingle(t *testing.T) {
	datas := `{
	"name": "t1",
	"shardtype": "SINGLE",
	"shardkey": "",
	"partitions": [
		{
			"table": "t1",
			"segment": "",
			"backend": "192.168.0.1"
		}
	]
}`
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)

	backends := []string{
		"192.168.0.1",
		"192.168.0.2",
		"192.168.0.3",
	}
	got, err := router.SingleUniform("t1", backends)
	assert.Nil(t, err)
	want, err := config.ReadTableConfig(datas)
	assert.Nil(t, err)
	assert.Equal(t, want, got)
}

func TestRouterComputeSingleError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	// backends is NULL.
	{
		assert.NotNil(t, router)
		backends := []string{}
		_, err := router.SingleUniform("t1", backends)
		assert.NotNil(t, err)
	}

	// Table is null.
	{
		assert.NotNil(t, router)
		backends := []string{"backend1"}
		_, err := router.SingleUniform("", backends)
		assert.NotNil(t, err)
	}
}

func TestRouterComputeListError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	// Shardkey is NULL.
	{
		assert.NotNil(t, router)
		_, err := router.ListUniform("t1", "", sqlparser.PartitionDefinitions{})
		assert.NotNil(t, err)
	}

	// Table is NULL.
	{
		assert.NotNil(t, router)
		_, err := router.ListUniform("", "i", sqlparser.PartitionDefinitions{})
		assert.NotNil(t, err)
	}

	// different backends with the same list value.
	{
		partitionDef := sqlparser.PartitionDefinitions{
			&sqlparser.PartitionDefinition{
				Backend: "node1",
				Row:     sqlparser.ValTuple{sqlparser.NewStrVal([]byte("1"))},
			},
			&sqlparser.PartitionDefinition{
				Backend: "node2",
				Row:     sqlparser.ValTuple{sqlparser.NewIntVal([]byte("1"))},
			},
		}

		assert.NotNil(t, router)
		_, err := router.ListUniform("t1", "i", partitionDef)
		assert.NotNil(t, err)
	}

	// empty PartitionDefinitions
	{
		assert.NotNil(t, router)
		_, err := router.ListUniform("t1", "i", sqlparser.PartitionDefinitions{})
		assert.NotNil(t, err)
	}
}

func TestRouterComputeList(t *testing.T) {
	datas := `{
	"name": "l",
	"shardtype": "LIST",
	"shardkey": "id",
	"partitions": [
		{
			"table": "l_0000",
			"segment": "",
			"backend": "node1",
			"listvalue": "2"
		},
		{
			"table": "l_0001",
			"segment": "",
			"backend": "node2",
			"listvalue": "4"
		},
		{
			"table": "l_0002",
			"segment": "",
			"backend": "node3",
			"listvalue": "6"
		}
	]
}`
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	assert.NotNil(t, router)

	partitionDef := sqlparser.PartitionDefinitions{
		&sqlparser.PartitionDefinition{
			Backend: "node1",
			Row:     sqlparser.ValTuple{sqlparser.NewStrVal([]byte("2"))},
		},
		&sqlparser.PartitionDefinition{
			Backend: "node2",
			Row:     sqlparser.ValTuple{sqlparser.NewIntVal([]byte("4"))},
		},
		&sqlparser.PartitionDefinition{
			Backend: "node3",
			Row:     sqlparser.ValTuple{sqlparser.NewIntVal([]byte("6"))},
		},
	}

	got, err := router.ListUniform("l", "id", partitionDef)
	assert.Nil(t, err)
	want, err := config.ReadTableConfig(datas)
	assert.Nil(t, err)

	assert.EqualValues(t, got.Name, want.Name)
	assert.EqualValues(t, got.ShardKey, want.ShardKey)
	assert.EqualValues(t, got.ShardType, want.ShardType)
	for _, gotPartition := range got.Partitions {
		for _, wantPartition := range want.Partitions {
			if wantPartition.Backend == gotPartition.Backend {
				assert.EqualValues(t, wantPartition.ListValue, gotPartition.ListValue)
			}
		}
	}
}
