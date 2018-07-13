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
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestRouterCompute(t *testing.T) {
	datas := `{
	"name": "t1",
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
	got, err := router.HashUniform("t1", "id", backends)
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
		_, err := router.HashUniform("t1", "id", backends)
		assert.NotNil(t, err)
	}
	// backends is too manys.
	{
		assert.NotNil(t, router)
		backends := []string{}
		for i := 0; i < router.conf.Slots; i++ {
			backends = append(backends, fmt.Sprintf("%d", i))
		}
		_, err := router.HashUniform("t1", "id", backends)
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
		_, err := router.HashUniform("", "id", backends)
		assert.NotNil(t, err)
	}

	// Shardkey is null.
	{
		assert.NotNil(t, router)
		backends := []string{"backend1"}
		_, err := router.HashUniform("t1", "", backends)
		assert.NotNil(t, err)
	}
}
