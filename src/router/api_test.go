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
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestApiRules(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	// add router of sbtest.A
	{
		err := router.CreateDatabase("sbtest")
		assert.Nil(t, err)
		err = router.addTable("sbtest", MockTableAConfig())
		assert.Nil(t, err)

		tConf, err := router.TableConfig("sbtest", "A")
		assert.Nil(t, err)
		assert.NotNil(t, tConf)
	}
	rules := router.Rules()
	want := "sbtest"
	got := rules.Schemas[0].DB
	assert.Equal(t, want, got)
}

func TestApiPartitionRuleShift(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

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

	// Shift backend from backend8 to backend88 for sbtest/A8.
	{
		from := "backend8"
		to := "backend88"
		database := "sbtest"
		table := "A8"
		err := router.PartitionRuleShift(from, to, database, table)
		assert.Nil(t, err)
		want := `{
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
								"Backend": "backend88",
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
}`
		got := router.JSON()
		assert.Equal(t, want, got)
		log.Error("gry22222")
	}

	// Drop.
	{
		err := router.DropDatabase("sbtest")
		assert.Nil(t, err)
		log.Error("gry33333")
	}
}

func TestApiPartitionRuleShiftGlobal(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	err := router.CreateDatabase("sbtest")
	assert.Nil(t, err)

	// add router of sbtest.G
	{
		err := router.addTable("sbtest", MockTableGConfig())
		assert.Nil(t, err)

		tConf, err := router.TableConfig("sbtest", "G")
		assert.Nil(t, err)
		assert.NotNil(t, tConf)
	}

	// Shift backend from backend1 to backend0 for sbtest/G.
	{
		from := "backend1"
		to := "backend0"
		database := "sbtest"
		table := "G"
		err := router.PartitionRuleShift(from, to, database, table)
		assert.Nil(t, err)
		want := `{
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
							},
							{
								"Table": "G",
								"Backend": "backend0",
								"Range": {}
							}
						]
					}
				}
			}
		}
	}
}`
		got := router.JSON()
		assert.Equal(t, want, got)
	}

	// Shift backend from backend1 to backend3 for sbtest/G.
	{
		from := "backend1"
		to := "backend3"
		database := "sbtest"
		table := "G"
		err := router.PartitionRuleShift(from, to, database, table)
		assert.Nil(t, err)
		want := `{
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
							},
							{
								"Table": "G",
								"Backend": "backend0",
								"Range": {}
							},
							{
								"Table": "G",
								"Backend": "backend3",
								"Range": {}
							}
						]
					}
				}
			}
		}
	}
}`
		got := router.JSON()
		assert.Equal(t, want, got)
	}

	// Drop.
	{
		err := router.DropDatabase("sbtest")
		assert.Nil(t, err)
	}
}

func TestApiPartitionRuleShiftErrors(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	err := router.CreateDatabase("sbtest")
	assert.Nil(t, err)

	// add router of sbtest.A
	{
		err = router.addTable("sbtest", MockTableAConfig())
		assert.Nil(t, err)

		tConf, err := router.TableConfig("sbtest", "A")
		assert.Nil(t, err)
		assert.NotNil(t, tConf)
	}

	// from == to.
	{
		from := "backend8"
		to := "backend8"
		database := "sbtest"
		table := "A8"
		err := router.PartitionRuleShift(from, to, database, table)
		want := "router.rule.change.from[backend8].cant.equal.to[backend8]"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// database can't found.
	{
		from := "backend8"
		to := "backend88"
		database := "sbtestx"
		table := "A8"
		err := router.PartitionRuleShift(from, to, database, table)
		want := "router.rule.change.cant.found.database:sbtestx"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// table can't found.
	{
		from := "backend8"
		to := "backend88"
		database := "sbtest"
		table := "A88"
		err := router.PartitionRuleShift(from, to, database, table)
		want := "router.rule.change.cant.found.backend[backend8]+table:[A88]"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// add router of sbtest.G.
	{
		err := router.addTable("sbtest", MockTableGConfig())
		assert.Nil(t, err)

		tConf, err := router.TableConfig("sbtest", "G")
		assert.Nil(t, err)
		assert.NotNil(t, tConf)
	}

	// table config already exists.
	{
		from := "backend1"
		to := "backend2"
		database := "sbtest"
		table := "G"
		err := router.PartitionRuleShift(from, to, database, table)
		want := "the.table:[G].already.exists.in.the.backend[backend2]"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	// writeFrmData err.
	{
		router.metadir = "/u100000/xx"
		from := "backend8"
		to := "backend4"
		database := "sbtest"
		table := "A8"
		err := router.PartitionRuleShift(from, to, database, table)
		assert.NotNil(t, err)
	}

	// writeFrmData err.
	{
		from := "backend1"
		to := "backend3"
		database := "sbtest"
		table := "G"
		err := router.PartitionRuleShift(from, to, database, table)
		assert.NotNil(t, err)
	}
}

func TestApiReLoad(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	err := router.CreateDatabase("sbtest")
	assert.Nil(t, err)

	// add router of sbtest.A.
	{
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateHashTable("sbtest", "t1", "id", TableTypePartitionHash, backends, nil, nil)
		assert.Nil(t, err)
	}

	for i := 0; i < 13; i++ {
		err := router.ReLoad()
		assert.Nil(t, err)
	}

	rules := router.Rules()
	want := "sbtest"
	got := rules.Schemas[0].DB
	assert.Equal(t, want, got)
}
