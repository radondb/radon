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
	"os"
	"path"
	"testing"

	"config"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func checkFileExistsForTest(router *Router, db, table string) bool {
	file := path.Join(router.metadir, db, fmt.Sprintf("%s.json", table))
	if _, err := os.Stat(file); err != nil {
		return false
	}
	return true
}

func makeFileBrokenForTest(router *Router, db, table string) {
	file := path.Join(router.metadir, db, fmt.Sprintf("%s.json", table))
	fd, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	fd.Write([]byte("wtf"))
	fd.Close()
}

func TestFrmTable(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	router.CreateDatabase("test")

	// Add 1.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateHashTable("test", "t1", "id", TableTypePartitionHash, backends, nil, &Extra{&config.AutoIncrement{"id"}})
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
	}

	// Add 2.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2"}
		err := router.CreateHashTable("test", "t2", "id", TableTypePartitionHash, backends, sqlparser.NewIntVal([]byte("16")), nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t2"))
	}

	// Add 2.
	{
		backends := []string{"backend1", "backend2"}
		err := router.CreateHashTable("test", "t2", "id", TableTypePartitionHash, backends, nil, nil)
		assert.NotNil(t, err)
	}

	// Add table error, table name include "/".
	{
		backends := []string{"backend1", "backend2"}
		err := router.CreateHashTable("test", "t2/t/t", "id", TableTypePartitionHash, backends, sqlparser.NewIntVal([]byte("16")), nil)
		assert.EqualError(t, err, "invalid.table.name.currently.not.support.tablename[t2/t/t].contains.with.char:'/' or space ' '")
	}

	// table name to long error
	{
		backends := []string{"backend1", "backend2"}
		err := router.CreateHashTable("test", "t012345678901234567890123456789012345678901234567890123456789", "id", TableTypePartitionHash, backends, sqlparser.NewIntVal([]byte("16")), nil)
		assert.EqualError(t, err, "Identifier name 't012345678901234567890123456789012345678901234567890123456789' is too long (errno 1059) (sqlstate 42000)")
	}

	// Add global table.
	{
		backends := []string{"backend1", "backend2"}
		err := router.CreateNonPartTable("test", "t3", TableTypeGlobal, backends, &Extra{&config.AutoIncrement{"id"}})
		assert.Nil(t, err)
	}

	// Add single table.
	{
		backends := []string{"backend1", "backend2"}
		err := router.CreateNonPartTable("test", "t3_single", TableTypeSingle, backends, nil)
		assert.Nil(t, err)
	}

	// Add list table.
	{
		partitionDef := sqlparser.PartitionOptions{
			&sqlparser.PartitionDefinition{
				Backend: "node1",
				Row:     sqlparser.ValTuple{sqlparser.NewStrVal([]byte("2"))},
			},
			&sqlparser.PartitionDefinition{
				Backend: "node2",
				Row:     sqlparser.ValTuple{sqlparser.NewIntVal([]byte("4"))},
			},
		}

		err := router.CreateListTable("test", "l", "id", TableTypePartitionList, partitionDef, nil)
		assert.Nil(t, err)
	}

	// Add partition table.
	{
		backends := []string{"backend1", "backend2"}
		err := router.CreateHashTable("test", "t3_partition", "shardkey1", TableTypePartitionHash, backends, nil, nil)
		assert.Nil(t, err)
	}

	// Remove 2.
	{
		tmpRouter := router
		err := router.DropTable("test", "t2")
		assert.Nil(t, err)
		assert.False(t, checkFileExistsForTest(tmpRouter, "test", "t2"))
	}

	// Refresh table.
	{
		{
			err := router.RefreshTable("test", "t1")
			assert.Nil(t, err)
		}

		{
			err := router.RefreshTable("test", "t2")
			assert.NotNil(t, err)
		}

		{
			err := router.RefreshTable("test", "t3")
			assert.Nil(t, err)
		}
	}
}

func TestFrmTableError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	router.CreateDatabase("test")

	// Add 1.
	{
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateHashTable("", "t1", "id", TableTypePartitionHash, backends, nil, nil)
		assert.NotNil(t, err)
	}

	// Add 2.
	{
		backends := []string{"backend1", "backend2"}
		err := router.CreateHashTable("test", "", "id", TableTypePartitionHash, backends, nil, nil)
		assert.NotNil(t, err)
	}

	// unsupported table type.
	{
		backends := []string{"backend1", "backend2"}
		err := router.CreateHashTable("test", "t1", "id", TableTypeSingle, backends, nil, nil)
		assert.NotNil(t, err)
	}

	// unsupported table type.
	{
		backends := []string{"backend1", "backend2"}
		err := router.CreateNonPartTable("test", "t1", TableTypePartitionHash, backends, nil)
		assert.NotNil(t, err)
	}

	// Add single table.
	{
		backends := []string{"backend1", "backend2"}
		err := router.CreateNonPartTable("test", "", TableTypeSingle, backends, nil)
		assert.NotNil(t, err)
	}

	// Add global table.
	{
		backends := []string{"backend1", "backend2"}
		err := router.CreateNonPartTable("test", "", TableTypeGlobal, backends, nil)
		assert.NotNil(t, err)
	}

	// Drop table.
	{
		err := router.DropTable("testxx", "t2")
		assert.NotNil(t, err)
	}

	// Add 1.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateHashTable("test", "t1", "id", TableTypePartitionHash, backends, nil, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
	}

	// Drop table.
	{
		router.metadir = "/u00000000001/"
		err := router.DropTable("test", "t1")
		assert.NotNil(t, err)
	}
}

func TestFrmDropDatabase(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	router.CreateDatabase("test")

	// Add 1.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateHashTable("test", "t1", "id", TableTypePartitionHash, backends, nil, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
	}

	// Add 2.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2"}
		err := router.CreateHashTable("test", "t2", "id", TableTypePartitionHash, backends, nil, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t2"))
	}

	{
		tmpRouter := router
		err := router.DropDatabase("test")
		assert.Nil(t, err)
		assert.False(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
		assert.False(t, checkFileExistsForTest(tmpRouter, "test", "t2"))
	}
}

func TestFrmLoad(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	router.CreateDatabase("test")

	// Add 1.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateHashTable("test", "t1", "id", TableTypePartitionHash, backends, nil, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
	}

	// Add 2.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2"}
		err := router.CreateHashTable("test", "t2", "id", TableTypePartitionHash, backends, nil, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t2"))
	}

	{
		router1, cleanup1 := MockNewRouter(log)
		defer cleanup1()
		assert.NotNil(t, router1)

		// load.
		err := router1.LoadConfig()
		assert.Nil(t, err)
		assert.Equal(t, router, router1)

		// load again.
		err = router1.LoadConfig()
		assert.Nil(t, err)
		assert.Equal(t, router, router1)
	}
}

func TestFrmReadFrmError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	{
		_, err := router.readTableFrmData("/u10000/xx.xx")
		assert.NotNil(t, err)
	}
}

func TestFrmWriteFrmError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	{
		router.metadir = "/u100000/xx"
		err := router.writeTableFrmData("test", "t1", nil)
		assert.NotNil(t, err)
	}
}

func TestFrmReadFileBroken(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	router.CreateDatabase("test")

	// Add 1.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateHashTable("test", "t1", "id", TableTypePartitionHash, backends, nil, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
		// Make file broken.
		makeFileBrokenForTest(tmpRouter, "test", "t1")
	}

	// Refresh table.
	{
		{
			err := router.RefreshTable("test", "t1")
			assert.NotNil(t, err)
		}

		{
			err := router.RefreshTable("test", "t2")
			assert.NotNil(t, err)
		}
	}
}

func TestFrmAddTableForTest(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	err := router.AddForTest("test", nil)
	assert.NotNil(t, err)
}

func TestFrmDatabaseNoTables(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	// Tables with database test1.
	router.CreateDatabase("test1")
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateHashTable("test1", "t1", "id", TableTypePartitionHash, backends, nil, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test1", "t1"))
	}

	// Database test2 without tables.
	router.CreateDatabase("test2")

	// Check.
	{
		router1, cleanup1 := MockNewRouter(log)
		defer cleanup1()
		assert.NotNil(t, router1)

		// load.
		err := router1.LoadConfig()
		assert.Nil(t, err)
		assert.Equal(t, router, router1)
	}

	err := router.CreateDatabase("test2")
	assert.NotNil(t, err)
}

func TestFrmTableRename(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	router.CreateDatabase("test")

	// Add 1.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateHashTable("test", "t1", "id", TableTypePartitionHash, backends, nil, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
	}

	// Add 2.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2"}
		err := router.CreateHashTable("test", "t2", "id", TableTypePartitionHash, backends, nil, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t2"))
	}

	// Rename 2.
	{
		tmpRouter := router
		err := router.RenameTable("test", "t2", "t3")
		assert.Nil(t, err)
		assert.False(t, checkFileExistsForTest(tmpRouter, "test", "t2"))
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t3"))
	}
}

func TestFrmTableRenameError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	router.CreateDatabase("test")

	// Add 1.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateHashTable("test", "t1", "id", TableTypePartitionHash, backends, nil, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
	}

	// Add 2.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2"}
		err := router.CreateHashTable("test", "t2", "id", TableTypePartitionHash, backends, nil, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t2"))
	}

	// Rename t3.
	{
		err := router.RenameTable("test", "t3", "t3")
		assert.NotNil(t, err)
	}

	{
		db := "test"
		fromTable := "t2"
		toTable := "t3"
		dir := path.Join(router.metadir, db)
		file := path.Join(dir, fmt.Sprintf("%s.json", fromTable))
		os.Remove(file)
		err := router.RenameTable(db, fromTable, toTable)
		assert.NotNil(t, err)
	}

	{
		db := "test"
		fromTable := "t1"
		toTable := "t4"
		dir := path.Join(router.metadir, db)
		file := path.Join(dir, fmt.Sprintf("%s.json", toTable))
		_, err := os.Create(file)
		err = os.Chmod(file, 0400)
		err = router.RenameTable("test", fromTable, toTable)
		assert.NotNil(t, err)
		err = os.Chmod(file, 0666)
	}
}

func TestFrmCheckDatabase(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	router.CreateDatabase("test")

	// Add 1.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateHashTable("test", "t1", "id", TableTypePartitionHash, backends, nil, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
	}

	router.CheckDatabase("test")
	router.CheckDatabase("test1")
}

func TestFrmCheckTable(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	router.CreateDatabase("test")

	// Add 1.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateHashTable("test", "t1", "id", TableTypePartitionHash, backends, nil, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
	}

	router.CheckTable("", "t1")
	router.CheckTable("test", "")
	router.CheckTable("", "")
	router.CheckTable("test", "t1")
	router.CheckTable("test1", "t1")
	router.CheckTable("test", "t3")
}

func TestFrmTableCreateListTable(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	router.CreateDatabase("test")

	// Add 1.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateHashTable("test", "t1", "id", TableTypePartitionHash, backends, nil, nil)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
	}

	// Add list table.
	{
		partitionDef := sqlparser.PartitionOptions{
			&sqlparser.PartitionDefinition{
				Backend: "node1",
				Row:     sqlparser.ValTuple{sqlparser.NewStrVal([]byte("2"))},
			},
			&sqlparser.PartitionDefinition{
				Backend: "node2",
				Row:     sqlparser.ValTuple{sqlparser.NewIntVal([]byte("4"))},
			},
		}

		err := router.CreateListTable("test", "l", "id", TableTypePartitionList, partitionDef, nil)
		assert.Nil(t, err)

		err = router.CreateListTable("test", "l", "", TableTypePartitionList, partitionDef, nil)
		assert.NotNil(t, err)

		err = router.CreateListTable("test", "l", "", TableTypePartitionHash, partitionDef, nil)
		assert.NotNil(t, err)

		err = router.CreateListTable("test", "l", "id", TableTypePartitionList, sqlparser.PartitionOptions{}, nil)
		assert.NotNil(t, err)

		err = router.CreateListTable("test", "l", "id", TableTypePartitionList, partitionDef, &Extra{&config.AutoIncrement{"id"}})
		assert.NotNil(t, err)
	}
}

func TestCreateDatabaseError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	err := router.CreateDatabase("")
	assert.EqualError(t, err, "router.database.should.not.be.empty")

	err = router.CreateDatabase("x/x/db")
	assert.EqualError(t, err, "invalid.database.name.currently.not.support.dbname[x/x/db].contains.with.char:'/' or space ' '")

	err = router.CreateDatabase("t0123456789012345678901234567890123456789012345678901234567890123")
	assert.EqualError(t, err, "Identifier name 't0123456789012345678901234567890123456789012345678901234567890123' is too long (errno 1059) (sqlstate 42000)")
}
