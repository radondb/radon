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

	"github.com/stretchr/testify/assert"
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

	// Add 1.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateTable("test", "t1", "id", backends)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
	}

	// Add 2.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2"}
		err := router.CreateTable("test", "t2", "id", backends)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t2"))
	}

	// Add 2.
	{
		backends := []string{"backend1", "backend2"}
		err := router.CreateTable("test", "t2", "id", backends)
		assert.NotNil(t, err)
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
	}
}

func TestFrmTableError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	// Add 1.
	{
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateTable("", "t1", "id", backends)
		assert.NotNil(t, err)
	}

	// Add 2.
	{
		backends := []string{"backend1", "backend2"}
		err := router.CreateTable("test", "", "id", backends)
		assert.NotNil(t, err)
	}

	// Add 2.
	{
		backends := []string{"backend1", "backend2"}
		err := router.CreateTable("test", "t2", "", backends)
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
		err := router.CreateTable("test", "t1", "id", backends)
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

	// Add 1.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateTable("test", "t1", "id", backends)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
	}

	// Add 2.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2"}
		err := router.CreateTable("test", "t2", "id", backends)
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

	// Add 1.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateTable("test", "t1", "id", backends)
		assert.Nil(t, err)
		assert.True(t, checkFileExistsForTest(tmpRouter, "test", "t1"))
	}

	// Add 2.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2"}
		err := router.CreateTable("test", "t2", "id", backends)
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
		_, err := router.readFrmData("/u10000/xx.xx")
		assert.NotNil(t, err)
	}
}

func TestFrmWriteFrmError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()
	{
		router.metadir = "/u100000/xx"
		err := router.writeFrmData("test", "t1", nil)
		assert.NotNil(t, err)
	}
}

func TestFrmReadFileBroken(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	router, cleanup := MockNewRouter(log)
	defer cleanup()

	// Add 1.
	{
		tmpRouter := router
		backends := []string{"backend1", "backend2", "backend3"}
		err := router.CreateTable("test", "t1", "id", backends)
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
