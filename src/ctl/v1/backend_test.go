/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package v1

import (
	"errors"
	"testing"

	"backend"
	"proxy"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/ant0ine/go-json-rest/rest/test"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestCtlV1BackendAdd(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Post("/v1/radon/backend", AddBackendHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	{
		p := &backendParams{
			Name:           "backend6",
			Address:        "192.168.0.1:3306",
			Replica:        "192.168.0.2:3306",
			User:           "mock",
			Password:       "pwd",
			MaxConnections: 1024,
		}
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/backend", p))
		recorded.CodeIs(200)
	}

	// duplicate address.
	{
		p := &backendParams{
			Name:           "backend7",
			Address:        "192.168.0.1:3306",
			User:           "mock",
			Password:       "pwd",
			MaxConnections: 1024,
		}
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/backend", p))
		recorded.CodeIs(500)
	}
}

func TestCtlV1BackendAddError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Post("/v1/radon/backend", AddBackendHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	// 500.
	{
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/backend", nil))
		recorded.CodeIs(500)
	}

	{
		p := &backendParams{
			Name:           "backend1",
			Address:        "192.168.0.1:3306",
			User:           "mock",
			Password:       "pwd",
			MaxConnections: 1024,
		}
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/backend", p))
		recorded.CodeIs(500)
	}
}

func TestCtlV1BackendAddInitBackend(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
	}

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Post("/v1/radon/backend", AddBackendHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	// create database.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create database test"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	fakedb1, _, _, addrs, cleanup := backend.MockTxnMgr(log, 2)
	defer cleanup()
	backend1 := addrs[1]

	// fakedbs.
	{
		fakedb1.AddQueryPattern("create .*", &sqltypes.Result{})
	}

	p1 := &backendParams{
		Name:           backend1,
		Address:        backend1,
		User:           "mock",
		Password:       "pwd",
		MaxConnections: 1024,
	}
	recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/backend", p1))
	recorded.CodeIs(200)

	// fakedbs.
	{
		fakedb1.ResetAll()
		fakedb1.AddQueryErrorPattern("create .*", errors.New("mock.execute.error"))
	}

	backend2 := addrs[2]
	p2 := &backendParams{
		Name:           backend2,
		Address:        backend2,
		User:           "mock",
		Password:       "pwd",
		MaxConnections: 1024,
	}
	recorded = test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/backend", p2))
	recorded.CodeIs(500)
}

func TestCtlV1BackendRemove(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Delete("/v1/radon/backend/:name", RemoveBackendHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	{
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("DELETE", "http://localhost/v1/radon/backend/backend1", nil))
		recorded.CodeIs(200)
	}
}

func TestCtlV1BackendRemoveError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Delete("/v1/radon/backend/:name", RemoveBackendHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	// 404.
	{
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("DELETE", "http://localhost/v1/radon/backend/xx", nil))
		recorded.CodeIs(500)
	}
}
