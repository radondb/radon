/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package v1

import (
	"testing"

	"proxy"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/ant0ine/go-json-rest/rest/test"
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
			User:           "mock",
			Password:       "pwd",
			MaxConnections: 1024,
		}
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/backend", p))
		recorded.CodeIs(200)
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

// backup
func TestCtlV1BackupAdd(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Post("/v1/radon/backup", AddBackupHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	{
		p := &backendParams{
			Name:           "backupnode",
			Address:        "192.168.0.1:3306",
			User:           "mock",
			Password:       "pwd",
			MaxConnections: 1024,
		}
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/backup", p))
		recorded.CodeIs(200)
	}
}

func TestCtlV1BackupAddError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxyWithBackup(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Post("/v1/radon/backup", AddBackupHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	// 500.
	{
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/backup", nil))
		recorded.CodeIs(500)
	}

	{
		p := &backendParams{
			Name:           "backupnode",
			Address:        "192.168.0.1:3306",
			User:           "mock",
			Password:       "pwd",
			MaxConnections: 1024,
		}
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/radon/backup", p))
		recorded.CodeIs(500)
	}
}

func TestCtlV1BackupRemove(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxyWithBackup(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Delete("/v1/radon/backup/:name", RemoveBackupHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	{
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("DELETE", "http://localhost/v1/radon/backup/backend4", nil))
		recorded.CodeIs(200)
	}
}

func TestCtlV1BackupRemoveError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Delete("/v1/radon/backup/:name", RemoveBackupHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	// 404.
	{
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("DELETE", "http://localhost/v1/radon/backup/xx", nil))
		recorded.CodeIs(500)
	}
}

func TestCtlV1BackupConfig(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxyWithBackup(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Get("/v1/radon/backupconfig", BackupConfigHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	{
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("GET", "http://localhost/v1/radon/backupconfig", nil))
		recorded.CodeIs(200)
	}
}
