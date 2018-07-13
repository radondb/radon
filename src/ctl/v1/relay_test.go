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
	"sync"
	"testing"
	"time"

	"proxy"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/ant0ine/go-json-rest/rest/test"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestCtlV1Relay(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.ERROR))
	fakedbs, proxy, cleanup := proxy.MockProxyWithBackup(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert .*", &sqltypes.Result{})
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	var wg sync.WaitGroup
	{
		n := 1000
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := driver.NewConn("mock", "mock", address, "", "utf8")
			assert.Nil(t, err)
			for i := 0; i < n; i++ {
				query := "insert into test.t1(id, b)values(1,1)"
				_, err = client.FetchAll(query, -1)
				assert.Nil(t, err)
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := driver.NewConn("mock", "mock", address, "", "utf8")
			assert.Nil(t, err)
			for i := 0; i < n; i++ {
				query := "insert into test.t1(id, b)values(2,2)"
				_, err = client.FetchAll(query, -1)
				assert.Nil(t, err)
			}
		}()
	}
	wg.Wait()
	time.Sleep(time.Second)

	// Relay status.
	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Get("/v1/relay/status", RelayStatusHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("GET", "http://localhost/v1/relay/status", nil))
		recorded.CodeIs(200)

		got := recorded.Recorder.Body.String()
		log.Debug(got)
		assert.True(t, strings.Contains(got, "true"))
	}

	// Stop relay worker.
	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Put("/v1/relay/stop", RelayStopHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		{
			recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("PUT", "http://localhost/v1/relay/stop", nil))
			recorded.CodeIs(200)
		}
	}

	// Relay status.
	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Get("/v1/relay/status", RelayStatusHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("GET", "http://localhost/v1/relay/status", nil))
		recorded.CodeIs(200)

		got := recorded.Recorder.Body.String()
		log.Info(got)
		assert.True(t, strings.Contains(got, "false"))
	}
	// Start relay worker.
	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Put("/v1/relay/start", RelayStartHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		{
			recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("PUT", "http://localhost/v1/relay/start", nil))
			recorded.CodeIs(200)
		}
	}

	// Relay status.
	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Get("/v1/relay/status", RelayStatusHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("GET", "http://localhost/v1/relay/status", nil))
		recorded.CodeIs(200)

		got := recorded.Recorder.Body.String()
		log.Info(got)
		assert.True(t, strings.Contains(got, "true"))
	}

	// Relay infos.
	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Get("/v1/relay/infos", RelayInfosHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("GET", "http://localhost/v1/relay/infos", nil))
		recorded.CodeIs(200)

		got := recorded.Recorder.Body.String()
		log.Info(got)
		assert.True(t, strings.Contains(got, "SecondBehinds"))
	}

	// Relay set max workers.
	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Post("/v1/relay/workers", RelayWorkersHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		// client
		p := &workersParams{
			Workers: 1,
		}
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/relay/workers", p))
		recorded.CodeIs(200)
	}
	// Relay set max workers error.
	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Post("/v1/relay/workers", RelayWorkersHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		// client
		p := &workersParams{
			Workers: 0,
		}
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/relay/workers", p))
		recorded.CodeIs(500)
	}
}

func TestCtlV1RelayReset(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.ERROR))
	fakedbs, proxy, cleanup := proxy.MockProxyWithBackup(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert .*", &sqltypes.Result{})
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	time.Sleep(time.Second)

	// Stop relay worker.
	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Put("/v1/relay/stop", RelayStopHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		{
			recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("PUT", "http://localhost/v1/relay/stop", nil))
			recorded.CodeIs(200)
		}
	}

	// Relay reset GTID.
	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Post("/v1/relay/reset", RelayResetHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		// client
		p := &resetParams{
			GTID: time.Now().UTC().UnixNano(),
		}
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/relay/reset", p))
		recorded.CodeIs(200)
	}

	// Relay reset GTID error.
	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Post("/v1/relay/reset", RelayResetHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		// client
		p := &resetParams{
			GTID: 2017,
		}
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/relay/reset", p))
		recorded.CodeIs(500)
	}
	// Start relay worker.
	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Put("/v1/relay/start", RelayStartHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		{
			recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("PUT", "http://localhost/v1/relay/start", nil))
			recorded.CodeIs(200)
		}
	}

	// Relay reset GTID.
	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Post("/v1/relay/reset", RelayResetHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		// client
		p := &resetParams{
			GTID: time.Now().UTC().UnixNano(),
		}
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/relay/reset", p))
		recorded.CodeIs(500)
	}

}

func TestCtlV1RelayParallelType(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.ERROR))
	fakedbs, proxy, cleanup := proxy.MockProxyWithBackup(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create table .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert .*", &sqltypes.Result{})
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	time.Sleep(time.Second)

	// Enable.
	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Put("/v1/relay/paralleltype", RelayParallelTypeHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		for i := 0; i < 100; i++ {
			p := &parallelTypeParams{
				Type: int32(i % 5),
			}
			recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("PUT", "http://localhost/v1/relay/paralleltype", p))
			recorded.CodeIs(200)
		}
	}
}
