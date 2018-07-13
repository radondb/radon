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
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestCtlV1PeerAdd(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Post("/v1/peer/add", AddPeerHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	{
		p := &peerParams{
			Address: "192.168.0.1:3306",
		}
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/peer/add", p))
		recorded.CodeIs(200)
	}
}

func TestCtlV1PeerAddError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Post("/v1/peer/add", AddPeerHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	// 500.
	{
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/peer/add", nil))
		recorded.CodeIs(500)
	}

	{
		p := &peerParams{}
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/peer/add", p))
		recorded.CodeIs(500)
	}
}

func TestCtlV1PeerRemove(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Post("/v1/peer/remove", RemovePeerHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	p := &peerParams{
		Address: "192.168.0.1:3306",
	}
	{
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/peer/remove", p))
		recorded.CodeIs(200)
	}
}

func TestCtlV1Peers(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	_, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()

	// server
	api := rest.NewApi()
	router, _ := rest.MakeRouter(
		rest.Post("/v1/peer/add", AddPeerHandler(log, proxy)),
	)
	api.SetApp(router)
	handler := api.MakeHandler()

	{
		p := &peerParams{
			Address: "192.168.0.1:3306",
		}
		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("POST", "http://localhost/v1/peer/add", p))
		recorded.CodeIs(200)
	}

	{
		api := rest.NewApi()
		router, _ := rest.MakeRouter(
			rest.Get("/v1/peer/peerz", PeerzHandler(log, proxy)),
		)
		api.SetApp(router)
		handler := api.MakeHandler()

		recorded := test.RunRequest(t, handler, test.MakeSimpleRequest("GET", "http://localhost/v1/peer/peerz", nil))
		recorded.CodeIs(200)

		want := "[\"127.0.0.1:8080\",\"192.168.0.1:3306\"]"
		got := recorded.Recorder.Body.String()
		log.Debug(got)
		assert.Equal(t, want, got)
	}
}
