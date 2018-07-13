/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package v1

import (
	"net/http"

	"proxy"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// PingHandler impl.
func PingHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		pingHandler(log, proxy, w, r)
	}
	return f
}

func pingHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	spanner := proxy.Spanner()
	if _, err := spanner.ExecuteScatter("select 1"); err != nil {
		log.Error("api.v1.ping.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusServiceUnavailable)
	}
}
