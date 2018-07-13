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

	"config"
	"proxy"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// VersionzHandler impl.
func VersionzHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		versionzHandler(log, proxy, w, r)
	}
	return f
}

func versionzHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	syncer := proxy.Syncer()
	version := &config.Version{
		Ts: syncer.MetaVersion(),
	}
	w.WriteJson(version)
}

type versionCheck struct {
	Latest bool     `json:"latest"`
	Peers  []string `json:"peers"`
}

// VersionCheckHandler impl.
func VersionCheckHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		versionCheckHandler(log, proxy, w, r)
	}
	return f
}

func versionCheckHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	syncer := proxy.Syncer()
	latest, peers := syncer.MetaVersionCheck()
	check := &versionCheck{
		Latest: latest,
		Peers:  peers,
	}
	w.WriteJson(check)
}

// MetazHandler impl.
func MetazHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		metazHandler(log, proxy, w, r)
	}
	return f
}

func metazHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	sync := proxy.Syncer()
	meta, err := sync.MetaJSON()
	if err != nil {
		log.Error("api.v1.radon.flush.config.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.WriteJson(meta)
}
