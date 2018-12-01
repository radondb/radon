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

type backendParams struct {
	Name           string `json:"name"`
	Address        string `json:"address"`
	User           string `json:"user"`
	Password       string `json:"password"`
	MaxConnections int    `json:"max-connections"`
}

// AddBackendHandler impl.
func AddBackendHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		addBackendHandler(log, proxy, w, r)
	}
	return f
}

func addBackendHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	scatter := proxy.Scatter()
	p := backendParams{}
	err := r.DecodeJsonPayload(&p)
	if err != nil {
		log.Error("api.v1.add.backend.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	conf := &config.BackendConfig{
		Name:           p.Name,
		Address:        p.Address,
		User:           p.User,
		Password:       p.Password,
		Charset:        "utf8",
		MaxConnections: p.MaxConnections,
	}
	log.Warning("api.v1.add[from:%v].backend[%+v]", r.RemoteAddr, conf)

	if err := scatter.Add(conf); err != nil {
		log.Error("api.v1.add.backend[%+v].error:%+v", conf, err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := scatter.FlushConfig(); err != nil {
		log.Error("api.v1.add.backend.flush.config.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// RemoveBackendHandler impl.
func RemoveBackendHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		removeBackendHandler(log, proxy, w, r)
	}
	return f
}

func removeBackendHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	scatter := proxy.Scatter()
	backend := r.PathParam("name")
	conf := &config.BackendConfig{
		Name: backend,
	}
	log.Warning("api.v1.remove[from:%v].backend[%+v]", r.RemoteAddr, conf)

	if err := scatter.Remove(conf); err != nil {
		log.Error("api.v1.remove.backend[%+v].error:%+v", conf, err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := scatter.FlushConfig(); err != nil {
		log.Error("api.v1.remove.backend.flush.config.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
