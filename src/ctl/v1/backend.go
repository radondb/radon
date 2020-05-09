/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package v1

import (
	"fmt"
	"net/http"

	"config"
	"proxy"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/xelabs/go-mysqlstack/xlog"
)

type backendParams struct {
	Name           string `json:"name"`
	Address        string `json:"address"`
	Replica        string `json:"replica-address"`
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

func initBackend(proxy *proxy.Proxy, backend string, log *xlog.Log) error {
	spanner := proxy.Spanner()
	router := proxy.Router()

	// create db from router on the new backend, make sure the db not exists, or else return err.
	tblList := router.Tables()
	for db := range tblList {
		query := fmt.Sprintf("create database %s", db)
		_, err := spanner.ExecuteOnThisBackend(backend, query)
		if err != nil {
			log.Error("api.v1.add.backend.initBackend.error:%v", err)
			return err
		}
	}
	return nil
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
		Replica:        p.Replica,
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

	if err := initBackend(proxy, conf.Name, log); err != nil {
		log.Error("api.v1.add.backend.Init.error:%+v", err)
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
