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

	"proxy"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/xelabs/go-mysqlstack/xlog"
)

type userParams struct {
	User     string `json:"user"`
	Password string `json:"password"`
}

// CreateUserHandler impl.
func CreateUserHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		createUserHandler(log, proxy, w, r)
	}
	return f
}

func createUserHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	spanner := proxy.Spanner()
	p := userParams{}
	err := r.DecodeJsonPayload(&p)
	if err != nil {
		log.Error("api.v1.create.user.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Warning("api.v1.create.user[from:%v].[%v]", r.RemoteAddr, p)

	query := fmt.Sprintf("GRANT SELECT ON *.* TO '%s'@'localhost' IDENTIFIED BY '%s'", p.User, p.Password)
	if _, err := spanner.ExecuteScatter(query); err != nil {
		log.Error("api.v1.create.user[%+v].error:%+v", p, err)
		rest.Error(w, err.Error(), http.StatusServiceUnavailable)
	}
}

// AlterUserHandler impl.
func AlterUserHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		alterUserHandler(log, proxy, w, r)
	}
	return f
}

func alterUserHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	spanner := proxy.Spanner()
	p := userParams{}
	err := r.DecodeJsonPayload(&p)
	if err != nil {
		log.Error("api.v1.alter.user.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Warning("api.v1.alter.user[from:%v].[%v]", r.RemoteAddr, p)

	query := fmt.Sprintf("ALTER USER '%s'@'localhost' IDENTIFIED BY '%s'", p.User, p.Password)
	if _, err := spanner.ExecuteScatter(query); err != nil {
		log.Error("api.v1.alter.user[%+v].error:%+v", p, err)
		rest.Error(w, err.Error(), http.StatusServiceUnavailable)
	}
}

// DropUserHandler impl.
func DropUserHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		dropUserHandler(log, proxy, w, r)
	}
	return f
}

func dropUserHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	spanner := proxy.Spanner()
	p := userParams{}
	err := r.DecodeJsonPayload(&p)
	if err != nil {
		log.Error("api.v1.drop.user.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Warning("api.v1.drop.user[from:%v].[%v]", r.RemoteAddr, p)

	query := fmt.Sprintf("DROP USER '%s'@'localhost'", p.User)
	if _, err := spanner.ExecuteScatter(query); err != nil {
		log.Error("api.v1.drop.user[%+v].error:%+v", p.User, err)
		rest.Error(w, err.Error(), http.StatusServiceUnavailable)
	}
}
