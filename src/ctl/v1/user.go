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
	"math/rand"
	"net/http"

	"proxy"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/xelabs/go-mysqlstack/xlog"
	"strings"
)

type userParams struct {
	Databases string `json:"databases"`
	User      string `json:"user"`
	Password  string `json:"password"`
	Privilege string `json:"privilege"`
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

	if len(p.User) == 0 || len(p.Password) == 0 {
		log.Error("api.v1.create.user[%+v].error:some param is empty", p)
		rest.Error(w, "some args are empty", http.StatusNoContent)
		return
	}

	if p.Databases == "" {
		p.Databases = "*"
	}

	log.Warning("api.v1.create.user[from:%v].[%v]", r.RemoteAddr, p)
	databases := strings.TrimSuffix(p.Databases, ",")
	dbList := strings.Split(databases, ",")
	priv := p.Privilege
	if priv == "" {
		priv = "ALL"
	}
	for _, db := range dbList {
		query := fmt.Sprintf("GRANT %s ON %s.* TO '%s'@'%%' IDENTIFIED BY '%s'", priv, db, p.User, p.Password)
		if _, err := spanner.ExecuteScatter(query); err != nil {
			log.Error("api.v1.create.user[%+v].error:%+v", p, err)
			rest.Error(w, err.Error(), http.StatusServiceUnavailable)
			break
		}
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

	query := fmt.Sprintf("ALTER USER '%s'@'%%' IDENTIFIED BY '%s'", p.User, p.Password)
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

	query := fmt.Sprintf("DROP USER '%s'@'%%'", p.User)
	if _, err := spanner.ExecuteScatter(query); err != nil {
		log.Error("api.v1.drop.user[%+v].error:%+v", p.User, err)
		rest.Error(w, err.Error(), http.StatusServiceUnavailable)
	}
}

// UserzHandler impl.
func UserzHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		userzHandler(log, proxy, w, r)
	}
	return f
}

func userzHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	scatter := proxy.Scatter()
	spanner := proxy.Spanner()
	backends := scatter.Backends()
	backend := backends[rand.Intn(len(backends))]
	log.Warning("api.v1.userz[from:%v]", r.RemoteAddr)

	query := "SELECT User,Host,Super_priv FROM mysql.user"
	qr, err := spanner.ExecuteOnThisBackend(backend, query)
	if err != nil {
		log.Error("api.v1.userz.get.mysql.user[from.backend:%v].error:%+v", backend, err)
		rest.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	type UserInfo struct {
		User      string
		Host      string
		SuperPriv string
	}
	var Users = make([]UserInfo, len(qr.Rows))
	for i, row := range qr.Rows {
		Users[i].User = string(row[0].Raw())
		Users[i].Host = string(row[1].Raw())
		Users[i].SuperPriv = string(row[2].Raw())
	}

	w.WriteJson(Users)
}
