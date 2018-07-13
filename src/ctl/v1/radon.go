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

type radonParams struct {
	MaxConnections *int     `json:"max-connections"`
	MaxResultSize  *int     `json:"max-result-size"`
	DDLTimeout     *int     `json:"ddl-timeout"`
	QueryTimeout   *int     `json:"query-timeout"`
	TwoPCEnable    *bool    `json:"twopc-enable"`
	AllowIP        []string `json:"allowip,omitempty"`
	AuditMode      *string  `json:"audit-mode"`
}

// RadonConfigHandler impl.
func RadonConfigHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		radonConfigHandler(log, proxy, w, r)
	}
	return f
}

func radonConfigHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	p := radonParams{}
	err := r.DecodeJsonPayload(&p)
	if err != nil {
		log.Error("api.v1.radon.config.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Warning("api.v1.radon[from:%v].body:%+v", r.RemoteAddr, p)
	if p.MaxConnections != nil {
		proxy.SetMaxConnections(*p.MaxConnections)
	}
	if p.MaxResultSize != nil {
		proxy.SetMaxResultSize(*p.MaxResultSize)
	}
	if p.DDLTimeout != nil {
		proxy.SetDDLTimeout(*p.DDLTimeout)
	}
	if p.QueryTimeout != nil {
		proxy.SetQueryTimeout(*p.QueryTimeout)
	}
	if p.TwoPCEnable != nil {
		proxy.SetTwoPC(*p.TwoPCEnable)
	}
	proxy.SetAllowIP(p.AllowIP)
	if p.AuditMode != nil {
		proxy.SetAuditMode(*p.AuditMode)
	}

	// reset the allow ip table list.
	proxy.IPTable().Refresh()

	// write to file.
	if err := proxy.FlushConfig(); err != nil {
		log.Error("api.v1.radon.flush.config.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type readonlyParams struct {
	ReadOnly bool `json:"readonly"`
}

// ReadonlyHandler impl.
func ReadonlyHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		readonlyHandler(log, proxy, w, r)
	}
	return f
}

func readonlyHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	p := readonlyParams{}
	err := r.DecodeJsonPayload(&p)
	if err != nil {
		log.Error("api.v1.readonly.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Warning("api.v1.readonly[from:%v].body:%+v", r.RemoteAddr, p)
	proxy.SetReadOnly(p.ReadOnly)
}

type twopcParams struct {
	Twopc bool `json:"twopc"`
}

// TwopcHandler impl.
func TwopcHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		twopcHandler(log, proxy, w, r)
	}
	return f
}

func twopcHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	p := twopcParams{}
	err := r.DecodeJsonPayload(&p)
	if err != nil {
		log.Error("api.v1.twopc.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Warning("api.v1.twopc[from:%v].body:%+v", r.RemoteAddr, p)
	proxy.SetTwoPC(p.Twopc)
}

type throttleParams struct {
	Limits int `json:"limits"`
}

// ThrottleHandler impl.
func ThrottleHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		throttleHandler(log, proxy, w, r)
	}
	return f
}

func throttleHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	p := throttleParams{}
	err := r.DecodeJsonPayload(&p)
	if err != nil {
		log.Error("api.v1.radon.throttle.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Warning("api.v1.radon.throttle[from:%v].body:%+v", r.RemoteAddr, p)
	proxy.SetThrottle(p.Limits)
}

// StatusHandler impl.
func StatusHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		statusHandler(log, proxy, w, r)
	}
	return f
}

func statusHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	spanner := proxy.Spanner()
	type status struct {
		ReadOnly bool `json:"readonly"`
	}
	statuz := &status{
		ReadOnly: spanner.ReadOnly(),
	}
	w.WriteJson(statuz)
}

// RestApiAddressHandler impl.
func RestApiAddressHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		type resp struct {
			Addr string `json:"address"`
		}
		rsp := &resp{Addr: proxy.PeerAddress()}
		w.WriteJson(rsp)
	}
	return f
}
