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

type peerParams struct {
	Address string `json:"address"`
}

// AddPeerHandler impl.
func AddPeerHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		addPeerHandler(log, proxy, w, r)
	}
	return f
}

func addPeerHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	syncer := proxy.Syncer()
	p := peerParams{}
	err := r.DecodeJsonPayload(&p)
	if err != nil {
		log.Error("api.v1.add.peer.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Warning("api.v1.add.peer[%+v].from[%v]", p, r.RemoteAddr)

	if err := syncer.AddPeer(p.Address); err != nil {
		log.Error("api.v1.add.peer[%+v].error:%+v", p, err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// RemovePeerHandler impl.
func RemovePeerHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		removePeerHandler(log, proxy, w, r)
	}
	return f
}

func removePeerHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	syncer := proxy.Syncer()
	p := peerParams{}
	err := r.DecodeJsonPayload(&p)
	if err != nil {
		log.Error("api.v1.remove.peer.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Warning("api.v1.remove.peer[%+v].from[%v]", p, r.RemoteAddr)

	if err := syncer.RemovePeer(p.Address); err != nil {
		log.Error("api.v1.remove.peer[%+v].error:%+v", p, err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// PeerzHandler impl.
func PeerzHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		peerzHandler(log, proxy, w, r)
	}
	return f
}

func peerzHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	syncer := proxy.Syncer()
	w.WriteJson(syncer.Peers())
}
