package syncer

import (
	"backend"
	"context"
	"crypto/sha1"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"config"
	"router"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func mockSyncer(log *xlog.Log, n int) ([]*Syncer, func()) {
	var peers []string
	var httpServers []*http.Server
	var syncers []*Syncer

	getCurrDir := func() string {
		dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
		if err != nil {
			log.Panicf("mock.getcurrent.directory.err:%+v", err)
		}
		return dir
	}
	dir := getCurrDir()

	for i := 0; i < n; i++ {
		var metadir string
		metadir = dir + "/radon_test_syncer_meta" + strconv.Itoa(i)

		os.Mkdir(metadir, 0777)
		peerAddr := fmt.Sprintf("127.0.0.1:%d", 8081+i)

		// scatter.
		conf1 := backend.MockBackendConfigDefault(fmt.Sprintf("node%d", i), peerAddr)
		scatter := backend.NewScatter(log, metadir)
		if err := scatter.Add(conf1); err != nil {
			log.Panicf("mock.syncer.error:%+v", err)
		}
		scatter.FlushConfig()

		// router.
		router := router.NewRouter(log, metadir, config.DefaultRouterConfig())
		db := fmt.Sprintf("sbtest%d", i)
		tbl := fmt.Sprintf("t%d", i)
		if err := router.CreateTable(db, tbl, "id", []string{peerAddr}); err != nil {
			log.Panicf("mock.syncer.error:%+v", err)
		}

		syncer := NewSyncer(log, metadir, peerAddr, router, scatter)
		syncer.Init()
		syncers = append(syncers, syncer)
		peers = append(peers, peerAddr)
		httpSvr := mockHTTP(log, syncer, mockVersions, mockMetas)
		httpServers = append(httpServers, httpSvr)
	}

	// Add peers for each syncer.
	for _, syncer := range syncers {
		for _, peer := range peers {
			if err := syncer.AddPeer(peer); err != nil {
				log.Panicf("mock.syncer.error:%+v", err)
			}
		}
	}

	return syncers, func() {
		// Check the SHA of the syncers's metadir.
		var oldSha1 [20]byte
		for i := 0; i < n; i++ {
			syncer := syncers[i]
			sha1 := mockSHA(log, syncer)
			if i != 0 {
				if oldSha1 != sha1 {
					log.Panic("syncer.mock.check.sha.error:oldsha1[%+v],sha1:[%+v]", oldSha1, sha1)
				}
			}
			oldSha1 = sha1
			syncer.Close()
			os.RemoveAll(syncer.metadir + "/")

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			httpServers[i].Shutdown(ctx)
		}
	}
}

type mockHandler func(log *xlog.Log, syncer *Syncer) rest.HandlerFunc

func mockHTTP(log *xlog.Log, syncer *Syncer, version mockHandler, metas mockHandler) *http.Server {
	api := rest.NewApi()
	api.Use(rest.DefaultDevStack...)

	router, err := rest.MakeRouter(
		rest.Get("/v1/meta/versions", version(log, syncer)),
		rest.Get("/v1/meta/metas", metas(log, syncer)),
	)
	if err != nil {
		log.Panicf("mock.rest.make.router.error:%+v", err)
	}
	api.SetApp(router)
	handlers := api.MakeHandler()
	h := &http.Server{Addr: syncer.peer.self, Handler: handlers}
	go func() {
		if err := h.ListenAndServe(); err != nil {
			log.Error("mock.rest.error:%+v", err)
			return
		}
	}()
	time.Sleep(time.Millisecond * 100)
	return h
}

func mockVersions(log *xlog.Log, syncer *Syncer) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		version := &config.Version{
			Ts: config.ReadVersion(syncer.metadir),
		}
		log.Debug("syncer.mock.version.handle.call:%+v.", version)
		w.WriteJson(version)
	}
	return f
}

func mockMetas(log *xlog.Log, syncer *Syncer) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		meta, err := syncer.MetaJSON()
		if err != nil {
			log.Panicf("mock.metas.meta.json.error:%+v", err)
		}
		log.Debug("syncer.mock.metas.handle.call:%+v.", meta)
		w.WriteJson(meta)
	}
	return f
}

func mockSHA(log *xlog.Log, syncer *Syncer) [20]byte {
	var datas []byte
	if err := filepath.Walk(syncer.metadir, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			data, err := readFile(log, path)
			if err != nil {
				log.Panicf("mock.sha.read.error:%+v", err)
			}
			datas = append(datas, []byte(data)...)
		}
		return nil
	}); err != nil {
		log.Panicf("mock.sha.read.error:%+v", err)
	}
	return sha1.Sum(datas)
}
