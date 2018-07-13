/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package syncer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"config"
	"xbase"

	"github.com/xelabs/go-mysqlstack/common"
	"github.com/xelabs/go-mysqlstack/xlog"
)

const (
	// metaRestURL url.
	metaRestURL = "v1/meta/metas"

	// versionRestURL url.
	versionRestURL = "v1/meta/versions"
)

// Meta tuple.
type Meta struct {
	Metas map[string]string `json:"metas"`
}

// readFile used to read file from disk.
func readFile(log *xlog.Log, file string) (string, error) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		log.Error("syncer.meta.json.read.file[%s].error:%+v", file, err)
		return "", err
	}
	return common.BytesToString(data), nil
}

// writeFile used to write file to disk.
func writeFile(log *xlog.Log, file string, data string) error {
	err := xbase.WriteFile(file, common.StringToBytes(data))
	if err != nil {
		log.Error("syncer.write.file[%s].error:%+v", file, err)
		return err
	}
	return nil
}

// MetaVersion returns the meta version.
func (s *Syncer) MetaVersion() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return config.ReadVersion(s.metadir)
}

// MetaVersionCheck used to check the version is synced or not.
func (s *Syncer) MetaVersionCheck() (bool, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log := s.log
	maxVer := int64(0)
	self := s.peer.self
	peers := s.peer.Clone()
	for _, peer := range peers {
		if peer != self {
			versionURL := "http://" + path.Join(peer, versionRestURL)
			peerVerStr, err := xbase.HTTPGet(versionURL)
			if err != nil {
				log.Error("syncer.check.version.get[%s].error:%+v", peerVerStr, err)
				continue
			}

			version := &config.Version{}
			if err := json.Unmarshal([]byte(peerVerStr), version); err != nil {
				log.Error("syncer.version.unmarshal[%s].error:%+v", peerVerStr, err)
				return false, s.peer.peers
			}
			peerVer := version.Ts
			if peerVer > maxVer {
				maxVer = peerVer
			}
		}
	}

	selfVer := config.ReadVersion(s.metadir)
	if maxVer > selfVer {
		return false, s.peer.peers
	}
	return true, s.peer.peers
}

// MetaJSON used to get the meta(in json) from the metadir.
func (s *Syncer) MetaJSON() (*Meta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log := s.log
	meta := &Meta{
		Metas: make(map[string]string),
	}

	if err := filepath.Walk(s.metadir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Error("syncer.meta.json.walk.read.file[%s].error:%+v", path, err)
			return err
		}

		if !info.IsDir() {
			file := strings.TrimPrefix(strings.TrimPrefix(path, s.metadir), "/")
			data, err := readFile(log, path)
			if err != nil {
				log.Error("syncer.meta.json.walk.read.file[%s].error:%+v", path, err)
				return err
			}
			meta.Metas[file] = data
		}
		return nil
	}); err != nil {
		return nil, err
	}
	log.Warning("syncer.get.meta.json:%+v", meta.Metas)
	return meta, nil
}

// MetaRebuild use to re-build the metadir infos from the meta json.
func (s *Syncer) MetaRebuild(meta *Meta) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log := s.log
	baseDir := path.Dir(strings.TrimSuffix(s.metadir, "/"))
	backupName := fmt.Sprintf("_backup_%s_%v", path.Base(s.metadir), time.Now().UTC().Format("20060102150405.000"))
	backupMetaDir := path.Join(baseDir, backupName)
	log.Warning("syncer.meta.rebuild.mv.metadir.from[%s].to[%s]...", s.metadir, backupMetaDir)
	if err := os.Rename(s.metadir, backupMetaDir); err != nil {
		log.Panicf("syncer.rebuild.rename.metadir.from[%s].to[%s].error:%v", s.metadir, backupMetaDir, err)
	}

	log.Warning("syncer.meta.rebuild.json:%+v", meta.Metas)
	for name, data := range meta.Metas {
		file := path.Join(s.metadir, name)
		dir := filepath.Dir(file)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			log.Warning("syncer.meta.rebuild.mkdir[%s]...", dir)
			if x := os.MkdirAll(dir, 0777); x != nil {
				log.Panicf("syncer.meta.rebuild.mkdir[%v].error:%v", dir, x)
			}
		}
		if err := writeFile(log, file, data); err != nil {
			log.Panicf("syncer.meta.rebuild.mkdir[%v].error:%v", dir, err)
		}
		log.Warning("syncer.meta.rebuild.create.file[%s].done...", file)
	}
	log.Warning("syncer.meta.rebuild.all.done...")
}

// MetaReload used to reload the config from metadir.
func (s *Syncer) MetaReload() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	log := s.log
	log.Warning("syncer.meta.reload.prepare...")
	if err := s.scatter.LoadConfig(); err != nil {
		log.Panicf("syncer.meta.scatter.load.config.error:%+v", err)
	}
	if err := s.router.LoadConfig(); err != nil {
		log.Panicf("syncer.meta.router.load.config.error:%+v", err)
	}
	if err := s.peer.LoadConfig(); err != nil {
		log.Panicf("syncer.meta.peer.load.config.error:%+v", err)
	}
	log.Warning("syncer.meta.reload.done...")
	return nil
}
