/*
 * Radon
 *
 * Copyright (c) 2017 QingCloud.com.
 * Code is licensed under the GPLv3.
 *
 */

package syncer

import (
	"encoding/json"
	"errors"
	"os"
	"path"
	"sync"

	"github.com/xelabs/go-mysqlstack/xlog"
)

const (
	// peersJSONFile file name.
	peersJSONFile = "peers.json"
)

// Peer tuple.
type Peer struct {
	log     *xlog.Log
	metadir string
	peers   []string

	// host:port
	self string
	mu   sync.Mutex
}

// NewPeer creates a new peer.
func NewPeer(log *xlog.Log, metadir string, self string) *Peer {
	return &Peer{
		log:     log,
		self:    self,
		metadir: metadir,
	}
}

// LoadConfig used to load peers info from peersJSONFile.
func (p *Peer) LoadConfig() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	log := p.log
	file := path.Join(p.metadir, peersJSONFile)
	if _, err := os.Stat(file); os.IsNotExist(err) {
		// peers.json does not exists.
		p.peers = append(p.peers, p.self)
		return nil
	}
	peers, err := p.readJSON()
	if err != nil {
		return err
	}
	p.peers = peers
	log.Warning("syncer.peer.load[%+v]", p.peers)
	return p.writeJSON(p.peers)
}

// Clone used to copy peers info.
func (p *Peer) Clone() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	var peers []string
	peers = append(peers, p.peers...)
	return peers
}

// Add used to add a new peer to the peer list.
func (p *Peer) Add(peer string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.log.Warning("peer.add[%s]", peer)
	if peer == "" {
		return errors.New("add.peer.can.not.be.empty")
	}

	for i := range p.peers {
		if p.peers[i] == peer {
			return nil
		}
	}
	p.peers = append(p.peers, peer)
	return p.writeJSON(p.peers)
}

// Remove used to remove a peer from the peer list.
func (p *Peer) Remove(peer string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.log.Warning("peer.remove[%s]", peer)
	for i := range p.peers {
		if p.peers[i] == peer {
			p.peers = append(p.peers[:i], p.peers[i+1:]...)
			return p.writeJSON(p.peers)
		}
	}
	return nil
}

func (p *Peer) readJSON() ([]string, error) {
	var peers []string
	log := p.log

	file := path.Join(p.metadir, peersJSONFile)
	buf, err := readFile(log, file)
	if err != nil {
		log.Error("syncer.peer.read.json[%s].error:%+v", file, err)
		return nil, err
	}

	err = json.Unmarshal([]byte(buf), &peers)
	if err != nil {
		log.Error("syncer.peer.unmarshal.json[%s].error:%+v", file, err)
		return nil, err
	}
	log.Warning("syncer.peer.read.json[%s].peers[%+v]", file, peers)
	return peers, nil
}

func (p *Peer) writeJSON(peers []string) error {
	log := p.log

	file := path.Join(p.metadir, peersJSONFile)
	log.Warning("syncer.peer.write.json[%s].peers[%+v]", file, peers)

	peersJSON, err := json.Marshal(peers)
	if err != nil {
		log.Error("syncer.peer.marshal.json[%s].error:%+v", file, err)
		return err
	}

	if err := writeFile(log, file, string(peersJSON)); err != nil {
		log.Error("syncer.peer.write.json[%s].error:%+v", file, err)
		return err
	}
	return nil
}
