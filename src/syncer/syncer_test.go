/*
 * Radon
 *
 * Copyright (c) 2017 QingCloud.com.
 * Code is licensed under the GPLv3.
 *
 */

package syncer

import (
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestSyncer(t *testing.T) {
	defer leaktest.Check(t)()
	defer testRemoveMetadir()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	syncers, cleanup := mockSyncer(log, 3)
	assert.NotNil(t, syncers)
	time.Sleep(time.Second * 2)
	defer cleanup()
}

func TestSyncerLock(t *testing.T) {
	defer leaktest.Check(t)()
	defer testRemoveMetadir()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	syncers, cleanup := mockSyncer(log, 1)
	assert.NotNil(t, syncers)
	defer cleanup()

	syncers[0].RLock()
	time.Sleep(10000)
	syncers[0].RUnlock()
}

func TestSyncerAddRemovePeers(t *testing.T) {
	defer leaktest.Check(t)()
	defer testRemoveMetadir()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	syncers, cleanup := mockSyncer(log, 1)
	assert.NotNil(t, syncers)
	defer cleanup()

	syncer := syncers[0]

	// Add.
	{
		syncer.AddPeer("127.0.0.1:9901")
		syncer.AddPeer("127.0.0.1:9902")

		want := []string{"127.0.0.1:8081", "127.0.0.1:9901", "127.0.0.1:9902"}
		got := syncer.peer.peers
		assert.Equal(t, want, got)
	}

	// Remove.
	{
		syncer.RemovePeer("127.0.0.1:9901")

		want := []string{"127.0.0.1:8081", "127.0.0.1:9902"}
		got := syncer.peer.peers
		assert.Equal(t, want, got)
	}

	{
		want := []string{"127.0.0.1:8081", "127.0.0.1:9902"}
		got := syncer.Peers()
		assert.Equal(t, want, got)
	}
}
