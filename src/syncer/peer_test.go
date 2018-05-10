/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package syncer

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestPeer(t *testing.T) {
	defer testRemoveMetadir()
	defer os.RemoveAll("/tmp/peers.json")
	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))
	peer := NewPeer(log, "/tmp/", "192.168.0.1")
	assert.NotNil(t, peer)
	err := peer.LoadConfig()
	assert.Nil(t, err)

	// Add peers.
	{
		peer.Add("192.168.0.2")
		peer.Add("192.168.0.3")
		peer.Add("192.168.0.4")

		want := []string{
			"192.168.0.1",
			"192.168.0.2",
			"192.168.0.3",
			"192.168.0.4",
		}
		got := peer.peers
		assert.Equal(t, want, got)
	}

	// Remove peers.
	{
		peer.Remove("192.168.0.3")
		peer.Remove("192.168.0.4")

		want := []string{
			"192.168.0.1",
			"192.168.0.2",
		}
		got := peer.peers
		assert.Equal(t, want, got)
	}

	// Load.
	{
		err := peer.LoadConfig()
		assert.Nil(t, err)
		want := []string{
			"192.168.0.1",
			"192.168.0.2",
		}
		got := peer.peers
		assert.Equal(t, want, got)
	}
}

func TestPeerError(t *testing.T) {
	defer testRemoveMetadir()
	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))
	peer := NewPeer(log, "/", "192.168.0.1")
	{
		err := peer.Add("192.168.0.2")
		assert.NotNil(t, err)
	}

	// Add empty peer.
	{
		err := peer.Add("")
		assert.NotNil(t, err)
	}

}
