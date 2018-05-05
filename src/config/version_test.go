/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersion(t *testing.T) {
	metadir := "/tmp/"
	defer os.RemoveAll("/tmp/version.json")

	// Update version.
	{
		err := UpdateVersion(metadir)
		assert.Nil(t, err)
	}

	// Read version.
	{
		ver := ReadVersion(metadir)
		assert.True(t, ver > 1501750907829399355)
	}
}

func TestVersionError(t *testing.T) {
	metadir := "/"

	// Update version.
	{
		err := UpdateVersion(metadir)
		assert.NotNil(t, err)
	}

	// Read version.
	{
		ver := ReadVersion(metadir)
		assert.Equal(t, int64(0), ver)
	}
}
