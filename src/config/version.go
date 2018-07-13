/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package config

import (
	"encoding/json"
	"io/ioutil"
	"path"
	"time"

	"xbase"

	"github.com/pkg/errors"
)

const (
	// versionJSONFile version file name.
	versionJSONFile = "version.json"
)

// Version tuple.
type Version struct {
	Ts int64 `json:"version"`
}

// UpdateVersion used to update the config version of the file.
func UpdateVersion(metadir string) error {
	name := path.Join(metadir, versionJSONFile)
	version := &Version{
		Ts: time.Now().UnixNano(),
	}
	b, err := json.Marshal(version)
	if err != nil {
		return errors.WithStack(err)
	}
	return xbase.WriteFile(name, b)
}

// ReadVersion used to read the config version from the file.
func ReadVersion(metadir string) int64 {
	name := path.Join(metadir, versionJSONFile)
	version := &Version{}
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return 0
	}
	if err := json.Unmarshal([]byte(data), version); err != nil {
		return 0
	}
	return version.Ts
}
