/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package build

import (
	"fmt"
	"runtime"
)

var (
	tag      = "unknown" // tag of this build
	git      string      // git hash
	time     string      // build time
	platform = fmt.Sprintf("%s %s", runtime.GOOS, runtime.GOARCH)
)

// Info tuple.
type Info struct {
	Tag       string
	Time      string
	Git       string
	GoVersion string
	Platform  string
}

// GetInfo returns the info.
func GetInfo() Info {
	return Info{
		GoVersion: runtime.Version(),
		Tag:       "RadonDB-" + tag,
		Time:      time,
		Git:       git,
		Platform:  platform,
	}
}
