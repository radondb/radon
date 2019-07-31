/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
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

type Info struct {
	Tag       string
	Time      string
	Git       string
	GoVersion string
	Platform  string
}

func GetInfo() Info {
	return Info{
		GoVersion: runtime.Version(),
		Tag:       tag,
		Time:      time,
		Git:       git,
		Platform:  platform,
	}
}
