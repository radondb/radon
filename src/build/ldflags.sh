#!/usr/bin/env sh

set -eu

cd "$(dirname "${0}")/.."

echo '-X "build.tag='$(git describe --tags --always)'"' \
     '-X "build.time='$(date -u '+%Y/%m/%d %H:%M:%S')'"' \
     '-X "build.git='$(git rev-parse --short HEAD)'"'
