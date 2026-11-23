#!/bin/sh
# usage: launch.sh BOOTDISK [SCSI3DISK] — starts iris headless in CI mode in the background
cd "$(dirname "$0")"
B=${IRIS_BIN:?set IRIS_BIN to the folder holding a release build of iris and iris-ci}
ln -sf "$1" boot.raw
ln -sf "${2:-/dev/null}" test.raw
exec "$B/iris" --config irix65.toml --ci --ci-socket "$PWD/iris.sock" --headless \
  --nvram "$PWD/nvram.bin" --serial-log "$PWD/console.log" --serial-log-overwrite
