#!/bin/sh
# usage: launch.sh BOOTDISK [SCSI3DISK] — starts iris headless in CI mode in the background
abs() { case "$1" in /*) echo "$1" ;; *) echo "$PWD/$1" ;; esac; }
boot=$(abs "$1"); test=$(abs "${2:-/dev/null}")
cd "$(dirname "$0")"
B=${IRIS_BIN:?set IRIS_BIN to the folder holding a release build of iris and iris-ci}
ln -sf "$boot" boot.raw
ln -sf "$test" test.raw
exec "$B/iris" --config irix65.toml --ci --ci-socket "$PWD/iris.sock" --headless \
  --nvram "$PWD/nvram.bin" --serial-log "$PWD/console.log" --serial-log-overwrite
