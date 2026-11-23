# IRIX log-replay oracle (IRIX 6.5 under IRIS)

Checks `crates/fs-xfs/src/xfs/log_replay.rs` on logs IRIX itself wrote, with IRIX as the
reference (see `docs/xfs_log_replay.md`, "IRIX logs"). Needs a release build of the IRIS
emulator (`iris` and `iris-ci`; set `IRIS_BIN` to their folder) and an installed IRIX 6.5
Indy disk as a raw image.

- `launch.sh BOOTDISK [TESTDISK]` starts IRIS headless in CI mode with BOOTDISK as SCSI 1,
  a scratch disk as SCSI 2 and TESTDISK as SCSI 3 (IRIX: `dks0d3`). Every disk's write cache
  is off, so killing the emulator loses nothing IRIX was told was written.
- `ci ARGS` runs `iris-ci` against that machine.
- `oracle.sh BOOTDISK TESTDISK OUTPREFIX` boots IRIX from BOOTDISK with TESTDISK as unit 3, runs IRIX's
  `xfs_repair -n` on it, mounts it on `/t` (counting SYSLOG's "XFS recovery" lines before and
  after), records `ls -ldni` and `sum -r` of every entry, unmounts it, runs `xfs_repair -n`
  again and shuts IRIX down. Compare the outputs of an IRIX-recovered copy and our replayed
  copy.

To make a dirty IRIX log: `rb-cli new hd sgi-efs test.raw --size 128M --label-only`, boot
with it as the test disk, `mkfs -t xfs /dev/rdsk/dks0d3s0` in IRIX, mount it, run a workload,
and kill the `iris` process mid-flight.
