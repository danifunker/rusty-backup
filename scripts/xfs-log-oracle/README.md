# XFS log-replay oracle

Makes XFS images with dirty logs and the kernel's own recovery of each, for checking
`crates/fs-xfs/src/xfs/log_replay.rs` (see `docs/xfs_log_replay.md`). Needs Docker with
privileged containers (loop mounts).

```bash
docker build -t rusty-xfs-log-oracle scripts/xfs-log-oracle
mkdir -p /tmp/xfslog && cp scripts/xfs-log-oracle/*.sh /tmp/xfslog/
docker run --rm --privileged -v /tmp/xfslog:/w rusty-xfs-log-oracle /w/make_rich.sh rich5 "-m crc=1"
```

Each `make_*.sh NAME "MKFS OPTIONS"` leaves `/w/NAME.dirty.img` (the crashed volume),
`/w/NAME.recovered.img` (after a kernel mount) and the recovered tree, MD5s, xattrs and
listing. To check our replay, copy the dirty image to `/w/NAME.ours.img`, run
`rb-cli fsck --repair` on it, then `/w/check_ours.sh NAME` in the container: it runs
`xfs_repair -n`, mounts the result and compares it with the kernel's.

- `make_dirty.sh`: small creates, a rename and deletes.
- `make_rich.sh`: mass deletes (buffer cancels), a 100-extent sparse file (btree root),
  xattrs, a device node, a hard link, a truncate and an unlink.
- `make_unlinked.sh`: a file deleted while still open at the crash (the AGI unlinked list).
- `make_quota.sh NAME "MKFS OPTIONS" "MOUNT OPTIONS"`: quota-enabled volumes (chowns,
  deletes, a limit); check with `check_quota.sh NAME "MOUNT OPTIONS"`, which also compares
  `xfs_quota report` and looks for a quotacheck.

Docker Desktop's kernel is built without XFS quota support, so the quota scripts run in a
Debian kernel under QEMU (the image is written for an arm64 Docker host, such as Apple silicon). Build `qemu/` as `rusty-xfs-qemu`; `qemu/vm.sh` boots it with the
container's root as the VM's root over 9p and runs `/w/vmjob.sh`, writing `/w/vmjob.out`:

```bash
docker build -t rusty-xfs-qemu scripts/xfs-log-oracle/qemu
cp scripts/xfs-log-oracle/qemu/vm.sh /tmp/xfslog/
echo 'bash /w/make_quota.sh q5 "-m crc=1" uquota,gquota,pquota' > /tmp/xfslog/vmjob.sh
docker run --rm --privileged -v /tmp/xfslog:/w rusty-xfs-qemu /w/vm.sh
```
