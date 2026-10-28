# Resume prompt: HP 9000 / QNX / KryoFlux / tapes work (branch `deps-cd-audio-image-only`)

Written 2026-10-28 at the end of a long session on m900. Paste everything below
the line into a new Claude Code session started in `/home/dani/repos/rusty-backup`.

---

## Standing rules (from the user; they override defaults)

- **Stay on branch `deps-cd-audio-image-only`.** Never create or switch branches.
  Commit there and push to **PR #92**
  (https://github.com/danifunker/rusty-backup/pull/92); everything goes in that PR.
- Do not use a separate worktree.
- Read `CLAUDE.md` and `CONTRIBUTING.md` before coding. Engine code (`src/`) must
  compile on **Rust 1.73**: route `is_multiple_of` / `is_none_or` through
  `crate::rust173_compat` (imported under `#[cfg(feature = "rust173-polyfill")]`),
  `repeat_n` / `io::Error::other` through `crate::compat`. Comments in `.rs`
  files are one line (two at most).
- **Regression runs**: targeted cases during development; a full run (~95 min)
  only when asked — suggest it with the time estimate after major changes.
- Every write path ships only after the native OS's own checker accepts it.
- Use **BigDisk** (`/run/media/dani/BigDisk`) for builds, temp files and VMs.
- Downloads need the user's OK unless they asked for that download.

## Environment

- No GTK dev headers here: build through a wrapper that swaps `rfd` to
  `xdg-portal` and always restores the manifest. Recreate it as
  `<scratchpad>/rbcargo.sh`:

  ```bash
  #!/bin/bash
  set -u
  R=/home/dani/repos/rusty-backup; S=$(dirname "$0")
  export CARGO_TARGET_DIR=/run/media/dani/BigDisk/rb-build/target TMPDIR=/run/media/dani/BigDisk/rb-tmp
  cd "$R" || exit 1
  cp Cargo.toml "$S/rbc.toml"; cp Cargo.lock "$S/rbc.lock"
  trap 'cp "$S/rbc.toml" "$R/Cargo.toml"; cp "$S/rbc.lock" "$R/Cargo.lock"' EXIT
  sed -i 's/features = \["gtk3"\], optional = true }/features = ["xdg-portal"], optional = true }/' Cargo.toml
  cargo "$@"
  ```

  Use `bash rbcargo.sh build --bin rb-cli`, `... test --lib <filter>`,
  `... clippy --all-targets`. Never run two cargo builds at once.
- Commit with `git commit --no-verify` (the hook's clippy needs GTK) after
  running `cargo fmt` and clippy yourself.
- Vintage check (real 1.73 toolchain is installed):
  `CARGO_TARGET_DIR=/run/media/dani/BigDisk/rb-build/vintage-target TMPDIR=/run/media/dani/BigDisk/rb-tmp cargo +1.73.0 build --manifest-path rb-cli-vintage/Cargo.toml --no-default-features --features native-zstd,remote,tui,rust173-polyfill,windows-legacy,yaml --ignore-rust-version`
- Regression runner:
  `/run/media/dani/BigDisk/rb-build/runner-target/debug/rb-regress run --rb-cli /run/media/dani/BigDisk/rb-build/target/debug/rb-cli --filter <id substring> --scratch-root /run/media/dani/BigDisk/rb-tmp/regress-scratch`
  (run from `regression-tests/`). Failures land in `regression-tests/runs/<id>/failures/`.
- Fixtures: `/run/media/dani/BigDisk/rb-fixtures` (`fixture-map.tsv` has NUL
  bytes: read it with python, not grep). Large ones in `fixtures-large`, relpath
  `../fixtures-large/...`. The NAS copy is
  `/run/user/1000/gvfs/smb-share:server=daninas.local,share=software/rb-fixtures`
  (mount with `gio mount -a smb://daninas.local/software`); push new fixtures
  append-only, SHA-verified, then confirm the two `fixture-map.tsv` files are
  byte-identical. All fixtures are on the NAS as of `83b376e`.
- Oracles (see the memory files `hpux-qemu-oracle.md` and `freebsd-qemu-oracle.md`):
  - HP-UX 11.11 on `qemu-system-hppa -M B160L`, boot disk
    `/run/media/dani/BigDisk/rb-tmp/lifw/hp715.img` (root has no password; the
    firmware boots the lowest SCSI ID, so hot-plug test disks through the QEMU
    monitor at IDs 3-5). Lab volumes in `/run/media/dani/BigDisk/rb-tmp/vxlab`.
    Move files in and out through the HFS test disk with rb-cli (`run.sh`
    pattern there).
  - FreeBSD 15.1 in `/run/media/dani/BigDisk/rb-vms/freebsd15` (serial console
    already enabled; `cc` in base for `offsetof` probes).

## Done on this branch (all pushed, PR #92 green before the last pushes)

deps refresh + CD audio; `no-physical-devices` build; #89 / #90 / #91 read
support (HP LIF, BSD disklabel, HP-UX LVM, HP-HFS, VxFS 2-4, AIX LVM + JFS1,
Sun QIC tapes); LIF editing; HP-HFS editing + UFS allocator fixes (fragment
tails, alignment, realloccg, cg summaries); FreeBSD 12+ UFS check-hashes,
cluster maps, lazy inode init, `di_dirdepth`; VxFS layout-4 editing
(create / delete / rename / chmod / chown, verified with HP-UX `fsck -F vxfs`);
QNX 2 read (Unisys ICON archive), QNX 4 and QNX 6 read (synthetic tests only);
KryoFlux stream sets through `rbformats::flux` (IBM MFM). Plans and notes:
`docs/PLAN-hpux-writes.md`, `docs/vxfs-layout4-write-notes.md`,
`docs/PLAN-qnx.md`, `docs/stubs.md`.

The last **full** regression run (416 pass + 1 timeout that passes on re-run)
predates the FreeBSD check-hash, VxFS, QNX and KryoFlux commits; targeted
runs since are green (125 read cases, UFS and VxFS edit cases). Offer the user a
full run (~95 min) before the PR is merged.

## Queue, in the user's order

1. **Sun tapes writable (plan phase W4), then a general tape layer.** Editing
   rebuilds the whole tape like `src/fs/squashfs_edit.rs`: load the tape
   (`src/fs/sun2tape.rs`), edit tar sections (recompress `compress(1)` ones) and
   the UFS miniroot in memory, rebuild into a new file, and only after the
   rebuild succeeds rename the original to `<name>.bak` (never overwrite an
   existing backup: `.bak.1`, ...) and move the new tape into its name. The user
   also wants other tape formats: split container (SUN2TAPE `.qic`, SIMH `.tap`,
   AWS / HET, a folder of tape files) from contents (tar, compressed tar, cpio,
   BSD dump, UFS image, ANSI-labelled / VMS BACKUP). Ask which machines' tapes
   matter most before going wide. Oracle: SunOS 4.1.x under emulation reading
   the tape back.
2. **QNX 2 follow-ups** (the user's spec; status report given 2026-10-28):
   dates as year-1980 in 4 bits plus a PM bit (`fseconds` is 0 on every ICON
   file); owner shown as group.member; permission bits; list / recover deleted
   entries (status `0x40` clear); optional RS (0x1E) to LF on extract; bitmap
   cross-check with an orphaned-block report (a QNX 2 fsck); hand-off of QDOS
   `.pf` / `.ph` images and `arch` archives stored inside QNX; hard-disk images
   (10 MB single volume; 64 MB with a partition record, a bad-block table and
   two 32 MB partitions) once samples exist; 640K (655,360-byte) geometry and
   LEXICON disks once samples exist. Writes wait for the user's ICON emulator.
3. **QNX 4 / QNX 6 verification**: needs real media, or the user's OK to
   download the 1.7 KB `qnxfs1g.img.bz2` from qnxfs.narod.ru.
4. **KryoFlux follow-ups**: a better PLL (Watcom BASIC still has 133
   unreadable sectors, fileserver boot 86, one upgrade disk lists 12 entries
   from flux against 17 from its dump); surface the fault report in the GUI
   (today it is one stderr line); more capture formats (SCP, HFE) and encodings
   (FM, Amiga MFM, GCR) on the same `rbformats::flux` layer.
5. **HP-UX writes left**: W5 BSD disklabel slot edits + resize on restore
   (oracle NetBSD/hppa `disklabel` under QEMU from the netbsd11-hppa ISO
   fixture), W6 HP-UX LVM LV resize / resize on restore (HP-UX `vgcfgbackup`,
   `vgdisplay`, `lvdisplay`). VxFS gaps: indirect / typed-extent files, our own
   VxFS fsck; layouts 5+ are blocked on VxFS 3.5 media.
6. **Blocked**: AIX LVM / JFS1 writes (waiting on the IBMRS6K42p MiSTer core to
   boot AIX 4.3.3); QNX writes (waiting on an ICON emulator the user will build).

## Archive paths

- Unisys ICON archive: NAS `ICON_ICON2/From YorkU/UNISYS_ICON`; local copies
  `/run/media/dani/BigDisk/rb-tmp/qnx/imgs` (36 `.img`) and
  `/run/media/dani/BigDisk/rb-tmp/kflux` (36 KryoFlux sets, prototypes
  `kf.py` / `kfdisk.py`).
- HP-UX 11.11 TCOE DVD fixture: `optical.iso.hpux1111-tcoe.cd` (base JFS 3.3 only).
