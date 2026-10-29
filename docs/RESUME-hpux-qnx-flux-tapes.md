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

## Done on 2026-10-28 (second session)

QNX 2: dates (corrected layout), owners, attributes, deleted entries, RS to
LF, fsck; **write support** (create / mkdir / rename / delete / chmod / chown)
verified with real QNX 4.10.01 `chkfsys` in icon-emu (memory
`icon-emu-qnx2-oracle.md`); LEXICON 10 MB / 64 MB hard disks (ICON header,
types 07/08/09, 1-based starts). QNX 4 read checked on real media
(`qnxfs1g.img`). Flux: SCP, HFE v1/v3, IBM FM, AmigaDOS MFM, a PLL, CRC-checked
voting, fault list in the GUI. Streaming audit: everything over ~200 MB streams
(defrag clones, .cbk, archives, sparse volume creation, NDIF, GHO MFT, file
extraction in UFS / JFS / SquashFS / ReiserFS / Minix / QNX 4/6 / ADFS / OS-9 /
Human68k / QDOS, HFS and PFS3 clones). Fixtures added: SCP / HFE / Amiga / DFS
flux, LEXICON 10 MB and 64 MB, ntfscomp.GHO, qnxfs1g.

## Queue, in the user's order

1. **Sun tapes writable (plan phase W4), then a general tape layer.** Editing
   rebuilds the whole tape like `src/fs/squashfs_edit.rs`; back up the original
   to `<name>.bak` (never overwrite: `.bak.1`, ...). Formats wanted: Sun, DEC
   (SIMH .tap / AWS / HET, ANSI labels, VMS BACKUP), HP 9000 / SGI / IBM, and a
   folder of tape files. Oracle: SunOS reading the tape back; nothing here can
   boot SunOS from QIC, so ask the user about the Sun-3 MiSTer core.
2. **QNX 2 follow-ups:** non-empty bad-block tables (warn today; need a
   sample), 640 KiB disks, `arch` archive floppies (none in the archive).
3. **Flux:** GCR (Apple / Commodore) decoding on `rbformats::flux`.
4. **HP-UX writes left:** W5 BSD disklabel slot edits + resize on restore, W6
   HP-UX LVM LV resize. VxFS gaps: indirect / typed-extent files, own fsck.
5. **Runner memory:** `rb-regress produce` keeps every pass-A artifact in
   memory (~300 MB peak); write them to disk and compare streaming.
6. **Blocked:** AIX LVM / JFS1 writes (MiSTer core).

## Archive paths

- Unisys ICON archive: NAS `ICON_ICON2/From YorkU/UNISYS_ICON`; local copies
  `/run/media/dani/BigDisk/rb-tmp/qnx/imgs` (36 `.img`) and
  `/run/media/dani/BigDisk/rb-tmp/kflux` (36 KryoFlux sets, prototypes
  `kf.py` / `kfdisk.py`).
- HP-UX 11.11 TCOE DVD fixture: `optical.iso.hpux1111-tcoe.cd` (base JFS 3.3 only).
