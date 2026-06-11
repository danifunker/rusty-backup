# SGI Bootable Install CD Imager — Implementation Plan

Status: PLAN (not started). Branch: `efs-cd-imager`.
Scope: **CLI only** for the initial release (GUI parity is explicitly deferred;
note it in OPEN-WORK when shipping).

## 1. Goal

Let a user create or alter an OS install CD image (NetBSD first; the same
machinery covers IRIX media and Linux/MIPS images) so it boots on an SGI
Indigo-class machine. The concrete operations:

1. **Inspect** an SGI CD/disk image's volume header: bootfile, volume
   directory (the PROM-visible "files in the label"), partition entries,
   checksum state.
2. **Extract / add / replace / remove volume-directory files** — `dvhtool`
   parity. This is the core "alter an install CD" use case: swap in a
   different bootloader or kernel, fix a stale entry.
3. **Make an existing ISO9660 image SGI-bootable** — stamp an SGI volume
   header + bootloader file(s) onto a plain ISO (e.g. a NetBSD sets ISO that
   lacks SGI boot blocks).
4. **Build an EFS-based CD from scratch** (IRIX-style media): blank EFS,
   populate from a host directory, wrap with volume header + boot files.

### Target platform for testing

`../iris` (`/Users/dani/repos/iris`) — the user's SGI **Indy** emulator
(IP24-class, R4400/R5000, ARCS PROM). The user refers to it as their Indigo;
note the distinction: a real Indigo is IP12 (R3000, older a.out PROM) or
IP20 (R4000). The Indy and IP20 Indigo share the ARCS boot path and the
NetBSD `IP2x` bootloader family, so everything verified on iris exercises
the same code path an R4000 Indigo uses. R3000 IP12 (a.out PROM, `aoutboot`)
is a stretch goal — produce the right voldir entries but don't block on
verifying them.

## 2. What already exists (verified in-tree)

### rusty-backup

- `src/partition/sgi.rs` — `SgiVolumeHeader` full parse **and** `to_bytes()`
  with checksum recompute (zero-sum over the 512-byte sector).
  `SgiVolumeDirEntry { name, block_num, bytes }` (15 slots, 16 bytes each at
  0x048) and `SgiPartitionEntry { blocks, first, type }` (16 slots, 12 bytes
  at 0x138) both round-trip. Partition sectors are **always 512-byte units**
  regardless of physical sector size — same on CD.
- `PartitionTable::Sgi` detection at sector 0 (`src/partition/mod.rs:387`),
  fixture test at `tests/fixtures/sgi/irix_volhdr.bin`.
- EFS: full read (`src/fs/efs.rs`), `EditableFilesystem` impl,
  `create_blank_efs(size, name)`, fsck (`efs_fsck.rs`), grow/shrink
  (`efs_resize.rs`). Building and populating an EFS volume is a solved
  problem.
- ISO9660 **read** comes from the external `opticaldiscs` crate (0.4.2,
  `optical` feature): `optical browse/extract/convert/rip` verbs. There is
  **no ISO9660 writer** in-tree, and this plan does not add one — we never
  master ISO content, we only wrap/append around an existing ISO.
- CLI: `api sgi shrink` exists but the `api` namespace is **deprecated**
  (`src/cli/api/mod.rs` header comment). New surface must be **flat verbs**
  at the crate root, following the `optical` verb's
  subcommand pattern (`src/cli/verbs/optical.rs`).
- Precedent for a disk-image builder living in the partition layer:
  `src/partition/x68k_hdd_builder.rs`.

### iris (the test bench)

- SCSI CD-ROM device in `src/scsi.rs`: image file is a flat byte array;
  `LBA -> byte offset = lba * logical_block_size`. Physical block size 2048
  (Yellow Book), logical defaults to 2048 and the guest switches 512<->2048
  via MODE SELECT block descriptor ("dksc switches CD-ROM to 512 for EFS,
  back to 2048 for ISO"). **Consequence: the flat byte layout of the image
  file is all that matters.** A volume header at byte 0 is read by the PROM
  as 512-byte LBA 0 once it MODE-SELECTs 512.
- `iris.toml` `[disk]`-style sections: `path = "cdrom.iso"`, `cdrom = true`,
  multi-disc `discs = [...]` changer with eject/`load_disc`.
- Headless mode (`--headless`), monitor console on TCP 8888, serial PROM
  console on 8881 (set PROM `console=d`). This is the automation surface
  for boot testing.
- README caveat: "NetBSD shows a white screen and probably goes into the
  weeds" — NetBSD **kernel** may not survive on iris today. Verification
  must therefore be staged (see section 7) so rusty-backup's deliverable
  (PROM finds and loads the boot file) is testable independently of iris's
  kernel-level emulation gaps.

## 3. Ground-truth research (do this FIRST, before writing code)

Do not trust memory for the on-disc layout details. Acquire and dissect:

1. **NetBSD/sgimips install ISO** (e.g. `NetBSD-10.x-sgimips.iso` from
   cdn.netbsd.org). Dissect with existing tooling + hexdump:
   - Confirm SGI volhdr magic `0x0BE5A941` at byte 0 (inside the ISO9660
     32 KiB system area — ISO9660 reserves the first 16 logical 2048-byte
     sectors, so a 512-byte volhdr + small files can coexist with a valid
     ISO).
   - Record the voldir entries (expected: `ip2xboot`, `ip3xboot`, possibly
     `aoutboot`), where their data lives (inside the 32 KiB system area, or
     appended past the ISO9660 data end?), the partition entries (which
     slot is the volhdr partition, type 0 `volhdr`; which slot spans the
     whole disc), `root_part_num`, and the `bootfile` string.
   - Save the first 64 KiB as a fixture:
     `tests/fixtures/sgi/netbsd_sgimips_cd_head.bin` (check size/licensing —
     boot code is redistributable under BSD, but keep the fixture minimal).
2. **NetBSD's own authoring tools** as the canonical reference:
   `sgivol(8)` (`src/sbin/sgivol/` — wait, confirm path; it may be
   `usr.sbin/sgivol`), and the install-CD build glue under
   `src/distrib/sgimips/` (the Makefile shows exactly how the official ISO
   gets its volhdr and where boot files are placed). Fetch from
   github.com/NetBSD/src. Mirror its allocation rules (voldir file start
   block, alignment, volhdr partition extent).
3. **An IRIX install CD image** (user has 6.5.22 media per
   `../iris/docs/irix-6.5.22-install.md`) as the EFS-style reference:
   voldir contains `sash`/`sgilabel` etc., partition 7-ish EFS spans the
   disc, partition 8 is volhdr. This anchors the `build-efs` mode.
4. **Indigo/Indy PROM expectations**: from iris's PROM behavior and SGI
   docs — how the PROM names the CD (`dksc(0,4,8)` = controller 0, SCSI ID
   4, partition 8 = volhdr) and that `boot -f dksc(0,4,8)filename` loads a
   voldir file by name. Capture the exact working invocation on iris in the
   runbook (section 7).

Write the findings into `docs/sgi_cd_format.md` (short, factual, with byte
offsets) before starting Phase 1. Future work (and the fsck-style checker)
references it.

## 4. Engine layer (Phase 1 + 2)

Per CONTRIBUTING.md: engine first, with unit tests, before any CLI wiring.
All new code ASCII-only in user-visible strings, streaming I/O, `log_cb`
callbacks at the leaf.

### Phase 1 — `src/partition/sgi_voldir.rs`: volume-directory editing

Operates on any `Read + Write + Seek` image with an SGI volhdr at offset 0
(CD or disk — this is generic `dvhtool` parity, useful beyond CDs).

```rust
pub struct VoldirFile { pub name: String, pub data_offset: u64, pub len: u32 }

pub fn list_voldir(img) -> Result<Vec<VoldirFile>>;
pub fn read_voldir_file(img, name, out: &mut dyn Write) -> Result<u64>;
pub fn add_voldir_file(img, name, data: &mut dyn Read, len: u64,
                       log_cb) -> Result<()>;   // replace-if-exists
pub fn remove_voldir_file(img, name) -> Result<()>;
pub fn set_bootfile(img, path: &str) -> Result<()>;
```

Design points:

- **Name limits**: 8 bytes, NUL-padded (existing `parse_fixed_ascii`).
  Reject longer names with a clear error.
- **Allocation**: voldir file data must land inside the volhdr partition
  (the entry with type `SgiPartitionType::VolumeHeader`). Build a free-extent
  map from existing voldir entries within that partition (header sector(s)
  themselves are reserved), first-fit allocate, 512-byte block granularity.
  Match sgivol's start-block convention found in research step 2.
- If the volhdr partition is too small to fit the new file, return a typed
  error telling the user how many blocks are missing — do NOT silently grow
  the partition (on a CD we may grow via the builder in Phase 2; on a disk
  growing would eat the data partition).
- Every mutation rewrites sector 0 via the existing `to_bytes()` (checksum
  recompute is already built in).
- **Tests**: round-trip add/extract/remove on a synthetic image; add-over-
  full error; parity check against the NetBSD fixture (list its voldir,
  re-serialize, byte-compare sector 0 modulo checksum).

### Phase 2 — `src/partition/sgi_cd_builder.rs`: CD authoring

Two builders, both pure engine functions with `progress_cb` /
`cancel_check` / `log_cb` in the standard pattern:

**(a) `make_iso_sgi_bootable(iso_in, boot_files, opts, out) -> Report`**

Input: an existing ISO9660 image + named bootloader files
(`[("ip2xboot", path), ...]`) + optional bootfile string.

Layout (informed by research step 1/2 — adjust to match NetBSD exactly):

- Bytes 0..512: SGI volume header.
- Boot file data: prefer the remainder of the ISO9660 system area
  (bytes 512..32768) when everything fits; otherwise append after the ISO
  data, padded to a 2048-byte boundary (iris and real drives address the
  image in 2048-byte physical blocks, so total size must be a 2048
  multiple).
- Partition entries: volhdr-type entry covering sector 0 through the last
  voldir block; a whole-disc entry (match what NetBSD's official ISO uses
  for slot/type) so the kernel can find the cd9660 filesystem.
- Never modify ISO content. Verify after writing that the PVD at byte
  0x8000 is intact (`CD001`), and that `optical browse` still opens the
  output.
- Refuse to clobber: output must not exist; input must parse as ISO9660
  (PVD check) unless `--force-raw`.

**(b) `build_efs_cd(source_dir, boot_files, opts, out) -> Report`**

- Size the EFS volume from the directory tree (walk + slack), call
  `create_blank_efs`, populate via the `EditableFilesystem` impl
  (create_directory / create_file / sync_metadata once at the end —
  caller-side sync per the editing rules).
- Wrap: volhdr at sector 0, voldir boot files, EFS partition entry at the
  offset/type matching the IRIX reference CD, volhdr partition covering the
  header+voldir region. `root_part_num` pointing at the EFS slice.
- Run `efs_fsck` on the result before declaring success.

Both report structs include final image size, per-file voldir placements,
and the partition map — the CLI prints them.

**Tests**: builder unit tests on small synthetic inputs (tiny valid ISO can
be generated with `hdiutil makehybrid -iso` on the mac, checked in as a
small fixture, or constructed minimally in-test: PVD + terminator is enough
for the wrap logic since we never parse deeper); EFS path verified by
re-opening with `EfsFilesystem::open` + fsck; voldir placements verified by
`list_voldir`.

## 5. CLI surface (Phase 3)

Flat verb (the `api` namespace is deprecated). Proposed grammar — check
`docs/cli-reference.md` conventions and keep nouns consistent with
`optical`/`partmap`:

```
rb-cli dvh ls <img>                          # voldir + partitions + bootfile
rb-cli dvh extract <img> <name> [-o FILE]
rb-cli dvh add <img> <name> --from FILE      # replace-if-exists
rb-cli dvh rm <img> <name>
rb-cli dvh set-bootfile <img> <path>

rb-cli sgicd make-bootable <in.iso> -o <out.iso> \
    --boot ip2xboot=FILE [--boot ip3xboot=FILE ...] [--bootfile STR]
rb-cli sgicd build-efs --from-dir DIR -o <out.img> \
    --boot sash=FILE [...] [--volname NAME]
```

`dvh` (disk volume header) deliberately mirrors IRIX's `dvhtool` so SGI
users recognize it. If reviewers prefer one verb, fold both under `sgicd`,
but `dvh` ops apply to hard disks too, so a separate verb is cleaner.

Wiring: `src/cli/verbs/dvh.rs` + `src/cli/verbs/sgicd.rs`, registered in
`src/cli/verbs/mod.rs`; thin arg-parsing only, all logic stays in the
engine modules. `api sgi shrink` stays untouched.

### Routing check (known trap)

Per the container-routing memory note: `source_reader::is_container_path`
gates decode in `inspect_tab`, `browse_session`, and `cli/resolve`. Verify
that a `.iso` carrying an SGI volhdr still reaches `PartitionTable::detect`
through `rb-cli inspect` / `dvh` (the `dvh` verb should open the file raw,
not through optical decode). Add a regression test: `rb-cli dvh ls` on the
NetBSD fixture head.

## 6. Doc sync (same commit as the feature, per CLAUDE.md checklist)

- `docs/cli-reference.md`: new `dvh` + `sgicd` sections.
- `README.md` image-formats table: SGI bootable CD row (read/write note).
- `DISK_IMAGE_EXTS` (`src/model/file_types.rs`): `.iso` is presumably
  already present via optical — verify, no change expected.
- `docs/full_MiSTer_support_status.md`: N/A (no MiSTer SGI core), skip.
- `docs/OPEN-WORK.md`: add deferred items — GUI parity, IP12/aoutboot
  verification, ISO9660 mastering (building the ISO itself), in-place
  `make-bootable` (current plan always writes a new file).

## 7. Verification on iris (Phase 4)

Staged success criteria, weakest to strongest. Stages A-C are rusty-backup
deliverables; stage D depends on iris emulation quality and is *recorded*,
not *required*.

- **A. Static parity**: `dvh ls` output on our generated image matches the
  official NetBSD ISO's voldir/partition structure (same entries, sane
  placements, checksum valid). `optical browse` still reads the ISO content.
- **B. PROM sees the disc**: iris headless with the image as
  `cdrom = true` at SCSI ID 4; PROM `hinv` lists the CD-ROM.
- **C. PROM loads the boot file**: from the PROM monitor (serial 8881,
  `console=d`, or monitor 8888): `boot -f dksc(0,4,8)ip2xboot` (exact
  syntax from research step 4). Success = NetBSD bootloader banner on the
  serial console. This proves the volume header, voldir placement, and
  512-byte-mode addressing are all correct — the entire rusty-backup
  surface.
- **D. Kernel/installer boots**: known-shaky on iris (README: white
  screen). If it fails after C passes, file it against iris, not this
  feature. Trying with serial console (`console=d`) instead of graphics is
  the first workaround to attempt.

Write the working invocation sequence into a short runbook section at the
bottom of `docs/sgi_cd_format.md` so the test is repeatable.

Also regression-check the EFS path: `sgicd build-efs` output mounts on
iris under IRIX (`mount -t efs` or PROM `ls` of the voldir) — the user has
a working IRIX 6.5.22 iris setup for this.

## 8. Execution order for Opus

1. Research + fixtures + `docs/sgi_cd_format.md` (section 3). Gate: layout
   facts confirmed against the real NetBSD ISO, fixture checked in.
2. Phase 1 `sgi_voldir.rs` + tests. Gate: parity test vs fixture green.
3. Phase 3a: `dvh` verb (it only needs Phase 1) + cli-reference docs.
   Gate: `dvh ls/extract/add/rm` round-trip on a copy of the NetBSD ISO;
   manual stage B/C smoke on iris with an *altered* official ISO
   (extract a boot file, re-add it — proves no corruption).
4. Phase 2a `make_iso_sgi_bootable` + tests, then `sgicd make-bootable`
   verb. Gate: stages A-C on a freshly stamped ISO.
5. Phase 2b `build_efs_cd` + `sgicd build-efs` verb. Gate: fsck-clean EFS,
   voldir correct, IRIX mounts it on iris.
6. Doc sync + OPEN-WORK deferrals + `cargo build --all-targets` zero
   warnings, `cargo test --lib` green, fmt, clippy.

Each step is one logical commit; messages explain the why.

## 9. Risks / open questions

- **Boot-file placement on the official NetBSD ISO** (system area vs
  appended) is the main unknown; the builder layout must copy whatever the
  official media does, since that is what the PROM provably accepts.
  Resolved by research step 1.
- **32 KiB system-area overflow**: NetBSD's bootloaders may exceed the
  ~32 KB available before the PVD; the appended-tail layout is the
  fallback and must keep total size 2048-aligned.
- **iris NetBSD kernel support** — stage D may fail through no fault of
  ours; the staged criteria isolate this.
- **IP12 (R3000 Indigo)** needs `aoutboot`-style entries and cannot be
  verified on iris (Indy ARCS PROM). Produce the entries if the official
  ISO has them; defer hardware verification.
- **Checksum staleness in the wild**: parse already tolerates bad
  checksums (`checksum_valid` flag); every write path recomputes. `dvh ls`
  should print a warning line when the source checksum is stale.
- **`.iso` permission/size**: install ISOs are hundreds of MB; all copies
  must stream in 64 KiB-1 MiB chunks (no full-image buffering) and honor
  `cancel_check`.
