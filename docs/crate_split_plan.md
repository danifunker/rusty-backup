# Crate split plan

Status: **draft, revised 2026-10-30.** Branch `native-frontends`.

This work comes *before* the native front-ends ([native_frontends_plan.md](native_frontends_plan.md)).

**Goal:** extract the code that other applications and emulators would want, meaning filesystems, partition tables, disk containers and Mac archives, into about 23 MIT-licensed crates. Users pick only what they need. Everything that is specific to rusty-backup stays in the app, under AGPL-3.0.

## 1. Principles

1. **A crate exists only if someone outside rusty-backup would reuse it.** Backup, restore, the backup output formats, format dispatch, resize, the HDD builders, the model, CLI, TUI and GUI all stay in the app.
2. **Group by family, choose by feature.** One crate per platform family (for example `fs-mac` = MFS + HFS + HFS+). Each filesystem or format inside it is a Cargo feature, so an emulator that only needs HFS enables `fs-mac/hfs` and builds nothing else.
3. **Don't duplicate crates we already maintain.**
   - CHD stays in **libchdman-rs**: the generic `ChdReader` / cooked-CD / compression helpers move upstream there.
   - Optical-disc parsing goes to **opticaldiscs**.
   - QNX 2 stays in **qnx2fs**.
   - CD audio stays in **cd-da-reader**.
   - Archive formats use the existing third-party crates: `tar`, `zip`, `bzip2`, `lzma-rs`, `lzfse_rust`, `lz4_flex`, `flate2`, `zstd`.
4. **MIT by default.** A crate or feature uses another license only when it contains code translated from a source under that license (§6).
5. **Every crate builds on Rust 1.73** with `edition = "2021"`, so the vintage and mrustc builds keep working. It also lets CI run each crate's tests on 1.73.

## 2. The crates

All names were checked against crates.io on 2026-10-30 and are free. `fs-core` was taken (an in-memory WebAssembly filesystem). The shared interface and the 1.73 shims were first drafted as two crates, `fs-api` and `rb-compat`, then merged into one base crate, **`disk-core`** (§3).

### Filesystems (13)

| Crate | Features (one per filesystem) | Approx. source |
|---|---|---|
| `disk-core` | the base crate (§3): 1.73 shims + neutral formatters always; the filesystem interface behind `fs` (default); `testing` (conformance suite, fixture helpers) to come | ~4.5k today |
| `fs-mac` | `mfs`, `hfs` (+ fsck, clone, boot blocks, HFV), `hfsplus` (+ journal, fsck, clone, defrag), `lisa` | large |
| `fs-apfs` | APFS; `crypto` for FileVault | separate because it's modern, large and has different users |
| `fs-apple2` | `apple-dos`, `prodos`, `ucsd` | |
| `fs-pc` | `fat`, `exfat`, `ntfs`, `hpfs`, `human68k` | |
| `fs-unix` | `ext`, `ufs`, `efs`, `jfs`, `btrfs`, `reiserfs`, `vxfs`, `minix`, `bfs` (BeOS), `sun2tape` (+ `tar_section`) | |
| `fs-xfs` | XFS read, fsck, repair (**GPL-2.0**, §6) | split out so `fs-unix` stays MIT |
| `fs-amiga` | `affs` (AFFS/OFS), `pfs3`, `sfs` | |
| `fs-squashfs` | read / write / edit / verify; `appimage` (find the SquashFS in an AppImage), `iso` (find it inside a live-CD ISO) | |
| `fs-cpm` | CP/M + disk-definition catalog | |
| `fs-8bit` | `cbm`, `atari-dos`, `acorn` (ADFS/DFS), `trdos`, `qdos` (+ microdrive), `coco` (RS-DOS/DragonDOS), `os9`, `ti99`, `oric`, `lif`, `andos` | |
| `fs-qnx` | QNX 2 (wraps `qnx2fs`), QNX 4, QNX 6 | |
| `fs-xerox` | Alto (Pilot/Cedar later). **Co-copyright: Alan Steremberg** | |

### Filesystem tools (1)

| Crate | Contents |
|---|---|
| `fs-tar` | export any `disk-core` filesystem (or a folder in it) to `.tar` / `.tar.gz` / `.tar.zst`, and import a tar into any editable filesystem. Handles permissions, symlinks, times and case-insensitive filesystems. Builds on the `tar` crate. |

### Partition tables (1)

| Crate | Features |
|---|---|
| `part-tables` | `mbr`, `gpt`, `apm`, `bsd`, `sun`, `sgi` (+ dklabel), `next`, `rdb` (Amiga), `atari` (AHDI), `x68k`, `solaris-x86`, `lvm`: parse and write only. Resize, restore-patching, provisioning and HDD builders stay in the app. |

### Disk containers (7)

| Crate | Features / contents |
|---|---|
| `dsk-apple` | `dc42` (incl. Twiggy), `2mg`, `woz` (read/write), `moof`, `dart` |
| `dsk-dmg` | DMG, NDIF, sparseimage; `crypto` for encrypted DMG |
| `dsk-floppy` | D88, MSA, XDF, HDM, DIM, ATR, G64/G71, WinImage IMZ (+ passport decryption), and the other floppy wrappers in `rbformats/containers/` |
| `dsk-flux` | flux captures (SCP, Gesswein `.emu`, …) |
| `dsk-vm` | VHD, VMDK (flat + sparse), QCOW2; home of the virtualization roadmap |
| `dsk-ghost` | Norton Ghost GHO (+ password); `file-aware` feature pulls `fs-pc` |
| `dsk-clonezilla` | partclone image reader, Clonezilla folder metadata, block cache. **Co-copyright: Alan Steremberg** |

### Other (2)

| Crate | Contents |
|---|---|
| `arc-mac` (MIT) | StuffIt container parser and writers, MAR, BinHex, MacBinary, archive detection and extraction. Depends on `fs-mac` for HFS metadata types, and on `arc-mac-codecs`. |
| `arc-mac-codecs` (**LGPL-2.1-or-later**) | The StuffIt / Compact Pro decompressors ported from XADMaster: Arsenic, Compact Pro, StuffIt 5, LZW decode, method 13, LZAH, Huffman. Licence decision D1 in [license_audit.md](license_audit.md). |

**Rule for moves (2026-10-30): minimal changes only.** Crates receive code as-is: `git mv`, path fixes to compile, notices, and comment corrections. Rewrites, including licence-driven ones (D2: `hfs_unicode` tables, `HFS_CHARORDER`, the `adfs` map walk, `hpfs` dnode editing, XFS `dir_hashname`), are separate follow-ups. **The licence follows the code:** a derived module still moves, into a crate or sub-crate under the upstream licence (`adfs`, `hpfs`, `hfs_unicode` → GPL-2.0-only; `fs-xfs` GPL-2.0; `fs-amiga` `MIT AND BSD-4-Clause` for pfs3aio). Clean code stays MIT. Details in [license_audit.md](license_audit.md) D2–D4.

### What stays in the app (AGPL-3.0)

- backup, restore, the backup output formats and reconstruction (`rbformats` export/compress/gzip/zstd/lz4 writers)
- CHD backup layout and `chd_edit`, over libchdman-rs
- format detection and dispatch (`fs/mod.rs` routing, `PartitionTable` detection, `model::source_reader`)
- partition resize, restore-patching, provisioning, the HDD and CD builders, make-bootable
- copy/replace/patch, export selection, fork export, Mac-archive import into images, carving, tree text
- `archive_fs`, `optical_fs` and the optical device, remote and GUI side
- zipped raw disks, NeXT MO, ICON HD, `cbk`
- `os`, `privileged`, `remote`, `model`, CLI, TUI, GUI

Detection and dispatch become a simple list: each crate exposes a `probe` function, and the app's registry calls them in order.

## 3. What `disk-core` is

`disk-core` is the one base crate under everything else. It was first drafted as two crates, `rb-compat` (1.73 shims) and `fs-api` (the filesystem interface). They were merged on 2026-10-30, because no realistic user wants the interface without the shims. Anyone who uses the interface already needs the shims, since the filesystem code calls the polyfills.

**Always available** (what `part-tables` and the `dsk-*` crates use, with `default-features = false`):
- `io_other`, `repeat_n`: std shims that build on current Rust and on 1.73.
- `rust173` (feature `rust173-polyfill` only): `is_multiple_of` / `Option::is_none_or` stand-ins.
- `display`: `format_size`, `decode_ostype` (Mac four-character codes), `unix_mode_string`.

**Feature `fs` (default):** the common shape every filesystem crate implements, and every user of a filesystem programs against. It moved from `src/fs` as `filesystem.rs`, `entry.rs`, `fsck.rs`, `times.rs` and `xattr.rs`. These still belong there and move later as their dependencies allow: `attrs`, `name_legalize`, `raw_name`, `id_names`, `unix_common`, `import_sink`.
- `Filesystem`: `root`, `list_directory`, `read_file`, `volume_label`, `total_size` / `used_size`, `fsck`, `validate_name`, `list_xattrs`, resource forks and journals (each with a "not supported" default).
- `EditableFilesystem`: `create_file`, `create_directory`, `delete_entry`, `rename`, `set_permissions`, `set_owner`, `set_xattr`.

With it, an emulator can open any supported filesystem and call `list_directory()` without knowing which crate is behind it. `fs-tar` and the conformance tests work with every filesystem automatically. Each filesystem crate depends only on `disk-core`, never on another family crate. The exceptions are deliberate: `dsk-ghost` → `fs-pc` and `arc-mac` → `fs-mac`. `resource_fork` goes to `fs-mac`, not here, because it needs HFS and MacBinary code.

**Trade-off accepted:** a breaking change to the `Filesystem` trait is a breaking `disk-core` release, which also touches the `dsk-*` / `part-*` crates that don't use it. Since every crate releases together from this repo, that's cheap. If outside users ever hit pointless bumps, `fs` can be split back into its own crate.

## 4. Dependency rules and cycles to break

```
disk-core  (feature `fs` on for filesystem crates, off for part/dsk)
   ^                     ^                ^
fs-* families        part-tables      dsk-*  (apple, dmg, floppy, flux, vm, clonezilla)
   ^                (dsk-ghost -> fs-pc)     (arc-mac -> fs-mac)
   ^
fs-tar
   ^
rusty-backup app  (depends on all of the above + libchdman-rs, opticaldiscs, qnx2fs, cd-da-reader)
```

| # | Today | Fix |
|---|---|---|
| C1 | fs modules -> `rbformats::zstd_compat` | Test-only, except SquashFS (checked). Becomes a dev-dependency; `fs-squashfs` takes `zstd` directly, behind `native-zstd`/`pure-zstd` features. |
| C2 | `fs` and `partition` -> `partition::format_size` | **Done (Step 0):** moved to `disk_core::display`; `partition::format_size` re-exports it |
| C3 | `fs/hfs_clone` and `fs/mac_scsi_bless` -> `partition::apm::build_minimal_apm` | The HFS clone's APM wrapping and `mac_scsi_bless` stay in the app; `fs-mac` only produces the bare volume |
| C4 | `fs/fat` -> the x68k HDD builder; `fs/dfs` -> `partition` | The builder call moves to the app; pass boot-sector bytes in as data |
| C5 | `partition/{resize,restore_patch,provision,editor,*_builder}` -> `fs` | Those files stay in the app; `part-tables` keeps parse and write only |
| C6 | `rbformats/*` -> `model::source_reader` | The containers expose readers; opening "any container" stays in the app's `source_reader` |
| C7 | `rbformats/{export,compress,gzip,zstd,lz4,chd,vhd,zip_disk}` -> backup/restore | Split each file: the format reader/writer goes to `dsk-*`, the backup glue stays in the app |
| C8 | `fs/mac_archive_import` -> `model::file_types` | `mac_archive_import` stays in the app; the extension list moves to `arc-mac` |
| C9 | `fs/prodos_types` -> `os::linux::real_user_home` | Pass the config directory in as a parameter |
| C10 | `macarchive` <-> `fs` (archive_fs, export_selection) | `arc-mac` sits above `fs-mac`; `archive_fs` and `export_selection` stay in the app |
| C11 | `clonezilla` -> `rbformats::zstd_compat` | `dsk-clonezilla` takes `zstd`/`xz` directly, with the same features as C1 |

## 5. Builds, features and the vintage targets

- **The app's `Cargo.toml`** lists the crates as path dependencies with the features it needs. The existing app features (`chd`, `crypto`, `native-zstd` / `pure-zstd`, `rust173-polyfill`, …) forward to the crate features, so current build commands keep working.
- **The vintage manifests (`rb-cli-vintage`, `rb-cli-ppc`, `rb-cli-sol9`)** keep their own lockfiles and vendor dirs and add the same path dependencies, forwarding `rust173-polyfill` to each crate. Adding a crate means one dependency line plus one feature-forward entry in each of the four manifests.
- **mrustc (PPC, Sol9) must build every crate in order.** See §9a for what Step 0 could and could not prove.
- **Testing the crates on 1.73 needs its own small manifest.** Cargo 1.73 cannot read the root workspace's lockfile (newer format), and cargo won't pass features to path crates from outside their workspace. Step 0 used a throwaway harness in `target/`: a package depending on each crate with `rust173-polyfill`, seeded with `rb-cli-vintage/Cargo.lock`. CI should get a checked-in equivalent (e.g. `crates-msrv/`, kept outside `crates/*`).
  - **Limit found in slice 2a:** cargo refuses to test a non-member path crate that has dev-dependencies ("cannot be tested because it requires dev-dependencies and is not a member of the workspace"). Once `fs-8bit` gained `zstd` as a dev-dependency (for the OS-9 fixture), the harness could only run `disk-core`'s tests on 1.73. The vintage `rb-cli` check still compiles `fs-8bit`'s library on 1.73.
  - Options for the CI job:
    - keep the crates' tests free of dev-dependencies, e.g. a `disk-core` `testing` feature that unpacks fixtures and that the harness can enable
    - or give the 1.73 job a workspace of its own that copies `crates/` in
- The `.cargo/config.toml` 4-job cap stays. Smaller crates lower the peak memory of each `rustc` process.

## 6. Licensing

- **Default: MIT** for every crate, with a `LICENSE` file and `license = "MIT"` in its `Cargo.toml`. The app stays **AGPL-3.0**, and an AGPL app can use MIT crates freely.
- **Contributors:** Dani Sarfati wrote 2,137 of 2,151 commits.
  - **Alan Steremberg** contributed to `clonezilla/partclone.rs`, `clonezilla/metadata.rs` and `fs/alto/pilot.rs`. **`dsk-clonezilla` and `fs-xerox` carry `Copyright (c) 2026 Dani Sarfati, Alan Steremberg`**, with Alan in `authors`.
    - **Consent confirmed 2026-10-30.** Dani asked on Discord whether he was OK converting that AGPL-3.0 code into MIT crates, and Alan (`alanswx`) replied "Any license is fine."
    - **No publishing hold (2026-10-30).** Alan's consent covers releasing them, so both publish like any other crate.
  - The other two contributors' commits don't touch crate code: Scott's is the `ppc-tiger` port, and the Jules bot's is a Windows build fix.
- **Audit done 2026-10-30: see [license_audit.md](license_audit.md)**, which supersedes the table below. It found more than this first list:
  - XADMaster-derived Mac archive codecs (LGPL-2.1+)
  - Linux-derived tables and code in `hfs_unicode`, `adfs` and `hpfs` editing, plus the XFS `dir_hashname`
  - an hfsutils table in `hfs_common`
  - pfs3aio's 4-clause BSD licence
  - unlicensed SFS sources
  - NTFS blobs reproduced from mkntfs output
  - a missing MIT notice for nyarime/gho

  It lists decisions D1–D4.
- **Original audit list** (kept for history). These files say they were ported or copied from other sources. This is not legal advice; each needs a proper look:

| Crate | File(s) | What the comments say | Proposed handling |
|---|---|---|---|
| `fs-xfs` | `xfs/*` repair, rebuild, edit | "Porting reference: `xfs_repair` @ v3.1.11" (xfsprogs, GPL-2.0) | Its own crate under **GPL-2.0**. Also check compatibility with the app's AGPL-3.0 if xfsprogs is GPL-2.0-*only*. |
| `fs-amiga` | `sfs_fsck.rs` | "A port of the `SFScheck` reference validator" | Check the SmartFilesystem license. Either an `sfs-fsck` feature under that license, or rewrite. |
| `fs-mac` | `hfsplus_journal.rs` `calc_checksum` | "Lifted verbatim from `core/hfs_journal.c`" (Apple hfs, APSL-2.0) | Rewrite the ~10-line checksum from TN1150's description, or keep an APSL notice. The structure layouts are documented formats. |
| `fs-8bit` | `adfs.rs` | "mirrors the Linux kernel's `fs/adfs/map.c` walker" (GPL-2.0) | Review: independent implementation, or translation? |
| `fs-8bit` | `qdos.rs` | "matches sQLux `QDisk.c`" | Check the sQLux license; most likely only layout facts are used |
| `fs-unix` | `ufs.rs` | NetBSD `makefs` for the writer (BSD) | Compatible: keep the BSD notice |
| `fs-pc` | `ntfs_format.rs` | "No `mkntfs.c` / `ntfs-3g` source … was read or translated" | Clean |

## 7. Tests

| Kind | Where | Notes |
|---|---|---|
| Unit tests (~2,680 today in fs/partition/rbformats) | inside each crate | They move with their files. `cargo test -p fs-mac` runs in seconds. |
| Single-format integration suites | `crates/<name>/tests/main.rs`: one binary per crate | `fs_e2e_suite/*` (apple_dos, cbm, cpc/cpm/pcw, os9, macplus_mfs, hfsplus_journal, atarist, d88), `sgi_efs_v1_e2e`, `floppy_container_roundtrip` |
| Conformance suite | `disk-core` `testing` feature; each `fs-*` opts in with a few lines | open, list, read; name and time round-trip; edit + reopen + clean fsck; free-space accounting; name-rule rejection, all against the crate's existing blank-volume builder |
| Shared fixtures | `tests/fixtures/` stays at the repo root; `disk-core/testing` resolves it and unpacks the zstd anchor images | Uses `tempfile` + `set_len` (32-bit CI heap rule) |
| Cross-format, dispatch, CLI, daemon, backup round-trips | the app's `tests/` | `filesystem_e2e`, `wave2_dispatch_e2e`, resize suites, `cli_*`, `remote_*`, `split_backup_roundtrip`, and others |
| Cross-OS regression harness | `regression-tests/` | Unchanged; CLI-level |

That's about 2 test binaries per crate (~48), plus the app's. The vintage CI jobs gain `cargo +1.73 test -p <crate>` for every crate, so vintage gets real tests for the first time.

## 8. Repository layout

**Recommendation: keep the crates in this repo**, as a Cargo workspace under `crates/`:
- **One pipeline.** A single CI definition runs every crate's tests (stable + 1.73), and per-crate publishing is driven by tags (e.g. `fs-mac-v0.1.0`) or `release-plz`.
- **Atomic changes.** A fix that touches `fs-mac` and the app lands in one PR, with no version bump-and-wait.
- **Simple vintage builds.** Path dependencies work for the vintage and mrustc builds; git dependencies would need vendoring.

The crates already kept separately stay separate, because other projects share them: `qnx2fs` with icon-emu, and `libchdman-rs` / `opticaldiscs` / `cd-da-reader` have their own C/C++ build concerns. If outside contributors later want a smaller repo, `git subtree split` can move `crates/` into a crates-only repo with history intact.

## 9. Migration order

0. **Skeleton + gate. Mostly done, see §9a.**
   - Turn the root into a workspace and add `disk-core`.
   - Move one small, clean filesystem end to end: `fs-8bit` with just `oric`.
   - **Gate:** desktop, MiSTer, musl, vintage 1.73 (macOS / Win7), and mrustc PPC + Sol9 all build.
1. **The interface part of `disk-core`.** Pulled into Step 0 because a filesystem crate can't exist without it. Old `crate::fs::…` paths are `pub use` shims, so the app doesn't change yet. The remaining interface-adjacent files (`attrs`, `name_legalize`, `raw_name`, `id_names`, `unix_common`, `import_sink`) follow as their dependencies allow.
2. **The rest of `fs-8bit`, `fs-cpm`, `fs-apple2`, `fs-qnx`, `fs-xerox`.** Small and mechanical.
   - **Slice 2a done (2026-10-30):** `fs-8bit` gained `cbm`, `atari_dos`, `dfs` (feature `acorn`), `trdos`, `rsdos` + `dragondos` (feature `coco`), `os9` (feature `os9`, implies `coco`), `ti99` and `andos`. That's about 12k lines with all 110 crate tests passing; `os9`'s fixture test now reads `tests/fixtures` via `CARGO_MANIFEST_DIR` through a `zstd` dev-dependency.
   - **Slice 2b done (2026-10-30):** new crates `fs-cpm` (`cpm`, `cpm_diskdefs`), `fs-apple2` (`ucsd`), `fs-qnx` (`qnx4`, `qnx6`) and `fs-xerox` (the whole `alto/` tree, MIT, co-copyright Alan Steremberg; the early `publish = false` hold was lifted the same day). `disk-core` gained `bits` (big-endian bitmap helpers, moved out of `hfs_common`, which now re-exports them) and `unix_common` (moved as-is; needs `anyhow` under `fs`).
   - **Held back from 2b; resolved in slice 2c (2026-10-30):**
     - `apple_dos` and `sector_order` (dependency-free; the app's `rbformats::containers` re-exports it) moved to `fs-apple2`.
     - `prodos` and `prodos_types` moved to `fs-apple2`, with `assets/prodos_file_types.json` moved into the crate.
       - The one real change: `prodos_types::set_user_config_dir()`, with a fallback to the platform config dir plus `rusty-backup`.
       - The app keeps its elevation-aware lookup in `fs::prodos_types::init_user_config_dir()`, called first in both `main` functions.
     - `CompactResult` (plain data, returned by every compactor) moved to `disk_core::filesystem`, re-exported from `fs`.
     - `qnx2` **stays in the app** (reverted after slice 2c, restored unchanged with its end-to-end `fs::copy` test).
       - It is an adapter from the external `qnx2fs` engine to the `disk-core` traits, not reusable format code, so it fails the rule below.
       - It can't fold into `qnx2fs` either while `disk-core` itself depends on `qnx2fs` (`FileEntry::qnx2_attrs`): that would be a dependency cycle.
       - **Decoupled (2026-10-30):** `disk-core` no longer depends on `qnx2fs`. `FileEntry::qnx2_attrs` and the create options carry `disk_core::entry::Qnx2Attrs` (the same six fields). The app's `fs/qnx2.rs` converts with `to_core` / `to_qnx2` and maps errors with `qnx_err`, since the orphan rule forbids `From` there.
       - **Folded into `qnx2fs` 0.3.1 (2026-10-30):**
         1. `disk-core` 0.1.0 is on crates.io, published by `.github/workflows/publish-crates.yml` from the tag `disk-core-v0.1.0`.
         2. The adapter is `qnx2fs::adapter`, behind the optional `disk-core` feature; the default `qnx2fs` build stays dependency-free. `From<qnx2fs::Error> for FilesystemError` and the `Attributes` <-> `Qnx2Attrs` conversions live there.
         3. The app takes `qnx2fs` with that feature, plus `[patch.crates-io] disk-core = { path = … }` in all four manifests so there's one `disk-core`. `src/fs/qnx2.rs` is now a re-export, keeping only the end-to-end `fs::copy` test, which needs the app.

   **Rule (2026-10-30): a crate must contain reusable format code.** A module that only adapts an external engine crate to the `disk-core` traits stays in the app, as `qnx2` over `qnx2fs` and `optical_fs` over `opticaldiscs` do. Crates are for code that implements a format.
   - **Held back:** `adfs` and `qdos` / `qdos_mdv` (license audit, §6) and `lif`. `lif`'s replace test needs both its private fields and the app's `fs::replace`; split the test, or give `lif` a small test-support accessor, before moving it.
3. **`fs-amiga`, `fs-unix`, `fs-xfs`, `fs-squashfs`, `fs-pc`, `fs-mac`, `fs-apfs`.** The big ones. The license-audit items get their handling here.
   - **`fs-amiga` done (2026-10-30).** `affs` / `affs_fsck`, `pfs3` / `pfs3_fsck` / `pfs3_clone` and `sfs` / `sfs_fsck` moved as-is, one feature each (`affs_common` always builds). Licence `MIT AND BSD-4-Clause` (D3), with pfs3aio's licence and the SFS / ADFlib provenance in `THIRD_PARTY_NOTICES.md`. The audit's comment fixes for `affs_common`, `sfs.rs` (the misattribution) and `sfs_fsck`'s header went in with the move.
     - `disk-core` gained `name_legalize` and `stream_pipe` (both moved as-is). The one `name_legalize` test that runs real FAT / HFS / ProDOS / MFS validators stays in the app's shim file.
     - These additions are backwards compatible, so `disk-core` can go out as 0.1.1 if `fs-amiga` publishes before 0.2.
   - **`fs-unix` done (2026-10-30)**, MIT: `ext`, `ufs`, `efs` (incl. EFS v1 and both resizers), `jfs` (incl. AIX JFS1), `btrfs`, `reiserfs`, `vxfs`, `minix`, `bfs`, one feature each. The partimage comparison came back FACTS for `jfs` / `reiserfs` / `ufs` (license_audit.md), and their comments were corrected.
     - `disk-core` gained `raw_name` (moved as-is; its UFS round-trip test stays in the app's shim) and `sgi` (`SgiLabelByteOrder`, `apply_byte_order`, `swab16_in_place`, moved out of `partition::sgi_dklabel`, which re-exports them). `sgi` is always built; its serde derive is behind a new `serde` feature that `fs` turns on.
     - Two tests that go through the app's dispatcher (`open_editable_filesystem` for UFS, `probe_0x83_fs_type` for ReiserFS) moved to the shim files and now run on the fixture images. The crate's fixture tests read `tests/fixtures` through `CARGO_MANIFEST_DIR` and a `zstd` dev-dependency.
     - `UfsFilesystem::dirblksiz` gained a `pub` accessor for the NeXT CD builder's test.
     - **Held back:** `ofs` / `ofs_fsck` / `ofs_write` (old BeOS fs) need `hfs_common::resolve_create_type_creator` and its type/creator table, which belong to `fs-mac` (or `disk-core` if both families should share it); move after that's settled. `sun2tape` / `sun2tape_edit` / `tar_section` need the Unix-`compress` codec from `macarchive::stuffit_lzw` (planned for `arc-mac`) and `mem_archive::open_tar`; move with Step 6 or give `fs-unix` its own small LZW.
   - **`fs-xfs` done (2026-10-30)**, GPL-2.0-only: the whole `xfs/` tree moved as `fs_xfs::xfs`. The "Porting reference" headers now read "Behavioural reference ... validated against `xfs_repair -n`" (audit follow-up). `dir_hashname` stays as-is, so the licence follows it (D2).
   - **`fs-squashfs` done (2026-10-30)**, MIT: `squashfs`, `squashfs_write`, `squashfs_edit` and `squashfs_verify`. C1: the crate has its own `native-zstd` / `pure-zstd` features (forwarded like `dsk-clonezilla`'s) behind a two-function `zstd_compat`. `disk-core` gained `attrs` (moved as-is). The dispatcher test (`open_filesystem` / `probe_0x83_fs_type` over a live-CD ISO) moved to the app's shim. The planned `appimage` / `iso` features don't exist as code yet.
   - **`fs-pc` done (2026-10-31)**, MIT: `fat`, `exfat`, `ntfs` and `human68k` (each with its fsck / format / clone modules), one feature each, plus the always-built `patch` and `layout_preserving` helpers they share. `ntfs_tables` and its `ntfs_template/` blobs moved too; D4's origin note is in `THIRD_PARTY_NOTICES.md`.
     - C4 was doc-only: `fat` never called the x68k builder; the builder calls `fat` and `human68k`, and stays in the app.
     - Made `pub` for app users: `fat::{cp437_to_char, char_to_cp437, has_fat_boot_start}` and the NTFS MFT parsing that `rbformats::gho` reuses (`parse_vbr`, `NtfsVbr`, `apply_fixup`, `parse_mft_attributes`, `MftAttribute`, `DataRun`).
     - The `exfat_clone` test's dispatcher assertion (`detect_defrag_clone_shape`) became its own app-side test on the exFAT fixture.
   - **`fs-hpfs` done (2026-10-31)**, GPL-2.0-only (D2): `hpfs` moved whole, since the derived dnode editing and the read side share one file.
   - **`fs-apfs` done (2026-10-31)**, MIT: `apfs` and `apfs_crypto`. FileVault sits behind the crate's `crypto` feature, which the app's `crypto` forwards; the 1.73 manifests take the crate without it, as before. Two `VolumeInfo` / `NxSuperblock` fields that only the unlock path reads got a `cfg_attr(not(feature = "crypto"), allow(dead_code))`.
   - **`fs-mac-unicode` done (2026-10-31)**, GPL-2.0-only (D2): `hfs_unicode` whole, plus `HFS_CHARORDER`, `compare_hfs_keys`, `fold_hfsplus_name_for_compare` and `compare_hfsplus_keys` (with their tests) carved out of `hfs_common` into `collate`; `hfs_common` re-exports the two comparisons.
   - **`fs-mac` done (2026-10-31)**, MIT, with features `hfs`, `mfs`, `hfsplus` and `forks`. It covers `mfs`, `hfs`, `hfs_fsck/`, `hfs_common`, `hfs_boot`, `hfs_clone`, `hfv`, `mac_alias`, all six `hfsplus*` modules, `resource_fork`, `binhex` and `fork_export`; `assets/hfs_file_types.json` moved into the crate (the GUI includes it from there).
     - C3: `hfs_clone::emit_apm_disk_with_hfs` (and its APM tests) and `hfs_boot::read_donor_boot_blocks_from_image` / `is_classic_hfs_partition` stay in the app's shim files.
     - `resolve_apple_hfs` moved from `fs/mod.rs` into `hfsplus` (re-exported). The four `can_defrag_clone_hfsplus` tests moved to the app shim, on `create_blank_hfsplus`, and `hfv`'s `PartitionTable` check became its own app test.
     - `binhex::rle90_{encode,decode}` became `pub` for `macarchive::stuffit`. `hfsplus_journal` now cites TN1150 instead of "verbatim" xnu (audit follow-up).
     - **Held back:** `lisa` (needs `rbformats::dc42`, which belongs to `dsk-apple`; move with Step 5). `mac_scsi_bless` stays in the app (C3).
   - **Step 3 is complete** except the held-back modules listed in the bullets above.
4. **`part-tables`** (C5).
5. **`dsk-*` and `dsk-clonezilla`** (C6, C7, C11). The upstream moves to libchdman-rs and opticaldiscs happen here.
   - **`dsk-clonezilla` done early (2026-10-30)**, ahead of Step 3, because its co-author had agreed to MIT.
     - `partclone`, `metadata`, `metadata_scan` and `block_cache` moved as-is. `metadata_scan` only depends on the other three, so it counts as format code: it finds FAT / NTFS / exFAT / ext / btrfs metadata blocks inside a partclone stream.
     - C11: the crate has its own `native-zstd` (default) / `pure-zstd` features, which the app's features of the same names forward. It takes `bzip2`, `lz4_flex`, `lzma-rs` and `flate2` directly, with version ranges that cover both the modern and the 1.73 lockfiles.
     - `src/clonezilla/mod.rs` is a `pub use dsk_clonezilla::*` shim.
6. **`arc-mac`, `fs-tar`.**
7. **Cleanup and docs.**
   - Remove the shims.
   - Add per-crate READMEs (from `src/fs/README.md` and `src/rbformats/README.md`).
   - Update CLAUDE.md § Architecture, CONTRIBUTING.md and `docs/export_pipelines.md`, and adapt the "add a filesystem" recipe to "add a feature to a family crate".

Files move with `git mv` so history follows them. Steps 2–6 parallelize across sessions after step 1, with one owner at a time for the shared files (`fs/mod.rs`, `partition/mod.rs`, `rbformats/mod.rs`, the root `Cargo.toml`).

### 9a. Step 0 results (2026-10-30)

**Built:** `crates/disk-core` (MIT) and `crates/fs-8bit` (MIT, `oric` feature). The root `Cargo.toml` is a workspace (`members = ["crates/*"]`), and the four manifests (root, `rb-cli-vintage`, `rb-cli-ppc`, `rb-cli-sol9`) depend on both crates and forward `rust173-polyfill`.

| Gate | Result |
|---|---|
| Modern `cargo check --all-targets` | Pass, no warnings |
| Modern full test suite (`cargo test --workspace`) | Pass: 3,755 passed, 0 failed |
| `cargo clippy -D warnings` on the new crates (and on `disk-core` with `fs` off) | Pass |
| Rust 1.73 `cargo check` of `rb-cli-vintage` (rb-cli, vintage features) | Pass; no warnings from the new crates |
| Rust 1.73 tests of `disk-core` + `fs-8bit` (polyfill on) | Pass: 19 + 7 |
| mrustc `scripts/build-ppc.sh hostc` on an Apple-silicon host | **Blocked, not by the split.** First the stale PPC vendor dir lacked `qnx2fs` (fixed by re-vendoring). Then the vendored `constant_time_eq` (via `zip`) fails: "Unknown architecture for asm!", because mrustc has no arm64 inline-asm support. No full-engine transpile exists in this machine's mrustc output dirs, so this predates the split. Transpile on x86_64 instead (next row). |
| **mrustc PowerPC build** (`scripts/build-ppc.sh ppc`, m900 x86_64 transpile with `~/repos/mrustc` branch `ppc-build-2026-09`, C compiled on the real dual-G5, Leopard 10.5.8, gcc 10.5) | **Pass (2026-10-30).** Linked `rb-cli` (Mach-O `ppc_750`) with `disk_core` + `fs_8bit` as path crates. On the G5 it ran `new floppy oric` → `put` → `ls` → `fsck` (clean). Needs `RB_DIR` pointed at the checkout being tested (the script defaults to `~/repos/rusty-backup`). |
| MiSTer / musl / Win7 builds | Not run locally; covered by CI |

**Design changes the move forced:**
- **Journal hook:** `Filesystem::read_journal` was removed from the trait; nothing called it through `dyn`. `journal_detail` now returns `Box<dyn Any + Send>`, which the GUI journal viewer downcasts to HFS+'s `JournalDetail`. This keeps HFS+ journal types and the APSL-flagged checksum out of the MIT base crate.
- **ProDOS type names:** `FileEntry::type_code_display()` became the app-side `FileEntryTypeDisplay` trait (`src/fs/entry.rs`), because ProDOS type names come from a user-overridable config file. `display_file_type` stays in the app too. Nine call sites import the trait.
- **QNX 2:** superseded. `disk-core` carries its own `Qnx2Attrs`, and `qnx2fs` (feature `disk-core`) depends on `disk-core`, not the other way round. Generalizing per-filesystem attribute fields is future work.
- **Display helpers:** `format_size`, `decode_ostype` and `unix_mode_string` moved to `disk_core::display`; the old paths re-export them.
- **Clippy:** declaring `rust-version = "1.73"` makes `incompatible_msrv` fire on the polyfilled `is_multiple_of` calls (false positives), so `disk-core` allows that lint at crate level. The real floor check is the 1.73 build.

**Environment notes:**
- Homebrew's `rustc` / `cargo` 1.98 come before rustup on this machine's `PATH`, so `rustup run 1.73.0 cargo` silently pairs cargo 1.73 with rustc 1.98. For 1.73, use `~/.rustup/toolchains/1.73.0-*/bin/{cargo,rustc}` explicitly with `RUSTC` set.
- Homebrew's clippy 1.98 adds lints (`chunks_exact_to_as_chunks`, `drain_collect`, …) that fail `cargo clippy -D warnings` in ~70 places of existing code. That predates this work. The suggested `as_chunks` fix needs Rust 1.88, so it must not be applied to engine code.

## 10. Risk

**MEDIUM.** The coupling is low, and the existing tests travel with the code.

The real risks:
- mrustc building ~23 path dependencies (Step 0 proved it for two path crates on PowerPC; Sol9 still comes from CI; see §9a)
- the `rbformats` split (C6, C7)
- the license audit

Every step's diff stays small, because the old paths keep working through re-exports until step 7.

## 11. Open questions

1. Publishing cadence: publish each crate as its family is done, or all at once after step 7? Either way, `dsk-clonezilla` and `fs-xerox` wait for Dani's go-ahead.
2. Do the crates carry a `rusty-backup` link and description, or stay neutral? Neutral reads better for outside users.
