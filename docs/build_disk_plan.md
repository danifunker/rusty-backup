# Build Disk: build any disk we support

Feature plan; work is on branch `add-build-disk` (cut from `main` 2026-11-05). Read
[CONTRIBUTING.md § Architecture policy (MVC)](../CONTRIBUTING.md#architecture-policy-mvc--read-before-adding-any-code)
first; it is binding. The user asked for full TUI parity including Build Disk,
and for the feature to be planned before it is built.

## Goal

One builder that makes any disk the engine can describe. A disk is three choices:

1. A **partition scheme**, or none (a bare volume or superfloppy).
2. A **filesystem** in each partition: formatted blank, filled from a source
   image, filled from a host folder, or left empty.
3. An **output**: a raw image, a container (CHD, VHD, QCOW2, 2MG, ...), or a
   physical device.

The GUI, the TUI and the CLI all drive the same controller, so a disk one of
them can build, the others can build too.

## Where it starts

Three separate paths build disks today. None of them can do everything:

| Path | Code | What it does |
|---|---|---|
| GUI Restore tab, Build Disk mode | `controller::restore` (`RestoreMode::BuildDisk`, `RestoreAction::Build*`), `controller::partition_edit::BuildAction` over `model::disk_builder::DiskBuilder`, the run in `model::provision_runner` | Image-file or device target with a size. The table type is any of the 11 in `provision::WRITABLE_TABLES`. You set alignment, heads and sectors per track (used by the cylinder schemes), and rows with size (`20M`, `rest`), type, name and an optional source image (its decoded size is measured in a job). Rows can be moved, removed and added. Validate, then Create Disk. A device target needs its name typed to confirm (`controller::restore::run`). Partitions without a source stay **empty**: no filesystem is formatted. |
| `rb-cli new floppy` / `new volume` | `src/cli/verbs/new.rs` (`FsKind`, `format_image`) | One bare volume of any of the 34 `FsKind` filesystems, written as a raw file. **The dispatch to the crate formatters lives in this CLI verb**, which is shell code. |
| `rb-cli new hd <scheme>` | `src/cli/verbs/new_partitioned_hd.rs` over `partition::provision` + `model::provision_runner` | Same engine as GUI Build Disk, plus two extras that live only in the verb: `--filesystem DOSTYPE=PATH` (`provision::write_rdb_filesystems`) and a fixed-VHD footer when the name ends in `.vhd` (`dsk_vm::vhd::build_vhd_footer`). `--fill N=PATH` pours in an image. Partitions are otherwise empty. |
| `rb-cli new hd x68k` / `sgi-efs`, `optical new sgi-efs / mac-hfs / mac-hfsplus / next-ufs` | `partition::{x68k_hdd_builder, sgi_hdd_builder, mac_cd_builder, next_cd_builder}` | Fixed-shape bootable builders. Each formats its own filesystem (Human68k on FAT, EFS, HFS / HFS+, 4.3BSD UFS). The SGI and CD builders can fill from a host folder. |
| TUI New Disk wizard | `src/cli/verbs/tui_app.rs` (`NewWizard`, `newdisk_create`); `controller::new_disk` only re-exports constants | Builds a `NewCommand` and calls `cli::verbs::new::run`. It offers 22 of the 34 filesystems, 10 hard-disk targets with **one** whole-disk partition, and the 4 CD targets. |

`rb-cli reformat` formats a partition in place, but only for HFS
(`src/cli/verbs/reformat.rs`). Restore's **New Disk** mode (restore a backup onto
a fresh MBR / GPT / APM) is a restore path, not a builder, and is out of scope.

## 1. The support matrix

The full cross product of scheme, filesystem and container is a few thousand
cells, and most are meaningless (who wants TR-DOS inside a Sun label). So the
matrix is the three axes, plus the natural pairings each scheme exists for.

Legend: **B** = buildable from a UI path today (G = GUI, C = CLI, T = TUI).
**U** = the engine pieces exist but no UI reaches them. **E** = an engine piece
is missing.

### 1a. Partition schemes

| Scheme | Engine writer | G | C | T | Status |
|---|---|:-:|:-:|:-:|---|
| None (bare volume) | the fs formatter itself | - | yes | yes | B (C, T) |
| MBR (4 primaries) | `provision::write_mbr` -> `mbr::build_minimal_mbr` | yes | yes | 1 part | B |
| GPT | `provision::write_gpt` | yes | yes | 1 part | B |
| APM | `provision::write_apm` -> `apm::build_minimal_apm` | yes | yes | 1 part | B |
| RDB | `provision::write_rdb`, `write_rdb_filesystems` | yes (no handlers) | yes | 1 part | B; handler embedding is C only |
| SGI volume header | `provision::write_sgi` | yes | yes | 1 part | B |
| SGI disk label | `provision::write_sgi_dklabel` | yes | yes | - | B (G, C) |
| Sun SMI VTOC | `provision::write_sun` | yes | yes | 1 part | B |
| NeXT | `provision::write_next` | yes | yes | - | B (G, C) |
| Solaris x86 VTOC | `provision::write_solaris_x86` | yes | yes | - | B (G, C) |
| AHDI | `provision::write_ahdi` (4 primaries) | yes | yes | 1 part | B |
| X68000 | `provision::write_x68k` | yes | yes | 1 part | B |
| MBR extended / logical | parse only | - | - | - | E |
| AHDI XGM extended chain | parse only (`partition_table_writers_backlog.md`) | - | - | - | E |
| APM-TS (pre-1986 Apple) | parse only | - | - | - | E |
| BSD disklabel | parse only | - | - | - | E |
| HP-UX LVM, AIX LVM | parse only (single-disk VGs) | - | - | - | E (L, low value) |
| DSD (two-sided DFS) | de-interleaving reader, no writer | - | - | - | E (S) |

Bootable flags and boot code: `write_mbr` always writes the active flag as
`false` and no boot code. `DiskBuilder` has no bootable column. Boot pieces
exist only in the fixed builders (X68000 IPL, SGI dvh, the CD builders) and in
`rb-cli make-bootable` for Mac disks.

### 1b. Filesystems (blank create)

Rows marked **stream** have a `Write + Seek` formatter, so they can format inside
a device partition without building the whole volume in RAM. The rest return a
`Vec<u8>`.

| Filesystem | Engine creator | stream | C (`new`) | T | G | Status |
|---|---|:-:|:-:|:-:|:-:|---|
| FAT12/16/32 | `fs_pc::fat::create_blank_fat{,32}`, `write_blank_fat_metadata_to_sink` | yes | floppy, volume | yes | - | B (C, T) |
| HFS | `fs_mac::hfs::create_blank_hfs_sized`, `write_blank_hfs_into` | yes | floppy, volume; `reformat` | yes | - | B (C, T) |
| HFV (flat HFS, <= 2047 MB) | `fs_mac::hfv::build_blank_hfv` | - | volume | yes | - | B (C, T) |
| HFS+ / HFSX | `fs_mac::hfsplus::create_blank_hfsplus*`, `write_blank_hfsplus_into` | yes | volume | yes | - | B (C, T) |
| MFS | `fs_mac::mfs::create_blank_mfs` | - | floppy | yes | - | B (C, T) |
| NTFS | `fs_pc::ntfs_format::create_ntfs` | yes | volume | yes | - | B (C, T) |
| ext2 / ext3 / ext4 | `fs_unix::ext_format::write_blank_ext{2,3,4}` | yes | volume | yes | - | B (C, T) |
| AFFS (OFS / FFS) | `fs_amiga::affs::create_blank_affs` | - | volume | yes | - | B (C, T) |
| ProDOS | `fs_apple2::prodos::create_blank_prodos` | - | volume | yes | - | B (C, T) |
| IRIX EFS | `fs_unix::efs::write_blank_efs` | yes | volume | yes | - | B (C, T) |
| Minix V1 / V2 / V3 | `fs_unix::minix::create_blank_minix` | - | floppy / volume | yes | - | B (C, T) |
| Atari DOS 2.0S, Apple DOS 3.3, OS-9, UCSD, TR-DOS, TI-99, ADFS (E) | `create_blank_*` in fs-8bit / fs-apple2 / fs-adfs | - | floppy | yes | - | B (C, T) |
| CP/M (9 DPBs) | `fs_cpm::cpm::create_blank(dpb)` | - | floppy | - (needs a preset) | - | B (C) |
| Oric Jasmin | `fs_8bit::oric::create_blank_oric` | - | floppy | - | - | B (C) |
| HPFS | `fs_hpfs::hpfs::create_blank_hpfs` | - | volume | - | - | B (C) |
| PFS3, SFS | `fs_amiga::{pfs3,sfs}::create_blank_*` | - | volume | - | - | B (C) |
| SGI EFS v1 | `fs_unix::efs_v1::create_blank_efs_v1` | - | volume | - | - | B (C) |
| XFS | `fs_xfs::xfs::format::write_blank_xfs` | yes | volume | - | - | B (C) |
| BFS, BeOS OFS | `fs_unix::{bfs_write,ofs_write}::create_blank_*` | - | volume | - | - | B (C) |
| UFS1 (4.4BSD, 4.3BSD/NeXT, Solaris) | `fs_unix::ufs_format::write_blank_ufs1` | yes | volume | - | - | B (C) |
| exFAT | `fs_pc::exfat::create_blank_exfat` | yes | - | - | - | U |
| CBM DOS (1541 ... 8250) | `fs_8bit::cbm::create_blank(variant, ..)` | - | - | - | - | U |
| Acorn DFS | `fs_8bit::dfs::create_blank_dfs` | - | - | - | - | U |
| RS-DOS | `fs_8bit::rsdos::create_blank(tracks)` | - | - | - | - | U |
| DragonDOS | `fs_8bit::dragondos::create_blank(tracks, sides)` | - | - | - | - | U |
| Human68k (standalone) | `fs_pc::human68k::create_blank_human68k` (`new hd x68k` uses `create_blank_fat_with_sector_size`) | - | via `new hd x68k` only | - | - | U |
| Alto BFS / TFS | `fs_xerox::alto::write::create_blank(geometry)` | - | - | - | - | U |
| Pilot / Cedar | `fs_xerox::alto::pilot::create_blank` (tests only; read-only in the GUI) | - | - | - | - | U (low) |
| APFS, btrfs, JFS2, AIX JFS1, ReiserFS, VxFS, QNX 4, QNX 6, Lisa | none | | | | | E |
| QNX 2, HP LIF, QDOS (QXL.WIN) | none (all three edit an existing volume) | | | | | E (S-M each) |
| SquashFS | whole-image rebuild on edit, no blank create | | | | | E (S) |
| ADFS old-map D / F, Atari DOS enhanced density | E-format and single density only | | | | | E (S each) |

### 1c. Outputs

| Output | Engine | Reached from a build today |
|---|---|---|
| Raw image | `compat::write_sparse`, `provision_runner` | all three paths |
| Device | `provision_runner` (`open_target_preserving`, `SectorAlignedWriter`) | GUI Build Disk only; `rb-cli new` writes files only |
| Fixed VHD | `dsk_vm::vhd::build_vhd_footer` | `new hd` only (a `.vhd` name); the code is in the CLI verb |
| CHD (hd / dvd), dynamic VHD, QCOW2, VMDK, 2MG, WOZ, MOOF, DC42 | `rbformats::export::ExportFormat` via `model::bulk_convert_runner` | a second step (`rb-cli convert`, Bulk Convert); never part of the build |
| ATR, XDF / HDM / DIM / D88, ADZ / HDZ | `dsk_floppy::*::encode_*`, `model::container_edit` (re-encode) | not from a build; `new floppy` writes the bare sectors whatever the extension |
| ISO (optical) | the CD builders | `optical new` and the TUI wizard |

### 1d. Natural pairings (scheme x filesystem)

The pairings each scheme exists for. **One step** means formatted during the
build. **Two step** means `new volume` and then `new hd --fill`, or a source
image in GUI Build Disk.

| Scheme | Filesystems | Today |
|---|---|---|
| MBR | FAT, NTFS, HPFS, ext, Minix, exFAT | two step (exFAT: U) |
| GPT | FAT32 ESP, ext, HFS+, NTFS, exFAT | two step (exFAT: U) |
| APM | HFS, HFS+, ProDOS (A/UX UFS: E) | two step; CD: one step (`optical new mac-*`) |
| RDB | AFFS, PFS3, SFS | two step, handlers via CLI |
| SGI vh | EFS, XFS | EFS one step (`new hd sgi-efs`); XFS two step |
| SGI disk label | EFS v1 | two step |
| Sun / Solaris x86 | UFS (Solaris) | two step |
| NeXT | UFS (4.3BSD) | two step; CD one step (`optical new next-ufs`) |
| AHDI | FAT (GEM / BGM) | two step |
| X68000 | Human68k | one step, bootable (`new hd x68k`) |
| None | every row of 1b | one step (C, and 22 fs in T) |

### Counts

- Schemes: 11 writable and buildable (GUI and CLI all 11, TUI 8 with one
  partition each); 7 parse-only (E).
- Filesystems: 34 creatable through `rb-cli new` (C). The TUI reaches 22 of them
  and the GUI none. 8 have an engine creator but no UI (U). About 13 have no
  creator (E), plus 3 variant gaps.
- Formatted partitions in one step: only the 6 fixed builders. Every other
  scheme x filesystem pairing is two steps on the CLI, or a pre-built source
  image in the GUI.
- Containers: raw everywhere, fixed VHD on `new hd` only, and everything else
  only as a separate convert.

## 2. Engine gaps

All of these belong in the model or a crate, never in a shell.

| # | Gap | Where | Size |
|---|---|---|:-:|
| G1 | **Format dispatch in the model.** Move `FsKind`, its option bag (`NewArgs`) and `format_image` out of `src/cli/verbs/new.rs` into `src/model/format.rs` (`FsKind` as a plain enum, no clap derive; the CLI keeps a `ValueEnum` mirror). Add per-fs capabilities: min / max size, fixed geometry, label rules, options, stream or `Vec`. | `src/model` | M |
| G2 | **Format into an extent.** `format_into(&mut W, offset, len, FsKind, opts)` so the builder can format partition N on an image or a device. Stream formatters write straight through. `Vec` formatters write the buffer at the offset, with a size cap (refuse above, for example, 2 GiB rather than eat RAM). | `src/model`, plus `write_*_into` variants in crates | M |
| G3 | **Streaming writers** for the large `Vec`-only filesystems: HPFS, PFS3, SFS, AFFS, BFS, OFS (the floppy-sized ones can stay `Vec`). Each writes only metadata through `Write + Seek` with memory bounded by one bitmap band / block, never the volume. The old `create_blank_*` becomes a wrapper that streams into a `Cursor`, and a test proves the bytes match the old output before the old code goes. | `fs-hpfs`, `fs-amiga`, `fs-unix` (publish + bump) | M each |
| G13 | **Layout donor**: read an existing disk's table into rows plus its verbatim head region; the label schemes resize through `restore_patch`. | `src/model`, `src/partition` | M |
| G14 | **More disc expansion**: ZIP (and Mac zips), MacBinary / AppleSingle single files, disk-image contents; `--expand-gunzip` on every CD format. **Done 2026-11-05** (`fs-tar` 0.1.2 `zip_import`, `fs-mac` 0.1.4 `parse_applesingle`, `fs::image_import`, `--expand-images`). | `src/fs/dir_import.rs` | S-M |
| G4 | **Wire the U rows**: exFAT, CBM, DFS, RS-DOS, DragonDOS, Human68k, Alto BFS (and Pilot if wanted) into G1's dispatch with their geometry options. | `src/model` | S each |
| G5 | **Container output as a build step**: build to a temp raw or straight to the sink, then encode through `ExportFormat` (CHD behind `chd`), the floppy encoders and fixed VHD. Lift the VHD footer out of `new_partitioned_hd.rs`. | `src/model` | M |
| G6 | **Bootable flag and boot code** per scheme: MBR active byte and an optional boot-code donor, RDB boot priority, the SGI dvh / Sun / NeXT boot slices that already exist, APM driver partition (reuse the `make-bootable` pieces). | `partition::provision`, `part-tables` | M |
| G7 | **RDB filesystem handlers** in the request (`write_rdb_filesystems` is in the engine; only the CLI passes them). | `model::provision_runner` | S |
| G8 | **Fill from host folder** for any editable fs: format, then import, reusing the import / batch apply path that `sgi-efs --from-dir` and the CD builders already use. | `src/model` | M |
| G9 | **Fixed builders as presets**: express x68k, sgi-efs and the four CD targets as builder presets (scheme + rows + boot pieces) where the shape fits, and keep the special code (IPL, volume header files) as preset hooks. | `src/partition`, `src/model` | M |
| G10 | Missing table writers: MBR extended / logical, AHDI XGM, BSD disklabel, APM-TS, DSD. | `part-tables`, `provision` | S-M each |
| G11 | Missing filesystem creators: QNX 2, HP LIF, QXL.WIN, SquashFS (empty), ADFS D / F, Atari ED first. APFS / btrfs / JFS / ReiserFS / VxFS / QNX 4/6 / Lisa are L and read-mostly today. Each needs its own oracle. | the fs crate | S-L |

### Found in phase 1

- **Streaming writers assume a zeroed target.** Every formatter writes only its metadata and
  leaves the rest to read as zero, which a fresh or sparse file does. A physical disk or a
  reused image does not: old inode-table or directory bytes would surface as garbage. Phase 4
  (formatting rows on a device) has to zero what each formatter skips that its fs reads (the
  inode tables, directory areas and bitmaps), not the whole partition. `FsCaps` grows a
  "regions that must read zero" answer per fs, or the writers take a `zero_fill` flag.
- **UFS writes its inode tables** (as `newfs` does), so a 1 TiB UFS1 writes tens of GB. That is
  correct, but Build Disk shows progress for it.

## 3. Proposed design

### One controller

`controller::build_disk::BuildDiskController` owns one `model::disk_builder::DiskBuilder`
(extended, not replaced). The three current entry points become views of it:

- GUI: Restore's Build Disk mode hosts it (the partition window renders its
  view), and a new **New Disk** dialog (File menu / Inspect) opens it with
  scheme = None for a bare volume.
- TUI: the New Disk wizard becomes this controller's screen, with the
  partition-row editor the GUI has.
- CLI: `rb-cli new floppy / volume / hd` keep their grammar and build a
  `BuildRequest` from it through the same model. Nothing in the verbs formats
  anything directly.

`RestoreController` keeps delegating `RestoreAction::Build(..)` to the new
controller, the same way it now owns the `DiskBuilder` and forwards `BuildAction`.

### Data model

```text
BuildRequest
  scheme: TableKind | None            // None = bare volume
  disk_size, geometry (heads, spt), align
  preset: Option<Preset>              // x68k-bootable, sgi-efs, mac-cd, ...
  rows: [Row]
  output: Output
Row
  size_text ("20M" | "rest"), type_text, name   // as today
  bootable: bool                                // per-scheme meaning
  content: Empty | Format{fs: FsKind, label, opts} | Image(path) | Folder(path)
  scheme_fields: DosType + boot pri (RDB) | VTOC tag + flags (Sun, Sol x86)
                 | fs-name (NeXT) | role (SGI dklabel) | GEM/BGM (AHDI)
Output
  File{path, container: Raw | Vhd | Chd | Qcow2 | ... | Atr | D88 ...} | Device{path}
```

`type_text` stays the table's own vocabulary (`type_catalog`). Choosing a
filesystem for a row proposes its type (FAT32 -> `0C`, HFS -> `Apple_HFS`,
PFS3 -> `PFS\3`, Sun UFS -> `root` / `usr`) unless the user typed one.

### Validation (one `validate()` gate, as today)

- Layout: `provision::place` as now (slots, reserved head / tail, alignment,
  one `rest`).
- Per-fs size: the fs's min / max from G1. Examples: HFV <= 2047 MB; classic
  HFS 65535 allocation blocks; FAT12 / 16 / 32 cluster-count windows (and
  `--fat32` for an ESP); ProDOS <= 65535 blocks; XFS >= 32 MiB; BFS >= 72 MiB;
  Minix V1 <= 64 MiB; fixed-geometry floppies an exact size. AHDI GEM over
  16 MiB is promoted to BGM (`provision::effective_type`).
- Fs vs scheme: warn (not block) on a pairing the target OS cannot read, for
  example 4.4BSD UFS in a NeXT label (needs `ufs-43bsd`) or in a Sun label
  (needs `ufs-solaris`). Warn when an Amiga DosType has no ROM handler and no
  handler was given.
- Labels: each fs's own rules (ProDOS name set, OS-9 sanitising, MFS 27 bytes).
- Output: container vs content (WOZ / MOOF only for Apple floppy geometries,
  ATR for Atari, CHD needs `chd`). Device: the typed device-name confirmation,
  as today.

### Actions and effects

`BuildDiskAction` extends `BuildAction`: `SetScheme`, `SetPreset`,
`SetRowContent`, `SetRowFs`, `SetRowFsOption`, `SetRowBootable`,
`SetRowSchemeField`, `PickRowSource` / `PickRowFolder`, `SetOutput`,
`PickOutput`, `Validate`, `Create`, `Cancel`. Effects are the shared ones:
`PickFile` / `PickFolder` (save), `Confirm` for a device (with the name rule),
`Message`. The build is one `model::job::Job`: table, then format, fill and
import each row, then the container encode, with cancel and progress. Every
shell matches the actions and effects with no `_` arm.

### Constraints

- Engine, model and controller code stays inside Rust 1.73. Use the compat
  shims, not clippy's `is_multiple_of` / `io::Error::other` fixes.
- mrustc (Solaris 9, PowerPC): no proc-macros in the new model types, so no
  clap or serde derive on `FsKind` in the model. Do the conversion in the CLI.
- Slim builds: CHD output is behind `chd`; the CD presets need nothing optical,
  but list them only where `optical new` exists today. A
  `no-physical-devices` build offers image targets only.
- `docs/platform_matrix.md`: the Restore tab and Partition window rows lose
  their "TUI: no ... build" gap. Add a New Disk row.

## 4. Phases

Each phase ships on its own: tests green, `scripts/check-boundaries.sh`, GUI build.

| # | Phase | Tests |
|---|---|---|
| 1 | G1: format dispatch in `model::format`. `rb-cli new floppy / volume` call it; byte-identical output. **Done 2026-11-05**: `model::format::{FsKind, FsCaps, FormatOptions, format_into, format_to_file, Window}`; FAT, HFS, HFS+ and HFV now stream (`fs-mac` 0.1.5 adds the sized HFS / HFS+ writers); `new volume ufs --bytes-per-inode` works (it was refused). | existing `new-volume*.toml` tier-1 cases, plus a model test that formats every `FsKind` at its minimum, a middle size and its largest buildable size (capped at 4 GiB) and opens and fscks it; images compared byte for byte with the old CLI (only clock / UUID bytes differ) |
| 2 | `controller::build_disk` over `DiskBuilder` with the wizard's step machine (§ 7), and the GUI's top-level **Build Disk** tab drawing it. It covers today's capability (partitioned disk, empty or image rows, image or device target); Restore's Build Disk mode moves here and leaves the Restore tab. | `tests/ui_scenarios/restore_build_disk.toml` moves to `build_disk_basic.toml`; wizard step tests (Next blocked with a reason, Back keeps entries) |
| 3 | TUI Build Disk screen on the same steps: scheme, geometry, rows (size / type / name / source), image or device target, device confirmation. Retire the TUI's one-partition HD list, its `NewWizard` and its `NewCommand` building. **Done 2026-11-05** (`tui_app/build_disk.rs`, a Build Disk tab); the TUI New Disk tab keeps only the two bootable presets (X68000, SGI EFS, until phase 9) and the CD-ROM targets (until the CD plan). | ui_scenario for multi-row MBR and RDB; TUI key-map entries |
| 3b | G3: streaming writers for HPFS, AFFS, PFS3, SFS, BFS, OFS, each byte-identical to today's blank volume, each published. **Done 2026-11-05**: `write_blank_{hpfs,affs,pfs3,sfs,bfs,ofs}_into` (fs-hpfs 0.1.1, fs-amiga 0.1.1, fs-unix 0.2.2); each `create_blank_*` now wraps its stream, and the old in-memory code stays in the tests as the byte-for-byte reference. Memory is bounded: HPFS and AFFS hold one bitmap band / page, SFS one bitmap block, PFS3 its used reserved blocks (a few MiB), BFS and OFS the bitmap prefix the metadata marks. A 64 GiB HPFS formats into a sparse file. | per fs: old-vs-new hash test; its existing fsck oracle on a 4 GiB blank volume built with a small memory limit (`ulimit -v`) |
| 4 | G2 + per-row **Format** content: format each row during the build, GUI and TUI. `new hd --partition SIZE:TYPE:NAME --format N=FS[:LABEL]` on the CLI. Start with the stream-capable filesystems. **Done 2026-11-05**: the GUI and TUI got formatted rows in phase 2; the CLI is `rb-cli build TARGET --table KIND --partition SPEC... --format N=FS[:LABEL]... --fill N=PATH...` (or `--fs FS --label L` for a bare volume), which drives the same controller, so its refusals are the wizard's. Per-filesystem options (FAT32, block size, ...) are GUI / TUI only for now. | tier-1 cases per scheme: build, then `fsck` and `ls` each partition; emulator scenarios: `fat.win7-blank`-style for MBR+FAT / NTFS, `ufs1-be.solaris-blank` on a Sun label, `ufs43.nextstep-blank` on a NeXT label, FS-UAE for RDB+PFS3 / SFS |
| 4b | G13: start from a donor disk's layout (copy / format blank / empty per row, verbatim head region, new disk size). **Done 2026-11-05** for image donors: the Disk step's "Start from: an existing disk's layout" (`model::disk_builder::Donor`, `DiskBuilder::from_donor`) takes the table and one row per partition, each copied by default and switchable to Format or Empty; a label donor (SGI, Sun, NeXT, RDB, AHDI, X68000, SGI disk label) keeps its head verbatim and its slots fixed, laid out and rewritten by `restore_patch`, which now also regenerates the whole-disk entries (SGI VOLUME and cylinder count, Sun backup slice and ncyl / pcyl) so a bigger disk is described whole (restores to a bigger disk gain this too). MBR / GPT / APM donors are rewritten by the provision writers with their rows free. `rb-cli build --from DONOR [--partition SIZE]... [--format N=FS] [--empty N]`. Not yet: a copied filesystem keeps its size (growing it in place, "Copy and resize", is next), physical-disk and backup-folder donors, and MBR logicals. | SGI vh donor -> larger disk, IRIX mounts it (Iris); RDB donor with handlers -> FS-UAE; MBR / APM donors; Review lists copied vs regenerated |
| 5 | Bare-volume builds in the same controller (scheme = None), a GUI New Disk dialog, every `FsKind` in the TUI (CP/M preset picker, Oric, HPFS, PFS3, SFS, EFS v1, XFS, BFS, OFS, UFS x3). **Done 2026-11-05** with phase 2: the Kind step's single volume is every `FsKind` in the GUI and the TUI (CP/M preset picker included), and the TUI New Disk tab keeps only the presets and CDs. | ui_scenario per class; existing `new-volume` CLI cases |
| 6 | G5: container output in the build (CHD, VHD, QCOW2, VMDK, 2MG, WOZ, DC42, ATR, D88 ...). **Done 2026-11-05**: an image output picks a container (raw, fixed / dynamic VHD, QCOW2, VMDK, CHD, 2MG, DiskCopy 4.2, WOZ, MOOF; the save name's extension picks one); the disk is built raw beside the target and encoded through `bulk_convert_runner::convert_image`, the Bulk Convert step. WOZ / MOOF / DiskCopy 4.2 refuse a size that is not one of their floppies. The X68000 floppy containers and ATR are not offered yet. | tier-4 `convert.toml`-style cases; `chdman info` and MAME for CHD; `qemu-img check` |
| 7 | G4: the U-row filesystems (exFAT, CBM, DFS, RS-DOS, DragonDOS, Human68k, Alto). **Done 2026-11-05** for exFAT (streams, Windows cluster sizes), CBM (D64 / D64-40 / D71 / D81 / D80 / D82), Acorn DFS (40 / 80 tracks), RS-DOS (35 / 40 / 80) and DragonDOS (40 / 80, one or two sides), with a geometry option; each geometry formats, opens and fscks in a test. That test found blank D71s leaving track 53 free, which the 1571 reserves whole (fs-8bit 0.1.1). Deferred: Human68k (its creator takes a donor template; the X68000 preset covers it) and Alto / Pilot (packs are PDI files, not raw volumes). | fs fsck round-trip per fs; VICE for CBM, the MiSTer cores where there is no emulator |
| 8 | G6 + G7: bootable flags, boot-code donors, RDB handlers in GUI / TUI. **Done 2026-11-05** for the flags and handlers: a Boot column (MBR active flag, one partition; RDB bootable flag, several) and the RDB handler list (`Add PFS\3 handler...` for each DosType the rows use that the ROM lacks, with a Review warning when none is added) in the GUI and TUI; `rb-cli build --bootable N --filesystem DOSTYPE=PATH`. Handler loading and its HUNK_HEADER check moved from `new hd` into `provision_runner::load_rdb_handlers`. Boot-code donors stay on `docs/bootable_disks_backlog.md` (decision 3). | QEMU MBR boot (the GRUB MBR-gap recipe); FS-UAE boots an RDB with an embedded handler |
| 9 | G8 + G9: fill from host folder; the fixed builders as presets. `new hd x68k` / `sgi-efs` / `optical new` become presets with byte-identical output. **Done 2026-11-05** for the hard-disk presets: the Kind step's Bootable preset offers the X68000 (SASI / SCSI, 1-8 Human68k partitions, an optional system floppy cloned in) and SGI IRIX (EFS label) disks, built by the same `build_x68k_hdd` / `write_sgi_efs_hdd` calls `new hd` makes (a test compares them), to a file, a device or a container; `rb-cli build --preset x68k|sgi-efs`. Fill from a host folder (G8) and the CD presets stay with docs/cdrom_building_plan.md; the TUI New Disk tab still offers the presets as shortcuts beside its CD targets. | existing x68k / sgi-efs / CD cases unchanged; Iris (IRIX 6.5) mounts a preset sgi-efs disk |
| 10+ | G10 / G11 one writer or creator at a time, each with its real-OS oracle (`regression-tests/ORACLE-HOSTS.md`, `EMULATORS.md`). | per item |

Phases 1-3 give the TUI parity the user asked for. 4-6 give one-step "build any
disk". 7-10 widen what "any" covers.

Each phase walks the pre-commit sync: README (Filesystems / formats tables
mention creation), `docs/cli-reference.md`, `docs/platform_matrix.md`, and
`docs/filesystem_coverage_audit.md`'s Create column.

## 5. Open questions

1. **CLI shape.** Grow `rb-cli new hd` (add `--format N=FS`, `--bootable N`,
   `--container`), or add one `rb-cli build` verb that takes the full request
   (flags, or a TOML / JSON spec like `batch`) and keep `new` as shorthand?
2. **Order.** Which schemes and filesystems first after TUI parity? The
   proposal starts with MBR + FAT / NTFS, RDB + PFS3 / SFS, APM + HFS / HFS+, and
   Sun / NeXT + UFS, because those have oracles.
3. **Boot scope.** Is "bootable" only flags plus donor boot code (MBR code, Mac
   boot blocks and driver, Amiga handlers), or do we ship boot code of our own?
   The X68000 IPL stub is the only one we ship today.
4. **Where the GUI entry lives.** Keep Build Disk inside the Restore tab and add
   a New Disk dialog, or move both into a top-level "Build" tab?
5. **Fixed builders.** Fold `new hd x68k` / `sgi-efs` and `optical new` into
   presets (phase 9), or leave them as separate verbs that only share the
   format dispatch?
6. **Engine-missing filesystems.** Do any of the E rows (QNX 2, HP LIF,
   QXL.WIN, SquashFS empty, ADFS D / F) belong in this feature, or do they stay
   on the filesystem backlog?
7. **In-RAM formatters.** For the `Vec` filesystems on a device: cap the size
   and refuse above it, or write streaming variants first (G3)?

## 6. Decisions (answered 2026-11-04)

1. **CLI:** a new `rb-cli build` verb takes the full request. `rb-cli new` stays for at
   least a couple of months as shorthand, then gets a deprecation notice.
2. **Order:** as proposed (MBR + FAT / NTFS, RDB + PFS3 / SFS, APM + HFS / HFS+, Sun / NeXT +
   UFS first). Each phase also adds test disks for the combinations it opens, not only the
   oracle-backed ones.
3. **Boot:** keep what exists (the X68000 IPL, the fixed builders' boot pieces,
   `make-bootable` for Mac). Per-filesystem "what bootable means" and booting from a
   validated donor disk go to `docs/bootable_disks_backlog.md`, not this feature.
4. **GUI entry:** Build Disk becomes its own section of the app (a top-level tab), out of
   the Restore tab.
5. **Fixed builders:** they become Build Disk presets (phase 9). `rb-cli new hd x68k` /
   `sgi-efs` and `optical new` stay as shortcuts with byte-identical output.
6. **Filesystems:** Build Disk can create every filesystem we support writing. The E rows
   with no creator stay on the filesystem backlog until a writer exists; each writer that
   lands joins Build Disk.
7. **Size limits:** AFFS, PFS3 (plus its multi-MI formatting for large disks), SFS, HPFS and
   BFS get streaming writers (G3). **Revised 2026-11-05:** all five, plus BeOS OFS, go
   streaming *before* Build Disk offers them (G3 moves ahead of phase 4's formatted rows),
   because rb-cli also runs on vintage machines (the Rust 1.73 / PowerPC / Solaris builds)
   where even a few hundred MB in RAM is out of reach; formatting an HPFS partition on a
   real disk from OS/2-era hardware has to work. No in-RAM cap is shown for them. The small in-RAM
   filesystems (ProDOS, MFS, Minix V1, the floppy formats, BeOS OFS, EFS v1) keep a 64 MB
   cap. Real-world maxima were given from memory; confirm each when its writer is done.
8. **Targets:** an image file (in any container) or a physical disk, as today's Build Disk
   does; a physical disk needs its device name typed to confirm.

## 7. The wizard (asked 2026-11-05)

The user asked for Build Disk to be a wizard in the GUI. The steps are controller state
(`BuildDiskController::step()`, `steps()`, `next_blocked() -> Option<String>`), so the TUI walks the
same steps and a scenario test drives both. The CLI's `rb-cli build` skips the steps and fills
the same `BuildRequest`; the same validators run.

| # | Step | Shown for | What it asks | Next is blocked when |
|---|---|---|---|---|
| 1 | **Kind** | always | Floppy, single volume (no table), partitioned disk, or a preset (an optical-disc kind comes with the CD plan) (X68000, SGI EFS hard disk, ...) | never |
| 2 | **Disk** | floppy: format + geometry; volume: filesystem + size; partitioned: scheme + size + alignment / heads / sectors; preset: its few fields | the base shape | size outside the fs / scheme limits (the reason is shown) |
| 3 | **Partitions** | partitioned, presets with rows | rows: size, type, name, content (empty, format with a filesystem, source image; host folder from phase 9), with the layout bar | the layout does not place, or a row's fs does not fit its size |
| 4 | **Filesystem options** | any row or volume that formats | label and per-fs options (FAT type, cluster size, HFS+ case, ext journal, CP/M DPB, ...) per formatted row | a label breaks its fs's rules |
| 5 | **Output** | always | image file (path + container: raw, CHD, VHD, ...) or a physical disk: an SD / CF card in a reader, a USB stick, any disk the device list shows (the Restore tab's list, with its identity check). A device fixes the disk size to its capacity, so step 2's size follows the card and `rest` fills it | no path or device; a container that cannot hold this content |
| 6 | **Review** | always | a summary of every choice, the warnings (fs vs scheme pairings, a missing Amiga handler), and for a device the typed device-name confirmation | the device name does not match |
| 7 | **Build** | always | progress and log of the one build job; on success "Open in Inspect" and "Build another" | (running) |

### Starting from an existing disk's layout (asked 2026-11-05)

Step 2 for a partitioned disk asks **Start from: a blank table, or an existing disk's layout**.
The donor is an image, a backup or a physical disk. Example: build a bigger SGI disk from a
small working one, keeping its volume header (sash, ide and the other volume-header files),
its swap and its slot layout, with a new disk size.

- The donor's table is read with the existing parsers, and its **head region rides
  verbatim** (SGI volume header and its files, RDB and its filesystem handlers, the X68000
  IPL, NeXT boot blocks, Sun boot slices). The resize uses `partition::restore_patch`, the
  same code that resizes these labels on restore today. MBR / GPT / APM tables are rewritten
  by the `provision` writers; APM driver partitions (`Apple_Driver43`, `Apple_Patches`, ...)
  are copied as rows.
- Each donor partition becomes a row, with its **content** picked per row:
  - **Copy**: the donor's bytes. This is the default for special partitions such as the
    volume header, drivers, boot slices and RDB handlers. A filesystem that can resize grows
    or shrinks to the row's size; one that can't keeps its size, and the rest of the row is
    padded.
  - **Format blank**: same filesystem and label as the donor. This is the default for data
    partitions.
  - **Empty**.
- Whole-disk entries (SGI slot 10 `volume`, Sun slice 2 `backup`) are not rows: they are
  regenerated to the new disk size.
- The rows stay editable, so sizes can change and rows can be added where the scheme has
  free slots. `rest` goes on the last data row by default.
- Review lists what came from the donor and what was regenerated.

This adds **G13: layout donor** (`model::disk_builder::DiskBuilder::from_donor`, reading a
table into rows plus the verbatim head region; the build runs `restore_patch` for the label
schemes) as phase 4b.

### Checking that each filesystem supports what is asked

The wizard only offers what the model says works, and tests check that the model is right:

- **Capabilities in the model (G1):** per `FsKind`, the minimum and maximum size, block /
  cluster choices, label rules, the schemes it pairs with, and whether it can format,
  resize, and be populated (has an editor). The size field, the filesystem list per scheme,
  the "Copy and resize" choice and the label field all read these.
- **A capability test** builds every `FsKind` at its minimum, a middle size and its maximum
  (streamed and sparse), then runs our fsck and the fs's oracle where one exists. A claimed
  limit that fails is a test failure, not a user bug report.
- **A pairing test** builds each natural pairing in § 1d (scheme x fs), then `fsck` + `ls` +
  a `put` / `get` round trip on every partition. The real-OS emulator scenarios
  (`regression-tests/emulator/`) cover the pairings that have an emulator.
- A capability that is not proven is not offered. For a donor row, the wizard checks
  `can_resize` before it offers "Copy and resize".

### Optical discs

Moved to [cdrom_building_plan.md](cdrom_building_plan.md) (2026-11-05): mastering discs
from content, the ISO 9660 writer and CD audio are a later plan. Build Disk keeps the
existing CD builders (`optical new`) as they are; only their archive expansion (G14) is
being done now, ahead of the phases.

