# Platform matrix

The single list of every build target: what each ships, which toolchain builds
it, and what it can't do. When you add a feature, walk this table (CLAUDE.md
§ "Feature fan-out"). Plan background: [native_frontends_plan.md](native_frontends_plan.md) §3 and §7.

`scripts/check-platform-matrix.py` (run in CI) fails if the **CI target**
column and the `ALL=` list in the `plan` job of `.github/workflows/release.yml`
disagree. A row with CI target `-` is built outside CI.

**Worlds:**
- **modern:** current stable Rust and current crates.
- **vintage:** Rust 1.73 (`rb-cli-vintage/`) or mrustc (`rb-cli-ppc/`,
  `rb-cli-sol9/`), with frozen dependencies. Engine, model and view-model code
  must build here: see CLAUDE.md § "Engine code must compile on Rust 1.73".

**Features:** `default` is `gui, chd, optical, audio, native-zlib, native-zstd,
seekable-zstd, remote, tui, crypto, yaml` (root `Cargo.toml`).

## Targets

<!-- matrix:begin -->
| Target | CI target | World | Ships today | Planned shell | Toolchain | Features | Limits |
|---|---|---|---|---|---|---|---|
| Windows 10/11 x64, i686 | windows | modern | egui GUI + rb-cli, Inno installer | Win32 (Phase 5) | stable | default | |
| Windows 11 ARM64 | windows-arm64 | modern | egui GUI + rb-cli | Win32 (Phase 5) | stable | default | |
| macOS 10.13+ arm64, x64 | macos | modern | egui .app + rb-cli, DMG | AppKit (Phase 3, first) | stable | default | |
| Linux x64, arm64 (AppImage) | appimage | modern | egui AppImage + rb-cli | GTK4 (Phase 4); GTK3 for old distros | stable | default | |
| Linux x64, arm64 (deb, rpm, Arch) | linux-packages | modern | egui GUI + rb-cli packages | GTK4 (Phase 4) | stable | default | |
| Linux riscv64 GUI | riscv64 | modern | egui GUI + rb-cli, deb + rpm | GTK4 (Phase 4) | stable | default | |
| Linux static musl x64, arm64, i586, i686, ppc64, ppc32 | musl | modern | rb-cli + TUI | TUI | stable; i586 pinned 1.98.1; ppc32 nightly + build-std; ppc via cargo-zigbuild | pure-zstd, remote, tui | no GUI, no CHD, no optical, no crypto |
| Linux glibc ppc64, ppc, riscv64 (extra-arch) | extra-arch | modern | rb-cli + TUI | TUI | stable | pure-zstd, remote, tui | no GUI, no CHD, no optical, no crypto |
| MiSTer armv7 | mister | modern | rb-cli-mini + TUI, MiSTer Downloader db | TUI | stable | chd, pure-zstd, remote, optical, tui | no GUI; framebuffer console, no GL |
| Buildroot appliance i586 / i686 | appliance | modern | static i586-musl rb-cli + crossterm menu, hybrid ISO | TUI or menu (open decision 4) | stable (Docker cross, `docker/cross-i586-musl.Dockerfile`) | pure-zstd, remote | no GUI, no TUI, no CHD, no optical, no crypto |
| macOS 10.7+ x64 | macos-vintage | vintage | rb-cli + TUI | AppKit on 1.73 bindings or ObjC over a C ABI (Phase 6) | 1.73 | native-zstd, remote, tui, rust173-polyfill, yaml | no CHD, no optical, no crypto (edition-2024 deps) |
| Windows 7/8 x64, i686 | windows-vintage | vintage | rb-cli + TUI | Win32 on `windows` 0.58 (Phase 6) | 1.73 | native-zstd, remote, tui, rust173-polyfill, windows-legacy, yaml | no CHD, no optical, no crypto |
| Solaris 9 SPARC | sol9 | vintage | rb-cli + TUI | Motif/CDE over a C ABI (deferred, §8) | mrustc (sparcv9-sun-solaris2.9) | native-zstd, remote, tui, rust173-polyfill | no GUI; mrustc gaps: no proc-macros, no inline asm |
| Mac OS X 10.4/10.5 PowerPC | - | vintage | rb-cli + TUI (G3 build) | Carbon/Cocoa over a C ABI (deferred, §8) | mrustc, C compiled on a G5 (`scripts/build-ppc.sh`) | native-zstd, remote, tui, rust173-polyfill, os-stub | no physical devices (os-stub); not in CI |
| DOS (crusty-backup) | cbdos | n/a | separate C program, FreeDOS floppy + CD | conio (out of scope) | DJGPP | - | not the Rust engine |
<!-- matrix:end -->

Not a platform: `crates-msrv` (every crate's tests on Rust 1.73, and qnx2fs
for wasm32) is a CI target too; the check script knows it.

## Shells

| Shell | Code | Targets |
|---|---|---|
| egui | `src/gui/` | the modern GUI rows, until Phase 7 retires it |
| TUI | `src/cli/verbs/tui*` (ratatui) | every rb-cli row |
| AppKit, GTK4, Win32 | `frontends/{appkit,gtk4,win32}/` (not yet) | Phases 3-5 |
| Vintage shells | `frontends-vintage/` (not yet) | Phase 6 |

Every shell renders `src/controller` view-models only (`scripts/check-boundaries.sh`).

## Screens on the shared controllers

| Screen | Controller | egui | TUI | Gaps |
|---|---|---|---|---|
| Commander | `controller::commander` (`CommanderController`), each pane a `controller::browser::BrowserPaneController` (`PaneProfile::commander`) | yes | yes (Phase 2 slice 8): staged edits + Apply (F10) with preflight, review and report; unlock; image, host, backup (native / Clonezilla + partclone caches), archive and optical sources; wrapper expansion; rename (F2); new folder (Ctrl+N); export (with an "Export as" format menu, slice 10); host-copy conflict review; rename notice; remote (Ctrl+K); checksums; File Info with mode / owner / xattr prompts and a text / hex preview of either fork (`r`, slice 10) | TUI: no clipboard (the text is shown to select instead); the type/creator, ProDOS and date editors are egui-only |
| Backup tab (source, partition grid, options, run) | `controller::backup` (`BackupController`, Phase 2 slice 12; sizing rules in `model::backup_plan`) | yes: device / image (wrapped images decoded) / remote-drive sources, Load partition info, the partition grid (selection, min sizes, Calc min, fragmentation, Compact Space), options (format, CHD codecs, sector-by-sector, resize, swap, split, checksum), Start with its waits (minimum sizes before a resize-to-minimum CHD), the VHD window (whole disk / per partition, per-partition sizes), Cancel | yes: image or device source, output folder, name, format (adds gzip and LZ4), checksum, run with progress, result and the controller's last log lines | TUI: no partition selection, Compact, resize, split, CHD codecs, remote drive or VHD window (a VHD backup is one .vhd per partition); slim builds: CHD needs `chd`, the remote drive `remote` |
| Restore tab (five modes, sizing, run) | `controller::restore` (`RestoreController`, Phase 2 slice 13; sizing rules in `model::restore_plan`, layouts in `crate::restore`) | yes: Full Disk (backup folder / Clonezilla, device / image / remote target, trailing free space with extend-last, alignment, per-partition sizes with the layout-fit warning), Single Partition (target scan, partition pick), New Disk (MBR / GPT / APM, alignment, bootable), Write Image File (decoded-size probe, then the Physical Disk Export window), Build Disk (image or device target, the partition editor, typed device-name confirmation), the confirmation, Cancel | yes: backup folder, `.cbk` or Clonezilla image to an image file or device (Original / Minimum sizes, Original / 1 MB alignment), its device confirmation, run with progress and result | TUI: no single-partition, new-disk, build, remote target, trailing free space or per-partition sizes; slim builds: the remote target needs `remote`, a CHD backup `chd` |
| Physical Disk Export / Write Image File (one image to a disk) | `controller::disk_write` (`DiskWriteController`, Phase 2 slice 14; rules in `model::disk_write_plan`, the write in `model::physical_write_runner`) | yes: the window Inspect's export and Restore's Write Image File open: source probe, device target, its partitions (whole disk or one partition, with the fit warning), the synthesized MBR / GPT around a superfloppy (alignment, type byte, size), the typed device-name prompt, the write with progress, Cancel and its log | yes: the Restore screen's image source to an image file or a device (whole disk or one partition, Space / arrows cycle), after its own device prompt, with progress and result | TUI: no synthesized table (an image source always has one); `rb-cli write` keeps its own request (`--partition` selector, system-disk and mount preflight) |
| Partition window (Edit Partition Table, Build Disk's plan) | `controller::partition_edit` (`TableEditAction` / `BuildAction` over the `PartitionEditor` / `DiskBuilder` that `InspectController` / `RestoreController` own, Phase 2 slice 14b) | yes: type field with the catalog, size modes, boot flag (MBR checkbox, RDB radios), delete / undo, free-gap fill, Add, Validate, Apply; Build Disk's table type, alignment, geometry, rows (size, type, name, source image measured in a job), move / remove / add, Create Disk; the Current / After and planned layout bars | - | TUI: no table editor or Build Disk (each is an existing `InspectAction::Table` / `RestoreAction::Build`) |
| Inspect tab (source, partition grid, disk tools) | `controller::inspect` (`InspectController`, Phase 2 slice 11) | yes: device / image / backup / Clonezilla / remote sources with the unsaved-edits question, partition grid (min sizes, Calc min, labels, layout bar), Check / Repair, Make Bootable, the table editor, Add Partition, Resize, Expand Image, Defragment (Human68k), word-order swap, Expand HFS, Alto resize, floppy convert, CHD Info, Export (whole disk / per partition / HFV, CHD estimate), Physical Disk Export, remote Back Up Image, Browse | opening an image file or a backup folder reads through the controller (rows, single-file-CHD backups open as their disk); devices keep the TUI's plain read | TUI: no disk tools, export or Calc min yet; a device is not opened through the controller (its open elevates, which prompts on macOS) |
| Inspect browse view (egui Inspect tab, one partition) | `CommanderController::single` + `PaneProfile::inspect` + the screen's modes (Phase 2 slice 10 step 3) | yes: combined tree (View menu: flat list, folder tree + list), side-panel File Info with preview and metadata editors, marks across levels, staged edits + Apply with preflight / review / report, host files and drops, delete (asks for a full folder), new folder, text editor (stages in Edit Mode), export of the marks or one entry (tar / StuffIt / .sit.hqx / .mar / BinHex), extraction with the overwrite review, check / repair (Edit Mode), journal, tree listing, boot blocks, Mac-archive viewer / save / import, tar import, free space, the CHD / archive / SquashFS edit modes, CHD Info and Open as Disk Image; export format defaults by filesystem family | - (the TUI uses the Explorer) | - |
| TUI Explorer (Inspect tab, one partition) | `CommanderController::single` + `PaneProfile::explorer` (Phase 2 slice 10) | - (egui has the Inspect browse view, not yet on the controller) | yes: folder tree + listing, edits written at once (delete asks), rename, new folder, import, export (format menu), File Info preview, metadata form (type / creator / dates / mode / owner), text editor, check / repair, bless, transforms into the `:` command line, checksums, session log | marks do not span folders (they are per folder, as in Commander) |
