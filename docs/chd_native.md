# Native CHD support

Rusty Backup compresses, decompresses, and inspects CHD files directly —
no external `chdman` binary is required, and there is no `chdman_path`
setting to configure. MAME's CHD core (the same C++ implementation that
powers `chdman`) is statically linked into the rusty-backup binary via
[libchdman-rs](https://github.com/danifunker/libchdman-rs).

## What this means in practice

- **Backup → CHD** writes a `<backup-name>.chd` directly with the codecs
  and hunk size chosen in the GUI. Output is a real disk image (partition
  table at sector 0, partitions at their declared offsets, gaps zero-
  filled) that `chdman info`, MAME, and any other CHD-aware tool will
  open.
- **Restore from CHD** streams the CHD's logical bytes onto the target
  device or image file. No subprocess, no temp file.
- **Optical → CHD** (`Optical` tab) handles ISO and BIN/CUE inputs
  natively, including multi-track CDs.
- **Edit Mode** on a compressed CHD uses the diff-against-parent flow
  (`src/rbformats/chd_edit.rs`) to write changes into a child CHD, then
  flattens the result so the saved file remains a single, parent-less
  CHD any tool can read.
- **Browse / Inspect** of `.chd` files goes through `ChdReader`, a
  thin `Read + Seek` wrapper over libchdman-rs that streams hunks on
  demand with a single-hunk cache.

## Codec support

The HD/DVD/CD profile defaults match `chdman`'s defaults. Supported codec
names: `zlib`, `zstd`, `lzma`, `huff`, `flac`, plus the optical `cdlz`,
`cdzl`, `cdfl` variants. Custom codec spec strings (e.g.
`"lzma,zlib,huff,flac"`) and hunk sizes are persisted across launches in
`config.json` (`last_chd_codecs` / `last_chd_hunk_size`); see
`docs/CONFIGURATION.md`.

## Build implications

The first build compiles MAME's CHD core (~2 minutes on a modern laptop).
Incremental builds are fast (~8 seconds). Cross-compiling: libchdman-rs
builds C++ via the `cc` crate, so a working host C++ toolchain is
required for AppImage / macOS / Windows release builds — the GitHub
Actions release workflow already handles this.

## Why this changed

Earlier versions of rusty-backup shelled out to a user-installed
`chdman` for compress/extract operations. The native swap (planned in
`docs/chdman_replacement.md`) removed that dependency, simplified the
install story (no PATH tweaking, no per-platform binary fetch), and
opened the door to features that were awkward across a subprocess
boundary — most notably the single-file CHD backup layout
(`docs/whole_disk_chd_backup.md`) and the parent/child diff edit flow.
