# SGI crates for the IRIS emulator

What the IRIS emulator (`../iris`) needs from rusty-backup's crates to build, open, check and
copy files into and out of SGI disks. Every crate below is MIT, and so is every crate in
its dependency tree (checked with `cargo tree --format '{p}|{l}'`: MIT, Apache-2.0,
BSD-3-Clause, Zlib, bzip2-1.0.6, Unicode-3.0; no GPL or LGPL).

| Need | Crate | Version | Features | Entry point |
|---|---|---|---|---|
| Parse the volume header | `sgi-disk` | 0.1.0 | default (`xfs`) | `read_volume_header<R: Read + Seek>(&mut R) -> Result<SgiVolumeHeader>`; `filesystem_partitions(&SgiVolumeHeader) -> Vec<SgiPartition>`; `partition(&SgiVolumeHeader, index) -> Result<SgiPartition>` |
| Build an EFS disk | `sgi-disk` | 0.1.0 | default | `write_sgi_disk<W: Write + Seek>(&mut W, &SgiHddOptions) -> Result<SgiHddLayout>` (streams); `build_sgi_disk(&SgiHddOptions) -> Result<(Vec<u8>, SgiHddLayout)>` (in memory). `SgiHddOptions::new(size_bytes, name)`, `root_fs = SgiRootFs::Efs` |
| Build an XFS disk | `sgi-disk` | 0.1.0 | `xfs` | as above with `root_fs = SgiRootFs::Xfs` |
| Label only (no format) | `sgi-disk` | 0.1.0 | none needed | `format_root = false` |
| Root + swap | `sgi-disk` | 0.1.0 | none needed | `layout = SgiPartitionLayout::RootSwap { swap_bytes }` (`DEFAULT_SWAP_BYTES`) |
| Geometry | `sgi-disk` | 0.1.0 | | `heads`, `sectors_per_track` (defaults `DEFAULT_HEADS` = 16, `DEFAULT_SECTORS_PER_TRACK` = 63); `fit: SgiFit::{RoundUp, WithinSize}` |
| Explicit header | `sgi-disk` | 0.1.0 | | `build_header(&SgiHeaderSpec) -> Result<[u8; 512]>` |
| Size a disk for a tree | `sgi-disk` | 0.1.0 | | `disk_bytes_for_root(files, dirs, content_bytes, &SgiHddOptions) -> u64` |
| Open read-only | `sgi-disk` | 0.1.0 | `xfs` for XFS | `open_partition<'a, R: Read + Seek + Send + 'a>(R, index) -> Result<Box<dyn Filesystem + 'a>>`; `open_kind(kind, R, offset, len)` |
| Open for editing | `sgi-disk` | 0.1.0 | `xfs` for XFS | `open_partition_editable<'a, R: Read + Write + Seek + Send + 'a>(R, index) -> Result<Box<dyn EditableFilesystem + 'a>>`; `open_kind_editable(kind, R, offset, len)` |
| Copy a host folder in | `rb-fsimporter` | 0.1.0 | default (`native-zstd`) or `pure-zstd` | `import_dir_with(&mut dyn EditableFilesystem, dest: &FileEntry, root: &Path, &DirImportOptions, expander: Option<&dyn Expander>, host: &dyn Host) -> Result<ImportStats>`; `Host { cancelled(&self) -> bool, progress(&self, &ImportStats) }`; cancel returns `Cancelled`. Simple form: `import_dir(.., progress: &dyn Fn(&ImportStats))` |
| Preflight / measure | `rb-fsimporter` | 0.1.0 | | `preflight_dir_with`, `measure_dir_for(root, &DirImportOptions) -> Result<(files, dirs, bytes)>` |
| Copy files out | `rb-fsexporter` | 0.1.4 | | `extract(&mut dyn Filesystem, &FileEntry, dest: &Path, &Options, &mut dyn Host) -> Result<Summary>`; `Options::new(ResourceForkMode)`; `Host::cancelled` / `wrote` for progress |
| Export a tar | `fs-tar` | 0.2.0 | | `export_tar<W: Write>(&mut dyn Filesystem, root: &FileEntry, prefix, W, TarCompression, &TarExportOptions, progress) -> Result<TarExportStats>` |
| Check | `disk-core` 0.3.0 trait, via `sgi-disk` | | | `Filesystem::fsck(&mut self) -> Option<Result<FsckResult, FilesystemError>>` (EFS and XFS return `Some`) |

Lower layers, if iris wants them directly: `part-tables` 0.2.0 (feature `sgi`), `fs-unix`
0.3.0 (feature `efs`), `fs-xfs` 0.2.0, `disk-core` 0.3.0 (feature `fs`).

## Not verified

- No disk built here has been mounted by a real IRIX (or by the emulator).
- **XFS root partitions are written as v5 (CRC) XFS, which IRIX cannot mount.** IRIX mounts
  pre-CRC (v4 superblock and older) XFS only, and `fs_xfs::write_blank_xfs` has no v4 mode.
  EFS is the root filesystem to use for IRIX today; the XFS variant suits Linux tools.
- The XFS directory hash was rewritten from SGI's "XFS Algorithms & Data Structures" and
  checked against 2,800+ hashes in directories made by Linux `mkfs.xfs` (v4 and v5), not
  against a disk IRIX made.
- The root + swap layout was not checked with IRIX `fx` / `prtvtoc`.
- GTK4 and Win32 shells (the "Format" control) were not compiled locally.

## Publishing

None of these are on crates.io yet. Pushing to `main` with these `Cargo.toml` versions
publishes them all (`.github/workflows/publish-crates.yml`). To publish by hand, in this
dependency order:

1. `disk-core` 0.3.0
2. `fs-mac` 0.1.11
3. `fs-pc` 0.1.7
4. `part-tables` 0.2.0
5. `fs-xfs` 0.2.0
6. `fs-unix` 0.3.0
7. `fs-tar` 0.2.0
8. `rb-fsexporter` 0.1.4
9. `rb-fsimporter` 0.1.0
10. `sgi-disk` 0.1.0
