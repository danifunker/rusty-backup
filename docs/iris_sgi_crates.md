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
| Copy files out | `rb-fsexporter` | 0.2.0 | | `extract(&mut dyn Filesystem, &FileEntry, dest: &Path, &Options, &mut dyn Host) -> Result<Summary>`; `Options::new(ResourceForkMode)`; `Host::cancelled` / `wrote` for progress |
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

32 crates have versions that are not on crates.io yet, and pushing to `main` with them
publishes them all (`.github/workflows/publish-crates.yml`, which orders them by dependency).
Iris needs nine of them: disk-core, part-tables, fs-unix, fs-xfs, sgi-disk, rb-fsimporter,
rb-fsexporter, fs-tar and fs-mac. Twenty others were moved to disk-core 0.3 earlier on this
branch without a version bump, so they get one now: 0.x.y to 0.(x+1).0, because disk-core's
types are part of their API. fs-mac, fs-pc and rb-fsexporter go to 0.2.0 for the same reason.

By hand, in dependency order:

disk-core 0.3.0, dsk-apple 0.2.0, dsk-clonezilla 0.2.0, dsk-floppy 0.2.0, dsk-flux 0.2.0,
dsk-imz 0.2.0, dsk-vm 0.2.0, fs-8bit 0.2.0, fs-adfs 0.2.0, fs-amiga 0.2.0, fs-apfs 0.2.0,
fs-apple2 0.2.0, fs-cpm 0.2.0, fs-hpfs 0.2.0, fs-mac-unicode 0.2.0, fs-mac 0.2.0,
arc-mac 0.3.0, dsk-dmg 0.2.0, fs-pc 0.2.0, dsk-ghost 0.2.0, fs-qnx 0.2.0, fs-squashfs 0.2.0,
fs-unix 0.3.0, fs-tar 0.2.0, dsk-tape 0.2.0, fs-xerox 0.2.0, fs-xfs 0.2.0, part-tables 0.2.0,
qnx2fs 0.4.0, rb-fsexporter 0.2.0, rb-fsimporter 0.1.0, sgi-disk 0.1.0.

`scripts/unpublished-crates.py` lists them; `cargo publish --dry-run -p A -p B ...` with that
list checks the whole set first, and `cargo publish -p A -p B ...` publishes it in order.
