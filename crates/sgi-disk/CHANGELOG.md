# Changelog

All notable changes to this crate are documented here. This project follows
[Semantic Versioning](https://semver.org/).

## 0.1.0

First release. Moved out of rusty-backup's `src/partition/sgi_hdd_builder.rs`
(EFS hard disks and EFS CD-ROMs), with additions:
- `SgiRootFs::Xfs` (feature `xfs`): the root partition typed XFS and formatted
  by `fs_xfs::xfs::format::write_blank_xfs`.
- `format_root = false`: the volume header and partition table only.
- `SgiPartitionLayout::RootSwap { swap_bytes }`: an IRIX system-disk layout.
- `SgiHddLayout`'s `efs_*` fields are `root_*`, plus `root_slot` and `swap_*`.
- The `open` module: find and open EFS / XFS partitions over any reader. Every
  opener takes the partition length (`open_kind` / `open_kind_editable` take it
  as an argument) and refuses a filesystem larger than its partition.
- `SgiHddOptions::fit` / `SgiFit`: round to cylinders up (default) or within
  the requested size, for a device or a fixed emulator disk.
- `disk_bytes_for_root`: the disk size a host tree needs, swap and filesystem
  overhead included.
- `build_header` / `SgiHeaderSpec` / `SgiSlot`: a volume header from explicit
  partitions. `build_volume_header` returns `Result` and checks the layout.
- An XFS root in a build without feature `xfs` is refused by
  `compute_sgi_layout`, before anything is written.
- The app's old names `write_sgi_efs_hdd` / `build_sgi_efs_hdd` stay in
  rusty-backup; this crate has `write_sgi_disk` / `build_sgi_disk` only.
