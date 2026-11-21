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
- The `open` module: find and open EFS / XFS partitions over any reader.
