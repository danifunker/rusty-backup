# sgi-disk

SGI / IRIX disk images, for emulators and tools:

- **build** a disk with an SGI volume header (dvh) and partition table at sector 0 and a root partition that is
  - formatted EFS (the default, IRIX 5.3 to 6.5),
  - formatted XFS (feature `xfs`, on by default; written by `fs-xfs` as v5 / CRC XFS, which IRIX itself cannot mount), or
  - left unformatted (`format_root = false`: the label-only stage);
  laid out as a data disk (one root partition, the default) or a system disk (`SgiPartitionLayout::RootSwap`: swap in slot 1 right after the volume header, the root after it). Geometry defaults to 16 heads and 63 sectors per track, the geometry the IRIS emulator's SCSI disk reports; IRIX `fx` rejects a label that disagrees with the drive. `SgiHddOptions::new_cdrom` builds an IRIX EFS CD-ROM (slot 7, SYSV).
- **fit** the disk to its target with `SgiHddOptions::fit`: `SgiFit::RoundUp` (the default) rounds `size_bytes` up to whole cylinders and grows it to the filesystem's minimum, for an image file sized to the disk; `SgiFit::WithinSize` rounds it **down** to the cylinders that fit, for a fixed target (a device, an emulator's disk), and refuses a target too small for the minimum disk, so the label never describes sectors the target lacks.
- **size** a disk to a host tree with `disk_bytes_for_root(files, dirs, content_bytes, &opts)`: the content with a per-entry allowance, plus the volume header, the swap and the root filesystem's overhead (EFS inode tables and bitmap; the XFS log, AG headers and its 32 MiB floor).
- **write a header** from explicit partitions with `build_header(&SgiHeaderSpec)` (any slots, types and geometry; checked to fit, never truncated). `build_volume_header` is that for a layout from `compute_sgi_layout`.
- **open** a partition of an existing disk: `read_volume_header`, `filesystem_partitions`, then `open_partition` (read-only) or `open_partition_editable`, over any `Read + Seek (+ Write) + Send` reader (a file, a block device, a buffer). EFS (type 7, and SYSV on CDs) or XFS (type 10). `open_kind` / `open_kind_editable` take the partition's offset and length. Every opener refuses a filesystem that declares more than its partition holds (EFS `fs_size` and its replica superblock, XFS `dblocks`), read-only too, so a corrupt label or a label-only relabel over an older, larger volume is never read or written past the partition end.

```rust
use sgi_disk::{build_sgi_disk, open_partition_editable, SgiHddOptions, SgiRootFs};

let mut opts = SgiHddOptions::new(64 * 1024 * 1024, "irix");
opts.root_fs = SgiRootFs::Xfs;
let (image, layout) = build_sgi_disk(&opts)?;
let mut fs = open_partition_editable(std::io::Cursor::new(image), layout.root_slot)?;
```

Errors are `anyhow::Error`. Builds on Rust 1.73 and later (`rust173-polyfill` for a 1.73 toolchain). Part of [rusty-backup](https://github.com/danifunker/rusty-backup). Licensed MIT; every dependency, `fs-xfs` included (MIT since 0.2.0), is MIT.
