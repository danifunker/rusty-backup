# part-tables

Parse, describe (as serde / JSON) and write the partition tables and disk labels found on real vintage and modern disks:

PC MBR (with logical partitions), GPT, Apple Partition Map, BSD disklabel, Sun VTOC, SGI volume header and IRIS 3000 disk label (either 16-bit word order), NeXT disk label, Amiga Rigid Disk Block, Atari AHDI, Sharp X68000, Solaris x86 VTOC, and AIX / HP-UX LVM volume groups. Each is a Cargo feature.

Errors come back as `PartitionError`. Detecting which scheme a disk uses, resizing, and building whole bootable disks are left to the application. Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

MIT licensed.
