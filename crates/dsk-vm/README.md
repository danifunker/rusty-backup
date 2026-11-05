# dsk-vm

Virtual-machine disk images:

| Module | Format | Support |
|---|---|---|
| `vhd` | Microsoft VHD (Virtual PC / Hyper-V), fixed and dynamic | footer and header building, CHS geometry, dynamic read and in-place write |
| `vmdk` | VMware VMDK, flat (monolithicFlat, twoGbMaxExtentFlat, embedded) | read, write |
| `vmdk_sparse` | VMware VMDK, monolithic sparse | read, write, edit in place |
| `qcow2` | QEMU QCOW2 (v2 / v3) | read, write, edit in place (allocates clusters, grows L2 and refcount tables) |

Writers skip all-zero data so outputs stay sparse. When `qemu-img` is installed, the tests also check their output with `qemu-img check`. Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

MIT licensed.
