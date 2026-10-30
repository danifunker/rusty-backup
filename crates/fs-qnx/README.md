# fs-qnx

Read QNX filesystems, one Cargo feature each.

| Feature | Filesystem |
|---|---|
| `qnx4` | QNX 4 (also QNX Neutrino 6.0–6.3 `fs-qnx4`) |
| `qnx6` | QNX 6 Power-Safe (QNX Neutrino 6.4 and later) |

For QNX 2 (Unisys ICON / LEXICON), see the [`qnx2fs`](https://crates.io/crates/qnx2fs) crate.

Implements the `disk-core` filesystem traits. Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup). MIT licensed.
