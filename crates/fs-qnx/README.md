# fs-qnx

QNX filesystems, one Cargo feature each. All are on by default.

| Feature | Filesystem |
|---|---|
| `qnx2` | QNX 2 (Unisys ICON / LEXICON), read and write, built on the [`qnx2fs`](https://crates.io/crates/qnx2fs) crate |
| `qnx4` | QNX 4 (also QNX Neutrino 6.0–6.3 `fs-qnx4`), read-only |
| `qnx6` | QNX 6 Power-Safe (QNX Neutrino 6.4 and later), read-only |

Implements the `disk-core` filesystem traits. Builds on Rust 1.73 and later. Part of [rusty-backup](https://github.com/danifunker/rusty-backup). MIT licensed.
