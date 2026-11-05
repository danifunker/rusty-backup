# dsk-imz

WinImage `.imz` files are a ZIP archive holding a single floppy image (`.ima` / `.img`). This crate opens them and extracts the image, ready to read as a flat disk.

Most IMZ files aren't protected. The password-protected ones use WinImage's own scheme (AES-128-CBC keyed from an MD5 of the password, in 32 KiB chunks), which is supported behind the `crypto` feature.

Builds on Rust 1.73 and later, where it uses zip 2.4 (turn on `rust173-polyfill`); current Rust uses zip 9. Part of [rusty-backup](https://github.com/danifunker/rusty-backup).

MIT licensed.
