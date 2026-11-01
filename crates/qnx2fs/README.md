# qnx2fs

Read and write **QNX 2** filesystems: QNX 2.x volumes, and the diskettes and
hard-disk partitions of the Unisys **ICON** school computer and its **LEXICON**
fileserver.

- List directories, read files, and check a volume (broken chains, cross-links,
  the free-block bitmap against what files hold, recoverable deleted files).
- Create files and directories, delete, rename, change permissions and owners.
- Copy files and folder trees between volumes (`copy_from`), and replace files
  (`replace`), keeping permissions, owners and dates.
  Writes follow what QNX 4.10.01 itself writes, and were checked with its
  `chkfsys` on the [icon-emu](https://github.com/danifunker/icon-emu) emulator.
- Convert text: QNX 2 ends lines with RS (`0x1E`) instead of LF.

It has no dependencies and works on any `Read + Seek` (and `Write` for edits)
device, including in WebAssembly.

```rust
use qnx2fs::{lf_to_rs, NewEntry, Volume};

let image = std::fs::read("diskette.img")?;
let mut vol = Volume::open(std::io::Cursor::new(image), 0)?;
for e in vol.list("/")? {
    println!("{:16} {:>8} {}", e.name, e.size, e.describe());
}
vol.mkdir("/docs", &NewEntry::default())?;
vol.put("/docs/notes", &lf_to_rs(b"hello\n"), &NewEntry::default())?;
std::fs::write("diskette.img", vol.into_inner().into_inner())?;
```

A volume inside a partitioned hard disk is opened at its byte offset. On ICON
hard disks, partition types 07, 08 and 09 hold QNX 2, and the partition's start
field is a 1-based block number: the volume starts at byte
`(start - 1) * 512`.

## Dates

A QNX 2 date is two 16-bit words. The first holds the year minus 1980 in 4
bits, plus the month, the day and AM/PM; the second holds the seconds since 12
o'clock. It is the local time of the machine that wrote it, with no time zone.
So **only the years 1980 to 1995 can be stored**.

Dates already on a disk are no problem:
- **Reading** gives the exact date (`Entry::modified`, `decode_date`). Treat it
  as local wall-clock time; to turn it into a host timestamp, interpret it in
  the host's time zone.
- **Copying** between QNX 2 volumes (`copy_from`, `NewEntry::like`) copies the
  stored date words exactly.

Modern dates can't be stored, so the crate **folds** them with `fold_date`
before writing. This applies to "now" (`Volume::set_clock`, or the system
clock) and to `NewEntry::mtime`:

- **The same month, day and time of day, in 1994.**
- **29 February goes to 1992**, the last leap year the ICON can store.
- Dates already in 1980–1995 are kept as they are.

| Date | Written as |
|---|---|
| 2026-03-14 10:00 | 1994-03-14 10:00 |
| 2024-02-29 09:00 | 1992-02-29 09:00 |
| 2024-03-01 08:00 | 1994-03-01 08:00 |
| 1988-03-23 16:05 | 1988-03-23 16:05 (unchanged) |

Why 1994:
- **It sorts after the real ICON software.** The last ICON system software is
  from 1991, so new and imported files look newer than anything original.
- **It survives New Year's Eve.** A clock left running past midnight on 31
  December moves into 1995, which can still be stored. (With 1995, it would
  roll into 1996, which wraps round to 1980.)
- **It is the rule icon-emu's emulated ICON clock uses**, so files written by
  this crate match the files QNX itself writes in the emulator.

The year itself is lost, so it is worth checking what an ICON thinks the date
is: its login screen shows it at start-up.

`encode_date` on its own still clamps to 1980-01-01 … 1995-12-31 23:59:59; call
`encode_date(fold_date(t))` to get the folded date.

On `wasm32-unknown-unknown` there is no system clock: call `Volume::set_clock`,
or new dates come out as 1 January 1994.

Used by [icon-emu](https://github.com/danifunker/icon-emu) and
[RustyBackup](https://github.com/danifunker/rusty-backup).

## The `disk-core` feature

Off by default. It adds `qnx2fs::adapter`, which puts a `Volume` behind the
`Filesystem` / `EditableFilesystem` traits of the
[disk-core](https://crates.io/crates/disk-core) crate, as RustyBackup uses it.
It pulls in `disk-core`, `chrono` and `log`, and reads the host clock and time zone,
so it is not for the browser build.

## License

MIT. See [LICENSE](LICENSE).

The format notes build on Mitchell Schoenbrun's public-domain `q2files.c` and
`q2read.c`.
