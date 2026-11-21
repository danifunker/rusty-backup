# Codebase review: findings ledger

Every finding from the whole-codebase review, checked against
[rubric.md](rubric.md). The review records; fixes land later, one cluster per
change, with `scripts/regress-changed.py` run on each.

**Status values:** `open`, `fixing`, `fixed (<commit>)`, `wontfix (<why>)`.
**Verdict:** `confirmed` (reproduced, or the quoted code shows it) or
`plausible` (says what would confirm it).

IDs are `<slice>-<nnn>`. A finding seen while reviewing one slice but belonging
to another carries the other slice's prefix and is listed under that slice.

## Progress

| Slice | Lines | Reviewed | Open findings |
|---|---:|---|---:|
| crates/disk-core | 4,943 | 2026-11-21 | 12 |
| crates/fs-squashfs | 6,425 | (seen in passing) | 1 |
| src/fs | 14,945 | (seen in passing) | 1 |

## Mechanical checks

Run per slice before reading (results go in the slice's section):

```bash
./scripts/similar-fns/target/release/similar-fns --repo . --unit crates/<name>
cargo clippy --offline -p <name> --all-targets -- -D warnings
grep -rnE 'leading_(zeros|ones)|eprintln!|\.unwrap\(\)|\.expect\(|panic!|unreachable!' crates/<name>/src
grep -rnE 'vec!\[0u?8?; *[a-z_]|with_capacity\([a-z_]' crates/<name>/src   # sizes taken from disk?
```

---

## crates/disk-core

Mechanical: clippy clean, 82 tests pass, no `unwrap` / `panic!` / `eprintln!`
outside tests, no narrow `leading_zeros`. Read in full.

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| DC-001 | S2 | confirmed | [attrs.rs:326](../../crates/disk-core/src/attrs.rs#L326) | `PreservedMeta` carries no `finder_flags`, so replacing an HFS / HFS+ / MFS file (`rb-cli put` over an existing file, `src/fs/replace.rs`) drops `hasBundle`, custom-icon and invisible bits, the loss `docs/bug_binhex_finder_flags.md` describes. `CreateFileOptions.finder_flags` exists; `preserved_meta` and `apply_to_options` just don't carry it. `put.rs:430` sets it only for a MacBinary / AppleDouble import. | open |
| DC-002 | S2 | confirmed | [xattr.rs:31](../../crates/disk-core/src/xattr.rs#L31) | `value_display` / `parse_value` don't round-trip a text value that starts with `0x`: `b"0x41"` displays as `0x41` and parses back as `[0x41]` (`A`). An editor that shows a value and saves it unchanged rewrites the attribute. Text that looks like hex needs an escape, or text values need a marker. | open |
| DC-003 | S2 | confirmed | [times.rs:446](../../crates/disk-core/src/times.rs#L446) | Clamped encoders keep the real month and day, which makes invalid dates: ProDOS clamps to 2039 (2040-02-29 is written as 2039-02-29, read back as no date); UCSD clamps to 1999 (any 29 February from 2028 on); LIF clamps to 2069. `unix_to_dos_datetime` already fixed this exact bug (comment at line 239). Pin the whole stamp, as DOS does. | open |
| DC-004 | S2 | confirmed | [times.rs:678](../../crates/disk-core/src/times.rs#L678) | `unix_to_qdos_date` wraps with `as u32` instead of clamping: 2100-01-01 encodes as 91474304, which decodes to nothing (or, near the boundary, to a wrong 1960s date). The module doc promises clamping. | open |
| DC-005 | S2 | plausible | [filesystem.rs:444](../../crates/disk-core/src/filesystem.rs#L444) | The default `delete_recursive` recurses with no depth limit or visited set. A corrupt image whose directory lists an ancestor (a FAT subdirectory cluster pointing back up) recurses until the stack overflows, and deletes along the way. Confirm with a FAT image whose subdir's first cluster is its parent's. | open |
| DC-006 | S3 | confirmed | [filesystem.rs:89](../../crates/disk-core/src/filesystem.rs#L89) | The default `write_file_to` reads the whole file into one `Vec` (`read_file(entry, usize::MAX)`). 20 drivers rely on it. Most are floppy formats under the 3 MB rule, but CP/M (hard disks to 512 MB), HP LIF, QNX 2, Xerox Pilot and the SquashFS editor can hold files far larger, and `stream_pipe` / extract then buffer them whole. List: `grep` for `Filesystem for` impls without `fn write_file_to`. | open |
| DC-007 | S3 | confirmed | [times.rs:378](../../crates/disk-core/src/times.rs#L378) | Local-time formats (FAT, HFS, ProDOS, exFAT without an offset) convert with today's UTC offset, not the offset in force on that date, so a summer file read in winter (or written then read across a DST change) is an hour out. Common practice (mtools does the same), but undocumented here. Either use chrono's offset for the date in question or say so in the module doc. | open |
| DC-008 | S4 | confirmed | [entry.rs:133](../../crates/disk-core/src/entry.rs#L133) | Six full field lists of `FileEntry`'s 27 fields: `root`, `new_directory`, `new_file`, `new_symlink`, `new_special` and `unix_common::unix_entry_from_inode`. Adding a field means editing six places (the `qnx2_attrs` addition did). One private `blank(entry_type)` plus struct-update syntax removes ~170 lines. | open |
| DC-009 | S4 | confirmed | [entry.rs:430](../../crates/disk-core/src/entry.rs#L430) | `entry::select_child` and `filesystem::find_child` are the same function with the arguments swapped (8 and 44 callers). Two more copies: `crates/fs-tar/src/import_sink.rs:570` and `src/fs/copy.rs:856`. Keep `find_child`. | open |
| DC-010 | S4 | confirmed | [inode.rs:165](../../crates/disk-core/src/unix_common/inode.rs#L165) | `format_unix_timestamp` re-implements the Hinnant civil-date algorithm that `times::ymd_hms` already has, in the same crate. | open |
| DC-011 | S4 | confirmed | (crate-wide) | disk-core has no little / big-endian byte readers, so every crate grows its own: `le32` 9 times, `be32` 6, `be16` 5, `read_u32` 5, `le16` 4 (see similar-functions.md, same-name section). A `disk_core::bytes` module (`le16/le32/le64/be16/be32/be64` over `&[u8]` at an offset, plus checked variants returning `Option`) gives the crates one tested version, and a place to add the bounds checks the rubric asks for. | open |
| DC-012 | S4 | confirmed | various | Small accuracy and maintenance slips: `bits.rs:43` points at `src/fs/hfsplus.rs` (now `crates/fs-mac`); `times.rs:582` labels the LIF encoder as the OS-9 section; `id_names.rs:115` says a symlinked `/etc/passwd` is followed and then rejects it; the `id_names.rs:163` test claims "unknown user, known group" and asserts the both-known case again; `#[allow(dead_code)]` on `pub` functions in `bits.rs` does nothing; `stream_pipe.rs:110` reports the consumer's wrapped string error instead of the producer's typed `FilesystemError` when the producer caused it. | open |

Checked and fine: `stream_pipe` (no deadlock on early stop or panic; producer
errors never become a silent short read), `raw_name` round trip, `name_legalize`
(host names can't escape the folder: `/` and `..` are refused and repaired),
`bits` arithmetic, the DOS / exFAT / NTFS / Mac / ADFS / OS-9 converters.

## crates/fs-squashfs

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| SQ-001 | S3 | confirmed | [squashfs_edit.rs:1](../../crates/fs-squashfs/src/squashfs_edit.rs#L1) | The editor holds the whole decompressed image in RAM (`FileContent::Bytes`) and rebuilds it into a buffer, against the streaming rule. The module doc calls lazy streaming "the follow-up"; it has no tracking entry. Peak memory is the decompressed image size. | open |

## src/fs

| ID | Sev | Verdict | Where | Finding | Status |
|---|---|---|---|---|---|
| FS-001 | S2 | plausible | [export_selection.rs:434](../../src/fs/export_selection.rs#L434) | Zip export builds member paths from raw entry names via `archive_name()`, which rejects only an exact `.` / `..`. Classic HFS and MFS allow `/` in a name, so `a/b` becomes a nested member and `../../x` a traversal member (`zip` does not refuse `..`; the `tar` crate does, so tar export fails instead). Folder export is safe (`sanitize_filename`). Confirm by `put`-ting a file named `../x` onto an HFS image and zipping it. | open |
