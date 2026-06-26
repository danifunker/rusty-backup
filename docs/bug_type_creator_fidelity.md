# Bug: Mac type/creator codes are corrupted on copy — lossy text round-trip

**Severity:** high (data-fidelity / Mac round-trip correctness)
**Component:** `FileEntry` type/creator model → `get-binhex`, `cp`, archives, remote copy
**Status:** fixed (2026-06-26)

## Resolution

`FileEntry.type_code` / `creator_code` are now the **raw 4-byte Mac `OSType`**
(`Option<[u8; 4]>`) instead of a lossy display `String`. The display string is
derived only at the boundary, per filesystem, via `FileEntry::type_code_display()`
/ `creator_code_display()` (and the shared `entry::display_file_type`), which
renders a Mac OSType lossily (`hfs_common::decode_ostype`, non-ASCII → `.`) and a
ProDOS type (now carried in the new `prodos_file_type: Option<u8>`) as `$XX`.

Every **write / round-trip** path reads the raw bytes:

- `get-binhex` encodes `entry.type_code` / `creator_code` directly.
- `CreateFileOptions` gained `os_type` / `os_creator: Option<[u8; 4]>`; HFS / HFS+
  / MFS `create_file` write these verbatim into the Finder info (preferred over
  the lossy text `type_code`). `cp` (`fs/copy.rs`) and the edit queue
  (`model/edit_queue.rs`) pass the source's raw bytes through them.
- The remote browse DTO (`WireEntry`) and the rb:// `CopyLocal` staged edit carry
  `[u8; 4]`; a host `StageUpload`'s user-typed ASCII `--type` is encoded to bytes
  when staged.

Covered by the existing round-trip tests (HFS/HFS+/MFS surface `*b"TEXT"` etc.;
`cp` preserves type/creator; ProDOS surfaces `$XX` via `type_code_display`).

## Summary

Copying a classic-Mac file with rb-cli **corrupted its type/creator code** when
either contained a non-ASCII byte. A `get-binhex` → `put-binhex` round-trip (and
`cp`, which shared the model) wrote the file back with the offending byte replaced
by `.` (`0x2E`).

The case that bit in practice: **Prince of Persia**'s creator is `50 6f C4 50`
(`PoƒP`, where `0xC4` is the MacRoman florin `ƒ`). Harvested copies came out
`50 6f 2E 50` (`Po.P`). The forks read back byte-identical — only the catalog
type/creator was wrong.

Because the Finder matches an app's `BNDL` (and its documents) by **creator code**,
and Prince of Persia locates its colour graphics file `Persia(COLOR)` (type
`PPgf`, creator `PoƒP`) by type+creator, the corrupted creator produced **generic
icons** *and* made PoP fall back to its **black-&-white** build instead of 256
colour.

## Root cause

`FileEntry.type_code` / `creator_code` were `Option<String>`, populated by the
HFS/HFS+/MFS catalog readers through a **display-oriented, lossy** decode that maps
any non-printable / non-ASCII byte to `.`:

`src/fs/hfs.rs` (old):
```rust
fn decode_fourcc(data: &[u8]) -> String {
    data.iter().map(|&b| if b.is_ascii_graphic() || b == b' ' { b as char } else { '.' }).collect()
}
...
let creator_code = decode_fourcc(&rec[8..12]); // 50 6f C4 50 -> "Po.P"
fe.creator_code = Some(creator_code);
```

The string was the **only** representation `FileEntry` carried, so every write path
had to reconstruct bytes *from the lossy string*:

`src/cli/verbs/binhex.rs` (encode, old):
```rust
let creator_code = code_to_bytes(entry.creator_code.as_deref().unwrap_or("????"));
// code_to_bytes("Po.P") = 50 6f 2E 50  -> the florin is gone
```

`cp` (`fs/copy.rs`) did the same via `encode_fourcc(src.creator_code)`. The bytes
died at the display boundary and never came back.

`decode_fourcc` itself is correct *for display* (a terminal column / GUI label
can't show a raw `0xC4`); it was simply being used as a round-trip codec.

## The model fix (why bytes, not a parallel field)

`type_code` / `creator_code` are an `OSType` — **4 raw bytes**, not text. The
filesystem-agnostic display field that `ls` / GUI / remote print is a *rendering*
of those bytes (and of ProDOS's 1-byte type, and CBM's `SEQ`). So the canonical
data is the bytes; the string is derived. The fix makes the stored field the
canonical bytes and moves the (per-filesystem, lossy) rendering to a display
helper. ProDOS, which has no OSType, moved its 1-byte type to `prodos_file_type`;
its `$XX` rendering is produced by the same `type_code_display()` helper.

## Reproduce (pre-fix)

```sh
rb-cli get-binhex src.hda@1 "/Games/Prince of Persia ƒ/Prince of Persia" pop.hqx
rb-cli put-binhex out.hda pop.hqx --dst-dir /Apps --force
# Read the catalog file record: creator is 50 6f 2E 50 ("Po.P"), not 50 6f C4 50.
```

A *Finder*-written copy of the identical files keeps `50 6f C4 50` and launches in
colour — proving the difference was the write path, not the fork bytes.

## Downstream impact (why this was found)

MacAtrium harvests game apps with `get-binhex` → `put-binhex` (and `cp`). Harvested
Prince of Persia ran in black-&-white with generic icons; copying the same files
through the real Finder produced colour. The on-disk catalog file record differed
only in the creator's florin byte (`0xC4` → `0x2E`) across *all* PoP files,
including the `Persia(COLOR)` data file PoP looks up by creator. See the sibling
fidelity bug `bug_binhex_finder_flags.md` (the dropped `hasBundle` flag) — both are
Mac round-trip metadata that `FileEntry` must surface byte-exact.
