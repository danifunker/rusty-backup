# Bug: Finder flags (fdFlags) are dropped on copy — `get-binhex` zeroes them

**Severity:** medium (data-fidelity / Mac round-trip correctness)
**Component:** `FileEntry` read path → `get-binhex` (and `cp`)
**Status:** fixed (2026-06-25)

## Resolution

`FileEntry` now carries `finder_flags: Option<u16>` (`src/fs/entry.rs`),
populated by the HFS / HFS+ / MFS catalog readers (`FInfo.fdFlags`, the 2-byte
field at offset 8 of the 16-byte Finder info) and `None` everywhere else.
`get-binhex` encodes `entry.finder_flags.unwrap_or(0)` instead of a hard `0`, and
the copy engine (`src/fs/copy.rs`, used by `cp` and the browse-view copy) stamps
the source flags onto the destination via `set_finder_info` after `create_file`
— the same path `put-macbinary` uses. Covered by unit tests in `hfs.rs`
(read surfaces `0x2000`) and `copy.rs` (disk-to-disk `cp` preserves `0x2000`).

## Summary

Copying a classic-Mac file with rb-cli **loses its Finder flags** (`fdFlags`:
`hasBundle`, `hasCustomIcon`, `isInvisible`, `nameLocked`, `isStationery`, …). A
`get-binhex` → `put-binhex` round-trip writes the file back with `fdFlags = 0`,
and `cp` has the same root cause. The forks, type, and creator survive intact —
only the flags are zeroed.

The flag that bites in practice is **`hasBundle` (0x2000)**: without it the Finder
never reads the app's `BNDL`, so the application **and every document that shares
its creator code show the generic icon** instead of their real ones.

## Root cause

`FileEntry` (the struct every read returns) **has no Finder-flags field**:

`src/fs/entry.rs`:
```rust
pub struct FileEntry {
    pub name: String,
    pub path: String,
    pub entry_type: EntryType,
    pub size: u64,
    pub location: u64,
    pub modified: Option<String>,
    pub type_code: Option<String>,     // <- type/creator are surfaced…
    pub creator_code: Option<String>,
    ...                                 // …but fdFlags is NOT
}
```

So the `get-binhex` **encode** path has nothing to read, and hard-codes zero:

`src/cli/verbs/binhex.rs` (encode):
```rust
let file = BinHexFile {
    name: entry.name.clone(),
    type_code,
    creator_code,
    // FileEntry doesn't surface Finder flags today; default to 0.
    flags: 0,                          // <-- the bug
    data_fork,
    resource_fork,
};
```

`cp` (`src/cli/verbs/cp.rs`) copies via the same `FileEntry`, so it loses the
flags the same way.

### Why `put-macbinary` is *not* affected (the model to follow)

`put-macbinary` reads `fdFlags` straight from the **MacBinary header** (bytes 73 +
101), not from a `FileEntry`, and writes them through `set_finder_info`:

`src/cli/verbs/put_macbinary.rs`:
```rust
let finder_flags_hi = bytes[73] as u16;
let finder_flags_lo = bytes[101] as u16;
let finder_flags = (finder_flags_hi << 8) | if version >= 129 { finder_flags_lo } else { 0 };
...
BigEndian::write_u16(&mut finfo[8..10], hdr.finder_flags);   // -> set_finder_info
```

So the **write side already preserves flags** (`set_finder_info` carries
`finfo[8..10]`). Only the **read side** (`FileEntry`) is missing them.

## Reproduce

```sh
# An app with hasBundle set (e.g. Prince of Persia, creator Po.P)
rb-cli get-binhex src.hda@1 "/Games/Prince of Persia ƒ/Prince of Persia" pop.hqx
# The .hqx header reports flags=0x0000 even though the source app has 0x2000.

# Round-trip into a fresh image and the installed file has fdFlags=0 -> generic icon.
rb-cli put-binhex out.hda pop.hqx --dst-dir /Apps
```

Contrast — `put-macbinary` keeps them (`fdFlags=0x2000` is logged):
```
put-macbinary: MacAtrium (... fdFlags=0x2000, MacBinary v1)
```

## Fix

1. **Surface the flags on read.** Add to `FileEntry` (`src/fs/entry.rs`):
   ```rust
   /// HFS/HFS+ Finder flags (fdFlags): hasBundle, hasCustomIcon, isInvisible, …
   /// None on non-Mac filesystems.
   pub finder_flags: Option<u16>,
   ```
   Populate it in the **HFS/HFS+ catalog reader** that builds `FileEntry` — the
   file's `FInfo.fdFlags` is the 2-byte field at offset 8 of the 16-byte Finder
   info (the same `finfo[8..10]` `put-macbinary` writes). Default `None` everywhere
   else (a `..Default::default()` or one-line addition per constructor).

2. **Use it in `get-binhex` encode** (`src/cli/verbs/binhex.rs`):
   ```rust
   flags: entry.finder_flags.unwrap_or(0),
   ```

3. **(Same root cause) `cp`:** carry `src_entry.finder_flags` into the destination
   Finder info on write (via `set_finder_info`, like `put-macbinary` already does).

### BinHex masking note

The BinHex 4.0 spec clears the *volatile* low-byte bits (busy/changed/inited) but
**preserves the high byte** — which is where `hasBundle (0x2000)` and
`hasCustomIcon (0x0400)` live. So `flags & 0xFF00` (or preserving the high byte) is
the correct masking if any is applied; do **not** zero the whole word.

## Test plan

- Unit: read a catalog entry with `fdFlags=0x2000`, assert `FileEntry.finder_flags
  == Some(0x2000)`; `get-binhex` produces a `.hqx` whose header flags = `0x2000`.
- Round-trip: `get-binhex` then `put-binhex`, then re-read the destination's
  `fdFlags` — should equal the source (high byte at least).
- `cp` an app folder disk-to-disk, verify `fdFlags` preserved.

## Downstream impact (why this was found)

MacAtrium harvests game apps with `get-binhex` → `put-binhex`. Harvested apps lose
`hasBundle`, so their `BNDL` never registers and the app **plus all its
creator-matched data files** (e.g. Prince of Persia's `Persia(BW/COLOR/LC)`, all
creator `Po.P`) render as generic icons — even though the `BNDL`/`FREF`/`ICN#`
resources copied across intact. Once `FileEntry` surfaces `finder_flags`, the
harvest round-trip (and a future switch to `cp`) preserve the icons.
