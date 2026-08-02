# Text encoding: two separate jobs, both outstanding

Parked 2026-08-02. Nothing here is started beyond one committed fix (`d3953cd`).
Everything below is measured on the tree at that commit unless marked otherwise.

These get conflated because both are "encoding", but the fix for one would damage
the other. Keep them apart.

---

## Job 1 — our own UI and log strings should be ASCII

**Why:** a UTF-8 em dash is three bytes that a Windows 7 console in code page 437
renders as three garbage characters, shifting the rest of the line. Same for a
serial console or a vintage terminal. CLAUDE.md's no-Unicode rule already covers
"log lines, button labels, dialog text, and any other user-visible string".

### Census (non-comment lines containing a string literal, all of `src/`)

| char | U+ | count | files | replace with |
|---|---|---|---|---|
| — | 2014 EM DASH | 274 | 84 | ` - ` |
| … | 2026 ELLIPSIS | 23 | 9 | `...` |
| × | 00D7 MULTIPLICATION | 11 | 4 | `x` |
| → | 2192 ARROW | 7 | 7 | `->` |
| ≥ | 2265 | 7 | 4 | `>=` |
| ≤ | 2264 | 3 | 3 | `<=` |
| • | 2022 BULLET | 2 | 1 | `-` |
| ⇒ | 21D2 | 1 | 1 | `=>` |

~325 replacements. 34 distinct non-ASCII characters appear overall.

### Do NOT sweep these

- **`é` (9), `Ä` (4), `ü` (2)** and the **CJK / katakana** (日本, アクセサリ, サ, セ, リ)
  are test fixtures — MacRoman round-trips and Shift-JIS / X68000 data. Leave them.
- Comments and doc comments never render. Skipping them keeps the diff to what
  actually matters.

### Not a landmine after all

`MAC_ROMAN_TABLE` in `src/fs/hfs.rs:21` is written entirely with `\u{....}`
escapes, so a literal-character sweep cannot corrupt it. That was the main reason
for hesitating; it does not apply. Still worth re-checking any *other* charset
table added later before running a sweep.

---

## Job 2 — each filesystem must decode names in its own charset

**Why:** `String::from_utf8_lossy` turns every byte >= 0x80 into `U+FFFD`. On a
filesystem whose native charset is not UTF-8, that silently destroys any accented
or non-Latin filename. There are **114** uses of `from_utf8_lossy` under `src/fs/`.

### Already correct

- **HFS** — `mac_roman_to_utf8` / `utf8_to_mac_roman` / `decode_mac_filename`
  (`src/fs/hfs.rs`), which tries UTF-8 first and falls back to Mac Roman.

### Unchecked — verify before changing

- **HFS+** should be UTF-16BE, Apple-decomposed (its own NFD variant, not stock NFD).
- **NTFS** should be UTF-16LE.
- **FAT** short names are an OEM code page (437/850/...); long names are UTF-16LE.
- **APFS** is UTF-8 and normalization-insensitive, so `from_utf8_lossy` is probably fine.

### Wrong, in rough priority order

| filesystem | should be | today |
|---|---|---|
| **AFFS / PFS3 / SFS** (Amiga) | ISO-8859-1 | `from_utf8_lossy` |
| **HPFS** (OS/2) | OEM code page (850/437) | `from_utf8_lossy` |
| **CP/M** | 7-bit ASCII — the high bit of each name byte is an *attribute* flag | `from_utf8_lossy` over unmasked bytes |
| Atari DOS, RS-DOS, DragonDOS, Oric, DFS, QDOS/MDV, TI-99 | 8-bit micro charsets | `from_utf8_lossy` |
| ext / UFS / XFS / btrfs / reiserfs / minix / squashfs | raw bytes, conventionally UTF-8 | `from_utf8_lossy` — defensible, matches Linux tools, lowest priority |

Amiga first: it is first-class per CLAUDE.md, Latin-1 is unambiguous, and the
three drivers already share conventions.

### Shape of the work

There is no central charset module — Mac Roman lives inside `src/fs/hfs.rs` and
several drivers carry their own private `decode_name`. A shared
`src/fs/charset.rs` (Latin-1, CP437/850, Mac Roman moved out of `hfs.rs`, plus the
micro tables) would stop each new driver reinventing it. Round-trip tests per
charset are the thing that makes it verifiable.

---

## Also parked (different thread)

The Windows 7 TUI rendering fault is **not reproducible yet**: no CI artifact
starts on a stock Win7. `rb-cli-win7-x86-2026-08-01-11-52` still imports
`combase.dll` (one symbol, `CoTaskMemFree`, from `dirs = "6"` calling
`SHGetKnownFolderPath`; on Win7 that symbol lives in `ole32.dll`). That artifact
was built 11:52 UTC and the fix `e2d2fb5` landed 13:54 UTC, so it predates it.
Needs a CI build from after 2026-08-01 13:54 UTC. The QEMU rig is ready and
drives the VM end to end.
