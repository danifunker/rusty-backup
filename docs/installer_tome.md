# Apple Installer Tome support (InstaCompOne)

Everything needed to read Apple Installer Tomes: the file format, both compression codecs, the Rust module that implements them (`crates/arc-mac/src/tome.rs`), how it was verified, and what is still open. As far as we know there is no other public spec or finished extractor for this format (XADMaster issue #71 is open; TomeViewerX shows metadata only; Pacifist is commercial).

## What a Tome is

A Tome (file type `idcp`, creator `kakc`) is the archive Apple's Installer reads from, used on System 6 through Mac OS 9 install disks and CDs and by many third-party installers built with Apple's Installer SDK. It holds a flat list of Mac files, each with a data fork, a resource fork and Finder info. The Installer script (a separate file) decides which files go where; the Tome is only the payload.

Each fork is a stream of 64 KiB blocks. A block is stored raw or compressed with one of two codecs:

- **InstaCompOne**: LZ77 with static prefix codes and 8-bit literals. Used for most resource forks and binaries.
- **Codec 0** (text): the same token grammar with 7-bit literals, its own copy-length codes and its own distance levels. Used for PPDs, modem scripts and other text, plus some localized resource files.

## Container

All integers are big-endian.

| Offset | Size | Field |
|---|---|---|
| 0x00 | 4 | magic `6b 63 00 01` (`kc\0\x01`) |
| 0x1a | 2 | record count |
| 0x24 | 0x80 × count | records |

Other header bytes vary and are not needed.

Record (0x80 bytes):

| Offset | Size | Field |
|---|---|---|
| +0x06 | 32 | name, Str31 (Mac Roman); the bytes after the string are often uninitialized memory |
| +0x26 | 4 | file type |
| +0x2a | 4 | creator |
| +0x2e | 4 | creation date (seconds since 1904) |
| +0x32 | 4 | modification date |
| +0x36 | 2 | Finder flags |
| +0x3c | 16 | data fork: u32 length, u32 offset of its stream in the Tome, u32 compressed length, u32 checksum |
| +0x4c | 16 | resource fork: same four fields |

A fork with compressed length 0 is absent. A fork's length can be an allocation size (round numbers like 6300 or 142000): the fork really is that long, the tail after the real content is zeros, and the stream may stop before the end (see End of stream). The rest of the record is often uninitialized memory.

## Blocks

A fork's stream is a sequence of blocks, one per absolute 64 KiB step of output: block k ends when output reaches (k+1) · 65536 or the fork length, whichever is smaller. Each block starts on a byte boundary with a 4-byte header:

- byte 0 = 1: stored. Raw bytes follow, up to the block's end.
- otherwise byte 1 is the codec: 1 = InstaCompOne, 0 = codec 0. Any other value is unknown (none seen).
- bytes 2 and 3 vary; ignore them.

A compressed block runs until output reaches the block's end. Its last copy may overshoot into the next block's range (by up to about 2 KB); the next block still ends at the next fixed multiple of 65536. After a compressed block the reader skips to the next byte boundary. The LZ window carries across blocks (distances can reach into earlier blocks), but the `mode` flag (below) resets to 1 at the start of every block. Blocks in one fork can be mixed: Drive Setup's resource fork is compressed, compressed, stored, compressed.

## End of stream

Decoding stops when the input reaches the fork's compressed length. A token that would read past it is padding: drop it. If output is still shorter than the fork length, fill the rest with zeros.

## InstaCompOne bitstream

Bits are read MSB-first. `mode` starts at 1 at the start of each block.

Loop until the block's end:

1. Read a copy-length code `c` (table below).
2. If `c > 0` or `mode == 0`: copy. Length = c + 2, plus 1 more if mode == 0. Read a distance d (below) and copy byte by byte from d bytes back (the source may overlap the destination). Set mode = 1.
3. Otherwise (c == 0 in mode 1): literal run. Read a count n (literal-count code), then n literal bytes of 8 bits each. Set mode = 0 if n < 63, else leave it at 1.

So after a short literal run the next token is always a copy, which is why the copy is one byte longer in that position.

Copy-length codes:

| Code | c |
|---|---|
| `00`, `01` | 0, 1 |
| `100` | 2 |
| `1010`, `1011` | 3, 4 |
| `11000`, `11001` | 5, 6 |
| `110100` … `110111` | 7 … 10 |
| `1110` + 3 bits | 11 … 18 |
| `11110` + 3 bits | 19 … 26 |
| `111110` + 5 bits | 27 … 58 |
| `1111110` + 6 bits | 59 … 122 |
| `11111110` + 7 bits | 123 … 250 |
| `111111110` + 8 bits | 251 … 506 |
| `1111111110` + 9 bits | 507 … 1018 |
| `1111111111` + 10 bits | 1019 … 2042 |

ResDecompress has the last code as `11111111110` + 10 bits; that is wrong (it isn't a complete prefix code and fails on real data).

Literal-count codes (both codecs):

| Code | n |
|---|---|
| `0` | 1 |
| `100`, `101` | 2, 3 |
| `110` + 2 bits | 4 … 7 |
| `1110` + 3 bits | 8 … 15 |
| `11110` + 4 bits | 16 … 31 |
| `11111` + 5 bits | 32 … 63 |

### Distance

Let pos be the current output position in the fork (absolute, across blocks). The level s is how many of these limits are less than pos:

`[10, 20, 40, 80, 160, 672, 1000, 2688, 5376, 10752, 21504]`

so s is 0 … 11. Then read one of three forms:

| Form | d |
|---|---|
| `0` + s bits v | v + 1 (1 … 2^s) |
| `10` + (s + 2) bits v | v + 2^s + 1 (2^s + 1 … 5 · 2^s) |
| `11` + L bits v | v + 5 · 2^s + 1 |

For the long form, x = pos − 5 · 2^s and L = ceil(log2 x), with L = 1 when x < 2, and two exceptions:

- L = 11 when 1004 < x ≤ 1024. This is only reachable at level 7, so the long field widens from 10 to 11 bits at pos 1645 instead of 1665. It looks like an off-by-something in Apple's encoder, but the decoder has to match it.
- L is capped at 15, so the window is 5 · 2^11 + 2^15 = 43008 bytes.

A distance larger than pos is a corrupt stream.

## Codec 0 bitstream (text)

The same token grammar and mode rule as InstaCompOne (mode starts at 1 in each block; c > 0 or mode 0 means a copy of c + 2 bytes, +1 in mode 0, then mode = 1; c = 0 in mode 1 means a literal run with the same literal-count codes; mode = 0 if n < 63). The differences:

- Literal bytes are 7 bits each (the content is ASCII).
- Its own copy-length codes:

| Code | c |
|---|---|
| `000` … `011` | 0 … 3 |
| `1000` … `1011` | 4 … 7 |
| `110` + 2 bits | 8 … 11 |
| `1110` + 3 bits | 12 … 19 |
| `11110` + 4 bits | 20 … 35 |
| `111110` + 5 bits | 36 … 67 |
| `1111110` + 6 bits | 68 … 131 |
| `11111110` + 7 bits | 132 … 259 |
| `111111110` + 8 bits | 260 … 515 |
| `1111111110` + 9 bits | 516 … 1027 (extrapolated, never seen) |
| `1111111111` + 10 bits | 1028 … 2051 (extrapolated, never seen) |

- Distance levels come from the limits `[10, 20, 40, 80, 160, 320, 832, 1280, 2560, 5120, 10240]`. 832 (not 640) is real, the counterpart of InstaCompOne's odd 1000.
- The long field is plain ceil(log2 x), at least 1, capped at 15, with no 1004 exception.

## Rust module

`crates/arc-mac/src/tome.rs`, MIT, standard library plus the crate's existing `anyhow`, `byteorder` and `fs_mac::hfs::decode_mac_filename`. Since arc-mac 0.2.0 it is wired into the rest of the crate (see Integration below).

```rust
pub const TOME_MAGIC: [u8; 4];
pub fn is_tome(bytes: &[u8]) -> bool;                       // magic + record table fits
pub fn parse(bytes: &[u8]) -> Result<Vec<TomeEntry>>;       // the record table
pub fn decode_fork(bytes: &[u8], fork: &TomeFork) -> Result<DecodedFork>;
pub fn decode_stream(input: &[u8], len: usize) -> Result<DecodedFork>;  // one fork's compressed bytes

pub struct TomeEntry { index, name_raw, name, type_code, creator_code, create_date, mod_date, finder_flags, data: TomeFork, rsrc: TomeFork }
pub struct TomeFork { len, offset, compressed_len, checksum }       // is_empty() = compressed_len == 0
pub struct DecodedFork { data: Vec<u8>, consumed: usize, blocks: Vec<BlockKind> }
pub enum BlockKind { Stored, InstaCompOne, Text }
```

`DecodedFork::data` is always exactly the fork length. Since the stored checksum isn't understood, the integrity check is `consumed == fork.compressed_len`: a wrong table or a corrupt stream almost never ends exactly on the stated length.

Usage:

```rust
let bytes = std::fs::read(path)?;
for e in arc_mac::tome::parse(&bytes)? {
    let data = arc_mac::tome::decode_fork(&bytes, &e.data)?;
    let rsrc = arc_mac::tome::decode_fork(&bytes, &e.rsrc)?;
    // e.name, e.type_code, e.creator_code, e.finder_flags, e.create_date, e.mod_date
}
```

### Integration (arc-mac 0.2.0)

- `detect.rs`: `MacArchiveKind::InstallerTome` when `tome::is_tome` matches, checked after the other strong-magic formats. The magic is only 4 bytes, so `is_tome` also requires the record table to fit in the file. It is multi-file.
- `extract.rs`: `open_bytes` maps each `TomeEntry` onto a `StuffItEntry` (flat path, `is_dir: false`) whose forks carry `ForkCodec::Tome`, and also peels a Tome out of a MacBinary or a single-file BinHex. The existing BinHex / MacBinary / AppleDouble / raw outputs then work unchanged.
- `stuffit::decompress_fork` routes by `ForkCodec`, so every caller (the browse viewer, extract, export) decodes Tome forks; a fork whose stream does not end at its stated compressed length is an error. A fork longer than `tome::MAX_FORK_LEN` (256 MiB) is refused before anything is allocated.
- In rusty-backup, a Tome is content-detected wherever Mac archives are: inside a mounted HFS volume or CD (the usual home of a Tome), and as a host file dropped onto an HFS volume, where the import prompt offers Expand. `rb-cli archive list` / `extract` take one directly.

Unit tests in the module cover the tables (complete, prefix-free), level and long-field edge cases (including the 1004 exception), both codecs on hand-built streams, padding at end of stream, stored blocks, an unknown codec and record parsing. `cargo test -p arc-mac --lib tome`.

## Verification

Test set: 58 Tomes. One is `Installation Tome 5` from a System 7.5 CD, two are single-file Tomes (PlainTalk's `InlineFilter.cmp` and `QuickDraw GX Helper` from the GX installer), and 55 come from a software-library HFS image of Apple system software and driver installers. They hold 1818 non-empty forks (438 data, 1380 resource) and use all three block types. The files are Apple's and aren't in the repo.

Results:

- Every fork decodes with output = stated length and consumed = stated compressed length, no errors.
- `crates/arc-mac/examples/tome_hash.rs` and the Python reference decoder print one line per fork (file, record, fork, lengths, consumed, SHA-1). On all 1818 forks the two outputs are identical.
- Against installed copies of the same files: 7 resource forks (Beep, Turn Sound Off, PrintMonitor, English Dialect, ImageWriter, Script Editor, LaserWriter 300/LS) match resource for resource. The only difference is `vers` ID 2, the package-version stamp of whichever release the installed copy came from. 10 InstaCompOne-decoded forks are SHA-1-identical to installed files from another source, including compressed ones (Apple Ethernet CS II, ENET Compatibility, ObjectSupportLib, PowerBook 3400 Ethernet/Modem, PowerBook Ethernet (PCI)).
- Codec 0: 19 PPDs decode byte-exact against the same PPDs installed on a Mac OS 9.2.2 volume, and 107 of 116 codec-0 data forks are SHA-1-identical to files in catalogs of other installs. The other 9 are versions not in those catalogs and decode to clean ASCII.
- Codec 0, long copies: all 33 codec-0 PPDs in the Mac OS 8.1 Update's `LaserWriter Software/Tome 1` are SHA-1-identical to the same PPDs stored uncompressed in the Mac OS 8.5 `Installation Tome` (on the 8.6 CD). They include LaserWriter 8500 v1.0 (43506 bytes, 356 copies of 38 bytes or more) and LaserWriter 12/640 PS v1.1 (37556 bytes, longest copy 383 bytes), so every length code up to `111111110` is confirmed by known plaintext.
- A further 16 Tomes (the 15-disk System 7.5.3 Revision 2 floppy set and the 7.5.5 Update's `Tome1`, 504 forks, InstaCompOne and codec 0) also decode with consumed = compressed length on every fork.

To hash a set of Tomes with the Rust decoder:

```bash
cargo run --release -p arc-mac --example tome_hash -- TOME... > rs.tsv
```

The Python reference decoder (`ic3.py`, about 140 lines) and `hashall.py` are in `reference-python/` of the zip this doc came in, not in the repo. `python -I reference-python/hashall.py TOME... > py.tsv` prints the same TSV, so the two decoders can be diffed on any new Tome (ignore line endings and the file-name column, which Windows Python prints in the console code page).

## Still open

- **The two longest codec-0 copy-length codes.** `1111111110` and `1111111111` (copies of 518 bytes or more) are extrapolated from the shorter codes. No known stream uses them: the longest codec-0 copy in all 74 Tomes is 383 bytes.
- **The first four codec-0 level limits** (10, 20, 40, 80) are only loosely constrained, since every PPD starts with the same header. 160 and up are each the only value that decodes all 19 plaintext pairs.
- **The per-fork checksum** (+0x48 and +0x58). Not CRC-32, Adler-32, CRC-16/XMODEM, byte or word sums, or XOR, over either the decoded or the compressed bytes. Extraction doesn't need it.
- **Other codec values** in block header byte 1. None seen; the decoder reports them as an error with the header bytes.

## How it was worked out

InstaCompOne started from the copy-length and literal-count tables in maximumspatium's ResDecompress (`InstaCompOne.py`, MIT), which handles the same codec inside compressed resources. The Tome-specific parts (the container, block structure, end-of-stream rule, distance levels, the long-field rule and its 1004 exception, the fixed last length code) were found by decoding Tome forks against installed copies of the same files and fitting each rule at the first byte where output diverged.

Codec 0 was cracked from three LaserWriter PPD streams that differ in a single bit exactly where the text says `4.0` vs `4.2`. That exposed raw 7-bit literals. The literal runs then lined up with InstaCompOne's literal-count codes, and the gaps between them decoded as InstaCompOne-style distances behind a short length code. Each level limit was then found by sweeping its value over all 19 plaintext pairs and keeping the only value that decodes them all.

Dead ends, so nobody retries them:

- Uniform level limits (10 · 2^s, 21 · 2^s, 5 · 2^s), more than 11 levels, or level 6 ending at 1024 or 1344.
- Long-field width from `bit_length`, x + 1, or x + copy length; a cutoff of 1000 instead of 1004 for the early 11-bit switch.
- Deciding stored vs compressed per fork from "compressed length = length + 4 · blocks": blocks are mixed.
- Treating header byte 0 = 0 as always InstaCompOne: `00 00 00 00` is codec 0.
- Codec 0 as Okumura LZHUF or any adaptive Huffman (any alphabet size, either bit order).
- Codec 0 with InstaCompOne's level limits (fails at output 321), a level-7 limit of 640 (fails at 641), the 1004 exception (breaks 8 of 19) or a 12th level (breaks 9 of 19).

## Credits

- InstaCompOne copy-length and literal-count codes: maximumspatium/ResDecompress, `InstaCompOne.py`, Copyright (c) 2018-2019 Maxim Poliakovski, MIT. Noted in `crates/arc-mac/THIRD_PARTY_NOTICES.md`.
- Everything else (format, codec 0, Rust module): original work, MIT.
