# PPC-Tiger feature-parity status

## What this doc is

A living scorecard of how much of rb-cli the PowerPC/Tiger C port
(`ppc-tiger/`) covers, and what's left worth pursuing. Re-tiered as
features ship — the older chronological breakdown is collapsed into a
single "Shipped" table; the tier list at the bottom is what remains.

## Endianness primer (relevant to every filesystem decision below)

PowerPC G3/G4/G5 is big-endian. On-disk formats land in one of three
camps from the port's perspective:

- **BE-native, free on PPC**: HFS, HFS+, AFFS, PFS3, SFS, ISO 9660
  fields-of-interest, MBR, APM, GPT (where it counts), SGI Volume
  Header, EFS.
- **LE on-disk, byteswap on PPC**: FAT12/16/32, NTFS, exFAT, ext2/3/4,
  btrfs, exFAT. The `read_le16/32` helpers do that work; PPC tests
  exercise the byteswap paths the dev box (little-endian arm64) can't.
- **Mixed/format-specific**: HFS+ B-tree keys are UTF-16 *big*-endian
  (free); exFAT and Joliet names are UTF-16 *little*-endian (swap).

---

## Shipped

Everything below is committed on `extend-ppc-tiger` (or main) and
exercised by `ppc-tiger/test_fs_readers.sh` where applicable. Commits
are listed for traceability; commit IDs are short SHAs on this branch.

### Output formats & wrappers

| Feature | Commit |
|---|---|
| Raw + gzip output, CRC32 | pre-branch |
| `--checksum sha256` (CommonCrypto) | `0a8100a` |
| `--split-size <MIB>` | `d0540fb` |
| `--sparse` (raw zero-skip) | `b260639` |
| `--format vhd` (Fixed footer) | `c9f1894` |
| DC42 + 2MG image-format wrappers (transparent) | `f97e2f1` |

### Partition tables (read)

| Feature | Commit |
|---|---|
| MBR + EBR chain (logical partitions) | pre-branch |
| APM | pre-branch |
| GPT (CRC32-validated, with `gpt.json` sidecar) | `a6eaf5d` |
| SGI Volume Header (IRIX disks; type-aware enumeration) | `65c5367` |
| Superfloppy detection (FAT / HFS / HFS+ / AFFS / ProDOS / ext / exFAT / EFS) | various |

### Filesystem browse (`ls` / `get`)

| Filesystem | Notes | Commit |
|---|---|---|
| ISO 9660 | + Joliet, no Rock Ridge | `f3fe107` |
| Classic HFS | Verified against 40 MiB Quadra image | `2ef985d` |
| FAT12/16/32 | Full cluster-chain + LFN | `b2d3fc7` |
| HFS+ / HFSX | UTF-16BE keys | `0c44180` |
| AFFS (Amiga FFS/OFS) | Extension-block chain | `d7c5970` |
| ProDOS (Apple II) | Seedling / sapling / tree | `b170fd2` |
| ext2/3/4 | 12-direct + 1/2/3 indirect + ext4 extent trees | `6f87669` |
| exFAT | FAT chains + NoFatChain extents | `6f87669` |
| SGI EFS | Direct + indirect 8-byte extents, 0xBEEF dir blocks | `65c5367` |

### Verbs

| Verb | Notes | Commit |
|---|---|---|
| `backup` / `restore` / `inspect` | core flow, pre-branch | pre-branch |
| `show devices` / `optical rip` | pre-branch | pre-branch |
| `show partmap` / `show fs-info` | MBR/GPT/APM/SGI | `4b09b1b` |
| `write IMG TARGET` | raw streamer with `--yes` device guard | `0af7573` |
| `ls` / `get` | per-filesystem dispatch | `f3fe107` |
| `fsck` | FAT12/16/32 read-only check | `34a21ec` |

### Test harness

| Item | Commit |
|---|---|
| `test_fs_readers.sh` integration test | `5dbe464` |
| Compressed self-contained fixtures (ext2, exFAT, EFS, SGI; ~90 KB total) | `da4a2ad`, `65c5367` |

---

## What's left, re-tiered

Tiers are sized by *implementation effort right now*, given everything
above is in place. Per-tier they're listed roughly highest-value first.

### Tier A — small / contained / no vendored code (days, not weeks)

| Feature | Effort | Notes |
|---|---|---|
| **RDB partition table** (Amiga) | Small (~300 LOC) | Unlocks the already-shipped AFFS reader on Amiga hard-disk images (`.hdf`, `.hdz`). Today AFFS only works on floppy-style `.adf`. RDSK + PART chain is straightforward BE parsing; similar shape to APM. |
| **`new --fs fat`** | Small | Format a blank FAT image of a given size. Still gated by lack of a `put` verb (so the blank image is empty until you mount it on the host), but cheap to ship and useful for round-tripping with macOS. |
| **HFS / HFS+ `fsck`** (read-only verifier) | Medium-high | Existing rb-cli code is large but the verify-only half is the smaller part. Caveat from this session's feedback: a "find problems, can't fix" verb is low value on its own — would land naturally bundled with HFS write (Tier D). |

### Tier B — moderate read-side work, still no external deps (1–3 weeks)

| Feature | Effort | Notes |
|---|---|---|
| **NTFS read** (`ls` / `get`) | ~3000 LOC C | The big missing one for "browse any common PC disk." MFT records, runlists, B+ tree indices, UTF-16LE names. Pure reads, no vendored code; extends the existing browse pattern. The Rust impl is ~800 LOC but the on-disk surface (security descriptors, attributes, sparse files) sprawls. |
| **ext3 / ext4 fuller read** | Mostly done | ext4 extent trees + ext3 (journal-ignore) already shipped with the ext2 reader. Added 2026-05-20: `INLINE_DATA` (ext4 tiny-file inline content) and symlink-refuse hint in `get` (also applied to EFS for consistency). What's left is htree-as-index for very large hashed directories — current linear leaf walk is correct, just O(N) on directories with thousands of entries. Likely YAGNI for vintage images. |

### Tier C — vendor third-party code (weeks of build-plumbing work)

| Feature | Effort | Notes |
|---|---|---|
| **Zstd output** (`--format zstd`) | Medium | Drop in zstd's portable C reference (~1.5 MB), patch `inttypes`-type glitches for gcc-4.0. No algorithm work — almost entirely build plumbing. Gives FAT / HFS / ext / exFAT / EFS backups real compression vs gzip. Slow on G3, fine on G4/G5. **Top of the queue if you want a tangible output-side win.** |
| **CHD read** (for restore + browse) | Medium-high | Vendor a stripped subset of MAME's `lib/util/chd*.{cpp,h}` + LZMA / FLAC / Deflate decoders. Reading drops scope by ~half vs writing (no codec selection). Unlocks PPC restoring desktop-rb-cli CHD backups. |
| **CHD output** (full chdman subset) | High | The big one — codec choice, hunk packing, parent CHDs. Roughly the same scope as the original PPC port itself. Only worth it if you want a PPC G5 producing CHDs natively. |

### Tier D — write / edit (each a multi-week sub-project)

| Feature | Effort | Notes |
|---|---|---|
| **`bless` / `chmeta` / `setrsrc`** | Small in isolation | Each is a tiny surgical write into HFS / HFS+ catalog records. Blocked on HFS *write* existing first. |
| **HFS edit** (+ fsck repair) | High | The HFS write side carried real complexity on the desktop (B-tree splits, map nodes, file-thread quirks, ROM-bootability invariants per the project memory). Re-tracking that in C is real work. |
| **HFS+ edit** | High | Same shape. |
| **NTFS write** | Very high | Bigger than HFS+. Realistically not happening in C. |
| **PFS3 / SFS edit** (Amiga) | High | Multi-week each on the desktop; same in C. |
| **`resize` / `shrink` / `grow`** | High | Each fs has its own in-place resize; needs the corresponding write side. |
| **`expand` (HFS block-size)** | High | Cloning to a fresh volume, CNID remapping, APM rebuilding. Whole `hfs_clone.rs` depends on HFS write. |
| **`defrag` clone** (HFS+ / PFS3) | High | Same. |
| **`partmap` edit** | Medium | MBR / APM / GPT / SGI partition-entry edits. Layered on top of existing parsers. |

### Tier E — likely impractical on PPC / Tiger (unchanged)

| Feature | Why not |
|---|---|
| **CD-DA audio rip** | rb-cli uses `rust-cd-da-reader`; on Tiger you'd need `cdda2wav` or `libcdio` — G3/G4 timing-sensitive on real hardware. The existing `optical rip` does data discs only. |
| **WOZ write** | Apple II floppy synthesis; rb-cli's `woz_write.rs` is 1442 LOC for a reason. |
| **XFS / btrfs** | Both huge; rb-cli's `btrfs.rs` is mostly placeholder reads. Skip. |
| **GUI feature parity** | The Carbon GUI exists at ~853 LOC; matching egui's browse view (per-file edit, archive-edit mode, expand dialogs, partmap editor) is essentially rewriting it. |
| **HTTPS / update-checker** | No HTTPS stack on Tiger by default. |
| **Modern Unicode normalization** | Heavy. Could vendor a minimal NFC/NFD subset for HFS. |
| **`terminal` REPL** | Low priority; could substitute GNU readline. |

---

## Recommended next steps

Given the user-stated preference for features that *produce something*
(extracted data, compressed output) over verify-only diagnostics, and
given everything in Tiers A and B that's already shipped:

1. **Zstd output** (Tier C) — biggest single tangible win. Replaces
   gzip in the output pipeline for every shipped filesystem and
   partition table without changing any other code. Build-plumbing
   heavy, no algorithm work.
2. **RDB partition table** (Tier A) — small, makes the existing AFFS
   reader usable on real Amiga hard-disk images, not just floppies.
3. **CHD read** (Tier C) — the moment this lands, a PPC G4/G5 can
   restore a desktop rb-cli CHD backup. Larger sub-project than zstd.
4. **NTFS read** (Tier B) — the last big missing browse target. Pure
   reads, no vendored code, but ~3000 LOC of careful C.

Everything past that (CHD write, NTFS write, the edit verbs, resize /
shrink / grow / expand / defrag) is real Rust-side feature work whose
C re-port costs more than it's worth — those will land on PPC the day
the `rust-ppc-tiger` compiler can build the rusty-backup crate
proper.
