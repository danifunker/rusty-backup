# PPC-Tiger feature-parity analysis

Methodology: cross-referenced rb-cli's verb set + module layout against
the PPC port's current C surface and Tiger 10.4's native library
inventory (zlib, CommonCrypto, BSD libc, Carbon, optionally the system
`gcc-4.0` C++ runtime). PowerPC G3/G4/G5 is big-endian, which actually
simplifies several formats vs x86 (HFS, AFFS, PFS3, SFS, MBR — all
big-endian on-disk or endian-neutral; HFS+ is BE; NTFS / exFAT / ext /
btrfs are LE and need byteswap macros).

The 5 tiers below are sized by "how much new C code does this need on
top of what's there".

---

## Tier 0 — already implemented

| Feature | Status |
|---|---|
| MBR (incl. EBR chain) | done |
| APM parsing | done |
| Superfloppy detection | done |
| FAT12/16/32 compaction | done |
| Raw output | done |
| Gzip output | done (zlib) |
| CRC32 checksums | done (zlib) |
| `backup` / `restore` / `inspect` / `show devices` / `optical rip` | done (CLI-alignment PR) |

### Tier 1 — shipped 2026-05-19

| Feature | Commit |
|---|---|
| `--checksum sha256` (CommonCrypto) | `0a8100a` |
| `--split-size <MIB>` | `d0540fb` |
| `--sparse` (raw zero-skip) | `2fd3314` |
| `--format vhd` (Fixed footer) | `31d0c3c` |
| GPT read + CRC32 + `gpt.json` sidecar | `1f919ee` |
| `show partmap` + `show fs-info` | `6fe952f` |

### Tier 2 — shipped 2026-05-20

| Feature | Commit |
|---|---|
| `write IMG TARGET` raw streamer | `0af7573` |
| `ls` / `get` + ISO 9660 reader | `f3fe107` |
| Classic HFS reader | `2ef985d` |
| FAT12/16/32 reader | `b2d3fc7` |
| HFS+ / HFSX reader | `0c44180` |
| AFFS (Amiga FFS/OFS) reader | `d7c5970` |
| ProDOS reader | `b170fd2` |
| DC42 + 2MG image wrappers (transparent) | `f97e2f1` |
| FAT `fsck` (read-only) | `34a21ec` |

---

## Tier 1 — free or near-free (≤ ~200 LOC, no external deps)

These use libraries Tiger already ships, on-disk formats that are small
and documented, or are pure byte arithmetic.

| Feature | Effort | Why it's cheap |
|---|---|---|
| **VHD (Fixed) output** | Low (~100 LOC) | Just a 512-byte footer + CHS geometry computation. No compression. `src/rbformats/vhd.rs` is the reference; the math is portable. |
| **GPT read** (parse primary + verify CRC32) | Low–Medium (~300 LOC) | Already have CRC32 via zlib; GPT is just two CRC'd structs + a partition-entry array. The existing `TODO` comment in `rust_cli_real.c` acknowledges this. |
| **`--checksum sha256`** | Low (~20 LOC) | CommonCrypto on Tiger 10.4 has `CC_SHA256_*` (same header that gave us SHA-1). Drop-in replacement; would replace the current `crc32`-only state with `crc32 + sha256`. |
| **Sparse-zero output / `is_all_zeros`** | Low | Trivial byte scan; matches rb-cli's raw splitter. |
| **`--split-size` for raw / gzip** | Low | Just `partition-N.partXX` naming + size threshold during write. |
| **`show fs-info IMG@N`** | Low | The PPC code already reads FAT BPB + HFS MDB for hints; surfacing them under a new subcommand is mostly plumbing. |
| **`partmap show` for MBR / APM** | Low | Both parsers already exist internally. Just emit a table. |

---

## Tier 2 — minimal (~200–700 LOC each, no exotic deps)

Small filesystems and well-spec'd formats. PowerPC's native BE byte
order makes several of these easier than they look.

✅ shipped 2026-05-20:

| Feature | Status |
|---|---|
| **ISO 9660 read** (`ls` / `get`) | done; Joliet detection works, no Rock Ridge |
| **HFS read** (classic Mac) | done; verified against 40 MiB Quadra image |
| **HFS+ / HFSX read** | done; verified against HFS+J test image |
| **FAT12/16/32 read** (`ls` / `get` with LFN) | done; full cluster-chain follow |
| **AFFS read** (Amiga) | done; FFS + OFS, extension-block chain |
| **ProDOS read** (Apple II) | done; seedling/sapling/tree storage types |
| **DC42 / 2MG image wrappers** | done; transparent — adds image_data_offset to PartTable |
| **`write IMG /dev/rdiskN`** | done; `--yes` guard on device targets |
| **`fsck` (FAT)** | done; read-only diagnostic for cross-links / lost / cycles |

⏭️ deferred (not shipped, future work):

| Feature | Why deferred |
|---|---|
| **`new --fs fat`** (blank FAT volume) | Limited value without a `put` verb to populate it; the user can still mount blank images via macOS Finder. |
| **`partmap` edit (MBR / APM)** | Significant write code (~600 LOC); higher value once restore-with-resize lands. |
| **`batch` JSON runner** | Depends on the read-only verbs being polished + a `put` verb; revisit after Tier 3. |
| **WOZ read** | Niche Apple II format; defer until a clear use case shows up. |
| **RDB partition table** | Needed to browse AFFS partitions inside Amiga hard-disk images. ~300 LOC. Deferred — most Amiga vintage images are floppies, which work via raw `.adf`. |

---

## Tier 3 — medium (700–2500 LOC each, may need to vendor 3rd-party C)

| Feature | Effort | Notes |
|---|---|---|
| **Zstd output** | Medium | Zstd has a portable C reference implementation; it builds on PPC/Tiger with mild patching (mostly `inttypes`). ~1.5 MB of vendored C. Slow on G3, fine on G4/G5. |
| **CHD output via libchdman C++** | Medium-high | **This is the big question.** `libchdman-rs` is a Rust wrapper; the Rust-to-PPC compiler in `rust-ppc-tiger` has only built 45 of 467 needed crates, so the Rust path is years out. **But the underlying C++ chdman code is portable** and gcc-4.0.1 + libstdc++ on Tiger can build it. You'd vendor a stripped subset of MAME's `lib/util/chd*.{cpp,h}` (~10k LOC C++) plus its LZMA + FLAC + Deflate codecs. Roughly the same scope as the original PPC port itself. Possible but a substantial sub-project. |
| **CHD *read*** (for restore + browse) | Medium | Same C++ vendoring; reading is much simpler than writing (no codec choice). If you only need read, the scope drops by ~half. |
| **HFS+ read** | Medium | Bigger B-trees than HFS, Unicode keys (UTF-16 BE), but still BE. ~2000 LOC C. Extends naturally from HFS work. |
| **ext2 read** | **DONE** | Shipped read-only `ls` / `get` for ext2/3/4 in `rust_cli_real.c` (`fs_ls_ext2` / `fs_get_ext2`): superblock + block-group descriptors, inodes, classic 12-direct + single/double/triple indirect block maps, and ext4 extent trees. LE-on-disk handled by `read_le16/32`. Detected as `FS_EXT2` both inside MBR partitions and as a bare superfloppy. Symlink-follow, htree-as-index, and inline-data are not implemented (linear leaf walk covers hashed dirs). |
| **exFAT read** | **DONE** | Shipped read-only `ls` / `get` in `rust_cli_real.c` (`fs_ls_exfat` / `fs_get_exfat`): VBR parse, 32-bit FAT cluster chains, directory entry sets (0x85 File + 0xC0 Stream + 0xC1 Name), and the NoFatChain contiguous-extent flag for both directory children and file data. Detected as `FS_EXFAT` (VBR magic "EXFAT   " beats `is_fat_vbr`) inside MBR partitions and as a bare superfloppy. UTF-16LE names rendered ASCII best-effort; path match is ASCII case-fold (upcase table not consulted). |
| **`fsck` for HFS / HFS+** | Medium-high | Existing rb-cli code is large but the reading-only verifier is the smaller half. |
| **`bless` / `chmeta` / `setrsrc`** | Medium | Each is a small surgical write into HFS / HFS+ catalog records. Once HFS *write* exists, these are short. |

---

## Tier 4 — high (≥ 2500 LOC and/or significant cross-port work)

| Feature | Effort | Notes |
|---|---|---|
| **NTFS read** | High | MFT records, data runs, B+ tree indices, Unicode keys. The Rust impl is ~800 LOC but the surface is sprawling — security descriptors, attributes, sparse files. ~3000 LOC C. |
| **NTFS write** | Very high | Same as rb-cli — bigger than HFS+. |
| **ext3 / ext4 read** | High | Journal replay (ext3), HTree dirs + extents (ext4). Significant. |
| **HFS edit + fsck repair** | High | The recent project memory shows how much hardening this needed (B-tree splits, map nodes, file-thread quirks, ROM-bootability invariants). Re-tracking that in C is real work. |
| **HFS+ edit** | High | Same. |
| **PFS3 / SFS edit** (Amiga) | High | Amiga FS rb-cli phases were multi-week projects. Read-only is Tier 2; edit is Tier 4. |
| **EFS read** (SGI) | **DONE** | Shipped read-only `ls` / `get` in `rust_cli_real.c` (`fs_ls_efs` / `fs_get_efs`): superblock (BE magic 0x00072959/0x0007295A at byte 540), 128-byte inodes located via cylinder-group geometry, 8-byte extent records (direct + indirect), and 0xBEEF directory blocks. Also added SGI Volume Header partition-table parsing (`PT_SGI`, magic 0x0BE5A941) so EFS partitions enumerate via `image@N`; bare EFS partitions detected as superfloppies too. Big-endian throughout (matches the PPC host). Browse only — no bitmap/fsck. |
| **`expand` (HFS block-size change)** | High | Cloning to a fresh volume, CNID remapping, APM rebuilding. Whole `hfs_clone.rs` depends on HFS write being there. |
| **`defrag` clone for HFS+ / PFS3** | High | Same. |
| **`resize` / `shrink` for HFS / HFS+ / NTFS / ext / exFAT / EFS** | High | Each fs has its own in-place resize; needs the corresponding write side. |
| **`grow` + `partmap` data-aware moves** | High | rb-cli doesn't move data either, so technically this is the same scope — but a complete impl is here. |

---

## Tier 5 — likely impractical on PPC / Tiger

| Feature | Why not |
|---|---|
| **CD-DA ripping (cdparanoia-style audio)** | rb-cli uses `rust-cd-da-reader`; on Tiger you'd need `cdda2wav` or `libcdio`, both portable but G3/G4 timing-sensitive on real hardware. The `optical rip` command we already have only does *data* discs (raw read). |
| **WOZ *write*** | Apple II floppy synthesis — possible in theory but huge spec; rb-cli's `woz_write.rs` is 1442 LOC for a reason. |
| **XFS / btrfs** (read or write) | XFS is huge (Linux-only mature impl); btrfs is even bigger. rb-cli's `btrfs.rs` is mostly placeholder reads. Skip entirely. |
| **GUI feature parity** (drag-drop, browse view) | The Carbon GUI already exists at ~853 LOC but extending it to match egui's browse view (per-file edit, archive-edit mode, expand dialogs, partmap editor) means rewriting most of the GUI. |
| **`reqwest` / `webbrowser` / update-checker** | No HTTPS stack on Tiger by default; could use the project's own `curl_mbedtls` build but is invasive. Probably not worth it. |
| **Modern Unicode normalization** | rb-cli's `unicode-normalization` crate is heavy; on Tiger you'd have to vendor ICU or write minimal NFC / NFD tables. Likely doable for the HFS-relevant subset only. |
| **`terminal` REPL** | `rustyline` is large; could substitute GNU readline (Tiger ships it), but priority is low. |

---

## Recommended phasing

If you want to push PPC further, the highest leverage per LOC is:

1. **SHA-256 + VHD + GPT-read + sparse-zero + `--split-size`** — these
   are all Tier 1 and bring you to genuine output-format parity for
   free.
2. **Read-only HFS + HFS+ + ISO 9660 browse** — Tier 2 wins big on the
   Mac-vintage use case the port targets. Browse-only, no edit.
3. **Zstd output** — Tier 3, vendored. Once it's in, FAT / HFS backups
   get rb-cli-quality compression instead of gzip.
4. **CHD read only** — Tier 3. Lets PPC *restore* backups produced by
   the desktop rb-cli; you don't have to add CHD write to be useful.
5. *(Optional)* **CHD write** — Tier 3+. The vendored chdman subset is
   doable but a real sub-project. Worth it only if you want a PPC G5
   producing CHDs natively.

Everything beyond that (NTFS, ext3 / ext4, FS edit, partmap edit with
data moves, the defrag / expand verbs) is real Rust-side feature work
whose C re-port costs more than it's worth — those will land on PPC the
day the `rust-ppc-tiger` compiler can build the rusty-backup Rust crate
proper.

---

**One concrete recommendation if you only do one thing:** add
**SHA-256 + GPT-read + CHD-read**. That trio means a desktop rb-cli
backup with default settings (CHD + SHA-256, possibly with a
GPT-labeled source) is *restorable* on PPC. Right now the formats
simply don't overlap enough for cross-tool backups to round-trip.
