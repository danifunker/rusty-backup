# License audit for the crate split

Status: **audit done 2026-10-30; decisions pending.** It feeds [crate_split_plan.md](crate_split_plan.md) §6.

**Question:** which code headed for the MIT crates was translated from, or copies data out of, code under a license that MIT can't carry?

**Method:**
1. A broad scan of `src/{fs,partition,rbformats,macarchive,clonezilla}` and `crates/` for provenance claims ("ported / mirrors / verbatim / based on / taken from", license names).
2. Every claim checked side by side against the original source where it is on disk: the Linux kernel, XADMaster, MAME, Aaru, ADFlib, amitools, pfs3aio, smartfilesystem and xfsprogs 3.1.11. Apple `hfs`, sQLux and nyarime/gho were read upstream.

This is engineering evidence (what matches what), **not legal advice**. Anything marked "decide" is a call for the copyright holder.

## Verdict key

| Verdict | Meaning | MIT crate? |
|---|---|---|
| **FACTS** | only on-disk layout, constants, magics, short format-mandated formulas | yes |
| **NOTICE** | follows code under a permissive license (BSD / MIT) | yes, with that license's notice shipped alongside |
| **DERIVED** | function-by-function translation, or copied tables, from GPL / LGPL code | no, unless rewritten |
| **UNCLEAR** | source has no license terms, or the comparison wasn't possible | see the item |

## Findings

### Already in an MIT crate

| Code | Source followed | Source license | Verdict | Action |
|---|---|---|---|---|
| `fs-8bit/oric.rs` | MAME `fs_oric_jasmin.cpp`: `prepare_name` is a close translation of `file_name_prepare` | BSD-3-Clause (Olivier Galibert) | NOTICE | **Done:** `crates/fs-8bit/THIRD_PARTY_NOTICES.md` |
| `fs-8bit/dragondos.rs` | MAME imgtool `dgndos.cpp` (layout reference) | BSD-3-Clause (tim lindner) | FACTS / NOTICE | Covered by the same notice file, as a precaution |
| other `fs-8bit` modules, `disk-core` | MAME imgtool as a test oracle only | n/a | FACTS | none |

### Filesystems (future `fs-*` crates)

| Code | Source followed | Source license | Verdict | Remediation |
|---|---|---|---|---|
| `fs/hfs_unicode.rs` | Linux `fs/hfsplus/tables.c` + `unicode.c`. Case-fold table (2,816 values) and decomposition trie (4,218) match exactly; `case_fold` and `decompose_nonhangul` are near-transliterations | GPL-2.0 (Linux) | **DERIVED** | The case-fold table originates in Apple TN1150 (the kernel says so): re-source it from TN1150 and diff. The decomposition trie is the kernel's own encoding: generate our own table from TN1150's list or UnicodeData.txt, and rewrite the lookup |
| `fs/hfs_common.rs` `HFS_CHARORDER` | hfsutils `libhfs/data.c` `hfs_charorder` (not the kernel's) | GPL-2.0-or-later | **DERIVED** (copied table) | Rebuild from Apple's documented Mac Roman sort order (Inside Macintosh: Text) or measured Mac OS behaviour, and record how |
| `fs/adfs.rs` `AdfsFsm` | Linux `fs/adfs/map.c`: `lookup_zone`, `scan_map`, `map_lookup`, `free_bytes`, `zone_metadata` keep the kernel's statement order and locals | GPL-2.0 | **DERIVED** | Clean-room rewrite of the map walk from the RISC OS PRM FileCore chapter; validate with the `CROS42` / `ICEBIRD` probes |
| `fs/hpfs.rs`, directory (dnode) **editing** | Linux `fs/hpfs/dnode.c`: same function breakdown, names and split heuristic (`/2 + 10`) | GPL-2.0 | **DERIVED** | Rewrite insert / split / delete with our own B-tree design; validate with `scripts/hpfs-oracle.py`. The read side is FACTS |
| `fs/xfs/edit.rs` `dir_hashname` | `xfs_da_hashname` (`libxfs/xfs_da_btree.c`), line for line | GPL-2.0 (xfsprogs) | **DERIVED** (tiny, format-mandated hash) | Rewrite from SGI's "XFS Algorithms & Data Structures" (CC-BY-SA) and cite that |
| `fs/xfs/*` repair / fsck / rebuild | xfs_repair 3.1.11 cited as "porting reference" | GPL-2.0 | FACTS (independent structure; xfs_repair used as an oracle) | Reword "Porting reference" to "behavioural reference, validated against `xfs_repair -n`" |
| `fs/xfs/*` read modules | `xfs_format.h` offsets / bitfields | GPL-2.0 | FACTS | none |
| `fs/hfsplus_journal.rs` `calc_checksum` + structures | Apple `core/hfs_journal.c` ("lifted verbatim") | APSL-2.0 | FACTS in substance: TN1150 "Journal" prints the same checksum and documents the structures | Re-cite to TN1150 and drop "verbatim / byte-for-byte" |
| `fs/pfs3.rs` | pfs3aio, cited by file:line ~12×; behaviour reimplemented, not translated | **BSD 4-clause** (Michiel Pelt), *with advertising clause* | NOTICE (conservative) / FACTS | **Decide (D3).** Either ship the notice (but the advertising clause makes the crate stricter than MIT and GPL-incompatible), or recast the citations as format descriptions, since the audit found no translated code |
| `fs/pfs3_fsck.rs`, `pfs3_clone.rs` | own design | n/a | FACTS | none |
| `fs/sfs.rs` | SFS sources (`filesystemmain.c`, `nodes.c`, `SFScheck.c`) for layout and behaviour | **"freeware", no terms**; `aros/bitfuncs` GPL-2+ | UNCLEAR | Recast citations as on-disk-format interop; fix the wrong "pfs3aio's SFS sources" attribution (`sfs.rs:459`); never use `bitfuncs` |
| `fs/sfs_fsck.rs` | claims "a port of SFScheck", but phases, names and messages all differ | as above | FACTS (leaning independent) | Fix the overstated header |
| `fs/affs*.rs` | ADFlib / amitools as references | ADFlib per-file GPL-2+ (top-level LGPL-2.1); amitools GPL-2 | FACTS | Re-cite `affs_common.rs:7` to the AmigaDOS manual / ADF FAQ |
| `fs/ofs*.rs` | BeOS "old Be File System", **not Amiga** | n/a | n/a | Move into the BeOS crate (`fs-unix` `bfs` feature, or its own), not `fs-amiga` |
| `fs/qdos.rs`, `qdos_mdv.rs` | sQLux `QDisk.c` layout and name-comparison behaviour | UQLX custom licence (no OSI licence) | FACTS (own allocator; only format semantics shared) | Re-cite as "QXL.WIN format, behaviour cross-checked with sQLux"; cite the kilgus QXL.WIN doc |
| `fs/vxfs.rs`, `reiserfs.rs` (kernel parts), `efs*.rs`, `ext*.rs`, `squashfs*.rs`, `btrfs.rs`, `minix.rs` | kernel / headers for layout only | GPL-2.0 | FACTS | Optionally re-cite EFS to the IRIX headers / `docs/SGI_Filesystems.md` |
| `fs/ufs*.rs` | FreeBSD `fs.h`, NetBSD makefs (fixture generator) | BSD | FACTS (add a BSD notice if any header text was copied) | none |
| `fs/jfs.rs` ("Tier-A reference port"), `reiserfs.rs`, `ufs.rs` | **partimage 0.6.9** `fs_*.cpp` | GPL-2.0 | **UNCLEAR, not compared** (partimage isn't on disk) | Compare against partimage before relicensing `fs-unix` |
| `fs/ntfs*.rs` code | clean-room (documented) | n/a | FACTS | none |
| `fs/ntfs_tables.rs` blobs | `$AttrDef` / `$UpCase` (Microsoft standard data); **`secure_sds.bin`, `root_secdesc.bin` reproduced from `mkntfs` output** | the mkntfs output's status is unclear | UNCLEAR | Regenerate `$Secure` / secdesc from a Windows-formatted volume or synthesise them from the published SD format |

### Partition tables and containers

| Code | Source followed | Source license | Verdict | Remediation |
|---|---|---|---|---|
| `partition/x68k.rs` | Aaru `Human68k.cs` (struct only). The comment says GPL-3; the file is actually **LGPL-2.1+** | LGPL-2.1+ | FACTS | Fix the licence label, or cite the BlueSCSI fixtures instead |
| `rbformats/gho.rs` Z1 Fast-LZ | nyarime/gho `fastlz.go`, explicitly ported | **MIT** (Nyarime, 2026) | NOTICE | Ship the Nyarime MIT notice. Needed **now** for the app too, since no notice exists anywhere in the repo |
| `rbformats/gho_crypto.rs` | reverse-engineered from the `ghostexp.exe` binary | n/a (no source) | FACTS-equivalent (interop reimplementation) | none |
| `partition/x68k_hdd_builder.rs` | boot bytes "lifted from `SCSI_NetBSD.hds`" | binary from a disk image | n/a (stays in the app) | Note only; not crate-bound |

### Mac archives (planned `arc-mac`)

XADMaster is **LGPL-2.1-or-later** (MacPaw; every relevant file header says "or any later version").

| Code | Verdict | Clean-room size if MIT is wanted |
|---|---|---|
| `stuffit_arsenic.rs` | **DERIVED** (incl. `inverse_bwt` from `BWT.c`) | ~450 lines |
| `compactpro.rs` (calls itself "faithful reproductions") | **DERIVED** | ~700 |
| `stuffit5.rs` | **DERIVED** | ~250 |
| `stuffit_lzw.rs`, decoder half (encoder `compress_unix_z` is independent) | **DERIVED** | ~150 |
| `stuffit_lzh.rs` (+ `stuffit13_tables.rs`, "auto-generated from XADMaster") | **DERIVED** code; tables are format data | ~300 |
| `stuffit_lzah.rs` | **DERIVED** | ~300 (LHarc / LZHUF is an independent public source) |
| `stuffit_huffman.rs` | borderline (format forces the shape) | ~60 |
| `stuffit.rs` container + writer, `bitreader_be.rs`, `detect.rs`, `extract.rs`, `macbinary.rs`, `maczip.rs`, `fs/binhex.rs`, `fs/resource_fork.rs` | FACTS / independent | none |
| `mar.rs` (claims clean-room from GPL-2.0 `mar-0.2.0`) | UNCLEAR: mar source not on disk to compare | Compare before release |

The format data tables (Arsenic randomization table, method-13 tables, LZAH distance lengths) are needed for interoperability. Whether they can ship under MIT is part of D1.

## Decisions needed

- **D1: Mac archive codecs (~2,200 lines). DECIDED 2026-10-30: (a) LGPL sub-crate.**
  - The XADMaster-derived decoders (`stuffit_arsenic`, `compactpro`, `stuffit5`, the `stuffit_lzw` decoder, `stuffit_lzh` + `stuffit13_tables`, `stuffit_lzah`, `stuffit_huffman`) go into **`arc-mac-codecs`**, licensed **LGPL-2.1-or-later**. It carries MacPaw's copyright notice and keeps the "Ported from" headers.
  - The independent code goes into MIT `arc-mac`, which depends on `arc-mac-codecs`: the container parser and writers, `bitreader_be`, BinHex, MacBinary, detect, extract, maczip, the Unix-`compress` LZW encoder, and `mar` once it's checked.
  - The name `arc-mac-codecs` was free on crates.io on 2026-10-30.
  - Options as originally weighed:
  - Either: (a) a separate **LGPL-2.1-or-later** crate for the codecs (e.g. `arc-mac-codecs`), with MacPaw notices, while the independent container / writer / BinHex / MacBinary code goes MIT in `arc-mac`.
  - Or: (b) clean-room rewrites from format descriptions, done by someone who hasn't been working from the XAD sources.
  - Recommendation: (a). It's honest about the origin, LGPL works with both MIT and AGPL users, and it costs nothing now.
- **D2–D4 DECIDED 2026-10-30: the licence follows the code; no rewrites now.** Code moves as-is, and each crate's `license` field and notices describe what it actually contains. Where only part of a family is derived, that part goes into a small sub-crate under the upstream licence, the way `arc-mac-codecs` does for D1, so the rest of the family stays MIT. Rewrites to MIT are optional follow-ups.
  - **D2 (Linux / hfsutils):**
    - `adfs` → its own crate, GPL-2.0-only.
    - `hpfs` → its own crate, GPL-2.0-only, since the derived dnode editing and the read side share one file.
    - `hfs_unicode` → a sub-crate, GPL-2.0-only (e.g. `fs-mac-unicode`).
    - `HFS_CHARORDER` → GPL-2.0-or-later. Settle the exact split when `fs-mac` moves: move the table into a sub-crate, or license the classic-HFS part accordingly.
    - XFS is already planned as `fs-xfs` (GPL-2.0).
    - The GPL-2.0-only vs AGPL-3.0 question for the app is unchanged by this. It needs separate legal advice.
  - **D3 (PFS3):** `fs-amiga` carries pfs3aio's full 4-clause BSD notice in `THIRD_PARTY_NOTICES.md`, including the advertising acknowledgement ("This product includes software developed by Michiel Pelt"). Crate licence `MIT AND BSD-4-Clause`. No code change.
  - **D4 (NTFS blobs):** keep `secure_sds.bin` / `root_secdesc.bin` as-is. Record their origin (captured from `mkntfs` output) in `fs-pc`'s notices. Regenerating them from a Windows volume is a follow-up.
- **Original D2 analysis.** Linux / hfsutils-derived code in the Mac and Acorn/OS-2 filesystems: `hfs_unicode` (tables + lookup), `HFS_CHARORDER`, `adfs` map walk, `hpfs` dnode editing, `xfs` `dir_hashname`.
  - To make `fs-mac`, `fs-pc` and `fs-xfs` MIT, these need re-sourcing (TN1150, Inside Macintosh) or clean-room rewrites. Each is bounded: two tables, one map walker, one B-tree editor, one hash.
  - **Independent of the crates:** the Linux pieces are GPL-2.0-*only*. The app is AGPL-3.0, and GPL-2.0-only code generally can't be combined into GPL-3 / AGPL-3 works. So this may be a pre-existing compatibility question for the app itself, which re-sourcing would also settle.
  - (hfsutils is GPL-2.0-*or-later*, so `HFS_CHARORDER` is fine for the AGPL app but not for MIT.)
- **D3: PFS3.** Ship pfs3aio's 4-clause BSD notice, which carries an advertising clause that is GPL-incompatible, or recast the citations as format facts, since the audit found reimplemented behaviour, not translated code.
- **D4: NTFS `$Secure` blobs.** Regenerate from a Windows-formatted volume.

## Mechanical follow-ups (no decision needed; do with each module's move)

- Comment re-citations: `hfsplus_journal.rs` (TN1150), `xfs/*` ("behavioural reference"), `affs_common.rs:7`, `sfs.rs:459` (misattribution), `sfs_fsck.rs:3`, `qdos.rs`, `partition/x68k.rs` (licence label), `efs.rs`.
- A repo-level `THIRD_PARTY_NOTICES.md` for the app: Nyarime MIT (GHO Fast-LZ) now, plus MAME BSD-3 (mirrors `fs-8bit`'s file).
- Still to compare: `jfs.rs` / `reiserfs.rs` / `ufs.rs` against partimage 0.6.9, and `mar.rs` against mar-0.2.0. Both sources need fetching.
