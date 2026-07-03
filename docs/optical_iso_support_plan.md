# Optical ISO Support Plan — WinWorld problematic-ISO corpus

Tracking doc for making rusty-backup open every disc in
[`problematic_isos.json`](../problematic_isos.json) (114 discs).

**Status: 109 / 114 open (Phases 1 + 2 + 3 + 4 landed); 5 need work.**

Progress log:
- **Phase 0 done** — path override active in `Cargo.toml`; rb-cli builds against
  local `../opticaldiscs-rs`.
- **Phase 1 done** — raw-2352 autodetect in `IsoSectorReader` (opticaldiscs
  `sector_reader.rs`); +6 discs (Photostyler, Fallout 2, Lindows, nebula,
  sunos_4.1.4, RESKIT2000). 3 unit tests added; no regressions. Only
  `AdobePageMill.iso` remains in the raw bucket — anomalous dump (raw sync on
  sector 0 only) wrapping an APM/HFS volume; deferred to HFS handling.
- **Phase 4 done** — High Sierra Format. `PrimaryVolumeDescriptor::parse`
  detects the `CDROM` id at byte 9 and reads HSF field offsets (root record
  @180); the shared browser threads a `high_sierra` flag so directory records
  read file-flags at offset 24 (not 25). New `FilesystemType::HighSierra`.
  +9 discs (bkshlf87, MSPL10, both Programmer's Library, wordbkshlf, 3× OS/2
  Developer Connection, k-nt1091). Synthetic end-to-end unit test; no regressions.
- **Phase 2 done** — BSD/Sun UFS. New `opticaldiscs/src/browse/ufs.rs`
  (`UfsFilesystem`): UFS1 superblock (endianness auto-detected via magic),
  cylinder-group inode math (`cgimin`), direct + single/double/triple-indirect
  block reading, and the pre-4.4 (OFSFMT) directory format (u16 namlen, no
  d_type). New `FilesystemType::Ufs`. All 13 Digital UNIX / Tru64 discs browse;
  a 5.5 MB file (indirect blocks) extracts **byte-identical** to an independent
  reference decoder (sha256 match). 4 unit tests; full 114-scan = 100 OK / 14
  FAIL, no regressions.
  > Known follow-up (rusty-backup, not the reader): `rb-cli optical extract` of a
  > **case-sensitive** volume (UFS/EFS/Rock Ridge) onto a **case-insensitive**
  > host (macOS APFS) aborts on the first name that collides only by case
  > ("Is a directory", os error 21). Pre-existing; the extractor should skip /
  > rename-and-continue rather than bail. Browsing and single-file reads are fine.
- **Phase 3 done** — NeXT / OpenStep / Rhapsody. Same `UfsFilesystem`, generalised
  with a `base_offset`: a NeXT `dlV` disk label wraps one or more FFS partitions,
  so the reader scans block-aligned offsets for UFS1 superblocks and picks the
  partition whose root inode has the most entries (NeXTSTEP has one; Rhapsody has
  a small boot volume + the real root at base 655360). NeXT keeps **big-endian**
  on-disk FFS even on Intel (Rhapsody is little-endian) — auto-detected. Also
  added special-inode handling: device/FIFO/socket inodes surface as empty files
  and are never block-read (fixes an extract-time read-past-EOF on `/dev`). All 9
  discs browse; `/bin/gdb` (indirect blocks, big-endian) extracts byte-identical
  (sha256) to a reference decoder. 109/114, no regressions.

> The `reason` / `pycdlib_errors` fields in `problematic_isos.json` come from a
> Python cataloguing tool (`pycdlib`), **not** from rusty-backup. pycdlib is a
> strict ISO9660 validator; rusty-backup's optical reader (the `opticaldiscs`
> crate) is deliberately lenient. So a pycdlib error tells us *nothing* about
> whether rusty-backup can read the disc. Every row below was re-tested with
> `rb-cli optical browse` against the actual bytes — that is the source of truth.

---

## How the optical path works (architecture)

- All optical filesystem parsing is delegated to the external **`opticaldiscs`**
  crate (v0.6, `Cargo.toml:133`). rusty-backup's own rich `src/fs/` parsers
  (`ufs.rs`, `efs.rs`, `hfs.rs`, `hfsplus.rs`, …) operate on **disk-image
  partitions and are never invoked for optical images.**
- Entry point: `src/fs/optical_fs.rs` → `DiscImageInfo::open` +
  `open_disc_filesystem`. On failure it returns
  `FilesystemError::Parse/Unsupported` with **no fallback** — nothing else is
  tried. All 42 failures surface as `error: opening disc filesystem:
  Unsupported filesystem`.
- `opticaldiscs` **v0.6.0** parses: **ISO9660 (lenient — reads LE fields only,
  no endian cross-check, no set-terminator requirement, no file-structure-version
  check), Joliet (UCS-2 SVD — `iso9660.rs:320`+), Rock Ridge/SUSP
  (`browse/rockridge.rs`, incl. symlinks), HFS, HFS+, and SGI EFS.** Our branch
  has since added **High Sierra** (Phase 4), **raw-2352 autodetect** in a bare
  `.iso` (Phase 1), **UFS1** (Phase 2), and **NeXT** (Phase 3, UFS1 in a
  `dlV` disk label). Still NOT parsed: UDF (enum only), VMS ODS-2, XFS. Everything is normalised to 2048-byte cooked sectors.
  > NOTE: an earlier draft of this plan (based on the cached 0.5.0 crate source)
  > wrongly listed Joliet and Rock Ridge as unsupported. The linked build is
  > 0.6.0, which has both — verified empirically (see Hybrid discs below).

### Strategy: all parsing lives in opticaldiscs-rs; develop via a local path override

**Architectural rule:** every disc always routes through `opticaldiscs`.
rusty-backup never grows a second, parallel optical-parsing path — no fallback
bridge, no `src/fs/`-based optical reader. `src/fs/optical_fs.rs` stays exactly
as it is: it calls `DiscImageInfo::open` + `open_disc_filesystem` and nothing else.

**How we develop without a fallback:** point rusty-backup's `opticaldiscs`
dependency at a **local checkout of `../opticaldiscs-rs`** via a Cargo path
override. All new parsers (UFS, NeXT, High Sierra, VMS ODS-2, raw-2352 autodetect)
are written *directly in that local crate* from day one. `cargo build` in
rusty-backup recompiles the local crate on every edit, so iteration is as fast as
editing rusty-backup itself — but the code lands in its permanent home
immediately, so there is **no port step and nothing to revert in-code**.

```toml
# rusty-backup/Cargo.toml — dev only
[patch.crates-io]
opticaldiscs = { path = "../opticaldiscs-rs" }
```

Shipping is a config change, not a code move: publish the new `opticaldiscs`
version, delete the `[patch.crates-io]` block, and bump the version in
`[dependencies]`. That single revert is the *entire* rollback surface.

| Work item | Where it's written | Notes |
|---|---|---|
| Raw-2352 auto-detect | `opticaldiscs` `sector_reader.rs` / `detect.rs` | Detect the `00 FF…FF 00` sync header in a bare `.iso`; the crate's own ISO/Joliet/RR reader then handles the 6 discs. No cook-and-re-enter hack — the crate owns both layers. |
| High Sierra Format | `opticaldiscs` (sibling of `iso9660.rs`) | Recognise `CDROM` sig + HSF descriptor/dir-record offsets. |
| BSD/Sun UFS | `opticaldiscs` `browse/ufs.rs` (new) | Port logic from rusty-backup's `src/fs/ufs.rs` as a reference; handle LE (Tru64/Alpha) + BE (SPARC) + sb location. |
| NeXT / OpenStep / Rhapsody | `opticaldiscs` `browse/next.rs` (new) | FFS variant + `dlV3` disklabel; shares the UFS core. |
| VMS ODS-2 | `opticaldiscs` `browse/ods2.rs` (new) | ODS-2 home block + `INDEXF.SYS`. |
| ~~Joliet / Rock Ridge~~ | already in opticaldiscs 0.6 | **no work** |

> rusty-backup's own `src/fs/ufs.rs` / `efs.rs` are for **disk-image partitions**
> and stay put — they are a *reference* for the opticaldiscs implementations, not
> the optical code path. If drift between the two UFS readers becomes annoying,
> the decision is whether to extract a shared lower crate; default is to keep them
> separate (see Phase 7 note).

---

## Hybrid discs

A hybrid disc carries **two or more filesystems on the same media**. This corpus
has three kinds; all but one are already handled:

| Hybrid kind | In corpus | Status |
|---|---|---|
| **ISO9660 + Joliet** (Unicode long names) | 11 | **Handled.** opticaldiscs 0.6 finds the Joliet SVD and browses its UCS-2 tree. Verified: the Whistler build `usa_2276` shows `i386/asms/1000/gdiplus/gdiplus.dll` (5781/5787 lines mixed-case) rather than 8.3 `GDIPLUS.DLL` — proof the Joliet tree, not the bare PVD, is being read. Others: Creative Writer, Photoshop 5, Fallout 2 (also raw-2352 → Phase 1), RESKIT2000 (raw-2352 → Phase 1), NT 3.5 Daytona, Win2000 RC2, SCO OpenServer 5. |
| **ISO9660 + Rock Ridge** (Unix long names, perms, symlinks) | ~6 | **Handled.** `browse/rockridge.rs`. Verified: Solaris 2.4 browses 14 227 long lowercase names **and 1 816 symlinks** (`->`). Others: Solaris 2.5.1, SCO OpenServer 5.0.6, Lindows 4.0.302, Access 97. |
| **ISO9660 + HFS** ("Mac/PC" dual-fork hybrid) | **0** | Not present. Scanning all 114 for a coexisting Apple Partition Map / HFS *and* a sector-16 `CD001` found none. The 6 Apple discs are **HFS-only** (Mac CDs), which already browse (with resource forks + type/creator). So there is no dual-filesystem disc where we silently expose one side and drop the other. |

Notes / caveats:
- **High Sierra is not a hybrid** — it is a standalone pre-ISO9660 format
  (Phase 4, now done). One disc (`k-nt1091`) carries both an HSF descriptor and a
  stray `CD001` marker; its primary is HSF and it now browses via the HSF path.
- **Joliet-vs-primary preference:** when both trees exist opticaldiscs presents
  the Joliet tree. That is the right default for name fidelity, but it means the
  DOS-side 8.3 tree is not separately exposed. Only relevant if a disc has
  files present in the primary tree but absent from Joliet (not observed here).
- **El Torito boot images** (Win2000/OS2/NT install CDs are bootable) are a
  different "hybrid" axis — the *data* filesystem reads fine; extracting the
  embedded boot floppy/emulation image is a separate feature, out of scope for
  this corpus's read/extract goal. Flagged, not planned.

---

## Phases (in recommended order)

Every phase is written **in the local `../opticaldiscs-rs` checkout** (reached via
the `[patch.crates-io]` override) and validated end-to-end through
`rb-cli optical browse`. rusty-backup source is not touched. Phase 7 is the ship
step — publishing the crate and removing the override.

### Phase 0 — Development setup (config only, no code)
- [ ] Clone the `opticaldiscs-rs` source to `../opticaldiscs-rs`.
- [ ] Add `[patch.crates-io] opticaldiscs = { path = "../opticaldiscs-rs" }` to
      rusty-backup's `Cargo.toml`; confirm `cargo build --features optical` picks
      up the local crate (edit a string in the crate, rebuild, see it change).
- [ ] Confirm the baseline: re-run the 114-disc scan — 72 `[x]` / 42 `[ ]`
      unchanged — so later phases measure only our additions.
- **No rusty-backup code changes in this or any phase 1–6.** `optical_fs.rs`
      keeps calling `open_disc_filesystem` and nothing else.

### Phase 1 — Raw-2352 auto-detect (opticaldiscs-rs) — 6 discs, low effort ✅ DONE
- [x] `IsoSectorReader::new` (opticaldiscs `sector_reader.rs`) sniffs the
      `00 FF…FF 00` sync header and reads raw 2352-byte sectors transparently
      (Mode1 data@16, Mode2 data@24, picked from the sector mode byte); cooked
      `.iso` unchanged. Covers both the detect and browse construction sites.
- [x] All 6 open through the crate's existing ISO/Joliet/RR reader. 3 unit tests
      (`raw_mode1…`, `raw_mode2…`, `cooked_iso…`) + full crate suite green.
- [x] **Verified:** `rb-cli optical browse RESKIT2000.ISO` lists the WinNT RK
      tree; Fallout 2 / Lindows / nebula / Photostyler / sunos_4.1.4 all browse.
- Note: `AdobePageMill.iso` is *not* uniform raw-2352 (sync on sector 0 only,
  APM/HFS inside) — deferred to the HFS path, tracked in the ledger.

### Phase 2 — BSD/Sun UFS (opticaldiscs-rs) — 13 discs ✅ DONE
- [x] New `browse/ufs.rs` `UfsFilesystem`: UFS1 superblock (offset 8192,
      endianness auto-detected from magic), `cgimin` cylinder-group inode math,
      direct + single/double/triple-indirect block reading, OFSFMT directory
      parsing (u16 namlen / no d_type — the Tru64 case). New `FilesystemType::Ufs`;
      wired into `detect.rs` + `browse/mod.rs`. UFS2 detected but rejected (none
      in corpus). Symlink targets resolved (inline + block).
- [x] All 13 Digital UNIX / Tru64 discs browse. (SunOS/Solaris CDs turned out to
      be ISO9660+Rock Ridge, already handled — the UFS bucket is all Tru64, LE.)
- [x] **Verified byte-exact:** a 5.5 MB file with indirect blocks (`DIABASE220`)
      extracts to the same sha256 as an independent reference decoder. 4 unit
      tests (dirent parse LE/BE/new-format, `cgimin`). 100/114, no regressions.
- Follow-up (rusty-backup extractor, not the reader): case-collision on
      case-insensitive hosts — see the progress-log note above.

### Phase 3 — NeXT / NeXTSTEP / OpenStep / Rhapsody (opticaldiscs-rs) — 9 discs ✅ DONE
- [x] Extended `UfsFilesystem` with a `base_offset` instead of a separate module:
      detect the `dlV` label, scan block-aligned offsets for UFS1 superblocks, and
      pick the partition whose root inode has the most entries (handles NeXTSTEP's
      single partition and Rhapsody's boot-vol + real-root-at-655360 split).
- [x] Endianness auto-detected: NeXTSTEP/OpenStep FFS is **big-endian even on
      Intel**; Rhapsody is little-endian. Both the `8000…`-header discs
      (`nextstep33_risc`, `Openstep-…-User`, label at block 4) browse.
- [x] Special-inode handling (device/FIFO/socket → empty file, never block-read)
      fixes an extract-time read-past-EOF on `/dev`.
- [x] **Verified:** all 9 browse; `/bin/gdb` (indirect blocks, big-endian)
      extracts byte-identical (sha256) to a reference decoder; full NeXT disc
      extracts cleanly (4793 files). 109/114, no regressions.

### Phase 4 — High Sierra Format (opticaldiscs-rs) — 9 discs ✅ DONE
- [x] `PrimaryVolumeDescriptor::parse` detects `CDROM` at byte 9 →
      `parse_high_sierra` reads HSF offsets (volume id @48, root record @180);
      new `high_sierra` flag on the PVD. New `FilesystemType::HighSierra`
      (browsable), dispatched to the ISO9660 browser in `browse/mod.rs`.
- [x] `DirectoryRecord::parse` takes `high_sierra`: file-flags at offset 24 and
      the 6-byte HSF recording date (ISO9660 uses 25 / 7 bytes). Threaded through
      `Iso9660Filesystem` + `detect_rock_ridge_root`.
- [x] Synthetic `high_sierra_end_to_end` unit test (detection, dir/file
      classification via flags@24, root@180, file read); 150 crate tests green.
- [x] **Verified:** all 9 browse (bkshlf87, MSPL10, both Programmer's Library,
      wordbkshlf, 3× OS/2 Developer Connection, k-nt1091). 87/114, no regressions.

### Phase 5 — VMS ODS-2 / Files-11 (opticaldiscs-rs) — 2 discs
- [ ] New `opticaldiscs` `browse/ods2.rs`: ODS-2 home block + file headers
      (`INDEXF.SYS`). Lower priority (2 discs), but self-contained.
- **Verify:** browse `OpenVMS552.iso` (VAX) — note VMS versioned filenames (`;N`).

### Phase 6 — Long tail / investigate — 2 discs
- [ ] `disk01.iso` — first bytes `SCO CD-ROM/TAPE`; SCO custom cdrom/tape label
      (likely cpio/tar payload). Investigate; may be out of scope.
- [ ] `Banyan VINES 8.50.iso` — no recognisable signature (all-zero head);
      needs manual inspection.

### Phase 7 — Ship (publish + remove the override)
No code moves — the parsers were written in `opticaldiscs-rs` all along. This is
the only "revert" in the whole plan.
- [ ] Publish the new `opticaldiscs` version to crates.io.
- [ ] Delete the `[patch.crates-io]` block from rusty-backup's `Cargo.toml` and
      bump the version in `[dependencies]` to the published one.
- [ ] Re-run the full 114-disc scan against the published crate; the ledger below
      should be all `[x]`.
- **Optional, decide later:** if rusty-backup's disk-image `src/fs/ufs.rs` and the
      new `opticaldiscs` UFS reader drift annoyingly, extract a shared lower crate
      both depend on. Default: leave them separate.

---

## Regular filesystems on optical media (coverage expansion)

Separate from the WinWorld corpus: make the mainstream filesystems rusty-backup
already reads on **disk images** also readable when they ride on **optical
media**. A "regular" filesystem reaches a disc three ways, and opticaldiscs 0.6
handles **none** of them (verified: no UDF parser — enum only; no El Torito; no
non-ISO content detection). Each mechanism is a distinct capability, written in
`opticaldiscs-rs` like the rest:

| # | Mechanism | What it is | New capability needed |
|---|---|---|---|
| **M1** | **UDF** (disc-native) | Every DVD/BD, packet-written CD-RW, DVD-RAM. Often a UDF/ISO9660 "bridge" disc carrying both. | Full UDF reader: AVDP → LVD/partition → FSD → FE/ICB, long/short/ext allocation descriptors, Unicode (dstring) names, symlinks. Revisions 1.02–2.60. |
| **M2** | **El Torito boot image** | A floppy/HDD image embedded in a bootable ISO's boot catalog — usually FAT12 (floppy emu) or FAT16/NTFS (HDD emu). | Boot Record VD → boot catalog parse → extract the emulated image extent → run it through the FAT/NTFS/ext reader as a nested volume. |
| **M3** | **Superfloppy / raw-image disc** | A disc whose bytes are just a filesystem (FAT/NTFS/ext/HFS…) with no ISO9660 — e.g. a raw image saved with an `.iso`/`.img` extension, DVD-RAM formatted FAT32, MRW. | A content-detection fallback in `detect.rs` (mirror of rusty-backup's superfloppy auto-detect) that probes sector 0 for FAT/NTFS/ext/HFS/exFAT when no CD001/UDF/HFS-APM is found. Then dispatch to the matching reader. |

Scope note — the **vintage floppy filesystems** rusty-backup supports (CBM, Atari
DOS, OS-9, DragonDOS, Amiga AFFS/PFS3/SFS, ProDOS, QDOS, Human68k, Alto, Pilot,
Lisa, MFS, ANDOS, RS-DOS, Acorn DFS/ADFS, CP/M) essentially **never occur on
optical media**. They are out of scope for optical *fixtures*. If one ever shows
up as a raw image with a disc extension, M3's superfloppy fallback routes it
through the existing detector — so one generic superfloppy fixture proves the
mechanism; no per-format optical fixture is needed.

### Phase 8 — UDF reader (opticaldiscs-rs) — M1
- [ ] AVDP@256 (and 512/N-256 fallbacks) → VDS → LVD + partition maps → FSD →
      root FE; directory + file ICBs; short/long/extended allocation descriptors;
      dstring (OSTA CS0) name decode; symlinks; UDF revisions 1.02–2.60; UDF/ISO
      bridge preference (present UDF when both exist).
- **Verify:** browse a DVD-Video ISO and a `mkudffs` image at each revision.

### Phase 9 — El Torito embedded boot images (opticaldiscs-rs) — M2
- [ ] Boot Record VD (sector 17) → boot catalog → for each floppy/HDD-emulation
      entry, expose the emulated image as a nested volume and dispatch to the
      FAT/NTFS/ext reader. (No-emulation entries carry raw code, not a FS — list
      but don't try to mount.)
- **Verify:** extract files from the FAT12 floppy image inside a Win98 boot CD.

### Phase 10 — Superfloppy fallback on optical (opticaldiscs-rs) — M3
- [ ] When no CD001 / UDF anchor / HFS-APM is found, probe sector 0 for
      FAT/exFAT/NTFS/ext (and the HFS MDB) and dispatch. Reuse the detection
      constants rusty-backup already has in `src/fs/`.
- **Verify:** browse a raw FAT32 image renamed `*.iso` and an ext2 superfloppy.

---

## Fixture manifest (regular-filesystems-on-optical)

Status: **manifest only — nothing generated yet** (awaiting go-ahead). Committed
fixtures stay small (few hundred KB–few MB; the repo already shrinks oversized
test images, e.g. `f856c4a`); large real discs are for *manual* validation only.

Layout: `tests/fixtures/optical/<mechanism>/<name>.<ext>`, each mechanism folder
with a `README` recording the recipe + what it exercises.

Tooling confirmed available: **m900** (Ubuntu 24.04, passwordless SSH) has
`mkudffs`/`xorriso`/`genisoimage`, `mkfs.vfat`/`mkfs.ntfs`/`mkfs.ext2`, and the
**populate-without-mount** tools `mcopy`/`mmd` (FAT), `ntfscp` (NTFS), `debugfs`
(ext). macOS has `hdiutil makehybrid`. m900 `sudo` still needs a password, so
recipes below avoid loop-mounts except where noted (pure-UDF populate).

### Real discs located (copy when ready)
| Fixture | Source path | Exercises | Commit? |
|---|---|---|---|
| `m2/eltorito-fat12-floppy.iso` | `/Volumes/Software/USBODE-backup/Operating Systems (PC)/DOS622.iso` (~2 MB) | El Torito **1.44 MB FAT12 floppy emulation** (M2) — the real one, small enough to commit | **Yes** (small) |
| _manual_ Win98 boot CD | `…/winworld-pc/extracted/windows-98/windows-98/Microsoft Windows 98 First Edition.7z/Windows 98 First Edition.iso` (654 MB) | Real Win98 El Torito 1.44 MB FAT12 floppy @ LBA 21 (verified: media 0xF0, `FAT12`, IO.SYS/MSDOS.SYS/ASPI2DOS.SYS) — realistic M2 extract target | No — manual |
| _manual_ WinMe boot CD | `…/winworld-pc/extracted/windows-me/final/…(OEM Full).7z/Windows Me (115 - OEM Full).iso` (523 MB) | Same, WinMe. (Note: the **Retail Full** Win98/95 editions are non-bootable data CDs — no El Torito.) | No — manual |
| `m3/fat-superfloppy.img` | `/Volumes/Software/Old DOS and Windows Stuff/memtest1-0.img` (~1.4 MB) | FAT superfloppy content-detect (M3) | **Yes** (small) |
| _manual_ UDF/ISO bridge | `/Volumes/Software/USBODE-backup/DVDs/FANTASIA_RESTORED.ISO` (4.5 GB) | UDF+ISO bridge preference (M1) | No — manual |
| _manual_ pure UDF 2.x | `/Volumes/Media/Star Trek VI…1080p.iso` (49 GB) | Blu-ray pure UDF (NSR03) | No — manual |
| _manual_ HFS+ | `…/USBODE-backup/Mac Restore Disks/Mac OS 9.1.iso` (670 MB, APM) | HFS+ still wins on optical | No — manual |
| _manual_ HFS | `…/Mac Restore Disks/96073-016A…System-7-5-3…iso` (270 MB, APM) | classic HFS regression | No — manual |

> Note found while probing: most bootable discs here (Win98SE, dos71cd, Hiren's,
> Win2000, Fedora, Debian, Macrium, pebuilder-exfat) are El Torito **no-emulation**
> — the boot entry is raw isolinux/bootmgr code, *not* a FAT filesystem — so they
> exercise only boot-catalog *listing*, not FAT-in-boot-image reading. `DOS622.iso`
> (2 MB) plus the Win98 FE / WinMe OEM boot CDs (above) are the real floppy-
> emulation discs; Win95 RTM/OSR1 and the Win98/95 **Retail Full** editions are
> non-bootable data CDs (no El Torito).

### Synthetic fixtures — exact recipes (run on m900 unless noted)

**M3 — FAT/NTFS/ext superfloppies (no sudo):**
```sh
# FAT32
dd if=/dev/zero of=fat32.img bs=1M count=16
mkfs.vfat -F 32 -n FAT32TEST fat32.img
mmd  -i fat32.img ::/SUB ; echo hi > f.txt ; mcopy -i fat32.img f.txt ::/SUB/HELLO.TXT
# NTFS
dd if=/dev/zero of=ntfs.img bs=1M count=16 ; mkfs.ntfs -F -s -L NTFSTEST ntfs.img
ntfscp ntfs.img f.txt /HELLO.TXT
# ext2
dd if=/dev/zero of=ext2.img bs=1M count=8 ; mkfs.ext2 -F -L EXT2TEST ext2.img
debugfs -w -R "write f.txt HELLO.TXT" ext2.img
# then present each as a disc: copy to *.iso (M3 detects content, ignores extension)
```

**M2 — El Torito FAT12 floppy emulation (no sudo):**
```sh
dd if=/dev/zero of=floppy.img bs=1024 count=1440 ; mkfs.vfat -F 12 floppy.img
mmd -i floppy.img ::/ ; mcopy -i floppy.img f.txt ::/BOOT.TXT
mkdir -p src ; echo iso-side > src/README.TXT
genisoimage -b floppy.img -c boot.cat -o eltorito.iso -R -J floppy.img src/
# (also keep the real DOS622.iso as the authoritative M2 fixture)
```

**M1 — UDF/ISO bridge, populated (no sudo):**
```sh
mkdir -p src/dir ; echo hi > src/readme.txt ; printf 'nested' > src/dir/deep.txt
xorriso -as mkisofs -udf -V BRIDGE -o udf-iso-bridge.iso src/   # ISO9660 + UDF
```

**M1 — pure UDF at specific revisions (populate needs one sudo loop-mount):**
```sh
mkudffs --udfrev=0x0102 --blocksize=2048 --vid=UDF102 udf102.img 8192   # empty fs
mkudffs --udfrev=0x0201 --blocksize=2048 --vid=UDF201 udf201.img 8192
mkudffs --udfrev=0x0250 --blocksize=2048 --vid=UDF250 udf250.img 8192   # BD: metadata partition
# empty images already exercise the AVDP→LVD→partition→FSD parse at each revision.
# to add files: sudo mount -o loop,rw udfNNN.img /mnt && cp ... && sudo umount /mnt
```
- [ ] **UDF Unicode + symlink** — on the mounted UDF above, add a non-ASCII name and `ln -s` (dstring decode + symlink path).

### Fixture checklist (create + wire a test per row)
- [ ] `m1/udf102.img`, `m1/udf201.img`, `m1/udf250.img` (pure UDF, revision parse)
- [ ] `m1/udf-iso-bridge.iso` (bridge preference)
- [ ] `m1/udf-unicode-symlink.img` (dstring + symlink)
- [ ] `m2/eltorito-fat12-floppy.iso` (copy DOS622.iso) + `m2/eltorito-fat12-synth.iso`
- [ ] `m3/fat32-superfloppy.iso`, `m3/ntfs-superfloppy.iso`, `m3/ext2-superfloppy.iso`
- [ ] `m3/fat-superfloppy.img` (copy memtest1-0.img) — generic superfloppy proof
- [ ] `regression/hfsplus.*`, `regression/hfs.*` (shrunk Mac disc or `hdiutil` synth)
- [ ] _deferred:_ exFAT (m900 has no `mkfs.exfat`; skip unless M3 exFAT is scoped)

Per the doc-sync checklist, any new picker extension (e.g. `.udf`) lands in
`DISK_IMAGE_EXTS` with a regression test when the reader ships.

---

## Notes / non-goals surfaced by the analysis

- **XFS is NOT needed for this corpus.** All 20 IRIX discs read via the existing
  EFS parser. (Still worth a spot-check that large-file extraction from the
  6.5.x overlay CDs is byte-correct — flag only, no driver needed.)
- The 46 ISO9660 discs and 6 HFS discs need **zero** work — they already open;
  they were only in the list because pycdlib is stricter than rusty-backup.
- Joliet and Rock Ridge are already handled by opticaldiscs 0.6, so the hybrid
  discs get full-fidelity names (Unicode / long Unix) and symlinks today — see
  the Hybrid discs section.
- **Doc-sync reminder** (per CLAUDE.md pre-commit checklist): when a phase lands,
  update README (Image-formats + Filesystems + MiSTer-cores tables),
  `docs/full_MiSTer_support_status.md`, and — if a new picker-visible extension
  appears — `DISK_IMAGE_EXTS` in `src/model/file_types.rs` with its regression test.

---

## Per-disc ledger (114 discs)

`[x]` = opens today via `rb-cli optical browse`; `[ ]` = fails (`Unsupported filesystem`).

### ISO9660 — 46 discs (OK 46 / FAIL 0)
- [x] 5.00.1515.1_x86fre_Workstation_en-us-NTWKS50A.iso
- [x] AMIPRO31.iso
- [x] Access 97.ISO
- [x] Alpha Systems Firmware Update v3.6 (AG-PTMWT-BE)(Digital Equipment Corporation)(June 1996).iso
- [x] BORLANDC40.ISO
- [x] BORLANDC_45.ISO
- [x] EASYCHEF45.iso
- [x] Fallout.ISO
- [x] HOA_96.iso
- [x] IBM_OS2_Warp_4.iso
- [x] IBM_VisualAgeCPP_3.53FP3_Win32.iso
- [x] LS40ADVANCE.iso
- [x] LindowsOS_v.4.0.302.iso
- [x] MCP1_en_D1.iso
- [x] MSCW200__CD.iso
- [x] Mavis Beacon Teaches Typing 4.1.ISO
- [x] Microsoft 500 Nations.iso
- [x] Microsoft Ancient Lands.iso
- [x] Microsoft Office Developers Kit 1.0.iso
- [x] Microsoft Office Developers Kit 1.0A.iso
- [x] Microsoft Windows 2000 Professional (''NT 5.0'' 5.00.2128.1 RC2) [AXP].iso
- [x] Netware.5.1.ISO
- [x] Netware.6.0.ISO
- [x] OFFS95_US01.iso
- [x] PB_NAVIGATOR.iso
- [x] PSHOP5.ISO
- [x] SCO OPEN Server 5.0.6 Enterprise.iso
- [x] SCO_OPENSERVER_5.ISO
- [x] VB50ENT.ISO
- [x] Visual Basic 5 Pro.iso
- [x] W2PIS_EN.iso
- [x] WATCOM_C11B.iso
- [x] WSEB_CP2-BootCD.iso
- [x] daytona_756__x86fre.wks.iso
- [x] en_winnt_3.5.srv.iso
- [x] en_winnt_3.51.srv.iso
- [x] ibm_warpclient_cp2_v4_52_boot.iso
- [x] ibm_warpserver_cp2_v4_52_boot.iso
- [x] mcp2-refresh-boot-en.iso
- [x] os2warpcd2-boot.iso
- [x] pbnav39.ISO
- [x] solaris_2.4_sparc.iso
- [x] solaris_2.5.1_1197.iso
- [x] usa_1946_win2000.prol_beta3-rc0.iso
- [x] usa_2202__x86fre.pro_whistler.iso
- [x] usa_2276.1__x86fre.pro_whistler.iso

### SGI EFS (IRIX) — 20 discs (OK 20 / FAIL 0)
- [x] Compiler_Execution_Environment_7.3.iso
- [x] Developer_Tools_Maintenance_Release_7.2.1.3m.iso
- [x] IRIX 6.5.21 Overlay 1 of 4.iso
- [x] IRIX 6.5.21 Overlay 2 of 4.iso
- [x] IRIX 6.5.21 Overlay 3 of 4.iso
- [x] IRIX 6.5.21 Overlay 4 of 4.iso
- [x] IRIX 6.5.22 Applications November-2003.iso
- [x] IRIX 6.5.22 Overlay 1 of 3.iso
- [x] IRIX 6.5.22 Overlay 2 of 3.iso
- [x] IRIX 6.5.22 Overlay 3 of 3.iso
- [x] IRIX-6.5-Foundation1.iso
- [x] IRIX-6.5-Foundation2.iso
- [x] IRIX_6.5.9_Applications_August-2000.iso
- [x] IRIX_Development_Foundation_1.2.iso
- [x] MIPSpro_C++_Compiler_7.3.iso
- [x] ProDev-Developers-Suite-May-1999.iso
- [x] irix-6.5.9-1of3.iso
- [x] irix-6.5.9-2of3.iso
- [x] irix-6.5.9-3of3.iso
- [x] oncnfsv3.iso

### Apple HFS (APM) — 6 discs (OK 6 / FAIL 0)
- [x] Adobe Illustrator 5.5 Deluxe for Mac.iso
- [x] MSO421.ISO
- [x] Mozart Beta Seed.iso
- [x] Office421a.iso
- [x] The Microsoft Office.iso
- [x] oracle.iso

### High Sierra — 9 discs (OK 9 / FAIL 0)
- [x] MSPL10.iso
- [x] Microsoft Programming's Library.ver.Version 1.1a.English.iso
- [x] Microsoft-Programers-Library-v1.3.iso
- [x] The Developer Connection OS2 v8-1995 disk-1.iso
- [x] The Developer Connection OS2 v8-1995 disk-2.iso
- [x] The Developer Connection OS2 v8-1995 disk-3.iso
- [x] bkshlf87.iso
- [x] k-nt1091.iso
- [x] wordbkshlf.iso

### BSD/Sun UFS — 13 discs (OK 13 / FAIL 0)
- [x] DECevent Utility v2.2 for Digital UNIX (AG-QAA7C-RE)(Digital Equipment Corporation)(August 1996).iso
- [x] DIGITAL UNIX - V4.0B - Associated Products - Volume 2.iso
- [x] DIGITAL UNIX - V4.0B - Documentation.iso
- [x] DIGITAL UNIX - V4.0B - Operating System.iso
- [x] DIGITAL UNIX - V4.0D - Associated Products - Volume 1.iso
- [x] DIGITAL UNIX - V4.0D - Associated Products - Volume 2.iso
- [x] DIGITAL UNIX - V4.0D - Operating System - Volume 1.iso
- [x] DIGITAL UNIX - V4.0E - Operating System.iso
- [x] Digital UNIX 3.2B.iso
- [x] Digital UNIX v3.2C Online Documentation (AG-QDWBB-XE)(Digital Equipment Corporation)(August 1995).iso
- [x] Digital UNIX v3.2G (Includes v3.2C) (AG-PS3NR-XE)(Digital Equipment Corporation)(July 1996).iso
- [x] Digital UNIX v3.2G Complementary Products (Includes TruCluster Software) (AG-Q3JRG-XE)(Digital Equipment Corporation)(July 1996).iso
- [x] Disk01.iso

### NeXT — 10 discs (OK 10 / FAIL 0)
- [x] NEXTSTEP 3.2 (M68K)(x86).iso
- [x] NeXT Step 3.1 Intel dev.iso
- [x] NeXT Step 3.1 Intel.iso
- [x] Openstep-4.2-Intel-Developer.iso
- [x] Openstep-4.2-Intel-User.iso
- [x] Rhapsody Intel.iso
- [x] nebula.iso
- [x] nextstep33_risc.iso
- [x] nextstep_3.3_intel.iso
- [x] rhapsody_dr2_x86.iso

### Raw 2352 — 6 discs (OK 5 / FAIL 1)
- [ ] AdobePageMill.iso
- [x] Fallout 2.iso
- [x] Lindows_1.1.1.iso
- [x] Photostyler 1.1a SE.iso
- [x] RESKIT2000.ISO
- [x] sunos_4.1.4_install.iso

### VMS ODS-2 — 2 discs (OK 0 / FAIL 2)
- [ ] OpenVMS552.iso
- [ ] VMS 552h4 VAX.iso

### SCO tape/cdrom — 1 discs (OK 0 / FAIL 1)
- [ ] disk01.iso

### Unknown — 1 discs (OK 0 / FAIL 1)
- [ ] Banyan VINES 8.50.iso

