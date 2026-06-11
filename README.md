# Rusty Backup

Cross-platform GUI tool for backing up, restoring, and inspecting vintage
computer disk images. The name is a play on words — both referencing the Rust
programming language and the rusty vintage machines we're preserving.

Primary users are retro computing enthusiasts archiving CF/SD cards, IDE/SCSI
drives, and floppy images from DOS, Windows 9x, early Linux, classic Mac OS,
and Apple II / IIgs systems.

Licensed under **AGPL-3.0**. See `PROJECT-SPEC.md` for the full design
specification and `docs/` for deep-dive topics.

## Installation

Rusty Backup ships as a single self-contained binary per platform.

1. Grab the latest build for your OS from the
   [GitHub Releases page](https://github.com/danifunker/rusty-backup/releases).
2. Drop the binary where you want it:
   - **Windows** — run `Setup.exe` for the installed experience
     (Start-Menu shortcut, "Add/Remove Programs" entry, optional
     file-association registration, `rb-cli` on `PATH`), or extract the
     portable ZIP and run `rusty-backup.exe` directly. Either install
     can self-update in place from within the **About / Update** UI;
     existing portable-ZIP users can run `Setup.exe` once to gain the
     Start-Menu / ARP integration without re-downloading later updates.
   - **macOS** — open the DMG and drag `Rusty Backup.app` into `/Applications`.
   - **Linux** — `chmod +x rusty-backup-*.AppImage` and launch it.
3. Raw physical disks require elevated privileges (admin on Windows, root on
   Linux, an authorisation prompt on macOS). Working with image files on disk
   does not.

To build from source: `cargo build --release`. See `CLAUDE.md` for the full
build matrix.

### The `rb-cli` headless CLI

The release artifacts also include `rb-cli`, the scriptable counterpart to
the GUI. Same engine, no eframe dependency, designed for shell pipelines
and automated build farms.

```
rb-cli new disk.dsk --fs hfs --size 800K --name "My Disk"
rb-cli put disk.dsk ./Finder /System/Finder --type FNDR --creator MACS
rb-cli ls  disk.dsk /System
rb-cli fsck disk.dsk --checkonly
rb-cli inspect disk.hda
rb-cli backup /dev/disk3 ./backups --format chd --checksum sha256
rb-cli restore ./backups/my-backup ./restored.img
rb-cli batch script.json --dry-run
rb-cli new-x68k-hdd hdd.hdf --size 16M --system-disk human68k.dim
rb-cli new-x68k-hdd c.hdf --size 32M --variant scsi --system-disk human68k.dim \
                          --boot-sector-donor hd0.hds      # zero manual steps, your donor
rb-cli new-x68k-hdd c.hdf --size 32M --variant scsi --system-disk human68k.dim \
                          --builtin-boot-sector            # zero manual steps, no donor needed
```

Shell completions for bash / zsh / fish / PowerShell:

```
rb-cli install-completions          # auto-detects $SHELL
rb-cli completions zsh > _rb-cli    # emit-to-stdout for packagers
```

Full verb-by-verb reference: [`docs/cli-reference.md`](docs/cli-reference.md)
(regenerated from `cargo run --example generate_cli_reference`).
Open CLI follow-ups (and everything else still to do) are tracked in
[`docs/OPEN-WORK.md`](docs/OPEN-WORK.md).

#### `rb-cli-mini` for MiSTer FPGA (armv7)

`rb-cli-mini` is the **MiSTer-specific** build of `rb-cli`: a slim
variant cross-compiled for the FPGA's Intel Cyclone V / Cortex-A9 SoC
(`armv7-unknown-linux-gnueabihf`, glibc 2.31 baseline from the
Buildroot rootfs). It excludes the GUI (eframe / egui / rfd), the
optical-disc stack (opticaldiscs / cd-da-reader), and the update
checker's reqwest client — but **keeps CHD support** via the upstream
`libchdman-rs` armv7 prebuilt, so `.chd` images work inline on the
device.

The desktop release builds use the full feature set; only the MiSTer
artifact runs `--no-default-features --features chd`.

```
# Cross-compile for MiSTer (armv7-unknown-linux-gnueabihf):
cargo install cross --git https://github.com/cross-rs/cross --locked
cross build --bin rb-cli --release \
            --target armv7-unknown-linux-gnueabihf \
            --no-default-features --features chd

# Strip + deploy. The release tarball ships the binary as `rb-cli-mini`;
# do the local rename here too so the on-MiSTer filename matches the
# downloads-page artifact (and so completion lookups land in the right
# spot — see below).
arm-linux-gnueabihf-strip target/armv7-unknown-linux-gnueabihf/release/rb-cli
scp target/armv7-unknown-linux-gnueabihf/release/rb-cli \
    root@mister.local:/media/fat/Scripts/rb-cli-mini
```

The repo's [`Cross.toml`](Cross.toml) pins the cross-compile Docker
image to `cross-rs`'s Ubuntu 20.04 / GCC 9.4 / glibc 2.31 build (at a
verified SHA digest) so the binary links against the same glibc +
libstdc++ baseline as both the MiSTer Buildroot rootfs and the
upstream libchdman-rs armv7 prebuilt — symbols line up without any
version drift.

CI ships a prebuilt `rb-cli-mini-armv7-linux-<version>.tar.gz` as part
of every release; grab it from the
[Releases page](https://github.com/danifunker/rusty-backup/releases) if
you don't want to set up the cross toolchain locally.

What's in the MiSTer build:
- Every filesystem operation (`ls`, `put`, `get`, `rm`, `mkdir`,
  `fsck`, `resize`, `expand`, `chmeta`, `bless`, …) on FAT, NTFS,
  exFAT, HFS, HFS+, ext, AFFS, PFS3, SFS, ProDOS, Human68k, ADFS, etc.
- `inspect`, `backup`, `restore` for Raw, VHD, QCOW2, VMDK, Zstd, the
  four floppy container formats, **and CHD**.
- `floppy convert` (XDF / HDM / DIM / D88, single-file and bulk) —
  the X68000 workflow runs inline on the device.
- Partition table editing (`partmap`), backup-folder operations.
- `shrink`, `grow .chd`, single-file CHD backups — all work.

What's excluded (operations exit with a clear "this binary was built
without the `optical` feature" message):
- `optical` verb (rip / disc browse / extract via the OS optical-drive
  layer — the MiSTer has no optical drive attached).
- GUI windows and the update-checker self-replace UI (only meaningful
  for the desktop binary).

Full background and the feature matrix live in
[`docs/mister_cli.md`](docs/mister_cli.md).

## Usage

The app has five tabs:

- **Backup** — pick a source (physical device or image file), choose a
  destination folder, pick an output format and checksum type, and start.
  Each backup is written as a folder containing `metadata.json`, the partition
  table sidecar (`mbr.json` / `gpt.json` / `apm.json` / RDB / SGI), and
  either one compressed file per partition (Zstd / Raw / per-partition VHD)
  or a single `<backup-name>.chd` disk image that `chdman info` opens and
  MAME loads directly (CHD output). The single-file CHD also serves as the
  unit edit mode operates on, so changes flow back into a CHD that any
  CHD-aware tool can still read.
- **Restore** — pick a backup folder and a target device or image file.
  Partition sizes can be left at original, shrunk to the filesystem minimum,
  or set to a custom value; the filesystem is expanded in place when the
  restored partition is larger than the minimum. Alignment (DOS/cylinder,
  1 MB, or custom) is preserved from the source by default. A top-level
  "Add free space for in-OS expansion" toggle pads the target image so
  the guest OS can extend partitions there after boot — see the *Disk
  expansion workflow* section below.
- **Inspect** — pick any supported source and browse the partition table,
  filesystem info, and file listings. Several actions live here:
  - **Browse** filesystem contents read-only (per partition).
  - **Edit Partition Table…** to add / resize / delete / retype
    partition entries on raw disks, image files, and devices. Writers
    cover MBR, GPT, APM, SGI, and RDB-bootable-flag.
  - **Resize Partitions…** for in-place partition data moves with
    filesystem-side patching.
  - **Add Partition…** as a streamlined entry into the editor when
    trailing free space exists, pre-filled per partition-table type.
  - **Expand Image…** to grow a raw, VHD, or CHD image with trailing
    zero-padding so you have room for new partitions.
  - **Export Disk Image…** to write VHD (fixed or dynamic), QCOW2,
    VMDK, Raw, 2MG, WOZ, DC42, HFV, or CHD (whole-disk or
    per-partition) — see `docs/vhd-export.md`.
  - **Convert Floppy Container…** to convert between the four
    X68000 / PC-98 / FM-7 floppy wrappers (XDF, HDM, DIM, D88) one
    file at a time. Bulk folder conversion lives in the existing
    **Bulk Convert** dialog, which now lists the same four formats
    as output targets.
  - **Check** (`fsck`) on classic HFS, HFS+, AmigaDOS (Disk Validator),
    and SGI EFS, with **Repair** that uses replica blocks + lost+found
    where supported.
  - **Defragment…** on Human68k (X68000) partitions: repack the volume so
    files are stored contiguously, reclaiming holes left by deleted files
    (also `rb-cli repack`).
  - **Build a Sharp X68000 HDD from scratch** via the shell:
    `rb-cli new-x68k-hdd out.hdf --size 16M --system-disk donor.dim`
    (SASI default, `--variant scsi` available) — emits a self-bootable
    HDD with the Sharp IPL signature, X68K partition table, and a full
    Human68k system clone from a donor `.dim` / `.D88` / `.xdf` /
    `.hdm` floppy. Boots clean in MAME `x68000 -sasi` / `x68030 -hard`.
    Add `--boot-sector-donor hd0.hds` (the well-known 100 MB
    Sharp/Keisoku Giken SCSI HDD image, file size 104,857,600 bytes,
    widely mirrored on retro-archive sites under that exact filename)
    plus `--variant scsi` to overlay the donor's Sharp partition boot
    sector — the HDD then self-boots straight to `C:>` on every
    power-on with no manual `SWITCH.X` step. Any `--size` from 1 MiB
    to ~512 MiB works: the builder patches the donor's embedded BPB
    with the output partition's actual FAT geometry, so the donor
    boots from your sized HDD regardless of how the donor itself was
    sized.
  - **Edit mode** on FAT, NTFS, exFAT, ext, HFS, HFS+, AFFS, PFS3, SFS,
    ProDOS, Apple DOS 3.3, MacPlus MFS, EFS, UFS, CP/M (multi-DPB),
    Human68k, and XFS (v4 + v5): stage create-file / new-folder /
    drag-and-drop / delete edits, then Apply atomically with snapshot
    rollback on error.
- **Optical** — browse and extract files from CD/DVD images and physical
  optical drives. Supports ISO9660, Joliet, Rock Ridge, and HFS hybrid
  discs. Re-opens automatically when the underlying disc changes.
- **Archives** — browse and extract classic Macintosh archives. Auto-detects
  StuffIt 1-5 (`.sit`, `.sea` self-extracting), Compact Pro (`.cpt`), and
  BinHex (`.hqx`) wrappers around any of them. Pick an archive, browse the
  entry tree (name / type / creator / size / codec), tick the entries to
  keep, and extract to a folder in your choice of fork-preserving
  container — BinHex, MacBinary, AppleDouble, or raw data + `.rsrc`
  sidecar. Single-entry archives whose payload is itself a disk image
  (DiskCopy 4.2, raw HFS, raw HFS+) get a one-click "Mount in new
  Inspect tab" handoff. `rb-cli sit list` / `sit extract` is the
  scriptable counterpart.

Most popups (Resize Partitions, Edit Partition Table, Export Disk Image,
restore-tab partition list) use a shared **Size Mode** radio set
(Original / Minimum / Custom / Fill) and a **Current → After** disk-layout
bar pair so the planned outcome is always visible before you commit.

VHD export is available from the Inspect tab: produce either a whole-disk
`.vhd` (partition table plus all partitions with their gaps) or per-partition
`.vhd` files, ready to mount in VirtualBox, Hyper-V, or QEMU. See
`docs/vhd-export.md`.

### Disk expansion workflow

Rusty Backup can grow an existing image so its guest OS sees a bigger
disk. This is useful for any filesystem the OS can expand at runtime —
XFS being the motivating case (`xfs_growfs` can only ever grow up to the
partition boundary, never the disk boundary), but the same workflow
works for ext, NTFS, FAT, HFS+, exFAT, btrfs, etc.

**Open an existing image and add trailing free space:**

1. Open the image in the **Inspect** tab. Any backup, raw disk image,
   VHD, or CHD works.
2. Click **Expand Image…** in the toolbar. Enter how much MiB to add and
   click Expand. Raw/VHD images grow instantly via `set_len`; CHD images
   re-encode in a background worker (the CHD hunk layout is fixed at
   creation, so there's no in-place grow).
3. Click **Re-inspect** to refresh the partition list. The new trailing
   region appears as a gray "Free" segment in the **Disk layout** bar.
4. Either:
   - Click **Add Partition…** to allocate the free space as a new
     partition (defaults are pre-filled per partition-table type — XFS
     for SGI, `0x83` for MBR, Linux Filesystem GUID for GPT,
     `Apple_HFS` for APM), or
   - Click **Edit Partition Table…** and bump the last partition's size
     via the *Size Mode* radios.
5. Boot the guest OS and run the filesystem's native grow tool
   (`xfs_growfs /mountpoint`, `resize2fs`, Disk Management's Extend
   Volume, IRIX `fx` + `xfs_growfs`, …).

**Same workflow during a restore** (useful when the target physical
disk is larger than the source):

1. **Restore** tab → pick the backup and an image-file target. (The
   feature is disabled for device targets — physical disk size is
   fixed.)
2. Tick **Add free space for in-OS expansion** and enter MiB.
3. Pick a mode:
   - **Leave as unpartitioned free space** *(recommended)* — partition
     table stays unchanged; the guest OS uses its native partitioner
     plus `xfs_growfs` (or equivalent). Works for any filesystem on
     any OS.
   - **Extend last partition automatically** — the last partition is
     sized to absorb the new free space during restore. After restore,
     only the filesystem-side grow tool is needed.
4. The **Current** / **After** disk-layout bar pair shows the planned
   result before you commit.

**Note on visualisations:** the Disk layout bar appears in five places
(Inspect, Restore, Resize Partitions, Edit Partition Table, Export Disk
Image) so what you see is always what'll be written. Partition colours
cycle through a stable palette; tiny partitions (≤ ~1 MiB) get a
minimum-width pip so GPT/APM disks with many small partitions stay
readable.

## Compatibility

### Image / backup formats

| Format         | Extension       | Read as source | Write as backup | Notes |
|----------------|-----------------|----------------|-----------------|-------|
| Raw            | `.img`, `.raw`, `.hda` | Yes     | Yes             | Sparse zero-skipping; optional splitting |
| Fixed VHD      | `.vhd`          | Yes            | Yes             | 512-byte footer; also used for VHD export |
| Dynamic VHD    | `.vhd`          | Yes            | Yes             | Sparse, allocate-on-write |
| QCOW2 (QEMU)   | `.qcow2`        | Yes            | Yes (create / edit) | v2 + v3 |
| VMDK (VMware)  | `.vmdk`         | Yes            | Yes (create / edit) | Flat and monolithic-sparse |
| Zstd stream    | `.zst`          | Yes            | Yes             | Good general compression, splittable |
| CHD (MAME)     | `.chd`          | Yes            | Yes             | Native (MAME's CHD core is bundled — no external `chdman` needed) |
| Norton Ghost   | `.gho`, `.ghs`  | Yes            | No              | File-aware FAT/NTFS browse, sector + spanned sets, Ghost 7.5, password-protected images decrypted automatically |
| WinImage       | `.imz`          | Yes            | No              | Including password-protected archives |
| BasiliskII HFV | `.hfv`          | Yes            | Yes             | Flat classic-HFS volume (≤ 2047 MB) for 68k Mac emulators |
| Apple 2MG      | `.2mg`          | Yes            | No              | Apple II / IIgs disk images |
| Apple II DSK   | `.dsk`, `.do`, `.po` | Yes       | No              | DOS-order, ProDOS-order, and auto-detect sector orderings |
| Disk Copy 4.2  | `.dc42`, `.image` | Yes          | No              | Classic Mac floppy images |
| Apple DMG      | `.dmg`          | Yes (raw/UDRW) | No              | Uncompressed DMGs only |
| WOZ            | `.woz`          | Yes            | Yes (export)    | Apple II 5.25" and 3.5"; WOZ2 writer regenerates a clean image |
| Amiga ADF / HDF | `.adf`, `.hdf` | Yes            | Yes (raw)       | Floppy + hard-disk images. RDB partition tables parsed. Arculator-wrapped `.hdf` (Acorn) auto-detected. |
| Amiga gzipped  | `.adz`, `.hdz`  | Yes            | No              | Transparently decompressed to a temp file at open |
| Atari MSA      | `.msa`          | Yes            | No              | Magic Shadow Archiver — Atari ST 720K / 800K / 1.44MB floppy |
| CPCEMU DSK / EDSK | `.dsk`       | Yes            | No              | Amstrad CPC / PCW / Einstein / Oric CP/M floppies |
| Commodore disk | `.d64`, `.d71`, `.d81`, `.d80`, `.d82` | Yes  | Yes (in-place edit) | 1541 / 1571 / 1581 + PET 8050 / 8250 (IEEE-488) flat sector dumps for the C64 / C128 / C16 / VIC-20 / PET cores. Read/browse/extract + add/delete persist back into the image (bidirectionally cross-validated against the `c1541` / Python `d64` reference). |
| Commodore GCR  | `.g64`, `.g71`  | Yes (decode) | No        | Raw 1541 / 1571 GCR track images (preservation-grade). Decoded to flat sectors so the CBM engine can read them; the `.g71` side-1 mapping is validated against a real VICE `c1541` image. |
| Atari disk     | `.atr`, `.xfd`  | Yes            | Yes (in-place edit) | 8-bit Atari (400/800/XL/XE) disk images for the Atari800 core. `.atr` = 16-byte header + sector body; `.xfd` headerless. Read/browse/extract + add/delete on the Atari DOS 2 volume. |
| Sharp D88      | `.d88`          | Yes            | Yes (convert + in-place edit) | X68000 / PC-88 / PC-98 / MSX / FM-7 sparse track-table container. Add/delete/mkdir on the contained Human68k FAT volume persist back into the container (decode -> edit -> re-encode). |
| X68000 XDF     | `.xdf`          | Yes            | Yes (convert + in-place edit) | Raw headerless X68000 floppy dump; geometry inferred from size. In-place file add/delete/edit supported. |
| X68000 HDD     | `.hda`, `.hdf`, `.hds`, `.ima` | Yes | Yes (in-place edit + resize + defrag repack) | Sharp SASI/SCSI hard-disk images; X68k partition table + Human68k FAT12/16. Read/browse/extract + add/delete/mkdir + in-place FS grow/shrink + contiguous repack (SHARP/KG big-endian BPB & FAT). Geometry auto-detected: SCSI `X68SCSI1` (table @ 0x800, 1024-byte sectors) and SASI (table @ 0x400, 256-byte sectors, incl. custom-IPL game disks). |
| PC-98 HDM      | `.hdm`          | Yes            | Yes (convert + in-place edit) | DiskExplorer raw headerless floppy dump (byte-identical to XDF). In-place file add/delete/edit supported. |
| DiskExplorer DIM | `.dim`        | Yes            | Yes (convert + in-place edit, DIFC) | DIFC 256-byte header + payload; generic 256-byte-header fallback for IBM XDF DIM on read. Add/delete/edit persist back into the container. |
| Raw physical disk | —            | Yes            | Yes (restore target) | CF/SD/USB/HDD/SSD — see below |

"Yes (convert)" means the format isn't a backup wrapper but is fully
round-trippable via the **Convert Floppy Container…** dialog and
`rb-cli floppy convert` — useful for moving images between MiSTer cores,
real hardware utilities, and emulators that each prefer a different
floppy container.

### Filesystems

All listed filesystems support browsing in the **Inspect** tab and are
preserved intact on backup/restore. "Shrink" means the filesystem can be
safely compacted to its minimum size during backup and re-expanded during
restore or VHD export. "Edit" means create / delete / drag-and-drop via the
inspect-tab Edit Mode.

| Filesystem     | Browse | Edit | Shrink / expand | fsck | Notes |
|----------------|:------:|:----:|:---------------:|:----:|-------|
| FAT12          | Yes    | Yes  | Yes             | —    | Apple II SuperDrive, DOS floppies |
| FAT16          | Yes    | Yes  | Yes             | —    | DOS / Windows 3.x / 9x |
| FAT32          | Yes    | Yes  | Yes             | —    | Windows 95 OSR2+ through XP, vintage Linux |
| exFAT          | Yes    | Yes  | Yes             | —    | Modern removable media |
| NTFS           | Yes    | Yes  | Yes             | —    | Windows NT / 2000 / XP |
| ext2 / ext3 / ext4 | Yes | Yes | Yes             | —    | Early Linux installs onward |
| HFS (Mac OS Standard) | Yes | Yes | Yes         | Yes (check + repair: replica copy, bitmap fixup, lost+found for orphans) | Classic Mac OS 68k / early PowerPC. Includes block-size expansion via clone (`Expand HFS Volume…`). |
| HFS+ / HFSX    | Yes    | Yes  | Yes (defrag clone) | Yes (check + repair) | Mac OS Extended; hardlink resolution. |
| btrfs          | Yes    | No   | No              | —    | Modern Linux; read-only browse |
| ProDOS         | Yes    | Yes  | Yes             | —    | Apple II / IIgs |
| CBM DOS (1541 / 1571 / 1581 / 8050 / 8250) | Yes | Yes | — (floppy, fixed geometry) | — | Commodore C64 / C128 / C16 / VIC-20 / PET. PETSCII names, bit-set-is-free BAM, linked-sector files. `.d64` / `.d71` / `.d81` / `.d80` / `.d82`; `.g64` GCR decoded to sectors. |
| Atari DOS 2 (2.0S / 2.5) | Yes | Yes | — (floppy, fixed geometry) | — | Atari 8-bit (Atari800 core). VTOC@360 (bit-set-is-free), 64-file directory, linked-sector files. Single + enhanced density `.atr` / `.xfd`. |
| Human68k (FAT12 / FAT16) | Yes | Yes | Yes (HDD in-place grow + shrink, plus defragmenting repack) | — | Sharp X68000. SASI/SCSI hard disks use a Sharp/KG big-endian BPB + big-endian FAT; floppies use standard little-endian FAT. Shift-JIS 18.3 filenames. Shrink stays above the FAT16 floor. `rb-cli repack` / the Inspect-tab "Defragment…" button repack the volume contiguously, reclaiming holes left by deleted files. |
| AFFS (OFS / FFS)  | Yes | Yes | Yes (in-place; bm_pages only) | Yes (Amiga Disk Validator) | Amiga `DOS\0`..`DOS\7`. In-place resize relocates root + bitmap pages; refuses on bm_ext-chain volumes or when allocated data would be clobbered. |
| PFS3 / PDS3 / muFS | Yes | Yes | Yes (in-place + defragmenting clone) | —    | Amiga PFS3 family. Shrink refuses to truncate live data; clone path packs the volume for genuinely smaller targets. |
| SFS (Smart File System) | Yes | Yes (single-leaf btree) | Yes (in-place trim/grow) | —    | Amiga `SFS\0` / `SFS\2`. |
| SGI EFS        | Yes    | Yes  | Yes (in-place grow + conservative + aggressive shrink) | Yes (check + repair: replica copy, bitmap fixup, lost+found) | IRIX < 6.0. Aggressive shrink renumbers inodes into low CGs. |
| SGI XFS (v4 / v5) | Yes | Yes (v4 only; v5 editing pending) | Grow via "Add free space" + in-OS `xfs_growfs`; shrink via clone-into-fresh is planned (see [`docs/OPEN-WORK.md`](docs/OPEN-WORK.md) §2.2) | Yes (R1-R8 repair pipeline; v4 oracle-validated) | IRIX 6.x and Linux. `xfs_repair`-clean writes. |

### Partition tables

| Scheme | Parse | Edit (resize / add / delete / retype) | Notes |
|--------|:-----:|:-------------------------------------:|-------|
| MBR    | Yes   | Yes  | PC standard. Logical partitions inside an extended container are surfaced read-only. |
| GPT    | Yes   | Yes  | Primary + backup header rewritten with refreshed CRCs on every edit. |
| APM    | Yes   | Yes  | Apple Partition Map (68k / PowerPC Macs). |
| RDB    | Yes   | Bootable flag only | Amiga `RDSK`. Full RDB editing deferred until the DosEnv geometry story is settled. |
| SGI    | Yes   | Yes  | SGI Volume Header (IRIX). 16 fixed slots; checksum recomputed on every write. |
| None (superfloppy) | Yes (FAT / HFS / HFS+ at sector 0) | — | Standard floppy sizes are recognised even without a partition table. |

The Clonezilla image format is also parsed as a source (MBR, GPT, partclone
images, partition table sidecars) for restore — see `docs/clonezilla.md`.

### What works well vs. what to watch out for

- **FAT12 / FAT16 / FAT32 → VHD** is the best-exercised path. Whole-disk and
  per-partition export, shrink, grow, and in-place FAT table patching are all
  covered by tests.
- **exFAT / NTFS / HFS / HFS+ / ext / btrfs → VHD** will write a correctly
  sized VHD with the filesystem patched in place, but less of this path is
  test-covered; verify the exported image by mounting it before trusting a
  restore from it. Some docs in `docs/` predate full coverage — the code
  now wires resize for every filesystem in the table.
- **HFS classic block-size expansion** ("Expand HFS Volume…") clones a
  source volume into a freshly formatted target with a larger allocation
  block size and a verified-bootable APM layout (DDR + APM map + driver
  partitions + alt MDB). Useful when an old 2 GB classic-HFS volume runs
  out of 16-bit block addresses.
- **SGI EFS / XFS**: EFS is fully read/write/resize. XFS gained a full
  edit + repair surface (R1-R8 repair pipeline; oracle-validated against
  `xfs_repair`) on the v4 format. Open XFS holes (multi-block leaf/node
  directories, bmap-btree forks, v5/CRC write side) and the planned
  shrink-via-clone path are tracked in
  [`docs/OPEN-WORK.md`](docs/OPEN-WORK.md) §2.1 and §2.2. XFS grow is
  still done at the disk-layout level ("Add free space" + in-OS
  `xfs_growfs`).
- **ProDOS → VHD** is not implemented yet; restore to raw / CHD / Zstd /
  physical disk works.
- **Raw → raw** restore always works regardless of filesystem; only the
  shrink/expand paths depend on filesystem-specific code.
- **Write-back to source formats**: WOZ2 export is supported (the writer
  regenerates a clean WOZ from the decoded sector buffer). 2MG, DC42, and
  DMG are still read-only as sources — to round-trip those, restore to raw /
  VHD / CHD / Zstd or a physical disk.
- **CHD as both source and edit target**: rusty-backup uses MAME's native
  CHD core, so `.chd` files are first-class — no external `chdman` required
  for read, write, browse, or in-place expand (Phase 6c of the disk-expansion
  workflow re-encodes the CHD with a new logical size).
- **Browsing compressed backups**: native `.zst` backups stream-decompress
  lazily on open, so browsing a multi-gigabyte zstd backup is fast. `.chd`
  backups currently require building a full seekable cache on open, which
  can be slow for large partitions — plan to work around this in a future
  release.

### MiSTer FPGA cores

Rusty Backup can build, browse, and convert images that drop straight
into [MiSTer FPGA](https://misterfpga.org/) computer cores. The list
below is the subset where the full filesystem + container + partition
pipeline works end to end. Full per-core status (including outstanding
cores) lives in [`docs/full_MiSTer_support_status.md`](docs/full_MiSTer_support_status.md).

| MiSTer core | Filesystem(s) | Media path |
|---|---|---|
| **ao486** (486 PC)             | FAT12 / FAT16 / FAT32 (MBR), ISO9660 | Floppy, HDD, CD |
| **PCXT**                       | FAT12 / FAT16 (MBR) | Floppy, HDD |
| **MSX / MSX1 / TurboR**        | FAT12 / FAT16 (Nextor VHD) | Floppy, HDD |
| **ZXNext** (ZX Spectrum Next)  | FAT32 / FAT16 / FAT12 | SD / HDD (VHD) |
| **TSConf** (ZX-Evolution)      | FAT32 (non-MBR) | SD / HDD (VHD) |
| **Minimig-AGA** (Amiga)        | AFFS (OFS/FFS), PFS3, SFS on RDB, ISO9660 | Floppy (`.adf`/`.adz`), HDD (`.hdf`/`.hdz`), CD |
| **MacPlus**                    | HFS | HDD (.hda / .hfv) — 400K MFS floppy outstanding |
| **AtariST**                    | GEMDOS (FAT12 / FAT16), MSA containers | Floppy (`.st` / `.msa`); HDD pending AHDI write-side |
| **Apple-II**                   | ProDOS + Apple DOS 3.3 | `.dsk` / `.do` / `.po` / `.2mg` / `.woz` (sector-order auto-detect) |
| **Atari800**                   | Atari DOS 2 (2.0S / 2.5, read + write) | Floppy `.atr` / `.xfd` (single + enhanced density) |
| **ZX-Spectrum**                | esxDOS FAT | DivMMC / esxDOS SD; native TR-DOS / +3DOS pending |
| **X68000** (Sharp)             | Human68k (FAT-derived) | Floppy (`.d88` / `.xdf` / `.hdm` / `.dim` — any-to-any conversion + in-place add/delete/mkdir), SASI/SCSI HDD (`.hda` / `.hdf` / `.hds` — read/browse/extract + add/delete/mkdir + in-place grow/shrink + defragmenting repack, incl. real BlueSCSI `X68SCSI1` 1024-byte-sector images). `rb-cli new-x68k-hdd` builds self-bootable HDDs from scratch (`--system-disk donor.dim` clones a Human68k system floppy into the partition; one `SWITCH.X /HD` on first FDD0 boot installs the partition boot sector and the HDD self-boots to C: thereafter). For users with the well-known `hd0.hds` donor (100 MB Sharp/Keisoku Giken SCSI HDD image, 104,857,600 bytes), `--boot-sector-donor hd0.hds --size 100M --variant scsi` overlays the donor's Sharp partition boot sector at build time — zero manual steps, self-boots to C:> on first power-on. MAME-verified on x68000 SASI + x68030 SCSI. |
| **Archie** (Acorn Archimedes)  | ADFS / FileCore (read) | `.adf` floppy, bare + Arculator-wrapped `.hdf` HDD |
| **QL** (Sinclair)              | QDOS (QXL.WIN, read + write) | HDD (.win) |
| **Amstrad CPC**                | AMSDOS + CP/M 2.2 / Plus (`amstrad_data` + `amstrad_sys` DPBs) | Floppy `.dsk` |
| **AmstradPCW**                 | CP/M Plus (`amstrad_pcw` DPB) | Floppy `.dsk` |
| **TatungEinstein**             | Xtal-DOS / CP/M (`einstein` DPB) | Floppy `.dsk` |
| **Altair8800**                 | CP/M (`altair_8in` 8-inch floppy + `altair_cf` CF/HDD DPBs) | Floppy + IDE/CF |
| **MultiComp**                  | CP/M (`multicomp` DPB) | Floppy `.dsk` |
| **C64 / C128**                 | CBM DOS (1541 / 1571 / 1581, read + write) | Floppy `.d64` / `.d71` / `.d81` |
| **VIC20 / C16 / Plus-4**       | CBM DOS (1541, read + write) | Floppy `.d64` |
| **PET / CBM-II**               | CBM DOS (1541 + 8050/8250 IEEE-488, read + write) | Floppy `.d64` / `.d80` / `.d82` |

For X68000 specifically, the floppy converter lets you take an image
in any of the four formats Sharp tooling and MiSTer cores expect — XDF
(headerless raw), HDM (PC-98 raw), DIM (DiskExplorer DIFC), or D88
(sparse track-table) — and produce any of the others, single-file or
in bulk. Geometry inference covers the X68000 + PC-98 set: 1.2 MB 2HD,
1.44 MB 2HD, 720 KB 2DD, and 640 KB 2DD.

### Physical drive compatibility

Rusty Backup talks to whatever the OS exposes as a raw block device. Anything
that shows up via the platform enumerator (Disk Management / `diskutil` /
`/sys/block`) is a candidate.

| Media                                   | Backup from physical | Restore to physical | Notes |
|-----------------------------------------|:--------------------:|:-------------------:|-------|
| CompactFlash (via USB/PCMCIA reader)    | Yes                  | Yes                 | Primary use case |
| SD / microSD / MMC                      | Yes                  | Yes                 |       |
| USB flash drives                        | Yes                  | Yes                 |       |
| USB-attached IDE / SATA (HDD, SSD)      | Yes                  | Yes                 | Docks, toasters, bridges |
| Internal SATA / NVMe drives             | Yes                  | Yes                 | Requires elevation; verify target |
| USB floppy drives (1.44 MB, 720 KB)     | Yes (as block device)| Yes                 | Treated as a regular block device; no copy-protection support |
| 5.25" / 3.5" floppies via Kryoflux / Greaseweazle / Applesauce | No (use their tools) | No | Dump to `.woz` / `.dc42` / `.2mg` and feed that image in |
| Optical media (CD/DVD/BD)               | Yes*                 | No                  | Needs additional testing, many/most filesystems support for CD and DVD, I don't think it supports any form of copy protection |
| Tape drives                             | No                   | No                  |       |

Physical floppies are supported only through the OS block-device layer, which
covers standard MFM formats (PC 1.44 MB, 720 KB). Copy-protected, variable
speed, or GCR-encoded Apple floppies must be dumped with a flux-level tool
first and then ingested as a `.woz` / `.dc42` / `.2mg` image.

## Further reading

- `PROJECT-SPEC.md` — full design document.
- `CONTRIBUTING.md` — contributor guide.
- `docs/` — per-feature deep dives (VHD export, alignment, code signing,
  Apple II floppy formats, …).

## Donations

Support me on ko-fi!  
(https://ko-fi.com/danifunker)


## License

AGPL-3.0 — see `LICENSE`.
