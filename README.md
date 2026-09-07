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
   does not, so the GUI always starts unprivileged. On Windows and Linux the
   shield button in the top bar ("Show Physical Devices" / "Unlock Physical
   Devices") restarts it elevated when you actually need a physical disk.

To build from source: `cargo build --release`. See `CLAUDE.md` for the full
build matrix.

### The `rb-cli` headless CLI

The release artifacts also include `rb-cli`, the scriptable counterpart to
the GUI. Same engine, no eframe dependency, designed for shell pipelines
and automated build farms.

```
rb-cli new volume hfs disk.dsk --size 800K --name "My Disk"
rb-cli put disk.dsk ./Finder /System/Finder --type FNDR --creator MACS
rb-cli ls  disk.dsk /System
rb-cli du  disk.dsk "/System Folder" --json   # recursive both-fork (data+resource) size of a path
rb-cli cp  floppy.adf / harddisk.hda@1 /Floppies/d01/ -r   # consolidate an image onto a HD
rb-cli tar irix.img@1 / irix.tar.gz   # archive a case-sensitive volume (keeps case + symlinks)
rb-cli untar disk.hda src.tar.gz /   # import an archive's contents INTO an image (skips ._* + unstorable names)
rb-cli import disk.hda ./stuff /     # copy a whole host folder in; --expand-archives unpacks tarballs + .sit/.cpt/.hqx it finds
rb-cli get backup.zip /unix ./unix --inside disk.img   # extract from a RAW disk inside a .zip
rb-cli fsck disk.dsk --checkonly
rb-cli inspect disk.hda
rb-cli backup /dev/disk3 ./backups --format chd --checksum sha256
rb-cli restore ./backups/my-backup ./restored.img
rb-cli batch script.json --dry-run
rb-cli new hd x68k hdd.hdf --size 16M --system-disk human68k.dim
rb-cli new hd x68k c.hdf --size 32M --variant scsi --system-disk human68k.dim \
                          --boot-sector-donor hd0.hds      # zero manual steps, your donor
rb-cli new hd x68k c.hdf --size 32M --variant scsi --system-disk human68k.dim \
                          --builtin-boot-sector            # zero manual steps, no donor needed
rb-cli new hd sgi-efs irix.img --size 50M                     # IRIX SGI dvh + EFS root HDD
rb-cli optical new sgi-efs irix.iso --size 600M                  # IRIX EFS CD-ROM (slot-7 SYSV, mount -t efs)
rb-cli optical new sgi-efs irix.iso --size auto --from-dir ./sgi-stuff  # format + fill in one step
rb-cli optical new sgi-efs irix.iso --size auto --from-dir ./sgi-stuff \
       --expand-archives --flatten-folders   # unpack every .tardist into one inst-ready root
rb-cli optical new mac-hfs mac.iso --size 600M --name "My CD"     # classic Mac CD-ROM (APM + HFS)
rb-cli optical new mac-hfsplus mac.iso --size auto --from-dir ./mac-stuff  # HFS+ (Mac OS 8.1+), filled
rb-cli optical new mac-hfs mac.iso --size auto --from-dir ./mac-stuff \
       --expand-archives     # unstuff every .sit/.cpt/.hqx onto the disc, forks intact
rb-cli put irix.img@1 ./bstoolbox /bstoolbox               # populate its EFS root partition
rb-cli swab16 3130.img 3130-native.img                     # flip 16-bit word order (its own inverse)
rb-cli mac-scsi-bless mac.hda                              # install Apple SCSI driver + DDR
rb-cli mac-scsi-bless mac.hda --driver-from donor.hda      # use a donor disk's driver verbatim
rb-cli make-bootable disk.dsk --boot-from "System 7.0 HD.dsk"  # auto: apply only what's missing to boot
rb-cli make-bootable mac.hda --dry-run                     # preview: detect kind + missing pieces
```

Shell completions for bash / zsh / fish / PowerShell:

```
rb-cli install-completions          # auto-detects $SHELL
rb-cli completions zsh > _rb-cli    # emit-to-stdout for packagers
```

Full verb-by-verb reference: [`docs/cli-reference.md`](docs/cli-reference.md)
(regenerated from `cargo run --example generate_cli_docs`).
Open CLI follow-ups (and everything else still to do) are tracked in
[`docs/OPEN-WORK.md`](docs/OPEN-WORK.md).

#### `rb-cli-mini` for MiSTer FPGA (armv7)

`rb-cli-mini` is the **MiSTer-specific** build of `rb-cli`: a slim
variant cross-compiled for the FPGA's Intel Cyclone V / Cortex-A9 SoC
(`armv7-unknown-linux-gnueabihf`, glibc 2.31 baseline from the
Buildroot rootfs). It excludes the GUI (eframe / egui / rfd) and the
update checker's reqwest client — but **keeps CHD support** via the
upstream `libchdman-rs` armv7 prebuilt (so `.chd` images work inline on
the device) **and the optical-disc stack** (opticaldiscs / cd-da-reader),
so devices with a CD/DVD drive (e.g. the SuperStation One) can rip discs
on-device.

The desktop release builds use the full feature set; only the MiSTer
artifact runs `--no-default-features --features chd,pure-zstd,remote,optical,tui`
(CHD via the C prebuilt; zstd via the pure-Rust bit-exact backend, since a
cross build won't link C libzstd; `remote` for the network daemon — see
[rb-daemon](#run-this-device-as-a-network-daemon-rb-daemon) below; `optical`
for CD/DVD ripping — cd-da-reader links no system libcdio, and
opticaldiscs reuses the same libchdman-rs 0.288.10 prebuilt as `chd`; `tui`
for the full-screen `rb-cli tui` — pure-Rust ratatui, so it costs the cross
build nothing and is what `rb-cli menu` opens).

```
# Cross-compile for MiSTer (armv7-unknown-linux-gnueabihf):
cargo install cross --git https://github.com/cross-rs/cross --locked
cross build --bin rb-cli --release \
            --target armv7-unknown-linux-gnueabihf \
            --no-default-features --features chd,pure-zstd,remote,optical,tui

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
- Every filesystem operation (`ls`, `put`, `get`, `tar`, `untar`, `rm`,
  `mkdir`, `fsck`, `resize`, `expand`, `chmeta`, `bless`, …) on FAT, NTFS,
  exFAT, HFS, HFS+, ext, AFFS, PFS3, SFS, ProDOS, Human68k, ADFS, etc.
- `inspect`, `backup`, `restore` for Raw, VHD, QCOW2, VMDK, Zstd, the
  four floppy container formats, **and CHD**.
- `floppy convert` (XDF / HDM / DIM / D88, single-file and bulk) —
  the X68000 workflow runs inline on the device.
- Partition table editing (`partmap`), backup-folder operations.
- `shrink`, `grow .chd`, single-file CHD backups — all work.
- The **rb-daemon** network daemon (`serve`) — host this device's images
  and disks on the LAN so the desktop app can browse/back up/restore them
  over `rb://`. Installs from the Scripts menu; see
  [below](#run-this-device-as-a-network-daemon-rb-daemon).
- **Optical-disc ripping** (`optical drives` to find the drive, then
  `optical rip --device /dev/sr0 --format iso|bincue`), plus `optical
  convert` (ISO ↔ BIN/CUE ↔ CD-CHD) and `optical browse` / `extract`. For
  devices with a CD/DVD drive such as the SuperStation One.
  **Data DVD and Blu-ray discs rip too** — to `iso` only, since the raw
  2352-byte sector formats `bincue` and CD-CHD need are CD-only concepts.
  Console discs (Wii, GameCube, original Xbox) can *not* be ripped from a
  standard drive: their media is proprietary and stock drive firmware will not
  read it. Dump those on the console itself and open the resulting image here —
  Wii/GameCube images (including NKit) and Xbox XDVDFS are fully supported.
- **Machine-readable optical inspection:** `optical browse --format json`
  emits a deterministic, path-sorted file listing (add `--hash sha256` for
  per-file content hashes), and `optical info --format json` reports
  volume-level metadata (ISO 9660 PVD identity + Rock Ridge / Joliet / UDF
  flags, El Torito boot catalog, HFS/APM) leniently — surfacing warnings
  rather than failing on the sloppy 90s mastering that trips strict parsers.
- **Hybrid Mac/PC discs:** `optical info` enumerates *every* filesystem on a
  disc, not just the primary. A hybrid CD (an ISO 9660 volume plus an
  Apple_HFS partition sharing one data track, the way most 90s Mac/PC game
  discs were mastered) lists both sides — so the Mac volume is visible even
  when the PC ISO 9660 tree is what the browser opens by default. Reach the Mac
  side with `optical browse --filesystem hfs` / `optical extract --filesystem
  hfs` / `optical du --filesystem hfs` (the default `auto` keeps opening the PC
  ISO 9660 tree), or pick it from the filesystem dropdown in the GUI's optical
  disc browser.
- **Both-fork disc sizing:** `optical du <disc> <path>... [--filesystem hfs]`
  reports recursive data + resource fork bytes (and allocation-block-rounded
  size) for a path on a disc — the disc counterpart of the top-level `du`, so a
  hybrid Mac disc's app folders size correctly instead of undercounting the
  resource-fork-only apps.
- **El Torito boot images:** `optical info` lists every boot entry (platform,
  bootable, media type, size + sha256) and names the *nested* filesystem inside
  each. `optical boot extract` pulls a boot image out to a file, which is just a
  disk image — so `ls` / `inspect` / `get` / `put` / `fsck` all work on it — and
  `optical boot replace` writes an edited image back into the catalog. (The disc
  layer is handled by the `opticaldiscs` crate; rusty-backup interprets and
  edits the boot image's filesystem.)
- **Remote ripping off-device:** run `rb-daemon` here and drive this drive
  from the desktop app / CLI — the device only issues SCSI reads while the
  desktop does the heavy CHD encoding, so the armv7 CPU isn't taxed.
  `optical rip --device rb://this-device:7341/dev/sr0`, or the desktop
  Optical tab's "Add remote daemon..." button. See
  [`docs/remote_ripping.md`](docs/remote_ripping.md).

What's excluded:
- GUI windows and the update-checker self-replace UI (only meaningful
  for the desktop binary).

Full background and the feature matrix live in
[`docs/mister_cli.md`](docs/mister_cli.md).

#### Install via MiSTer Downloader (custom database)

The easiest way to get Rusty Backup onto a MiSTer is the built-in
**Downloader** (the same tool `update_all` drives). It installs and
**keeps itself updated** from the Scripts menu — no SSH, no tarball, no
cross toolchain. Add this section to `/media/fat/downloader.ini` on the
SD card:

```ini
[danifunker/rusty-backup]
db_url = 'https://github.com/danifunker/rusty-backup/releases/latest/download/rusty-backup.json.zip'
```

Then run **Scripts > update** (or **Downloader**) on the MiSTer. It
drops `rb-cli` and `rb-daemon.sh` into `/media/fat/Scripts`. The
`db_url` points at the `latest` release, so every later Downloader run
picks up new builds automatically.

This is a [MiSTer Downloader *custom database*](https://github.com/MiSTer-devel/Downloader_MiSTer/blob/main/docs/custom-databases.md):
each release's CI publishes the slim armv7 binary and the daemon shim as
standalone assets plus a generated `rusty-backup.json.zip` database that
references them by md5 + size. Because Downloader places individual files
(it doesn't unpack tarballs), the bare `rb-cli` may arrive without its
executable bit — `rb-daemon.sh` re-applies it on first run, so the menu
entry works regardless.

Prefer to install by hand? The `rb-cli-mini-...tar.gz` from the
[Releases page](https://github.com/danifunker/rusty-backup/releases) still
ships its own `install.sh` (see below).

#### Run this device as a network daemon (rb-daemon)

The MiSTer build can run as a small **network daemon** so the desktop
Rusty Backup app reaches into the MiSTer over your LAN — browse the SD
card and the disk images on it, copy files in and out, and back up /
restore whole disks — without pulling the card. It works like mrext's
*Remote*: one entry in the Scripts menu, auto-start on boot once enabled.

Install it from the `rb-cli-mini` release tarball (it now bundles the
daemon shim + installer):

```
# On the MiSTer (or over SSH), from the unpacked tarball:
./install.sh
```

That drops two files onto the SD card:

- `/media/fat/Scripts/rb-cli` — the program (no `.sh`, so it is **not** a
  second Scripts-menu entry).
- `/media/fat/Scripts/rb-daemon.sh` — the **only** menu entry: open it to
  bring up the daemon console.

Then open **rb-daemon** from the MiSTer Scripts menu. The console shows
whether the daemon is running, whether it auto-starts on boot, and the
**IP:port** other machines connect to, with these actions:

- **Start Now** / **Stop Now** — run or stop the daemon immediately.
- **Install Autostart** — start it now *and* launch it on every boot.
- **Uninstall Autostart** — stop launching it on boot (a running daemon
  keeps running).

Everything is scriptable too — no console needed:

```
rb-cli serve service install     # enable on boot + start now
rb-cli serve service status      # ACTIVE/INACTIVE, autostart, IP:port
rb-cli serve service stop        # stop the running daemon
rb-cli serve service uninstall   # remove the boot entry
```

Defaults (editable in `/media/fat/Scripts/rb-daemon.ini`) serve the whole
`/media/fat` card on `0.0.0.0:7341`, writable. From the desktop, connect to
`rb://<mister-ip>:7341/`. Both Commander clients reach a daemon **both ways** —
browse its files and copy *out*, and copy files/folders back *in* (a remote host
folder or an image on it): in the **GUI Commander** point a pane at it via "Open"
-> "Connect to Remote...", and in the **`rb-cli tui` Commander** press **`R`** and
enter `host:port`. For a browse-only daemon that refuses every write, start it
with `rb-cli serve --read-only` (or set `writable = no` in `rb-daemon.ini`). The
daemon design lives in [`docs/remote_transfer_plan.md`](docs/remote_transfer_plan.md).

### Bootable backup appliances (boot the metal, no host OS)

For machines too old or too bare to run the desktop app, the same engine ships
as **bootable media you run *on* (or beside) the vintage box**:

- **Linux appliance** — a minimal Buildroot Linux that boots straight into the
  `rb-cli` backup/restore menu (on a VGA monitor *or* a serial console) with the
  static `rb-cli` baked in. One hybrid ISO boots from a CD-ROM **or** a USB
  stick / CF card (`dd` the same file onto the device). The kernel carries a
  broad **vintage-hardware driver set** — ISA / PCI / PC-Card NICs, SCSI HBAs,
  bare-486 / VL-Bus + PCI IDE, Multi-I/O serial & parallel, parallel-port ZIP,
  USB storage — so it sees the disks and cards in a 486/Pentium-class machine
  out of the box. **What's baked in, how to make an unusual card work (ISA
  `modprobe io=/irq=` recipes), and how to rebuild the kernel for more** are in
  [`docs/appliance_hardware_support.md`](docs/appliance_hardware_support.md);
  build/boot overview in [`docs/linux_486_appliance.md`](docs/linux_486_appliance.md).
- **cb-dos** — the DOS-native lane: a FreeDOS floppy / CD that images disks with
  a hand-written C tool over BIOS int 13h, for boxes where even a minimal Linux
  is too heavy. See [`docs/cb_dos.md`](docs/cb_dos.md).

Both build from the repo (`buildroot/`, `crusty-backup/`).

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
  expansion workflow* section below. A **Write Image File** mode pours a
  plain disk image onto a device, or into one of its partitions, leaving
  the partition table and the rest of the disk untouched; any format
  Rusty Backup can read works (raw, DMG, CHD, VHD, 2MG, GHO, IMZ, and
  gzip- or zip-wrapped images are decoded on the way to the disk). The
  CLI equivalent is `rb-cli write IMAGE DEVICE [--partition N] --yes`.
  A **Build Disk** mode goes the other way: pick a blank device or name a
  new image file, choose a table type (MBR / GPT / APM / SGI volume header
  / SGI disk label / X68000), lay out the partitions in the shared
  partition editor, and
  optionally assign a source image to each one. Applying writes the table
  and pours every assigned image into its partition in a single pass. The
  CLI equivalent is
  `rb-cli new hd {mbr|gpt|apm|sgi|sgi-dklabel|x68k} IMG --size 2G --partition 512M:0C:DOS --partition rest --fill 1=dos.img`.
- **Inspect** — pick any supported source and browse the partition table,
  filesystem info, and file listings. Several actions live here:
  - **Browse** filesystem contents read-only (per partition). A checkbox
    column (Space to mark in the TUI) multi-selects files and folders; pick a
    format from the **Export as** pulldown and click **Export** to bundle the
    whole selection into a single archive — Mac Archive (`.mar`), StuffIt
    (`.sit`), Zip, or tar (gzip / zstd). Available in the Inspect tab, the
    Optical disc browser, Commander, and the `rb-cli tui` Explorer; all four
    run the same `export_selection` engine.
  - **Edit Partition Table…** to add / resize / delete / retype
    partition entries on raw disks, image files, and devices. Writers
    cover MBR, GPT, APM, SGI, and RDB-bootable-flag.
  - **Resize Partitions…** for in-place partition data moves with
    filesystem-side patching. Its **Disk size** field enlarges the image
    file itself, so a partition can be grown past the end of the current
    container without dropping to `rb-cli grow` first (a physical device's
    size is fixed, and a compressed container can't be extended in place).
  - **Add Partition…** as a streamlined entry into the editor when
    trailing free space exists, pre-filled per partition-table type.
  - **Expand Image…** to grow a raw, VHD, or CHD image with trailing
    zero-padding so you have room for new partitions.
  - **Export Disk Image…** to write VHD (fixed or dynamic), QCOW2,
    VMDK, Raw, 2MG, WOZ, MOOF, DC42, HFV, or CHD (whole-disk or
    per-partition) — see `docs/vhd-export.md`.
  - **Convert Floppy Container…** to convert between the four
    X68000 / PC-98 / FM-7 floppy wrappers (XDF, HDM, DIM, D88) one
    file at a time. Bulk folder conversion lives in the existing
    **Bulk Convert** dialog, which now lists the same four formats
    as output targets.
  - **Check** (`fsck`) + **Repair** on every filesystem whose driver
    implements it. From the Inspect grid: HFS / HFS+, FAT / exFAT / NTFS / HPFS,
    ext2/3/4, SGI EFS, UFS, XFS, JFS, AmigaDOS OFS/FFS (Disk Validator) /
    PFS3 / SFS, and the retro floppies CBM DOS / DragonDOS / RS-DOS /
    Acorn DFS / Human68k. From the browse view: Alto BFS/TFS (their packs
    open through the container path, not the block factory). Repair uses
    replica superblocks, lost+found, FAT-mirror resync, or allocation-bitmap
    rebuild depending on the filesystem. Everything here is also scriptable
    via `rb-cli fsck` (+ `--repair`).
  - **Defragment…** on Human68k (X68000) partitions: repack the volume so
    files are stored contiguously, reclaiming holes left by deleted files
    (also `rb-cli repack`).
  - **Build a Sharp X68000 HDD from scratch** via the shell:
    `rb-cli new hd x68k out.hdf --size 16M --system-disk donor.dim`
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
  - **Make a classic-Mac SCSI disk bootable** via the shell:
    `rb-cli mac-scsi-bless mac.hda` installs an Apple SCSI driver and a
    valid Driver Descriptor Record into an APM disk so a Macintosh ROM
    (e.g. Quadra 800) registers the drive over SCSI. Uses a bundled
    known-good driver by default, or `--driver-from donor.hda` to copy a
    donor disk's driver verbatim. Operates in place; partition data never
    moves, and it is idempotent. (This registers the *driver* so the ROM
    can read the disk — it does not change HFS boot-block behavior.)
    Verified against the real Quadra 800 ROM in QEMU `-M q800`.
  - **Make a Mac disk bootable** — `rb-cli make-bootable disk.img
    [--boot-from "System 7.5.3 HD.dsk"]` (or the Inspect tab's
    **Make Bootable...** button) auto-detects what the disk is and applies only
    what it's missing:
    - a **flat `.hfv`/`.dsk`** (BasiliskII / SheepShaver / Mini vMac, e.g. a
      customized [infinite-mac](https://infinitemac.org) disk) — boot blocks +
      blessed System Folder, kept flat (no APM wrapper added);
    - a **full APM disk** (an infinite-mac "device image" with DDR + map +
      drivers, or any Mac SCSI disk image) — a SCSI driver + DDR (bundled, or
      `--driver-from donor`) if absent, then boot blocks on the `Apple_HFS`
      partition + bless, leaving the DDR / map / drivers untouched.

    Boot blocks are never synthesized — they're copied verbatim from a
    `--boot-from` donor that already boots that System, so they stay
    version-matched. The operation is idempotent (`--dry-run` previews it). The
    lower-level pieces are also available on their own: `rb-cli put IMG[@N]
    --boot-from DONOR`, `rb-cli bless set`, and the browse-view
    **Boot Blocks...** / **Bless Folder** buttons.
  - **Edit mode** on FAT, NTFS, exFAT, HPFS, ext, HFS, HFS+, AFFS, PFS3, SFS,
    ProDOS, Apple DOS 3.3, MacPlus MFS, EFS, UFS, CP/M (multi-DPB),
    Human68k, and XFS (v4 + v5): stage create-file / new-folder /
    drag-and-drop / delete edits, then Apply atomically with snapshot
    rollback on error.
- **Optical** — browse and extract files from CD/DVD/BD images and physical
  optical drives. Reads ISO 9660 (with Joliet and Rock Ridge extensions),
  High Sierra (pre-ISO 9660), UDF, HFS and HFS+ (Mac hybrid / data discs),
  SGI EFS (IRIX), UFS/FFS (Tru64 / Solaris / NeXT), VMS ODS-2 (OpenVMS), and
  the video-game console filesystems — Nintendo GameCube & Wii (Wii decrypted
  internally, no key file needed), Sega Dreamcast GD-ROM (`.gdi` + CHD),
  Philips CD-i, and 3DO Opera — discs. Also identifies the console, game
  serial, title, and region for PlayStation, Saturn, Mega-CD, Neo Geo CD,
  PC-FX, CD32, and the browsable consoles. See the *Optical disc filesystems*
  table below. Re-opens automatically when the underlying disc changes.
- **Archives** — browse and extract classic Macintosh archives. Auto-detects
  StuffIt 1-5 (`.sit`, `.sea` self-extracting), Compact Pro (`.cpt`), MAR
  (`.mar`, read + write), MacBinary I/II/III (`.bin`), MacZip (Info-ZIP's
  Macintosh port — a `.zip` carrying `Mac3` Finder metadata and `XtraStuf.mac/`
  resource forks), and BinHex (`.hqx`) wrappers around any of them. `.bin` and
  `.zip` are content-detected, so a raw `.bin` disk image or a plain
  disk-image-in-a-zip still opens as a disk image. A MacBinary whose data fork
  is itself a StuffIt/Compact Pro archive is peeled through to the inner
  entries.
  Pick an archive, browse the entry tree (name / type / creator / size /
  codec), tick the entries to keep, and extract to a folder in your choice
  of fork-preserving container — BinHex, MacBinary, AppleDouble, or raw
  data + `.rsrc` sidecar. Export a file or folder back out as `.sit`,
  `.sit.hqx`, `.hqx`, or `.mar`. Single-entry archives whose payload is
  itself a disk image (DiskCopy 4.2, raw HFS, raw HFS+) get a one-click
  "Mount in new Inspect tab" handoff. `rb-cli archive list` / `archive
  extract` / `archive create` (formerly `sit`, still accepted) is the
  scriptable counterpart. The same decoders back `--expand-archives` on
  `rb-cli import` and `optical new`, which unstuffs a folder's archives
  straight into the image — forks and Finder type/creator intact — instead
  of copying them in packed.

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
| QCOW2 (QEMU)   | `.qcow2`, `.qcow` | Yes          | Yes (create / edit) | v2 + v3, including UTM's classic-Mac disks. An image carrying **internal snapshots** opens read-only — UTM parks its VM suspend state in one, so this is the common case; flatten it (`qemu-img snapshot -d <name>`) to edit. Backing files, compressed clusters, external data files, extended L2 (subclusters) and encryption are refused at open with a message naming the reason |
| VMDK (VMware)  | `.vmdk`         | Yes            | Yes (create / edit) | Flat and monolithic-sparse |
| Zstd stream    | `.zst`          | Yes            | Yes             | Good general compression, splittable |
| Gzip stream    | `.gz`           | Yes            | Yes             | DEFLATE per-partition member; the codec shared with crusty-backup (`cb-dos`) so DOS-side backups restore + resize here unchanged. `--format gzip` |
| LZ4 stream     | `.lz4`          | Yes            | Yes             | LZ4 frame per-partition member; the other codec shared with crusty-backup (`cb-dos` `/CODEC:LZ4`) — faster than gzip on a slow CPU at a lower ratio. Restored + resized exactly like a `.gz` member. `--format lz4` |
| cb-dos container | `.cbk`        | Yes (native)   | Yes (`cbk pack`) | Single-file form of a backup folder (chunked gzip members + index). Opens like any disk image — `inspect`, `ls`/`get` (browse + extract), `fsck`, GUI Inspect, and `restore` all work directly, no extract step. Large partitions are split into ~4 MiB source-span gzip members (via the `partition-N.gz.idx` seek layout), so the lazy reader seeks per-chunk instead of decompressing from the start. `rb-cli cbk pack/unpack` convert to/from a folder. Frozen v1; the eventual cb-dos network transport's on-disk artifact |
| CHD (MAME)     | `.chd`          | Yes            | Yes             | Native (MAME's CHD core is bundled — no external `chdman` needed) |
| AppImage       | `.AppImage`     | Yes            | No              | A type-2 AppImage is an ELF runtime stub with a **SquashFS appended**, so the payload opens in place — browse, extract, and **edit** the application's filesystem without unpacking anything. Because the payload is the tail of the file, a rebuild is free to grow it and the stub in front is left byte-for-byte alone. Detected by content (the `AI\x02` marker in the ELF header), not by name, since AppImages are routinely shipped without an extension. Type-1 AppImages wrap an ISO 9660 instead and are recognised but not opened |
| Norton Ghost   | `.gho`, `.ghs`  | Yes            | No              | File-aware FAT/NTFS browse, sector + spanned sets, Ghost 7.5, password-protected images decrypted automatically |
| WinImage       | `.imz`          | Yes            | No              | Including password-protected archives |
| ZIP (raw disk) | `.zip`          | Yes            | No              | A RAW disk image inside a plain ZIP. Auto-picks the disk entry (`--inside NAME` to choose one of several); inflated sparsely to a temp file at open, so a mostly-empty multi-GB image only uses its real content. Picker-visible but not OS-associated |
| BasiliskII HFV | `.hfv`          | Yes            | Yes             | Flat classic-HFS volume (≤ 2047 MB) for 68k Mac emulators |
| Apple 2MG      | `.2mg`          | Yes            | No              | Apple II / IIgs disk images |
| Apple II DSK   | `.dsk`, `.do`, `.po` | Yes       | No              | DOS-order, ProDOS-order, and auto-detect sector orderings |
| Disk Copy 4.2  | `.dc42`, `.image` | Yes          | No              | Classic Mac floppy images. Apple Twiggy / FileWare 871 KB prototype images (the recovered MacPaint 0.5 / early Finder disks, disk-format byte `0x54`) are auto-detected and de-interleaved into their MFS/HFS volume — the two sides are stored sequentially, so the volume is recovered by rotating the sector array onto its Master Directory Block. |
| Apple DMG      | `.dmg`          | Yes (UDIF)     | No              | UDIF read-only images: raw/UDRW plus zlib (UDZO), bzip2 (UDBZ), ADC (UDCO), LZFSE (ULFO), and LZMA/xz (ULMO) compression. GPT/APM/MBR-partitioned and superfloppy layouts; a GPT-wrapped HFS/HFS+ volume (`hdiutil create` output) browses/inspects/extracts directly. **Encrypted** images (`encrcdsa` v2, AES-128/256 — `hdiutil -encryption`) decrypt with `--password`: PBKDF2-HMAC-SHA1 key unwrap + per-block AES-CBC with an HMAC-SHA1 IV, fully offline. `cdsaencr` v1 recognized but not yet decoded |
| Self-mounting / NDIF | `.smi`, `.img`, `.smi.bin` | Yes | No | Classic-Mac **self-mounting images** and pre-UDIF **NDIF** disk images (Disk Copy 6, Mac OS 8/9). The disk chunks live in the file's data fork, described by a `bcem` block map in the resource fork — delivered as a MacBinary (`.smi.bin`), AppleDouble (`._name`) pair, or native resource fork. Zero / raw / ADC chunks decoded; RLE / LZH / KenCode / StuffIt chunks are recognized and reported (await sample images) |
| Apple sparse image | `.sparseimage` | Yes (UDSP) | No          | Apple growable sparse image (`sprs` band map, Mac OS X 10.3+). Stores only written 1 MiB bands; unallocated bands read as zeros. Browses/inspects/extracts like any disk image. Up to ~1 GiB of *written* data (single-header band map); larger images not yet supported |
| WOZ            | `.woz`          | Yes            | Yes (export)    | Apple II 5.25" and 3.5"; WOZ2 writer regenerates a clean image |
| MOOF           | `.moof`         | Yes            | Yes (export)    | Applesauce Macintosh 3.5" GCR bitstream (400K single-sided / 800K double-sided) — the Mac sibling of WOZ, loadable by Snow / MAME / Applesauce. Read decodes the GCR bitstream to logical sectors (shared 3.5" decoder); `rb-cli convert IN OUT --format moof` regenerates a clean image, encoding the same Sony GCR tracks (zoned 12/11/10/9/8 sectors, 6-and-2) in a Mac-flavoured container. Ideal for handing a formatted MFS/HFS floppy to a classic-Mac emulator. |
| DART           | `.dart`, `.image`, extensionless | Yes (decode) | No | Apple's Disk Archive/Retrieval Tool — the compressed disk image contemporary with DiskCopy 4.2. Content-detected (no magic number). Fast (word-RLE), best (LZHUF), and uncompressed chunks all handled; LZHUF validated byte-exact against a real Apple image. Preserves the 12-byte sector tags, so **Lisa** DART disks open as Lisa volumes; Mac / Apple II / MS-DOS DART disks decode to their block data and route to the normal filesystem detection. |
| Amiga ADF / HDF | `.adf`, `.hdf` | Yes            | Yes (raw)       | Floppy + hard-disk images. RDB partition tables parsed. Arculator-wrapped `.hdf` (Acorn) auto-detected. |
| Gzip-wrapped    | `.adz`, `.hdz`, `.gz` | Yes      | `.adz`/`.hdz` only | Any gzip-wrapped disk image, transparently decompressed at open. `.adz`/`.hdz` are the editable Amiga floppy/HDD wrappers; a bare `.gz` (e.g. a `.pdi.gz` Alto/Pilot pack or a gzipped raw image) is read-only. |
| Atari MSA      | `.msa`          | Yes            | No              | Magic Shadow Archiver — Atari ST 720K / 800K / 1.44MB floppy |
| CPCEMU DSK / EDSK | `.dsk`       | Yes            | No              | Amstrad CPC / PCW / Einstein / Oric CP/M floppies |
| Commodore disk | `.d64`, `.d71`, `.d81`, `.d80`, `.d82` | Yes  | Yes (in-place edit) | 1541 / 1571 / 1581 + PET 8050 / 8250 (IEEE-488) flat sector dumps for the C64 / C128 / C16 / VIC-20 / PET cores. Read/browse/extract + add/delete persist back into the image (bidirectionally cross-validated against the `c1541` / Python `d64` reference). |
| Commodore GCR  | `.g64`, `.g71`  | Yes (decode) | No        | Raw 1541 / 1571 GCR track images (preservation-grade). Decoded to flat sectors so the CBM engine can read them; the `.g71` side-1 mapping is validated against a real VICE `c1541` image. |
| Atari disk     | `.atr`, `.xfd`  | Yes            | Yes (in-place edit) | 8-bit Atari (400/800/XL/XE) disk images for the Atari800 core. `.atr` = 16-byte header + sector body; `.xfd` headerless. Read/browse/extract + add/delete on the Atari DOS 2 volume. |
| CoCo disk      | `.dsk`, `.jvc`, `.vdk` | Yes     | Yes (in-place edit) | Tandy Color Computer (CoCo2 / CoCo3 cores) raw 35- / 40-track sector dumps. Headerless flat body (length a multiple of 256). Auto-detects the volume's filesystem: RS-DOS / Disk BASIC (flat granule FS) or OS-9 / NitrOS-9 RBF (hierarchical). Read/browse/extract + add/delete on both. |
| Acorn DFS disk | `.ssd`, `.dsd`  | Yes            | Yes (in-place edit) | BBC Micro / BBC Master / Acorn Electron (MiSTer BBCMicro / AcornElectron cores) floppy. Flat 40-track (100K) / 80-track (200K) sector dump in logical order. Read/browse/extract + add/delete on the Acorn DFS catalogue. Single-sided `.ssd` opens as one volume; **double-sided `.dsd`** (the two sides stored track-interleaved) is de-interleaved and presented as **two** Acorn DFS partitions (`IMG@1` = side 0, `IMG@2` = side 1) — edits to either side re-interleave back into the `.dsd` on save. |
| ZX Spectrum TR-DOS | `.trd`      | Yes            | Yes (in-place edit) | ZX Spectrum Beta Disk (MiSTer ZX-Spectrum core). Flat raw sector dump in logical track order (80-/40-track, single-/double-sided; 16 × 256-byte sectors). Read/browse/extract + add/delete/rename on the TR-DOS catalogue; geometry auto-detected from the disk-info sector. |
| TI-99/4A disk  | `.dsk`          | Yes            | Yes (in-place edit) | TI-99/4A (MiSTer TI-99_4A core) flat V9T9 sector image (SSSD 90K / DSSD / SSDD 180K / DSDD 360K). Read/browse/extract + add/delete/rename on the TI disk filesystem (VIB + FDIR + extent-based FDR files, big-endian); geometry read from the VIB. Content-detected via the "DSK" volume marker. |
| Sharp D88      | `.d88`          | Yes            | Yes (convert + in-place edit) | X68000 / PC-88 / PC-98 / MSX / FM-7 sparse track-table container. Add/delete/mkdir on the contained Human68k FAT volume persist back into the container (decode -> edit -> re-encode). |
| X68000 XDF     | `.xdf`          | Yes            | Yes (convert + in-place edit) | Raw headerless X68000 floppy dump; geometry inferred from size. In-place file add/delete/edit supported. |
| X68000 HDD     | `.hda`, `.hdf`, `.hds`, `.ima` | Yes | Yes (in-place edit + resize + defrag repack) | Sharp SASI/SCSI hard-disk images; X68k partition table + Human68k FAT12/16. Read/browse/extract + add/delete/mkdir + in-place FS grow/shrink + contiguous repack (SHARP/KG big-endian BPB & FAT). Geometry auto-detected: SCSI `X68SCSI1` (table @ 0x800, 1024-byte sectors) and SASI (table @ 0x400, 256-byte sectors, incl. custom-IPL game disks). |
| PC-98 HDM      | `.hdm`          | Yes            | Yes (convert + in-place edit) | DiskExplorer raw headerless floppy dump (byte-identical to XDF). In-place file add/delete/edit supported. |
| DiskExplorer DIM | `.dim`        | Yes            | Yes (convert + in-place edit, DIFC) | DIFC 256-byte header + payload; generic 256-byte-header fallback for IBM XDF DIM on read. Add/delete/edit persist back into the container. |
| Xerox Alto pack | `.pdi`, `.bfs`, `.copydisk`, `.altodisk` | Yes | Yes | Diablo 31/44 disk packs for the Xerox Alto. `.pdi` = **PARC Disk Image** (a flat, self-describing, label-inclusive container designed as the recommended emulator format); `.bfs` / `.copydisk` / `.altodisk` = period CopyDisk streams, imported transparently. Detected by magic, surfaced as a single browsable `Alto BFS` volume. Browse + extract + add/delete + resize; edits save as PDI. |
| Salto disk | `.dsk` | Yes | Yes | Salto Alto-II emulator "cooked" Diablo-31 image (`[pageno][header][label][data]` per sector). Byte order auto-detected (Salto-native little-endian or big-endian); export writes Salto-native little-endian so the result loads in the emulator. Same `Alto BFS` content as the other Alto packs. |
| ContrAlto2 Diablo | `.dsk` | Yes | No | ContrAlto2 / Bitsavers Diablo-31 pack (`[dummy][header][label][data]`, little-endian, sector-interleaved) — same size as a Salto `.dsk` but distinguished by content; sectors placed by their header disk address. Read as an `Alto BFS` volume. |
| Trident pack | — (raw, size-detected) | Yes | Yes | Trident T-80 / T-300 pack image (ContrAlto2 / dorado layout: `[dummy][header][label 10w][data 1024w]` per sector, little-endian, 2048-byte pages, physical sector interleave). The same Alto file system (TFS) on Trident hardware; recognized by the exact T-80 (~76 MB) / T-300 (~285 MB) size, surfaced as an `Alto BFS` volume. Validated against ContrAlto2's real Spruce print-server T-300 pack. |
| Xerox Pilot/Cedar volume | `.pdi` (`fsFamily=2`) | Yes | Yes (create / add file) | D-machine Pilot/Cedar filesystem in a PARC Disk Image. Physical/logical volume roots, subvolume table, VAM, run-table files; both file-ID generations (32-bit Cedar nucleus / 80-bit original Pilot via `flags` bit 2). Surfaced as a read-only `Pilot/Cedar` volume in the GUI; blank-volume + add-file via `pilot_probe`. See `docs/` PARC specs. |
| Dwarf 6085 disk | `.zdisk`, `.zdelta` | Yes | No | Dwarf "Draco" 6085/Daybreak emulator rigid-disk image — a zlib stream of label-inclusive Pilot sectors (10-word label + 256-word data; the 6085/IOP stores labels byte-swapped, normalized on read). Opens as a read-only `Pilot/Cedar` volume; lists and extracts files. The disks shipped with Dwarf (ViewPoint 2.0, XDE 5.0) are the real Pilot volumes our reader was validated against. |
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
| FAT12          | Yes    | Yes  | Yes             | Yes (check + repair) | Apple II SuperDrive, DOS floppies. `rb-cli backup --defrag` repacks files contiguously (boot-aware). fsck reconciles FAT chains (loops, bad/lost/cross-linked clusters, size vs. chain) and FAT-mirror consistency; repair is FAT-only (frees lost chains, truncates, resyncs mirrors), validated against `fsck_msdos`. |
| FAT16          | Yes    | Yes  | Yes             | Yes (check + repair) | DOS / Windows 3.x / 9x. `rb-cli backup --defrag` repacks files contiguously (boot-aware). fsck reconciles FAT chains + mirror copies; see FAT12. |
| FAT32          | Yes    | Yes  | Yes             | Yes (check + repair) | Windows 95 OSR2+ through XP, vintage Linux. `rb-cli backup --defrag` repacks files contiguously (boot-aware). fsck reconciles FAT chains + mirror copies; see FAT12. |
| exFAT          | Yes    | Yes  | Yes (in-place + defragmenting clone) | Yes (check + repair) | Modern removable media (e.g. MiSTer SD cards). In-place resize trims trailing free space; the defragmenting clone (Compact Space toggle / shrink-to-minimum) repacks allocated clusters into a fresh, smaller volume, so a fragmented card backs up to ~its real data size. fsck reconciles the allocation bitmap against the directory tree (lost / cross-linked clusters), checks the boot-region checksum + backup, and clears the VolumeDirty flag; repair rebuilds the bitmap and resyncs the boot regions, validated against `fsck_exfat`. |
| NTFS           | Yes    | Yes  | Yes (in-place + defragmenting clone) | Yes (check + repair) | Windows NT / 2000 / XP. In-place resize trims trailing free space; the defragmenting clone (Compact Space toggle / shrink-to-minimum) repacks into a fresh, smaller NTFS volume (from-scratch clean-room formatter, validated to mount under ntfs-3g). Create blank volumes with `rb-cli new volume ntfs` (selectable `--cluster-size` / `--sector-size`, 512 B–2 MiB clusters); the defragmenting clone inherits the source volume's cluster and sector size. fsck reconciles `$Bitmap` against the MFT walk, resyncs `$MFTMirr` and the backup boot sector, and clears the VolumeDirty flag; repair rewrites these metadata structures, validated against Windows `chkdsk`. Edit mode maintains real directory B-trees (a full resident `$INDEX_ROOT` is promoted to an `$INDEX_ALLOCATION` index and full nodes split), so directories take arbitrarily many entries, and created files inherit the parent's ACL. Verified on Windows 7: a volume we format and populate mounts clean, `chkdsk` reports no problems, an executable we wrote runs from it, and Windows itself adds files to directories we built. |
| HPFS           | Yes    | Yes  | —               | Yes (check) | OS/2 High Performance File System (OS/2 1.2 → Warp 4, eComStation, ArcaOS) — the defining OS/2 filesystem, shares MBR type `0x07` with NTFS/exFAT and is told apart by the super/spare-block magics. Browse + backup + edit (create / delete files and directories, including the B-tree dnode split + delete-rebalance) + create blank volumes with `rb-cli new volume hpfs`. fsck walks the dnode B-tree and fnode/anode allocation trees, detects cross-links, and reconciles the per-band free-space bitmaps + the directory-band dnode bitmap. Cross-validated against a clean-room reference (`scripts/hpfs-oracle.py`) modeled on the Linux kernel `fs/hpfs` on-disk structures; resize/shrink is future work. |
| ext2 / ext3 / ext4 | Yes | Yes | Yes (backup-compaction group shrink) | Yes (check + repair, incl. ext4 `metadata_csum`) | Early Linux installs onward. Create blank volumes with `rb-cli new volume ext` (plain ext2), `volume ext3` (adds a jbd2 journal), or `volume ext4` (extents + `metadata_csum` + journal); all `e2fsck`-clean. fsck reconciles the block + inode bitmaps and free counts (Pass-5 style) and repairs them, validated against `e2fsck`. On `metadata_csum` (ext4) volumes the crc32c on the superblock, descriptors, bitmaps, inodes, and directory blocks is verified and recomputed — so fsck-repair and in-place edits (create/delete/rename) keep the volume `e2fsck`-clean. The backup compactor packs a volume into fewer block groups (`resize2fs`-grade: flex_bg metadata migration, dropping the resize inode, and relocating the journal + multi-block files as contiguous runs), so a lightly-used real-world ext4 backs up to ~its real data size. Restoring at original size grows it back by adding block groups (the mirror `resize2fs`-grade grow) to fill the partition; both directions verify `e2fsck`-clean. (A grow whose GDT would need more blocks — e.g. a 1 KiB-block sub-512 MiB volume gaining groups — is left for `resize2fs`; 4 KiB-block ext4 up to 8 TB grows in place.) |
| SquashFS (v4.0) | Yes | Yes (whole-image rebuild) | — | — | The read-only compressed filesystem Linux appliance and live images are built from — Buildroot roots, and `casper/filesystem.squashfs` inside practically every live-CD ISO. Browse + read files and symlinks, including multi-block files and fragment-packed tails. **Edit** (create / delete / mkdir / rename / symlink) works by rebuilding the whole image, exactly as `mksquashfs` does — the format has no in-place write. Unchanged files keep their exact bytes, permissions, ownership, timestamps and extended attributes (e.g. `security.capability`); new files get their POSIX attributes from the shared resolver (`--mode` / `--uid` / `--gid`, else inherited), and a file you *replace* keeps the extended attributes of the one it displaced — so overwriting a capability-bearing binary doesn't silently strip its `security.capability`. Extended attributes are scriptable with `rb-cli xattr list|set|rm`, not just editable in the GUI. Rebuilds within ~0.4% of `mksquashfs`'s size on a real rootfs and are accepted by `unsquashfs`. Compressors read: gzip, XZ, LZMA, LZ4, zstd; **write: gzip / XZ / zstd** (an LZMA / LZ4 / **LZO** image is refused for edit by name rather than silently re-compressed). Auto-detected by its `hsqs` magic as a superfloppy, inside an MBR type-0x83 partition, **appended to an AppImage's ELF stub**, or as a **file inside a live-CD ISO** (`casper/filesystem.squashfs` and friends, found by content wherever the distribution puts it) — and editable in the first three. The ISO form is browse + extract only, since its payload sits between other files on the disc and cannot grow; an edit is refused up front with that explanation rather than part-way through a rebuild. Because a rebuild's size can only be bounded and never predicted, an edit declares a **size budget** first — `rb-cli squashfs plan` shows the numbers to choose from, `rb-cli squashfs put` / `rm` take `--size` / `--grow`, and the GUI asks before entering Edit Mode. A rebuild that overruns is refused with the original untouched, and a partition-hosted image can never outgrow its partition. A bare `.squashfs` is replaced atomically (temp + fsync + rename), so a crash mid-save cannot destroy the only copy. **Create** a new image from a host directory with `rb-cli squashfs create DIR IMG` (like `mksquashfs`; gzip / XZ / zstd), and **structurally verify** one with `rb-cli squashfs verify` — a full traversal that decompresses every metadata block, inode, dirent and data block and cross-references them. That is *not* an fsck: SquashFS carries no checksums, so corruption shows up as a decompression or reference failure and there is nothing to repair from. |
| HFS (Mac OS Standard) | Yes | Yes | Yes         | Yes (check + repair: replica copy, bitmap fixup, lost+found for orphans) | Classic Mac OS 68k / early PowerPC. Includes block-size expansion via clone (`Expand HFS Volume…`). Volumes written by rusty-backup are `fsck_hfs`-clean and mountable on real Mac OS (verified on Mac OS X 10.4). |
| HFS+ / HFSX    | Yes    | Yes  | Yes (defrag clone) | Yes (check + repair) | Mac OS Extended; hardlink resolution. Create blank volumes with `rb-cli new volume hfsplus` (`--case-sensitive` for HFSX). The catalog / extents-overflow / attributes B-trees grow their backing fork on demand, so an under-sized or foreign catalog fills in place without a spurious "disk full"; all written volumes are `fsck_hfs`-clean and mountable on macOS. **POSIX permissions** (`HFSPlusBSDInfo` — how OS X carries mode / uid / gid on HFS+) are read and written: they show in the browse views and are editable with `rb-cli chmod` / `chown`. A record whose `fileMode` is 0 has no POSIX info recorded (a Classic Mac OS volume, say) and reads as "no permissions" rather than mode 0000; setting either one populates the block. |
| MacPlus MFS    | Yes    | Yes  | — (fixed floppy geometry) | Yes (check + repair) | Macintosh File System — the original flat Mac filesystem for 128K / 512K / Plus 400 KB single-sided floppies (pre-HFS, 1984-86). Read + edit + create blank volumes with `rb-cli new floppy mfs` (400 KB / 800 KB, geometry solved under the 12-bit-map 4094-block ceiling). fsck reconciles the volume allocation-block map against every file's data + resource fork chains (broken / cross-linked chains, leaked blocks, free + file counts); repair reclaims unreachable blocks and resyncs the MDB counters, withheld on structural damage. **Resource forks** round-trip both ways, like HFS: extracted (`get-binhex`, MacBinary / AppleDouble export, `cp`, remote) *and* written (`put-binhex`, `put-macbinary`, `setrsrc`, fork-carrying `cp` into MFS) — each fork stored as its own allocation-block chain. Applies to Twiggy / FileWare `.dc42` disks too (they de-interleave to a flat MFS volume). |
| APFS           | Yes    | No   | —               | —    | Apple File System (macOS 10.13+). **Read-only browse + extract**, including **FileVault-encrypted** volumes. Walks the container (NXSB) → object map → volume superblock (APSB) → catalog B-tree, resolving virtual objects through the omap and verifying the Fletcher-64 block checksums; file data is streamed from its extents (sparse holes read as zeros) and symlink targets come from the `com.apple.fs.symlink` xattr. Classic-Mac **resource forks** (from the `com.apple.ResourceFork` xattr, both inline and stream-backed) and **type/creator/Finder flags** (from `com.apple.FinderInfo`) are read too, so `rb-cli du` counts both forks and `get-binhex` / MacBinary / AppleDouble export a faithful fork + metadata. **Encryption:** supply the volume password or personal recovery key (`--password` in the CLI); the driver walks the container/volume keybags (AES-XTS), unwraps the KEK via PBKDF2 and the VEK via RFC-3394, and AES-XTS-decrypts the catalog and data (no Apple ID / Secure-Enclave path — decryption is fully offline and cross-machine). Browses the container's first volume (GPT `Apple_APFS` partition or a raw container image). **Snapshots** and any edit / shrink / fsck are out of scope (see `docs/apfs_support_plan.md`). |
| btrfs          | Yes    | No   | Yes (volume resize) | —    | Modern Linux; read-only browse (no file-level edit). |
| JFS (JFS2)     | Yes    | Metadata only | Yes (compaction) | Yes (check + repair) | IBM OS/2 Warp Server, AIX 5+, Linux JFS2. Browse + backup compaction + integrity check, plus permission / ownership editing; creating, deleting and renaming entries is not implemented. fsck repair reconnects orphaned (allocated-but-unreachable) inodes into `/lost+found`, verified against Linux `fsck.jfs`. Legacy AIX JFS1 is detected but not read. |
| ReiserFS (v3)  | Yes    | No   | Yes (compaction) | —    | Linux, late-1990s to mid-2000s. v3.5 / v3.6 read-only browse + backup compaction. Reiser4 is detected but not supported. |
| Minix (V1/V2/V3) | Yes  | Yes  | —               | Yes (check + repair) | Minix, and Linux's original filesystem (pre-ext). Browse + edit (create / delete / rename / **symlink**) of all three on-disk generations: V1 (magic 0x137F/0x138F, 14/30-char names), V2 (0x2468/0x2478), V3 (0x4D5A, 60-char names). Symlink targets are bounded by one block, as `minix_symlink` bounds them. Create blank volumes with `rb-cli new floppy minix` (V1), `volume minix2`, or `volume minix3` — geometry matches `mkfs.minix`. fsck reconciles the inode + zone bitmaps and link counts against the directory-tree walk and adopts orphaned inodes into `/lost+found`. Auto-detected as a raw floppy / hard-disk superfloppy or MBR type 0x81. Every write is validated against Linux `fsck.minix`. |
| UFS / FFS (UFS1 / UFS2) | Yes | Yes | Yes (compaction) | Yes (check + repair) | BSD Fast File System — 4.2/4.4BSD, FreeBSD, SunOS / Solaris, NeXTSTEP, MIPS RISC/os. Read + edit (create / delete / rename / **symlink**, fixture-validated), backup compaction, and fsck with replica-superblock / bitmap / orphan repair. Both byte orders, and both generations of `struct cg` and `struct direct`: 4.4BSD's `d_type` byte plus the 16-bit `d_namlen` that Solaris, SunOS and NeXTSTEP still use (chosen by `fs_maxsymlinklen <= 0`, the kernel's own test), and the 4.3BSD cylinder-group header whose magic sits at byte 980 with the bitmaps at compile-time offsets rather than the ones a 4.4BSD header stores. Fast symlinks are recognised by `di_blocks == 0` rather than by `fs_maxsymlinklen`, which a 4.3BSD superblock never set. Symlinks are written in whichever form the target's length calls for — inline in the dinode when it fits (60 bytes on UFS1, 120 on UFS2), in data blocks when it doesn't. File creation builds the single- and double-indirect levels as needed (triple indirection, which starts around 32 GiB at 8 KiB blocks, is refused). `rb-cli new volume ufs` formats a blank **UFS1** volume in either byte order, following `newfs(8)`'s own `initcg` sequence: 8 KiB blocks over 1 KiB fragments (`--block-size` moves both), one inode per 4 fragments (`--bytes-per-inode`), and the largest cylinder group whose header, bitmaps and cluster maps still fit in one block. **Both `struct cg` generations are written**: `new volume ufs-43bsd` produces the pre-4.4BSD (NeXTSTEP / OPENSTEP) shape — 4.3BSD `struct cg`, 16-bit `d_namlen` directory entries with no `d_type`, device blocks the size of a fragment, cylinder groups staggered across tracks by `fs_cgoffset`, and live `fs_postbl` / `fs_rotbl` / `cg_btot` / `cg_b` rotational tables. That is what a `new hd next` partition needs: a 4.4BSD volume parses there but puts `d_type` where a NeXTSTEP kernel reads the high byte of a name length, so its root directory comes back claiming a 1025-character name. Every field of the 4.3BSD form is pinned against the NeXTSTEP 3.3 reference disks — for a same-sized volume our superblock matches all 45 checked fields plus `fs_postbl` and `fs_rotbl` byte for byte — and the result is **validated against a real NeXTSTEP 3.3** under the Previous emulator, which parses our disk label and boots from a volume our writer has modified. Directory chunking follows the volume's own `DEV_BSIZE` (`fs_fsize >> fs_fsbtodb`) — 512 on BSD but **1024 on NeXTSTEP** — and the allocator keeps FFS's block-versus-fragment accounting (`cs_nbfree` for wholly free blocks, `cs_nffree` only for loose fragments), propagating every change to the cylinder-summary area at `fs_csaddr` and to `fs_cstotal`, which is what a real `fsck` cross-checks. |
| ProDOS         | Yes    | Yes  | Yes             | Yes  | Apple II / IIgs (`.po` / `.hdv` / `.2mg`, 8 KiB–32 MiB). Create blank volumes with `rb-cli new volume prodos` (boot + 4-block volume directory + bitmap). fsck reconciles the volume bitmap against the directory-tree + file-index walk (seedling / sapling / tree files + subdirectories, the CBM VALIDATE model): reclaims leaked blocks and flags blocks in use but marked free; repair rebuilds the bitmap, withheld on cross-links / past-end blocks / directory-chain cycles. |
| Apple DOS 3.3  | Yes    | Yes  | —               | Yes  | Apple II. 140 KB 5.25" disks (`.dsk` / `.do` / `.po`, sector-order auto-detected). Gated to the exact 140 KB geometry. Create blank (non-bootable) data disks with `rb-cli new floppy apple-dos`. fsck reconciles the VTOC free map (bit-set-is-free) against the catalog + file T/S-list chains (the CBM VALIDATE model): reclaims leaked sectors, flags sectors in use but marked free, surfaces cross-links / broken chains read-only; the DOS-image region on tracks 0-2 is reported as a benign warning, never reclaimed. |
| UCSD p-System  | Yes    | Yes  | —               | Yes (check + repair) | UCSD Pascal (Apple II/III, PC, and many late-1970s/80s machines). A flat single directory of up to 77 **contiguous** files (no subdirectories, no allocation bitmap). Browse + edit (create / delete / rename — contiguous first-fit allocation; a create that won't fit any single free run is refused rather than fragmenting). Create blank volumes with `rb-cli new floppy ucsd` (alias `pascal`). fsck validates the directory self-consistency (volume label, in-bounds non-overlapping runs, file count); repair re-sorts entries, corrects the count, and drops invalid entries, while overlaps / past-end runs are surfaced read-only. Auto-detected via the block-2 volume label, with byte order recovered from the label's `DLASTBLK`. Every write is validated against an independent clean-room oracle (`scripts/ucsd-oracle.py`). |
| Apple Lisa File System | Yes | No | — | — | Apple Lisa (Office System 1.0 / 2.0 / 3.0), read-only browse + extract. Opened from tag-bearing DiskCopy 4.2 (`.dc42` / `.image`) and DART images. Files are reconstructed from the 12-byte sector tags (file ID + file-relative block) — independent of the three catalog formats — so extraction works on every disk; friendly names come from the flat-table (`0x0e`) / flat-hash (`0x0f`) catalog, while the hierarchical B-tree version (`0x11`, Office System 3.0) falls back to `file-XXXX`. The metadata label (first 0xF0 bytes) is stripped from each file's data fork. Requires an image that still carries tags (DiskCopy versions after 4.2 strip them). Mirrors Ray Arachelian's `lisafsh-tool`. |
| CBM DOS (1541 / 1571 / 1581 / 8050 / 8250) | Yes | Yes | — (floppy, fixed geometry) | Yes | Commodore C64 / C128 / C16 / VIC-20 / PET. PETSCII names, bit-set-is-free BAM, linked-sector files. `.d64` / `.d71` / `.d81` / `.d80` / `.d82`; `.g64` GCR decoded to sectors. fsck = CBM DOS VALIDATE (BAM reconciliation: leaked / used-but-free blocks, free-count fixes), rewrite cross-validated byte-for-byte against `c1541 validate`. |
| Atari DOS 2 (2.0S / 2.5) | Yes | Yes | — (floppy, fixed geometry) | Yes | Atari 8-bit (Atari800 core). VTOC@360 (bit-set-is-free), 64-file directory, linked-sector files. Single + enhanced density `.atr` / `.xfd`. Create blank single-density disks with `rb-cli new floppy atari`. fsck reconciles the VTOC bitmap + free-sector count against the directory's linked-sector chains (the CBM VALIDATE model): reclaims leaked sectors, flags sectors in use but marked free, surfaces cross-links / broken chains read-only; repair rewrites the bitmap + count (single density — the DOS 2.5 VTOC2 upper region is left unchecked). |
| RS-DOS (CoCo Disk BASIC) | Yes | Yes | — (floppy, fixed geometry) | Yes | Tandy Color Computer (CoCo2 / CoCo3 cores). Granule allocation table on track 17, 72-file directory, granule-chain files. Raw 35- / 40-track `.dsk` / `.jvc`. Read/extract + add/delete bidirectionally cross-validated against an independent clean-room reader/writer derived from the toolshed `libdecb` semantics. fsck reconciles the granule table against the directory file chains (the CBM VALIDATE model): reclaims leaked granules into the free list, surfaces cross-linked granules and broken chains read-only. |
| OS-9 / NitrOS-9 RBF | Yes | Yes | — (floppy, fixed geometry) | Yes | Tandy Color Computer (CoCo2 / CoCo3 cores) and Dragon. Hierarchical Unix-like FS: LSN-0 identification sector, per-file/dir 256-byte file descriptors, segment-list extents, allocation bitmap (set-bit = allocated). Raw `.dsk` / `.vdk`. Read/extract + add/delete (incl. subdirectories) cross-validated byte-exact against an independent clean-room RBF reader on real NitrOS-9 toolshed disks. Create blank 35-track floppies with `rb-cli new floppy os9`. fsck walks the directory tree from the root FD (FDs + segment-list runs, recursing into subdirectories) and reconciles it against the cluster bitmap: a referenced cluster marked free is a repairable error; allocated-but-unreferenced clusters (the boot area or a reserved track — a real NitrOS-9 disk reserves its last track) are surfaced as a benign warning, never freed; cross-links / past-end segments are read-only. |
| DragonDOS | Yes | Yes | — (floppy, fixed geometry) | Yes | Dragon Data Dragon 32/64 (and CoCo running DragonDOS), MiSTer Dragon core. Directory track 20 (backup on 16), one's-complement geometry signature, sector bitmap (set-bit = free), 25-byte directory entries with header + continuation extent blocks. Raw single- / double-sided 40-track `.dsk`. Read/extract + add/delete cross-validated byte-exact against an independent clean-room reader/writer and against real third-party DragonDOS disks (empty + a populated 9-file disk). fsck reconciles the sector bitmap against the directory extent chains (the CBM VALIDATE model — leaked / used-but-free sectors), rewriting both the main and backup directory-track copies. |
| Acorn DFS | Yes | Yes | — (floppy, fixed geometry) | Yes | BBC Micro / BBC Master / Acorn Electron (MiSTer BBCMicro / AcornElectron cores). Flat-catalogue FS in sectors 0–1: 12-char disc title, up to 31 contiguous files in descending start-sector order, single-character directory namespaces, 18-bit load/exec/length. Single-sided `.ssd` (40-/80-track) opens as one volume; double-sided `.dsd` de-interleaves its two track-interleaved sides into two Acorn DFS partitions (`IMG@1` / `IMG@2`), edits re-interleaving on save. Read/extract + add/delete bidirectionally cross-validated byte-exact against an independent clean-room DFS reader/writer (locked files, non-`$` directories, real load/exec addresses all round-trip). fsck verifies the contiguous-file catalogue is self-consistent — flags overlapping and out-of-bounds files read-only, and reorders a scrambled catalogue back into canonical descending start-sector order as the one safe repair. |
| Acorn ADFS / FileCore | Yes | Yes | — (fixed floppy geometry) | Yes (new-map) | Acorn Archimedes, BBC Master, RISC OS. Read + browse + edit (create / delete / rename) on **both** the **new-map** layouts (E / F / HD) and the **old-map D-format** — the latter validated against real Repton 3 / Lemmings floppies, with writes leaving the old-map checksums valid so RISC OS still mounts the disc. `.adf` floppy and bare / Arculator-wrapped `.hdf` HDD. Create blank E-format floppies with `rb-cli new floppy adfs` (800 KB, single-zone new-map FSM + empty Hugo root). fsck (new-map) verifies the FSM zone checksums + cross-check and reconciles the allocated fragments against the directory-tree walk (dangling references, leaked fragments); repair re-stamps the zone checksums (a full FileCore map rebuild — RISC OS `*CheckMap` — is left for leaked-fragment reclamation). |
| TI-99 | Yes | Yes | — (floppy, fixed geometry) | Yes (check + repair) | TI-99/4A (MiSTer TI-99_4A core). A proper little filesystem (unlike the contiguous CoCo/ZX floppies): a Volume Information Block with an allocation bitmap (bit set = used), a sorted File Descriptor Index of File Descriptor Records, and **extent-based** files (a packed 3-byte-per-cluster chain). All fields big-endian (TMS9900). Flat V9T9 `.dsk` (SSSD 90K / DSSD / SSDD 180K / DSDD 360K); geometry read from the VIB. Browse + edit (create / delete / rename): create allocates from the bitmap, writes an FDR + cluster chain, and inserts a sorted FDIR pointer; delete frees the bitmap bits (files are stored as PROGRAM files). Create blank volumes with `rb-cli new floppy ti99` (alias `ti99_4a`); the geometry follows `--size` (smallest of 90 KB SSSD / 180 KB DSSD / 360 KB DSDD that covers it, DSDD default). fsck reconciles the VIB bitmap against the directory walk (FDIR -> FDR -> cluster chain): structural checks (in-range FDR/data sectors, chain-length agreement, FDIR sort order, cross-linked sectors) then the bitmap comparison; repair rebuilds the bitmap and is withheld on structural damage. Content-detected via the "DSK" volume marker. Every write is cross-validated against both MAME's `imgtool` reader and an independent clean-room oracle (`scripts/ti99-oracle.py`). |
| TR-DOS | Yes | Yes | — (floppy, fixed geometry) | Yes (check + repair) | ZX Spectrum Beta Disk (MiSTer ZX-Spectrum core). A flat single directory of up to 128 **contiguous** files (no subdirectories, no allocation bitmap); files pack from the first data sector and a first-free high-water mark advances on save, never retreating on erase (space is reclaimed only by TR-DOS's MOVE). Raw `.trd` — geometry (80-/40-track, single-/double-sided) comes from the disk-info sector's type byte; addressing is a flat logical-track sector array. Browse + edit (create / delete / rename): create appends at the high-water mark, delete tombstones the entry (name byte 0x01, deleted-count bumped, sectors kept — authentic TR-DOS). A file addresses its type via a `NAME.T` suffix (e.g. `LOADER.B`, `SCREEN.C`). Create blank volumes with `rb-cli new floppy trdos` (alias `beta`); the geometry follows `--size` (smallest of 160 KB / 320 KB / 640 KB that covers it, 640 KB default). fsck verifies the disk-info signature and the contiguous catalogue packing, then reconciles the disk-info counters (file / deleted counts, first-free pointer, free-sector count) against a catalogue walk; repair recomputes those counters, and is withheld when the packing itself is damaged (non-contiguous / past-end entries are surfaced read-only). Auto-detected via the disk-info sector's id byte (0x10). Every write is cross-validated against an independent clean-room oracle (`scripts/trdos-oracle.py`). |
| Oric Jasmin | Yes | Yes | — (floppy, fixed geometry) | Yes (check) | Oric-1 / Atmos / Telestrat (MiSTer Oric core), the Jasmin floppy controller's filesystem. Flat 256-byte-sector `.dsk` (single-sided 178 KB / 697 blocks, or double-sided 356 KB). A free map (3 bytes per 17-sector track, set-bit-is-free) at block 340, a chained directory of 18-byte entries from block 341, and sector-list "inode" files (an inode block lists its data sectors, chaining to more inode blocks past 125 sectors). Browse + edit (create / delete files) + create blank volumes with `rb-cli new floppy oric` (alias `jasmin`). fsck walks the directory chain + each file's inode/data sectors, detects cross-links, and reconciles the free map. Modeled on MAME's `fs_oric_jasmin` and cross-validated against `floptool` (byte-identical formatter; `floptool flopdir` independently confirms every file's name + length). Sedoric — the more common Oric DOS — is a separate future target. |
| Human68k (FAT12 / FAT16) | Yes | Yes | Yes (HDD in-place grow + shrink, plus defragmenting repack) | Yes | Sharp X68000. SASI/SCSI hard disks use a Sharp/KG big-endian BPB + big-endian FAT; floppies use standard little-endian FAT. Shift-JIS 18.3 filenames. Shrink stays above the FAT16 floor. `rb-cli repack` / the Inspect-tab "Defragment…" button repack the volume contiguously, reclaiming holes left by deleted files. fsck reconciles the FAT against the directory tree (the CBM VALIDATE model): reclaims lost clusters, resyncs the backup FAT copy from the primary, and surfaces cross-linked clusters and broken chains read-only. |
| CP/M (2.2 / 3 / Plus) | Yes | Yes | — | Yes | Amstrad CPC / PCW, Tatung Einstein, Spectravideo SV-328, MITS Altair, Grant Searle MultiComp, ZX Spectrum +3. Nine built-in disk-parameter blocks (DPBs); CP/M has no on-disk signature, so the format is chosen explicitly (`--fs-type cpm:<preset>`, or `rb-cli new floppy cpm --cpm-preset <name>` to format a blank disk). fsck is a directory self-consistency check (CP/M keeps no on-disk free map): it flags cross-linked blocks, out-of-range block pointers, and invalid directory entries (CP/M 3 disk-label / timestamp entries are recognized as valid); repair reclaims invalid entries — cross-links have no redundant metadata to recover from, so they are surfaced read-only. |
| QDOS (QXL.WIN) | Yes | Yes | Yes (in-place resize) | — | Sinclair QL hard-disk container. Read + write + resize; the per-file 64-byte QDOS header is preserved. Microdrive `.mdv` is detect-only. |
| AFFS (OFS / FFS)  | Yes | Yes | Yes (in-place; bm_pages only) | Yes (Amiga Disk Validator) | Amiga `DOS\0`..`DOS\7`. Volumes we write mount **Read/Write under a real Kickstart 3.1** (FS-UAE, `oracles/fsuae/affs_mount.py`) and are accepted by amitools' `xdftool`. In-place resize relocates root + bitmap pages; refuses on bm_ext-chain volumes or when allocated data would be clobbered. |
| PFS3 / PDS3 / muFS | Yes | Yes | Yes (in-place + defragmenting clone) | Yes (validate + repair) | Amiga PFS3 family. Create blank volumes with `rb-cli new volume pfs3`; a bare (RDB-less) PFS3 image is detected at sector 0 by its boot magic and routed to the driver by DosType. Shrink refuses to truncate live data; clone path packs the volume for genuinely smaller targets. fsck walks the directory tree + anode chains and reconciles both the data and reserved allocation bitmaps (plus the free-block counters) against the walk, rebuilding them when the structure is intact — the classic "validation needed" after an unclean unmount. PFS3 has no block checksums, so structural damage is surfaced read-only rather than silently rewritten. |
| SFS (Smart File System) | Yes | Yes (single-leaf btree) | Yes (in-place trim/grow) | Yes (validate + repair) | Amiga `SFS\0` / `SFS\2`. Create blank volumes with `rb-cli new volume sfs`. fsck validates every metadata-block checksum, walks the AdminSpaceContainer chain + object tree, and reconciles the single block bitmap against the walk, rebuilding it when the structure is intact. Repair touches only bitmap blocks, so it is safe regardless of btree depth. |
| SGI EFS        | Yes    | Yes  | Yes (in-place grow + conservative + aggressive shrink) | Yes (check + repair: replica copy, bitmap fixup, lost+found) | IRIX < 6.0. Volumes we write pass **IRIX 6.5's own `fsck`** on all five phases, and IRIX mounts them and writes into them (R-039). Symbolic links are read **and written**, so `untar` restores an archive's links instead of dropping them. Aggressive shrink renumbers inodes into low CGs. Resizes move in whole cylinder groups (`fs_size == firstcg + ncg * cgfsize`, as `mkfs_efs` lays it out), so a target size rounds down to a group boundary. Files are read **and written** through indirect extents, up to EFS's 2 GiB per-file limit. |
| SGI EFS v1     | Yes    | Yes  | Yes (in-place grow + conservative shrink) | Yes (check + repair) | The **original** Extent File System, on IRIS 2000 / 3000 series disks (1986-1988) — the ancestor of IRIX EFS above, not a dialect of it. Different magic (`0x041755` vs `0x072959`), a shorter superblock packed for the 68020's 2-byte alignment, and System V directories (a 16-bit inode plus a 14-byte name) instead of EFS's `0xBEEF` slotted directory blocks; only the inode and extent layout is shared. Reads regular files, directories, symlinks, device nodes, sparse holes, and indirect extents. **Auto-detects byte-swapped images**, which is how disks off period SGI controllers dump; `inspect` and `ls` report which orientation an image is in, and `rb-cli swab16` converts between them. Arrives through the SGI disk label (`SGI-DkLabel`) or by probing a bare partition image. **Writable**: create / delete / rename files and directories, symlinks, permissions and ownership, with indirect extents for large files. Grows and shrinks in place (whole cylinder groups; a shrink that would cut live data is refused), and `rb-cli fsck` checks and repairs — bitmap rebuild, counter and checksum drift, and orphan adoption into `lost+found`, with repair refusing to touch a structurally damaged volume. Writes are byte-order symmetric, so a byte-swapped volume stays internally consistent. Create blank volumes with `rb-cli new volume efs-v1` (written in native order). Verified file-for-file against a real IRIS 3130 `Priam V170` disk — 2,982 entries, every SHA-256 matching an independent decoder, and writing into that disk leaves every pre-existing file byte-identical with a valid superblock checksum — and **validated by IRIX 3.7 itself**: `scripts/sgi-efs-v1-oracle.sh prove` has rb-cli rewrite-and-grow `/etc/rc.s0`, create a file, create a directory and allocate a 256 KiB file, then boots that disk under the Motion IRIS 3130 emulator, where `init` runs the rewritten script, `cat` reads the new file and directory, System V `sum` agrees to the byte, and `/etc/mount` mounts the second EFS v1 filesystem off `md0c`. The layout is taken from SGI's own `<sys/efs_sb.h>` / `<sys/efs_ino.h>` / `<sys/dir.h>`, recovered from that disk. See [`docs/SGI_EFS_v1.md`](docs/SGI_EFS_v1.md). |
| SGI XFS (v4 / v5) | Yes | Yes (v4 only; v5 editing pending) | Grow only — disk-layout "Add free space" + in-OS `xfs_growfs`. **Known limitation: no backup compaction or in-place shrink** (XFS backups are written full-size); clone-into-fresh shrink is planned (see [`docs/OPEN-WORK.md`](docs/OPEN-WORK.md) §2.2) | Yes (R1-R8 repair pipeline; v4 oracle-validated) | IRIX 6.x and Linux. `xfs_repair`-clean writes. Create blank volumes with `rb-cli new volume xfs` — a v5/CRC filesystem (4 KiB blocks, 512-byte inodes, internal log, `ftype` on and the optional btrees off), minimum 32 MiB. Validated against xfsprogs 6.6 `xfs_repair -n` from 32 MiB to 16 GiB (`scripts/xfs-oracle.sh sweep`), which reaches well below the 300 MB floor `mkfs.xfs` itself imposes. |
| Alto BFS / TFS | Yes | Yes | Yes (resize) | Yes | Xerox Alto Basic File System on Diablo 31/44 packs **and the same file system on Trident T-80/T-300 (TFS)** — one codec parameterized by page size (512 vs 2048 B), label shape (8- vs 10-word), and disk-address width (1- vs 2-word). Flat SysDir namespace, leader pages, page-chain files, and **out-of-band sector labels** (the file structure lives in the labels, not the data area). Browse + extract + add/delete + resize; opened from `.pdi` / `.bfs` / `.copydisk` / `.altodisk` / Salto `.dsk` / Trident pack images (edits save as PDI). Diablo validated against every CopyDisk pack in the CHM Xerox PARC archive + the Salto/dorado disks; Trident validated against ContrAlto2's real Spruce print-server T-300 pack (plus synthetic round-trip for the write path). fsck reconciles the DiskDescriptor free-page bitmap + count against the file page-chains (VALIDATE model), flagging overlaps / broken chains read-only and rebuilding the bitmap as the repair; `rb-cli fsck --repair` writes the fix back as a PDI (in place for a `.pdi` input). |
| Pilot / Cedar | Yes | No (read-only in GUI) | — | — | Xerox D-machine Pilot/Cedar nucleus filesystem (Dolphin/Dorado/Dandelion), structurally unrelated to BFS: physical/logical volume roots (seals `121212`₈ / `131313`₈), a subvolume table, the VAM free bitmap, and extent-based files behind **out-of-band sector labels**. Both file-ID generations (32-bit Cedar nucleus / 80-bit original Pilot) and both label schemes (Cedar-nucleus + classic Pilot 12.3). Browse + extract files in the GUI (enumerated by page-label scan across all subvolumes; the nucleus has no name directory, so real names come from the Cedar **client name directory** — the FS name->FileID B-tree in `rootFile[client]`, decoded when present — then from each file's leader page (XDE volumes name ~90% of files this way, ViewPoint names its boot/system files), and otherwise are synthesized from the file ID); blank-volume creation + add/delete files + **installing a client name directory** (`pilot_probe set-dir`) via `pilot_probe`. Validated against real ViewPoint 2.0 / XDE 5.0 volumes from the Dwarf 6085 emulator (`.zdisk`) as well as round-trip. (ViewPoint *client* files have no on-disk name — no leader name and no Pilot central directory; their names live in the desktop / NS-Filing layer, not on the local disk — so they surface by ID.) See the PARC specs under `docs/`. |
| BFS (BeOS / Haiku) | Yes | Yes | — | Yes (check + repair) | The Be File System, from BeOS DR9 / PR / R3-R5 through Haiku. Create blank volumes with `rb-cli new volume bfs` (`--big-endian` for the BeOS/PPC byte order, `--block-size` for the block size; minimum 72 MiB, because the root directory lives in allocation group 8). 64-bit extent filesystem with B+tree directories and inline `small_data` attributes. **Both byte orders**: BeOS/Intel is little-endian with the superblock at byte 512, BeOS/PPC is big-endian with it at byte 0 (which is also where a bare, partition-less BFS volume keeps it). Opened from MBR type `0xEB`, APM `Be_BFS`, or a bare volume. Edit covers create / delete / mkdir / rename / symlink / chmod / chown, with leaf splits that promote a separator and grow a new B+tree root. **Editing is refused on a volume whose journal is non-empty or whose flags say `DIRT`** — we do not maintain the log, and BeOS would replay it over our changes; unmount cleanly or run `chkbfs` first. Index directories and out-of-line attributes are read through as ordinary files, not surfaced as attributes. fsck reconciles every block the tree owns — including per-file attribute directories, which hang off the inode rather than off a path — against the allocation bitmap, and repairs the two things the volume proves: leaked bitmap bits and a wrong `used_blocks`. Structural findings are reported and left alone; BeOS ships `chkbfs`. Validated clean against the BeOS R5 (13,301 files) and both BeOS/PPC volumes, one of which had 3 genuinely leaked blocks. |
| BeOS OFS | Yes | Yes | — | Yes (check + repair) | Create blank volumes with `rb-cli new volume ofs`. The *old* Be filesystem, from the 1993-94 Hobbit BeBox prototypes and the early PowerPC Developer Releases — everything before BFS arrived in DR9. No inodes: a file's metadata is entirely its parent directory entry, and directories are chains of 63-slot blocks. Files are stored contiguously where a free run allows and through an extent-list sector otherwise. Early BeOS typed files exactly the way the Mac Finder did, so type/creator codes appear in the same columns HFS uses. Big-endian throughout, 512-byte sectors, bare volume (no partition table). Edit covers create / delete / mkdir / rename. fsck reconciles the sector bitmap against the directory tree and repairs leaked sectors and a wrong `used_sectors` — the Hobbit image has 546 leaked sectors from files BeOS deleted without freeing. Version 1 is validated against the Hobbit image; versions 2 and 3 are implemented from the published `ofs-extractor` but untested. |
| Carve (raw recovery) | Yes (read-only) | No | — | — | Fallback for disks with **no mountable filesystem**: custom bootblock Amiga disks (demos / intros / diagnostics that boot from the boot block and write raw sectors — AmigaDOS labels these "NDOS"), and any superfloppy whose filesystem isn't recognized. Surfaces `whole-disk.img`, `bootblock.bin` (Amiga), and `carved-blkNNNNNN.{jsonl,json,txt}` for each recoverable run of contiguous text. Browse + extract only (`rb-cli ls` / `get`). Scans the first 10 MB by default; the browse-view **Full scan** toggle (CLI `--carve-full`) scans the whole image. |

Two more filesystems are **detect-only scaffolds** — Rusty Backup recognizes
them but cannot yet browse their contents: **ANDOS** (Soviet BK0011M /
Elektronika BK) and the Sinclair QL **Microdrive** (`.mdv`). The complete
engine-level matrix — every filesystem's detect / browse / edit / create /
shrink / fsck level, plus the filesystems *not* yet supported and why — lives
in [`docs/filesystem_coverage_audit.md`](docs/filesystem_coverage_audit.md).

### Optical disc filesystems

Optical discs are read through the
[`opticaldiscs`](https://github.com/danifunker/opticaldiscs-rs) engine and
surfaced in the **Optical** tab (and `rb-cli optical browse` / `extract`).
These are **browse + extract only** — no edit, resize, or fsck — and are read
from `.iso` / `.toast`, `.bin` + `.cue`, and CD/DVD `.chd` containers (a raw
2352-byte-sector image inside a bare `.iso` is auto-detected), plus the
Dreamcast `.gdi` track descriptor and the Nintendo GameCube / Wii container
family (`.gcm .rvz .wbfs .ciso .gcz .wia .tgc .nfs`; a raw GameCube/Wii dump in
a bare `.iso` is auto-detected by magic).

| Filesystem | Typical discs |
|------------|---------------|
| ISO 9660 (+ Joliet, Rock Ridge) | PC / Unix / mixed data CDs and DVDs. Joliet = Unicode long names; Rock Ridge = POSIX names, permissions, and symlinks. |
| High Sierra | Pre-ISO 9660 CD-ROMs (early Microsoft / IBM titles). |
| UDF | DVDs and data discs (UDF 1.02–2.01). UDF 2.50+ metadata-partition discs (Blu-ray) are detected only. |
| HFS / HFS+ | Classic Mac and Mac OS X CDs / DVDs, including "Mac/PC" hybrids — resource forks and type/creator preserved. |
| SGI EFS | IRIX install / distribution CDs (read via the SGI Volume Header). |
| UFS / FFS | Digital UNIX / Tru64 and SunOS / Solaris CDs, plus NeXT / OpenStep / Rhapsody discs. |
| VMS ODS-2 / Files-11 | OpenVMS (VAX / Alpha) discs. |
| GameCube / Wii | Nintendo GameCube (GCM/FST) and Wii discs. Wii encrypted partitions are decrypted internally — no key file required. Read via the `nod` crate, including the compressed `.rvz` / `.wbfs` / `.ciso` / `.gcz` / `.wia` containers. |
| CD-i | Philips CD-i (Green Book) discs — big-endian ISO 9660 variant. |
| 3DO Opera | Panasonic / 3DO game discs (big-endian block tree). |

Game discs also carry a **console / serial / title / region** identity line
(shown in the Optical tab, the browse header, and `rb-cli optical browse`),
recognized for PlayStation 1/2, Saturn, Mega-CD, Dreamcast, Neo Geo CD, PC-FX,
PC Engine CD, CD32, GameCube, Wii, CD-i, and 3DO.

### Partition tables

| Scheme | Parse | Edit (resize / add / delete / retype) | Notes |
|--------|:-----:|:-------------------------------------:|-------|
| MBR    | Yes   | Yes  | PC standard. Logical partitions inside an extended container are surfaced read-only. |
| GPT    | Yes   | Yes  | Primary + backup header rewritten with refreshed CRCs on every edit. |
| APM    | Yes   | Yes  | Apple Partition Map (68k / PowerPC Macs). `rb-cli optical new mac-hfs` / `mac-hfsplus` synthesizes a classic-Mac CD-ROM image from scratch — DDR + map + one `Apple_HFS` partition holding a blank HFS or HFS+ volume (Mac-only; no ISO 9660 side). |
| RDB    | Yes   | Bootable flag only; writes whole tables from scratch | Amiga `RDSK`. `rb-cli new hd rdb` lays down an RDSK plus a `PART` chain with the DosType tags you name (`DOS\3`, `PFS\3`, `SFS\0`, …), cylinder-aligned from `--heads` / `--sectors`; the output is read back cleanly by `amitools`' `rdbtool`. Editing an *existing* RDB is still bootable-flag-only, deferred until the DosEnv geometry story is settled. |
| SGI    | Yes   | Yes  | SGI Volume Header (IRIX). 16 fixed slots; checksum recomputed on every write; geometry (`vh_dp`) preserved across edits. `rb-cli new hd sgi-efs` synthesizes a dvh + EFS-root hard disk from scratch (IRIX 5.3-6.5). |
| SGI-DkLabel | Yes | Yes; also writes whole labels from scratch | The pre-IRIX SGI disk label, on IRIS 2000 / 3000 series disks. One `struct disk_label` at block 0: drive geometry, the alternate-block region, and 8 `{base, size}` slots with no type field — roles come from `d_bootfs` / `d_swapfs` / `d_rootfs`, and the whole-disk wrapper slots are excluded from the list. Big-endian, packed for the 68020's 2-byte alignment, and **auto-detects the byte-swapped images** period SGI disk controllers produce. The detected orientation is reported by `inspect` / `ls` and on the GUI's Inspect tab, which also offers a *Swap Word Order...* button; `rb-cli swab16` is the scriptable equivalent. Surfaces its slots to the SGI EFS v1 driver (browse / inspect / extract, and editing the volumes). The eight `{d_base, d_size}` slots are editable — resize / move / add / delete, plus moving `d_bootfs` — with edits written back in the image's own word order and partial slot overlap refused (whole-disk wrapper slots are allowed, since containment is how the label spells "the whole drive"). There is no per-slot type field, so `set-type` is refused with that reason. `rb-cli new hd sgi-dklabel` writes a fresh label with the slots you size and give a role (`root` / `swap` / `boot` / `slice`), cylinder-aligned from `--heads` / `--sectors`, plus the whole-disk wrapper slot every label of the era carries; on the reference geometry (987c/7h/17s) the slots land on the same blocks a real IRIS 3130 uses. Fill them with `rb-cli new volume efs-v1` and `--fill N=PATH`, then `rb-cli swab16` if the target machine wants the controller's reversed-word order. Full-disk backup is future work. |
| AHDI   | Yes   | No (browse); writes whole tables from scratch | Atari ST / TT / Falcon hard disks. Four primary entries at 0x1C6 plus XGM extended chains, big-endian, no magic number — detection keys off the 0x1234 word-sum and plausible geometry. `rb-cli new hd atari` writes a fresh root sector with the tags you name (GEM / BGM / RAW); a GEM partition over 16 MiB is promoted to BGM, which is what TOS needs. Creating an XGM chain, and grafting in a bootable bootstrap, are future work. |
| Sun    | Yes   | No (browse); writes whole labels from scratch | Sun disk label / SMI VTOC (SPARC Solaris / SunOS). 8 big-endian slices (magic `0xDABE`), geometry-derived offsets; the whole-disk "backup" slice is excluded from the list. Surfaces the UFS slices to the existing big-endian-SPARC UFS reader (browse / inspect / extract). `rb-cli new hd sun` writes a fresh label with the slice tags you name (`root`, `usr`, `swap`, … or a bare tag number), cylinder-aligned from `--heads` / `--sectors`, with slice 2 reserved for the whole-disk alias. Parser and writer both cross-validated against `fdisk` / `sfdisk`; editing an existing label and full-disk backup are future work. |
| NeXT   | Yes   | Yes  | NeXT disk label (NeXTSTEP / OPENSTEP, black m68k hardware **and** NeXTSTEP/Intel). Up to 8 partitions, big-endian on both architectures, written as four checksummed copies at 512-byte blocks 0/15/30/45 — all four share one checksum, because it is computed with `dl_label_blkno` read as zero. Partition offsets are counted in the label's own `d_secsize` (1024 bytes on every disk we have) and measured from the end of a front porch, so `PartitionInfo` carries an explicit byte offset for them. Probed ahead of MBR because a NeXTSTEP/Intel disk also carries a valid `0xAA55` boot sector with an empty partition table. Surfaces its partitions to the existing big-endian UFS reader (browse / inspect / extract / edit). `rb-cli new hd next` writes a fresh `dlV3` label with the partitions you name (`4.3BSD`, `swap`, or any 8-byte type string), a 160-sector front porch, and the entry's optional NAME field as the partition's `p_mountpt`; `--heads` / `--sectors` set the recorded geometry and are counted in the label's own 1024-byte sectors. Fill it with `rb-cli new volume ufs-43bsd`, which writes the pre-4.4BSD UFS1 NeXTSTEP actually reads. The 8 slots are editable — resize / move / add / delete, `set-type` on the `p_type` name, and `set-bootable` moving `d_rootpartition` — with every edit converted out of 512-byte LBAs into porch-relative label sectors, overlap refused, and **only the copies the disk actually has** rewritten, so a NeXTSTEP/Intel disk keeps the PC boot sector that stands where its block-0 copy would be. Full-disk backup is future work. |
| Solaris-x86 | Yes | Yes  | Solaris x86 nests a 16-slice VTOC in **sector 1 of an MBR partition** (type `0x82` on Solaris 2.x-9, `0xBF` on 10+) rather than replacing the MBR the way SPARC's Sun label does. Little-endian, slice offsets relative to the Solaris partition. Type `0x82` is shared with Linux swap, so detection requires the `0x600DDEEE` sanity word, `v_version == 1`, and slices that fit inside the partition. Surfaces the UFS slices to the existing reader; the disk's other MBR primaries and EBR logicals list after them, since a Solaris disk is still an MBR disk. `rb-cli new hd solaris-x86` writes both halves at once: an MBR with one bootable `0x82` entry starting at cylinder 1, and a full `struct dk_label` in its second sector — VTOC, geometry tail, `0xDABE` magic and the XOR checksum Solaris validates. Slices are cylinder-aligned; slice 2 is the whole-partition backup alias, slice 8 the boot cylinder and slice 9 the two alternates cylinders, so user slices start at the disk's fourth cylinder. The 16 slices are editable — resize / move / add / delete, and `set-type` on the VTOC tag by name or number — with absolute LBAs translated into partition-relative sectors, slices bounded by the label's own `dkl_ncyl` data area, partial overlap refused (the backup alias containing everything is not), and the checksum re-stamped so Solaris still accepts the label. `set-bootable` is refused with the reason: the boot bit is on the MBR entry hosting the label, not on the slices inside it. Backup treats the disk as the MBR it is (the Solaris partition rides as one body) and records the VTOC in `solaris_x86.json`. |
| X68k   | Yes   | No (browse); writes whole tables from scratch | Sharp X68000 SASI/SCSI hard disks — Human68k's native scheme. 16-byte header plus 8 entries at byte 2048, big-endian, no magic number. Both geometries are auto-detected: SCSI (`X68SCSI1`, table at 0x800, 1024-byte sectors) and SASI (table at 0x400, 256-byte sectors), including custom-IPL game disks. `rb-cli new hd x68k` synthesizes a bootable disk with the Sharp IPL signature and a Human68k FAT volume. |
| DSD    | Yes   | — (fixed floppy geometry) | Double-sided Acorn DFS (`.dsd`). Not a table on the disk: the two sides are stored track-interleaved, so the reader de-interleaves them and this scheme presents them as **two** Acorn DFS partitions — side 0 at byte 0, side 1 at half the image. Edits to either side re-interleave on save. |
| None (superfloppy) | Yes — auto-detects the filesystem at sector 0 (FAT / NTFS / exFAT / HPFS / ext / XFS / JFS / UFS / ReiserFS / btrfs / SquashFS / HFS / HFS+ / APFS / Amiga SFS / Amiga PFS3 / Apple DOS 3.3 / CBM DOS / Atari DOS / RS-DOS / OS-9 RBF / DragonDOS / Acorn DFS / ADFS / TR-DOS / TI-99 / QDOS / Human68k / Alto BFS / Pilot/Cedar / Apple Lisa FS / …) | — | Standard floppy / disk sizes are recognised even without a partition table. Xerox Alto packs (`.pdi` / `.bfs` / CopyDisk / Salto `.dsk`), Pilot/Cedar PDIs (`fsFamily=2`), Dwarf 6085 `.zdisk` images, and tag-bearing Apple Lisa DiskCopy 4.2 / DART disks are detected by content and presented as a single browsable volume. |

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
- **Optical CD-DA preview** (desktop GUI only): the Optical tab plays audio
  tracks straight from a CHD or BIN/CUE image — select the disc, open "Audio
  Tracks", and play/stop any track with a live position readout. Playback is
  desktop-only (rodio, linking ALSA on Linux / WASAPI on Windows / CoreAudio on
  macOS); the slim `rb-cli-mini` MiSTer build still rips and converts optical
  images but ships no player.
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
| **ao486** (486 PC)             | FAT12 / FAT16 / FAT32 (MBR), HPFS (OS/2), ISO9660 | Floppy, HDD, CD |
| **PCXT**                       | FAT12 / FAT16 (MBR) | Floppy, HDD |
| **MSX / MSX1 / TurboR**        | FAT12 / FAT16 (Nextor VHD) | Floppy, HDD |
| **ZXNext** (ZX Spectrum Next)  | FAT32 / FAT16 / FAT12 | SD / HDD (VHD) |
| **TSConf** (ZX-Evolution)      | FAT32 (non-MBR) | SD / HDD (VHD) |
| **Minimig-AGA** (Amiga)        | AFFS (OFS/FFS), PFS3, SFS on RDB, ISO9660 | Floppy (`.adf`/`.adz`), HDD (`.hdf`/`.hdz`), CD |
| **MacPlus**                    | HFS, MFS (read + edit + create + fsck) | HDD (.hda / .hfv), 400K / 800K MFS floppy |
| **AtariST**                    | GEMDOS (FAT12 / FAT16), MSA containers | Floppy (`.st` / `.msa`); HDD pending AHDI write-side |
| **Apple-II**                   | ProDOS + Apple DOS 3.3 | `.dsk` / `.do` / `.po` / `.2mg` / `.woz` (sector-order auto-detect) |
| **Atari800**                   | Atari DOS 2 (2.0S / 2.5, read + write) | Floppy `.atr` / `.xfd` (single + enhanced density) |
| **ZX-Spectrum**                | TR-DOS (read + write + create + fsck), esxDOS FAT | TR-DOS floppy (`.trd`), DivMMC / esxDOS SD; +3DOS via CP/M (`zxplus3`), G+DOS (MGT) pending |
| **X68000** (Sharp)             | Human68k (FAT-derived) | Floppy (`.d88` / `.xdf` / `.hdm` / `.dim` — any-to-any conversion + in-place add/delete/mkdir), SASI/SCSI HDD (`.hda` / `.hdf` / `.hds` — read/browse/extract + add/delete/mkdir + in-place grow/shrink + defragmenting repack, incl. real BlueSCSI `X68SCSI1` 1024-byte-sector images). `rb-cli new hd x68k` builds self-bootable HDDs from scratch (`--system-disk donor.dim` clones a Human68k system floppy into the partition; one `SWITCH.X /HD` on first FDD0 boot installs the partition boot sector and the HDD self-boots to C: thereafter). For users with the well-known `hd0.hds` donor (100 MB Sharp/Keisoku Giken SCSI HDD image, 104,857,600 bytes), `--boot-sector-donor hd0.hds --size 100M --variant scsi` overlays the donor's Sharp partition boot sector at build time — zero manual steps, self-boots to C:> on first power-on. MAME-verified on x68000 SASI + x68030 SCSI. |
| **Archie** (Acorn Archimedes)  | ADFS / FileCore (read + edit on new-map E/F/HD **and** old-map D-format; E-format create; new-map fsck) | `.adf` floppy, bare + Arculator-wrapped `.hdf` HDD |
| **QL** (Sinclair)              | QDOS (QXL.WIN, read + write) | HDD (.win) |
| **Amstrad CPC**                | AMSDOS + CP/M 2.2 / Plus (`amstrad_data` + `amstrad_sys` DPBs) | Floppy `.dsk` |
| **AmstradPCW**                 | CP/M Plus (`amstrad_pcw` DPB) | Floppy `.dsk` |
| **TatungEinstein**             | Xtal-DOS / CP/M (`einstein` DPB) | Floppy `.dsk` |
| **Altair8800**                 | CP/M (`altair_8in` 8-inch floppy + `altair_cf` CF/HDD DPBs) | Floppy + IDE/CF |
| **MultiComp**                  | CP/M (`multicomp` DPB) | Floppy `.dsk` |
| **C64 / C128**                 | CBM DOS (1541 / 1571 / 1581, read + write) | Floppy `.d64` / `.d71` / `.d81` |
| **VIC20 / C16 / Plus-4**       | CBM DOS (1541, read + write) | Floppy `.d64` |
| **PET / CBM-II**               | CBM DOS (1541 + 8050/8250 IEEE-488, read + write) | Floppy `.d64` / `.d80` / `.d82` |
| **CoCo2 / CoCo3** (Tandy)      | RS-DOS / Disk BASIC + OS-9 / NitrOS-9 RBF (both read + write) | Floppy `.dsk` / `.jvc` / `.vdk` (35- / 40-track) |
| **Dragon** (Dragon 32/64)      | DragonDOS + OS-9 / NitrOS-9 RBF (both read + write) | Floppy `.dsk` (single- / double-sided 40-track) |
| **BBCMicro / AcornElectron**   | Acorn DFS (read + write) | Floppy `.ssd` (single-sided) + `.dsd` (double-sided, two partitions) |

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
- [`docs/linux_486_appliance.md`](docs/linux_486_appliance.md) /
  [`docs/appliance_hardware_support.md`](docs/appliance_hardware_support.md) —
  the bootable Linux backup appliance and its vintage-hardware driver support
  (which cards work, and how to add more in Buildroot).
- [`docs/build-ppc-mrustc.md`](docs/build-ppc-mrustc.md) — building `rb-cli` for
  PowerPC Mac OS X 10.4/10.5 through the mrustc fork (`scripts/build-ppc.sh`).
- [`docs/build-sol9-mrustc.md`](docs/build-sol9-mrustc.md) — building `rb-cli`
  for **Solaris 9 on SPARC** (`scripts/build-sol9.sh`), also via mrustc. Unlike
  the PowerPC build this one cross-compiles end to end on Linux, so it needs no
  second machine. Verified on a Sun Blade 2500: both parity gates agree with the
  desktop build, byte for byte.

## Donations

Support me on ko-fi!  
(https://ko-fi.com/danifunker)


## License

AGPL-3.0 — see `LICENSE`.
