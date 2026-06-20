# The rusty-backup Linux appliance (Buildroot)

A bootable, minimal Linux image that runs **`rb-cli` for backup *and* restore**
on a vintage 486/Pentium-class machine — the Linux sibling of the DOS-native
[`cb-dos`](cb_dos.md). You boot it on (or beside) the old box, and it images /
restores the local disk through the full Rust filesystem engine.

> **Status: first working image — verified in qemu.** The scaffolding below
> builds a bootable image with `rb-cli` baked in. It boots in
> `qemu-system-i386`, detects the attached disks, and **`rb-cli inspect` +
> `backup --format zstd` run on a real DOS disk** (`/dev/sdb` → MBR/FAT16 parsed,
> backup folder written); restore writes a device with `--device --yes`.
> Networking is intentionally deferred to a separate branch (see "Networking").
> Nothing here has run on real 486 hardware yet.

## Why a Linux appliance (vs. cb-dos)

| | cb-dos (DOS) | rusty-backup appliance (Linux) |
|---|---|---|
| OS on the box | none (single floppy) | minimal Linux (Buildroot) |
| Backup engine | hand-written C | the **whole Rust `rb-cli`** (FAT/NTFS/HFS/ext/…) |
| Disk access | BIOS int 13h | `/dev/hda`, `/dev/sda` |
| Networking | packet driver + mTCP (hard) | kernel NIC + TCP (later — the real win) |
| Boot media | one floppy | kernel + rootfs on CF/USB/PXE |

The appliance trades a heavier boot medium for **reusing the entire backup/
restore codebase** and, eventually, **kernel networking**.

## Architecture

The clean trick: `rb-cli` goes in as a **static** binary, so Buildroot only
supplies the kernel + a BusyBox userland — it never builds Rust, and the
appliance's own libc is irrelevant to `rb-cli`.

```
 ┌──────────────────────── appliance image ────────────────────────┐
 │  bzImage (Linux kernel)   — IDE/SATA/USB + vfat/ext4 (fragment)  │
 │  rootfs.ext2 (BusyBox)    — + rootfs overlay:                    │
 │      /usr/bin/rb-cli           static musl binary (backup+restore)│
 │      /etc/init.d/S99appliance  boot banner + disk summary        │
 └──────────────────────────────────────────────────────────────────┘
        boots on the vintage box / qemu-system-i386
        reads /dev/hda  ──►  rb-cli backup ──►  /mnt (2nd disk / USB / net)
        /mnt  ──►  rb-cli restore  ──►  /dev/hda
```

`rb-cli` reads **raw block devices** and parses FAT/NTFS/etc. itself, so the
kernel only needs block-device drivers + the *destination* filesystem (where the
backup lands), not the source filesystems. See `buildroot/kernel.fragment`.

## The pieces (all in-repo)

- `docker/buildroot-appliance.Dockerfile` — the Buildroot build environment.
- `buildroot/build.sh` — starts from Buildroot's `qemu_x86_defconfig`, layers in
  the overlay + destination-FS tools + the kernel fragment, and builds.
- `buildroot/overlay/` — the rootfs overlay (the boot banner/disk-summary init;
  the static `rb-cli` is staged into `usr/bin/` at build time).
- `buildroot/kernel.fragment` — IDE/SATA/USB + vfat/ext4 kernel options.

## Build

```sh
# 1. the static rb-cli (once) — see docker/README.md
docker build -t rb-cross-i586-musl - < docker/cross-i586-musl.Dockerfile
docker run  --rm -v "$PWD":/src rb-cross-i586-musl

# 2. the appliance image
docker build -t rb-buildroot - < docker/buildroot-appliance.Dockerfile
docker run  --rm -v "$PWD":/work rb-buildroot
# -> buildroot/output/{bzImage, rootfs.ext2, rusty-backup-appliance.iso}
```

## Deployable artifacts — how the appliance ships

The build emits raw pieces (`bzImage`, `rootfs.ext2`) plus bootable media. The
shape, and which machine each suits:

| Artifact | Boots on | Status |
|---|---|---|
| **`rusty-backup-appliance.iso`** (hybrid) | a CD-ROM **or** (dd'd to a stick/CF) a USB/CF | building |
| **GRUB boot floppy** → loads the appliance off the **CD** | pre-El-Torito BIOSes that can't boot CDs | planned |
| **PXE bundle** (`bzImage` + initramfs + `pxelinux.cfg` + README) | a diskless backup *station* over the network | planned |

Notes:
- The **hybrid ISO** is the workhorse: one file is both the "CD ISO" and the
  "USB/CF `.img`" — `dd if=…​.iso of=/dev/sdX` makes a bootable stick. So we don't
  need a separate genimage `.img` target.
- **Linux can't ship on a floppy** — the kernel alone (~5 MB) overflows 1.44 MB.
  For an old machine whose BIOS predates El Torito (can't boot a CD), the bridge
  is a tiny **GRUB boot floppy** that does `root=(cd0); linux /boot/bzImage; …` —
  GRUB fits on a floppy and reads ISO9660, so the floppy boots and pulls the
  appliance off the CD. (The floppy *as a self-contained OS* is the cb-dos lane,
  not this one — see [`cb_dos.md`](cb_dos.md).)
- **PXE** isn't one boot file: it's a tarball of `bzImage` + the rootfs as an
  initramfs + a sample `pxelinux.cfg` + a README, dropped onto a TFTP/dnsmasq
  box. Good for a permanent backup station; overkill for a one-off.

All of these (plus the cb-dos FreeDOS floppy + CD) get built in CI and attached
to the GitHub release — see the release workflow.

## Building bootable media with rusty-backup itself

rb-cli already mints bootable disk images for X68000 / SGI / Mac (partition
table + filesystem + boot setup), so assembling/writing the appliance media is
in its wheelhouse: it partitions + formats + drops files into a CF/USB image,
and **writes any artifact to physical media** (that's just "restore to a
device"). Buildroot produces the kernel + rootfs and the ready-to-`dd` hybrid
ISO, so the bootloader plumbing isn't reinvented; rb-cli is the natural tool for
the final write-to-CF/USB step.

## Boot + use (qemu, then real hardware)

```sh
# attach the appliance rootfs as the first disk, the disk-to-back-up as the 2nd
qemu-system-i386 -M pc -m 256 -nographic \
  -kernel buildroot/output/bzImage \
  -drive file=buildroot/output/rootfs.ext2,format=raw,if=ide \
  -drive file=some-dos-disk.img,format=raw,if=ide \
  -append "root=/dev/sda rw console=ttyS0"
```

At the shell (the banner lists detected disks):

```sh
rb-cli inspect /dev/sdb              # show the partition layout
mount /dev/sdc1 /mnt                 # a destination medium
rb-cli backup  /dev/sdb /mnt/backup  # image it
rb-cli restore /mnt/backup /dev/sdb  # …and restore it
```

On real hardware the disk to back up is typically `/dev/hda`; write the image to
CF/USB and boot it, or PXE-boot it for a diskless backup station.

## CPU baseline — the one knob that needs care

`qemu_x86_defconfig` targets **i686** (Pentium Pro+), so the first image is an
i686 appliance — it boots in qemu and proves the whole flow, and the static
`rb-cli` (i586) runs on it. To actually target **486/Pentium** hardware, two
things must drop to the right baseline together:

1. **The appliance** (kernel + BusyBox): set `BR2_x86_i586` (Pentium) or
   `BR2_x86_i486` in the Buildroot config. i486 requires Buildroot's *internal*
   toolchain (Bootlin's prebuilt x86 toolchains are i686-class), a longer build.
2. **`rb-cli`**: the static `i586-musl` binary runs on Pentium+ but **faults on a
   bare 486** (it uses `CMPXCHG8B`; verified under `qemu -cpu 486`). A true-486
   appliance needs an i486 `rb-cli` — the i486 codegen story in
   [`linux_486_build.md`](linux_486_build.md) (`cross-i486` gives the codegen; a
   fully static i486 binary needs an i486 libc, i.e. this same Buildroot
   toolchain).

So: **i586/Pentium is the practical sweet spot today** (static binary just
works); **i486** is a config + toolchain step on top.

## Networking (separate branch)

Deliberately out of scope here. Once the network branch lands, the appliance
gains the kernel's NIC + TCP stack, and the backup *destination* becomes a remote
host instead of a 2nd local disk — the decisive advantage over cb-dos. Until
then the destination is a second disk or a USB stick.

## Gotchas found while bringing it up

- The slim `rb-cli` has **no CHD** (desktop-only), and `backup` defaults to CHD —
  so the appliance must `backup --format zstd` (pure-Rust). The boot banner says so.
- Writing a device needs `--device --yes` (an overwrite-safety gate — it correctly
  refused a device restore without it).
- qemu's PIIX IDE shows disks as `/dev/sdX` (libata), not `hdX`. A bare-IDE 486
  may present `/dev/hdX`; the banner notes both.
- The init waits ~15 s for `eth0` (no NIC yet — networking is the deferred branch);
  harmless boot delay.

## Auto-login (installer-CD style)

The appliance boots **straight to a root shell — no login prompt** (the
`S99appliance` banner is what greets you). `build.sh` sets
`BR2_TARGET_GENERIC_GETTY_OPTIONS="-n -l /bin/sh"`, so the console `getty` runs a
shell directly instead of `login`. A future step replaces that bare shell with a
small backup/restore **menu** (boot straight into the tool, like an installer).

## Open / next

- [x] First boot + **backup *and* restore** smoke test in qemu — full round-trip
      works (backup `/dev/sdb`, restore to a blank `/dev/sdc`, partition
      reconstructed).
- [x] Auto-login (no login prompt) — installer-CD style.
- [x] Backup/restore text menu (`rb-cli menu`) + folder-picker screens; appliance
      auto-launches it.
- [~] Bootable hybrid ISO (CD + USB) — building.
- [ ] GRUB boot-floppy → loads the appliance off the CD (pre-El-Torito BIOSes).
- [ ] PXE bundle (kernel + initramfs + pxelinux.cfg + README).
- [ ] cb-dos FreeDOS floppy + CD (the DOS lane; see cb_dos.md).
- [ ] Wire all deployables into the GitHub release workflow.
- [ ] Retarget to `BR2_x86_i586` (Pentium) / `BR2_x86_i486` (true 486) + matching rb-cli.
- [ ] Networking (separate branch) → remote destination.
- [ ] Real 486/Pentium hardware boot.
