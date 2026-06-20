# Handoff — appliance deployable artifacts (resume here)

Branch `add-crusty-backup-dos-poc` (12 commits ahead of origin). Goal: build the
deployable boot media for both appliances and attach them to the GitHub release.
Full design: [`linux_486_appliance.md`](linux_486_appliance.md) +
[`linux_486_build.md`](linux_486_build.md) + [`cb_dos.md`](cb_dos.md).

## DONE + verified

- **Linux appliance** (Buildroot kernel + rootfs with static `rb-cli` baked in via
  rootfs overlay). Boots in qemu via `-kernel`/`-initrd`; auto-logins straight into
  `rb-cli menu`; **backup + restore + the directory-picker screens all verified
  live** (drove disk→action→folder-picker→backup to /mnt over serial).
- **`rb-cli menu`** TUI + **dir-picker** screens — `src/cli/verbs/menu.rs`,
  `src/cli/dir_picker.rs`, shared `src/cli/tui.rs`. Builds (default + slim),
  clippy-clean, unit-tested.
- **4 Docker cross-build images** — `docker/cross-i586{,-musl}.Dockerfile`,
  `cross-i486.Dockerfile`, `cb-dos.Dockerfile`. Static i586-musl `rb-cli` runs on
  Pentium+, faults on bare 486 (verified `qemu -cpu 486`).
- **cb-dos DOS media** — `crusty-backup/dist/cbdos-freedos.img` (FreeDOS 1.4 base +
  cb-dos tools + banner; **booted in DOSBox-X**) and `crusty-backup/dist/cbdos.iso`
  (bootable El-Torito CD). Built by `crusty-backup/mkmedia.sh`
  (`FDBASE="/Users/dani/Downloads/Dorado'/FD14-FloppyEdition/144m/x86BOOT.img"`).
- **Appliance hybrid ISO BUILDS** — `buildroot/output/rusty-backup-appliance.iso`
  (32 MB, isohybrid: `ISO 9660 (DOS/MBR boot sector) bootable`). Config in
  `buildroot/build.sh` (ISO9660 + isolinux + HYBRID) + `buildroot/kernel.fragment`
  (CD-ROM drivers). The config is `syncconfig`-clean.

## OPEN ISSUE — the appliance ISO does not boot-verify (the thing that "didn't work")

Booting the ISO with `qemu -kernel bzImage -initrd <extracted initrd> -append
"root=/dev/sr0 console=ttyS0" -cdrom <iso> -boot d` **hung with no serial output**.

The ISO's `isolinux.cfg` is: `kernel /boot/bzImage; initrd /boot/initrd; append
root=/dev/sr0` — note **no `console=ttyS0`** (so a real CD boot goes to VGA tty0,
which `-nographic` can't capture), and **`root=/dev/sr0`** = a *read-only* iso9660
CD root with a 26 MB initrd.

Likely causes (diagnose in this order):
1. **Console** — the ISO boots to VGA, not serial. First, just **boot it with a
   display window and watch it**: `qemu-system-i386 -M pc -m 256 -cdrom
   buildroot/output/rusty-backup-appliance.iso -boot d` (no `-nographic`). That
   shows the actual failure instead of a silent hang.
2. **Read-only root** — the appliance init/menu were only ever verified on the
   *read-write ext2* rootfs. On a read-only iso9660 root, `mount -o remount,rw /`
   fails and writable dirs (/var, /root, /tmp) may not exist. Options: set up tmpfs
   overlays for those, OR switch the ISO to the **initrd-in-RAM** model
   (`BR2_TARGET_ROOTFS_ISO9660_INITRD`, read/write) — but that needs RAM ≥ rootfs
   (~96 MB), excluding low-RAM machines. Pick based on target RAM.
3. **Add `console=ttyS0 console=tty0` to the kernel cmdline** (kernel
   `CONFIG_CMDLINE` in `buildroot/kernel.fragment`, or a Buildroot cmdline option)
   so future boots are serial-capturable AND show on VGA.

The ext2 rootfs path is known-good — the regression is purely the iso9660 read-only
boot. The hybrid MBR/El-Torito structure is fine; this is a runtime/init problem.

## NOT STARTED (remaining pipeline)

- **GRUB boot-floppy → CD bridge** for pre-El-Torito 486 BIOSes (GRUB on a floppy
  does `root=(cd0); linux /boot/bzImage; initrd …`). Design in
  `linux_486_appliance.md`.
- **PXE bundle** — `bzImage` + rootfs-as-initramfs + sample `pxelinux.cfg` + README.
- **Release CI** — wire every artifact into `.github/workflows/release.yml`:
  cb-dos floppy + CD, the 4 cross-build binaries, the appliance ISO, (later) the
  GRUB floppy + PXE bundle. Decide: **vendor the FreeDOS base floppy** vs fetch it
  from the FreeDOS 1.4 release in CI (it's GPL/redistributable; ~1.4 MB).

## Mechanics to reuse

- **Buildroot build** (~25-30 min, full; no incremental — a named volume at
  `/opt/buildroot/output` breaks `syncconfig`, don't retry it):
  `docker build -t rb-buildroot - < docker/buildroot-appliance.Dockerfile` then
  `docker run --rm -v "$PWD":/work rb-buildroot`. Needs the static `rb-cli` first:
  `docker run --rm -v "$PWD":/src rb-cross-i586-musl`.
- **Fast appliance demo** without a rebuild: rebuild just `rb-cli`
  (`rb-cross-i586-musl`, ~35 s) → inject into `rootfs.ext2` with `debugfs`
  (`rm/write/sif /usr/bin/rb-cli mode 0100755`) → `qemu-system-i386 -kernel bzImage
  -initrd … -append "root=/dev/sda rw console=ttyS0"` → drive over serial with
  vim-keys (`j`/`\r`=Enter/`q`).
- Outputs `buildroot/output/` and `crusty-backup/dist/` are gitignored.
- Host has `qemu-system-i386` (11.0.1) and DOSBox-X (`/Applications/dosbox-x.app`).
