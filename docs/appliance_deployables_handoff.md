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
- **Appliance hybrid ISO BUILDS + BOOTS** — `buildroot/output/rusty-backup-appliance.iso`
  (isohybrid: `ISO 9660 (DOS/MBR boot sector) bootable`). Boots via El-Torito all
  the way to the rb-cli menu — verified over **serial** (`-nographic`) and on
  **VGA** (`-vga cirrus` screendump). Config in `buildroot/build.sh` (ISO9660 +
  isolinux + HYBRID + serial-console `buildroot/isolinux.cfg`) + the broadened
  `buildroot/kernel.fragment`. See the RESOLVED section below.

## RESOLVED — the appliance ISO boots fine (the "hang" was a qemu display artifact)

**The ISO was never broken.** Booted via the real El-Torito CD path
(`qemu-system-i386 -M pc -m 256 -cdrom rusty-backup-appliance.iso -boot d`) with
**`-vga cirrus`**, the unmodified original ISO boots all the way to the rb-cli
menu (banner + disk detection + menu + login prompt — screen-captured via the
qemu monitor). The "silent hang" was two things stacked:

1. **qemu's `-vga std` (Bochs stdvga) stops repainting** the framebuffer partway
   through the kernel boot (around the PCI/ACPI lines), so the boot *looks* frozen
   on a `-vga std` screendump even though the kernel is running fine. Proven by
   serial: with a serial console the same boot sails through to `/init` and the
   menu. Use `-vga cirrus` or the serial console to watch a qemu boot.
2. **No serial console** in the stock isolinux.cfg (`append root=/dev/sr0`, no
   `console=`), so a `-nographic` boot showed nothing — combined with the ~15 s
   `eth0` DHCP wait, it read as a hang.

The initrd, by the way, is the **whole rootfs as an in-RAM initramfs** (27 MB
uncompressed cpio with `/init`); `root=/dev/sr0` is ignored. So it's the
read/write-in-RAM model — the read-only-root worry from the old notes was moot.

### What changed to make it solid + verifiable

- **`buildroot/isolinux.cfg`** (new; wired via `BR2_TARGET_ROOTFS_ISO9660_BOOT_MENU`)
  — adds `serial 0 115200` + `append … console=tty0 console=ttyS0,115200`, so the
  whole boot is visible/drivable on a VGA monitor **and** headless over serial
  (`-nographic`, a real serial terminal, CI).
- **`buildroot/overlay/etc/inittab`** (new) — runs `appliance-menu` on **both**
  `tty1` (VGA, real hardware) and `ttyS0` (serial/headless), instead of the single
  `/dev/console` getty. Validated by repacking the initrd and booting `-kernel`:
  the menu appears cleanly on serial. Buildroot copies the overlay after its own
  inittab fixups, so this file wins.
- **`BR2_SYSTEM_DHCP=""`** in `build.sh` — drops the 15 s `wait_iface` eth0 stall
  (networking is deferred). Re-enable when the network branch lands.
- **`buildroot/kernel.fragment`** — broadened to a wide vintage-hardware driver
  set (ISA/PCI/PC-Card NICs, SCSI HBAs, bare-486/VLB + PCI IDE, Multi-I/O serial
  + parallel, parallel-port ZIP, USB storage; PCI built-in, ISA as modules).
  User guide: [`appliance_hardware_support.md`](appliance_hardware_support.md).
  `CONFIG_IKCONFIG_PROC` exposes the live config at `/proc/config.gz`.

### Verifying a boot (the reliable recipe)

```sh
# headless / CI — drive everything over serial:
qemu-system-i386 -M pc -m 256 -cdrom buildroot/output/rusty-backup-appliance.iso \
  -boot d -nographic
# or watch the VGA path with a working emulation:
qemu-system-i386 -M pc -m 256 -cdrom …iso -boot d -vga cirrus
```
Do **not** judge a boot by a `-vga std` screendump — it freezes its own picture.

## DONE since the original handoff

- **GRUB boot-floppy → CD bridge** — `buildroot/make-grub-floppy.sh` builds a
  1.44 MB GRUB i386-pc floppy whose embedded config `search`es every drive for
  `/boot/bzImage` and boots the appliance off the CD. **Key fix:** a pre-El-Torito
  BIOS won't expose the CD over int 13h, so the floppy bundles GRUB's *native*
  `ata`/`ahci`/`usbms` drivers (not just `biosdisk`) — verified in qemu booting
  `-fda grub-floppy.img -cdrom appliance.iso -boot a` straight to the rb-cli menu
  (GRUB read the CD as `(ata2)`). Don't use `menuentry { }` in the embedded
  config (needs `normal` mode); plain `search`/`linux`/`initrd`/`boot` commands.
- **PXE bundle** — `buildroot/package-pxe.sh` emits
  `rusty-backup-appliance-pxe-<ver>.tar.gz` (bzImage + the initramfs + a sample
  `pxelinux.cfg` + README). Verified the bundled kernel+initrd boot to the menu.
- **Release CI (appliance)** — `build-appliance` job in `release.yml` builds the
  cross `rb-cli` + the Buildroot ISO, then runs the two scripts above; uploads
  the ISO + PXE tarball + GRUB floppy; wired into the `release` job's `needs:`,
  the flatten glob (`.iso`/`.img`), and the downloads table. Gated to main +
  `continue-on-error` like the MiSTer job.

- **Release CI (cb-dos)** — `build-cb-dos` job in `release.yml`: builds the DOS
  tools (DJGPP), **fetches** the FreeDOS 1.4 FloppyEdition boot floppy in CI
  (freely redistributable; not vendored —
  `.../distributions/1.4/FD14-FloppyEdition.zip`, inner `144m/x86BOOT.img`), runs
  `mkmedia.sh`, uploads `cbdos-freedos-<ver>.img` + `cbdos-<ver>.iso`. Verified
  the full flow locally and that the produced CD boots to the cb-dos banner.

## NOT STARTED (remaining pipeline)

- **Cross-binary release assets (optional)** — ship the i586/i486 standalone
  `rb-cli` binaries as their own downloads (the i586-musl one is already bundled
  inside the appliance, so this is just convenience).
- **Retarget to i586/i486** + matching `rb-cli`. Note this is also the prerequisite
  for the **ITX-Llama / Vortex86EX** boards — they're Pentium-class (i586), so the
  i686 kernel won't boot on them; their onboard **RDC R6040** NIC driver is already
  in `kernel.fragment`, waiting on the i586 build.
- **Networking** (separate branch); **real 486/Pentium hardware** boot.

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
