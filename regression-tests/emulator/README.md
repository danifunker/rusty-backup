# Emulator scenarios

The fixture suite (`rb-regress run`) checks rb-cli against itself and against
reference images. These scenarios add the other half: rb-cli **changes** a real
volume, then the **real operating system** that owns the format judges it. A
reader and writer that share a mistake agree with each other; FreeBSD, QNX and
HP-UX do not share our mistakes.

```bash
regression-tests/scripts/regress-emulators.sh            # every scenario
regression-tests/scripts/regress-emulators.sh ufs        # only ids containing "ufs"
RB_CLI=/path/to/rb-cli TMPDIR=/big/disk regression-tests/scripts/regress-emulators.sh
```

`scripts/regress-local.sh` runs these last, after the fixture suite, `produce`
and `verify` (`--skip-emulators` leaves them out). Each scenario boots an
emulator, so a pass takes about six minutes on m900.

## Scenarios

`cases.txt` lists them, each with the bug it is expected to fail on, if any.

| Id | OS that judges | What rb-cli does first |
|---|---|---|
| `ufs2.freebsd-edit` | FreeBSD 15.1 `fsck_ffs -fn` | mkdir, put (one past the direct blocks), mv, rm on a FreeBSD-made UFS2 volume with check-hashes |
| `ufs1.freebsd-edit` | FreeBSD 15.1 `fsck_ffs -fn` | the same on a FreeBSD-made UFS1 volume (found R-072) |
| `ufs1.blank` | FreeBSD 15.1 `fsck_ffs -fn` | `new volume ufs` (found R-071) |
| `ufs1.blank-edit` | FreeBSD 15.1 `fsck_ffs -fn` | `new volume ufs`, then the same edits |
| `qnx2.lexicon-edit` | QNX 4.10.01 `chkfsys` in icon-emu | mkdir, put (several 128-block extents), mv, rm on the LEXICON 10 MB disk |
| `hpux-hfs.stand-edit` | HP-UX 11.11 `fsck -F hfs -n` on QEMU B160L | mkdir, put, mv, rm on an HP 715's `/stand` |
| `ufs1-be.solaris-edit` | Solaris 9 `fsck -F ufs -n` on QEMU SS-5 | Solaris `newfs`es a slice on a first boot; then mkdir, put, mv, rm (found R-074) |
| `ufs1-be.solaris-blank` | Solaris 9 `fsck -F ufs -n` | `new volume ufs-solaris` in a `new hd sun` label, then the same edits (found R-073) |
| `ufs43.nextstep-blank` | NeXTSTEP 3.3 `fsck -n` in Previous | `new volume ufs-43bsd` in a `new hd next` label, then mkdir, put, mv, rm (found R-075) |
| `ufs43.nextstep-edit` | NeXTSTEP 3.3 `fsck -n` in Previous | the same edits on a copy of the NeXTSTEP boot volume itself |
| `ntfs.win7-edit` | Windows 7 `chkdsk` (read-only) and `dir /s` | mkdir, put (non-resident), mv, rm on `fs.ntfs.base.hd` |
| `ntfs.win7-blank` | Windows 7 `chkdsk` and `dir /s` | `new volume ntfs`, then the same edits |
| `fat.win7-blank` | Windows 7 `chkdsk` and `dir /s` | `new volume fat` (FAT16, 64 MB), then the same edits |
| `fat32.win7-blank` | Windows 7 `chkdsk` and `dir /s` | `new volume fat --fat32` (300 MB), then the same edits |

The Windows scenarios are keyboard-driven (`oracles/win7/win7vm.py`): the host
waits for the taskbar, opens an elevated prompt (Start, `cmd`, Ctrl+Shift+Enter,
Alt+Y) and types one line that runs `rbrun.bat` from a FAT results disk. The
batch file runs `chkdsk` (with `n` piped in, since a damaged FAT volume prompts
even read-only) and `dir /s`, writes both and the exit code to the results disk
and shuts Windows down; the host reads them back with rb-cli. Nothing is read
off the screen. `fs.fat32.base.hd` is not used: mkfs.fat made it with 32,232
clusters, under FAT32's 65,525 minimum, and Windows' chkdsk fails on it
untouched (`win7_chkdsk.py fat32-pristine`).

Exit codes, shared with the `verify` oracles (`oracles/serialvm.py`):
0 pass, 1 finding, 77 cannot run here, 99 no verdict. A scenario that cannot
find its emulator or fixture skips; it never fails.

## Setting up a host

Fixtures come from `fixture_root` in `regression-tests/local.toml`. The
emulators come from its `[emulators]` table (see `local.toml.example`):

| Key | What it is |
|---|---|
| `freebsd15` | A directory with `fbsd.qcow2`: FreeBSD 15.1-RELEASE amd64 UFS (official image, as a qcow2 overlay) whose `loader.conf` has `console="comconsole,vidconsole"`; root without a password. Needs `qemu-system-x86_64` (KVM if present). |
| `icon_cli` | `icon-cli` built from github.com/danifunker/icon-emu (`cargo build --release -p icon-cli`). |
| `icon_rom` | `icon1_rom_combined.bin`, the ICON 1 firmware. |
| `hpux_boot` | A bootable HP-UX 11.11 HP 715 disk (from the `part.hpux-lvm.hpux1111-hp715` fixture, `/etc/ioconfig` rebuilt); root without a password. Needs `qemu-system-hppa`. |
| `win7` | A qcow2 overlay on a Windows 7 image that logs straight in to the desktop. Run `oracles/win7/win7vm.py settle <overlay>` once: it boots the overlay with the scenarios' hardware (AHCI, fixed disk serials, no NIC), restarts after the driver installs and shuts down, so later runs (always `-snapshot`) never meet a restart prompt. Never boot the base image itself without `-snapshot`, or the overlay breaks. |
| `solaris9` | A raw sun4m Solaris 9 disk that boots on `qemu-system-sparc -M SS-5` with OpenBIOS and has no root password: the "Preinstalled SPARC Solaris 9 Image for QEMU" (github.com/andarazoroflove/sparc, release `v5.9-fork`, `sparc9.qcow2` converted to raw). `-nographic` is required: with `-display none` alone OpenBIOS hands Solaris a framebuffer console and the serial line goes quiet after "switching to new context". The NAS's `HD0 Solaris9_patched` disk is a sun4u (Blade 2500) install that QEMU's sun4u OpenBIOS cannot boot. |
| `previous`, `nextstep_boot` | The Previous fork with command-line automation (github.com/danifunker/previous-fork; release archives carry the ROMs), and a NeXTSTEP 3.3 disk whose `/private/etc/rc.local` starts a root shell on serial port A and prints `PREVIOUS-READY` (the fork's `automation.howto.txt`). Runs headless (`SDL_VIDEODRIVER=dummy`) with ttya on two FIFOs; each run boots a fresh copy of the 2 GB boot disk. |
| `copperline`, `kickstart_a1200` | For the `verify` AFFS oracle (`oracles/copperline/affs_mount.py`): Copperline built from github.com/CopperlineHQ/Copperline, and an A1200 Kickstart 3.1 or 3.2. Its 3.x `Mount` comes from `oracle-assets/amiga/Mount-3x` beside the fixture root (3.2's is `C/Mount` on `fs.sfs.workbench-dh0.hd`). |

Every guest runs with `-snapshot`, so neither the VM nor the work copy of the
fixture is changed by the check.

## Controls

A check that cannot fail proves nothing, so every scenario has a control:

```bash
regression-tests/scripts/regress-emulators.sh --controls
```

runs each scenario with `RB_EMU_CONTROL=1`. After the rb-cli edits,
`scenario.damage()` wrecks a structure the checker must notice: the header of
cylinder group 0 on UFS and HP-UX HFS (`scenario.ffs_first_cg()` finds it from
the superblock in either byte order), and filesystem blocks 1-5 on the QNX disk.
The run passes only when every checker reports a finding. Scenarios marked with
a known bug are skipped, since they fail anyway. Run it after changing an
oracle or a scenario. Two traps the controls exposed:

- `fsck_ffs -n` (and HP-UX `fsck -n`) declines every repair and **still exits
  0**, so the oracles also fail on any `...? no` question.
- QNX `chkfsys` stops at `Type <CR> to continue?` on a damaged disk and the
  session times out; the error on screen is still the verdict.
- Windows 7 self-heals NTFS on mount, before `chkdsk` runs: a wrecked MFT
  record, and on a blank volume even a wrecked `$Bitmap`, come back clean
  (Windows writes `System Volume Information` at mount and repairs what its
  allocator trips over). The NTFS controls damage `$Bitmap`, which is caught on
  the corpus volume; `ntfs.win7-blank` is marked `-nc` and skipped. A
  write-protected test disk would stop the healing, but Windows then mounts the
  NTFS volume as RAW (an mkntfs volume's `$LogFile` is not clean), so the
  scenarios keep it writable. The `dir /s` listing still fails anything Windows
  cannot read.
- `chkdsk` on a damaged FAT volume asks `Convert lost chains to files (Y/N)?`
  even read-only; `rbrun.bat` pipes in `n`.

## Deferred

These need an answer or a setup step before they can be automated:

- **FS-UAE (AFFS)** has a scripted `verify` oracle established on Kickstart
  3.1; this host has 3.2. Copperline (below) covers AFFS meanwhile.
- **MiSTer cores, WinUAE, AppleCommander, Snow, MAME**: listed in
  `data/oracles.toml` with no runnable check yet.
