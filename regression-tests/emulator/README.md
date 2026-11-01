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
| `ufs1.freebsd-edit` | FreeBSD 15.1 `fsck_ffs -fn` | the same on a FreeBSD-made UFS1 volume (R-072) |
| `ufs1.blank` | FreeBSD 15.1 `fsck_ffs -fn` | `new volume ufs` (R-071) |
| `qnx2.lexicon-edit` | QNX 4.10.01 `chkfsys` in icon-emu | mkdir, put (several 128-block extents), mv, rm on the LEXICON 10 MB disk |
| `hpux-hfs.stand-edit` | HP-UX 11.11 `fsck -F hfs -n` on QEMU B160L | mkdir, put, mv, rm on an HP 715's `/stand` |

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

## Deferred

These need an answer or a setup step before they can be automated:

- **FS-UAE (AFFS)** has a scripted `verify` oracle, but this host's Kickstarts
  are 3.2 (exec 47.7) and the oracle was established on 3.1. Use 3.2, or find
  the 3.1 ROM?
- **Copperline (AFFS)**: not built on this host.
- **Solaris 9 / 10 SPARC (UFS big-endian)**: the disk images are on the NAS;
  a test disk needs a Sun VTOC label, and the root password is unknown.
- **NeXTSTEP 3.3 in Previous (4.3BSD UFS)**: drivable by keyboard only (the
  guest mouse does not track), so a check needs a typed-command script.
- **Windows 7 (NTFS / FAT, chkdsk)**: no serial console; needs a different
  control channel.
- **MiSTer cores, WinUAE, AppleCommander, Snow, MAME**: listed in
  `data/oracles.toml` with no runnable check yet.
