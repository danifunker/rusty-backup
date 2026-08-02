# Oracle Hosts and Emulator Profiles

How we verify everything when no single machine can. Companion to
`VERIFICATION-MATRIX.md`, which says *what* verifies each format; this says
*where* it runs and how the runner reaches it.

## Two kinds of oracle

1. **Package oracle** — a tool installed on some OS. `fsck.ext4`, `chdman`,
   `ghostexp.exe`, `hdiutil`. Cheap, scriptable, exact.
2. **Emulator oracle** — a preconfigured emulated machine that opens or
   boots our artifact. Slower, needs setup, but it is the only oracle that
   exists for a large part of the vintage matrix, and it is the only one
   that answers "would this actually work".

Emulator oracles need **preconfiguration** — ROMs, a config file, an
installed guest OS, boot media. That setup is an asset with a version, not
something to redo by hand each run. See § Emulator profiles.

---

## Emulator platform support

The important question for architecture: which oracles pin us to a specific
OS, and which travel.

| Emulator | Windows | macOS | Linux | Verifies |
|----------|:-------:|:-----:|:-----:|----------|
| **WinUAE** | yes | **no** | **no** | AFFS, PFS3, SFS, RDB, ADF/HDF |
| **FS-UAE** | yes | yes | yes | same as WinUAE — **the portable Amiga choice** |
| Amiberry | no | no | ARM only | same, on Pi-class hardware |
| **86Box** | yes | yes | yes | FAT, **HPFS via OS/2**, NTFS, real DOS/OS-2/Win boots |
| PCem | yes | no | yes | as 86Box, older fork |
| **VICE** | yes | yes | yes | CBM DOS, D64/D71/D81/G64/G71 |
| **MAME** | yes | yes | yes | CHD, plus a long tail of vintage systems |
| **Hatari** | yes | yes | yes | Atari DOS, AHDI, ST/MSA |
| **Basilisk II / SheepShaver** | yes | yes | yes | HFV, HFS, HFS+, DC42, resource forks |
| **Mini vMac** | yes | yes | yes | MFS, early HFS, 400/800K floppies |
| **QEMU** | yes | yes | yes | FAT/ext/NTFS/ISO/UDF + every qemu-img container |
| Altirra | yes | via Wine | via Wine | Atari 8-bit, ATR/XFD |
| Arculator / RPCEmu | yes | yes | yes | Acorn ADFS / FileCore |
| Snow | unverified | unverified | unverified | 68k Mac — site returned 403, **confirm before relying on it** |

**Conclusion: almost everything is cross-platform.** WinUAE is the one hard
Windows pin, and FS-UAE covers the same ground portably. So emulator oracles
do *not* force a particular host OS — package oracles do.

Already on the NAS, preconfigured:
`Emulators/WinUAE` (with `Roms/` and `Configurations/`), `Emulators/86box`
(with `86box.cfg`), `Emulators/mame0.273` — and `chdman.exe` lives in the
MAME folders, which is where our CHD oracle came from.

---

## What actually pins us to an OS

Only three things, and only one of them is immovable:

| Pin | Why | Escapable? |
|-----|-----|-----------|
| **macOS** | `fsck_hfs`, `fsck_apfs`, `hdiutil`, `diskutil`. Apple ships no equivalents elsewhere, and macOS VMs are only practical/licensed on Apple hardware. | **No.** A Mac is required for HFS, HFS+, APFS, DMG, sparseimage, NDIF, APM. |
| **Windows** | `ghostexp.exe` (GHO), `chkdsk` (NTFS). | Partly — `ghostexp` may run under Wine; `ntfsfix`/`ntfs3` mount covers some NTFS ground on Linux. |
| **Full Linux** | Kernel mounts for hfs, hfsplus, affs, minix, jfs, ntfs3, ufs, efs. | No, but any Linux does — bare metal, VM or container. |

Everything else travels.

---

## Recommended topology

### Mac as hub — the one that works

A modern Apple-Silicon Mac can host every role except the MiSTer:

- **macOS natively** — `fsck_hfs`, `fsck_apfs`, `hdiutil`, `diskutil`, and
  the Apple-format emulators (Mini vMac, Basilisk II, SheepShaver).
- **Linux guest** — UTM, Parallels, Lima or Docker. Gives the full mount
  set: hfs, hfsplus, affs, minix, jfs, ntfs3, ufs, efs, plus every
  `fsck.*`.
- **Windows guest** — Parallels/UTM running Windows on ARM. `chkdsk` is
  native; `ghostexp.exe` is 32-bit x86 and runs under Windows-on-ARM's
  emulation layer. Slow, but this is a monthly run.
- **Cross-platform emulators natively** — FS-UAE, 86Box, VICE, MAME,
  Hatari, QEMU, Arculator.
- **MiSTer over the network** — AFFS mount and every core.

This is the recommendation because the macOS pin is the only one that cannot
be virtualised away. Build around it and everything else follows.

### Keep the existing Windows box

Not required, but it already holds preconfigured WinUAE (with ROMs), 86Box,
MAME + `chdman`, `qemu-img`, 7-Zip and three copies of `ghostexp.exe`. It is
the fastest path for the Windows-pinned oracles and a useful second opinion.
Its WSL Ubuntu covers the lighter Linux oracles without a VM.

### Minimum viable set

| Hosts | Coverage |
|-------|----------|
| Windows box + WSL + MiSTer | everything except HFS/HFS+/APFS/DMG/APM and the full-Linux mount set |
| **+ any Linux (VM or metal)** | adds hfs/hfsplus/affs/minix/jfs/ntfs3/ufs/efs mounts |
| **+ a Mac** | complete |

---

## Runner support: oracle routing

The runner needs to send an artifact to whichever host can judge it. Design:

`regression-tests/oracle-hosts.toml` — **gitignored**, template committed as
`oracle-hosts.toml.example`. Addresses, users and key paths never enter git.

```toml
[[host]]
name      = "local"
roles     = ["windows"]
transport = "local"

[[host]]
name      = "wsl"
roles     = ["linux-light"]
transport = "wsl"
distro    = "Ubuntu-24.04"

[[host]]
name      = "linuxbox"
roles     = ["linux-full"]
transport = "ssh"          # address/user/identity from local.toml

[[host]]
name      = "mac"
roles     = ["macos"]
transport = "ssh"

[[host]]
name      = "mister"
roles     = ["mister", "affs"]
transport = "ssh"
```

A case declares what it needs rather than where it runs:

```toml
[[case.oracle]]
tool = "fsck_hfs"
role = "macos"
args = ["-n", "{artifact}"]
expect_exit = 0
```

Runner behaviour:

1. Find a configured host advertising `role` that actually has `tool`.
2. Ship `{artifact}` there — local copy, `wslpath`, or `scp`.
3. Run the oracle, capture exit code plus stdout/stderr.
4. Bring the result back into `results.jsonl` tagged with the host.
5. **No host available -> `skip-tool`**, recorded in `oracle-skips.md` with
   the role that was missing.

Step 5 is what makes a partial setup honest rather than misleading: a
Windows-only run stays valid and simply reports a long skip list. The full
matrix is the union of the per-host bundles, and the summary says which
roles were never reached.

---

## Emulator profiles

An emulator oracle is only reproducible if its configuration is. Each
profile is a versioned bundle on the NAS:

```
rb-fixtures/emulator-profiles/<name>/
  profile.toml      what it verifies, host requirements, how to invoke
  config/           emulator config files (86box.cfg, .fs-uae, .vice, ...)
  boot/             installed guest disk (the expensive part — an OS/2
                    install, a Workbench install)
  roms.sha256       hashes only where ROMs cannot be redistributed
  README.md         how to rebuild from scratch if the bundle is lost
```

`profile.toml` records the invocation and the assertion channel:

```toml
name        = "86box-os2-hpfs"
verifies    = ["fs.hpfs"]
emulator    = "86Box"
platforms   = ["windows", "macos", "linux"]
boot_disk   = "boot/os2-warp4.img"
attach_as   = "secondary-hdd"
assertion   = "guest runs CHKDSK on the attached volume, writes result to a
               shared FAT partition the harness reads back"
```

The guest-writes-a-result-file pattern is the general one, and it is much
more robust than screenshot diffing: the guest OS runs its own native check
tool and drops the output somewhere the host can read.

---

## Answers to the five open questions

**1. WinUAE.** Windows only. **FS-UAE** is the cross-platform equivalent and
should be the default Amiga oracle so the check runs on whatever host the
suite is on; keep the preconfigured WinUAE on the Windows box as a second
opinion.

**2. cbk — verify the full flow, not the format.** Right call. `cbk` is our
own container, so no third-party reader exists and format inspection is
circular by construction. Instead: take a backup of an emulated machine's
disk into `cbk`, restore it, and **boot the restored disk in the same
emulator**. The oracle becomes "the guest still boots and its files are
intact", which is a stronger claim than any structural check. 86Box or QEMU
both work; QEMU is the cheaper one to script. This pattern generalises to
every format we invent.

**3. Emulator-only vintage formats -> MiSTer.** Agreed, and it is the best
option available: the cores are the reference implementations, the board is
already on the network with key auth, and it can mount AFFS natively as a
bonus. `EMULATORS.md` § 7A already carries the deploy-and-assert mechanism.

**4. HPFS via 86Box + OS/2.** Yes, and it is the only real HPFS oracle
going — the Linux `hpfs` module is read-only and absent from WSL. OS/2's own
`CHKDSK` is the authoritative checker. 86Box is cross-platform and already
on the NAS with a config. The cost is building the OS/2 guest once; after
that it is a profile bundle. Worth it, since it converts HPFS from "thin" to
properly covered.

**5. Apple floppy containers.** Two complementary answers. For *structural*
checks, **AppleCommander** is Java and runs on all three hosts, so wire it
first — it reads 2MG/DSK/PO/DO and lists ProDOS and DOS 3.3 volumes. For
*fidelity* checks on WOZ/MOOF/DC42, an emulated Mac (Mini vMac or Basilisk
II) running Apple's own Disk Copy is the real oracle. **Snow** looks like a
good modern option but its site returned HTTP 403, so confirm it exists and
is scriptable before planning around it.

---

## Action items, cheapest first

1. **`chdman` — done.** Found at
   `Emulators/mame0.273/chdman.exe` on the NAS and verified against our own
   CHD output: *"Raw SHA1 verification successful! Overall SHA1
   verification successful!"* Wire it into the runner and add the path to
   `local.toml`.
2. `apt install cpmtools xorriso` in WSL — closes the nine-DPB CP/M axis and
   ISO checking for one command.
3. Wire `ghostexp.exe` (already present, Ghost 11.5) as the GHO oracle.
4. Add oracle routing + `oracle-hosts.toml` to the runner.
5. Stand up one Linux host with `linux-modules-extra` — biggest single
   coverage jump.
6. Build the `86box-os2-hpfs` profile.
7. Get a Mac into the loop. Nothing else closes HFS/HFS+/APFS/DMG/APM.
