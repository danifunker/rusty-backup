# Motion / IRIX 3.7 oracle

A real **IRIX 3.7** on an emulated **SGI IRIS 3130** (68020), mounting our EFS
v1 volumes with its own kernel. `strength = authoritative`: this is the
implementation the format was written for, not a reimplementation.

The counterpart to [`../iris`](../iris/README.md), one machine generation
earlier. Iris checks IRIX EFS on an Indy; Motion checks **EFS v1** — different
magic, a 68020-packed superblock, System V directories — on the hardware that
format shipped on. Neither can stand in for the other.

## Why it is worth having

Everything else that checks EFS v1 is ours. `docs/SGI_EFS_v1.md` used to say
so plainly: no emulator ran IRIS 3000-series hardware well enough to boot the
OS, so three self-consistency invariants and a comparison against an
independent decoder stood in for one. R-039 — where our bitmap bit order was
wrong in the reader *and* the writer, so our formatter and our fsck agreed with
each other and both disagreed with IRIX — is the case for why that is not
enough. A self-consistent convention cannot be caught by self-consistent
fixtures.

## Setup

Machine-specific paths live in the gitignored `data/oracles.local.toml`, never
here. What the oracle expects:

* a Motion checkout with `motion` built — `$MOTION_ROOT`, default
  `~/repos/motion`, binary under `build/output/RelWithDebInfo/`
* the reference IRIS 3130 disk — `$IRIS_DISK`, default `~/3130.img`

Nothing else: no PROM hunting (the ROM ships in the checkout), no guest
networking, no control socket.

## Running it

```
regression-tests/oracles/motion/efs_v1_mount.sh <volume.img>
```

which forwards to `scripts/sgi-efs-v1-oracle.sh mount`. That script is the one
implementation of the boot machinery; developers drive it directly and
`rb-regress verify` drives it through the wrapper, so the two cannot drift.

Exit codes follow the runner's convention — 0 pass, 1 IRIX disagreed, 77
nothing to run it with on this host, 99 ran and reached no verdict.

## How the verdict gets out

There is no control socket and no screen scraping. Run Motion **without**
`+set enableGF2 1` and the guest's serial console lands on stdout, one line per

```
[Emulation - Serial] ... [line 1] <text>
```

so a whole boot is greppable. The check drives the guest by patching
`/etc/rc.s0` — the `sysinit` entry, which runs at every boot regardless of the
disk's `initdefault` — to mount the volume and list it, and reads the answer
off the console.

The volume under test goes into the reference disk's **`/usr` slot** (`md0c`,
block 35700, 79730 blocks). The disk's own label is untouched, so what is being
checked is the filesystem, not the partitioning.

## Five things that cost time

1. **It opens the disk read-only and attaches drive 0 only.** `dsd5217.cpp`
   calls `Profile::OpenDisk(0)` and nothing else; `profileDisk1Path` exists but
   is unwired. So "attach a whole disk we built as `md1`" does not work yet —
   hence the splice into an existing slot, and hence `part.sgi-dklabel` is
   declared but untested. The image is byte-identical afterwards.
2. **Word order is load-bearing.** The medium is swapped within every 16-bit
   word (the IP2 crosses the Multibus byte lanes). `new volume efs-v1` writes
   native, so the check `swab16`s it on the way in. Skip that and the kernel
   sees noise and the run reads as a filesystem fault.
3. **About one boot in six dies at kernel entry** before running anything —
   the last console line is `Jumping to loaded program @ 20000400`. That is the
   emulator, not the volume, so the check retries once and then reports exit 99
   rather than inventing a finding.
4. **`/mnt` does not exist on the reference disk.** Without a `mkdir` the mount
   fails in a way that reads like a filesystem fault.
5. **IRIX 3.7's `mount` is silent on success.** Its refusal is
   `/dev/md0c:Invalid argument` — nothing with `mount:` in it. Matching the
   wrong string is exactly how a broken volume passes.

## Always run a control

8 MB of `/dev/urandom` in the same slot, through the same path:

```
head -c 8388608 /dev/urandom > noise.img
regression-tests/oracles/motion/efs_v1_mount.sh noise.img
```

Run 2026-08-24: `/dev/md0c:Invalid argument`, exit 1. So a pass is the oracle
discriminating, not the oracle passing everything.

## Beyond mount: the deeper form

`scripts/sgi-efs-v1-oracle.sh prove` is not wired to an artifact and so is not
a `check` — it is what a person runs after touching the write path. It takes
the reference disk itself and has rb-cli rewrite and **grow** `/etc/rc.s0` past
its allocated blocks, create a file, create a directory with a file in it, and
allocate a fresh 256 KiB file. Booting it, IRIX runs the rewritten script,
`cat`s the new file and directory, `sum`s the allocated file to the right
checksum (262144 x `A` folds to 260), and mounts the *second* EFS v1
filesystem off `md0c`. Apart from those lines the console is identical to a
stock boot.

Writing from inside the guest — the step that closed R-039 on the Indy — is not
available here while the emulator holds the disk read-only.
