# Iris / IRIX oracle

IRIX 6.5.22 on an emulated SGI Indy, checking our EFS with **its own**
`/sbin/fsck` and mounting it with its own kernel. `strength = authoritative`:
this is the implementation, not a reimplementation. It produced R-039 and then
confirmed the fix.

## Why this one is different

Every EFS check before it was our code checked by our code. R-039 is the case
for why that is not enough: the free-space bitmap was MSB-first in our reader
*and* our writer, so our formatter and our fsck agreed with each other
perfectly and were both wrong. A self-consistent convention cannot be caught by
self-consistent fixtures. IRIX caught it in one run.

## Setup

Machine-specific paths live in the gitignored `data/oracles.local.toml`, never
here. What the oracle expects:

* `iris.exe` and `iris-ci.exe`
* an IRIX **6.5** disk image (5.3 hangs at "The system is coming up" —
  `Find Error: 10`, never reaches a login prompt)
* a scratch SCSI device declared **in `iris.toml`**, not on the command line:

```toml
[scsi.1]
path = "disks/Indy-IRIX65_dev.chd"
cdrom = false

[scsi.2]
path = "disks/scratch.img"
cdrom = false
scratch = true
size_mb = 64
```

`scsi` is a map keyed by id, not an array of tables, and every device needs
`cdrom`. iris creates the scratch volume itself, SGI volume header and all.

## Running it

```
IRIS_JIT=1 iris --headless --noaudio --ci \
    --scsi1 disks/Indy-IRIX65_dev.chd --cdrom4 <any.iso>

iris-ci boot && iris-ci login
iris-ci save booted-shell          # then every later run is a rollback
iris-ci scratch write ./volume.img
iris-ci run "/sbin/fsck -t efs -n /dev/rdsk/dks0d2s0 < /dev/null"
```

SCSI id 2 is `dks0d2`; `s0` is the payload partition.

## Five things that cost time

1. **No PROM needed.** It warns about a missing `prom.bin` and uses an
   embedded one. Do not go hunting for Indy firmware.
2. **`--ci` is required.** Without it iris listens only on the monitor port
   8888 and `iris-ci` cannot reach 19851.
3. **A CD-ROM must be attached even when booting from disk**, or startup dies
   with "could not attach cdrom4.iso". Any ISO will do.
4. **`< /dev/null` is load-bearing.** Without it fsck blocks forever on the
   SALVAGE prompt and the run just hangs.
5. **Size the volume to `s0`, not to the device.** `prtvtoc /dev/rdsk/dks0d2vh`
   reports `s0` starting at sector 8 with 131064 sectors on a 64 MB scratch —
   8 sectors short of the whole device, because the SGI volume header occupies
   the front. `scratch write` lands at exactly `s0`'s start, so a filesystem
   built to the full 64 MB overhangs the partition and fsck reports
   "Primary superblock size check: filesystem larger than device" — which
   looks like a filesystem defect and is not one. Build it at
   `65532K` (= 131064 sectors).

Also note `iris-ci run` reports `guest exit -1` on every command, including
ones that plainly worked. Judge results by stdout, not exit status.

## Beyond fsck: mount it

fsck passing is necessary, not sufficient. The stronger check is to let IRIX
use the filesystem:

```
iris-ci run "mkdir -p /mnt2 && mount -t efs /dev/dsk/dks0d2s0 /mnt2"
iris-ci run "echo hello > /mnt2/f.txt && mkdir /mnt2/d && umount /mnt2"
iris-ci run "/sbin/fsck -t efs -n /dev/rdsk/dks0d2s0 < /dev/null"
```

then pull it back with `iris-ci scratch read` and confirm rb-cli reads what
IRIX wrote. That full loop is what closed R-039.

## Always run a control

`mkfs_efs` on the same device, in the same session, through the same path is
the control — and it is also the best diagnostic tool here. When R-039 was
open, the difference between our volume and the one `mkfs_efs` produced *on
the same device* is what identified the bit order:

```
iris-ci run "/sbin/mkfs_efs /dev/rdsk/dks0d2s0 < /dev/null"
iris-ci scratch read ./mkfs-reference.img
```

Reference geometry on the 64 MB scratch: `blocks=131064 inodes=18696
sectors=128 cgfsize=21838 cgalign=1 ialign=1 ncg=6 firstcg=34 cgisize=779
bitmap blocks=32`.
