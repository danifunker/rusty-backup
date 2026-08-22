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

## Validating an SGI volume header (F-006 step 1)

`rb-cli new hd sgi-efs` writes an SGI volume header, and until 2026-08-15 the
verb printed "real IRIX fx/prtvtoc validation is unverified without
hardware/emulator". It is verified now, and the method is cheap enough to
repeat whenever the header code changes.

**Do not transfer the whole disk image.** `iris-ci put` of a 64 MB image stages
it through the scratch device and then copies it guest-side, which took longer
than the 120 s command timeout and left the shell blocked. The label is 512
bytes and is all the check needs:

```
head -c 512 sgihd.img > our-vh.bin          # on the host
iris-ci scratch write ./our-vh.bin           # lands at payload = device sector 8
iris-ci run "dd if=/dev/rdsk/dks0d2s0 of=/dev/rdsk/dks0d2vh bs=512 count=1"
iris-ci run "prtvtoc /dev/rdsk/dks0d2vh"
```

The `dd` shifts the label from the payload start back to sector 0, where a
volume header belongs.

For the stricter tool, `fx` is menu-driven and its input must be *piped*, not
redirected — `< /dev/null` swallows the script and leaves it sitting at a
prompt:

```
iris-ci run "printf 'label
show
all
..
..
exi
' | fx -x 'dksc(0,2,0)'"
```

Size the image to whole cylinders that fit the device: the builder rounds
*up*, so 65540K yields 132048 sectors and overshoots the 131080-sector scratch;
65520K yields 130 cylinders / 131040 sectors and fits.

**Result, 2026-08-15.** `fx` 6.5 opens the drive, passes selftest and prints
our partitions, bootinfo and geometry without complaint. `prtvtoc` agrees
field for field. The directory section is empty, which is correct for what we
write and is exactly the gap a bootable disc has to fill:

```
----- partitions-----
  0: efs        5040 + 126000         2 + 62
  8: volhdr        0 + 5040           0 + 2
 10: volume        0 + 131040         0 + 64
----- bootinfo-----
 root partition = 0     swap partition = 0    bootfile = /unix
----- directory entries-----
```

Two limits on that claim. `fx` *read* the label; nothing here tests fx
rewriting one, or the kernel mounting the filesystem from it (only the 512-byte
label was written to the device, not the EFS behind it). And the label declares
130 cylinders while the drive reports 131 — `fx` did not object, but a bootable
disk should match the drive exactly, so the rounding wants revisiting before
step 2.
