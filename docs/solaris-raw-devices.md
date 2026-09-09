# Raw devices on Solaris

How `rb-cli` sees disks on Solaris, why enumeration works the way it does, and what to do
when a USB device wedges the driver.

## Device naming, and the difference between SPARC and x86

Solaris exposes every disk twice. `/dev/dsk/...` is the buffered block device; `/dev/rdsk/...`
is the raw character device. Only the raw node gives unbuffered access, and it insists that
every read and write be a whole multiple of the sector size -- an unaligned one fails
`EINVAL` rather than returning a short count. That is why `SectorAlignedWriter` is mandatory
here rather than an optimisation.

The whole-disk node is spelled differently per architecture:

| | Whole disk | Partitions |
|---|---|---|
| **SPARC** | `cXtYdZs2` -- slice 2 is the whole disk by SMI/VTOC convention | `s0`-`s7` |
| **x86** | `cXtYdZp0` -- the fdisk table | `p1`-`p4`, with `s0`-`s7` inside the Solaris one |

`src/os/solaris.rs` probes `p0` then `s2`, so one binary handles both. This matters because
Oracle's own USB documentation shows `p0` throughout -- those examples are from x86 systems.
On SPARC **every** device gets `s0`-`s7`, including USB sticks; there are no `p` nodes at all.

## Enumeration never opens a device

A wedged driver blocks in `open(2)` and **ignores `O_NONBLOCK`**. Measured on a Sun Blade 2500
running Solaris 9: a USB stick behind `scsa2usb` left processes stuck in the kernel that
`kill -9` could not touch. Solaris' own tools have the same problem --

    format(1M)     scans by opening /dev/rdsk/*s2   -> hangs
    rmformat(1)    the documented removable tool     -> hangs
    iostat -E      reads published kstats            -> returns instantly

-- so listing disks by opening them is not merely slow, it is a hang waiting for one bad
device. Oracle explicitly warns never to point `format` at a USB drive.

`rb-cli` therefore reads what the driver already publishes:

1. `/dev/rdsk/<disk>{p0,s2}` is a symlink into `/devices` -- `readlink`, no open.
2. `/etc/path_to_inst` maps that physical path to a driver instance (`sd`, instance 32).
3. `libkstat` reads `<driver>err:<instance>` for `Size`, `Vendor` and `Product`.
4. `/etc/mnttab` supplies mount points.

A device that has stopped answering publishes `Size 0`, which is how it is excluded without
anything touching it. `libdiskmgt` -- the API `format` and the installer use -- is **32-bit
only on Solaris 9**, so it is unavailable to a `sparcv9` binary.

The one ioctl still used is `DKIOCREMOVABLE`, and only for disks whose kstat already reports
a real size, so the device is known to be answering first.

## USB mass storage is beta

USB support on Solaris 9 is **beta** and depends more on the device than on the OS. The
`scsa2usb` driver issues a full SCSI command set that many cheap flash bridges do not
implement; those devices then wedge on open, exactly as above. Symptoms, all visible without
touching the device:

    iostat -En            Size: 0.00GB <0 bytes>, high Transport Errors
    kstat -p sderr:::     <driver>err:<inst>:Size  0

`scripts/solaris-usb-unblock.sh` reports this state and prints the documented remedies.

The supported fix is to tell the driver to use the reduced command set, per
<https://docs.oracle.com/cd/E19253-01/817-5093/devusbtasks-12/index.html>. In
`/kernel/drv/scsa2usb.conf`:

    attribute-override-list = "vid=* reduced-cmd-support=true";

then `update_drv -f scsa2usb`. This edits kernel driver configuration and affects **all** USB
storage on the machine, so it is not something rb-cli does for you. Note that the driver
cannot reload while processes are still stuck holding it open, and that a malformed `.conf`
will not be noticed until the next boot -- which matters on a machine whose `auto-boot?` is
false.

Once wedged, the only reliable recovery is to unplug the device; the stuck processes clear
when it goes away.
