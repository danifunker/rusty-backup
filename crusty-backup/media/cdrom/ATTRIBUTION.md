# cb-dos bundled CD-ROM drivers — attribution & licensing

These DOS CD-ROM drivers are bundled on the cb-dos media so a freshly-booted
machine can read an optical drive (e.g. to copy a packet driver off the CD, or
restore from a disc). They are **third-party programs merely aggregated** onto
the boot media — **not part of rusty-backup and not under its AGPL-3.0 license**.

| File | What it is | License |
|---|---|---|
| `oakcdrom.sys` | OAK Technology generic ATAPI/IDE CD-ROM driver (the Win98-boot-disk one) | **Proprietary**, (C) Oak Technology Inc. 1987-1997 |
| `usbaspi.exe` | Panasonic USB mass-storage ASPI manager (USB-2/EHCI capable) | **Proprietary** (Panasonic) |
| `usbcd1.sys` | Panasonic USB CD-ROM class driver (over ASPI) | **Proprietary** (Panasonic) |
| `shsucdx.com` | SHSUCDX — MSCDEX-compatible CD-ROM redirector (assigns the drive letter) | **Freeware / open**, by Jason Hood |

`oakcdrom.sys` and the two Panasonic USB drivers are **proprietary abandonware**
(no explicit redistribution grant, but freely circulated for decades); they are
included unmodified, as-is hardware drivers, with their copyright notices
preserved — the same posture as the bundled 3Com NIC driver
(`net/drivers/3c90xpd.com`). `shsucdx.com` is freely redistributable.

To rebuild the media without the proprietary CD drivers, remove the files from
`media/cdrom/` and drop the `DEVICE=` lines from `media/cbdos-fdconfig.sys`.
