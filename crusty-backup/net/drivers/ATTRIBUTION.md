# cb-dos bundled packet drivers — attribution & licensing

The packet drivers in this directory (and shipped under `\NET\DRIVERS` on the
cb-dos floppy/CD) are third-party programs under their own licenses. They are
**independent works merely aggregated** onto the boot media — they are **not part
of rusty-backup and not covered by its AGPL-3.0 license**.

## Crynwr packet-driver collection — GPL v2
Most of the drivers here (`ne2000.com`, the `3c5xx.com` ISA set, `pcntpk.com`,
`rtspkt.com`, `wd*.com`, `smc_wd.com`, … ~98 files) are the **Crynwr Software**
packet-driver collection.

- License: **GNU GPL v2** — full text in [`CRYNWR-GPL.txt`](CRYNWR-GPL.txt).
- Source: fetched by [`../fetch-drivers.sh`](../fetch-drivers.sh) from the
  FreeDOS mirror (`crynwr.zip`), which also contains the driver source.

## `r6040pd.com` — RDC / DM&P Vortex86 R6040 (ITX-Llama onboard NIC) — GPL
"Packet driver for RDC R6040 PCI Series Ethernet Adapter", v0.1.6. Built on the
Crynwr GPL packet-driver skeleton (it embeds *"Packet driver skeleton copyright
1988-93, Crynwr Software"*), so it falls under the same **GPL v2** umbrella.
Added manually (not part of `crynwr.zip`).

## `3c90xpd.com` — 3Com EtherLink PCI Bus Master (3C90x / 3C905) — PROPRIETARY
"3Com EtherLink PCI Bus Master Packet Driver v5.2.6",
**(C) Copyright 1999 3Com Corporation. All rights reserved.**

- This is the **one bundled driver that is NOT (A)GPL or open source.** It is
  3Com's proprietary DOS packet driver, included **unmodified** so the 3C905
  family works out of the box.
- It is redistributed here as an as-is hardware driver. 3Com is defunct
  (acquired by HP in 2010) and this DOS packet driver has been freely
  distributed for decades; it is bundled on that basis, with this notice and
  3Com's copyright preserved.
- **To redistribute this media without any proprietary driver**, delete
  `3c90xpd.com` from `net/drivers/` and remove `3c90xpd` from the floppy subset
  in `mkmedia.sh` before building.
- Added manually (not part of `crynwr.zip`).
