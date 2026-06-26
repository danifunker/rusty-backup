# cb-dos networking — moving backups off the DOS box (mTCP)

[`cb-dos`](cb_dos.md) images a vintage machine's disk to a file over BIOS
`int 13h`. To get that file *off* the machine without shuffling floppies, the
cb-dos CD can carry a small DOS TCP/IP stack — **[mTCP](https://www.brutman.com/mTCP/)**
(GPLv3, by Michael Brutman) — so you can FTP the backup to another PC. This page
is the user guide and the packet-driver reference.

## The two pieces you need

DOS networking is always two layers:

1. **A packet driver** — a tiny TSR (`.COM`) *specific to your network card*. It
   talks to the NIC and exposes it on a software interrupt (we use `0x60`).
   There is **no universal driver**; you supply the one for your card. The
   card → driver map is in [`../crusty-backup/net/drivers/DRIVERS.TXT`](../crusty-backup/net/drivers/DRIVERS.TXT)
   (and it ships on the CD as `\NET\DRIVERS\DRIVERS.TXT`).
2. **mTCP** — the TCP/IP applications (`DHCP`, `FTP`, `FTPSRV`, `PING`, …) that
   run on top of the packet driver.

## Quick start on the booted DOS box

The CD mounts as a drive letter (typically `D:` — the FreeDOS boot floppy loads
a CD-ROM driver). Then:

```dos
REM 1. Load YOUR card's packet driver on software int 60h (see DRIVERS.TXT).
REM    NE2000 clone at I/O 300h, IRQ 3:
D:\NET\DRIVERS\NE2000 0x60 3 0x300

REM 2. Get an IP and see what's next:
D:\NET\NET

REM 3a. Serve the backup so another PC pulls it (easiest):
D:\NET\FTPSRV
REM    ...then from your modern PC:  ftp <dos-box-ip>   (log in, `get backup.img`)

REM 3b. ...or push it out to an FTP server:
D:\NET\FTP <server>      (log in, `put backup.img`)
```

`\NET\NET.BAT` sets `MTCPCFG=\NET\MTCP.CFG` and runs `DHCP` for you; tweak
`MTCP.CFG` for a static address. Copy `\NET` to a writable disk first if you
want to keep DHCP's leased settings between runs.

## No network card? Use a serial cable (SLIP)

mTCP speaks SLIP over a COM port, so any machine with a serial port can transfer
over a null-modem cable to a host running a SLIP peer — slow, but it needs no
NIC and no packet driver. See the mTCP PDF (`EtherSLIP`) bundled with the apps.

## The packet-driver "library"

We can't bundle every driver (they're per-card and come from card vendors / the
GPL **Crynwr** collection), so the repo ships the **map** and you drop in the
one(s) you need:

| Card / chipset | Packet driver | Notes |
|---|---|---|
| NE2000 / RTL8019 (ISA) | `NE2000.COM` | the workhorse; `NE2000 0x60 <irq> <io>` |
| 3Com 3C509 (ISA) | `3C5X9PD.COM` | self-configuring |
| 3Com 3C905 (PCI) | `3C90XPD.COM` | |
| AMD PCnet (ISA/PCI) | `PCNTPK.COM` | common in emulators/qemu too |
| Intel EtherExpress Pro/100 | `E100BPKT.COM` | |
| DEC 21x4x "Tulip" (PCI) | `DC21X4.COM` | |
| Realtek 8139 / 8169 (PCI) | `RTSPKT.COM` / `R8169PD.COM` | |
| SMC/WD 80x3 (ISA) | `WD8003E.COM` | |
| DM&P Vortex86 **R6040** (ITX-Llama) | vendor DOS packet driver | from DM&P |

Get drivers from the card's driver disk/vendor site, the **Crynwr** packet
driver collection (GPL; mirrored on DOS archives and the Internet Archive — look
for `PKTD11.ZIP`), or the FreeDOS networking packages.

## Building media with networking

There are two networking lanes, and they ship differently:

- **crustybk's own `backup rb://host/name`** (the WATT-32 stack baked into
  `CRUSTYBK.EXE`) works from **both** the floppy and the CD. The floppy carries
  `WATTCP.CFG` (DHCP by default) at its root and a `\NET\DRIVERS` directory; drop
  your card's packet driver into `net/drivers/` and it lands there. (To fit the
  ~1 MB networked `CRUSTYBK.EXE` on a 1.44 MB floppy, `mkmedia.sh` UPX-packs it —
  it self-extracts into RAM at launch, so the stack is fully intact.)
- **The mTCP FTP suite** (move a finished backup *file* off the box over FTP) is
  larger and rides the **CD only**:

```sh
sh crusty-backup/net/fetch-mtcp.sh                 # download mTCP (GPLv3), once
cp /path/to/NE2000.COM crusty-backup/net/drivers/  # your card's packet driver(s)
FDBASE=/path/to/x86BOOT.img sh crusty-backup/mkmedia.sh
```

`mkmedia.sh` adds a `\NET` directory (mTCP apps + your packet drivers +
`MTCP.CFG` + `NET.BAT`) to `cbdos.iso` when `net/mtcp/` is populated; without it
the CD's mTCP lane is absent (the floppy's `\NET\DRIVERS` + `WATTCP.CFG` for
`backup rb://` are always present). Repo-side layout:
[`../crusty-backup/net/README.md`](../crusty-backup/net/README.md).

## Status / caveats

- This is the **transport** for backups (FTP), not an integration into cb-dos's
  imaging — image to a file, then FTP it. A future step could stream directly.
- Accessing `\NET` assumes the boot environment loaded a CD-ROM driver (the
  FreeDOS 1.4 boot floppy does); if not, copy `\NET` from the CD by hand.
- Not yet tested on real hardware with a real NIC — the build wiring is in
  place; the DOS-side walkthrough above follows standard mTCP usage.
- mTCP needs ~96–256 KB free RAM depending on the app; fine on anything that
  runs DOS networking at all.
