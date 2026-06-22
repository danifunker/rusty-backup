# cb-dos networking (mTCP)

Optional DOS TCP/IP for the cb-dos backup box, so you can move a backup image
**off the machine over the network** (FTP) instead of swapping disks. Built on
[mTCP](https://www.brutman.com/mTCP/) (GPLv3) + a per-card **packet driver**.

Full user guide: [`../../docs/cb_dos_networking.md`](../../docs/cb_dos_networking.md).

## What's here

| Path | What it is | In git? |
|---|---|---|
| `fetch-mtcp.sh` | downloads the mTCP apps into `mtcp/` (GPLv3) | yes |
| `mtcp/` | the fetched mTCP `.EXE`s (DHCP, FTP, FTPSRV, PING, …) | no (fetched) |
| `drivers/DRIVERS.TXT` | the card → packet-driver map (the "library") | yes |
| `drivers/*.COM` | packet driver(s) you drop in for your NIC | no (you add) |
| `MTCP.CFG`, `NET.BAT` | sample mTCP config + a load/DHCP helper | yes |

## How it gets onto the media

`mkmedia.sh` adds a `\NET` directory to the **cb-dos CD** (the floppy is too
small) whenever `mtcp/` contains the apps. So:

```sh
sh crusty-backup/net/fetch-mtcp.sh        # get mTCP (once)
cp /path/to/NE2000.COM crusty-backup/net/drivers/   # your card's packet driver
FDBASE=/path/to/x86BOOT.img sh crusty-backup/mkmedia.sh
```

On the booted DOS box: load your packet driver, then run `\NET\NET` — see the
guide for the full walkthrough (DHCP, FTPSRV/FTP, and the no-NIC serial/SLIP
option).
