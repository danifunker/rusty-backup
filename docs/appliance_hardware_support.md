# Appliance hardware support & Buildroot customization

The rusty-backup Linux appliance (see [`linux_486_appliance.md`](linux_486_appliance.md))
ships a kernel built to recognise as much **486/Pentium-era hardware** as is
practical out of the box — the disks you back up, the medium you write to, and
the cards on a vintage motherboard or in its ISA/PCI/PC-Card slots. This page is
the user's guide to **what's baked in**, **how to make an unusual card work**,
and **how to rebuild the kernel** when you need something we didn't include.

The driver list lives in [`../buildroot/kernel.fragment`](../buildroot/kernel.fragment)
— that one file is the source of truth; everything below describes it.

---

## What's baked in

| Class | Built-in (auto-detected) | Loadable module (needs `modprobe`) |
|---|---|---|
| **IDE / PATA** | libata: PIIX, generic, AHCI/SATA, plus PCI chipsets (VIA, SiS, ALi, AMD, HPT, CMD64x, Promise, ServerWorks, SiL680, ITE, …) and the bare-486 / VL-Bus / ISA controllers (`pata_legacy`, QDI, Winbond-VLB, RZ1000, CMD640) | — |
| **SCSI** | PCI HBAs: Adaptec aic7xxx/aic79xx, Symbios/NCR 53c8xx, BusLogic, AdvanSys, Initio, AMD 53c974, DC395x, QLogic 1280 | ISA HBAs: Adaptec aha152x/1542/1740, NCR5380 family, QLogic FAS, DTC3280, PAS16, T128, NCR53c406a; parallel-port ZIP/JAZ (`ppa`, `imm`) |
| **Ethernet** | PCI NICs: NE2000-PCI, RTL8139/8169, AMD PCnet32, DEC Tulip & clones (DE2104x, dmfe, uli526x, Winbond-840), 3c59x/3c90x, Intel e100/e1000, VIA Rhine/Velocity, SiS900, NatSemi, SMC EPIC, Sundance/DL2k, Starfire, ThunderLAN, … | ISA NICs: NE2000 (`ne`), 3c509 (`3c509`), 3c515, SMC Ultra/WD80x3, AMD Lance/NI65, SMC9194, Cirrus CS89x0; PLIP (parallel-cable IP) |
| **PCMCIA / CardBus** | bridges: Yenta (CardBus), i82365 / TCIC (ISA PCIC), PD6729, i82092 | PC-Card NICs (3c589, 3c574, pcnet_cs, axnet_cs, smc91c92, xirc2ps, nmclan, fmvj18x), PC-Card IDE (`pata_pcmcia`), PC-Card serial |
| **USB storage** | UHCI + OHCI + EHCI host controllers, USB mass-storage (a stick/drive as a destination) | — |
| **Serial / parallel (Multi-I/O)** | 8250/16550 up to 8 UARTs with IRQ auto-detect & shared-IRQ (the extra COM ports on a Multi-I/O card), PC parallel port | — |
| **Display** | `vgacon` standard VGA text console — works on **every** VGA/SVGA card | VESA / VGA16 framebuffer (optional; needs a `vga=` mode on the kernel command line) |
| **Filesystems (destination)** | FAT/VFAT, ext4, ISO9660+Joliet | — |

> **Why built-in vs. module?** PCI and onboard controllers *enumerate themselves*
> on the bus, so a built-in driver binds automatically with zero configuration.
> ISA and PC-Card devices have **no enumeration** — the driver has to be told
> which I/O port and IRQ to poke — and blind probing at boot is slow and can wedge
> a machine, so those ship as **modules you load on demand** with the right
> parameters. The modules are already in the image under `/lib/modules`.

> **Display note.** The appliance is a *text* console, and `vgacon` (standard
> 80×25 VGA text mode) is supported by literally every VGA-compatible card ever
> made, so your monitor "just works" regardless of the chipset (Trident, Cirrus,
> S3, ET4000, …). The VESA framebuffer is only there if you specifically want a
> graphical console; you don't need it. (See the boot-console note at the bottom.)

---

## Making a card work

### PCI cards — nothing to do
Plug it in (or it's onboard) and boot. Confirm it was found:

```sh
lspci -k                 # cards + the driver bound to each (if pciutils present)
dmesg | grep -iE 'eth|scsi|ata|sym|aic'
ip link                  # network interfaces
ls /sys/class/scsi_disk  # SCSI disks
```

### ISA cards — load the module with its I/O port + IRQ
You need to know the card's jumper/soft settings (I/O base + IRQ). Examples:

```sh
# NE2000 ISA network card at 0x300, IRQ 10
modprobe ne io=0x300 irq=10

# 3Com 3c509 (it self-identifies — no params needed)
modprobe 3c509

# Adaptec AHA-1520 ISA SCSI at 0x340, IRQ 11, host ID 7
modprobe aha152x io=0x340 irq=11 scsiid=7

# Adaptec AHA-1542 ISA SCSI (probes its standard ports)
modprobe aha1542

# Parallel-port Iomega ZIP drive
modprobe ppa            # or: modprobe imm   (for newer ZIP/parallel models)
```

After loading, re-check `dmesg`, `ip link`, or the SCSI disk list. A network
device shows up as `eth0`/`eth1`; a SCSI disk as `/dev/sda`, etc. — then use it
with `rb-cli` exactly like any other device.

### PCMCIA / CardBus (vintage laptops)
The socket bridge is built in. Insert the card, then load its driver module
(e.g. `modprobe pcnet_cs` for many PC-Card NE2000 clones, `modprobe 3c589_cs`
for a 3Com 3c589). CardBus (32-bit PC Card) NICs are usually just PCI under the
hood and bind automatically.

### Extra serial ports on a Multi-I/O card
The kernel auto-detects up to 8 16550 UARTs with shared/auto IRQs. If a port
isn't found automatically you can register it by hand with `setserial`
(if installed) or pass `8250.io=…` style options — but COM1/COM2 at the
standard 0x3F8/0x2F8 are always there.

---

## Did my driver actually load?

```sh
lsmod                       # loaded modules
dmesg | less                # probe messages / errors
zcat /proc/config.gz | grep CONFIG_NE2K_PCI    # is a driver built into THIS kernel?
ls /lib/modules/$(uname -r)/kernel/drivers/net # what modules are available to load
```

`/proc/config.gz` is the **exact** configuration of the running kernel (enabled
by `CONFIG_IKCONFIG`), so it's the definitive answer to "is driver X in here?".

---

## Rebuilding the kernel with different drivers

The appliance image is built by Buildroot inside a Docker container. Two ways to
change what the kernel supports:

### 1. Edit the fragment (quick, scriptable)
Add or change lines in [`../buildroot/kernel.fragment`](../buildroot/kernel.fragment):

```
CONFIG_SOMEDRIVER=y     # build it into the kernel (auto-loads, bigger kernel)
CONFIG_SOMEDRIVER=m     # build it as a loadable module (modprobe on demand)
# (delete the line, or set =n, to leave it out)
```

Then rebuild (≈25–30 min, see [`linux_486_appliance.md`](linux_486_appliance.md)):

```sh
docker run --rm -v "$PWD":/work rb-buildroot
# -> buildroot/output/{bzImage, rootfs.ext2, rusty-backup-appliance.iso}
```

Unknown/renamed option names are **silently ignored** by Buildroot's config
merge (you'll see a warning in the build log, not an error), so after a rebuild
always confirm with `/proc/config.gz` that the driver you wanted is actually in.

### 2. Interactive picker (`make linux-menuconfig`)
For browsing the full driver tree with help text and dependency handling, run
menuconfig **inside the build container**:

```sh
# start the container with a shell instead of the default build command
docker run --rm -it -v "$PWD":/work --entrypoint sh rb-buildroot
# then, inside the container:
cd /opt/buildroot
make qemu_x86_defconfig
# (apply the same overlays/options build.sh does, or just explore)
make linux-menuconfig          # Device Drivers -> Network/SCSI/ATA/...
# copy the resulting fragment back out, or save your selections into
# /work/buildroot/kernel.fragment so they persist across rebuilds
```

The durable place for any change is **`buildroot/kernel.fragment`** (it's in the
repo and feeds every build); menuconfig is for discovering option names.

---

## CPU baseline (which machines the kernel itself runs on)

The stock build targets **i686** (Pentium Pro and up), which boots on any
Pentium-class or newer box and in qemu. Targeting an actual **486** or **Pentium
(i586)** is a separate change to *both* the kernel and `rb-cli`; see the
"CPU baseline" section of [`linux_486_appliance.md`](linux_486_appliance.md) and
[`linux_486_build.md`](linux_486_build.md). The driver set in this document is
independent of the CPU target — it applies whichever baseline you build.

---

## Boot console (where the boot text and menu appear)

The ISO boots with `console=tty0 console=ttyS0,115200`, so the kernel logs to
**both** the VGA monitor and the first serial port, and the rb-cli menu runs on
both (a getty on `tty1` for the screen and on `ttyS0` for a serial terminal).
That means you can drive the appliance from a real monitor *or* headless over a
null-modem cable / `qemu -nographic`.

> **QEMU gotcha:** qemu's default `-vga std` (Bochs stdvga) emulation stops
> repainting the framebuffer partway through a Linux boot, so a qemu boot can
> *look* frozen at the PCI/ACPI lines even though the kernel is running fine.
> Watch a qemu boot over the **serial console** (`-nographic` / `-serial`), or
> use **`-vga cirrus`**, and it boots through to the menu. This is purely a qemu
> display artifact — real VGA hardware is unaffected.
