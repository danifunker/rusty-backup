/* cmd_netdetect.c -- detect the PCI network card and report which bundled
 * packet driver applies. Driven by NET.BAT / FDAUTO.BAT: the EXIT CODE selects
 * the driver, which the BAT then loads from \NET\DRIVERS (if present) before
 * crustybk's WATT-32 networked backup attaches to it.
 *
 * PCI is walked read-only through the PCI BIOS (INT 1Ah) via DPMI -- the same
 * mechanism cbdisk.c uses for INT 13h -- so detection is non-destructive. ISA
 * cards (a bare NE2000, 3C509, WD80x3, ...) are not PCI-enumerable and stay
 * manual; for those, use the "NE2000-compatible ISA" boot-menu option.
 *
 * The exit code is the contract with NET.BAT -- keep enum + BAT in sync.
 */
#include <stdio.h>
#include <string.h>
#include <dpmi.h>

enum {
    NIC_NONE   = 0, /* no PCI / no card / unmatched -> ISA-manual */
    NIC_NE2000 = 1, /* RTL8029 PCI NE2000   -> NE2000.COM   */
    NIC_RTSPKT = 2, /* Realtek RTL8139      -> RTSPKT.COM   */
    NIC_R8169  = 3, /* Realtek RTL8169      -> R8169PD.COM  (CD-only) */
    NIC_PCNTPK = 4, /* AMD PCnet            -> PCNTPK.COM   */
    NIC_R6040  = 5, /* RDC/Vortex86 R6040   -> R6040PD.COM  */
    NIC_3C90X  = 6, /* 3Com 3C90x           -> 3C90XPD.COM  */
    NIC_E100   = 7, /* Intel 8255x          -> E100BPKT.COM */
    NIC_DC21X4 = 8  /* DEC 21x4x Tulip      -> DC21X4.COM   (CD-only) */
};

typedef struct {
    unsigned short ven, dev;
    int code;
    const char *drv;  /* 8.3 basename in \NET\DRIVERS (no extension) */
    const char *name;
} nicmap_t;

/* PCI vendor:device -> bundled packet driver. Covers the cards the floppy/CD
 * ship plus the common emulator NICs (QEMU/VirtualBox/86Box). */
static const nicmap_t NICS[] = {
    {0x10EC, 0x8029, NIC_NE2000, "NE2000",   "Realtek RTL8029 (PCI NE2000)"},
    {0x10EC, 0x8139, NIC_RTSPKT, "RTSPKT",   "Realtek RTL8139"},
    {0x10EC, 0x8138, NIC_RTSPKT, "RTSPKT",   "Realtek RTL8139B"},
    {0x10EC, 0x8169, NIC_R8169,  "R8169PD",  "Realtek RTL8169"},
    {0x10EC, 0x8167, NIC_R8169,  "R8169PD",  "Realtek RTL8169SC"},
    {0x10EC, 0x8168, NIC_R8169,  "R8169PD",  "Realtek RTL8168"},
    {0x10EC, 0x8136, NIC_R8169,  "R8169PD",  "Realtek RTL8101E"},
    {0x1022, 0x2000, NIC_PCNTPK, "PCNTPK",   "AMD PCnet (Am79C970/971)"},
    {0x1022, 0x2001, NIC_PCNTPK, "PCNTPK",   "AMD PCnet (Am79C978)"},
    {0x17F3, 0x6040, NIC_R6040,  "R6040PD",  "RDC/DM&P Vortex86 R6040"},
    {0x10B7, 0x9000, NIC_3C90X,  "3C90XPD",  "3Com 3C900 Boomerang"},
    {0x10B7, 0x9001, NIC_3C90X,  "3C90XPD",  "3Com 3C900B-TPO"},
    {0x10B7, 0x9004, NIC_3C90X,  "3C90XPD",  "3Com 3C900B-Combo"},
    {0x10B7, 0x9005, NIC_3C90X,  "3C90XPD",  "3Com 3C900B"},
    {0x10B7, 0x9050, NIC_3C90X,  "3C90XPD",  "3Com 3C905 Boomerang"},
    {0x10B7, 0x9051, NIC_3C90X,  "3C90XPD",  "3Com 3C905-T4"},
    {0x10B7, 0x9055, NIC_3C90X,  "3C90XPD",  "3Com 3C905B Cyclone"},
    {0x10B7, 0x9200, NIC_3C90X,  "3C90XPD",  "3Com 3C905C Tornado"},
    {0x10B7, 0x9201, NIC_3C90X,  "3C90XPD",  "3Com 3C920"},
    {0x8086, 0x1229, NIC_E100,   "E100BPKT", "Intel 8255x EtherExpress Pro/100"},
    {0x8086, 0x1209, NIC_E100,   "E100BPKT", "Intel 82559ER"},
    {0x8086, 0x1029, NIC_E100,   "E100BPKT", "Intel 82559"},
    {0x8086, 0x103D, NIC_E100,   "E100BPKT", "Intel 82801 (8255x)"},
    {0x1011, 0x0009, NIC_DC21X4, "DC21X4",   "DEC 21140 Tulip"},
    {0x1011, 0x0019, NIC_DC21X4, "DC21X4",   "DEC 21143 Tulip"}
};
#define NNICS ((int)(sizeof NICS / sizeof NICS[0]))

/* PCI BIOS present? (INT 1Ah, AX=B101h -> EDX='PCI ', CL=last bus) */
static int pci_present(int *last_bus) {
    __dpmi_regs r;
    memset(&r, 0, sizeof r);
    r.x.ax = 0xB101;
    __dpmi_int(0x1A, &r);
    if (r.x.flags & 1) return 0;            /* carry = not present */
    if (r.h.ah != 0) return 0;
    if (r.d.edx != 0x20494350UL) return 0;  /* 'PCI ' little-endian */
    if (last_bus) *last_bus = r.h.cl;
    return 1;
}

/* Read a PCI config dword (INT 1Ah, AX=B10Ah). 0xFFFFFFFF on error. */
static unsigned long pci_cfg_dword(int bus, int dev, int func, int reg) {
    __dpmi_regs r;
    memset(&r, 0, sizeof r);
    r.x.ax = 0xB10A;
    r.x.bx = (unsigned short)((bus << 8) | (dev << 3) | (func & 7));
    r.x.di = (unsigned short)reg;
    __dpmi_int(0x1A, &r);
    if (r.h.ah != 0) return 0xFFFFFFFFUL;
    return r.d.ecx;
}

/* Is \NET\DRIVERS\<drv>.COM present on the given drive letter? */
static int driver_present(char drive, const char *drv) {
    char path[64];
    FILE *f;
    sprintf(path, "%c:\\NET\\DRIVERS\\%s.COM", drive, drv);
    f = fopen(path, "rb");
    if (!f) return 0;
    fclose(f);
    return 1;
}

int cmd_netdetect(int argc, char **argv) {
    int last_bus = 0, bus, dev, func;
    int found_class = 0;
    unsigned short net_ven = 0, net_dev = 0;
    const nicmap_t *hit = NULL;
    (void)argc; (void)argv;

    printf("Scanning the PCI bus for a network card...\n");
    if (!pci_present(&last_bus)) {
        printf("  No PCI BIOS -- cannot auto-detect. For an ISA card use the\n");
        printf("  \"NE2000-compatible ISA\" boot option (see \\NET\\DRIVERS\\DRIVERS.TXT).\n");
        return NIC_NONE;
    }

    for (bus = 0; bus <= last_bus && !hit; bus++) {
        for (dev = 0; dev < 32 && !hit; dev++) {
            unsigned long id0 = pci_cfg_dword(bus, dev, 0, 0x00);
            unsigned short v0 = (unsigned short)(id0 & 0xFFFF);
            int nfunc, i;
            if (v0 == 0xFFFF || v0 == 0x0000) continue;
            nfunc = ((pci_cfg_dword(bus, dev, 0, 0x0C) >> 16) & 0x80) ? 8 : 1;
            for (func = 0; func < nfunc && !hit; func++) {
                unsigned long id = pci_cfg_dword(bus, dev, func, 0x00);
                unsigned short ven = (unsigned short)(id & 0xFFFF);
                unsigned short dvc = (unsigned short)(id >> 16);
                if (ven == 0xFFFF || ven == 0x0000) continue;
                if (((pci_cfg_dword(bus, dev, func, 0x08) >> 24) & 0xFF) != 0x02)
                    continue;               /* base class 0x02 = network */
                if (!found_class) { found_class = 1; net_ven = ven; net_dev = dvc; }
                for (i = 0; i < NNICS; i++)
                    if (NICS[i].ven == ven && NICS[i].dev == dvc) { hit = &NICS[i]; break; }
            }
        }
    }

    if (hit) {
        printf("  Found: %s  [%04X:%04X]\n", hit->name, hit->ven, hit->dev);
        printf("  Packet driver: %s.COM\n", hit->drv);
        if (driver_present('A', hit->drv)) {
            printf("  Present on A:\\NET\\DRIVERS -- it will be loaded.\n");
        } else {
            char d;
            int on_cd = 0;
            for (d = 'C'; d <= 'Z'; d++) {
                if (driver_present(d, hit->drv)) {
                    printf("  Not on the floppy. Copy it from the CD, then re-run NET:\n");
                    printf("    COPY %c:\\NET\\DRIVERS\\%s.COM A:\\NET\\DRIVERS\n", d, hit->drv);
                    on_cd = 1;
                    break;
                }
            }
            if (!on_cd)
                printf("  %s.COM is not on A: or any CD -- add it to A:\\NET\\DRIVERS.\n", hit->drv);
        }
        return hit->code;
    }

    if (found_class)
        printf("  A PCI network card [%04X:%04X] was found but no bundled driver\n"
               "  matches it -- see \\NET\\DRIVERS\\DRIVERS.TXT.\n", net_ven, net_dev);
    else
        printf("  No PCI network card found. (ISA cards are manual -- DRIVERS.TXT.)\n");
    return NIC_NONE;
}
