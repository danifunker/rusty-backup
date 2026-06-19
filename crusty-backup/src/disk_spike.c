/* cb-dos Phase 0b -- "hello disk" spike.
 *
 * Proves the low-level disk path cb-dos needs on real DOS hardware:
 *   1. enumerate BIOS hard drives        (int 13h AH=08h)
 *   2. detect LBA extensions             (int 13h AH=41h)
 *   3. read sectors, ext with CHS fall back (int 13h AH=42h / AH=02h)
 *   4. classify sector 0: MBR vs FAT boot sector, dump the partition table
 *   5. parse the FAT BPB (FAT12/16/32)
 *   6. read the FAT and count used/free clusters (the "allocation bitmap")
 *
 * DJGPP runs in 32-bit protected mode, but int 13h is a real-mode BIOS
 * service: the Disk Address Packet and the sector buffer must live in
 * conventional (< 1 MiB) memory reachable by a real-mode segment. We
 * allocate one DOS-memory transfer buffer up front and bounce sectors
 * through it with dosmemget/dosmemput.
 *
 * Output goes to both the screen and C:\SPIKE.LOG, so a headless run
 * (dosbox-x -exit) leaves its findings on the host-mounted drive.
 *
 * Build:  make            (adds build/disk_spike.exe)
 * Run:    DISKSPK [drive-hex]   e.g.  DISKSPK 80   (default 0x80)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdint.h>
#include <dpmi.h>
#include <go32.h>
#include <sys/movedata.h>

/* ----- logging to screen + file ------------------------------------ */

static FILE *g_log;

static void slog(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    if (g_log) {
        va_start(ap, fmt);
        vfprintf(g_log, fmt, ap);
        va_end(ap);
        fflush(g_log);
    }
}

/* ----- DOS-memory transfer buffer ---------------------------------- */

#define XFER_SECTORS 16                 /* 8 KiB bounce buffer */
#define XFER_BYTES   (XFER_SECTORS * 512)

static int      g_buf_seg;              /* real-mode segment of the buffer */
static int      g_buf_sel;             /* selector, for freeing */

static int xfer_init(void) {
    int para = (XFER_BYTES + 15) >> 4;
    g_buf_seg = __dpmi_allocate_dos_memory(para, &g_buf_sel);
    return g_buf_seg;                    /* -1 on failure */
}

static void xfer_free(void) {
    if (g_buf_seg > 0)
        __dpmi_free_dos_memory(g_buf_sel);
}

/* ----- drive geometry ---------------------------------------------- */

typedef struct {
    int  present;
    int  ext;                           /* LBA extensions available */
    unsigned cyls, heads, spt;          /* CHS geometry from AH=08h */
} drive_info_t;

/* int 13h AH=08h -- read drive parameters. */
static void drive_params(int drive, drive_info_t *di) {
    __dpmi_regs r;
    memset(di, 0, sizeof *di);
    memset(&r, 0, sizeof r);
    r.h.ah = 0x08;
    r.h.dl = drive;
    r.x.es = 0;                         /* recommended: ES:DI = 0:0 */
    r.x.di = 0;
    __dpmi_int(0x13, &r);
    if (r.x.flags & 1)                  /* CF set => no such drive */
        return;
    di->present = 1;
    di->heads = (unsigned)r.h.dh + 1;
    di->spt   = r.h.cl & 0x3F;
    di->cyls  = (((unsigned)(r.h.cl & 0xC0) << 2) | r.h.ch) + 1;
}

/* int 13h AH=41h -- INT 13 extensions installation check. */
static int drive_has_ext(int drive) {
    __dpmi_regs r;
    memset(&r, 0, sizeof r);
    r.h.ah = 0x41;
    r.x.bx = 0x55AA;
    r.h.dl = drive;
    __dpmi_int(0x13, &r);
    if (r.x.flags & 1) return 0;        /* CF => not supported */
    if (r.x.bx != 0xAA55) return 0;     /* signature must flip */
    return (r.x.cx & 0x0001) != 0;      /* bit0 = packet access supported */
}

/* ----- sector I/O --------------------------------------------------- */

/* Read `count` (<= XFER_SECTORS) sectors at `lba` into host `dst`.
 * Returns 0 on success, or the BIOS AH error code (nonzero). */
static int read_lba(const drive_info_t *di, int drive, uint64_t lba,
                    int count, void *dst) {
    __dpmi_regs r;
    int err;

    if (count < 1 || count > XFER_SECTORS) return 0xFF;

    memset(&r, 0, sizeof r);
    if (di->ext) {
        /* Build a 16-byte Disk Address Packet in the low buffer (after
         * the sector area would overlap, so stash it at the very end). */
        uint8_t dap[16];
        memset(dap, 0, sizeof dap);
        dap[0] = 0x10;                  /* packet size */
        dap[2] = count & 0xFF;
        dap[3] = (count >> 8) & 0xFF;
        dap[4] = 0;                     /* buffer offset lo */
        dap[5] = 0;                     /* buffer offset hi */
        dap[6] = g_buf_seg & 0xFF;      /* buffer segment lo */
        dap[7] = (g_buf_seg >> 8) & 0xFF;
        for (int i = 0; i < 8; i++)
            dap[8 + i] = (uint8_t)(lba >> (8 * i));
        /* place the DAP 16 bytes past the sector region in low memory */
        unsigned dap_lin = g_buf_seg * 16 + XFER_BYTES;
        dosmemput(dap, sizeof dap, dap_lin);

        r.h.ah = 0x42;
        r.h.dl = drive;
        r.x.ds = g_buf_seg + (XFER_BYTES >> 4); /* segment of the DAP */
        r.x.si = 0;
        __dpmi_int(0x13, &r);
        err = (r.x.flags & 1) ? r.h.ah : 0;
    } else {
        /* CHS read (AH=02h). Convert LBA -> CHS with AH=08h geometry. */
        unsigned spt = di->spt ? di->spt : 63;
        unsigned heads = di->heads ? di->heads : 16;
        unsigned cyl = (unsigned)(lba / (spt * heads));
        unsigned tmp = (unsigned)(lba % (spt * heads));
        unsigned head = tmp / spt;
        unsigned sect = tmp % spt + 1;  /* sectors are 1-based */
        if (cyl > 1023) return 0xFE;    /* beyond CHS reach */

        r.h.ah = 0x02;
        r.h.al = count;
        r.h.ch = cyl & 0xFF;
        r.h.cl = (uint8_t)((sect & 0x3F) | ((cyl >> 2) & 0xC0));
        r.h.dh = head;
        r.h.dl = drive;
        r.x.es = g_buf_seg;
        r.x.bx = 0;
        __dpmi_int(0x13, &r);
        err = (r.x.flags & 1) ? r.h.ah : 0;
    }

    if (err == 0)
        dosmemget(g_buf_seg * 16, count * 512, dst);
    return err;
}

/* ----- little-endian helpers --------------------------------------- */

static uint16_t rd16(const uint8_t *p) { return p[0] | (p[1] << 8); }
static uint32_t rd32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
static uint64_t rd64(const uint8_t *p) {
    return (uint64_t)rd32(p) | ((uint64_t)rd32(p + 4) << 32);
}

/* ----- FAT BPB ------------------------------------------------------ */

typedef struct {
    unsigned bytes_per_sec;
    unsigned sec_per_clus;
    unsigned reserved;
    unsigned num_fats;
    unsigned root_entries;
    uint32_t total_secs;
    uint32_t fat_secs;
    int      fat_bits;                  /* 12, 16, or 32 */
    uint32_t data_secs;
    uint32_t clusters;                  /* count of data clusters */
    uint32_t fat_start_lba;             /* relative to volume start */
} bpb_t;

/* Parse a FAT boot sector into bpb. Returns 0 on success. */
static int parse_bpb(const uint8_t *s, bpb_t *b) {
    memset(b, 0, sizeof *b);
    b->bytes_per_sec = rd16(s + 11);
    b->sec_per_clus  = s[13];
    b->reserved      = rd16(s + 14);
    b->num_fats      = s[16];
    b->root_entries  = rd16(s + 17);
    uint32_t tot16   = rd16(s + 19);
    uint32_t fat16   = rd16(s + 22);
    uint32_t tot32   = rd32(s + 32);
    uint32_t fat32   = rd32(s + 36);

    if (b->bytes_per_sec == 0 || b->sec_per_clus == 0) return -1;

    b->total_secs = tot16 ? tot16 : tot32;
    b->fat_secs   = fat16 ? fat16 : fat32;
    if (b->fat_secs == 0 || b->total_secs == 0) return -1;

    /* root dir occupies ceil(root_entries*32 / bytes_per_sec) sectors */
    unsigned root_secs =
        ((b->root_entries * 32) + (b->bytes_per_sec - 1)) / b->bytes_per_sec;

    uint32_t meta = b->reserved + b->num_fats * b->fat_secs + root_secs;
    if (meta >= b->total_secs) return -1;
    b->data_secs = b->total_secs - meta;
    b->clusters  = b->data_secs / b->sec_per_clus;

    /* Microsoft's canonical FAT-type rule is by cluster count. */
    if (b->clusters < 4085)       b->fat_bits = 12;
    else if (b->clusters < 65525) b->fat_bits = 16;
    else                          b->fat_bits = 32;

    b->fat_start_lba = b->reserved;
    return 0;
}

/* Walk the FAT, counting used (nonzero, non-bad) data clusters. */
static void count_fat_usage(const drive_info_t *di, int drive,
                            uint64_t vol_lba, const bpb_t *b) {
    uint32_t total = b->clusters + 2;   /* clusters 0,1 reserved */
    uint32_t used = 0, bad = 0, scanned = 0;
    uint8_t  buf[XFER_BYTES];
    uint64_t fat_lba = vol_lba + b->fat_start_lba;
    uint32_t fat_bytes = b->fat_secs * b->bytes_per_sec;
    uint32_t done = 0;
    uint8_t  carry[2]; int carry_n = 0;   /* FAT12 nibble carry across reads */

    while (done < fat_bytes && scanned < total) {
        uint32_t want = fat_bytes - done;
        int secs = (int)((want + 511) / 512);
        if (secs > XFER_SECTORS) secs = XFER_SECTORS;
        if (read_lba(di, drive, fat_lba + done / 512, secs, buf) != 0) {
            slog("  FAT read error at byte %lu\n", (unsigned long)done);
            return;
        }
        int got = secs * 512;

        if (b->fat_bits == 16) {
            for (int i = 0; i + 1 < got && scanned < total; i += 2, scanned++) {
                if (scanned < 2) continue;
                uint16_t e = rd16(buf + i);
                if (e == 0xFFF7) bad++;
                else if (e != 0) used++;
            }
        } else if (b->fat_bits == 32) {
            for (int i = 0; i + 3 < got && scanned < total; i += 4, scanned++) {
                if (scanned < 2) continue;
                uint32_t e = rd32(buf + i) & 0x0FFFFFFF;
                if (e == 0x0FFFFFF7) bad++;
                else if (e != 0) used++;
            }
        } else { /* FAT12: 3 bytes per 2 entries */
            int i = 0;
            /* handle a byte carried over from the previous block */
            (void)carry; (void)carry_n;
            for (; i + 2 < got && scanned + 1 < total; i += 3) {
                uint32_t pair = buf[i] | (buf[i+1] << 8) | (buf[i+2] << 16);
                uint16_t e0 = pair & 0xFFF;
                uint16_t e1 = (pair >> 12) & 0xFFF;
                if (scanned >= 2) { if (e0 == 0xFF7) bad++; else if (e0) used++; }
                scanned++;
                if (scanned < total) {
                    if (scanned >= 2) { if (e1 == 0xFF7) bad++; else if (e1) used++; }
                    scanned++;
                }
            }
        }
        done += got;
        if (done >= fat_bytes) break;
    }

    uint32_t free_cl = b->clusters > used + bad ? b->clusters - used - bad : 0;
    slog("  clusters: %lu total, %lu used, %lu free, %lu bad\n",
         (unsigned long)b->clusters, (unsigned long)used,
         (unsigned long)free_cl, (unsigned long)bad);
    if (b->sec_per_clus && b->bytes_per_sec) {
        double cl_kib = (double)b->sec_per_clus * b->bytes_per_sec / 1024.0;
        slog("  used %.1f KiB, free %.1f KiB (cluster = %.1f KiB)\n",
             used * cl_kib, free_cl * cl_kib, cl_kib);
    }
}

/* ----- sector-0 classification + reporting -------------------------- */

static int looks_like_fat_boot(const uint8_t *s) {
    /* jump (EB xx 90 or E9 xx xx) + valid bytes/sector + boot signature */
    int jmp = (s[0] == 0xEB && s[2] == 0x90) || s[0] == 0xE9;
    uint16_t bps = rd16(s + 11);
    int bps_ok = (bps == 512 || bps == 1024 || bps == 2048 || bps == 4096);
    return jmp && bps_ok && s[510] == 0x55 && s[511] == 0xAA;
}

static const char *part_type_name(uint8_t t) {
    switch (t) {
        case 0x00: return "empty";
        case 0x01: return "FAT12";
        case 0x04: return "FAT16 <32M";
        case 0x05: return "extended (CHS)";
        case 0x06: return "FAT16";
        case 0x07: return "NTFS/exFAT";
        case 0x0B: return "FAT32 (CHS)";
        case 0x0C: return "FAT32 (LBA)";
        case 0x0E: return "FAT16 (LBA)";
        case 0x0F: return "extended (LBA)";
        case 0x83: return "Linux";
        default:   return "other";
    }
}

static int is_fat_part_type(uint8_t t) {
    return t == 0x01 || t == 0x04 || t == 0x06 ||
           t == 0x0B || t == 0x0C || t == 0x0E;
}

/* MBR types whose volume boot sector we can parse (FAT or NTFS). */
static int is_imageable_part_type(uint8_t t) {
    return is_fat_part_type(t) || t == 0x07;   /* 0x07 = NTFS/exFAT */
}

/* ----- NTFS ($Bitmap allocation) ----------------------------------- */

typedef struct {
    unsigned bytes_per_sec;
    unsigned sec_per_clus;
    uint64_t total_secs;
    uint64_t mft_lcn;
    uint32_t mft_rec_size;
    uint64_t cluster_size;
} ntfs_bpb_t;

static int parse_ntfs_bpb(const uint8_t *s, ntfs_bpb_t *b) {
    memset(b, 0, sizeof *b);
    b->bytes_per_sec = rd16(s + 0x0B);
    b->sec_per_clus  = s[0x0D];
    if (b->bytes_per_sec == 0 || b->sec_per_clus == 0) return -1;
    b->total_secs   = rd64(s + 0x28);
    b->mft_lcn      = rd64(s + 0x30);
    b->cluster_size = (uint64_t)b->bytes_per_sec * b->sec_per_clus;
    int8_t raw = (int8_t)s[0x40];           /* clusters/MFT-record, signed */
    if (raw < 0) b->mft_rec_size = 1u << (-raw);
    else         b->mft_rec_size = (uint32_t)raw * b->cluster_size;
    if (b->mft_rec_size == 0 || b->mft_rec_size > 8192) return -1;
    return 0;
}

/* Apply the NTFS update-sequence (fixup) array in place. */
static void ntfs_fixup(uint8_t *rec, uint32_t rec_size, unsigned bps) {
    unsigned usa_off = rd16(rec + 0x04);
    unsigned usa_cnt = rd16(rec + 0x06);
    if (usa_cnt < 2 || usa_off + usa_cnt * 2 > rec_size) return;
    for (unsigned i = 1; i < usa_cnt; i++) {
        unsigned sector_end = i * bps;
        if (sector_end < 2 || sector_end > rec_size) break;
        rec[sector_end - 2] = rec[usa_off + i * 2];
        rec[sector_end - 1] = rec[usa_off + i * 2 + 1];
    }
}

/* Read MFT record `n`. The MFT's first extent is always contiguous at
 * mft_lcn and covers the metadata records 0..15, so $Bitmap (#6) is a
 * straight offset from mft_lcn -- no need to chase $MFT's own runs. */
static int ntfs_read_mft_rec(const drive_info_t *di, int drive,
                             uint64_t vol_lba, const ntfs_bpb_t *b,
                             uint32_t n, uint8_t *rec) {
    uint64_t byte = b->mft_lcn * b->cluster_size + (uint64_t)n * b->mft_rec_size;
    uint64_t lba = vol_lba + byte / 512;
    int secs = (int)(b->mft_rec_size / 512);
    if (secs < 1) secs = 1;
    int done = 0;
    while (done < secs) {
        int chunk = secs - done;
        if (chunk > XFER_SECTORS) chunk = XFER_SECTORS;
        if (read_lba(di, drive, lba + done, chunk, rec + done * 512) != 0)
            return -1;
        done += chunk;
    }
    if (memcmp(rec, "FILE", 4) != 0) return -2;
    ntfs_fixup(rec, b->mft_rec_size, b->bytes_per_sec);
    return 0;
}

/* Read $Bitmap (MFT #6) and count allocated clusters (set bit = used). */
static void ntfs_count_bitmap(const drive_info_t *di, int drive,
                              uint64_t vol_lba, const ntfs_bpb_t *b) {
    static uint8_t rec[8192];
    if (ntfs_read_mft_rec(di, drive, vol_lba, b, 6, rec) != 0) {
        slog("  could not read $Bitmap MFT record (#6)\n");
        return;
    }
    uint64_t total_clusters = b->total_secs / b->sec_per_clus;
    uint64_t bits_to_count = total_clusters;
    uint64_t allocated = 0, counted = 0;

    unsigned pos = rd16(rec + 0x14);        /* first attribute offset */
    int found = 0;
    while (pos + 16 <= b->mft_rec_size) {
        uint32_t type = rd32(rec + pos);
        if (type == 0xFFFFFFFF || type == 0) break;
        uint32_t alen = rd32(rec + pos + 4);
        if (alen < 16 || pos + alen > b->mft_rec_size) break;
        if (type == 0x80) {                 /* $DATA */
            found = 1;
            if (!rec[pos + 8]) {            /* resident: content inline */
                uint32_t clen = rd32(rec + pos + 0x10);
                unsigned coff = rd16(rec + pos + 0x14);
                for (uint32_t i = 0; i < clen && counted < bits_to_count; i++) {
                    uint8_t v = rec[pos + coff + i];
                    for (int bit = 0; bit < 8 && counted < bits_to_count; bit++) {
                        if (v & (1 << bit)) allocated++;
                        counted++;
                    }
                }
            } else {                        /* non-resident: decode runs */
                unsigned rp = pos + rd16(rec + pos + 0x20);
                int64_t lcn = 0;
                uint8_t buf[XFER_BYTES];
                while (rp < pos + alen && counted < bits_to_count) {
                    uint8_t hdr = rec[rp++];
                    if (hdr == 0) break;
                    int lsz = hdr & 0x0F, osz = (hdr >> 4) & 0x0F;
                    if (lsz == 0) break;
                    uint64_t rlen = 0;
                    for (int i = 0; i < lsz; i++)
                        rlen |= (uint64_t)rec[rp + i] << (i * 8);
                    rp += lsz;
                    if (osz == 0) {         /* sparse run = free clusters */
                        counted += rlen * b->cluster_size * 8;
                        continue;
                    }
                    int64_t off = 0;
                    for (int i = 0; i < osz; i++)
                        off |= (int64_t)rec[rp + i] << (i * 8);
                    if (osz < 8 && (rec[rp + osz - 1] & 0x80))
                        for (int i = osz; i < 8; i++)
                            off |= (int64_t)0xFF << (i * 8);
                    rp += osz;
                    lcn += off;

                    uint64_t run_lba = vol_lba + (uint64_t)lcn * b->sec_per_clus;
                    uint64_t run_secs = rlen * b->sec_per_clus, ds = 0;
                    while (ds < run_secs && counted < bits_to_count) {
                        int chunk = (int)(run_secs - ds);
                        if (chunk > XFER_SECTORS) chunk = XFER_SECTORS;
                        if (read_lba(di, drive, run_lba + ds, chunk, buf) != 0) {
                            slog("  $Bitmap read error\n");
                            return;
                        }
                        int got = chunk * 512;
                        for (int byi = 0; byi < got && counted < bits_to_count; byi++) {
                            uint8_t v = buf[byi];
                            for (int bit = 0; bit < 8 && counted < bits_to_count; bit++) {
                                if (v & (1 << bit)) allocated++;
                                counted++;
                            }
                        }
                        ds += chunk;
                    }
                }
            }
            break;
        }
        pos += alen;
    }
    if (!found) { slog("  $Bitmap: no $DATA attribute found\n"); return; }

    uint64_t freecl = total_clusters > allocated ? total_clusters - allocated : 0;
    double cl_kib = b->cluster_size / 1024.0;
    slog("  clusters: %lu total, %lu allocated, %lu free\n",
         (unsigned long)total_clusters, (unsigned long)allocated,
         (unsigned long)freecl);
    slog("  allocated %.1f MiB, free %.1f MiB (cluster = %.1f KiB)\n",
         allocated * cl_kib / 1024.0, freecl * cl_kib / 1024.0, cl_kib);
}

static void report_ntfs(const drive_info_t *di, int drive, uint64_t vol_lba,
                        const uint8_t *vbr) {
    ntfs_bpb_t b;
    if (parse_ntfs_bpb(vbr, &b) != 0) {
        slog("  NTFS BPB failed sanity checks\n");
        return;
    }
    slog("  NTFS  OEM '%.8s'  bytes/sec %u  sec/clus %u  cluster %lu B\n",
         vbr + 3, b.bytes_per_sec, b.sec_per_clus,
         (unsigned long)b.cluster_size);
    slog("  total %lu sec (%.1f MiB)  MFT @ LCN %lu  MFT-rec %lu B\n",
         (unsigned long)b.total_secs,
         b.total_secs * (double)b.bytes_per_sec / (1024.0 * 1024.0),
         (unsigned long)b.mft_lcn, (unsigned long)b.mft_rec_size);
    ntfs_count_bitmap(di, drive, vol_lba, &b);
}

/* Read the volume boot sector at `vol_lba`, parse + report its BPB. */
static void report_volume(const drive_info_t *di, int drive, uint64_t vol_lba) {
    uint8_t sec[512];
    if (read_lba(di, drive, vol_lba, 1, sec) != 0) {
        slog("  could not read volume boot sector at LBA %lu\n",
             (unsigned long)vol_lba);
        return;
    }
    if (memcmp(sec + 3, "NTFS    ", 8) == 0) {
        report_ntfs(di, drive, vol_lba, sec);
        return;
    }
    if (memcmp(sec + 3, "EXFAT   ", 8) == 0) {
        slog("  exFAT volume (OEM 'EXFAT') -- not parsed by this spike\n");
        return;
    }
    if (!looks_like_fat_boot(sec)) {
        slog("  LBA %lu is not a FAT/NTFS boot sector (OEM '%.8s')\n",
             (unsigned long)vol_lba, sec + 3);
        return;
    }
    bpb_t b;
    if (parse_bpb(sec, &b) != 0) {
        slog("  BPB at LBA %lu failed sanity checks\n", (unsigned long)vol_lba);
        return;
    }
    slog("  FAT%d  OEM '%.8s'  label '%.11s'\n", b.fat_bits, sec + 3,
         sec + (b.fat_bits == 32 ? 71 : 43));
    slog("  bytes/sec %u  sec/clus %u  reserved %u  FATs %u  root-ents %u\n",
         b.bytes_per_sec, b.sec_per_clus, b.reserved, b.num_fats,
         b.root_entries);
    slog("  total %lu sec (%.1f MiB)  FAT %lu sec  data %lu sec\n",
         (unsigned long)b.total_secs,
         b.total_secs * (double)b.bytes_per_sec / (1024.0 * 1024.0),
         (unsigned long)b.fat_secs, (unsigned long)b.data_secs);
    count_fat_usage(di, drive, vol_lba, &b);
}

static void report_drive(int drive) {
    drive_info_t di;
    drive_params(drive, &di);
    if (!di.present) {
        slog("drive 0x%02X: not present\n", drive);
        return;
    }
    di.ext = drive_has_ext(drive);
    slog("drive 0x%02X: present, CHS %u/%u/%u, LBA ext %s\n",
         drive, di.cyls, di.heads, di.spt, di.ext ? "yes" : "no");

    uint8_t s0[512];
    if (read_lba(&di, drive, 0, 1, s0) != 0) {
        slog("  could not read LBA 0\n");
        return;
    }
    if (s0[510] != 0x55 || s0[511] != 0xAA) {
        slog("  LBA 0 has no 55AA signature -- unknown\n");
        return;
    }

    if (looks_like_fat_boot(s0)) {
        slog("  LBA 0 is a FAT boot sector -- superfloppy (no partition table)\n");
        report_volume(&di, drive, 0);
        return;
    }

    /* Treat as MBR: 4 entries at offset 446, 16 bytes each. */
    slog("  LBA 0 is an MBR. Partition table:\n");
    uint64_t first_vol_lba = 0;
    for (int i = 0; i < 4; i++) {
        const uint8_t *e = s0 + 446 + i * 16;
        uint8_t boot = e[0];
        uint8_t type = e[4];
        uint32_t start = rd32(e + 8);
        uint32_t size = rd32(e + 12);
        if (type == 0 && start == 0 && size == 0) {
            slog("    [%d] empty\n", i + 1);
            continue;
        }
        slog("    [%d] %s type 0x%02X (%s) start LBA %lu size %lu sec (%.1f MiB)\n",
             i + 1, boot == 0x80 ? "active" : "      ", type,
             part_type_name(type), (unsigned long)start, (unsigned long)size,
             size * (double)512 / (1024.0 * 1024.0));
        if (is_imageable_part_type(type) && first_vol_lba == 0)
            first_vol_lba = start;
    }
    if (first_vol_lba) {
        slog("  parsing first FAT/NTFS partition at LBA %lu:\n",
             (unsigned long)first_vol_lba);
        report_volume(&di, drive, first_vol_lba);
    } else {
        slog("  no FAT/NTFS partition found to parse\n");
    }
}

int main(int argc, char **argv) {
    g_log = fopen("SPIKE.LOG", "w");

    slog("cb-dos disk spike (Phase 0b)\n");
    slog("============================\n");

    if (xfer_init() < 0) {
        slog("FATAL: could not allocate DOS transfer buffer\n");
        if (g_log) fclose(g_log);
        return 1;
    }

    if (argc > 1) {
        int drive = (int)strtol(argv[1], NULL, 16);
        report_drive(drive);
    } else {
        /* enumerate from 0x80 upward until one is absent */
        int any = 0;
        for (int drive = 0x80; drive <= 0x87; drive++) {
            drive_info_t di;
            drive_params(drive, &di);
            if (!di.present) break;
            any = 1;
            report_drive(drive);
            slog("\n");
        }
        if (!any)
            slog("no BIOS hard drives found (0x80+)\n");
    }

    xfer_free();
    slog("done.\n");
    if (g_log) fclose(g_log);
    return 0;
}
