/* cbclone.c -- cb-dos direct disk-to-disk clone (Phase 4b, docs/cb_dos.md S2e).
 *
 * Clones a FAT disk straight onto a second disk on the vintage machine itself,
 * with no intermediate .gz backup folder: read source (int 13h) -> smart-compact
 * each FAT partition on the fly (zero the free clusters) -> write directly to the
 * target, optionally resizing the filesystem to a different-sized disk. The
 * target *is* the destination, so this sidesteps the "where do I stage the
 * backup?" problem entirely -- boot from a floppy/Gotek, source on one IDE
 * channel, target on the other, clone live.
 *
 * It is exactly cbbackup's read+compaction engine fused with cbrestore's
 * write+resize engine, minus the gzip/file layer. The low-level primitives
 * (xfer buffer, int13h r/w, FAT parse, the resize) are the proven copies from
 * those two tools, kept inline here per the project's "lift into cbdisk.{h,c}
 * later" note (now that three tools share them, that refactor is well-motivated).
 *
 * Build:  make clone        (-> build/cbclone.exe; no zlib needed)
 * Run:    CBCLONE <src-hex> <tgt-hex> /Y [/SIZE:mode] [/CUSTOM:bytes] [/PARTS:i,j]
 *         e.g.  CBCLONE 80 81 /Y                 clone disk 0x80 -> 0x81, same size
 *               CBCLONE 80 81 /Y /SIZE:ENTIRE    grow to fill the target disk
 *               CBCLONE 80 81 /Y /SIZE:MINIMUM   shrink each FAT to its used data
 *               CBCLONE 80 81 /Y /PARTS:0        clone only MBR slot 0
 *
 * Source is read-only; /Y gates the destructive write to the target. Refuses to
 * clone a disk onto itself. MBR primary FAT12/16/32 with 512-byte sectors;
 * NTFS/exFAT/logical partitions are skipped (handled on the desktop).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <dpmi.h>
#include <go32.h>
#include <sys/movedata.h>

/* ----- DOS-memory transfer buffer ----------------------------------- */

#define XFER_SECTORS 16
#define XFER_BYTES   (XFER_SECTORS * 512)
static int g_buf_seg, g_buf_sel;

static int xfer_init(void) {
    /* +16 for the int13h Disk Address Packet read_lba/write_lba place at offset
     * XFER_BYTES -- without it the DAP overruns the next MCB and DOS hangs at
     * program exit under CWSDPMI (see cbbackup.c for the full story). */
    int para = (XFER_BYTES + 16 + 15) >> 4;
    g_buf_seg = __dpmi_allocate_dos_memory(para, &g_buf_sel);
    return g_buf_seg;
}
static void xfer_free(void) {
    if (g_buf_seg > 0) __dpmi_free_dos_memory(g_buf_sel);
}

/* ----- little-endian helpers ---------------------------------------- */

static uint16_t rd16(const uint8_t *p) { return p[0] | (p[1] << 8); }
static uint32_t rd32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
static void wr16(uint8_t *p, uint16_t v) { p[0] = v & 0xFF; p[1] = (v >> 8) & 0xFF; }
static void wr32(uint8_t *p, uint32_t v) {
    p[0] = v & 0xFF; p[1] = (v >> 8) & 0xFF;
    p[2] = (v >> 16) & 0xFF; p[3] = (v >> 24) & 0xFF;
}

/* ----- drive geometry + sector read/write --------------------------- */

typedef struct { int present, ext; unsigned cyls, heads, spt; } drive_info_t;

static void drive_params(int drive, drive_info_t *di) {
    __dpmi_regs r;
    memset(di, 0, sizeof *di);
    memset(&r, 0, sizeof r);
    r.h.ah = 0x08; r.h.dl = drive; r.x.es = 0; r.x.di = 0;
    __dpmi_int(0x13, &r);
    if (r.x.flags & 1) return;
    di->present = 1;
    di->heads = (unsigned)r.h.dh + 1;
    di->spt   = r.h.cl & 0x3F;
    di->cyls  = (((unsigned)(r.h.cl & 0xC0) << 2) | r.h.ch) + 1;
}
static int drive_has_ext(int drive) {
    __dpmi_regs r;
    memset(&r, 0, sizeof r);
    r.h.ah = 0x41; r.x.bx = 0x55AA; r.h.dl = drive;
    __dpmi_int(0x13, &r);
    if (r.x.flags & 1) return 0;
    if (r.x.bx != 0xAA55) return 0;
    return (r.x.cx & 0x0001) != 0;
}

/* Total addressable sectors. AH=48h (EDD) for LBA drives, CHS product otherwise. */
static uint64_t drive_total_sectors(const drive_info_t *di, int drive) {
    if (di->ext) {
        __dpmi_regs r;
        uint8_t edd[32];
        memset(edd, 0, sizeof edd);
        wr16(edd, 0x1E);
        dosmemput(edd, sizeof edd, g_buf_seg * 16);
        memset(&r, 0, sizeof r);
        r.h.ah = 0x48; r.h.dl = drive;
        r.x.ds = g_buf_seg; r.x.si = 0;
        __dpmi_int(0x13, &r);
        if (!(r.x.flags & 1)) {
            dosmemget(g_buf_seg * 16, sizeof edd, edd);
            uint64_t total = (uint64_t)rd32(edd + 16) | ((uint64_t)rd32(edd + 20) << 32);
            if (total > 0) return total;
        }
    }
    return (uint64_t)di->cyls * di->heads * di->spt;
}

static int chs_regs(const drive_info_t *di, uint64_t lba, int count, __dpmi_regs *r) {
    unsigned spt = di->spt ? di->spt : 63;
    unsigned heads = di->heads ? di->heads : 16;
    unsigned cyl = (unsigned)(lba / (spt * heads));
    unsigned tmp = (unsigned)(lba % (spt * heads));
    unsigned head = tmp / spt;
    unsigned sect = tmp % spt + 1;
    if (cyl > 1023) return -1;
    r->h.al = count;
    r->h.ch = cyl & 0xFF;
    r->h.cl = (uint8_t)((sect & 0x3F) | ((cyl >> 2) & 0xC0));
    r->h.dh = head;
    return 0;
}

static int read_lba(const drive_info_t *di, int drive, uint64_t lba, int count, void *dst) {
    __dpmi_regs r;
    int err;
    if (count < 1 || count > XFER_SECTORS) return 0xFF;
    memset(&r, 0, sizeof r);
    if (di->ext) {
        uint8_t dap[16];
        memset(dap, 0, sizeof dap);
        dap[0] = 0x10; dap[2] = count & 0xFF; dap[3] = (count >> 8) & 0xFF;
        dap[6] = g_buf_seg & 0xFF; dap[7] = (g_buf_seg >> 8) & 0xFF;
        for (int i = 0; i < 8; i++) dap[8 + i] = (uint8_t)(lba >> (8 * i));
        dosmemput(dap, sizeof dap, g_buf_seg * 16 + XFER_BYTES);
        r.h.ah = 0x42; r.h.dl = drive;
        r.x.ds = g_buf_seg + (XFER_BYTES >> 4); r.x.si = 0;
        __dpmi_int(0x13, &r);
        err = (r.x.flags & 1) ? r.h.ah : 0;
    } else {
        if (chs_regs(di, lba, count, &r) != 0) return 0xFE;
        r.h.ah = 0x02; r.h.dl = drive; r.x.es = g_buf_seg; r.x.bx = 0;
        __dpmi_int(0x13, &r);
        err = (r.x.flags & 1) ? r.h.ah : 0;
    }
    if (err == 0) dosmemget(g_buf_seg * 16, count * 512, dst);
    return err;
}

static int write_lba(const drive_info_t *di, int drive, uint64_t lba, int count, const void *src) {
    __dpmi_regs r;
    if (count < 1 || count > XFER_SECTORS) return 0xFF;
    dosmemput(src, count * 512, g_buf_seg * 16);
    memset(&r, 0, sizeof r);
    if (di->ext) {
        uint8_t dap[16];
        memset(dap, 0, sizeof dap);
        dap[0] = 0x10; dap[2] = count & 0xFF; dap[3] = (count >> 8) & 0xFF;
        dap[6] = g_buf_seg & 0xFF; dap[7] = (g_buf_seg >> 8) & 0xFF;
        for (int i = 0; i < 8; i++) dap[8 + i] = (uint8_t)(lba >> (8 * i));
        dosmemput(dap, sizeof dap, g_buf_seg * 16 + XFER_BYTES);
        r.h.ah = 0x43; r.h.al = 0x00;
        r.h.dl = drive;
        r.x.ds = g_buf_seg + (XFER_BYTES >> 4); r.x.si = 0;
        __dpmi_int(0x13, &r);
        return (r.x.flags & 1) ? r.h.ah : 0;
    } else {
        if (chs_regs(di, lba, count, &r) != 0) return 0xFE;
        r.h.ah = 0x03; r.h.dl = drive; r.x.es = g_buf_seg; r.x.bx = 0;
        __dpmi_int(0x13, &r);
        return (r.x.flags & 1) ? r.h.ah : 0;
    }
}

static int load_region(const drive_info_t *di, int drive, uint64_t lba,
                       uint32_t bytes, uint8_t *dst) {
    uint32_t done = 0;
    while (done < bytes) {
        uint32_t want = bytes - done;
        int secs = (int)((want + 511) / 512);
        if (secs > XFER_SECTORS) secs = XFER_SECTORS;
        if (read_lba(di, drive, lba + done / 512, secs, dst + done) != 0) return -1;
        done += (uint32_t)secs * 512;
    }
    return 0;
}
static int store_region(const drive_info_t *di, int drive, uint64_t lba,
                        uint32_t bytes, const uint8_t *src) {
    uint32_t done = 0;
    while (done < bytes) {
        uint32_t want = bytes - done;
        int secs = (int)((want + 511) / 512);
        if (secs > XFER_SECTORS) secs = XFER_SECTORS;
        if (write_lba(di, drive, lba + done / 512, secs, src + done) != 0) return -1;
        done += (uint32_t)secs * 512;
    }
    return 0;
}

/* ----- FAT layout + entries ----------------------------------------- */

typedef struct {
    int      ok;
    unsigned bps, spc, reserved, num_fats, root_entries;
    uint32_t old_spf, old_total, root_dir_secs;
    int      is_fat32, fat_bits;
} fatlay_t;

static void parse_fatlay(const uint8_t *bpb, fatlay_t *L) {
    memset(L, 0, sizeof *L);
    if (bpb[0] != 0xEB && bpb[0] != 0xE9) return;
    L->bps = rd16(bpb + 11);
    if (L->bps != 512 && L->bps != 1024 && L->bps != 2048 && L->bps != 4096) return;
    L->spc = bpb[13];
    if (L->spc == 0 || (L->spc & (L->spc - 1)) != 0) return;
    L->reserved = rd16(bpb + 14);
    L->num_fats = bpb[16];
    if (L->num_fats == 0 || L->num_fats > 2) return;
    L->root_entries = rd16(bpb + 17);
    uint32_t ts16 = rd16(bpb + 19), spf16 = rd16(bpb + 22);
    uint32_t ts32 = rd32(bpb + 32), spf32 = rd32(bpb + 36);
    L->is_fat32 = (spf16 == 0 && L->root_entries == 0);
    L->old_spf = L->is_fat32 ? spf32 : spf16;
    L->old_total = ts16 ? ts16 : ts32;
    if (L->old_spf == 0 || L->old_total == 0) return;
    L->root_dir_secs = L->is_fat32 ? 0 : ((L->root_entries * 32 + L->bps - 1) / L->bps);
    uint32_t data_start = L->reserved + L->num_fats * L->old_spf + L->root_dir_secs;
    uint32_t data_secs = (L->old_total > data_start) ? L->old_total - data_start : 0;
    uint32_t clusters = data_secs / L->spc;
    if (L->is_fat32)          L->fat_bits = 32;
    else if (clusters < 4085) L->fat_bits = 12;
    else                      L->fat_bits = 16;
    L->ok = 1;
}

/* FAT entry value for cluster n (0 == free). */
static uint32_t fat_entry(const uint8_t *fat, int bits, uint32_t n) {
    if (bits == 16) return rd16(fat + n * 2);
    if (bits == 32) return rd32(fat + n * 4) & 0x0FFFFFFF;
    uint32_t off = n + (n / 2);
    uint32_t pair = fat[off] | (fat[off + 1] << 8);
    return (n & 1) ? (pair >> 4) : (pair & 0xFFF);
}

static int is_fat_part_type(uint8_t t) {
    return t == 0x01 || t == 0x04 || t == 0x06 ||
           t == 0x0B || t == 0x0C || t == 0x0E;
}

/* ----- FAT resize (the C port of resize_fat_in_place, src/fs/fat.rs) - */

static uint32_t compute_fat_sectors(uint32_t total, uint32_t reserved, uint32_t num_fats,
                                    uint32_t root_dir_secs, uint32_t spc,
                                    int fat_bits, unsigned bps) {
    uint64_t avail = (total > reserved + root_dir_secs)
        ? (uint64_t)(total - reserved - root_dir_secs) : 0;
    uint64_t b = bps, s = spc, n = num_fats;
    if (fat_bits == 12) {
        uint32_t spf = 1;
        for (;;) {
            uint64_t data = (avail > n * spf) ? avail - n * spf : 0;
            uint64_t clusters = data / s;
            uint64_t fat_bytes = ((clusters + 2) * 3 + 1) / 2;
            uint32_t needed = (uint32_t)((fat_bytes + b - 1) / b);
            if (needed <= spf) return spf;
            spf = needed;
        }
    } else if (fat_bits == 16) {
        uint64_t num = 2 * (avail + 2 * s);
        uint64_t den = b * s + 2 * n;
        return (uint32_t)((num + den - 1) / den);
    } else {
        uint64_t num = 4 * (avail + 2 * s);
        uint64_t den = b * s + 4 * n;
        return (uint32_t)((num + den - 1) / den);
    }
}

static int shift_region_forward(const drive_info_t *di, int drive, uint64_t part_lba,
                                uint32_t src_start, uint32_t src_end, uint32_t shift) {
    uint8_t buf[XFER_BYTES];
    if (shift == 0) return 0;
    uint32_t remaining = (src_end > src_start) ? src_end - src_start : 0;
    while (remaining > 0) {
        uint32_t chunk = remaining > XFER_SECTORS ? XFER_SECTORS : remaining;
        uint32_t pos = src_start + remaining - chunk;
        if (read_lba(di, drive, part_lba + pos, chunk, buf) != 0) return -1;
        if (write_lba(di, drive, part_lba + pos + shift, chunk, buf) != 0) return -1;
        remaining -= chunk;
    }
    memset(buf, 0, XFER_BYTES);
    uint32_t gap = shift, pos = src_start;
    while (gap > 0) {
        uint32_t chunk = gap > XFER_SECTORS ? XFER_SECTORS : gap;
        if (write_lba(di, drive, part_lba + pos, chunk, buf) != 0) return -1;
        pos += chunk; gap -= chunk;
    }
    return 0;
}

static int shift_region_backward(const drive_info_t *di, int drive, uint64_t part_lba,
                                 uint32_t src_start, uint32_t src_end, uint32_t shift) {
    uint8_t buf[XFER_BYTES];
    if (shift == 0 || src_end <= src_start) return 0;
    uint32_t pos = src_start;
    while (pos < src_end) {
        uint32_t chunk = src_end - pos;
        if (chunk > XFER_SECTORS) chunk = XFER_SECTORS;
        if (read_lba(di, drive, part_lba + pos, chunk, buf) != 0) return -1;
        if (write_lba(di, drive, part_lba + pos - shift, chunk, buf) != 0) return -1;
        pos += chunk;
    }
    return 0;
}

static uint32_t fat_max_clusters(int fat_bits) {
    if (fat_bits == 12) return 4084;
    if (fat_bits == 16) return 65524;
    return 0x0FFFFFF5u;
}
static uint64_t max_fat_window(const fatlay_t *L) {
    uint64_t maxc = fat_max_clusters(L->fat_bits);
    uint64_t spf;
    if (L->fat_bits == 12) spf = (((maxc + 2) * 3 + 1) / 2 + L->bps - 1) / L->bps;
    else { unsigned bpe = (L->fat_bits == 16) ? 2 : 4; spf = ((maxc + 2) * bpe + L->bps - 1) / L->bps; }
    return (uint64_t)L->reserved + (uint64_t)L->num_fats * spf + L->root_dir_secs + maxc * L->spc;
}
static uint64_t min_fat_window(const fatlay_t *L) {
    uint64_t minc = (L->fat_bits == 16) ? 4085 : (L->fat_bits == 32) ? 65525 : 1;
    uint64_t spf;
    if (L->fat_bits == 12) spf = (((minc + 2) * 3 + 1) / 2 + L->bps - 1) / L->bps;
    else { unsigned bpe = (L->fat_bits == 16) ? 2 : 4; spf = ((minc + 2) * bpe + L->bps - 1) / L->bps; }
    return (uint64_t)L->reserved + (uint64_t)L->num_fats * spf + L->root_dir_secs + minc * L->spc;
}

static void patch_bpb_total(uint8_t *bpb, uint32_t new_total, uint32_t old_ts16) {
    if (old_ts16 != 0 && new_total <= 0xFFFF) {
        wr16(bpb + 19, (uint16_t)new_total);
        wr32(bpb + 32, 0);
    } else {
        wr16(bpb + 19, 0);
        wr32(bpb + 32, new_total);
    }
}

static int write_bpb(const drive_info_t *di, int drive, uint64_t part_lba,
                     const uint8_t *bpb, int is_fat32, unsigned bps) {
    if (write_lba(di, drive, part_lba, 1, bpb) != 0) return -1;
    if (is_fat32) {
        uint32_t ratio = bps / 512; if (ratio == 0) ratio = 1;
        if (write_lba(di, drive, part_lba + 6 * ratio, 1, bpb) != 0) return -1;
    }
    return 0;
}

static void reset_fsinfo(const drive_info_t *di, int drive, uint64_t part_lba,
                         const uint8_t *bpb, unsigned bps) {
    uint32_t ratio = bps / 512; if (ratio == 0) ratio = 1;
    unsigned fsinfo_sec = rd16(bpb + 48);
    if (fsinfo_sec == 0 || fsinfo_sec == 0xFFFF) return;
    uint64_t fsinfo_lba = part_lba + (uint64_t)fsinfo_sec * ratio;
    uint8_t sec[512];
    if (read_lba(di, drive, fsinfo_lba, 1, sec) != 0) return;
    if (rd32(sec) != 0x41615252) return;
    if (rd32(sec + 484) != 0x61417272) return;
    wr32(sec + 488, 0xFFFFFFFF);
    wr32(sec + 492, 0xFFFFFFFF);
    write_lba(di, drive, fsinfo_lba, 1, sec);
}

/* Resize the FAT at `part_lba` (on `drive`) to `new_total` sectors in place.
 * `imaged_secs` is the end of meaningful data (everything past it is zero).
 * Returns 1 if changed, 0 if no-op/non-FAT, -1 on error. 512-byte sectors. */
static int fat_resize(const drive_info_t *di, int drive, uint64_t part_lba,
                      uint32_t new_total, uint32_t imaged_secs) {
    uint8_t bpb[512];
    if (read_lba(di, drive, part_lba, 1, bpb) != 0) return -1;
    fatlay_t L;
    parse_fatlay(bpb, &L);
    if (!L.ok || L.bps != 512) return 0;
    if (L.old_total == new_total) return 0;

    uint32_t ts16 = rd16(bpb + 19);
    uint64_t cap = max_fat_window(&L);
    if (!L.is_fat32 && new_total > cap) {
        printf("  resize: FAT%d cluster limit -- capped %lu -> %lu sectors\n",
               L.fat_bits, (unsigned long)new_total, (unsigned long)cap);
        new_total = (uint32_t)cap;
        if (new_total == L.old_total) return 0;
    }

    uint32_t new_spf = compute_fat_sectors(new_total, L.reserved, L.num_fats,
                                           L.root_dir_secs, L.spc, L.fat_bits, L.bps);
    uint32_t new_data_start = L.reserved + L.num_fats * new_spf + L.root_dir_secs;
    uint32_t new_data_secs = (new_total > new_data_start) ? new_total - new_data_start : 0;
    uint32_t new_clusters = new_data_secs / L.spc;
    int new_fat_bits = L.is_fat32 ? 32 : (new_clusters < 4085 ? 12 : 16);

    if (new_fat_bits != L.fat_bits) {
        printf("  resize: FAT%d->FAT%d boundary -- BPB total only\n", L.fat_bits, new_fat_bits);
        patch_bpb_total(bpb, new_total, ts16);
        return (write_bpb(di, drive, part_lba, bpb, L.is_fat32, L.bps) == 0) ? 1 : -1;
    }

    printf("  FAT%d resize: clusters -> %lu, spf %lu -> %lu, total %lu -> %lu\n",
           L.fat_bits, (unsigned long)new_clusters,
           (unsigned long)L.old_spf, (unsigned long)new_spf,
           (unsigned long)L.old_total, (unsigned long)new_total);

    uint32_t move_start = L.reserved + L.num_fats * L.old_spf;
    uint32_t move_end = imaged_secs;
    if (move_end < move_start) move_end = move_start;
    if (move_end > L.old_total) move_end = L.old_total;
    uint32_t new_fat_bytes = new_spf * L.bps;

    if (new_spf > L.old_spf) {
        uint32_t shift = (new_spf - L.old_spf) * L.num_fats;
        uint8_t *fat = malloc(new_fat_bytes);
        if (!fat) { printf("  resize: out of memory\n"); return -1; }
        memset(fat, 0, new_fat_bytes);
        if (load_region(di, drive, part_lba + L.reserved, L.old_spf * L.bps, fat) != 0) {
            free(fat); printf("  resize: FAT read failed\n"); return -1;
        }
        if (shift_region_forward(di, drive, part_lba, move_start, move_end, shift) != 0) {
            free(fat); printf("  resize: data shift failed\n"); return -1;
        }
        for (uint32_t c = 0; c < L.num_fats; c++)
            if (store_region(di, drive, part_lba + L.reserved + (uint64_t)c * new_spf,
                             new_fat_bytes, fat) != 0) {
                free(fat); printf("  resize: FAT write failed\n"); return -1;
            }
        free(fat);
    } else if (new_spf < L.old_spf) {
        uint32_t shift = (L.old_spf - new_spf) * L.num_fats;
        uint8_t *fat = malloc(L.old_spf * L.bps);
        if (!fat) { printf("  resize: out of memory\n"); return -1; }
        if (load_region(di, drive, part_lba + L.reserved, L.old_spf * L.bps, fat) != 0) {
            free(fat); printf("  resize: FAT read failed\n"); return -1;
        }
        for (uint32_t c = 0; c < L.num_fats; c++)
            if (store_region(di, drive, part_lba + L.reserved + (uint64_t)c * new_spf,
                             new_fat_bytes, fat) != 0) {
                free(fat); printf("  resize: FAT write failed\n"); return -1;
            }
        free(fat);
        if (shift_region_backward(di, drive, part_lba, move_start, move_end, shift) != 0) {
            printf("  resize: data shift failed\n"); return -1;
        }
    }

    patch_bpb_total(bpb, new_total, ts16);
    if (new_spf != L.old_spf) {
        if (L.is_fat32) wr32(bpb + 36, new_spf);
        else            wr16(bpb + 22, (uint16_t)new_spf);
    }
    if (write_bpb(di, drive, part_lba, bpb, L.is_fat32, L.bps) != 0) return -1;
    if (L.is_fat32) reset_fsinfo(di, drive, part_lba, bpb, L.bps);
    return 1;
}

static void set_clean_flags(const drive_info_t *di, int drive, uint64_t part_lba) {
    uint8_t bpb[512];
    if (read_lba(di, drive, part_lba, 1, bpb) != 0) return;
    fatlay_t L;
    parse_fatlay(bpb, &L);
    if (!L.ok || L.bps != 512 || L.fat_bits == 12) return;
    uint32_t spf = L.is_fat32 ? rd32(bpb + 36) : rd16(bpb + 22);
    uint64_t fat_start = part_lba + L.reserved;
    for (uint32_t c = 0; c < L.num_fats; c++) {
        uint64_t copy = fat_start + (uint64_t)c * spf;
        uint8_t sec[512];
        if (read_lba(di, drive, copy, 1, sec) != 0) continue;
        if (L.fat_bits == 16) { uint16_t v = rd16(sec + 2); v |= 0xC000; wr16(sec + 2, v); }
        else                  { uint32_t v = rd32(sec + 4); v |= 0x0C000000u; wr32(sec + 4, v); }
        write_lba(di, drive, copy, 1, sec);
    }
}

/* ----- clone one partition (source -> target, compacted) ------------ */

/* Copy the source partition [start, imaged_secs) to the target at the same
 * start_lba, zeroing free clusters in flight (the smart-compaction), then
 * zero-pad the target out to `window_bytes`. The source is never modified. */
static int clone_partition(const drive_info_t *sdi, int sdrive,
                           const drive_info_t *tdi, int tdrive,
                           uint64_t start_lba, uint64_t window_bytes,
                           const fatlay_t *L, const uint8_t *fat, uint32_t imaged_secs) {
    uint32_t first_data = L->reserved + L->num_fats * L->old_spf + L->root_dir_secs;
    uint32_t clusters = (L->old_total > first_data) ? (L->old_total - first_data) / L->spc : 0;
    uint8_t buf[XFER_BYTES];
    uint32_t s = 0;
    while (s < imaged_secs) {
        int n = (int)(imaged_secs - s);
        if (n > XFER_SECTORS) n = XFER_SECTORS;
        if (read_lba(sdi, sdrive, start_lba + s, n, buf) != 0) {
            printf("  clone: source read error at sector %lu\n", (unsigned long)s);
            return -1;
        }
        for (int j = 0; j < n; j++) {
            uint32_t rel = s + j;
            if (rel >= first_data) {
                uint32_t cl = 2 + (rel - first_data) / L->spc;
                if (cl < clusters + 2 && fat_entry(fat, L->fat_bits, cl) == 0)
                    memset(buf + j * 512, 0, 512);   /* free cluster -> zero */
            }
        }
        if (write_lba(tdi, tdrive, start_lba + s, n, buf) != 0) {
            printf("  clone: target write error at sector %lu\n", (unsigned long)s);
            return -1;
        }
        s += n;
    }
    memset(buf, 0, XFER_BYTES);
    uint64_t written = (uint64_t)imaged_secs * 512;
    while (written < window_bytes) {
        uint64_t left = window_bytes - written;
        int secs = (left / 512 > XFER_SECTORS) ? XFER_SECTORS : (int)(left / 512);
        if (secs < 1) break;
        if (write_lba(tdi, tdrive, start_lba + written / 512, secs, buf) != 0) break;
        written += (uint64_t)secs * 512;
    }
    return 0;
}

/* ----- arg helpers -------------------------------------------------- */

enum { SZ_ORIGINAL, SZ_MINIMUM, SZ_ENTIRE, SZ_CUSTOM };

static const char *switch_val(const char *arg, const char *prefix) {
    int i;
    for (i = 0; prefix[i]; i++) {
        char a = arg[i], b = prefix[i];
        if (a >= 'a' && a <= 'z') a -= 32;
        if (b >= 'a' && b <= 'z') b -= 32;
        if (a != b) return NULL;
    }
    return arg + i;
}
static int eq_ci(const char *a, const char *b) {
    for (; *a && *b; a++, b++) {
        char x = *a, y = *b;
        if (x >= 'a' && x <= 'z') x -= 32;
        if (y >= 'a' && y <= 'z') y -= 32;
        if (x != y) return 0;
    }
    return *a == 0 && *b == 0;
}
static uint64_t round_up_512(uint64_t v) { return (v + 511) & ~(uint64_t)511; }

static int parse_parts(const char *v, unsigned *mask) {
    unsigned m = 0; int any = 0;
    while (*v) {
        while (*v == ',' || *v == ' ') v++;
        if (!*v) break;
        if (*v < '0' || *v > '9') return -1;
        int n = 0;
        while (*v >= '0' && *v <= '9') { n = n * 10 + (*v - '0'); v++; }
        if (n >= 0 && n < 32) { m |= (1u << n); any = 1; }
    }
    *mask = m;
    return any ? 0 : -1;
}

/* ----- main --------------------------------------------------------- */

int main(int argc, char **argv) {
    setvbuf(stdout, NULL, _IONBF, 0);
    if (argc < 3) {
        printf("cb-dos clone (Phase 4b) -- direct disk-to-disk, no staging file\n");
        printf("usage: CBCLONE <src-hex> <tgt-hex> /Y [/SIZE:mode] [/CUSTOM:bytes] [/PARTS:i,j]\n");
        printf("  /Y               confirms the destructive write to the target drive\n");
        printf("  /SIZE:ORIGINAL   clone at the source sizes (default; free clusters zeroed)\n");
        printf("  /SIZE:MINIMUM    shrink each FAT partition to its used data\n");
        printf("  /SIZE:ENTIRE     grow each FAT partition to fill the target disk\n");
        printf("  /SIZE:CUSTOM     resize to /CUSTOM:<bytes>\n");
        printf("  /PARTS:i,j       clone only these MBR slot indices\n");
        return 2;
    }
    int sdrive = (int)strtol(argv[1], NULL, 16);
    int tdrive = (int)strtol(argv[2], NULL, 16);
    int confirmed = 0, mode = SZ_ORIGINAL;
    uint64_t custom_bytes = 0;
    unsigned sel_mask = 0; int has_filter = 0;
    for (int i = 3; i < argc; i++) {
        const char *v;
        if (eq_ci(argv[i], "/Y")) confirmed = 1;
        else if ((v = switch_val(argv[i], "/SIZE:")) != NULL) {
            if (eq_ci(v, "ORIGINAL"))      mode = SZ_ORIGINAL;
            else if (eq_ci(v, "MINIMUM"))  mode = SZ_MINIMUM;
            else if (eq_ci(v, "ENTIRE"))   mode = SZ_ENTIRE;
            else if (eq_ci(v, "CUSTOM"))   mode = SZ_CUSTOM;
            else { printf("unknown /SIZE:%s\n", v); return 2; }
        }
        else if ((v = switch_val(argv[i], "/CUSTOM:")) != NULL) {
            custom_bytes = strtoul(v, NULL, 10);
            if (mode == SZ_ORIGINAL) mode = SZ_CUSTOM;
        }
        else if ((v = switch_val(argv[i], "/PARTS:")) != NULL) {
            if (parse_parts(v, &sel_mask) != 0) { printf("bad /PARTS list\n"); return 2; }
            has_filter = 1;
        }
    }
    if (sdrive == tdrive) { printf("source and target are the same drive (0x%02X)\n", sdrive); return 1; }
    if (mode == SZ_CUSTOM && custom_bytes == 0) { printf("/SIZE:CUSTOM needs /CUSTOM:<bytes>\n"); return 2; }

    if (xfer_init() < 0) { printf("DOS memory alloc failed\n"); return 1; }
    drive_info_t sdi, tdi;
    drive_params(sdrive, &sdi); sdi.ext = drive_has_ext(sdrive);
    drive_params(tdrive, &tdi); tdi.ext = drive_has_ext(tdrive);
    if (!sdi.present) { printf("source drive 0x%02X not present\n", sdrive); xfer_free(); return 1; }
    if (!tdi.present) { printf("target drive 0x%02X not present\n", tdrive); xfer_free(); return 1; }
    uint64_t tgt_sectors = drive_total_sectors(&tdi, tdrive);
    printf("source 0x%02X: %u cyl %u head %u spt, ext=%s\n",
           sdrive, sdi.cyls, sdi.heads, sdi.spt, sdi.ext ? "yes" : "no");
    printf("target 0x%02X: %u cyl %u head %u spt, ext=%s, %lu sectors\n",
           tdrive, tdi.cyls, tdi.heads, tdi.spt, tdi.ext ? "yes" : "no",
           (unsigned long)tgt_sectors);
    if (mode == SZ_CUSTOM)      printf("size policy: custom (%lu bytes)\n", (unsigned long)custom_bytes);
    else if (mode != SZ_ORIGINAL) printf("size policy: %s\n", mode == SZ_MINIMUM ? "minimum" : "entire");
    if (has_filter) printf("partition filter: /PARTS mask 0x%X\n", sel_mask);

    if (!confirmed) {
        printf("REFUSING to write without /Y (this ERASES drive 0x%02X)\n", tdrive);
        xfer_free();
        return 1;
    }

    uint8_t mbr[512];
    if (read_lba(&sdi, sdrive, 0, 1, mbr) != 0) { printf("source MBR read failed\n"); xfer_free(); return 1; }
    if (mbr[510] != 0x55 || mbr[511] != 0xAA) {
        printf("source has no MBR signature -- superfloppy not supported\n");
        xfer_free(); return 1;
    }

    typedef struct { int slot; uint8_t type; uint64_t start, count, window_sec; } clp_t;
    clp_t P[4]; int np = 0;
    for (int i = 0; i < 4; i++) {
        const uint8_t *e = mbr + 446 + i * 16;
        if (e[4] == 0 || rd32(e + 12) == 0) continue;
        P[np].slot = i; P[np].type = e[4];
        P[np].start = rd32(e + 8); P[np].count = rd32(e + 12);
        P[np].window_sec = P[np].count;
        np++;
    }
    if (np == 0) { printf("no partitions in source MBR\n"); xfer_free(); return 1; }

    int cloned = 0;
    for (int k = 0; k < np; k++) {
        clp_t *p = &P[k];
        if (has_filter && !(sel_mask & (1u << p->slot))) {
            printf("  slot %d: not in /PARTS -- skipped\n", p->slot);
            continue;
        }
        if (!is_fat_part_type(p->type)) {
            printf("  slot %d: type 0x%02X not FAT -- skipped\n", p->slot, p->type);
            continue;
        }

        uint8_t vbr[512];
        if (read_lba(&sdi, sdrive, p->start, 1, vbr) != 0) {
            printf("  slot %d: VBR read failed -- skipped\n", p->slot);
            continue;
        }
        fatlay_t L;
        parse_fatlay(vbr, &L);
        if (!L.ok || L.bps != 512) {
            printf("  slot %d: not a 512-byte FAT -- skipped\n", p->slot);
            continue;
        }

        uint8_t *fat = malloc(L.old_spf * L.bps);
        if (!fat) { printf("  slot %d: out of memory\n", p->slot); xfer_free(); return 1; }
        if (load_region(&sdi, sdrive, p->start + L.reserved, L.old_spf * L.bps, fat) != 0) {
            printf("  slot %d: FAT read failed -- skipped\n", p->slot);
            free(fat); continue;
        }
        uint32_t first_data = L.reserved + L.num_fats * L.old_spf + L.root_dir_secs;
        uint32_t clusters = (L.old_total > first_data) ? (L.old_total - first_data) / L.spc : 0;
        uint32_t last_used = 0;
        for (uint32_t cl = 2; cl < clusters + 2; cl++)
            if (fat_entry(fat, L.fat_bits, cl) != 0) last_used = cl;
        uint32_t imaged_secs = (last_used >= 2) ? first_data + (last_used - 1) * L.spc : first_data;
        if (imaged_secs > L.old_total) imaged_secs = L.old_total;

        /* no-overlap ceiling: up to the next partition, else the target disk end */
        uint64_t limit_sec = tgt_sectors;
        for (int j = 0; j < np; j++)
            if (P[j].start > p->start && P[j].start < limit_sec) limit_sec = P[j].start;
        uint64_t limit_window = (limit_sec > p->start) ? limit_sec - p->start : 0;

        uint64_t win;
        switch (mode) {
            case SZ_MINIMUM: win = imaged_secs; break;
            case SZ_ENTIRE:  win = limit_window; break;
            case SZ_CUSTOM:  win = round_up_512(custom_bytes) / 512; break;
            default:         win = p->count; break;
        }
        uint64_t cap = max_fat_window(&L);
        if (win > cap) {
            printf("  slot %d: FAT%d cluster limit -- window capped to %lu KiB\n",
                   p->slot, L.fat_bits, (unsigned long)(cap * 512 / 1024));
            win = cap;
        }
        if (win < imaged_secs) win = imaged_secs;
        uint64_t floor = min_fat_window(&L);
        if (win < floor) win = floor;
        if (limit_window && win > limit_window) win = limit_window;
        if (win < imaged_secs) {
            printf("  slot %d: target too small for used data (%lu KiB) -- aborting\n",
                   p->slot, (unsigned long)((uint64_t)imaged_secs * 512 / 1024));
            free(fat); xfer_free(); return 1;
        }
        p->window_sec = win;

        printf("  slot %d (FAT%d) lba %lu: %lu KiB used -> %lu KiB window\n",
               p->slot, L.fat_bits, (unsigned long)p->start,
               (unsigned long)((uint64_t)imaged_secs * 512 / 1024),
               (unsigned long)(win * 512 / 1024));
        if (clone_partition(&sdi, sdrive, &tdi, tdrive, p->start, win * 512ULL,
                            &L, fat, imaged_secs) != 0) {
            free(fat); xfer_free(); return 1;
        }
        free(fat);

        if ((uint32_t)win != L.old_total) {
            int r = fat_resize(&tdi, tdrive, p->start, (uint32_t)win, imaged_secs);
            if (r > 0) set_clean_flags(&tdi, tdrive, p->start);
        }
        cloned++;
    }

    if (cloned == 0) { printf("no partitions cloned\n"); xfer_free(); return 1; }

    /* Patch the MBR windows that changed, then commit it to the target. */
    {
        uint32_t h = tdi.heads, s = tdi.spt;
        for (int k = 0; k < np; k++) {
            clp_t *p = &P[k];
            uint8_t *ent = mbr + 446 + p->slot * 16;
            uint32_t ns = (uint32_t)p->window_sec;
            if (rd32(ent + 12) == ns) continue;          /* unchanged -- verbatim */
            wr32(ent + 12, ns);
            if (h && s) {
                uint32_t end = (uint32_t)p->start + (ns ? ns - 1 : 0);
                uint32_t ec = end / (h * s);
                uint32_t et = end % (h * s);
                uint32_t eh = et / s, es = et % s + 1;
                if (ec > 1023) { ec = 1023; eh = h - 1; es = s; }
                ent[5] = (uint8_t)eh;
                ent[6] = (uint8_t)(((ec >> 2) & 0xC0) | (es & 0x3F));
                ent[7] = (uint8_t)ec;
            }
        }
    }
    if (write_lba(&tdi, tdrive, 0, 1, mbr) != 0) { printf("target MBR write failed\n"); xfer_free(); return 1; }
    printf("wrote MBR\n");

    printf("clone complete: %d partition%s 0x%02X -> 0x%02X\n",
           cloned, cloned == 1 ? "" : "s", sdrive, tdrive);
    xfer_free();
    return 0;
}
