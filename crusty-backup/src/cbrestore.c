/* cbrestore.c -- cb-dos restore engine (Phase 3 + on-DOS resize, docs/cb_dos.md).
 *
 * The inverse of cbbackup: reads the desktop's native PerPartition folder
 * (metadata.json + mbr.bin + partition-N.gz) from a DOS path and writes it back
 * onto a disk via int 13h, reconstructing a bootable card. Each partition's
 * gzip member is streamed through zlib gzread straight to the target sectors;
 * compacted partitions are zero-padded out to their target window so the FAT
 * geometry recorded in the BPB stays consistent.
 *
 * Sizing (mirrors the desktop's restore --size policy):
 *   /SIZE:ORIGINAL   restore at the recorded partition windows (default; the
 *                    proven byte-identical path -- no FS is touched)
 *   /SIZE:MINIMUM    shrink each FAT partition to its imaged (last-used-cluster)
 *                    size -- BPB total_sectors only, the oversized FAT is harmless
 *   /SIZE:ENTIRE     grow each FAT partition to fill the disk (or up to the next
 *                    partition) -- extends the FAT + shifts the data region when
 *                    the larger volume needs a bigger FAT
 *   /SIZE:CUSTOM /CUSTOM:<bytes>   resize each FAT partition to <bytes>
 *
 * On-DOS resize is FAT-only and needs 512-byte sectors (universal on CF/SD);
 * a non-512 or non-FAT partition is restored at original size with a note and
 * can be resized on the desktop. The algorithm is the C port of the desktop's
 * resize_fat_in_place (src/fs/fat.rs): shrink updates total_sectors only; grow
 * extends the FAT tables with free entries, shifting the data region forward
 * when the FAT needs more sectors, then patches the BPB. FAT16/32 clean-shutdown
 * flags are set afterwards; FAT32 FSInfo free counts are reset to "unknown".
 *
 * Build:  sh deps/fetch-zlib.sh   then   make restore   (-> build/cbrestore.exe)
 * Run:    CBRESTORE <folder> <target-drive-hex> /Y [/SIZE:mode] [/CUSTOM:bytes]
 *         e.g.  CBRESTORE C:\BK 81 /Y                  (original size)
 *               CBRESTORE C:\BK 81 /Y /SIZE:MINIMUM    (shrink to used data)
 *               CBRESTORE C:\BK 81 /Y /SIZE:ENTIRE     (grow to fill the disk)
 *               CBRESTORE C:\BK 81 /Y /SIZE:CUSTOM /CUSTOM:33554432
 *
 * See the (now-fixed) DAP/MCB note in cbbackup.c: xfer_init allocates
 * XFER_BYTES + 16 so the int13h Disk Address Packet stays inside the owned
 * block (the old under-allocation hung the process at exit under CWSDPMI).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <dpmi.h>
#include <go32.h>
#include <sys/movedata.h>
#include <zlib.h>

/* ----- DOS-memory transfer buffer ----------------------------------- */

#define XFER_SECTORS 16
#define XFER_BYTES   (XFER_SECTORS * 512)
static int g_buf_seg, g_buf_sel;

static int xfer_init(void) {
    /* +16 bytes for the int13h Disk Address Packet that write_lba/read_lba place
     * at offset XFER_BYTES. Without it the block is exactly XFER_BYTES and the
     * DAP overruns the next MCB, which DOS only trips over when it walks the
     * memory arena at program exit -> the CWSDPMI termination hang. */
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

/* Total addressable sectors. AH=48h (EDD) for LBA drives -- the true capacity,
 * needed to size /SIZE:ENTIRE on a disk bigger than CHS can describe. Falls
 * back to the CHS geometry product. */
static uint64_t drive_total_sectors(const drive_info_t *di, int drive) {
    if (di->ext) {
        __dpmi_regs r;
        uint8_t edd[32];
        memset(edd, 0, sizeof edd);
        wr16(edd, 0x1E);                 /* result buffer size */
        dosmemput(edd, sizeof edd, g_buf_seg * 16);
        memset(&r, 0, sizeof r);
        r.h.ah = 0x48; r.h.dl = drive;
        r.x.ds = g_buf_seg; r.x.si = 0;
        __dpmi_int(0x13, &r);
        if (!(r.x.flags & 1)) {
            dosmemget(g_buf_seg * 16, sizeof edd, edd);
            uint64_t total = (uint64_t)rd32(edd + 16) |
                             ((uint64_t)rd32(edd + 20) << 32);
            if (total > 0) return total;
        }
    }
    return (uint64_t)di->cyls * di->heads * di->spt;
}

/* Build the CHS register set for AH=02h/03h. Returns -1 if beyond CHS reach. */
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

/* Read `count` (<= XFER_SECTORS) sectors at `lba` into host `dst`.
 * 0 on success, else the BIOS AH error code. */
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

/* Write `count` sectors from host `src` to `lba`. 0 on success else BIOS AH. */
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
        r.h.ah = 0x43; r.h.al = 0x00;       /* 0 = write, no verify */
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

/* Read/write a sector-multiple byte region in <=XFER_SECTORS chunks. */
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

/* ----- tiny metadata.json field scanner ----------------------------- */
/* metadata.json is flat, machine-generated JSON. We don't need a real parser:
 * find `"key"` from a cursor and read the unsigned number / quoted string that
 * follows. Fields appear in a fixed order within each partition object, so a
 * forward scan keyed off `"index"` extracts each partition correctly. */

static const char *skip_to_value(const char *p) {
    while (*p && (*p == ':' || *p == ' ' || *p == '\t' || *p == '\n' || *p == '\r'))
        p++;
    return p;
}

/* First `"key"` at or after *cur. Returns pointer just past it, or NULL. */
static const char *find_key(const char *cur, const char *key) {
    char pat[64];
    sprintf(pat, "\"%s\"", key);
    const char *p = strstr(cur, pat);
    return p ? p + strlen(pat) : NULL;
}

static uint64_t u64_after(const char *cur, const char *key, uint64_t dflt) {
    const char *p = find_key(cur, key);
    if (!p) return dflt;
    p = skip_to_value(p);
    uint64_t v = 0; int any = 0;
    while (*p >= '0' && *p <= '9') { v = v * 10 + (uint64_t)(*p - '0'); p++; any = 1; }
    return any ? v : dflt;
}

/* Read the unsigned value that immediately follows the cursor (which already
 * sits just past a key) -- unlike u64_after, this does NOT search for another
 * occurrence of the key (which, for "index", would grab the next partition's). */
static uint64_t u64_at(const char *p, uint64_t dflt) {
    p = skip_to_value(p);
    uint64_t v = 0; int any = 0;
    while (*p >= '0' && *p <= '9') { v = v * 10 + (uint64_t)(*p - '0'); p++; any = 1; }
    return any ? v : dflt;
}

/* Copy the first quoted string after `"key"` into out (size cap). */
static int str_after(const char *cur, const char *key, char *out, int cap) {
    const char *p = find_key(cur, key);
    if (!p) return -1;
    p = strchr(p, '"');
    if (!p) return -1;
    p++;
    int n = 0;
    while (*p && *p != '"' && n < cap - 1) out[n++] = *p++;
    out[n] = 0;
    return 0;
}

/* ----- FAT layout (the subset resize needs) ------------------------- */

typedef struct {
    int      ok;
    unsigned bps, spc, reserved, num_fats, root_entries;
    uint32_t old_spf, old_total, root_dir_secs;
    int      is_fat32, fat_bits;
} fatlay_t;

/* Parse the 512-byte boot sector into a FAT layout. ok=0 if not a FAT BPB. */
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
    L->root_dir_secs = L->is_fat32
        ? 0 : ((L->root_entries * 32 + L->bps - 1) / L->bps);
    uint32_t data_start = L->reserved + L->num_fats * L->old_spf + L->root_dir_secs;
    uint32_t data_secs = (L->old_total > data_start) ? L->old_total - data_start : 0;
    uint32_t clusters = data_secs / L->spc;
    if (L->is_fat32)       L->fat_bits = 32;
    else if (clusters < 4085) L->fat_bits = 12;
    else                   L->fat_bits = 16;
    L->ok = 1;
}

/* Sectors needed for one FAT copy at a given total size and FAT type.
 * Mirrors compute_fat_sectors() in src/fs/fat.rs. */
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

/* Shift a sector region [src_start, src_end) (relative to part_lba) forward by
 * `shift` sectors, then zero-fill the gap it left. Reads backward so a partial
 * overlap can't clobber unread data. Mirrors shift_region_forward(). */
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

/* Shift a sector region [src_start, src_end) backward by `shift` sectors (its
 * new home is `shift` sectors lower). Copies low->high so a partial overlap
 * can't clobber unread data. The freed tail is left as-is -- it lies outside
 * the shrunk volume. Symmetric to shift_region_forward(). */
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

/* Highest valid cluster index for each FAT type. */
static uint32_t fat_max_clusters(int fat_bits) {
    if (fat_bits == 12) return 4084;
    if (fat_bits == 16) return 65524;
    return 0x0FFFFFF5u;
}

/* The largest partition window (sectors) this FAT type + cluster size can
 * address. Growing past it would overflow the cluster count into the next FAT
 * width (e.g. a FAT16 with >65524 clusters is not a valid FAT16 -- it must
 * become FAT32, which is a full re-cluster we don't attempt on DOS). */
static uint64_t max_fat_window(const fatlay_t *L) {
    uint64_t maxc = fat_max_clusters(L->fat_bits);
    uint64_t spf;
    if (L->fat_bits == 12) spf = (((maxc + 2) * 3 + 1) / 2 + L->bps - 1) / L->bps;
    else {
        unsigned bpe = (L->fat_bits == 16) ? 2 : 4;
        spf = ((maxc + 2) * bpe + L->bps - 1) / L->bps;
    }
    return (uint64_t)L->reserved + (uint64_t)L->num_fats * spf +
           L->root_dir_secs + maxc * L->spc;
}

/* The smallest window (sectors) that keeps the volume a valid FAT of its
 * current type -- FAT16 needs >= 4085 clusters, FAT32 >= 65525. Shrinking
 * below this would slip into a narrower FAT width (the type-change path). */
static uint64_t min_fat_window(const fatlay_t *L) {
    uint64_t minc = (L->fat_bits == 16) ? 4085 : (L->fat_bits == 32) ? 65525 : 1;
    uint64_t spf;
    if (L->fat_bits == 12) spf = (((minc + 2) * 3 + 1) / 2 + L->bps - 1) / L->bps;
    else {
        unsigned bpe = (L->fat_bits == 16) ? 2 : 4;
        spf = ((minc + 2) * bpe + L->bps - 1) / L->bps;
    }
    return (uint64_t)L->reserved + (uint64_t)L->num_fats * spf +
           L->root_dir_secs + minc * L->spc;
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

/* Write the BPB to the boot sector (and, for FAT32, the sector-6 backup). */
static int write_bpb(const drive_info_t *di, int drive, uint64_t part_lba,
                     const uint8_t *bpb, int is_fat32, unsigned bps) {
    if (write_lba(di, drive, part_lba, 1, bpb) != 0) return -1;
    if (is_fat32) {
        uint32_t ratio = bps / 512; if (ratio == 0) ratio = 1;
        if (write_lba(di, drive, part_lba + 6 * ratio, 1, bpb) != 0) return -1;
    }
    return 0;
}

/* Reset the FAT32 FSInfo free-cluster counts to "unknown" so the OS recomputes
 * them after a resize. Best-effort -- silently ignores a missing/invalid FSInfo. */
static void reset_fsinfo(const drive_info_t *di, int drive, uint64_t part_lba,
                         const uint8_t *bpb, unsigned bps) {
    uint32_t ratio = bps / 512; if (ratio == 0) ratio = 1;
    unsigned fsinfo_sec = rd16(bpb + 48);
    if (fsinfo_sec == 0 || fsinfo_sec == 0xFFFF) return;
    uint64_t fsinfo_lba = part_lba + (uint64_t)fsinfo_sec * ratio;
    uint8_t sec[512];
    if (read_lba(di, drive, fsinfo_lba, 1, sec) != 0) return;
    if (rd32(sec) != 0x41615252) return;              /* "RRaA" lead signature */
    if (rd32(sec + 484) != 0x61417272) return;        /* "rrAa" struct signature */
    wr32(sec + 488, 0xFFFFFFFF);                      /* free count = unknown */
    wr32(sec + 492, 0xFFFFFFFF);                      /* next free = unknown */
    write_lba(di, drive, fsinfo_lba, 1, sec);
}

/* Resize the FAT filesystem at `part_lba` to `new_total` sectors in place.
 * `imaged_secs` is the end (relative to part start) of meaningful data written
 * by the restore -- everything past it is already zero, so a grow only needs to
 * shift up to there. Returns 1 if changed, 0 if no-op/non-FAT, -1 on error.
 * The C port of resize_fat_in_place() (src/fs/fat.rs). 512-byte sectors only. */
static int fat_resize(const drive_info_t *di, int drive, uint64_t part_lba,
                      uint32_t new_total, uint32_t imaged_secs) {
    uint8_t bpb[512];
    if (read_lba(di, drive, part_lba, 1, bpb) != 0) return -1;
    fatlay_t L;
    parse_fatlay(bpb, &L);
    if (!L.ok) return 0;
    if (L.bps != 512) {
        printf("  resize: %u-byte sectors unsupported on DOS -- left at original\n", L.bps);
        return 0;
    }
    if (L.old_total == new_total) return 0;            /* nothing to do */

    uint32_t ts16 = rd16(bpb + 19);

    /* A FAT16/12 can only address fat_max_clusters() clusters at the current
     * cluster size; growing past that would need FAT32 (a full re-cluster we
     * don't do on DOS). Cap the target so the volume stays a valid FAT. */
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
        /* Type would flip across the FAT12/16 (4085-cluster) boundary -- e.g.
         * shrinking a small FAT16. Don't rewrite the table as the other width;
         * update the BPB total only, matching the desktop's same-boundary path. */
        printf("  resize: FAT%d->FAT%d boundary -- BPB total only\n",
               L.fat_bits, new_fat_bits);
        patch_bpb_total(bpb, new_total, ts16);
        return (write_bpb(di, drive, part_lba, bpb, L.is_fat32, L.bps) == 0) ? 1 : -1;
    }

    printf("  FAT%d resize: clusters -> %lu, spf %lu -> %lu, total %lu -> %lu\n",
           L.fat_bits, (unsigned long)new_clusters,
           (unsigned long)L.old_spf, (unsigned long)new_spf,
           (unsigned long)L.old_total, (unsigned long)new_total);

    /* The data region (root dir + used clusters) we may have to relocate. In
     * the OLD layout it sits at [move_start, imaged_secs); everything past
     * imaged_secs is already zero, so we never move it. */
    uint32_t move_start = L.reserved + L.num_fats * L.old_spf;
    uint32_t move_end = imaged_secs;
    if (move_end < move_start) move_end = move_start;
    if (move_end > L.old_total) move_end = L.old_total;
    uint32_t new_fat_bytes = new_spf * L.bps;

    if (new_spf > L.old_spf) {
        /* GROW the FAT: shift data forward, then write the zero-extended FAT. */
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
        /* SHRINK the FAT: write the truncated FAT (all surviving cluster chains
         * live in its first new_spf sectors), then shift data backward onto it.
         * Keeps the volume a clean, minimally-sized FAT instead of an oversized
         * table whose cluster count slips below the FAT16 floor. */
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
    /* else new_spf == old_spf: the data region doesn't move. */

    patch_bpb_total(bpb, new_total, ts16);
    if (new_spf != L.old_spf) {
        if (L.is_fat32) wr32(bpb + 36, new_spf);
        else            wr16(bpb + 22, (uint16_t)new_spf);
    }
    if (write_bpb(di, drive, part_lba, bpb, L.is_fat32, L.bps) != 0) return -1;
    if (L.is_fat32) reset_fsinfo(di, drive, part_lba, bpb, L.bps);
    return 1;
}

/* Set the FAT16/32 clean-shutdown + no-I/O-error flags in FAT[1] (each copy).
 * FAT12 has no such flags. Mirrors set_fat_clean_flags() (src/fs/fat.rs). */
static void set_clean_flags(const drive_info_t *di, int drive, uint64_t part_lba) {
    uint8_t bpb[512];
    if (read_lba(di, drive, part_lba, 1, bpb) != 0) return;
    fatlay_t L;
    parse_fatlay(bpb, &L);
    if (!L.ok || L.bps != 512 || L.fat_bits == 12) return;
    /* old_spf in L is the on-disk value, which we just (maybe) changed; re-read. */
    uint32_t spf = L.is_fat32 ? rd32(bpb + 36) : rd16(bpb + 22);
    uint64_t fat_start = part_lba + L.reserved;
    for (uint32_t c = 0; c < L.num_fats; c++) {
        uint64_t copy = fat_start + (uint64_t)c * spf;
        uint8_t sec[512];
        if (read_lba(di, drive, copy, 1, sec) != 0) continue;
        if (L.fat_bits == 16) {
            uint16_t v = rd16(sec + 2); v |= 0xC000; wr16(sec + 2, v);
        } else {
            uint32_t v = rd32(sec + 4); v |= 0x0C000000u; wr32(sec + 4, v);
        }
        write_lba(di, drive, copy, 1, sec);
    }
}

/* ----- restore one partition ---------------------------------------- */

/* Read just the first sector of <folder>/<gzname> and parse its FAT layout
 * (bps / old total) -- needed before choosing a window so a non-512-byte or
 * non-FAT partition can fall back to original size. 0 on success, -1 if the
 * gz can't be opened. */
static int peek_partition(const char *folder, const char *gzname, fatlay_t *L) {
    char path[200];
    sprintf(path, "%s\\%s", folder, gzname);
    memset(L, 0, sizeof *L);
    gzFile gz = gzopen(path, "rb");
    if (!gz) return -1;
    uint8_t first[512];
    int n = gzread(gz, first, 512);
    gzclose(gz);
    if (n >= 512) parse_fatlay(first, L);
    return 0;
}

/* Stream <folder>/<gzname> to `start_lba`, zero-padding out to `window_bytes`. */
static int restore_partition(const drive_info_t *di, int drive, const char *folder,
                             const char *gzname, uint64_t start_lba,
                             uint64_t window_bytes) {
    char path[200];
    sprintf(path, "%s\\%s", folder, gzname);
    gzFile gz = gzopen(path, "rb");
    if (!gz) { printf("  cannot open %s\n", path); return -1; }

    uint8_t acc[XFER_BYTES];
    int acc_len = 0;
    uint64_t written = 0;
    int rc = 0;

    for (;;) {
        int want = XFER_BYTES - acc_len;
        int n = gzread(gz, acc + acc_len, want);
        if (n < 0) { printf("  gzread error\n"); rc = -1; break; }
        if (n == 0 && acc_len == 0) break;
        acc_len += n;
        int full = acc_len / 512;
        if (full > 0) {
            if (write_lba(di, drive, start_lba + written / 512, full, acc) != 0) {
                printf("  write error at lba %lu\n",
                       (unsigned long)(start_lba + written / 512));
                rc = -1; break;
            }
            written += (uint64_t)full * 512;
            int rem = acc_len - full * 512;
            if (rem) memmove(acc, acc + full * 512, rem);
            acc_len = rem;
        }
        if (n == 0) break;   /* EOF and the tail (< 1 sector) handled below */
    }
    if (rc == 0 && acc_len > 0) {                     /* partial trailing sector */
        memset(acc + acc_len, 0, 512 - acc_len);
        if (write_lba(di, drive, start_lba + written / 512, 1, acc) == 0)
            written += 512;
    }
    gzclose(gz);
    if (rc != 0) return rc;

    /* Zero-pad out to the target partition window. */
    if (written < window_bytes) {
        memset(acc, 0, XFER_BYTES);
        while (written < window_bytes) {
            uint64_t left = window_bytes - written;
            int secs = (left / 512 > XFER_SECTORS) ? XFER_SECTORS : (int)(left / 512);
            if (secs < 1) break;
            if (write_lba(di, drive, start_lba + written / 512, secs, acc) != 0) break;
            written += (uint64_t)secs * 512;
        }
    }
    return 0;
}

/* ----- size policy -------------------------------------------------- */

enum { SZ_ORIGINAL, SZ_MINIMUM, SZ_ENTIRE, SZ_CUSTOM };

/* If `arg` begins with `prefix` (case-insensitive), return the rest; else NULL. */
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

/* Parse a comma/space-separated list of partition indices (the metadata "index"
 * / MBR slot) into a bitmask. Returns 0 (and sets *mask) on success, else -1. */
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

/* A parsed partition entry from metadata.json. */
typedef struct {
    int      index;
    uint64_t start_lba;
    uint64_t original;
    uint64_t imaged;
    uint64_t minimum;
    char     gz[40];
    int      is_fat;          /* has a .gz member -> a restorable FAT partition */
    uint64_t window_sec;      /* chosen window (sectors), filled during restore */
} part_t;

/* ----- main --------------------------------------------------------- */

int main(int argc, char **argv) {
    setvbuf(stdout, NULL, _IONBF, 0);
    if (argc < 3) {
        printf("cb-dos restore (Phase 3 + on-DOS resize + selective)\n");
        printf("usage: CBRESTORE <folder> <target-drive-hex> /Y [/SIZE:mode] [/CUSTOM:bytes] [/PARTS:i,j]\n");
        printf("  /Y               confirms the destructive write to the target drive\n");
        printf("  /SIZE:ORIGINAL   restore at the recorded sizes (default)\n");
        printf("  /SIZE:MINIMUM    shrink each FAT partition to its used data\n");
        printf("  /SIZE:ENTIRE     grow each FAT partition to fill the disk\n");
        printf("  /SIZE:CUSTOM     resize to /CUSTOM:<bytes>\n");
        printf("  /PARTS:i,j       restore only these partition indices (metadata \"index\")\n");
        return 2;
    }
    const char *folder = argv[1];
    int drive = (int)strtol(argv[2], NULL, 16);
    int confirmed = 0, mode = SZ_ORIGINAL;
    uint64_t custom_bytes = 0;
    unsigned sel_mask = 0; int has_filter = 0;
    for (int i = 3; i < argc; i++) {
        const char *v;
        if (eq_ci(argv[i], "/Y")) { confirmed = 1; }
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
    if (mode == SZ_CUSTOM && custom_bytes == 0) {
        printf("/SIZE:CUSTOM needs /CUSTOM:<bytes>\n");
        return 2;
    }

    /* Read metadata.json. */
    char mpath[200];
    sprintf(mpath, "%s\\metadata.json", folder);
    FILE *mf = fopen(mpath, "rb");
    if (!mf) { printf("cannot open %s\n", mpath); return 1; }
    static char meta[16384];
    size_t mn = fread(meta, 1, sizeof meta - 1, mf);
    fclose(mf);
    meta[mn] = 0;

    char ptype[16] = "";
    str_after(meta, "partition_table_type", ptype, sizeof ptype);
    if (strcmp(ptype, "MBR") != 0) {
        printf("partition_table_type \"%s\" not supported by this MVP (MBR only)\n", ptype);
        return 1;
    }

    /* Geometry for CHS recompute when patching the MBR (backup-recorded). */
    uint32_t md_heads = (uint32_t)u64_after(meta, "heads", 0);
    uint32_t md_spt = (uint32_t)u64_after(meta, "sectors_per_track", 0);

    /* Parse the partitions array. */
    static part_t parts[16];
    int nparts = 0;
    {
        const char *cur = strstr(meta, "\"partitions\"");
        if (!cur) cur = meta;
        for (; nparts < 16;) {
            const char *idx = find_key(cur, "index");
            if (!idx) break;
            part_t *p = &parts[nparts];
            memset(p, 0, sizeof *p);
            p->index = (int)u64_at(idx, 0);   /* value right after THIS "index" */
            p->start_lba = u64_after(idx, "start_lba", 0);
            p->original = u64_after(idx, "original_size_bytes", 0);
            p->imaged = u64_after(idx, "imaged_size_bytes", p->original);
            p->minimum = u64_after(idx, "minimum_size_bytes", p->imaged);
            str_after(idx, "compressed_files", p->gz, sizeof p->gz);
            p->is_fat = (p->gz[0] != 0 && strstr(p->gz, ".gz") != NULL &&
                         p->start_lba != 0 && p->original != 0);
            const char *next = find_key(idx, "compacted");
            cur = next ? next : (idx + 1);
            nparts++;
        }
    }
    if (nparts == 0) { printf("no partitions in metadata\n"); return 1; }

    if (xfer_init() < 0) { printf("DOS memory alloc failed\n"); return 1; }
    drive_info_t di;
    drive_params(drive, &di);
    if (!di.present) { printf("target drive 0x%02X not present\n", drive); xfer_free(); return 1; }
    di.ext = drive_has_ext(drive);
    uint64_t disk_sectors = drive_total_sectors(&di, drive);
    printf("target drive 0x%02X: %u cyl %u head %u spt, LBA-ext=%s, %lu sectors\n",
           drive, di.cyls, di.heads, di.spt, di.ext ? "yes" : "no",
           (unsigned long)disk_sectors);
    if (mode == SZ_CUSTOM)
        printf("size policy: custom (%lu bytes)\n", (unsigned long)custom_bytes);
    else if (mode != SZ_ORIGINAL)
        printf("size policy: %s\n", mode == SZ_MINIMUM ? "minimum" : "entire");
    if (has_filter) printf("partition filter: /PARTS mask 0x%X\n", sel_mask);

    if (!confirmed) {
        printf("REFUSING to write without /Y (this ERASES drive 0x%02X)\n", drive);
        xfer_free();
        return 1;
    }

    /* Read mbr.bin (patched + written after the data, with final windows). */
    char bpath[200];
    sprintf(bpath, "%s\\mbr.bin", folder);
    FILE *bf = fopen(bpath, "rb");
    if (!bf) { printf("cannot open %s\n", bpath); xfer_free(); return 1; }
    uint8_t mbr[512];
    if (fread(mbr, 1, 512, bf) != 512) { printf("short mbr.bin\n"); fclose(bf); xfer_free(); return 1; }
    fclose(bf);

    /* Restore each FAT partition's data, then resize the filesystem in place. */
    int restored = 0;
    for (int k = 0; k < nparts; k++) {
        part_t *p = &parts[k];
        p->window_sec = p->original / 512;            /* default: original window */
        if (has_filter && (p->index < 0 || p->index >= 32 ||
                           !(sel_mask & (1u << p->index)))) {
            printf("  partition %d: not in /PARTS -- skipped\n", p->index);
            continue;
        }
        if (!p->is_fat) {
            if (p->gz[0])
                printf("  partition %d: codec not gzip (%s) -- MVP restores .gz only\n",
                       p->index, p->gz);
            continue;
        }

        /* Peek the BPB: resize is FAT-only and needs 512-byte sectors. */
        fatlay_t pl;
        if (peek_partition(folder, p->gz, &pl) != 0) {
            printf("  cannot open %s\\%s\n", folder, p->gz);
            xfer_free();
            return 1;
        }
        int is_512_fat = pl.ok && pl.bps == 512;

        /* The no-overlap ceiling: up to the next partition, else the disk end. */
        uint64_t limit_sec = disk_sectors;
        for (int j = 0; j < nparts; j++)
            if (parts[j].start_lba > p->start_lba && parts[j].start_lba < limit_sec)
                limit_sec = parts[j].start_lba;
        uint64_t limit_window = (limit_sec > p->start_lba) ? limit_sec - p->start_lba : 0;
        uint64_t imaged_sec = round_up_512(p->imaged) / 512;

        /* Decide the target window (sectors). The resize itself compares this
         * against the gz's actual BPB total, so ORIGINAL still grows a backup
         * whose gz holds a pre-minimized FS (the desktop's compacted gzip). */
        uint64_t win;
        switch (mode) {
            case SZ_MINIMUM: {
                uint64_t m = p->minimum > p->imaged ? p->minimum : p->imaged;
                win = round_up_512(m) / 512;
            } break;
            case SZ_ENTIRE:  win = limit_window; break;
            case SZ_CUSTOM:  win = round_up_512(custom_bytes) / 512; break;
            default:         win = p->original / 512; break;
        }

        int can_resize = is_512_fat;
        if (!can_resize) {
            if (mode != SZ_ORIGINAL)
                printf("  partition %d: %s -- on-DOS resize needs a 512-byte FAT; "
                       "original size (resize on desktop)\n",
                       p->index, pl.ok ? "non-512 sectors" : "not FAT");
            win = p->original / 512;               /* can't touch the FS */
        }

        if (can_resize) {
            uint64_t cap = max_fat_window(&pl);    /* FAT cluster-count ceiling */
            if (win > cap) {
                printf("  partition %d: FAT%d cluster limit -- window capped to %lu KiB\n",
                       p->index, pl.fat_bits, (unsigned long)(cap * 512 / 1024));
                win = cap;
            }
            if (win < imaged_sec) win = imaged_sec;            /* keep used data */
            uint64_t floor = min_fat_window(&pl);              /* stay a valid FAT */
            if (win < floor) win = floor;
            if (limit_window && win > limit_window) win = limit_window; /* no overlap */
            if (win < imaged_sec) {
                printf("  partition %d: target disk too small for used data "
                       "(%lu KiB) -- aborting\n",
                       p->index, (unsigned long)(imaged_sec * 512 / 1024));
                xfer_free();
                return 1;
            }
        } else if (limit_window && win > limit_window) {
            printf("  partition %d: original window exceeds disk -- target too small\n",
                   p->index);
            xfer_free();
            return 1;
        }
        p->window_sec = win;

        if (restore_partition(&di, drive, folder, p->gz, p->start_lba,
                              win * 512ULL) != 0) {
            xfer_free();
            return 1;
        }

        int resized = 0;
        if (can_resize && (uint32_t)win != pl.old_total) {
            int r = fat_resize(&di, drive, p->start_lba, (uint32_t)win, (uint32_t)imaged_sec);
            if (r > 0) { set_clean_flags(&di, drive, p->start_lba); resized = 1; }
        }

        printf("  restored %s -> lba %lu (%lu KiB window%s)\n",
               p->gz, (unsigned long)p->start_lba,
               (unsigned long)(p->window_sec * 512 / 1024),
               resized ? ", resized" : "");
        restored++;
    }

    if (restored == 0) { printf("no partitions restored\n"); xfer_free(); return 1; }

    /* Patch MBR entries whose window changed, then commit the table to sector 0.
     * An unchanged window leaves its entry byte-for-byte verbatim (so an
     * original-size restore keeps the exact source MBR -- the proven path). */
    {
        uint32_t h = md_heads ? md_heads : di.heads;
        uint32_t s = md_spt ? md_spt : di.spt;
        for (int k = 0; k < nparts; k++) {
            part_t *p = &parts[k];
            if (!p->is_fat) continue;
            for (int e = 0; e < 4; e++) {
                uint8_t *ent = mbr + 446 + e * 16;
                if (ent[4] == 0) continue;
                if (rd32(ent + 8) != (uint32_t)p->start_lba) continue;
                uint32_t new_sectors = (uint32_t)p->window_sec;
                if (rd32(ent + 12) == new_sectors) break;   /* unchanged -- verbatim */
                wr32(ent + 12, new_sectors);
                if (h && s) {
                    uint32_t end = (uint32_t)p->start_lba + (new_sectors ? new_sectors - 1 : 0);
                    uint32_t ec = end / (h * s);
                    uint32_t et = end % (h * s);
                    uint32_t eh = et / s, es = et % s + 1;
                    if (ec > 1023) { ec = 1023; eh = h - 1; es = s; }
                    ent[5] = (uint8_t)eh;
                    ent[6] = (uint8_t)(((ec >> 2) & 0xC0) | (es & 0x3F));
                    ent[7] = (uint8_t)ec;
                }
                break;
            }
        }
    }
    if (write_lba(&di, drive, 0, 1, mbr) != 0) {
        printf("MBR write failed\n"); xfer_free(); return 1;
    }
    printf("wrote MBR\n");

    printf("restore complete: %d partition%s written to drive 0x%02X\n",
           restored, restored == 1 ? "" : "s", drive);
    xfer_free();
    return 0;
}
