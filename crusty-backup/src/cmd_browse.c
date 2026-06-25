/* cmd_browse.c -- the `ls` / `get` commands + the shared browse engine
 * (cbbrowse.h), Phase 4c.
 *
 * Browse + extract single files from a backup with no full restore and no scratch
 * space: a FAT12/16/32 directory reader + file extractor that works straight out
 * of a compacted `partition-N.gz`. zlib's gzseek gives random access into the
 * compressed partition (each seek decompresses from the start, O(offset);
 * backward seeks rewind -- fine for grabbing a file, the chunked `.cbk` lazy
 * reader is the eventual fast path). The whole first FAT is cached in RAM;
 * directories + file data are read on demand. The same engine backs the TUI
 * browse screen (crustybk.c).
 *
 *   CRUSTYBK ls  <folder> [N] [path]          list a directory in partition N
 *   CRUSTYBK get <folder> [N] <path> <dest>   extract one file to a DOS path
 */

#include "cbbrowse.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/stat.h>

#define ATTR_VOL  0x08
#define ATTR_LFN  0x0F

/* Random read of `len` bytes at partition byte offset `off`, dispatched to
 * whichever backend the volume was opened with: zlib gzseek for a backup, or
 * int13h sector reads for a live disk. 0 / -1.
 *
 * In practice every offset the engine asks for is sector-aligned (BPB, FAT,
 * cluster, and root-dir offsets all are); for the live path the common
 * aligned case reads straight into `buf`, and a sector-padded bounce buffer
 * covers any unaligned head/tail (an odd root_entries count). */
static int vol_read_at(fatvol_t *v, uint64_t off, void *buf, uint32_t len) {
    if (v->gz) {
        if (gzseek(v->gz, (z_off_t)off, SEEK_SET) < 0) return -1;
        int n = gzread(v->gz, buf, len);
        return (n == (int)len) ? 0 : -1;
    }
    /* live disk: off is a byte offset within the partition */
    uint64_t abs = (v->part_lba << 9) + off;       /* absolute disk byte */
    uint64_t lba = abs >> 9;
    uint32_t head = (uint32_t)(abs & 511);
    if (head == 0 && (len & 511) == 0)
        return load_region(&v->di, v->drive, lba, len, (uint8_t *)buf);

    uint32_t span = head + len;
    uint32_t nsec = (span + 511) >> 9;
    uint8_t *tmp = malloc((size_t)nsec * 512);
    if (!tmp) return -1;
    int rc = load_region(&v->di, v->drive, lba, nsec * 512, tmp);
    if (rc == 0) memcpy(buf, tmp + head, len);
    free(tmp);
    return rc;
}

static uint64_t cluster_off(const fatvol_t *v, uint32_t cl) {
    return ((uint64_t)v->L.first_data_sec + (uint64_t)(cl - 2) * v->L.spc) * v->L.bps;
}
static uint32_t eoc_mark(int fat_bits) {
    return (fat_bits == 12) ? 0x0FF8u : (fat_bits == 16) ? 0xFFF8u : 0x0FFFFFF8u;
}

int cbk_default_part(const char *folder, int part) {
    if (part >= 0) return part;
    for (int n = 0; n < 16; n++) {
        char path[200];
        sprintf(path, "%s\\partition-%d.gz", folder, n);
        FILE *f = fopen(path, "rb");
        if (f) { fclose(f); return n; }
    }
    return 0;
}

/* Finish opening a volume whose read backend (gz or live disk) is already set
 * on `v`: read + parse the BPB, cache the first FAT, compute the root layout.
 * Leaves the backend handle for cbk_close_vol to release on the error path. */
static int vol_finish_open(fatvol_t *v) {
    uint8_t bpb[512];
    if (vol_read_at(v, 0, bpb, 512) != 0) { printf("BPB read failed\n"); return -1; }
    parse_fatlay(bpb, &v->L);
    if (!v->L.ok)        { printf("not a FAT volume\n"); return -1; }
    if (v->L.bps != 512) { printf("only 512-byte sectors supported\n"); return -1; }
    v->fat_bytes = v->L.old_spf * v->L.bps;
    v->fat = malloc(v->fat_bytes);
    if (!v->fat)         { printf("out of memory\n"); return -1; }
    if (vol_read_at(v, (uint64_t)v->L.reserved * v->L.bps, v->fat, v->fat_bytes) != 0) {
        printf("FAT read failed\n"); return -1;
    }
    v->root_off = (uint32_t)((uint64_t)(v->L.reserved + v->L.num_fats * v->L.old_spf) * v->L.bps);
    v->root_bytes = v->L.root_entries * 32;
    v->root_cluster = v->L.is_fat32 ? rd32(bpb + 44) : 0;
    return 0;
}

int cbk_open_vol(const char *folder, int part, fatvol_t *v) {
    char path[200];
    sprintf(path, "%s\\partition-%d.gz", folder, part);
    memset(v, 0, sizeof *v);
    v->gz = gzopen(path, "rb");
    if (!v->gz) { printf("cannot open %s\n", path); return -1; }
    if (vol_finish_open(v) != 0) { cbk_close_vol(v); return -1; }
    return 0;
}

int cbk_open_vol_live(const drive_info_t *di, int drive, uint64_t part_lba, fatvol_t *v) {
    memset(v, 0, sizeof *v);
    v->gz = NULL;                         /* live backend */
    v->di = *di;
    v->drive = drive;
    v->part_lba = part_lba;
    if (vol_finish_open(v) != 0) { cbk_close_vol(v); return -1; }
    return 0;
}

void cbk_close_vol(fatvol_t *v) {
    if (v->fat) free(v->fat);
    if (v->gz) gzclose(v->gz);
    v->fat = NULL; v->gz = NULL;
}

/* Read all bytes of a directory (root or a subdir chain) into a malloc'd buffer. */
static uint8_t *read_dir_bytes(fatvol_t *v, uint32_t first_cluster, int is_fixed_root, uint32_t *out_len) {
    if (is_fixed_root) {
        uint8_t *buf = malloc(v->root_bytes ? v->root_bytes : 1);
        if (!buf) return NULL;
        if (v->root_bytes && vol_read_at(v, v->root_off, buf, v->root_bytes) != 0) { free(buf); return NULL; }
        *out_len = v->root_bytes;
        return buf;
    }
    uint32_t csize = v->L.spc * v->L.bps;
    uint32_t eoc = eoc_mark(v->L.fat_bits);
    uint32_t cl = first_cluster, ncl = 0;
    while (cl >= 2 && cl < eoc && ncl < 65536) { ncl++; cl = fat_entry(v->fat, v->L.fat_bits, cl); }
    if (ncl == 0) { *out_len = 0; return malloc(1); }
    uint8_t *buf = malloc((size_t)ncl * csize);
    if (!buf) return NULL;
    cl = first_cluster;
    uint32_t pos = 0;
    for (uint32_t i = 0; i < ncl; i++) {
        if (vol_read_at(v, cluster_off(v, cl), buf + pos, csize) != 0) { free(buf); return NULL; }
        pos += csize;
        cl = fat_entry(v->fat, v->L.fat_bits, cl);
    }
    *out_len = pos;
    return buf;
}

static void name_83(const uint8_t *e, char *out) {
    int n = 0;
    for (int i = 0; i < 8; i++) { if (e[i] == ' ') break; out[n++] = (char)e[i]; }
    int hasext = 0;
    for (int i = 8; i < 11; i++) if (e[i] != ' ') { hasext = 1; break; }
    if (hasext) { out[n++] = '.'; for (int i = 8; i < 11; i++) { if (e[i] == ' ') break; out[n++] = (char)e[i]; } }
    out[n] = 0;
}

typedef struct { const uint8_t *buf; uint32_t len, off; } dir_iter_t;
static void dir_iter_init(dir_iter_t *it, const uint8_t *buf, uint32_t len) {
    it->buf = buf; it->len = len; it->off = 0;
}
static int dir_iter_next(dir_iter_t *it, dirent_t *d) {
    uint16_t lfn[260];
    int lfnmax = 0;
    while (it->off + 32 <= it->len) {
        const uint8_t *e = it->buf + it->off;
        it->off += 32;
        if (e[0] == 0x00) return 0;                 /* end of directory */
        if (e[0] == 0xE5) { lfnmax = 0; continue; } /* deleted */
        uint8_t attr = e[11];
        if (attr == ATTR_LFN) {
            int seq = e[0] & 0x1F;
            if (seq >= 1 && seq <= 20) {
                int idx = (seq - 1) * 13;
                static const int o5[5] = {1, 3, 5, 7, 9};
                static const int o6[6] = {14, 16, 18, 20, 22, 24};
                static const int o2[2] = {28, 30};
                for (int i = 0; i < 5; i++) lfn[idx++] = e[o5[i]] | (e[o5[i] + 1] << 8);
                for (int i = 0; i < 6; i++) lfn[idx++] = e[o6[i]] | (e[o6[i] + 1] << 8);
                for (int i = 0; i < 2; i++) lfn[idx++] = e[o2[i]] | (e[o2[i] + 1] << 8);
                if (idx > lfnmax) lfnmax = idx;
            }
            continue;
        }
        if (attr & ATTR_VOL) { lfnmax = 0; continue; }   /* volume label */

        if (lfnmax > 0) {
            int n = 0;
            for (int i = 0; i < lfnmax && n < (int)sizeof d->name - 1; i++) {
                uint16_t c = lfn[i];
                if (c == 0 || c == 0xFFFF) break;
                d->name[n++] = (c < 128) ? (char)c : '?';   /* cb-dos is ASCII */
            }
            d->name[n] = 0;
        } else {
            name_83(e, d->name);
        }
        d->attr = attr;
        d->first_cluster = ((uint32_t)rd16(e + 20) << 16) | rd16(e + 26);
        d->size = rd32(e + 28);
        return 1;
    }
    return 0;
}

int cbk_list_dir(fatvol_t *v, uint32_t cluster, int fixed_root, dirent_t *out, int max) {
    uint32_t len;
    uint8_t *dir = read_dir_bytes(v, cluster, fixed_root, &len);
    if (!dir) return -1;
    dir_iter_t it;
    dir_iter_init(&it, dir, len);
    dirent_t d;
    int n = 0;
    while (n < max && dir_iter_next(&it, &d)) {
        if (strcmp(d.name, ".") == 0 || strcmp(d.name, "..") == 0) continue;
        out[n++] = d;
    }
    free(dir);
    return n;
}

static int dir_find(const uint8_t *buf, uint32_t len, const char *name, dirent_t *out) {
    dir_iter_t it;
    dir_iter_init(&it, buf, len);
    dirent_t d;
    while (dir_iter_next(&it, &d))
        if (eq_ci(d.name, name)) { *out = d; return 1; }
    return 0;
}

/* Descend `path` from the root. Fills *out + *is_dir (empty path = root). */
static int resolve_path(fatvol_t *v, const char *path, dirent_t *out, int *is_dir) {
    uint32_t cur_cluster = v->root_cluster;
    int cur_is_fixed_root = !v->L.is_fat32;
    memset(out, 0, sizeof *out);
    out->attr = CBK_ATTR_DIR;
    out->first_cluster = cur_cluster;
    *is_dir = 1;

    char comp[256];
    const char *p = path;
    while (*p) {
        while (*p == '\\' || *p == '/') p++;
        if (!*p) break;
        int n = 0;
        while (*p && *p != '\\' && *p != '/' && n < (int)sizeof comp - 1) comp[n++] = *p++;
        comp[n] = 0;

        uint32_t len;
        uint8_t *dir = read_dir_bytes(v, cur_cluster, cur_is_fixed_root, &len);
        if (!dir) { printf("read error\n"); return -1; }
        dirent_t found;
        int ok = dir_find(dir, len, comp, &found);
        free(dir);
        if (!ok) { printf("not found: %s\n", comp); return -1; }
        *out = found;
        *is_dir = (found.attr & CBK_ATTR_DIR) != 0;
        cur_cluster = found.first_cluster;
        cur_is_fixed_root = 0;
    }
    return 0;
}

int cbk_extract(fatvol_t *v, const dirent_t *f, const char *dest) {
    FILE *out = fopen(dest, "wb");
    if (!out) { printf("cannot create %s\n", dest); return -1; }
    uint32_t csize = v->L.spc * v->L.bps;
    uint8_t *buf = malloc(csize);
    if (!buf) { printf("out of memory\n"); fclose(out); return -1; }
    uint32_t eoc = eoc_mark(v->L.fat_bits);
    uint32_t cl = f->first_cluster, remaining = f->size, written = 0;
    int rc = 0;
    while (remaining > 0 && cl >= 2 && cl < eoc) {
        if (vol_read_at(v, cluster_off(v, cl), buf, csize) != 0) { printf("read error\n"); rc = -1; break; }
        uint32_t w = remaining < csize ? remaining : csize;
        if (fwrite(buf, 1, w, out) != w) { printf("write error\n"); rc = -1; break; }
        written += w; remaining -= w;
        cl = fat_entry(v->fat, v->L.fat_bits, cl);
    }
    free(buf);
    fclose(out);
    if (rc == 0)
        printf("  %lu bytes -> %s\n", (unsigned long)written, dest);
    return rc;
}

/* Recursively extract a directory (by start cluster) into dest_dir. */
static int extract_tree_depth(fatvol_t *v, uint32_t cluster, const char *dest_dir, int depth) {
    if (depth > 24) { printf("  (too deep, stopping at %s)\n", dest_dir); return -1; }
    mkdir(dest_dir, 0777);
    dirent_t *ents = malloc(sizeof(dirent_t) * 256);
    if (!ents) { printf("out of memory\n"); return -1; }
    int n = cbk_list_dir(v, cluster, 0, ents, 256);
    int rc = 0;
    for (int i = 0; i < n && i >= 0; i++) {
        char child[300];
        snprintf(child, sizeof child, "%s\\%s", dest_dir, ents[i].name);
        if (ents[i].attr & CBK_ATTR_DIR) {
            if (extract_tree_depth(v, ents[i].first_cluster, child, depth + 1) != 0) rc = -1;
        } else {
            if (cbk_extract(v, &ents[i], child) != 0) rc = -1;
        }
    }
    free(ents);
    return rc;
}
int cbk_extract_tree(fatvol_t *v, uint32_t cluster, const char *dest_dir) {
    return extract_tree_depth(v, cluster, dest_dir, 0);
}

/* ----- the `ls` / `get` CLI commands -------------------------------- */

static void list_dir_print(fatvol_t *v, uint32_t first_cluster, int is_fixed_root) {
    static dirent_t ents[512];
    int n = cbk_list_dir(v, first_cluster, is_fixed_root, ents, 512);
    if (n < 0) { printf("read error\n"); return; }
    int files = 0, dirs = 0;
    for (int i = 0; i < n; i++) {
        if (ents[i].attr & CBK_ATTR_DIR) { printf("  <DIR>       %s\n", ents[i].name); dirs++; }
        else { printf("  %10lu  %s\n", (unsigned long)ents[i].size, ents[i].name); files++; }
    }
    printf("  %d file%s, %d dir%s\n", files, files == 1 ? "" : "s", dirs, dirs == 1 ? "" : "s");
}

static int all_digits(const char *s) {
    if (!*s) return 0;
    for (; *s; s++) if (*s < '0' || *s > '9') return 0;
    return 1;
}

/* Resolve an MBR slot -> partition start LBA on a live disk. slot>=0 picks that
 * MBR primary slot; slot<0 picks the first FAT slot (or the whole disk as a
 * superfloppy when there is no partition table). 0 / -1. */
static int live_part_lba(const drive_info_t *di, int drive, int slot, uint64_t *out_lba) {
    uint8_t mbr[512];
    if (read_lba(di, drive, 0, 1, mbr) != 0) { printf("sector 0 read failed\n"); return -1; }
    if (mbr[510] != 0x55 || mbr[511] != 0xAA) {
        if (slot > 0) { printf("no partition table; cannot select slot %d\n", slot); return -1; }
        *out_lba = 0;                          /* superfloppy: FAT BPB at sector 0 */
        return 0;
    }
    for (int i = 0; i < 4; i++) {
        const uint8_t *e = mbr + 446 + i * 16;
        if (e[4] == 0 || rd32(e + 12) == 0) continue;
        if (slot >= 0) { if (i != slot) continue; *out_lba = rd32(e + 8); return 0; }
        if (is_fat_part_type(e[4])) { *out_lba = rd32(e + 8); return 0; }
    }
    if (slot >= 0) printf("slot %d is empty\n", slot);
    else           printf("no FAT partition found on drive 0x%02X\n", drive);
    return -1;
}

/* Open a browse source named on the command line: either a backup folder, or
 * "@HH" for live BIOS drive 0xHH (then `part` is the MBR slot, <0 = first FAT).
 * On a live open *held becomes 1 -- the caller must xfer_free() after closing
 * the volume (the engine borrowed the shared transfer buffer). 0 / -1. */
static int open_browse_src(const char *src, int part, fatvol_t *v, int *held) {
    *held = 0;
    if (src[0] != '@')
        return cbk_open_vol(src, cbk_default_part(src, part), v);

    int drive = (int)strtol(src + 1, NULL, 16);
    if (drive < 0x80 || drive > 0xFF) { printf("bad drive %s (use @80, @81, ...)\n", src); return -1; }
    if (xfer_init() < 0) { printf("DOS memory alloc failed\n"); return -1; }
    *held = 1;
    drive_info_t di;
    drive_params(drive, &di);
    if (!di.present) { printf("drive 0x%02X not present\n", drive); xfer_free(); *held = 0; return -1; }
    di.ext = drive_has_ext(drive);
    uint64_t plba;
    if (live_part_lba(&di, drive, part, &plba) != 0 ||
        cbk_open_vol_live(&di, drive, plba, v) != 0) { xfer_free(); *held = 0; return -1; }
    return 0;
}

int cmd_ls(int argc, char **argv) {
    setvbuf(stdout, NULL, _IONBF, 0);
    if (argc < 2) {
        printf("usage: CRUSTYBK ls <src> [N] [path]\n");
        printf("  <src>  a backup folder, or @HH for live BIOS drive 0xHH\n");
        printf("  N      partition / MBR slot (default: first FAT)\n");
        return 2;
    }
    const char *src = argv[1];
    int part = -1;
    const char *path = "";
    for (int i = 2; i < argc; i++) {
        if (part < 0 && all_digits(argv[i])) part = atoi(argv[i]);
        else path = argv[i];
    }

    fatvol_t v;
    int held;
    if (open_browse_src(src, part, &v, &held) != 0) return 1;
    printf("FAT%d, %lu KiB\n", v.L.fat_bits,
           (unsigned long)((uint64_t)v.L.old_total * v.L.bps / 1024));

    int rc = 0;
    if (!*path) {
        list_dir_print(&v, v.root_cluster, !v.L.is_fat32);
    } else {
        dirent_t d; int is_dir;
        if (resolve_path(&v, path, &d, &is_dir) != 0) rc = 1;
        else if (is_dir) list_dir_print(&v, d.first_cluster, 0);
        else printf("  %10lu  %s\n", (unsigned long)d.size, d.name);
    }
    cbk_close_vol(&v);
    if (held) xfer_free();
    return rc;
}

int cmd_get(int argc, char **argv) {
    setvbuf(stdout, NULL, _IONBF, 0);
    if (argc < 4) {
        printf("usage: CRUSTYBK get <src> [N] <path> <dest-file>\n");
        printf("  <src>  a backup folder, or @HH for live BIOS drive 0xHH\n");
        printf("  extract one file (partition / MBR slot N) to a DOS path\n");
        return 2;
    }
    const char *src = argv[1];
    int part = -1, ai = 2;
    if (all_digits(argv[ai]) && argc >= 5) part = atoi(argv[ai++]);
    if (ai + 1 >= argc) { printf("need <path> <dest-file>\n"); return 2; }
    const char *path = argv[ai++];
    const char *dest = argv[ai++];

    fatvol_t v;
    int held;
    if (open_browse_src(src, part, &v, &held) != 0) return 1;

    int rc = 0;
    dirent_t d; int is_dir;
    if (resolve_path(&v, path, &d, &is_dir) != 0) rc = 1;
    else if (is_dir) { printf("%s is a directory; use the TUI browse to extract a folder\n", path); rc = 1; }
    else if (cbk_extract(&v, &d, dest) != 0) rc = 1;
    else printf("extracted %s\n", dest);
    cbk_close_vol(&v);
    if (held) xfer_free();
    return rc;
}
