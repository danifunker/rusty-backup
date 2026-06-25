/* cmd_browse.c -- the `ls` / `get` commands: browse + extract single files from
 * a backup, no full restore (Phase 4c).
 *
 * A FAT12/16/32 directory reader + file extractor that works straight out of a
 * compacted `partition-N.gz` -- no scratch space and no materialize. zlib's
 * gzseek gives random access into the compressed partition (each seek decompresses
 * from the start; backward seeks rewind, so it's O(offset) -- fine for grabbing a
 * file, the chunked .cbk lazy reader is the eventual fast path). The whole first
 * FAT is loaded once into RAM; directories + file data are read on demand.
 *
 *   CRUSTYBK ls  <folder> [N] [path]          list a directory in partition N
 *   CRUSTYBK get <folder> [N] <path> <dest>   extract one file to a DOS path
 *
 * N is the partition index (the N in partition-N.gz / the metadata "index",
 * 0-based, matching /PARTS); it defaults to the first partition-N.gz present.
 * path uses '\' or '/' separators; a leading separator is optional.
 */

#include "cbdisk.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <zlib.h>

typedef struct {
    gzFile   gz;
    fatlay_t L;
    uint8_t *fat;
    uint32_t fat_bytes;
    uint32_t root_off;       /* byte offset of the FAT12/16 root region */
    uint32_t root_bytes;
    uint32_t root_cluster;   /* FAT32 root start cluster */
} fatvol_t;

typedef struct {
    char     name[256];
    uint8_t  attr;
    uint32_t first_cluster;
    uint32_t size;
} dirent_t;

#define ATTR_DIR  0x10
#define ATTR_VOL  0x08
#define ATTR_LFN  0x0F

/* Random read at an absolute partition byte offset, via gzseek. 0 / -1. */
static int gz_read_at(fatvol_t *v, uint64_t off, void *buf, uint32_t len) {
    if (gzseek(v->gz, (z_off_t)off, SEEK_SET) < 0) return -1;
    int n = gzread(v->gz, buf, len);
    return (n == (int)len) ? 0 : -1;
}

static uint64_t cluster_off(const fatvol_t *v, uint32_t cl) {
    return ((uint64_t)v->L.first_data_sec + (uint64_t)(cl - 2) * v->L.spc) * v->L.bps;
}
static uint32_t eoc_mark(int fat_bits) {
    return (fat_bits == 12) ? 0x0FF8u : (fat_bits == 16) ? 0xFFF8u : 0x0FFFFFF8u;
}

static int open_vol(const char *folder, int part, fatvol_t *v) {
    char path[200];
    sprintf(path, "%s\\partition-%d.gz", folder, part);
    memset(v, 0, sizeof *v);
    v->gz = gzopen(path, "rb");
    if (!v->gz) { printf("cannot open %s\n", path); return -1; }
    uint8_t bpb[512];
    if (gzread(v->gz, bpb, 512) != 512) { printf("short read on %s\n", path); gzclose(v->gz); return -1; }
    parse_fatlay(bpb, &v->L);
    if (!v->L.ok) { printf("partition %d is not a FAT volume\n", part); gzclose(v->gz); return -1; }
    if (v->L.bps != 512) { printf("only 512-byte sectors supported\n"); gzclose(v->gz); return -1; }
    v->fat_bytes = v->L.old_spf * v->L.bps;
    v->fat = malloc(v->fat_bytes);
    if (!v->fat) { printf("out of memory\n"); gzclose(v->gz); return -1; }
    if (gz_read_at(v, (uint64_t)v->L.reserved * v->L.bps, v->fat, v->fat_bytes) != 0) {
        printf("FAT read failed\n"); free(v->fat); gzclose(v->gz); return -1;
    }
    v->root_off = (uint32_t)((uint64_t)(v->L.reserved + v->L.num_fats * v->L.old_spf) * v->L.bps);
    v->root_bytes = v->L.root_entries * 32;
    v->root_cluster = v->L.is_fat32 ? rd32(bpb + 44) : 0;
    return 0;
}
static void close_vol(fatvol_t *v) {
    if (v->fat) free(v->fat);
    if (v->gz) gzclose(v->gz);
}

/* Read all bytes of a directory (root or a subdir cluster chain) into a malloc'd
 * buffer. is_fixed_root selects the FAT12/16 root region. Caller frees. */
static uint8_t *read_dir_bytes(fatvol_t *v, uint32_t first_cluster, int is_fixed_root, uint32_t *out_len) {
    if (is_fixed_root) {
        uint8_t *buf = malloc(v->root_bytes ? v->root_bytes : 1);
        if (!buf) return NULL;
        if (v->root_bytes && gz_read_at(v, v->root_off, buf, v->root_bytes) != 0) { free(buf); return NULL; }
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
        if (gz_read_at(v, cluster_off(v, cl), buf + pos, csize) != 0) { free(buf); return NULL; }
        pos += csize;
        cl = fat_entry(v->fat, v->L.fat_bits, cl);
    }
    *out_len = pos;
    return buf;
}

/* Build a printable name from an 8.3 directory entry. */
static void name_83(const uint8_t *e, char *out) {
    int n = 0;
    for (int i = 0; i < 8; i++) { if (e[i] == ' ') break; out[n++] = (char)e[i]; }
    int hasext = 0;
    for (int i = 8; i < 11; i++) if (e[i] != ' ') { hasext = 1; break; }
    if (hasext) { out[n++] = '.'; for (int i = 8; i < 11; i++) { if (e[i] == ' ') break; out[n++] = (char)e[i]; } }
    out[n] = 0;
}

/* Directory iterator: yields each real (non-LFN, non-deleted, non-volume) entry,
 * reassembling its long name from the preceding LFN records. Returns 1 per entry
 * (filling *d), 0 at end of directory. */
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

/* Find a child entry by name (case-insensitive). 1 + fills *out, or 0. */
static int dir_find(const uint8_t *buf, uint32_t len, const char *name, dirent_t *out) {
    dir_iter_t it;
    dir_iter_init(&it, buf, len);
    dirent_t d;
    while (dir_iter_next(&it, &d))
        if (eq_ci(d.name, name)) { *out = d; return 1; }
    return 0;
}

/* Descend `path` from the root. Fills *out with the final entry; sets *is_dir.
 * Empty path resolves to the root directory (is_dir=1, first_cluster=root). */
static int resolve_path(fatvol_t *v, const char *path, dirent_t *out, int *is_dir) {
    uint32_t cur_cluster = v->root_cluster;   /* meaningful for FAT32 root / subdirs */
    int cur_is_fixed_root = !v->L.is_fat32;
    memset(out, 0, sizeof *out);
    out->attr = ATTR_DIR;
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
        *is_dir = (found.attr & ATTR_DIR) != 0;
        cur_cluster = found.first_cluster;
        cur_is_fixed_root = 0;   /* only the top-level FAT12/16 root is fixed */
    }
    return 0;
}

static void list_dir(fatvol_t *v, uint32_t first_cluster, int is_fixed_root) {
    uint32_t len;
    uint8_t *dir = read_dir_bytes(v, first_cluster, is_fixed_root, &len);
    if (!dir) { printf("read error\n"); return; }
    dir_iter_t it;
    dir_iter_init(&it, dir, len);
    dirent_t d;
    int files = 0, dirs = 0;
    while (dir_iter_next(&it, &d)) {
        if (strcmp(d.name, ".") == 0 || strcmp(d.name, "..") == 0) continue;
        if (d.attr & ATTR_DIR) { printf("  <DIR>       %s\n", d.name); dirs++; }
        else { printf("  %10lu  %s\n", (unsigned long)d.size, d.name); files++; }
    }
    free(dir);
    printf("  %d file%s, %d dir%s\n", files, files == 1 ? "" : "s", dirs, dirs == 1 ? "" : "s");
}

static int extract_file(fatvol_t *v, const dirent_t *f, const char *dest) {
    FILE *out = fopen(dest, "wb");
    if (!out) { printf("cannot create %s\n", dest); return -1; }
    uint32_t csize = v->L.spc * v->L.bps;
    uint8_t *buf = malloc(csize);
    if (!buf) { printf("out of memory\n"); fclose(out); return -1; }
    uint32_t eoc = eoc_mark(v->L.fat_bits);
    uint32_t cl = f->first_cluster, remaining = f->size, written = 0;
    int rc = 0;
    while (remaining > 0 && cl >= 2 && cl < eoc) {
        if (gz_read_at(v, cluster_off(v, cl), buf, csize) != 0) { printf("read error\n"); rc = -1; break; }
        uint32_t w = remaining < csize ? remaining : csize;
        if (fwrite(buf, 1, w, out) != w) { printf("write error\n"); rc = -1; break; }
        written += w; remaining -= w;
        cl = fat_entry(v->fat, v->L.fat_bits, cl);
    }
    free(buf);
    fclose(out);
    if (rc == 0)
        printf("extracted %lu bytes -> %s\n", (unsigned long)written, dest);
    return rc;
}

/* Pick the partition: explicit `part` (>=0), else the first partition-N.gz present. */
static int default_part(const char *folder, int part) {
    if (part >= 0) return part;
    for (int n = 0; n < 16; n++) {
        char path[200];
        sprintf(path, "%s\\partition-%d.gz", folder, n);
        FILE *f = fopen(path, "rb");
        if (f) { fclose(f); return n; }
    }
    return 0;
}

static int all_digits(const char *s) {
    if (!*s) return 0;
    for (; *s; s++) if (*s < '0' || *s > '9') return 0;
    return 1;
}

int cmd_ls(int argc, char **argv) {
    setvbuf(stdout, NULL, _IONBF, 0);
    if (argc < 2) {
        printf("usage: CRUSTYBK ls <folder> [N] [path]\n");
        printf("  list a directory inside partition N of a backup folder\n");
        return 2;
    }
    const char *folder = argv[1];
    int part = -1;
    const char *path = "";
    for (int i = 2; i < argc; i++) {
        if (part < 0 && all_digits(argv[i])) part = atoi(argv[i]);
        else path = argv[i];
    }
    part = default_part(folder, part);

    fatvol_t v;
    if (open_vol(folder, part, &v) != 0) return 1;
    printf("partition %d: FAT%d, %lu KiB imaged\n", part, v.L.fat_bits,
           (unsigned long)((uint64_t)v.L.old_total * v.L.bps / 1024));

    int rc = 0;
    if (!*path) {
        list_dir(&v, v.root_cluster, !v.L.is_fat32);
    } else {
        dirent_t d; int is_dir;
        if (resolve_path(&v, path, &d, &is_dir) != 0) { rc = 1; }
        else if (is_dir) list_dir(&v, d.first_cluster, 0);
        else printf("  %10lu  %s\n", (unsigned long)d.size, d.name);
    }
    close_vol(&v);
    return rc;
}

int cmd_get(int argc, char **argv) {
    setvbuf(stdout, NULL, _IONBF, 0);
    if (argc < 4) {
        printf("usage: CRUSTYBK get <folder> [N] <path> <dest-file>\n");
        printf("  extract one file from partition N of a backup folder to a DOS path\n");
        return 2;
    }
    const char *folder = argv[1];
    int part = -1, ai = 2;
    if (all_digits(argv[ai]) && argc >= 5) part = atoi(argv[ai++]);
    if (ai + 1 >= argc) { printf("need <path> <dest-file>\n"); return 2; }
    const char *path = argv[ai++];
    const char *dest = argv[ai++];
    part = default_part(folder, part);

    fatvol_t v;
    if (open_vol(folder, part, &v) != 0) return 1;

    int rc = 0;
    dirent_t d; int is_dir;
    if (resolve_path(&v, path, &d, &is_dir) != 0) { rc = 1; }
    else if (is_dir) { printf("%s is a directory, not a file\n", path); rc = 1; }
    else if (extract_file(&v, &d, dest) != 0) { rc = 1; }
    close_vol(&v);
    return rc;
}
