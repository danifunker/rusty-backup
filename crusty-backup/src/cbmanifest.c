/* cbmanifest.c -- build a FAT partition's file manifest (Phase 7f).
 *
 * Walks the live FAT volume read-only (reusing the cbbrowse reader) and emits a
 * deterministic JSON document: a `system` boot-fingerprint block followed by a
 * flat `files` array (every file and directory, depth-first in on-disk order).
 * Deterministic because the on-disk directory order is identical between the
 * source and a block-level restore of it -- which is what makes the manifest a
 * byte-stable backup->restore->backup idempotency check. See cbmanifest.h. */

#include "cbmanifest.h"
#include "cbbrowse.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <zlib.h>

/* DOS system files we fingerprint in the root (the SYS-command set). */
static const char *SYSFILE_NAMES[] = {
    "IO.SYS", "MSDOS.SYS", "COMMAND.COM", "IBMBIO.COM", "IBMDOS.COM", "KERNEL.SYS"
};
#define N_SYSFILE_NAMES ((int)(sizeof SYSFILE_NAMES / sizeof SYSFILE_NAMES[0]))

#define WALK_DIR_MAX   512   /* entries read per directory (warned if exceeded) */
#define WALK_DEPTH_MAX  24   /* matches the extract recursion guard */

/* ---- a grow-on-demand byte buffer (DJGPP has a flat 32-bit heap) ---------- */
typedef struct { char *p; uint32_t len, cap; int err; } mbuf_t;

static void mb_need(mbuf_t *m, uint32_t extra) {
    if (m->err) return;
    if (m->len + extra + 1 <= m->cap) return;
    uint32_t nc = m->cap ? m->cap : 8192;
    while (m->len + extra + 1 > nc) nc *= 2;
    char *np = realloc(m->p, nc);
    if (!np) { m->err = 1; return; }
    m->p = np; m->cap = nc;
}
static void mb_puts(mbuf_t *m, const char *s) {
    uint32_t n = (uint32_t)strlen(s);
    mb_need(m, n);
    if (m->err) return;
    memcpy(m->p + m->len, s, n);
    m->len += n;
}
static void mb_printf(mbuf_t *m, const char *fmt, ...) {
    char tmp[160];                 /* numeric fields only -- never the path */
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(tmp, sizeof tmp, fmt, ap);
    va_end(ap);
    if (n < 0) { m->err = 1; return; }
    if (n >= (int)sizeof tmp) n = (int)sizeof tmp - 1;
    mb_need(m, (uint32_t)n);
    if (m->err) return;
    memcpy(m->p + m->len, tmp, (uint32_t)n);
    m->len += (uint32_t)n;
}
/* Emit `s` as a JSON string literal (escapes \, ", and control chars). Handles
 * any length, so paths -- which use DOS '\' separators -- never truncate. */
static void mb_json_str(mbuf_t *m, const char *s) {
    mb_puts(m, "\"");
    for (; *s; s++) {
        unsigned char c = (unsigned char)*s;
        if (c == '"' || c == '\\') { char e[3] = {'\\', (char)c, 0}; mb_puts(m, e); }
        else if (c < 0x20)         { mb_printf(m, "\\u%04x", c); }
        else                       { char e[2] = {(char)c, 0}; mb_puts(m, e); }
    }
    mb_puts(m, "\"");
}

/* Decode a DOS-packed date+time pair into "YYYY-MM-DDTHH:MM:SS". */
static void dos_dt_iso(uint16_t date, uint16_t time, char *out) {
    int yr = 1980 + ((date >> 9) & 0x7F);
    int mo = (date >> 5) & 0x0F;
    int dy = date & 0x1F;
    int hh = (time >> 11) & 0x1F;
    int mi = (time >> 5) & 0x3F;
    int ss = (time & 0x1F) * 2;
    sprintf(out, "%04d-%02d-%02dT%02d:%02d:%02d", yr, mo, dy, hh, mi, ss);
}

static uint32_t eoc_threshold(int fat_bits) {
    return (fat_bits == 12) ? 0x0FF8u : (fat_bits == 16) ? 0xFFF8u : 0x0FFFFFF8u;
}

/* Is a file's cluster chain a single contiguous run of the right length? Boot
 * loaders care (the SYS rule pins IO.SYS first + contiguous). Pure FAT walk, no
 * data read. */
static int chain_contiguous(fatvol_t *v, uint32_t first, uint32_t size) {
    uint32_t csize = (uint32_t)v->L.spc * v->L.bps;
    uint32_t need = csize ? (size + csize - 1) / csize : 0;
    if (need == 0) return 1;                 /* zero-length: vacuously contiguous */
    if (first < 2) return 0;
    uint32_t eoc = eoc_threshold(v->L.fat_bits);
    uint32_t cl = first, prev = 0, count = 0;
    while (cl >= 2 && cl < eoc && count <= need) {
        if (count > 0 && cl != prev + 1) return 0;
        prev = cl; count++;
        cl = fat_entry(v->fat, v->L.fat_bits, cl);
    }
    return (count == need);
}

/* Emit one file/dir object. Caller manages the leading comma via *first. */
static void emit_entry(mbuf_t *m, const char *path, const dirent_t *d, int *first) {
    char iso[24];
    dos_dt_iso(d->dos_date, d->dos_time, iso);
    int is_dir = (d->attr & CBK_ATTR_DIR) != 0;
    if (!*first) mb_puts(m, ",");
    *first = 0;
    mb_puts(m, "\n    {\"path\": ");
    mb_json_str(m, path);
    mb_printf(m, ", \"size\": %lu, \"mtime\": \"%s\", \"attr\": %u, \"start_cluster\": %lu",
              (unsigned long)d->size, iso, (unsigned)d->attr,
              (unsigned long)d->first_cluster);
    if (is_dir) mb_puts(m, ", \"dir\": true");
    mb_puts(m, "}");
}

/* Depth-first walk emitting every file + directory under `cluster`. `prefix` is
 * the parent's DOS path (no trailing separator; "" at the root). */
static void walk_dir(fatvol_t *v, uint32_t cluster, int fixed_root,
                     const char *prefix, mbuf_t *m, int *first, int depth) {
    if (depth > WALK_DEPTH_MAX || m->err) return;
    dirent_t *ents = malloc(sizeof(dirent_t) * WALK_DIR_MAX);
    if (!ents) { m->err = 1; return; }
    int n = cbk_list_dir(v, cluster, fixed_root, ents, WALK_DIR_MAX);
    if (n < 0) { free(ents); return; }            /* unreadable dir: best-effort skip */
    if (n == WALK_DIR_MAX)
        printf("  manifest: directory '%s' has >%d entries -- truncated\n",
               prefix[0] ? prefix : "\\", WALK_DIR_MAX);
    for (int i = 0; i < n && !m->err; i++) {
        size_t plen = strlen(prefix) + 1 + strlen(ents[i].name) + 1;
        char *path = malloc(plen);
        if (!path) { m->err = 1; break; }
        sprintf(path, "%s\\%s", prefix, ents[i].name);
        emit_entry(m, path, &ents[i], first);
        if ((ents[i].attr & CBK_ATTR_DIR) && ents[i].first_cluster >= 2)
            walk_dir(v, ents[i].first_cluster, 0, path, m, first, depth + 1);
        free(path);
    }
    free(ents);
}

/* Emit the `system` boot-fingerprint block: the MBR boot code CRC, the
 * partition's reserved-sector CRC (VBR + FSInfo + backup boot), and each DOS
 * system file found in the root (size/mtime/attr/first cluster/contiguity). */
static void emit_system(mbuf_t *m, fatvol_t *v, const drive_info_t *di, int drive,
                        uint64_t start_lba, const uint8_t *mbr) {
    unsigned long mbr_crc = crc32(crc32(0L, Z_NULL, 0), mbr, 446);
    unsigned long boot_crc = 0;
    uint32_t rbytes = (uint32_t)v->L.reserved * v->L.bps;
    uint8_t *rbuf = malloc(rbytes ? rbytes : 1);
    if (rbuf) {
        if (rbytes && load_region(di, drive, start_lba, rbytes, rbuf) == 0)
            boot_crc = crc32(crc32(0L, Z_NULL, 0), rbuf, rbytes);
        free(rbuf);
    }

    mb_printf(m, "  \"system\": {\n    \"mbr_boot_code_crc\": \"%08lx\",\n", mbr_crc);
    mb_printf(m, "    \"boot_sectors_crc\": \"%08lx\",\n", boot_crc);
    mb_puts(m, "    \"sysfiles\": [");

    dirent_t *ents = malloc(sizeof(dirent_t) * WALK_DIR_MAX);
    int first = 1;
    if (ents) {
        int n = cbk_list_dir(v, v->root_cluster, !v->L.is_fat32, ents, WALK_DIR_MAX);
        for (int i = 0; i < n; i++) {
            if (ents[i].attr & CBK_ATTR_DIR) continue;
            int match = 0;
            for (int s = 0; s < N_SYSFILE_NAMES; s++)
                if (eq_ci(ents[i].name, SYSFILE_NAMES[s])) { match = 1; break; }
            if (!match) continue;
            char iso[24];
            dos_dt_iso(ents[i].dos_date, ents[i].dos_time, iso);
            if (!first) mb_puts(m, ",");
            first = 0;
            mb_puts(m, "\n      {\"name\": ");
            mb_json_str(m, ents[i].name);
            mb_printf(m, ", \"size\": %lu, \"mtime\": \"%s\", \"attr\": %u, "
                         "\"first_cluster\": %lu, \"contiguous\": %s}",
                      (unsigned long)ents[i].size, iso, (unsigned)ents[i].attr,
                      (unsigned long)ents[i].first_cluster,
                      chain_contiguous(v, ents[i].first_cluster, ents[i].size) ? "true" : "false");
        }
        free(ents);
    }
    mb_puts(m, first ? "]\n  },\n" : "\n    ]\n  },\n");
}

int manifest_build_fat(const drive_info_t *di, int drive, uint64_t start_lba,
                       const uint8_t *mbr, char **out_buf, uint32_t *out_len) {
    fatvol_t v;
    if (cbk_open_vol_live(di, drive, start_lba, &v) != 0) return -1;

    mbuf_t m;
    memset(&m, 0, sizeof m);
    mb_printf(&m, "{\n  \"manifest_version\": 1,\n  \"filesystem\": \"fat%d\",\n", v.L.fat_bits);
    emit_system(&m, &v, di, drive, start_lba, mbr);
    mb_puts(&m, "  \"files\": [");
    int first = 1;
    walk_dir(&v, v.root_cluster, !v.L.is_fat32, "", &m, &first, 0);
    mb_puts(&m, first ? "]\n}\n" : "\n  ]\n}\n");

    cbk_close_vol(&v);
    if (m.err) { free(m.p); return -1; }
    *out_buf = m.p;
    *out_len = m.len;
    return 0;
}
