/* cbdefrag.c -- file-level FAT defragmentation for `backup /DEFRAG` (see
 * cbdefrag.h). The C analogue of a classic defragmenter, but it never touches
 * the source: it walks the live volume read-only and writes a relocated,
 * compacted partition image straight into the compressed stream (gzip or lz4).
 *
 * Algorithm:
 *   1. Walk the directory tree from the root, following each file's + subdir's
 *      cluster chain, assigning each a contiguous run of new cluster numbers
 *      starting at 2. Boot files (IO.SYS / MSDOS.SYS / IBMBIO.COM / IBMDOS.COM
 *      / KERNEL.SYS) in the root are pinned first so they stay first+contiguous
 *      at the data-area start (the DOS `SYS` rule) and the disk still boots.
 *   2. Build a fresh FAT encoding those contiguous chains.
 *   3. Emit reserved sectors (verbatim; FAT32 BPB root-cluster + FSInfo patched)
 *      -> the new FAT(s) -> the FAT12/16 root region -> the data area, object by
 *      object in new-cluster order. File clusters are copied from the source;
 *      directory clusters are read, their entries' cluster pointers (incl. the
 *      "." / ".." links) rewritten through the relocation map, then written.
 *      LFN entries carry no cluster and ride along verbatim next to their 8.3
 *      entry.
 *
 * It is deliberately conservative: a not-provably-clean filesystem (lost or
 * bad clusters, cross-linked chains, unreadable directories, OOM) makes it
 * decline (return 1) before writing a single gzip byte, and the caller images
 * the volume with the ordinary compaction path instead. */

#include "cbdefrag.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* ----- FAT entry helpers (write side; the read side lives in cbdisk) ----- */

static uint32_t eoc_mark(int bits) {          /* chain-end threshold */
    return bits == 12 ? 0x0FF8u : bits == 16 ? 0xFFF8u : 0x0FFFFFF8u;
}
static uint32_t eoc_val(int bits) {           /* value we write for end-of-chain */
    return bits == 12 ? 0x0FFFu : bits == 16 ? 0xFFFFu : 0x0FFFFFFFu;
}
static int is_bad(const uint8_t *fat, int bits, uint32_t n) {
    uint32_t bad = bits == 12 ? 0x0FF7u : bits == 16 ? 0xFFF7u : 0x0FFFFFF7u;
    return fat_entry(fat, bits, n) == bad;
}
static void fat_set(uint8_t *fat, int bits, uint32_t n, uint32_t v) {
    if (bits == 16) { wr16(fat + n * 2, (uint16_t)v); return; }
    if (bits == 32) {
        uint32_t cur = rd32(fat + n * 4) & 0xF0000000u;       /* keep top 4 bits */
        wr32(fat + n * 4, (v & 0x0FFFFFFFu) | cur);
        return;
    }
    uint32_t off = n + (n / 2);
    uint16_t pair = fat[off] | (fat[off + 1] << 8);
    if (n & 1) pair = (uint16_t)((pair & 0x000F) | ((v & 0x0FFF) << 4));
    else       pair = (uint16_t)((pair & 0xF000) | (v & 0x0FFF));
    fat[off] = pair & 0xFF; fat[off + 1] = (pair >> 8) & 0xFF;
}

/* Reassemble an 8.3 name (no LFN) for boot-file matching. */
static void name_83(const uint8_t *e, char *out) {
    int n = 0;
    for (int i = 0; i < 8; i++) { if (e[i] == ' ') break; out[n++] = (char)e[i]; }
    int hasext = 0;
    for (int i = 8; i < 11; i++) if (e[i] != ' ') { hasext = 1; break; }
    if (hasext) { out[n++] = '.'; for (int i = 8; i < 11; i++) { if (e[i] == ' ') break; out[n++] = (char)e[i]; } }
    out[n] = 0;
}

/* ----- the plan ----------------------------------------------------------- */

typedef struct {
    uint32_t old_first;    /* source first cluster (>= 2) */
    uint32_t new_first;    /* assigned contiguous run start (>= 2) */
    uint32_t ncl;          /* cluster count */
    uint32_t self_new;     /* dirs: "." target = new_first */
    uint32_t parent_new;   /* dirs: ".." target (0 = parent is the fixed root) */
    int      is_dir;
} dfobj_t;

typedef struct {
    const drive_info_t *di;
    int                 drive;
    uint64_t            start_lba;
    const fatlay_t     *L;
    uint8_t            *oldfat;   /* borrowed */
    uint32_t           *remap;    /* [clusters+2], 0 = unmapped */
    uint32_t            ncap;     /* clusters + 2 */
    dfobj_t            *objs;
    int                 nobjs, objcap;
    uint32_t            next_cluster;  /* next new cluster to hand out */
    uint32_t            total_alloc;   /* clusters allocated so far */
    int                 failed;
    const char         *reason;
} plan_t;

/* Read a directory's whole region (sector-aligned) from the live source disk.
 * fixed_root: the FAT12/16 fixed root region; else first_cluster's chain. The
 * returned buffer is malloc'd; *out_bytes is its length. NULL on read/OOM. */
static uint8_t *read_dir_region(plan_t *P, uint32_t first_cluster, int fixed_root,
                                uint32_t *out_bytes) {
    const fatlay_t *L = P->L;
    if (fixed_root) {
        uint32_t rb = L->root_dir_secs * L->bps;
        uint64_t off = (uint64_t)L->reserved + (uint64_t)L->num_fats * L->old_spf;
        uint8_t *buf = malloc(rb ? rb : 1);
        if (!buf) return NULL;
        if (rb && load_region(P->di, P->drive, P->start_lba + off, rb, buf) != 0) {
            free(buf); return NULL;
        }
        *out_bytes = rb;
        return buf;
    }
    uint32_t csize = L->spc * L->bps;
    uint32_t eoc = eoc_mark(L->fat_bits);
    uint32_t cl = first_cluster, ncl = 0;
    while (cl >= 2 && cl < eoc && ncl < P->ncap) { ncl++; cl = fat_entry(P->oldfat, L->fat_bits, cl); }
    uint32_t rb = ncl * csize;
    uint8_t *buf = malloc(rb ? rb : 1);
    if (!buf) return NULL;
    cl = first_cluster;
    uint32_t pos = 0;
    for (uint32_t i = 0; i < ncl; i++) {
        uint64_t sec = (uint64_t)L->first_data_sec + (uint64_t)(cl - 2) * L->spc;
        if (load_region(P->di, P->drive, P->start_lba + sec, csize, buf + pos) != 0) { free(buf); return NULL; }
        pos += csize;
        cl = fat_entry(P->oldfat, L->fat_bits, cl);
    }
    *out_bytes = rb;
    return buf;
}

static int add_obj(plan_t *P, const dfobj_t *o) {
    if (P->nobjs == P->objcap) {
        int nc = P->objcap ? P->objcap * 2 : 64;
        dfobj_t *n = realloc(P->objs, (size_t)nc * sizeof(dfobj_t));
        if (!n) { P->failed = 1; P->reason = "out of memory"; return -1; }
        P->objs = n; P->objcap = nc;
    }
    P->objs[P->nobjs++] = *o;
    return 0;
}

/* Assign a contiguous new-cluster run to the chain starting at old_first and
 * record an object. Returns new_first (>= 2), or 0 if the chain is empty
 * (old_first < 2). Sets P->failed on a cross-link / bad cluster / loop. */
static uint32_t alloc_chain(plan_t *P, uint32_t old_first, int is_dir, uint32_t parent_new) {
    int bits = P->L->fat_bits;
    uint32_t eoc = eoc_mark(bits);
    if (old_first < 2 || old_first >= eoc) {
        if (is_dir) { P->failed = 1; P->reason = "directory with no clusters"; }
        return 0;   /* empty file: nothing to relocate */
    }
    uint32_t new_first = P->next_cluster, cl = old_first, ncl = 0;
    while (cl >= 2 && cl < eoc) {
        if (cl >= P->ncap)              { P->failed = 1; P->reason = "cluster out of range"; return 0; }
        if (is_bad(P->oldfat, bits, cl)) { P->failed = 1; P->reason = "bad cluster in a chain"; return 0; }
        if (P->remap[cl] != 0)          { P->failed = 1; P->reason = "cross-linked clusters"; return 0; }
        P->remap[cl] = P->next_cluster++;
        if (++ncl > P->ncap)            { P->failed = 1; P->reason = "cluster chain loop"; return 0; }
        cl = fat_entry(P->oldfat, bits, cl);
    }
    P->total_alloc += ncl;
    dfobj_t o;
    memset(&o, 0, sizeof o);
    o.old_first = old_first; o.new_first = new_first; o.ncl = ncl;
    o.self_new = new_first; o.parent_new = parent_new; o.is_dir = is_dir;
    if (add_obj(P, &o) != 0) return 0;
    return new_first;
}

#define ATTR_LFN 0x0F
#define ATTR_VOL 0x08
#define ATTR_DIR 0x10

/* Find a root-level FILE entry by 8.3 name; returns its first cluster (0 if
 * absent / a directory / empty). */
static uint32_t find_root_file(const uint8_t *dir, uint32_t len, const char *name) {
    for (uint32_t off = 0; off + 32 <= len; off += 32) {
        const uint8_t *e = dir + off;
        if (e[0] == 0x00) break;
        if (e[0] == 0xE5) continue;
        uint8_t attr = e[11];
        if ((attr & ATTR_LFN) == ATTR_LFN || (attr & ATTR_VOL) || (attr & ATTR_DIR)) continue;
        char nm[16];
        name_83(e, nm);
        if (eq_ci(nm, name))
            return ((uint32_t)rd16(e + 20) << 16) | rd16(e + 26);
    }
    return 0;
}

/* Recursively walk a directory, allocating new clusters for every file + subdir
 * reachable from it. this_dir_new is the new cluster of the directory being
 * walked (the ".." target for its child subdirs; 0 for the fixed root). */
static void walk_dir(plan_t *P, const uint8_t *dir, uint32_t len, uint32_t this_dir_new, int depth) {
    if (P->failed) return;
    if (depth > 64) { P->failed = 1; P->reason = "directory nesting too deep"; return; }
    for (uint32_t off = 0; off + 32 <= len; off += 32) {
        const uint8_t *e = dir + off;
        if (e[0] == 0x00) break;
        if (e[0] == 0xE5) continue;
        uint8_t attr = e[11];
        if ((attr & ATTR_LFN) == ATTR_LFN || (attr & ATTR_VOL)) continue;
        if (e[0] == '.') continue;          /* "." / ".." */
        uint32_t oc = ((uint32_t)rd16(e + 20) << 16) | rd16(e + 26);
        if (attr & ATTR_DIR) {
            if (oc < 2) continue;
            if (P->remap[oc] != 0) { P->failed = 1; P->reason = "directory referenced twice"; return; }
            uint32_t dnew = alloc_chain(P, oc, 1, this_dir_new);
            if (P->failed) return;
            uint32_t sl;
            uint8_t *sub = read_dir_region(P, oc, 0, &sl);
            if (!sub) { P->failed = 1; P->reason = "directory read failed"; return; }
            walk_dir(P, sub, sl, dnew, depth + 1);
            free(sub);
            if (P->failed) return;
        } else if (oc >= 2 && P->remap[oc] == 0) {
            alloc_chain(P, oc, 0, 0);        /* a file (boot files already pinned) */
            if (P->failed) return;
        }
    }
}

/* Rewrite a directory's entry cluster pointers through the relocation map.
 * self != NULL for a subdir (its "." / ".." links get the special targets);
 * NULL for the root, which has none. */
static void patch_dir(plan_t *P, uint8_t *dir, uint32_t len, const dfobj_t *self) {
    for (uint32_t off = 0; off + 32 <= len; off += 32) {
        uint8_t *e = dir + off;
        if (e[0] == 0x00) break;
        if (e[0] == 0xE5) continue;
        uint8_t attr = e[11];
        if ((attr & ATTR_LFN) == ATTR_LFN || (attr & ATTR_VOL)) continue;
        uint32_t oc = ((uint32_t)rd16(e + 20) << 16) | rd16(e + 26);
        uint32_t nc;
        if (self && e[0] == '.' && e[1] == '.')      nc = self->parent_new;
        else if (self && e[0] == '.')                nc = self->self_new;
        else if (oc >= 2 && oc < P->ncap)            nc = P->remap[oc];
        else                                         nc = oc;     /* empty / out of range */
        wr16(e + 20, (uint16_t)(nc >> 16));
        wr16(e + 26, (uint16_t)(nc & 0xFFFF));
    }
}

/* ----- emit --------------------------------------------------------------- */

/* The relocated image is emitted as a sequential byte stream through a sink, so
 * the same code serves both `backup` (sink = the compressed writer) and `clone`
 * (sink = the target disk). Every chunk handed to a sink is a 512-byte multiple
 * (reserved/FAT/root/cluster regions all are), so the disk sink writes directly. */
typedef int (*defrag_sink_fn)(void *ctx, const uint8_t *data, uint32_t len);

/* backup sink: the compressed (gzip/lz4) writer. */
static int cbw_sink(void *ctx, const uint8_t *d, uint32_t len) {
    return cbw_write((cbw_t *)ctx, d, (int)len);
}

/* clone sink: sequential sectors to the target disk. */
typedef struct { const drive_info_t *di; int drive; uint64_t lba; } disk_sink_t;
static int disk_sink(void *ctx, const uint8_t *d, uint32_t len) {
    disk_sink_t *s = (disk_sink_t *)ctx;
    if (store_region(s->di, s->drive, s->lba, len, d) != 0) return -1;
    s->lba += len / 512;
    return 0;
}

static int emit_bytes(defrag_sink_fn sink, void *ctx, const uint8_t *p, uint32_t len,
                      progress_t *pr, uint64_t *emitted) {
    uint32_t off = 0;
    while (off < len) {
        uint32_t chunk = len - off;
        if (chunk > XFER_BYTES) chunk = XFER_BYTES;
        if (sink(ctx, p + off, chunk) != 0) return -1;
        off += chunk;
        *emitted += chunk;
        progress_update(pr, *emitted);
    }
    return 0;
}

static int defrag_emit(plan_t *P, defrag_sink_fn sink, void *ctx, const uint8_t *newfat,
                       uint32_t root_new, uint32_t root_cluster, uint32_t imaged_secs,
                       const char *label, progress_t *pr) {
    const fatlay_t *L = P->L;
    uint64_t emitted = 0;
    progress_begin(pr, label, (uint64_t)imaged_secs * L->bps);

    /* 1. reserved sectors (boot, FSInfo, backup boot). FAT32: repoint the BPB
     *    root cluster at its relocated home + invalidate the FSInfo free count. */
    {
        uint32_t rb = L->reserved * L->bps;
        uint8_t *res = malloc(rb);
        if (!res) return -1;
        if (load_region(P->di, P->drive, P->start_lba, rb, res) != 0) { free(res); return -1; }
        if (L->is_fat32) {
            wr32(res + 44, root_new);
            if (L->reserved > 6) wr32(res + 6 * L->bps + 44, root_new);
            uint32_t fsinfo = rd16(res + 48);
            if (fsinfo > 0 && fsinfo < L->reserved) {
                uint8_t *fi = res + (uint32_t)fsinfo * L->bps;
                if (rd32(fi) == 0x41615252u && rd32(fi + 484) == 0x61417272u) {
                    wr32(fi + 488, 0xFFFFFFFFu);     /* free count = unknown */
                    wr32(fi + 492, 0xFFFFFFFFu);     /* next free   = unknown */
                }
            }
        }
        int rc = emit_bytes(sink, ctx, res, rb, pr, &emitted);
        free(res);
        if (rc != 0) return -1;
        (void)root_cluster;
    }

    /* 2. the new FAT, num_fats copies. */
    for (uint32_t c = 0; c < L->num_fats; c++)
        if (emit_bytes(sink, ctx, newfat, L->old_spf * L->bps, pr, &emitted) != 0) return -1;

    /* 3. FAT12/16 fixed root region (FAT32's root is a data-area object). */
    if (!L->is_fat32) {
        uint32_t rl;
        uint8_t *root = read_dir_region(P, 0, 1, &rl);
        if (!root) return -1;
        patch_dir(P, root, rl, NULL);
        int rc = emit_bytes(sink, ctx, root, rl, pr, &emitted);
        free(root);
        if (rc != 0) return -1;
    }

    /* 4. the data area, object by object in new-cluster order. */
    uint32_t csize = L->spc * L->bps;
    int bits = L->fat_bits;
    uint32_t eoc = eoc_mark(bits);
    uint8_t *cb = malloc(csize);
    if (!cb) return -1;
    for (int i = 0; i < P->nobjs; i++) {
        dfobj_t *o = &P->objs[i];
        if (o->is_dir) {
            uint32_t dl;
            uint8_t *d = read_dir_region(P, o->old_first, 0, &dl);
            if (!d) { free(cb); return -1; }
            patch_dir(P, d, dl, o);
            int rc = emit_bytes(sink, ctx, d, dl, pr, &emitted);
            free(d);
            if (rc != 0) { free(cb); return -1; }
        } else {
            uint32_t cl = o->old_first;
            for (uint32_t k = 0; k < o->ncl && cl >= 2 && cl < eoc; k++) {
                uint64_t sec = (uint64_t)L->first_data_sec + (uint64_t)(cl - 2) * L->spc;
                if (load_region(P->di, P->drive, P->start_lba + sec, csize, cb) != 0) { free(cb); return -1; }
                if (emit_bytes(sink, ctx, cb, csize, pr, &emitted) != 0) { free(cb); return -1; }
                cl = fat_entry(P->oldfat, bits, cl);
            }
        }
    }
    free(cb);
    progress_finish(pr);
    return 0;
}

/* ----- plan build (shared by backup + clone) ------------------------------ */

static const char *BOOT_FILES[] = { "IO.SYS", "MSDOS.SYS", "IBMBIO.COM", "IBMDOS.COM", "KERNEL.SYS" };

/* Build the relocation plan + new FAT for the FAT volume `P` points at (read-only
 * on P->di/drive/start_lba). On success (0), P->objs / P->remap and *newfat_out
 * are allocated for the caller to free after emit, and the root cluster, its new
 * home, and the imaged sector count come back via the out-params. On decline (1)
 * everything is freed and the reason printed; the caller images the volume as-is. */
static int defrag_plan_build(plan_t *P, uint32_t *root_cluster_out, uint32_t *root_new_out,
                             uint8_t **newfat_out, uint32_t *imaged_secs_out) {
    const fatlay_t *L = P->L;
    P->ncap = L->clusters + 2; P->next_cluster = 2;
    P->remap = calloc(P->ncap, sizeof(uint32_t));
    if (!P->remap) { printf("  defrag: out of memory -- imaging as-is\n"); return 1; }

    uint8_t bpb[512];
    uint32_t root_cluster = 0;
    if (read_lba(P->di, P->drive, P->start_lba, 1, bpb) == 0 && L->is_fat32)
        root_cluster = rd32(bpb + 44);

    uint32_t rootlen = 0;
    uint8_t *root = read_dir_region(P, root_cluster, !L->is_fat32, &rootlen);
    if (!root) {
        printf("  defrag declined: root read failed -- imaging as-is\n");
        free(P->remap); P->remap = NULL; return 1;
    }

    uint32_t root_new = 0;
    if (L->is_fat32) {
        root_new = alloc_chain(P, root_cluster, 1, 0);
        if (P->failed) goto decline;
    }
    for (unsigned i = 0; i < sizeof BOOT_FILES / sizeof BOOT_FILES[0]; i++) {
        uint32_t fc = find_root_file(root, rootlen, BOOT_FILES[i]);
        if (fc >= 2 && P->remap[fc] == 0) {
            alloc_chain(P, fc, 0, 0);
            if (P->failed) goto decline;
        }
    }
    walk_dir(P, root, rootlen, L->is_fat32 ? root_new : 0, 0);
    if (P->failed) goto decline;
    free(root); root = NULL;

    /* Provably clean? Any allocated-but-unreached cluster, or any bad cluster,
     * means the FS is not in a state we can safely repack -- decline. */
    for (uint32_t c = 2; c < L->clusters + 2; c++) {
        if (is_bad(P->oldfat, L->fat_bits, c)) { P->reason = "bad clusters present"; P->failed = 1; break; }
        if (fat_entry(P->oldfat, L->fat_bits, c) != 0 && P->remap[c] == 0) {
            P->reason = "lost clusters present -- run CHKDSK first"; P->failed = 1; break;
        }
    }
    if (P->failed) goto decline;

    /* Build the new FAT: reserved entries 0/1 verbatim, then a contiguous chain
     * per object. */
    uint8_t *newfat = calloc(L->old_spf * L->bps, 1);
    if (!newfat) { P->reason = "out of memory (FAT)"; goto decline; }
    memcpy(newfat, P->oldfat, (L->fat_bits == 12) ? 3 : (L->fat_bits == 16) ? 4 : 8);
    for (int i = 0; i < P->nobjs; i++) {
        dfobj_t *o = &P->objs[i];
        for (uint32_t k = 0; k < o->ncl; k++) {
            uint32_t c = o->new_first + k;
            fat_set(newfat, L->fat_bits, c, (k + 1 < o->ncl) ? (c + 1) : eoc_val(L->fat_bits));
        }
    }

    uint32_t imaged_secs = L->first_data_sec + P->total_alloc * L->spc;
    if (imaged_secs > L->old_total) imaged_secs = L->old_total;

    *root_cluster_out = root_cluster;
    *root_new_out = root_new;
    *newfat_out = newfat;
    *imaged_secs_out = imaged_secs;
    return 0;

decline:
    if (root) free(root);
    printf("  defrag declined: %s -- imaging as-is\n", P->reason ? P->reason : "unknown");
    free(P->objs); free(P->remap); P->objs = NULL; P->remap = NULL;
    return 1;
}

/* ----- public entry points ------------------------------------------------ */

int defrag_backup_fat(const drive_info_t *di, int drive, uint64_t start_lba,
                      const fatlay_t *L, uint8_t *fat, cbw_t *w,
                      const char *label, progress_t *pr, uint32_t *out_imaged_secs) {
    plan_t P;
    memset(&P, 0, sizeof P);
    P.di = di; P.drive = drive; P.start_lba = start_lba; P.L = L; P.oldfat = fat;

    uint32_t root_cluster, root_new, imaged_secs;
    uint8_t *newfat = NULL;
    if (defrag_plan_build(&P, &root_cluster, &root_new, &newfat, &imaged_secs) != 0)
        return 1;   /* declined -- caller images as-is */

    int rc = defrag_emit(&P, cbw_sink, w, newfat, root_new, root_cluster, imaged_secs, label, pr);
    free(newfat); free(P.objs); free(P.remap);
    if (rc != 0) return -1;

    printf("  defrag: %d objects, %lu used clusters -> imaged %lu KiB\n",
           P.nobjs, (unsigned long)P.total_alloc,
           (unsigned long)((uint64_t)imaged_secs * L->bps / 1024));
    *out_imaged_secs = imaged_secs;
    return 0;
}

int defrag_clone_fat(const drive_info_t *sdi, int sdrive, uint64_t start_lba,
                     const fatlay_t *L, uint8_t *fat,
                     const drive_info_t *tdi, int tdrive, uint64_t win_sectors,
                     const char *label, progress_t *pr, uint32_t *out_imaged_secs) {
    plan_t P;
    memset(&P, 0, sizeof P);
    P.di = sdi; P.drive = sdrive; P.start_lba = start_lba; P.L = L; P.oldfat = fat;

    uint32_t root_cluster, root_new, imaged_secs;
    uint8_t *newfat = NULL;
    if (defrag_plan_build(&P, &root_cluster, &root_new, &newfat, &imaged_secs) != 0)
        return 1;   /* declined -- caller clones as-is */

    /* Emit the relocated image straight to the target at the same start_lba
     * (defrag clone is same-size, so the MBR window is unchanged). */
    disk_sink_t ds = { tdi, tdrive, start_lba };
    int rc = defrag_emit(&P, disk_sink, &ds, newfat, root_new, root_cluster, imaged_secs, label, pr);
    free(newfat); free(P.objs); free(P.remap);
    if (rc != 0) return -1;

    /* Zero the free tail of the target window [imaged_secs, win_sectors). */
    if (win_sectors > imaged_secs) {
        uint8_t z[XFER_BYTES];
        memset(z, 0, sizeof z);
        uint64_t s = imaged_secs;
        while (s < win_sectors) {
            uint32_t n = (win_sectors - s > XFER_SECTORS) ? XFER_SECTORS : (uint32_t)(win_sectors - s);
            if (store_region(tdi, tdrive, start_lba + s, n * 512, z) != 0) return -1;
            s += n;
        }
    }

    printf("  defrag: %d objects, %lu used clusters -> %lu KiB (same-size clone)\n",
           P.nobjs, (unsigned long)P.total_alloc,
           (unsigned long)((uint64_t)imaged_secs * L->bps / 1024));
    *out_imaged_secs = imaged_secs;
    return 0;
}
