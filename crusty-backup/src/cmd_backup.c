/* cmd_backup.c -- the `backup` command (Phase 2 + selective + NTFS).
 *
 * Images a disk read via int13h to the desktop's native PerPartition folder
 * (metadata.json + mbr.bin + partition-N.gz). Each partition is smart-compacted
 * (free clusters zeroed pre-gzip) and streamed through zlib gzwrite: FAT from its
 * FAT (image up to the last used cluster), NTFS from its $Bitmap (full window,
 * free clusters zeroed). The disk/FAT engine lives in cbdisk, the NTFS $Bitmap
 * reader in cbntfs. */

#include "cbdisk.h"
#include "cbntfs.h"
#include "cbdefrag.h"
#include "cbcodec.h"
#include "cbnet.h"
#include "cbmanifest.h"
#include "cbswap.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <time.h>
#include <zlib.h>

static const char *part_type_name(uint8_t t) {
    switch (t) {
        case 0x01: return "FAT12";
        case 0x04: return "FAT16 (<32MB)";
        case 0x06: return "FAT16 (>32MB)";
        case 0x07: return "NTFS";
        case 0x0B: return "FAT32 (CHS)";
        case 0x0C: return "FAT32 (LBA)";
        case 0x0E: return "FAT16 (LBA)";
        default:   return "FAT";
    }
}

typedef struct {
    int      index;
    uint8_t  type_byte;
    uint64_t start_lba;
    uint64_t original_size;   /* partition window bytes (MBR count*512) */
    uint64_t imaged_size;     /* bytes actually imaged */
    uint64_t minimum_size;    /* == imaged_size here */
    int      compacted;
    int      is_logical;      /* inside an extended container (EBR chain) */
    char     gz_name[24];
    char     crc_hex[9];
} part_meta_t;

/* The MBR extended-partition container (if any), for the metadata sidecar so
 * restore can rebuild the EBR chain. */
typedef struct {
    int      found;
    int      mbr_index;       /* primary slot 0-3 holding the container */
    uint8_t  type_byte;       /* 0x05 | 0x0F | 0x85 */
    uint64_t start_lba;
    uint64_t size_bytes;
} ext_meta_t;

/* CRC32 the finished .gz (the desktop's checksum) and write its .crc32 sidecar. */
static void gz_crc_sidecar(const char *dest, part_meta_t *pm) {
    char path[160];
    sprintf(path, "%s\\%s", dest, pm->gz_name);
    FILE *f = fopen(path, "rb");
    uLong crc = crc32(0L, Z_NULL, 0);
    if (f) {
        uint8_t rb[4096]; size_t got;
        while ((got = fread(rb, 1, sizeof rb, f)) > 0)
            crc = crc32(crc, rb, (uInt)got);
        fclose(f);
    }
    sprintf(pm->crc_hex, "%08lx", (unsigned long)crc);

    char crcpath[180];
    sprintf(crcpath, "%s\\%s.crc32", dest, pm->gz_name);
    FILE *cf = fopen(crcpath, "wb");
    if (cf) { fprintf(cf, "%s", pm->crc_hex); fclose(cf); }
}

/* Build the per-cluster "force zero" bitmap for Level-1 swap exclusion (§6c): a
 * read-only walk of the live volume for allowlisted swap/page files, marking
 * their cluster chains. Returns a malloc'd bitmap (bit i = cluster i; caller
 * frees) when at least one swap file was found, else NULL (no mask: keep_swap,
 * none present, or out of memory -- the imager then only zeros free clusters).
 * Logs every exclusion (never silent, §6d). */
static uint8_t *build_swap_mask(const drive_info_t *di, int drive, uint64_t start_lba,
                                const fatlay_t *L, int index, int keep_swap) {
    if (keep_swap) return NULL;
    fatvol_t v;
    if (cbk_open_vol_live(di, drive, start_lba, &v) != 0) return NULL;
    uint8_t *bm = calloc((L->clusters + 2 + 7) / 8, 1);
    cbswap_found_t found[8];
    int nf = bm ? cbswap_collect(&v, bm, L->clusters, found, 8) : -1;
    cbk_close_vol(&v);
    if (nf <= 0) { free(bm); return NULL; }
    for (int i = 0; i < nf; i++)
        printf("  part %d: excluded %s (%lu KiB, zeroed)\n",
               index, found[i].name, (unsigned long)(found[i].size / 1024));
    return bm;
}

/* True if cluster `cl` is allocated to an excluded swap file (a force-zero hit).
 * `bm` may be NULL (no swap mask) -- always false then. */
static int swap_hit(const uint8_t *bm, uint32_t cl) {
    return bm && (bm[cl >> 3] & (1u << (cl & 7)));
}

/* Smart-compact + gzip a FAT partition into <dest>/partition-N.gz, fill `pm`.
 * When `defrag` is set, the volume's files are first relocated into contiguous
 * runs (boot-file aware) via cbdefrag; if that path declines (an unclean FS, or
 * out of memory) it falls back to the plain last-used-cluster compaction, with
 * the gzip stream untouched up to that point. */
static int backup_fat_partition(const drive_info_t *di, int drive,
                                const char *dest, int index, uint8_t type_byte,
                                uint64_t start_lba, uint64_t part_sectors,
                                int defrag, int keep_swap, int codec, part_meta_t *pm) {
    uint8_t vbr[512];
    if (read_lba(di, drive, start_lba, 1, vbr) != 0) {
        printf("  part %d: VBR read failed\n", index);
        return -1;
    }
    fatlay_t L;
    parse_fatlay(vbr, &L);
    if (!L.ok) { printf("  part %d: not a FAT BPB, skipping\n", index); return -1; }

    uint8_t *fat = malloc(L.old_spf * L.bps);
    if (!fat) { printf("  part %d: out of memory\n", index); return -1; }
    if (load_region(di, drive, start_lba + L.reserved, L.old_spf * L.bps, fat) != 0) {
        printf("  part %d: FAT load failed\n", index); free(fat); return -1;
    }

    uint32_t last_used = 0;
    for (uint32_t n = 2; n < L.clusters + 2; n++)
        if (fat_entry(fat, L.fat_bits, n) != 0) last_used = n;
    uint32_t norm_imaged = (last_used >= 2)
        ? L.first_data_sec + (last_used - 1) * L.spc : L.first_data_sec;
    if (norm_imaged > L.old_total) norm_imaged = L.old_total;

    pm->index = index;
    pm->type_byte = type_byte;
    pm->start_lba = start_lba;
    pm->original_size = part_sectors * 512ULL;
    sprintf(pm->gz_name, "partition-%d.%s", index, codec_ext(codec));

    char path[160];
    sprintf(path, "%s\\%s", dest, pm->gz_name);
    cbw_t *w = cbw_open(path, codec);
    if (!w) { printf("  part %d: cannot create %s\n", index, path); free(fat); return -1; }

    progress_t pr;
    char lbl[40];
    sprintf(lbl, "part %d (%s)", index, part_type_name(type_byte));

    uint32_t imaged_secs = norm_imaged;
    int used_defrag = 0, rc = 0;

    if (defrag) {
        uint32_t ds;
        int dr = defrag_backup_fat(di, drive, start_lba, &L, fat, w, lbl, &pr, &ds);
        if (dr < 0) { cbw_close(w); free(fat); printf("  part %d: defrag emit failed\n", index); return -1; }
        if (dr == 0) { imaged_secs = ds; used_defrag = 1; }
        /* dr == 1: declined; `w` is still empty -- fall through to plain compaction */
    }

    /* Level-1 swap exclusion (§6c): zero swap files' content too, not just free
     * clusters. Skipped under /DEFRAG (the repacker relocates clusters, so a
     * by-cluster mask wouldn't line up) and under --keep-swap. */
    uint8_t *swapbm = used_defrag ? NULL
                                  : build_swap_mask(di, drive, start_lba, &L, index, keep_swap);

    if (!used_defrag) {
        progress_begin(&pr, lbl, (uint64_t)norm_imaged * 512);
        uint8_t buf[XFER_BYTES];
        uint32_t s = 0;
        while (s < norm_imaged) {
            int n = (int)(norm_imaged - s);
            if (n > XFER_SECTORS) n = XFER_SECTORS;
            if (read_lba(di, drive, start_lba + s, n, buf) != 0) {
                printf("  part %d: read error at sector %lu\n", index, (unsigned long)s);
                rc = -1; break;
            }
            for (int j = 0; j < n; j++) {
                uint32_t rel = s + j;
                if (rel >= L.first_data_sec) {
                    uint32_t cl = 2 + (rel - L.first_data_sec) / L.spc;
                    if (cl < L.clusters + 2 &&
                        (fat_entry(fat, L.fat_bits, cl) == 0 || swap_hit(swapbm, cl)))
                        memset(buf + j * 512, 0, 512);
                }
            }
            if (cbw_write(w, buf, n * 512) != 0) {
                printf("  part %d: compress write failed\n", index);
                rc = -1; break;
            }
            s += n;
            progress_update(&pr, (uint64_t)s * 512);
        }
        progress_finish(&pr);
    }
    free(swapbm);

    if (cbw_close(w) != 0 && rc == 0) { printf("  part %d: compress finalize failed\n", index); rc = -1; }
    free(fat);
    if (rc != 0) return rc;

    pm->imaged_size = (uint64_t)imaged_secs * L.bps;
    pm->minimum_size = pm->imaged_size;
    pm->compacted = (imaged_secs < part_sectors) ? 1 : 0;

    gz_crc_sidecar(dest, pm);

    printf("  part %d (%s): imaged %lu KiB -> %s, crc %s%s%s\n",
           index, part_type_name(type_byte),
           (unsigned long)(pm->imaged_size / 1024), pm->gz_name, pm->crc_hex,
           used_defrag ? " [defragged]" : "", pm->compacted ? " [compacted]" : "");
    return 0;
}

/* Smart-compact + gzip an NTFS partition into <dest>/partition-N.gz, fill `pm`.
 * Mirrors backup_fat_partition but drives the free-cluster zeroing from the NTFS
 * $Bitmap instead of the FAT. Images the FULL partition window (zeroing only free
 * clusters), so the volume's backup boot sector + any tail are preserved verbatim
 * and restore is byte-faithful at the original size. On-DOS NTFS resize is out of
 * scope; the desktop resizes via resize_ntfs_in_place. gzip crushes the zeroed
 * free space, so the .gz still tracks used data. */
static int backup_ntfs_partition(const drive_info_t *di, int drive,
                                 const char *dest, int index, uint8_t type_byte,
                                 uint64_t start_lba, uint64_t part_sectors,
                                 int codec, part_meta_t *pm) {
    uint8_t vbr[512];
    if (read_lba(di, drive, start_lba, 1, vbr) != 0) {
        printf("  part %d: VBR read failed\n", index);
        return -1;
    }
    if (!ntfs_is_ntfs(vbr)) {
        printf("  part %d: type 0x07 but not NTFS (exFAT/HPFS?) -- skipped\n", index);
        return -1;
    }
    ntfs_vol_t v;
    if (ntfs_parse(vbr, &v) != 0) {
        printf("  part %d: NTFS BPB unsupported (need 512-byte sectors) -- skipped\n", index);
        return -1;
    }
    uint8_t *bm = NULL;
    if (ntfs_load_bitmap(di, drive, start_lba, &v, &bm) != 0) {
        printf("  part %d: NTFS $Bitmap read failed -- skipped\n", index);
        return -1;
    }

    pm->index = index;
    pm->type_byte = type_byte;
    pm->start_lba = start_lba;
    pm->original_size = part_sectors * 512ULL;
    pm->imaged_size = part_sectors * 512ULL;     /* full window, free clusters zeroed */
    pm->minimum_size = pm->imaged_size;          /* desktop computes the true NTFS min */
    pm->compacted = 1;
    sprintf(pm->gz_name, "partition-%d.%s", index, codec_ext(codec));

    char path[160];
    sprintf(path, "%s\\%s", dest, pm->gz_name);
    cbw_t *w = cbw_open(path, codec);
    if (!w) { printf("  part %d: cannot create %s\n", index, path); free(bm); return -1; }

    progress_t pr;
    char lbl[40];
    sprintf(lbl, "part %d (NTFS)", index);
    progress_begin(&pr, lbl, part_sectors * 512ULL);

    uint8_t buf[XFER_BYTES];
    uint64_t s = 0;
    int rc = 0;
    while (s < part_sectors) {
        int n = (int)(part_sectors - s);
        if (n > XFER_SECTORS) n = XFER_SECTORS;
        if (read_lba(di, drive, start_lba + s, n, buf) != 0) {
            printf("  part %d: read error at sector %lu\n", index, (unsigned long)s);
            rc = -1; break;
        }
        for (int j = 0; j < n; j++) {
            uint64_t lcn = (s + j) / v.sec_per_clus;
            if (lcn < v.total_clusters && !ntfs_cluster_used(bm, v.total_clusters, lcn))
                memset(buf + j * 512, 0, 512);
        }
        if (cbw_write(w, buf, n * 512) != 0) {
            printf("  part %d: compress write failed\n", index);
            rc = -1; break;
        }
        s += n;
        progress_update(&pr, s * 512ULL);
    }
    if (cbw_close(w) != 0 && rc == 0) { printf("  part %d: compress finalize failed\n", index); rc = -1; }
    free(bm);
    progress_finish(&pr);
    if (rc != 0) return rc;

    gz_crc_sidecar(dest, pm);

    printf("  part %d (NTFS): imaged %lu KiB -> %s, crc %s [compacted]\n",
           index, (unsigned long)(pm->imaged_size / 1024), pm->gz_name, pm->crc_hex);
    return 0;
}

/* Append printf-style to buf[*pos..cap); advances *pos (clamped to cap). */
static void apnd(char *buf, int cap, int *pos, const char *fmt, ...) {
    if (*pos >= cap) return;
    va_list ap;
    va_start(ap, fmt);
    int w = vsnprintf(buf + *pos, (size_t)(cap - *pos), fmt, ap);
    va_end(ap);
    if (w > 0) *pos += w;
    if (*pos > cap) *pos = cap;   /* truncated -- caller sizes generously */
}

/* Build the metadata.json text into `out` (cap bytes). Returns the length, or -1
 * on overflow. The single source of truth for the metadata layout, shared by the
 * local-folder backup (write_metadata) and the networked PUT (cmd_backup's
 * rb:// path) so the two never drift. */
static int build_metadata(char *out, int cap, const char *src_label,
                          uint64_t disk_bytes, uint64_t first_lba,
                          const drive_info_t *di,
                          const part_meta_t *parts, int nparts,
                          const ext_meta_t *ext, int codec) {
    char created[32];
    time_t now = time(NULL);
    struct tm *tm = gmtime(&now);
    strftime(created, sizeof created, "%Y-%m-%dT%H:%M:%SZ", tm);

    const char *aligntype =
        (first_lba == 2048) ? "Modern 1MB boundaries" :
        (first_lba == 63)   ? "DOS Traditional (255x63)" : "Custom";

    int pos = 0;
    apnd(out, cap, &pos, "{\n");
    apnd(out, cap, &pos, "  \"version\": 1,\n");
    apnd(out, cap, &pos, "  \"created\": \"%s\",\n", created);
    apnd(out, cap, &pos, "  \"source_device\": \"%s\",\n", src_label);
    apnd(out, cap, &pos, "  \"source_size_bytes\": %lu,\n", (unsigned long)disk_bytes);
    apnd(out, cap, &pos, "  \"partition_table_type\": \"MBR\",\n");
    apnd(out, cap, &pos, "  \"checksum_type\": \"crc32\",\n");
    apnd(out, cap, &pos, "  \"compression_type\": \"%s\",\n", codec_name(codec));
    apnd(out, cap, &pos, "  \"split_size_mib\": null,\n");
    apnd(out, cap, &pos, "  \"sector_by_sector\": false,\n");
    apnd(out, cap, &pos, "  \"layout\": \"per-partition\",\n");
    apnd(out, cap, &pos, "  \"alignment\": {\n");
    apnd(out, cap, &pos, "    \"detected_type\": \"%s\",\n", aligntype);
    apnd(out, cap, &pos, "    \"first_partition_lba\": %lu,\n", (unsigned long)first_lba);
    apnd(out, cap, &pos, "    \"alignment_sectors\": %lu,\n", (unsigned long)first_lba);
    apnd(out, cap, &pos, "    \"heads\": %u,\n", di->heads);
    apnd(out, cap, &pos, "    \"sectors_per_track\": %u\n", di->spt);
    apnd(out, cap, &pos, "  },\n");
    apnd(out, cap, &pos, "  \"partitions\": [\n");
    for (int i = 0; i < nparts; i++) {
        const part_meta_t *p = &parts[i];
        apnd(out, cap, &pos, "    {\n");
        apnd(out, cap, &pos, "      \"index\": %d,\n", p->index);
        apnd(out, cap, &pos, "      \"type_name\": \"%s\",\n", part_type_name(p->type_byte));
        apnd(out, cap, &pos, "      \"partition_type_byte\": %u,\n", p->type_byte);
        apnd(out, cap, &pos, "      \"start_lba\": %lu,\n", (unsigned long)p->start_lba);
        apnd(out, cap, &pos, "      \"original_size_bytes\": %lu,\n", (unsigned long)p->original_size);
        apnd(out, cap, &pos, "      \"imaged_size_bytes\": %lu,\n", (unsigned long)p->imaged_size);
        apnd(out, cap, &pos, "      \"compressed_files\": [\"%s\"],\n", p->gz_name);
        apnd(out, cap, &pos, "      \"checksum\": \"%s\",\n", p->crc_hex);
        apnd(out, cap, &pos, "      \"resized\": false,\n");
        apnd(out, cap, &pos, "      \"compacted\": %s,\n", p->compacted ? "true" : "false");
        apnd(out, cap, &pos, "      \"is_logical\": %s,\n", p->is_logical ? "true" : "false");
        apnd(out, cap, &pos, "      \"minimum_size_bytes\": %lu\n", (unsigned long)p->minimum_size);
        apnd(out, cap, &pos, "    }%s\n", (i + 1 < nparts) ? "," : "");
    }
    apnd(out, cap, &pos, "  ]");
    if (ext && ext->found) {
        /* Emitted after "partitions" so cb-dos restore's positional scanner reads
         * the container's own start_lba/type without hitting a partition's. */
        apnd(out, cap, &pos, ",\n  \"extended_container\": {\n");
        apnd(out, cap, &pos, "    \"mbr_index\": %d,\n", ext->mbr_index);
        apnd(out, cap, &pos, "    \"partition_type_byte\": %u,\n", ext->type_byte);
        apnd(out, cap, &pos, "    \"start_lba\": %lu,\n", (unsigned long)ext->start_lba);
        apnd(out, cap, &pos, "    \"size_bytes\": %lu\n", (unsigned long)ext->size_bytes);
        apnd(out, cap, &pos, "  }\n");
    } else {
        apnd(out, cap, &pos, "\n");
    }
    apnd(out, cap, &pos, "}\n");
    return (pos >= cap) ? -1 : pos;
}

/* Shared metadata buffer (cb-dos is single-threaded; a 24-partition image fits). */
static char g_meta[16384];

static void write_metadata(const char *dest, const char *src_label,
                           uint64_t disk_bytes, uint64_t first_lba,
                           const drive_info_t *di,
                           const part_meta_t *parts, int nparts,
                           const ext_meta_t *ext, int codec) {
    int len = build_metadata(g_meta, (int)sizeof g_meta, src_label, disk_bytes,
                             first_lba, di, parts, nparts, ext, codec);
    if (len < 0) { printf("metadata.json too large\n"); return; }
    char path[160];
    sprintf(path, "%s\\metadata.json", dest);
    FILE *f = fopen(path, "wb");
    if (!f) { printf("cannot write metadata.json\n"); return; }
    fwrite(g_meta, 1, (size_t)len, f);
    fclose(f);
    printf("wrote metadata.json (%d partition%s)\n", nparts, nparts == 1 ? "" : "s");
}

/* Build the FAT partition's file manifest (§5) from the live source and write it
 * as <dest>\manifest-<index>.json next to partition-<index>.gz. Best-effort: a
 * failure logs and leaves the block backup intact (the manifest is a change-
 * detection sidecar, not required for restore). FAT only -- NTFS has no on-DOS
 * directory reader. */
static void emit_partition_manifest(const char *dest, int index, const drive_info_t *di,
                                    int drive, uint64_t start_lba, const uint8_t *mbr,
                                    int keep_swap) {
    char *buf;
    uint32_t len;
    if (manifest_build_fat(di, drive, start_lba, mbr, keep_swap, &buf, &len) != 0) {
        printf("  part %d: manifest skipped (could not read directory tree)\n", index);
        return;
    }
    char mp[160];
    sprintf(mp, "%s\\manifest-%d.json", dest, index);
    FILE *f = fopen(mp, "wb");
    if (f) {
        fwrite(buf, 1, len, f);
        fclose(f);
        printf("  wrote manifest-%d.json (%lu bytes)\n", index, (unsigned long)len);
    } else {
        printf("  cannot write manifest-%d.json\n", index);
    }
    free(buf);
}

/* ---- networked backup: image a disk block-level straight to an rb-cli serve
 * daemon as a Family-B chunk PUT (no intermediate folder on the DOS box). The
 * partition is read over int13h, smart-compacted, and compressed into gzip-member
 * spans streamed as chunks; mbr.bin + metadata.json ride as small Raw members.
 * docs/cb_dos_network_and_state.md §2. */

/* Stream one FAT partition's smart-compacted image as a Gz member into `net`. */
static int netstream_fat_partition(const drive_info_t *di, int drive, cbnet_t *net,
                                   int index, uint8_t type_byte,
                                   uint64_t start_lba, uint64_t part_sectors,
                                   int keep_swap, part_meta_t *pm) {
    uint8_t vbr[512];
    if (read_lba(di, drive, start_lba, 1, vbr) != 0) {
        printf("  part %d: VBR read failed\n", index); return -1;
    }
    fatlay_t L;
    parse_fatlay(vbr, &L);
    if (!L.ok) { printf("  part %d: not a FAT BPB, skipping\n", index); return -1; }

    uint8_t *fat = malloc(L.old_spf * L.bps);
    if (!fat) { printf("  part %d: out of memory\n", index); return -1; }
    if (load_region(di, drive, start_lba + L.reserved, L.old_spf * L.bps, fat) != 0) {
        printf("  part %d: FAT load failed\n", index); free(fat); return -1;
    }
    uint32_t last_used = 0;
    for (uint32_t nn = 2; nn < L.clusters + 2; nn++)
        if (fat_entry(fat, L.fat_bits, nn) != 0) last_used = nn;
    uint32_t norm_imaged = (last_used >= 2)
        ? L.first_data_sec + (last_used - 1) * L.spc : L.first_data_sec;
    if (norm_imaged > L.old_total) norm_imaged = L.old_total;

    pm->index = index; pm->type_byte = type_byte; pm->start_lba = start_lba;
    pm->original_size = part_sectors * 512ULL;
    sprintf(pm->gz_name, "partition-%d.gz", index);

    uint32_t committed = 0;
    if (cbnet_part_begin(net, pm->gz_name, index, (uint64_t)norm_imaged * 512, &committed) != 0) {
        printf("  part %d: net begin failed\n", index); free(fat); return -1;
    }
    /* On resume the agent already holds `committed` spans -- skip them in the
     * source (each span is CBNET_SPAN/512 sectors of uncompressed data). */
    uint32_t resume_sec = committed * (CBNET_SPAN / 512);
    if (resume_sec > norm_imaged) resume_sec = norm_imaged;

    /* Level-1 swap exclusion (§6c): deterministic zeros, so resume is unaffected
     * (a re-sent span re-zeros the same clusters) and the §4 FAT fingerprint is
     * unchanged (allocation kept, only content zeroed). */
    uint8_t *swapbm = build_swap_mask(di, drive, start_lba, &L, index, keep_swap);

    progress_t pr; char lbl[40];
    sprintf(lbl, "part %d (%s)", index, part_type_name(type_byte));
    progress_begin(&pr, lbl, (uint64_t)norm_imaged * 512);

    uint8_t buf[XFER_BYTES];
    uint32_t s = resume_sec; int rc = 0;
    while (s < norm_imaged) {
        int nsec = (int)(norm_imaged - s);
        if (nsec > XFER_SECTORS) nsec = XFER_SECTORS;
        if (read_lba(di, drive, start_lba + s, nsec, buf) != 0) {
            printf("  part %d: read error at sector %lu\n", index, (unsigned long)s);
            rc = -1; break;
        }
        for (int j = 0; j < nsec; j++) {
            uint32_t rel = s + j;
            if (rel >= L.first_data_sec) {
                uint32_t cl = 2 + (rel - L.first_data_sec) / L.spc;
                if (cl < L.clusters + 2 &&
                    (fat_entry(fat, L.fat_bits, cl) == 0 || swap_hit(swapbm, cl)))
                    memset(buf + j * 512, 0, 512);
            }
        }
        if (cbnet_part_write(net, buf, (uint32_t)nsec * 512) != 0) {
            printf("  part %d: network send failed\n", index); rc = -1; break;
        }
        s += nsec;
        progress_update(&pr, (uint64_t)s * 512);
    }
    progress_finish(&pr);
    free(swapbm);
    free(fat);

    if (cbnet_part_end(net) != 0 && rc == 0) {
        printf("  part %d: net finalize failed\n", index); rc = -1;
    }
    if (rc != 0) return rc;

    pm->imaged_size = (uint64_t)norm_imaged * L.bps;
    pm->minimum_size = pm->imaged_size;
    pm->compacted = (norm_imaged < part_sectors) ? 1 : 0;
    strcpy(pm->crc_hex, "00000000");  /* placeholder; the agent fills the real gz CRC */
    printf("  part %d (%s): streamed %lu KiB -> %s%s%s\n",
           index, part_type_name(type_byte), (unsigned long)(pm->imaged_size / 1024),
           pm->gz_name, committed ? " [resumed]" : "", pm->compacted ? " [compacted]" : "");
    return 0;
}

/* Stream one NTFS partition (full window, $Bitmap free clusters zeroed). */
static int netstream_ntfs_partition(const drive_info_t *di, int drive, cbnet_t *net,
                                    int index, uint8_t type_byte,
                                    uint64_t start_lba, uint64_t part_sectors,
                                    part_meta_t *pm) {
    uint8_t vbr[512];
    if (read_lba(di, drive, start_lba, 1, vbr) != 0) {
        printf("  part %d: VBR read failed\n", index); return -1;
    }
    if (!ntfs_is_ntfs(vbr)) {
        printf("  part %d: type 0x07 but not NTFS -- skipped\n", index); return -1;
    }
    ntfs_vol_t v;
    if (ntfs_parse(vbr, &v) != 0) {
        printf("  part %d: NTFS BPB unsupported -- skipped\n", index); return -1;
    }
    uint8_t *bm = NULL;
    if (ntfs_load_bitmap(di, drive, start_lba, &v, &bm) != 0) {
        printf("  part %d: NTFS $Bitmap read failed -- skipped\n", index); return -1;
    }

    pm->index = index; pm->type_byte = type_byte; pm->start_lba = start_lba;
    pm->original_size = part_sectors * 512ULL;
    pm->imaged_size = part_sectors * 512ULL;
    pm->minimum_size = pm->imaged_size;
    pm->compacted = 1;
    sprintf(pm->gz_name, "partition-%d.gz", index);

    uint32_t committed = 0;
    if (cbnet_part_begin(net, pm->gz_name, index, part_sectors * 512ULL, &committed) != 0) {
        printf("  part %d: net begin failed\n", index); free(bm); return -1;
    }
    uint64_t resume_sec = (uint64_t)committed * (CBNET_SPAN / 512);
    if (resume_sec > part_sectors) resume_sec = part_sectors;

    progress_t pr; char lbl[40];
    sprintf(lbl, "part %d (NTFS)", index);
    progress_begin(&pr, lbl, part_sectors * 512ULL);

    uint8_t buf[XFER_BYTES];
    uint64_t s = resume_sec; int rc = 0;
    while (s < part_sectors) {
        int nsec = (int)(part_sectors - s);
        if (nsec > XFER_SECTORS) nsec = XFER_SECTORS;
        if (read_lba(di, drive, start_lba + s, nsec, buf) != 0) {
            printf("  part %d: read error at sector %lu\n", index, (unsigned long)s);
            rc = -1; break;
        }
        for (int j = 0; j < nsec; j++) {
            uint64_t lcn = (s + j) / v.sec_per_clus;
            if (lcn < v.total_clusters && !ntfs_cluster_used(bm, v.total_clusters, lcn))
                memset(buf + j * 512, 0, 512);
        }
        if (cbnet_part_write(net, buf, (uint32_t)nsec * 512) != 0) {
            printf("  part %d: network send failed\n", index); rc = -1; break;
        }
        s += nsec;
        progress_update(&pr, s * 512ULL);
    }
    progress_finish(&pr);
    free(bm);

    if (cbnet_part_end(net) != 0 && rc == 0) {
        printf("  part %d: net finalize failed\n", index); rc = -1;
    }
    if (rc != 0) return rc;

    strcpy(pm->crc_hex, "00000000");  /* placeholder; the agent fills the real gz CRC */
    printf("  part %d (NTFS): streamed %lu KiB -> %s%s\n",
           index, (unsigned long)(pm->imaged_size / 1024), pm->gz_name,
           committed ? " [resumed]" : "");
    return 0;
}

/* Networked backup: scan the MBR, stream the selected primary FAT/NTFS
 * partitions block-level to the agent, send mbr.bin + metadata.json, finish. */
static int cmd_netbackup(int drive, unsigned sel_mask, int has_filter, int keep_swap,
                         int codec, const char *host, unsigned short port, const char *name) {
    if (codec != CODEC_GZIP) { printf("network backup is gzip-only for now\n"); return 2; }
    if (xfer_init() < 0) { printf("DOS memory alloc failed\n"); return 1; }

    drive_info_t di;
    drive_params(drive, &di);
    if (!di.present) { printf("drive 0x%02X not present\n", drive); xfer_free(); return 1; }
    di.ext = drive_has_ext(drive);
    printf("source drive 0x%02X: %u cyl %u head %u spt, LBA-ext=%s\n",
           drive, di.cyls, di.heads, di.spt, di.ext ? "yes" : "no");

    uint8_t mbr[512];
    if (read_lba(&di, drive, 0, 1, mbr) != 0) { printf("MBR read failed\n"); xfer_free(); return 1; }
    if (mbr[510] != 0x55 || mbr[511] != 0xAA) {
        printf("no MBR signature -- superfloppy not supported\n"); xfer_free(); return 1;
    }

    /* Enumerate the selected primary FAT/NTFS partitions (logicals: TODO). */
    struct { int idx; uint8_t type; uint64_t lba, cnt; } sel[4];
    int nsel = 0, saw_ext = 0;
    uint64_t disk_bytes = (uint64_t)di.cyls * di.heads * di.spt * 512;
    uint64_t first_lba = 0;
    for (int i = 0; i < 4; i++) {
        const uint8_t *e = mbr + 446 + i * 16;
        uint8_t type = e[4];
        uint32_t lba = rd32(e + 8), cnt = rd32(e + 12);
        if (type == 0 || cnt == 0) continue;
        if (first_lba == 0) first_lba = lba;
        uint64_t end = (uint64_t)lba + cnt;
        if (end * 512 > disk_bytes) disk_bytes = end * 512;
        if (is_extended_type(type)) { saw_ext = 1; continue; }
        if (has_filter && !(sel_mask & (1u << i))) continue;
        if (!is_fat_part_type(type) && type != 0x07) {
            printf("  part %d: type 0x%02X not FAT/NTFS -- skipped\n", i, type);
            continue;
        }
        sel[nsel].idx = i; sel[nsel].type = type; sel[nsel].lba = lba; sel[nsel].cnt = cnt;
        nsel++;
    }
    if (saw_ext)
        printf("  note: extended/logical partitions are not yet streamed over the wire (skipped)\n");
    if (nsel == 0) { printf("no FAT/NTFS primary partitions to back up\n"); xfer_free(); return 1; }

    /* Cheap source fingerprint for the resume gate (§4): CRC32 over the MBR, the
     * disk size, and each selected partition's boot sector + FAT (the allocation
     * map -- the workhorse that catches add/delete/move/resize/reformat). The
     * agent only checks equality; a mismatch means a swapped/edited card -> it
     * restarts rather than splice two vintages of the disk together. */
    unsigned long fingerprint = crc32(0L, mbr, 512);
    {
        uint64_t tot = drive_total_sectors(&di, drive);
        unsigned char tb[8];
        for (int i = 0; i < 8; i++) tb[i] = (unsigned char)((tot >> (8 * i)) & 0xFF);
        fingerprint = crc32(fingerprint, tb, 8);
        for (int k = 0; k < nsel; k++) {
            uint8_t vbr[512];
            if (read_lba(&di, drive, sel[k].lba, 1, vbr) != 0) continue;
            fingerprint = crc32(fingerprint, vbr, 512);
            if (is_fat_part_type(sel[k].type)) {
                fatlay_t L; parse_fatlay(vbr, &L);
                if (L.ok) {
                    uint8_t *fat = malloc(L.old_spf * L.bps);
                    if (fat) {
                        if (load_region(&di, drive, sel[k].lba + L.reserved,
                                        L.old_spf * L.bps, fat) == 0)
                            fingerprint = crc32(fingerprint, fat, L.old_spf * L.bps);
                        free(fat);
                    }
                }
            }
        }
    }

    /* Each FAT partition also ships a manifest-<idx>.json Raw member (§5); count
     * them up front so the fixed member_count matches what we send. A FAT
     * partition whose directory tree can't be read still sends a placeholder
     * manifest so the count never desyncs. NTFS gets no manifest (no on-DOS
     * dir reader). */
    int n_manifests = 0;
    for (int k = 0; k < nsel; k++)
        if (is_fat_part_type(sel[k].type)) n_manifests++;

    int member_count = 1 /*mbr.bin*/ + nsel + n_manifests + 1 /*metadata.json*/;
    cbnet_t *net = cbnet_start(host, port, name, fingerprint, member_count);
    if (!net) { xfer_free(); return 1; }

    if (cbnet_raw_member(net, "mbr.bin", mbr, 512) != 0) {
        printf("sending mbr.bin failed\n"); cbnet_close(net); xfer_free(); return 1;
    }

    part_meta_t parts[4];
    ext_meta_t ext; memset(&ext, 0, sizeof ext);
    int nparts = 0;
    for (int k = 0; k < nsel; k++) {
        int ok = is_fat_part_type(sel[k].type)
            ? netstream_fat_partition(&di, drive, net, sel[k].idx, sel[k].type,
                                      sel[k].lba, sel[k].cnt, keep_swap, &parts[nparts])
            : netstream_ntfs_partition(&di, drive, net, sel[k].idx, sel[k].type,
                                       sel[k].lba, sel[k].cnt, &parts[nparts]);
        if (ok != 0) {
            printf("aborting network backup (partition %d failed)\n", sel[k].idx);
            cbnet_close(net); xfer_free(); return 1;
        }
        parts[nparts].is_logical = 0;
        nparts++;

        /* Ship this FAT partition's file manifest right after its image (rides
         * into the .cbk as a Raw member via the host's pack_folder_to_cbk). */
        if (is_fat_part_type(sel[k].type)) {
            char mn[24];
            sprintf(mn, "manifest-%d.json", sel[k].idx);
            char *mbuf;
            uint32_t mblen;
            int sent;
            if (manifest_build_fat(&di, drive, sel[k].lba, mbr, keep_swap, &mbuf, &mblen) == 0) {
                printf("  part %d: manifest %lu bytes -> %s\n",
                       sel[k].idx, (unsigned long)mblen, mn);
                sent = cbnet_raw_member(net, mn, mbuf, mblen);
                free(mbuf);
            } else {
                static const char ph[] =
                    "{\n  \"manifest_version\": 1,\n  \"error\": \"manifest unavailable\"\n}\n";
                printf("  part %d: manifest unavailable -- sending placeholder\n", sel[k].idx);
                sent = cbnet_raw_member(net, mn, ph, (uint32_t)(sizeof ph - 1));
            }
            if (sent != 0) {
                printf("sending %s failed\n", mn);
                cbnet_close(net); xfer_free(); return 1;
            }
        }
    }

    char label[16];
    sprintf(label, "0x%02X", drive);
    int mlen = build_metadata(g_meta, (int)sizeof g_meta, label, disk_bytes, first_lba,
                              &di, parts, nparts, &ext, codec);
    if (mlen < 0) { printf("metadata too large\n"); cbnet_close(net); xfer_free(); return 1; }
    if (cbnet_raw_member(net, "metadata.json", g_meta, (uint32_t)mlen) != 0) {
        printf("sending metadata.json failed\n"); cbnet_close(net); xfer_free(); return 1;
    }

    unsigned long long cbk_size = 0;
    int rc = cbnet_finish(net, &cbk_size);
    cbnet_close(net);
    xfer_free();
    if (rc != 0) { printf("network backup failed\n"); return 1; }
    printf("network backup complete: %s.cbk assembled on %s:%u (%llu bytes)\n",
           name, host, (unsigned)port, cbk_size);
    return 0;
}

int cmd_backup(int argc, char **argv) {
    setvbuf(stdout, NULL, _IOLBF, 0);
    if (argc < 2) {
        printf("usage: CRUSTYBK backup <dest> [drive-hex] [/PARTS:i,j] [/DEFRAG] [/CODEC:LZ4]\n");
        printf("  <dest> is a folder (A:\\BK) OR a network agent rb://HOST[:PORT]/NAME\n");
        printf("  e.g. CRUSTYBK backup A:\\BK 80               image to a local folder\n");
        printf("       CRUSTYBK backup rb://10.0.2.2/MYDISK 80 stream straight to an rb-cli serve\n");
        printf("       CRUSTYBK backup A:\\BK 80 /PARTS:0      image only MBR slot 0\n");
        printf("       CRUSTYBK backup A:\\BK 80 /DEFRAG       repack FAT files contiguously\n");
        printf("       CRUSTYBK backup A:\\BK 80 /CODEC:LZ4    LZ4 instead of gzip (faster, larger)\n");
        printf("       CRUSTYBK backup A:\\BK 80 /KEEPSWAP    keep swap/page-file content (don't zero)\n");
        printf("  /PARTS indices are the 0-based MBR primary slots (the \"part N\"\n");
        printf("  numbers below and the metadata.json \"index\" values).\n");
        printf("  rb:// streams the disk block-level to the agent (no local folder);\n");
        printf("  the agent assembles a NAME.cbk. gzip only, primaries only for now.\n");
        printf("  /DEFRAG relocates each FAT volume's files into contiguous runs\n");
        printf("  (boot files first) before imaging -- smaller image, defragged restore.\n");
        printf("  /CODEC:LZ4 trades ratio for speed on a slow CPU (default GZIP);\n");
        printf("  restore auto-detects the codec from metadata.\n");
        printf("  By default swap/page files (WIN386.SWP, 386SPART.PAR, PAGEFILE.SYS,\n");
        printf("  HIBERFIL.SYS, SWAPPER.DAT) are kept full-size but zeroed (they\n");
        printf("  reinitialize on boot); /KEEPSWAP images their content verbatim.\n");
        return 2;
    }

    const char *dest = argv[1];
    int drive = 0x80, drive_set = 0;
    unsigned sel_mask = 0; int has_filter = 0, defrag = 0, keep_swap = 0, codec = CODEC_GZIP;
    for (int a = 2; a < argc; a++) {
        const char *v = switch_val(argv[a], "/PARTS:");
        const char *cv = switch_val(argv[a], "/CODEC:");
        if (v) {
            if (parse_parts(v, &sel_mask) != 0) { printf("bad /PARTS list\n"); return 2; }
            has_filter = 1;
        } else if (cv) {
            codec = codec_from_name(cv);
            if (codec < 0) { printf("bad /CODEC (use GZIP or LZ4)\n"); return 2; }
        } else if (eq_ci(argv[a], "/DEFRAG")) {
            defrag = 1;
        } else if (eq_ci(argv[a], "/KEEPSWAP")) {
            keep_swap = 1;
        } else if (!drive_set) {
            drive = (int)strtol(argv[a], NULL, 16);
            drive_set = 1;
        }
    }

    /* Network destination: stream the disk block-level to an rb-cli serve daemon
     * instead of writing a local folder (no spare DOS storage needed). */
    if (strncmp(dest, "rb://", 5) == 0) {
        if (defrag) { printf("network backup does not support /DEFRAG yet\n"); return 2; }
        char host[64], name[64];
        unsigned short port;
        if (cbnet_parse_url(dest, host, sizeof host, &port, name, sizeof name) != 0) {
            printf("bad rb:// destination (use rb://HOST[:PORT]/NAME)\n");
            return 2;
        }
        return cmd_netbackup(drive, sel_mask, has_filter, keep_swap, codec, host, port, name);
    }

    if (xfer_init() < 0) { printf("DOS memory alloc failed\n"); return 1; }

    drive_info_t di;
    drive_params(drive, &di);
    if (!di.present) { printf("drive 0x%02X not present\n", drive); xfer_free(); return 1; }
    di.ext = drive_has_ext(drive);
    printf("source drive 0x%02X: %u cyl %u head %u spt, LBA-ext=%s\n",
           drive, di.cyls, di.heads, di.spt, di.ext ? "yes" : "no");
    if (has_filter) printf("partition filter: /PARTS mask 0x%X\n", sel_mask);
    if (defrag) printf("defrag: FAT volumes will be repacked contiguously (boot-aware)\n");
    if (keep_swap) printf("keep-swap: swap/page files imaged verbatim (not zeroed)\n");
    if (codec != CODEC_GZIP) printf("codec: %s (partition-N.%s)\n", codec_name(codec), codec_ext(codec));

    uint8_t mbr[512];
    if (read_lba(&di, drive, 0, 1, mbr) != 0) { printf("MBR read failed\n"); xfer_free(); return 1; }
    if (mbr[510] != 0x55 || mbr[511] != 0xAA) {
        printf("no MBR signature -- superfloppy not supported\n");
        xfer_free(); return 1;
    }

    char mpath[160];
    sprintf(mpath, "%s\\mbr.bin", dest);
    FILE *mf = fopen(mpath, "wb");
    if (mf) { fwrite(mbr, 1, 512, mf); fclose(mf); printf("wrote mbr.bin\n"); }
    else { printf("cannot write mbr.bin (does %s exist?)\n", dest); xfer_free(); return 1; }

    uint64_t disk_bytes = (uint64_t)di.cyls * di.heads * di.spt * 512;
    uint64_t first_lba = 0;

    part_meta_t parts[24];
    ext_meta_t ext; memset(&ext, 0, sizeof ext);
    int nparts = 0;
    for (int i = 0; i < 4 && nparts < 24; i++) {
        const uint8_t *e = mbr + 446 + i * 16;
        uint8_t type = e[4];
        uint32_t lba = rd32(e + 8);
        uint32_t cnt = rd32(e + 12);
        if (type == 0 || cnt == 0) continue;
        if (first_lba == 0) first_lba = lba;       /* disk-wide alignment hint */
        uint64_t end = (uint64_t)lba + cnt;
        if (end * 512 > disk_bytes) disk_bytes = end * 512;

        /* Extended container: image the logical partitions inside its EBR chain. */
        if (is_extended_type(type)) {
            ext.found = 1; ext.mbr_index = i; ext.type_byte = type;
            ext.start_lba = lba; ext.size_bytes = (uint64_t)cnt * 512;
            printf("  part %d: extended container (type 0x%02X) lba %lu, %lu MB\n",
                   i, type, (unsigned long)lba, (unsigned long)(cnt / 2048));
            logical_t logs[20];
            int nl = walk_ebr_chain(&di, drive, lba, logs, 20);
            if (nl < 0) { printf("  (EBR chain read failed)\n"); continue; }
            for (int j = 0; j < nl && nparts < 24; j++) {
                int lidx = 4 + j;
                uint64_t lend = logs[j].start_lba + logs[j].count;
                if (lend * 512 > disk_bytes) disk_bytes = lend * 512;
                if (has_filter && !(sel_mask & (1u << lidx))) {
                    printf("  logical %d: not in /PARTS -- skipped\n", lidx);
                    continue;
                }
                int lok;
                if (is_fat_part_type(logs[j].type))
                    lok = backup_fat_partition(&di, drive, dest, lidx, logs[j].type,
                                               logs[j].start_lba, logs[j].count, defrag, keep_swap, codec, &parts[nparts]);
                else if (logs[j].type == 0x07)
                    lok = backup_ntfs_partition(&di, drive, dest, lidx, logs[j].type,
                                                logs[j].start_lba, logs[j].count, codec, &parts[nparts]);
                else {
                    printf("  logical %d: type 0x%02X not FAT/NTFS -- skipped\n", lidx, logs[j].type);
                    continue;
                }
                if (lok == 0) {
                    parts[nparts].is_logical = 1;
                    nparts++;
                    if (is_fat_part_type(logs[j].type))
                        emit_partition_manifest(dest, lidx, &di, drive, logs[j].start_lba, mbr, keep_swap);
                }
            }
            continue;
        }

        if (has_filter && !(sel_mask & (1u << i))) {
            printf("  part %d: not in /PARTS -- skipped\n", i);
            continue;
        }
        int ok;
        if (is_fat_part_type(type))
            ok = backup_fat_partition(&di, drive, dest, i, type, lba, cnt, defrag, keep_swap, codec, &parts[nparts]);
        else if (type == 0x07)
            ok = backup_ntfs_partition(&di, drive, dest, i, type, lba, cnt, codec, &parts[nparts]);
        else {
            printf("  part %d: type 0x%02X not FAT/NTFS -- skipped\n", i, type);
            continue;
        }
        if (ok == 0) {
            parts[nparts].is_logical = 0;
            nparts++;
            if (is_fat_part_type(type))
                emit_partition_manifest(dest, i, &di, drive, lba, mbr, keep_swap);
        }
    }

    if (nparts == 0) { printf("no FAT/NTFS partitions imaged\n"); xfer_free(); return 1; }

    char label[16];
    sprintf(label, "0x%02X", drive);
    write_metadata(dest, label, disk_bytes, first_lba, &di, parts, nparts, &ext, codec);

    printf("backup complete: %d partition%s in %s\n", nparts, nparts == 1 ? "" : "s", dest);
    xfer_free();
    return 0;
}
