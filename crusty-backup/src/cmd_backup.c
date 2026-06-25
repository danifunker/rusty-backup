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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
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

/* Smart-compact + gzip a FAT partition into <dest>/partition-N.gz, fill `pm`.
 * When `defrag` is set, the volume's files are first relocated into contiguous
 * runs (boot-file aware) via cbdefrag; if that path declines (an unclean FS, or
 * out of memory) it falls back to the plain last-used-cluster compaction, with
 * the gzip stream untouched up to that point. */
static int backup_fat_partition(const drive_info_t *di, int drive,
                                const char *dest, int index, uint8_t type_byte,
                                uint64_t start_lba, uint64_t part_sectors,
                                int defrag, part_meta_t *pm) {
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
    sprintf(pm->gz_name, "partition-%d.gz", index);

    char path[160];
    sprintf(path, "%s\\%s", dest, pm->gz_name);
    gzFile gz = gzopen(path, "wb6");
    if (!gz) { printf("  part %d: cannot create %s\n", index, path); free(fat); return -1; }

    progress_t pr;
    char lbl[40];
    sprintf(lbl, "part %d (%s)", index, part_type_name(type_byte));

    uint32_t imaged_secs = norm_imaged;
    int used_defrag = 0, rc = 0;

    if (defrag) {
        uint32_t ds;
        int dr = defrag_backup_fat(di, drive, start_lba, &L, fat, gz, lbl, &pr, &ds);
        if (dr < 0) { gzclose(gz); free(fat); printf("  part %d: defrag emit failed\n", index); return -1; }
        if (dr == 0) { imaged_secs = ds; used_defrag = 1; }
        /* dr == 1: declined; gz is still empty -- fall through to plain compaction */
    }

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
                    if (cl < L.clusters + 2 && fat_entry(fat, L.fat_bits, cl) == 0)
                        memset(buf + j * 512, 0, 512);
                }
            }
            if (gzwrite(gz, buf, n * 512) != n * 512) {
                printf("  part %d: gzwrite failed\n", index);
                rc = -1; break;
            }
            s += n;
            progress_update(&pr, (uint64_t)s * 512);
        }
        progress_finish(&pr);
    }

    gzclose(gz);
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
                                 part_meta_t *pm) {
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
    sprintf(pm->gz_name, "partition-%d.gz", index);

    char path[160];
    sprintf(path, "%s\\%s", dest, pm->gz_name);
    gzFile gz = gzopen(path, "wb6");
    if (!gz) { printf("  part %d: cannot create %s\n", index, path); free(bm); return -1; }

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
        if (gzwrite(gz, buf, n * 512) != n * 512) {
            printf("  part %d: gzwrite failed\n", index);
            rc = -1; break;
        }
        s += n;
        progress_update(&pr, s * 512ULL);
    }
    gzclose(gz);
    free(bm);
    progress_finish(&pr);
    if (rc != 0) return rc;

    gz_crc_sidecar(dest, pm);

    printf("  part %d (NTFS): imaged %lu KiB -> %s, crc %s [compacted]\n",
           index, (unsigned long)(pm->imaged_size / 1024), pm->gz_name, pm->crc_hex);
    return 0;
}

static void write_metadata(const char *dest, const char *src_label,
                           uint64_t disk_bytes, uint64_t first_lba,
                           const drive_info_t *di,
                           const part_meta_t *parts, int nparts,
                           const ext_meta_t *ext) {
    char path[160];
    sprintf(path, "%s\\metadata.json", dest);
    FILE *f = fopen(path, "wb");
    if (!f) { printf("cannot write metadata.json\n"); return; }

    char created[32];
    time_t now = time(NULL);
    struct tm *tm = gmtime(&now);
    strftime(created, sizeof created, "%Y-%m-%dT%H:%M:%SZ", tm);

    const char *aligntype =
        (first_lba == 2048) ? "Modern 1MB boundaries" :
        (first_lba == 63)   ? "DOS Traditional (255x63)" : "Custom";

    fprintf(f, "{\n");
    fprintf(f, "  \"version\": 1,\n");
    fprintf(f, "  \"created\": \"%s\",\n", created);
    fprintf(f, "  \"source_device\": \"%s\",\n", src_label);
    fprintf(f, "  \"source_size_bytes\": %lu,\n", (unsigned long)disk_bytes);
    fprintf(f, "  \"partition_table_type\": \"MBR\",\n");
    fprintf(f, "  \"checksum_type\": \"crc32\",\n");
    fprintf(f, "  \"compression_type\": \"gzip\",\n");
    fprintf(f, "  \"split_size_mib\": null,\n");
    fprintf(f, "  \"sector_by_sector\": false,\n");
    fprintf(f, "  \"layout\": \"per-partition\",\n");
    fprintf(f, "  \"alignment\": {\n");
    fprintf(f, "    \"detected_type\": \"%s\",\n", aligntype);
    fprintf(f, "    \"first_partition_lba\": %lu,\n", (unsigned long)first_lba);
    fprintf(f, "    \"alignment_sectors\": %lu,\n", (unsigned long)first_lba);
    fprintf(f, "    \"heads\": %u,\n", di->heads);
    fprintf(f, "    \"sectors_per_track\": %u\n", di->spt);
    fprintf(f, "  },\n");
    fprintf(f, "  \"partitions\": [\n");
    for (int i = 0; i < nparts; i++) {
        const part_meta_t *p = &parts[i];
        fprintf(f, "    {\n");
        fprintf(f, "      \"index\": %d,\n", p->index);
        fprintf(f, "      \"type_name\": \"%s\",\n", part_type_name(p->type_byte));
        fprintf(f, "      \"partition_type_byte\": %u,\n", p->type_byte);
        fprintf(f, "      \"start_lba\": %lu,\n", (unsigned long)p->start_lba);
        fprintf(f, "      \"original_size_bytes\": %lu,\n", (unsigned long)p->original_size);
        fprintf(f, "      \"imaged_size_bytes\": %lu,\n", (unsigned long)p->imaged_size);
        fprintf(f, "      \"compressed_files\": [\"%s\"],\n", p->gz_name);
        fprintf(f, "      \"checksum\": \"%s\",\n", p->crc_hex);
        fprintf(f, "      \"resized\": false,\n");
        fprintf(f, "      \"compacted\": %s,\n", p->compacted ? "true" : "false");
        fprintf(f, "      \"is_logical\": %s,\n", p->is_logical ? "true" : "false");
        fprintf(f, "      \"minimum_size_bytes\": %lu\n", (unsigned long)p->minimum_size);
        fprintf(f, "    }%s\n", (i + 1 < nparts) ? "," : "");
    }
    fprintf(f, "  ]");
    if (ext && ext->found) {
        /* Emitted after "partitions" so cb-dos restore's positional scanner reads
         * the container's own start_lba/type without hitting a partition's. */
        fprintf(f, ",\n  \"extended_container\": {\n");
        fprintf(f, "    \"mbr_index\": %d,\n", ext->mbr_index);
        fprintf(f, "    \"partition_type_byte\": %u,\n", ext->type_byte);
        fprintf(f, "    \"start_lba\": %lu,\n", (unsigned long)ext->start_lba);
        fprintf(f, "    \"size_bytes\": %lu\n", (unsigned long)ext->size_bytes);
        fprintf(f, "  }\n");
    } else {
        fprintf(f, "\n");
    }
    fprintf(f, "}\n");
    fclose(f);
    printf("wrote metadata.json (%d partition%s)\n", nparts, nparts == 1 ? "" : "s");
}

int cmd_backup(int argc, char **argv) {
    setvbuf(stdout, NULL, _IONBF, 0);
    if (argc < 2) {
        printf("usage: CRUSTYBK backup <dest-dir> [drive-hex] [/PARTS:i,j] [/DEFRAG]\n");
        printf("  e.g. CRUSTYBK backup A:\\BK 80           image every FAT/NTFS partition\n");
        printf("       CRUSTYBK backup A:\\BK 80 /PARTS:0  image only MBR slot 0\n");
        printf("       CRUSTYBK backup A:\\BK 80 /DEFRAG   repack FAT files contiguously\n");
        printf("  /PARTS indices are the 0-based MBR primary slots (the \"part N\"\n");
        printf("  numbers below and the metadata.json \"index\" values).\n");
        printf("  /DEFRAG relocates each FAT volume's files into contiguous runs\n");
        printf("  (boot files first) before imaging -- smaller .gz, defragged restore.\n");
        return 2;
    }

    const char *dest = argv[1];
    int drive = 0x80, drive_set = 0;
    unsigned sel_mask = 0; int has_filter = 0, defrag = 0;
    for (int a = 2; a < argc; a++) {
        const char *v = switch_val(argv[a], "/PARTS:");
        if (v) {
            if (parse_parts(v, &sel_mask) != 0) { printf("bad /PARTS list\n"); return 2; }
            has_filter = 1;
        } else if (eq_ci(argv[a], "/DEFRAG")) {
            defrag = 1;
        } else if (!drive_set) {
            drive = (int)strtol(argv[a], NULL, 16);
            drive_set = 1;
        }
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
                                               logs[j].start_lba, logs[j].count, defrag, &parts[nparts]);
                else if (logs[j].type == 0x07)
                    lok = backup_ntfs_partition(&di, drive, dest, lidx, logs[j].type,
                                                logs[j].start_lba, logs[j].count, &parts[nparts]);
                else {
                    printf("  logical %d: type 0x%02X not FAT/NTFS -- skipped\n", lidx, logs[j].type);
                    continue;
                }
                if (lok == 0) { parts[nparts].is_logical = 1; nparts++; }
            }
            continue;
        }

        if (has_filter && !(sel_mask & (1u << i))) {
            printf("  part %d: not in /PARTS -- skipped\n", i);
            continue;
        }
        int ok;
        if (is_fat_part_type(type))
            ok = backup_fat_partition(&di, drive, dest, i, type, lba, cnt, defrag, &parts[nparts]);
        else if (type == 0x07)
            ok = backup_ntfs_partition(&di, drive, dest, i, type, lba, cnt, &parts[nparts]);
        else {
            printf("  part %d: type 0x%02X not FAT/NTFS -- skipped\n", i, type);
            continue;
        }
        if (ok == 0) { parts[nparts].is_logical = 0; nparts++; }
    }

    if (nparts == 0) { printf("no FAT/NTFS partitions imaged\n"); xfer_free(); return 1; }

    char label[16];
    sprintf(label, "0x%02X", drive);
    write_metadata(dest, label, disk_bytes, first_lba, &di, parts, nparts, &ext);

    printf("backup complete: %d partition%s in %s\n", nparts, nparts == 1 ? "" : "s", dest);
    xfer_free();
    return 0;
}
