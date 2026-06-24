/* cbbackup.c -- cb-dos backup engine MVP (Phase 2, docs/cb_dos.md).
 *
 * Reads a FAT disk on the vintage machine itself (int 13h) and writes the
 * desktop's *native PerPartition backup folder* to a DOS path:
 *
 *     <dest>\ metadata.json
 *            mbr.bin
 *            partition-0.gz   partition-0.gz.crc32
 *            partition-1.gz   partition-1.gz.crc32  ...
 *
 * Each FAT partition is "smart-compacted": we image only up to the end of the
 * last used cluster, zero the free clusters inside that range, and stream the
 * result through gzip (zlib gzwrite). The desktop restores it -- and resizes
 * it to a different-sized disk -- with no cb-dos-specific code, because this
 * is byte-for-byte the same folder its own zstd/raw backups produce (only the
 * codec differs: .gz instead of .zst). See the frozen metadata schema in
 * docs/cb_dos.md S3.
 *
 * MVP scope: MBR primary FAT12/16/32 partitions. NTFS/exFAT and logical
 * (extended) partitions are skipped with a note -- later phases.
 *
 * Build:  sh deps/fetch-zlib.sh   then   make backup    (-> build/cbbackup.exe)
 * Run:    CBBACKUP <dest-dir> [drive-hex]    e.g.  CBBACKUP A:\BK 80
 *
 * The low-level disk primitives (xfer buffer / read_lba / parse_bpb) mirror
 * disk_spike.c; kept inline here so the spike stays untouched. A later phase
 * can lift the shared parts into a cbdisk.{h,c}.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <dpmi.h>
#include <go32.h>
#include <sys/movedata.h>
#include <zlib.h>

/* FIXED (2026-06-24): the cb-dos tools used to hang at *program termination*
 * under CWSDPMI on real FreeDOS (the backup completed and restored fine; the
 * process just never returned). Root cause: xfer_init allocated the DOS
 * transfer buffer as exactly XFER_BYTES, but read_lba places the 16-byte int13h
 * Disk Address Packet at offset XFER_BYTES -- one byte past the block -- which
 * overran the adjacent memory-control block. DOS only notices the corrupt MCB
 * when it walks the arena to free memory at exit, hence a hang at termination
 * (only after an AH=42 LBA read; AH=08 alone never touches the DAP). DOSBox-X's
 * DPMI was just more forgiving. The fix is the +16 in xfer_init below. */

/* ----- DOS-memory transfer buffer (real-mode reachable) ------------- */

#define XFER_SECTORS 16
#define XFER_BYTES   (XFER_SECTORS * 512)
static int g_buf_seg, g_buf_sel;

static int xfer_init(void) {
    /* +16 bytes for the int13h Disk Address Packet that read_lba places at
     * offset XFER_BYTES. Without it the block is exactly XFER_BYTES and the DAP
     * overruns the next MCB, which DOS only trips over when it walks the memory
     * arena at program exit -> the long-mysterious CWSDPMI termination hang. */
    int para = (XFER_BYTES + 16 + 15) >> 4;
    g_buf_seg = __dpmi_allocate_dos_memory(para, &g_buf_sel);
    return g_buf_seg;
}
static void xfer_free(void) {
    if (g_buf_seg > 0) __dpmi_free_dos_memory(g_buf_sel);
}

/* ----- drive geometry + sector I/O (read only) ---------------------- */

typedef struct {
    int present, ext;
    unsigned cyls, heads, spt;
} drive_info_t;

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

/* Read `count` (<= XFER_SECTORS) sectors at `lba` into host `dst`.
 * Returns 0 on success, else the BIOS AH error code. */
static int read_lba(const drive_info_t *di, int drive, uint64_t lba,
                    int count, void *dst) {
    __dpmi_regs r;
    int err;
    if (count < 1 || count > XFER_SECTORS) return 0xFF;
    memset(&r, 0, sizeof r);
    if (di->ext) {
        uint8_t dap[16];
        memset(dap, 0, sizeof dap);
        dap[0] = 0x10;
        dap[2] = count & 0xFF;
        dap[3] = (count >> 8) & 0xFF;
        dap[6] = g_buf_seg & 0xFF;
        dap[7] = (g_buf_seg >> 8) & 0xFF;
        for (int i = 0; i < 8; i++) dap[8 + i] = (uint8_t)(lba >> (8 * i));
        unsigned dap_lin = g_buf_seg * 16 + XFER_BYTES;
        dosmemput(dap, sizeof dap, dap_lin);
        r.h.ah = 0x42; r.h.dl = drive;
        r.x.ds = g_buf_seg + (XFER_BYTES >> 4); r.x.si = 0;
        __dpmi_int(0x13, &r);
        err = (r.x.flags & 1) ? r.h.ah : 0;
    } else {
        unsigned spt = di->spt ? di->spt : 63;
        unsigned heads = di->heads ? di->heads : 16;
        unsigned cyl = (unsigned)(lba / (spt * heads));
        unsigned tmp = (unsigned)(lba % (spt * heads));
        unsigned head = tmp / spt;
        unsigned sect = tmp % spt + 1;
        if (cyl > 1023) return 0xFE;
        r.h.ah = 0x02; r.h.al = count;
        r.h.ch = cyl & 0xFF;
        r.h.cl = (uint8_t)((sect & 0x3F) | ((cyl >> 2) & 0xC0));
        r.h.dh = head; r.h.dl = drive;
        r.x.es = g_buf_seg; r.x.bx = 0;
        __dpmi_int(0x13, &r);
        err = (r.x.flags & 1) ? r.h.ah : 0;
    }
    if (err == 0) dosmemget(g_buf_seg * 16, count * 512, dst);
    return err;
}

/* ----- little-endian helpers ---------------------------------------- */

static uint16_t rd16(const uint8_t *p) { return p[0] | (p[1] << 8); }
static uint32_t rd32(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

/* ----- FAT BPB ------------------------------------------------------ */

typedef struct {
    unsigned bytes_per_sec, sec_per_clus, reserved, num_fats, root_entries;
    uint32_t total_secs, fat_secs;
    int      fat_bits;
    uint32_t data_secs, clusters, first_data_sec;
} bpb_t;

static int parse_bpb(const uint8_t *s, bpb_t *b) {
    memset(b, 0, sizeof *b);
    b->bytes_per_sec = rd16(s + 11);
    b->sec_per_clus  = s[13];
    b->reserved      = rd16(s + 14);
    b->num_fats      = s[16];
    b->root_entries  = rd16(s + 17);
    uint32_t tot16 = rd16(s + 19), fat16 = rd16(s + 22);
    uint32_t tot32 = rd32(s + 32), fat32 = rd32(s + 36);
    if (b->bytes_per_sec == 0 || b->sec_per_clus == 0) return -1;
    b->total_secs = tot16 ? tot16 : tot32;
    b->fat_secs   = fat16 ? fat16 : fat32;
    if (b->fat_secs == 0 || b->total_secs == 0) return -1;
    unsigned root_secs =
        ((b->root_entries * 32) + (b->bytes_per_sec - 1)) / b->bytes_per_sec;
    uint32_t meta = b->reserved + b->num_fats * b->fat_secs + root_secs;
    if (meta >= b->total_secs) return -1;
    b->data_secs = b->total_secs - meta;
    b->clusters  = b->data_secs / b->sec_per_clus;
    if (b->clusters < 4085)       b->fat_bits = 12;
    else if (b->clusters < 65525) b->fat_bits = 16;
    else                          b->fat_bits = 32;
    b->first_data_sec = meta;
    return 0;
}

static int is_fat_part_type(uint8_t t) {
    return t == 0x01 || t == 0x04 || t == 0x06 ||
           t == 0x0B || t == 0x0C || t == 0x0E;
}
static const char *part_type_name(uint8_t t) {
    switch (t) {
        case 0x01: return "FAT12";
        case 0x04: return "FAT16 (<32MB)";
        case 0x06: return "FAT16 (>32MB)";
        case 0x0B: return "FAT32 (CHS)";
        case 0x0C: return "FAT32 (LBA)";
        case 0x0E: return "FAT16 (LBA)";
        default:   return "FAT";
    }
}

/* ----- FAT allocation table (loaded whole) -------------------------- */

/* Load the first FAT into a malloc'd buffer. Returns NULL on error. */
static uint8_t *load_fat(const drive_info_t *di, int drive,
                         uint64_t vol_lba, const bpb_t *b) {
    uint32_t fat_bytes = b->fat_secs * b->bytes_per_sec;
    uint8_t *fat = malloc(fat_bytes);
    if (!fat) return NULL;
    uint64_t fat_lba = vol_lba + b->reserved;
    uint32_t done = 0;
    while (done < fat_bytes) {
        uint32_t want = fat_bytes - done;
        int secs = (int)((want + 511) / 512);
        if (secs > XFER_SECTORS) secs = XFER_SECTORS;
        if (read_lba(di, drive, fat_lba + done / 512, secs, fat + done) != 0) {
            free(fat); return NULL;
        }
        done += (uint32_t)secs * 512;
    }
    return fat;
}

/* FAT entry value for cluster n (0 == free). */
static uint32_t fat_entry(const uint8_t *fat, int bits, uint32_t n) {
    if (bits == 16) return rd16(fat + n * 2);
    if (bits == 32) return rd32(fat + n * 4) & 0x0FFFFFFF;
    /* FAT12: 12 bits packed */
    uint32_t off = n + (n / 2);
    uint32_t pair = fat[off] | (fat[off + 1] << 8);
    return (n & 1) ? (pair >> 4) : (pair & 0xFFF);
}

/* ----- backup of one partition -------------------------------------- */

typedef struct {
    int      index;
    uint8_t  type_byte;
    uint64_t start_lba;
    uint64_t original_size;   /* partition window bytes (MBR count*512) */
    uint64_t imaged_size;     /* bytes actually imaged */
    uint64_t minimum_size;    /* == imaged_size here */
    int      compacted;
    char     gz_name[24];
    char     crc_hex[9];
} part_meta_t;

/* Smart-compact + gzip a FAT partition into <dest>/partition-N.gz, fill `pm`.
 * Returns 0 on success. */
static int backup_fat_partition(const drive_info_t *di, int drive,
                                const char *dest, int index, uint8_t type_byte,
                                uint64_t start_lba, uint64_t part_sectors,
                                part_meta_t *pm) {
    uint8_t vbr[512];
    if (read_lba(di, drive, start_lba, 1, vbr) != 0) {
        printf("  part %d: VBR read failed\n", index);
        return -1;
    }
    bpb_t b;
    if (parse_bpb(vbr, &b) != 0) {
        printf("  part %d: not a FAT BPB, skipping\n", index);
        return -1;
    }
    uint8_t *fat = load_fat(di, drive, start_lba, &b);
    if (!fat) { printf("  part %d: FAT load failed\n", index); return -1; }

    /* Highest used data cluster -> imaged length. */
    uint32_t last_used = 0;
    for (uint32_t n = 2; n < b.clusters + 2; n++)
        if (fat_entry(fat, b.fat_bits, n) != 0) last_used = n;
    uint32_t imaged_secs = (last_used >= 2)
        ? b.first_data_sec + (last_used - 1) * b.sec_per_clus
        : b.first_data_sec;
    if (imaged_secs > b.total_secs) imaged_secs = b.total_secs;

    pm->index = index;
    pm->type_byte = type_byte;
    pm->start_lba = start_lba;
    pm->original_size = part_sectors * 512ULL;
    pm->imaged_size = (uint64_t)imaged_secs * b.bytes_per_sec;
    pm->minimum_size = pm->imaged_size;
    pm->compacted = (imaged_secs < part_sectors) ? 1 : 0;
    sprintf(pm->gz_name, "partition-%d.gz", index);

    char path[160];
    sprintf(path, "%s\\%s", dest, pm->gz_name);
    gzFile gz = gzopen(path, "wb6");
    if (!gz) { printf("  part %d: cannot create %s\n", index, path); free(fat); return -1; }

    uint8_t buf[XFER_BYTES];
    uint32_t s = 0;
    int rc = 0;
    while (s < imaged_secs) {
        int n = (int)(imaged_secs - s);
        if (n > XFER_SECTORS) n = XFER_SECTORS;
        if (read_lba(di, drive, start_lba + s, n, buf) != 0) {
            printf("  part %d: read error at sector %lu\n", index, (unsigned long)s);
            rc = -1; break;
        }
        /* Zero the sectors of free data clusters (gzip then crushes them). */
        for (int j = 0; j < n; j++) {
            uint32_t rel = s + j;
            if (rel >= b.first_data_sec) {
                uint32_t cl = 2 + (rel - b.first_data_sec) / b.sec_per_clus;
                if (cl < b.clusters + 2 && fat_entry(fat, b.fat_bits, cl) == 0)
                    memset(buf + j * 512, 0, 512);
            }
        }
        if (gzwrite(gz, buf, n * 512) != n * 512) {
            printf("  part %d: gzwrite failed\n", index);
            rc = -1; break;
        }
        s += n;
    }
    gzclose(gz);
    free(fat);
    if (rc != 0) return rc;

    /* CRC32 over the compressed .gz file (the desktop's checksum). */
    FILE *f = fopen(path, "rb");
    uLong crc = crc32(0L, Z_NULL, 0);
    if (f) {
        uint8_t rb[4096]; size_t got;
        while ((got = fread(rb, 1, sizeof rb, f)) > 0)
            crc = crc32(crc, rb, (uInt)got);
        fclose(f);
    }
    sprintf(pm->crc_hex, "%08lx", (unsigned long)crc);

    /* Sidecar partition-N.gz.crc32 (lowercase hex, matching the desktop). */
    char crcpath[180];
    sprintf(crcpath, "%s\\%s.crc32", dest, pm->gz_name);
    FILE *cf = fopen(crcpath, "wb");
    if (cf) { fprintf(cf, "%s", pm->crc_hex); fclose(cf); }

    printf("  part %d (%s): imaged %lu KiB -> %s, crc %s%s\n",
           index, part_type_name(type_byte),
           (unsigned long)(pm->imaged_size / 1024), pm->gz_name, pm->crc_hex,
           pm->compacted ? " [compacted]" : "");
    return 0;
}

/* ----- metadata.json ------------------------------------------------ */

static void write_metadata(const char *dest, const char *src_label,
                           uint64_t disk_bytes, uint64_t first_lba,
                           const drive_info_t *di,
                           const part_meta_t *parts, int nparts) {
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
        fprintf(f, "      \"is_logical\": false,\n");
        fprintf(f, "      \"minimum_size_bytes\": %lu\n", (unsigned long)p->minimum_size);
        fprintf(f, "    }%s\n", (i + 1 < nparts) ? "," : "");
    }
    fprintf(f, "  ]\n");
    fprintf(f, "}\n");
    fclose(f);
    printf("wrote metadata.json (%d partition%s)\n", nparts, nparts == 1 ? "" : "s");
}

/* ----- main --------------------------------------------------------- */

int main(int argc, char **argv) {
    if (argc < 2) {
        printf("cb-dos backup (Phase 2 MVP)\n");
        printf("usage: CBBACKUP <dest-dir> [drive-hex]\n");
        printf("  e.g. CBBACKUP A:\\BK 80   (default drive 0x80)\n");
        return 2;
    }
    /* Unbuffered so progress survives a redirect (and any abnormal exit). */
    setvbuf(stdout, NULL, _IONBF, 0);

    const char *dest = argv[1];
    int drive = (argc >= 3) ? (int)strtol(argv[2], NULL, 16) : 0x80;

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
        printf("no MBR signature -- superfloppy not supported by this MVP\n");
        xfer_free(); return 1;
    }

    /* mbr.bin */
    char mpath[160];
    sprintf(mpath, "%s\\mbr.bin", dest);
    FILE *mf = fopen(mpath, "wb");
    if (mf) { fwrite(mbr, 1, 512, mf); fclose(mf); printf("wrote mbr.bin\n"); }
    else { printf("cannot write mbr.bin (does %s exist?)\n", dest); xfer_free(); return 1; }

    /* Disk size: prefer CHS geometry product, fall back to last partition end. */
    uint64_t disk_bytes = (uint64_t)di.cyls * di.heads * di.spt * 512;
    uint64_t first_lba = 0;

    part_meta_t parts[4];
    int nparts = 0;
    for (int i = 0; i < 4; i++) {
        const uint8_t *e = mbr + 446 + i * 16;
        uint8_t type = e[4];
        uint32_t lba = rd32(e + 8);
        uint32_t cnt = rd32(e + 12);
        if (type == 0 || cnt == 0) continue;
        if (first_lba == 0) first_lba = lba;
        uint64_t end = (uint64_t)lba + cnt;
        if (end * 512 > disk_bytes) disk_bytes = end * 512;
        if (!is_fat_part_type(type)) {
            printf("  part %d: type 0x%02X not FAT -- skipped (MVP)\n", i, type);
            continue;
        }
        if (backup_fat_partition(&di, drive, dest, i, type, lba, cnt, &parts[nparts]) == 0)
            nparts++;
    }

    if (nparts == 0) { printf("no FAT partitions imaged\n"); xfer_free(); return 1; }

    char label[16];
    sprintf(label, "0x%02X", drive);
    write_metadata(dest, label, disk_bytes, first_lba, &di, parts, nparts);

    printf("backup complete: %d partition%s in %s\n",
           nparts, nparts == 1 ? "" : "s", dest);
    xfer_free();
    return 0;
}
