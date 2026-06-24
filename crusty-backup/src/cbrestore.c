/* cbrestore.c -- cb-dos restore engine MVP (Phase 3, docs/cb_dos.md).
 *
 * The inverse of cbbackup: reads the desktop's native PerPartition folder
 * (metadata.json + mbr.bin + partition-N.gz) from a DOS path and writes it back
 * onto a disk via int 13h, reconstructing a bootable card. Each partition's
 * gzip member is streamed through zlib gzread straight to the target sectors;
 * compacted partitions are zero-padded out to their original window so the FAT
 * geometry recorded in the BPB stays consistent.
 *
 * MVP scope: restore at the *original* size (the partition windows recorded in
 * metadata.json), MBR + FAT. On-DOS resize (shrink/grow the FAT to a different
 * target) is the next increment -- the desktop already resizes on restore, so a
 * cb-dos backup can be resized there meanwhile. NTFS/exFAT and logical
 * partitions are not written (skipped with a note).
 *
 * Build:  sh deps/fetch-zlib.sh   then   make restore   (-> build/cbrestore.exe)
 * Run:    CBRESTORE <folder> <target-drive-hex> /Y
 *         e.g.  CBRESTORE C:\BK 81 /Y      (/Y confirms the destructive write)
 *
 * See the KNOWN ISSUE in cbbackup.c: under CWSDPMI an int13h-using DJGPP
 * program may hang at termination after the work is done.
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

/* ----- drive geometry + sector write (and read for the MBR check) --- */

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

/* ----- restore one partition ---------------------------------------- */

static int restore_partition(const drive_info_t *di, int drive, const char *folder,
                             const char *gzname, uint64_t start_lba,
                             uint64_t imaged_size, uint64_t original_size) {
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

    /* Zero-pad the compacted tail out to the original partition window. */
    if (written < original_size) {
        memset(acc, 0, XFER_BYTES);
        while (written < original_size) {
            uint64_t left = original_size - written;
            int secs = (left / 512 > XFER_SECTORS) ? XFER_SECTORS : (int)(left / 512);
            if (secs < 1) break;
            if (write_lba(di, drive, start_lba + written / 512, secs, acc) != 0) break;
            written += (uint64_t)secs * 512;
        }
    }
    printf("  restored %s -> lba %lu (%lu KiB imaged, %lu KiB window)\n",
           gzname, (unsigned long)start_lba,
           (unsigned long)(imaged_size / 1024), (unsigned long)(original_size / 1024));
    (void)imaged_size;
    return 0;
}

/* ----- main --------------------------------------------------------- */

int main(int argc, char **argv) {
    setvbuf(stdout, NULL, _IONBF, 0);
    if (argc < 3) {
        printf("cb-dos restore (Phase 3 MVP)\n");
        printf("usage: CBRESTORE <folder> <target-drive-hex> /Y\n");
        printf("  /Y confirms the destructive write to the target drive.\n");
        return 2;
    }
    const char *folder = argv[1];
    int drive = (int)strtol(argv[2], NULL, 16);
    int confirmed = 0;
    for (int i = 3; i < argc; i++)
        if (strcmp(argv[i], "/Y") == 0 || strcmp(argv[i], "/y") == 0) confirmed = 1;

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

    if (xfer_init() < 0) { printf("DOS memory alloc failed\n"); return 1; }
    drive_info_t di;
    drive_params(drive, &di);
    if (!di.present) { printf("target drive 0x%02X not present\n", drive); xfer_free(); return 1; }
    di.ext = drive_has_ext(drive);
    printf("target drive 0x%02X: %u cyl %u head %u spt, LBA-ext=%s\n",
           drive, di.cyls, di.heads, di.spt, di.ext ? "yes" : "no");

    if (!confirmed) {
        printf("REFUSING to write without /Y (this ERASES drive 0x%02X)\n", drive);
        xfer_free();
        return 1;
    }

    /* Write mbr.bin to sector 0. */
    char bpath[200];
    sprintf(bpath, "%s\\mbr.bin", folder);
    FILE *bf = fopen(bpath, "rb");
    if (!bf) { printf("cannot open %s\n", bpath); xfer_free(); return 1; }
    uint8_t mbr[512];
    if (fread(mbr, 1, 512, bf) != 512) { printf("short mbr.bin\n"); fclose(bf); xfer_free(); return 1; }
    fclose(bf);
    if (write_lba(&di, drive, 0, 1, mbr) != 0) { printf("MBR write failed\n"); xfer_free(); return 1; }
    printf("wrote MBR\n");

    /* Walk the partitions array. */
    const char *cur = strstr(meta, "\"partitions\"");
    if (!cur) cur = meta;
    int n = 0;
    for (;;) {
        const char *idx = find_key(cur, "index");
        if (!idx) break;
        uint64_t start_lba = u64_after(idx, "start_lba", 0);
        uint64_t orig = u64_after(idx, "original_size_bytes", 0);
        uint64_t imaged = u64_after(idx, "imaged_size_bytes", orig);
        char gz[40] = "";
        str_after(idx, "compressed_files", gz, sizeof gz);
        /* Advance the cursor past this partition's fields. */
        const char *next = find_key(idx, "compacted");
        cur = next ? next : (idx + 1);

        if (gz[0] == 0 || start_lba == 0 || orig == 0) {
            printf("  partition %d: incomplete metadata, skipping\n", n);
            continue;
        }
        if (strstr(gz, ".gz") == NULL) {
            printf("  partition %d: codec not gzip (%s) -- this MVP restores .gz only\n", n, gz);
            continue;
        }
        if (restore_partition(&di, drive, folder, gz, start_lba, imaged, orig) == 0)
            n++;
    }

    if (n == 0) { printf("no partitions restored\n"); xfer_free(); return 1; }
    printf("restore complete: %d partition%s written to drive 0x%02X\n",
           n, n == 1 ? "" : "s", drive);
    xfer_free();
    return 0;
}
