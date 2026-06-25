/* cmd_restore.c -- the `restore` command (Phase 3 + on-DOS resize + selective).
 *
 * Reads the desktop's native PerPartition folder (metadata.json + mbr.bin +
 * partition-N.gz) and writes it back to a disk via int13h, with /SIZE resize and
 * /PARTS selection. The disk/FAT/resize engine lives in cbdisk; this module owns
 * the metadata.json scanner and the gzip restore stream. */

#include "cbdisk.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <zlib.h>

enum { SZ_ORIGINAL, SZ_MINIMUM, SZ_ENTIRE, SZ_CUSTOM };

/* ----- tiny metadata.json field scanner ----------------------------- */

static const char *skip_to_value(const char *p) {
    while (*p && (*p == ':' || *p == ' ' || *p == '\t' || *p == '\n' || *p == '\r'))
        p++;
    return p;
}
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
/* Read the value right after the cursor (already past a key) -- does NOT search
 * for another occurrence (which, for "index", would grab the next partition's). */
static uint64_t u64_at(const char *p, uint64_t dflt) {
    p = skip_to_value(p);
    uint64_t v = 0; int any = 0;
    while (*p >= '0' && *p <= '9') { v = v * 10 + (uint64_t)(*p - '0'); p++; any = 1; }
    return any ? v : dflt;
}
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

/* ----- restore one partition (gzip stream -> disk) ------------------ */

/* Peek the first sector of <folder>/<gzname> and parse its FAT layout. */
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

/* Stream <folder>/<gzname> to start_lba, zero-padding out to window_bytes. */
static int restore_partition(const drive_info_t *di, int drive, const char *folder,
                             const char *gzname, uint64_t start_lba, uint64_t window_bytes) {
    char path[200];
    sprintf(path, "%s\\%s", folder, gzname);
    gzFile gz = gzopen(path, "rb");
    if (!gz) { printf("  cannot open %s\n", path); return -1; }

    progress_t pr;
    progress_begin(&pr, gzname, window_bytes);

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
                printf("  write error at lba %lu\n", (unsigned long)(start_lba + written / 512));
                rc = -1; break;
            }
            written += (uint64_t)full * 512;
            int rem = acc_len - full * 512;
            if (rem) memmove(acc, acc + full * 512, rem);
            acc_len = rem;
            progress_update(&pr, written);
        }
        if (n == 0) break;
    }
    if (rc == 0 && acc_len > 0) {
        memset(acc + acc_len, 0, 512 - acc_len);
        if (write_lba(di, drive, start_lba + written / 512, 1, acc) == 0)
            written += 512;
    }
    gzclose(gz);
    if (rc != 0) { progress_finish(&pr); return rc; }

    if (written < window_bytes) {
        memset(acc, 0, XFER_BYTES);
        while (written < window_bytes) {
            uint64_t left = window_bytes - written;
            int secs = (left / 512 > XFER_SECTORS) ? XFER_SECTORS : (int)(left / 512);
            if (secs < 1) break;
            if (write_lba(di, drive, start_lba + written / 512, secs, acc) != 0) break;
            written += (uint64_t)secs * 512;
            progress_update(&pr, written);
        }
    }
    progress_update(&pr, window_bytes);
    progress_finish(&pr);
    return 0;
}

typedef struct {
    int      index;
    uint64_t start_lba, original, imaged, minimum;
    char     gz[40];
    int      is_fat;
    uint64_t window_sec;
} part_t;

int cmd_restore(int argc, char **argv) {
    setvbuf(stdout, NULL, _IONBF, 0);
    if (argc < 3) {
        printf("usage: CRUSTYBK restore <folder> <target-drive-hex> /Y [/SIZE:mode] [/CUSTOM:bytes] [/PARTS:i,j]\n");
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
    if (mode == SZ_CUSTOM && custom_bytes == 0) { printf("/SIZE:CUSTOM needs /CUSTOM:<bytes>\n"); return 2; }

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
        printf("partition_table_type \"%s\" not supported (MBR only)\n", ptype);
        return 1;
    }

    uint32_t md_heads = (uint32_t)u64_after(meta, "heads", 0);
    uint32_t md_spt = (uint32_t)u64_after(meta, "sectors_per_track", 0);

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
            p->index = (int)u64_at(idx, 0);
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
           drive, di.cyls, di.heads, di.spt, di.ext ? "yes" : "no", (unsigned long)disk_sectors);
    if (mode == SZ_CUSTOM)        printf("size policy: custom (%lu bytes)\n", (unsigned long)custom_bytes);
    else if (mode != SZ_ORIGINAL) printf("size policy: %s\n", mode == SZ_MINIMUM ? "minimum" : "entire");
    if (has_filter) printf("partition filter: /PARTS mask 0x%X\n", sel_mask);

    if (!confirmed) {
        printf("REFUSING to write without /Y (this ERASES drive 0x%02X)\n", drive);
        xfer_free();
        return 1;
    }

    char bpath[200];
    sprintf(bpath, "%s\\mbr.bin", folder);
    FILE *bf = fopen(bpath, "rb");
    if (!bf) { printf("cannot open %s\n", bpath); xfer_free(); return 1; }
    uint8_t mbr[512];
    if (fread(mbr, 1, 512, bf) != 512) { printf("short mbr.bin\n"); fclose(bf); xfer_free(); return 1; }
    fclose(bf);

    int restored = 0;
    for (int k = 0; k < nparts; k++) {
        part_t *p = &parts[k];
        p->window_sec = p->original / 512;
        if (has_filter && (p->index < 0 || p->index >= 32 || !(sel_mask & (1u << p->index)))) {
            printf("  partition %d: not in /PARTS -- skipped\n", p->index);
            continue;
        }
        if (!p->is_fat) {
            if (p->gz[0])
                printf("  partition %d: codec not gzip (%s) -- restores .gz only\n", p->index, p->gz);
            continue;
        }

        fatlay_t pl;
        if (peek_partition(folder, p->gz, &pl) != 0) {
            printf("  cannot open %s\\%s\n", folder, p->gz);
            xfer_free();
            return 1;
        }
        int is_512_fat = pl.ok && pl.bps == 512;

        uint64_t limit_sec = disk_sectors;
        for (int j = 0; j < nparts; j++)
            if (parts[j].start_lba > p->start_lba && parts[j].start_lba < limit_sec)
                limit_sec = parts[j].start_lba;
        uint64_t limit_window = (limit_sec > p->start_lba) ? limit_sec - p->start_lba : 0;
        uint64_t imaged_sec = round_up_512(p->imaged) / 512;

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
            win = p->original / 512;
        }

        if (can_resize) {
            uint64_t cap = max_fat_window(&pl);
            if (win > cap) {
                printf("  partition %d: FAT%d cluster limit -- window capped to %lu KiB\n",
                       p->index, pl.fat_bits, (unsigned long)(cap * 512 / 1024));
                win = cap;
            }
            if (win < imaged_sec) win = imaged_sec;
            uint64_t floor = min_fat_window(&pl);
            if (win < floor) win = floor;
            if (limit_window && win > limit_window) win = limit_window;
            if (win < imaged_sec) {
                printf("  partition %d: target disk too small for used data (%lu KiB) -- aborting\n",
                       p->index, (unsigned long)(imaged_sec * 512 / 1024));
                xfer_free();
                return 1;
            }
        } else if (limit_window && win > limit_window) {
            printf("  partition %d: original window exceeds disk -- target too small\n", p->index);
            xfer_free();
            return 1;
        }
        p->window_sec = win;

        if (restore_partition(&di, drive, folder, p->gz, p->start_lba, win * 512ULL) != 0) {
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
               (unsigned long)(p->window_sec * 512 / 1024), resized ? ", resized" : "");
        restored++;
    }

    if (restored == 0) { printf("no partitions restored\n"); xfer_free(); return 1; }

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
    if (write_lba(&di, drive, 0, 1, mbr) != 0) { printf("MBR write failed\n"); xfer_free(); return 1; }
    printf("wrote MBR\n");
    printf("restore complete: %d partition%s written to drive 0x%02X\n",
           restored, restored == 1 ? "" : "s", drive);
    xfer_free();
    return 0;
}
