/* cmd_clone.c -- the `clone` command (Phase 4b, docs/cb_dos.md S2e) + NTFS.
 *
 * Direct disk-to-disk clone with no intermediate file: read source (int13h) ->
 * smart-compact each partition on the fly (zero free clusters) -> write straight
 * to the target, optionally resizing. cbbackup's read+compaction fused with
 * cbrestore's write+resize, over the shared cbdisk engine. FAT partitions resize
 * (FAT from its FAT); NTFS partitions clone same-size (free clusters zeroed from
 * the $Bitmap) -- on-DOS NTFS resize is out of scope, the desktop handles it. */

#include "cbdisk.h"
#include "cbntfs.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

enum { SZ_ORIGINAL, SZ_MINIMUM, SZ_ENTIRE, SZ_CUSTOM };

/* Copy source partition [start, imaged_secs) to the target at the same
 * start_lba, zeroing free clusters in flight, then zero-pad to window_bytes. */
static int clone_partition(const drive_info_t *sdi, int sdrive,
                           const drive_info_t *tdi, int tdrive,
                           uint64_t start_lba, uint64_t window_bytes,
                           const fatlay_t *L, const uint8_t *fat, uint32_t imaged_secs,
                           const char *label) {
    progress_t pr;
    progress_begin(&pr, label, window_bytes);
    uint8_t buf[XFER_BYTES];
    uint32_t s = 0;
    while (s < imaged_secs) {
        int n = (int)(imaged_secs - s);
        if (n > XFER_SECTORS) n = XFER_SECTORS;
        if (read_lba(sdi, sdrive, start_lba + s, n, buf) != 0) {
            printf("  clone: source read error at sector %lu\n", (unsigned long)s);
            progress_finish(&pr); return -1;
        }
        for (int j = 0; j < n; j++) {
            uint32_t rel = s + j;
            if (rel >= L->first_data_sec) {
                uint32_t cl = 2 + (rel - L->first_data_sec) / L->spc;
                if (cl < L->clusters + 2 && fat_entry(fat, L->fat_bits, cl) == 0)
                    memset(buf + j * 512, 0, 512);   /* free cluster -> zero */
            }
        }
        if (write_lba(tdi, tdrive, start_lba + s, n, buf) != 0) {
            printf("  clone: target write error at sector %lu\n", (unsigned long)s);
            progress_finish(&pr); return -1;
        }
        s += n;
        progress_update(&pr, (uint64_t)s * 512);
    }
    memset(buf, 0, XFER_BYTES);
    uint64_t written = (uint64_t)imaged_secs * 512;
    while (written < window_bytes) {
        uint64_t left = window_bytes - written;
        int secs = (left / 512 > XFER_SECTORS) ? XFER_SECTORS : (int)(left / 512);
        if (secs < 1) break;
        if (write_lba(tdi, tdrive, start_lba + written / 512, secs, buf) != 0) break;
        written += (uint64_t)secs * 512;
        progress_update(&pr, written);
    }
    progress_update(&pr, window_bytes);
    progress_finish(&pr);
    return 0;
}

/* Clone an NTFS partition same-size: copy the full window [start, start+win_sec)
 * to the target at the same start_lba, zeroing free clusters in flight per the
 * $Bitmap. No resize (NTFS resize is desktop-only), so no zero-pad beyond. */
static int clone_ntfs_partition(const drive_info_t *sdi, int sdrive,
                                const drive_info_t *tdi, int tdrive,
                                uint64_t start_lba, uint64_t win_sec,
                                const ntfs_vol_t *v, const uint8_t *bm,
                                const char *label) {
    progress_t pr;
    progress_begin(&pr, label, win_sec * 512ULL);
    uint8_t buf[XFER_BYTES];
    uint64_t s = 0;
    while (s < win_sec) {
        int n = (int)(win_sec - s);
        if (n > XFER_SECTORS) n = XFER_SECTORS;
        if (read_lba(sdi, sdrive, start_lba + s, n, buf) != 0) {
            printf("  clone: source read error at sector %lu\n", (unsigned long)s);
            progress_finish(&pr); return -1;
        }
        for (int j = 0; j < n; j++) {
            uint64_t lcn = (s + j) / v->sec_per_clus;
            if (lcn < v->total_clusters && !ntfs_cluster_used(bm, v->total_clusters, lcn))
                memset(buf + j * 512, 0, 512);
        }
        if (write_lba(tdi, tdrive, start_lba + s, n, buf) != 0) {
            printf("  clone: target write error at sector %lu\n", (unsigned long)s);
            progress_finish(&pr); return -1;
        }
        s += n;
        progress_update(&pr, s * 512ULL);
    }
    progress_update(&pr, win_sec * 512ULL);
    progress_finish(&pr);
    return 0;
}

int cmd_clone(int argc, char **argv) {
    setvbuf(stdout, NULL, _IOLBF, 0);
    if (argc < 3) {
        printf("usage: CRUSTYBK clone <src-hex> <tgt-hex> /Y [/SIZE:mode] [/CUSTOM:bytes] [/PARTS:i,j]\n");
        printf("  direct disk-to-disk clone, no staging file\n");
        printf("  /Y               confirms the destructive write to the target drive\n");
        printf("  /SIZE:ORIGINAL   clone at the source sizes (default; free clusters zeroed)\n");
        printf("  /SIZE:MINIMUM    shrink each FAT partition to its used data\n");
        printf("  /SIZE:ENTIRE     grow each FAT partition to fill the target disk\n");
        printf("  /SIZE:CUSTOM     resize to /CUSTOM:<bytes>\n");
        printf("  /PARTS:i,j       clone only these MBR slot indices\n");
        return 2;
    }
    int sdrive = (int)strtol(argv[1], NULL, 16);
    int tdrive = (int)strtol(argv[2], NULL, 16);
    int confirmed = 0, mode = SZ_ORIGINAL;
    uint64_t custom_bytes = 0;
    unsigned sel_mask = 0; int has_filter = 0;
    for (int i = 3; i < argc; i++) {
        const char *v;
        if (eq_ci(argv[i], "/Y")) confirmed = 1;
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
    if (sdrive == tdrive) { printf("source and target are the same drive (0x%02X)\n", sdrive); return 1; }
    if (mode == SZ_CUSTOM && custom_bytes == 0) { printf("/SIZE:CUSTOM needs /CUSTOM:<bytes>\n"); return 2; }

    if (xfer_init() < 0) { printf("DOS memory alloc failed\n"); return 1; }
    drive_info_t sdi, tdi;
    drive_params(sdrive, &sdi); sdi.ext = drive_has_ext(sdrive);
    drive_params(tdrive, &tdi); tdi.ext = drive_has_ext(tdrive);
    if (!sdi.present) { printf("source drive 0x%02X not present\n", sdrive); xfer_free(); return 1; }
    if (!tdi.present) { printf("target drive 0x%02X not present\n", tdrive); xfer_free(); return 1; }
    uint64_t tgt_sectors = drive_total_sectors(&tdi, tdrive);
    printf("source 0x%02X: %u cyl %u head %u spt, ext=%s\n",
           sdrive, sdi.cyls, sdi.heads, sdi.spt, sdi.ext ? "yes" : "no");
    printf("target 0x%02X: %u cyl %u head %u spt, ext=%s, %lu sectors\n",
           tdrive, tdi.cyls, tdi.heads, tdi.spt, tdi.ext ? "yes" : "no",
           (unsigned long)tgt_sectors);
    if (mode == SZ_CUSTOM)        printf("size policy: custom (%lu bytes)\n", (unsigned long)custom_bytes);
    else if (mode != SZ_ORIGINAL) printf("size policy: %s\n", mode == SZ_MINIMUM ? "minimum" : "entire");
    if (has_filter) printf("partition filter: /PARTS mask 0x%X\n", sel_mask);

    if (!confirmed) {
        printf("REFUSING to write without /Y (this ERASES drive 0x%02X)\n", tdrive);
        xfer_free();
        return 1;
    }

    uint8_t mbr[512];
    if (read_lba(&sdi, sdrive, 0, 1, mbr) != 0) { printf("source MBR read failed\n"); xfer_free(); return 1; }
    if (mbr[510] != 0x55 || mbr[511] != 0xAA) {
        printf("source has no MBR signature -- superfloppy not supported\n");
        xfer_free(); return 1;
    }

    typedef struct { int slot; uint8_t type; uint64_t start, count, window_sec; } clp_t;
    clp_t P[4]; int np = 0;
    for (int i = 0; i < 4; i++) {
        const uint8_t *e = mbr + 446 + i * 16;
        if (e[4] == 0 || rd32(e + 12) == 0) continue;
        P[np].slot = i; P[np].type = e[4];
        P[np].start = rd32(e + 8); P[np].count = rd32(e + 12);
        P[np].window_sec = P[np].count;
        np++;
    }
    if (np == 0) { printf("no partitions in source MBR\n"); xfer_free(); return 1; }

    int cloned = 0;
    for (int k = 0; k < np; k++) {
        clp_t *p = &P[k];

        /* Extended container: copy each EBR sector verbatim (chain stays intact
         * on the target) and clone each logical FAT/NTFS volume same-size. */
        if (is_extended_type(p->type)) {
            printf("  slot %d: extended container (type 0x%02X) -- cloning logicals\n",
                   p->slot, p->type);
            if (mode != SZ_ORIGINAL)
                printf("  (logicals clone same-size; resize on the desktop)\n");
            logical_t logs[20];
            int nl = walk_ebr_chain(&sdi, sdrive, p->start, logs, 20);
            if (nl < 0) { printf("  (EBR chain read failed)\n"); continue; }
            for (int j = 0; j < nl; j++) {
                int lidx = 4 + j;
                uint8_t ebrsec[512];                 /* copy the EBR verbatim */
                if (read_lba(&sdi, sdrive, logs[j].ebr_lba, 1, ebrsec) == 0)
                    write_lba(&tdi, tdrive, logs[j].ebr_lba, 1, ebrsec);
                if (has_filter && !(sel_mask & (1u << lidx))) {
                    printf("  logical %d: not in /PARTS -- skipped\n", lidx);
                    continue;
                }
                uint8_t lvbr[512];
                if (read_lba(&sdi, sdrive, logs[j].start_lba, 1, lvbr) != 0) {
                    printf("  logical %d: VBR read failed -- skipped\n", lidx);
                    continue;
                }
                char llbl[40];
                fatlay_t L; parse_fatlay(lvbr, &L);
                if (L.ok && L.bps == 512) {
                    uint8_t *fat = malloc(L.old_spf * L.bps);
                    if (!fat) { printf("  logical %d: out of memory\n", lidx); xfer_free(); return 1; }
                    if (load_region(&sdi, sdrive, logs[j].start_lba + L.reserved, L.old_spf * L.bps, fat) != 0) {
                        printf("  logical %d: FAT read failed -- skipped\n", lidx); free(fat); continue;
                    }
                    uint32_t last_used = 0;
                    for (uint32_t cl = 2; cl < L.clusters + 2; cl++)
                        if (fat_entry(fat, L.fat_bits, cl) != 0) last_used = cl;
                    uint32_t imaged_secs = (last_used >= 2) ? L.first_data_sec + (last_used - 1) * L.spc : L.first_data_sec;
                    if (imaged_secs > L.old_total) imaged_secs = L.old_total;
                    sprintf(llbl, "logical %d (FAT%d)", lidx, L.fat_bits);
                    printf("  logical %d (FAT%d) lba %lu: %lu KiB (same-size)\n", lidx, L.fat_bits,
                           (unsigned long)logs[j].start_lba, (unsigned long)(logs[j].count * 512 / 1024));
                    if (clone_partition(&sdi, sdrive, &tdi, tdrive, logs[j].start_lba, logs[j].count * 512ULL,
                                        &L, fat, imaged_secs, llbl) != 0) { free(fat); xfer_free(); return 1; }
                    free(fat);
                    cloned++;
                } else if (ntfs_is_ntfs(lvbr)) {
                    ntfs_vol_t nv;
                    if (ntfs_parse(lvbr, &nv) != 0) { printf("  logical %d: NTFS BPB unsupported -- skipped\n", lidx); continue; }
                    uint8_t *nbm = NULL;
                    if (ntfs_load_bitmap(&sdi, sdrive, logs[j].start_lba, &nv, &nbm) != 0) { printf("  logical %d: NTFS $Bitmap read failed -- skipped\n", lidx); continue; }
                    sprintf(llbl, "logical %d (NTFS)", lidx);
                    printf("  logical %d (NTFS) lba %lu: %lu KiB (same-size)\n", lidx,
                           (unsigned long)logs[j].start_lba, (unsigned long)(logs[j].count * 512 / 1024));
                    if (clone_ntfs_partition(&sdi, sdrive, &tdi, tdrive, logs[j].start_lba, logs[j].count, &nv, nbm, llbl) != 0) { free(nbm); xfer_free(); return 1; }
                    free(nbm);
                    cloned++;
                } else {
                    printf("  logical %d: type 0x%02X not FAT/NTFS -- skipped\n", lidx, logs[j].type);
                }
            }
            continue;
        }

        if (has_filter && !(sel_mask & (1u << p->slot))) {
            printf("  slot %d: not in /PARTS -- skipped\n", p->slot);
            continue;
        }
        uint8_t vbr[512];
        if (read_lba(&sdi, sdrive, p->start, 1, vbr) != 0) {
            printf("  slot %d: VBR read failed -- skipped\n", p->slot);
            continue;
        }

        /* NTFS: same-size clone (no on-DOS resize), free clusters zeroed. */
        if (!is_fat_part_type(p->type)) {
            if (p->type != 0x07 || !ntfs_is_ntfs(vbr)) {
                printf("  slot %d: type 0x%02X not FAT/NTFS -- skipped\n", p->slot, p->type);
                continue;
            }
            ntfs_vol_t nv;
            if (ntfs_parse(vbr, &nv) != 0) {
                printf("  slot %d: NTFS BPB unsupported -- skipped\n", p->slot);
                continue;
            }
            uint8_t *nbm = NULL;
            if (ntfs_load_bitmap(&sdi, sdrive, p->start, &nv, &nbm) != 0) {
                printf("  slot %d: NTFS $Bitmap read failed -- skipped\n", p->slot);
                continue;
            }
            if (mode != SZ_ORIGINAL)
                printf("  slot %d: NTFS -- on-DOS resize unsupported, cloning same size "
                       "(resize on desktop)\n", p->slot);
            uint64_t win = p->count;                 /* same size -> MBR verbatim */
            uint64_t limit_sec = tgt_sectors;
            for (int j = 0; j < np; j++)
                if (P[j].start > p->start && P[j].start < limit_sec) limit_sec = P[j].start;
            if (p->start + win > tgt_sectors ||
                (limit_sec > p->start && win > limit_sec - p->start)) {
                printf("  slot %d: target too small for the NTFS partition -- aborting\n", p->slot);
                free(nbm); xfer_free(); return 1;
            }
            printf("  slot %d (NTFS) lba %lu: %lu KiB window (compacted)\n",
                   p->slot, (unsigned long)p->start, (unsigned long)(win * 512 / 1024));
            char nlbl[40];
            sprintf(nlbl, "slot %d (NTFS)", p->slot);
            if (clone_ntfs_partition(&sdi, sdrive, &tdi, tdrive, p->start, win, &nv, nbm, nlbl) != 0) {
                free(nbm); xfer_free(); return 1;
            }
            free(nbm);
            p->window_sec = win;
            cloned++;
            continue;
        }

        fatlay_t L;
        parse_fatlay(vbr, &L);
        if (!L.ok || L.bps != 512) {
            printf("  slot %d: not a 512-byte FAT -- skipped\n", p->slot);
            continue;
        }

        uint8_t *fat = malloc(L.old_spf * L.bps);
        if (!fat) { printf("  slot %d: out of memory\n", p->slot); xfer_free(); return 1; }
        if (load_region(&sdi, sdrive, p->start + L.reserved, L.old_spf * L.bps, fat) != 0) {
            printf("  slot %d: FAT read failed -- skipped\n", p->slot);
            free(fat); continue;
        }
        uint32_t last_used = 0;
        for (uint32_t cl = 2; cl < L.clusters + 2; cl++)
            if (fat_entry(fat, L.fat_bits, cl) != 0) last_used = cl;
        uint32_t imaged_secs = (last_used >= 2) ? L.first_data_sec + (last_used - 1) * L.spc : L.first_data_sec;
        if (imaged_secs > L.old_total) imaged_secs = L.old_total;

        uint64_t limit_sec = tgt_sectors;
        for (int j = 0; j < np; j++)
            if (P[j].start > p->start && P[j].start < limit_sec) limit_sec = P[j].start;
        uint64_t limit_window = (limit_sec > p->start) ? limit_sec - p->start : 0;

        uint64_t win;
        switch (mode) {
            case SZ_MINIMUM: win = imaged_secs; break;
            case SZ_ENTIRE:  win = limit_window; break;
            case SZ_CUSTOM:  win = round_up_512(custom_bytes) / 512; break;
            default:         win = p->count; break;
        }
        uint64_t cap = max_fat_window(&L);
        if (win > cap) {
            printf("  slot %d: FAT%d cluster limit -- window capped to %lu KiB\n",
                   p->slot, L.fat_bits, (unsigned long)(cap * 512 / 1024));
            win = cap;
        }
        if (win < imaged_secs) win = imaged_secs;
        uint64_t floor = min_fat_window(&L);
        if (win < floor) win = floor;
        if (limit_window && win > limit_window) win = limit_window;
        if (win < imaged_secs) {
            printf("  slot %d: target too small for used data (%lu KiB) -- aborting\n",
                   p->slot, (unsigned long)((uint64_t)imaged_secs * 512 / 1024));
            free(fat); xfer_free(); return 1;
        }
        p->window_sec = win;

        printf("  slot %d (FAT%d) lba %lu: %lu KiB used -> %lu KiB window\n",
               p->slot, L.fat_bits, (unsigned long)p->start,
               (unsigned long)((uint64_t)imaged_secs * 512 / 1024),
               (unsigned long)(win * 512 / 1024));
        char flbl[40];
        sprintf(flbl, "slot %d (FAT%d)", p->slot, L.fat_bits);
        if (clone_partition(&sdi, sdrive, &tdi, tdrive, p->start, win * 512ULL,
                            &L, fat, imaged_secs, flbl) != 0) {
            free(fat); xfer_free(); return 1;
        }
        free(fat);

        if ((uint32_t)win != L.old_total) {
            int r = fat_resize(&tdi, tdrive, p->start, (uint32_t)win, imaged_secs);
            if (r > 0) set_clean_flags(&tdi, tdrive, p->start);
        }
        cloned++;
    }

    if (cloned == 0) { printf("no partitions cloned\n"); xfer_free(); return 1; }

    {
        uint32_t h = tdi.heads, s = tdi.spt;
        for (int k = 0; k < np; k++) {
            clp_t *p = &P[k];
            uint8_t *ent = mbr + 446 + p->slot * 16;
            uint32_t ns = (uint32_t)p->window_sec;
            if (rd32(ent + 12) == ns) continue;          /* unchanged -- verbatim */
            wr32(ent + 12, ns);
            if (h && s) {
                uint32_t end = (uint32_t)p->start + (ns ? ns - 1 : 0);
                uint32_t ec = end / (h * s);
                uint32_t et = end % (h * s);
                uint32_t eh = et / s, es = et % s + 1;
                if (ec > 1023) { ec = 1023; eh = h - 1; es = s; }
                ent[5] = (uint8_t)eh;
                ent[6] = (uint8_t)(((ec >> 2) & 0xC0) | (es & 0x3F));
                ent[7] = (uint8_t)ec;
            }
        }
    }
    if (write_lba(&tdi, tdrive, 0, 1, mbr) != 0) { printf("target MBR write failed\n"); xfer_free(); return 1; }
    printf("wrote MBR\n");
    printf("clone complete: %d partition%s 0x%02X -> 0x%02X\n",
           cloned, cloned == 1 ? "" : "s", sdrive, tdrive);
    xfer_free();
    return 0;
}
