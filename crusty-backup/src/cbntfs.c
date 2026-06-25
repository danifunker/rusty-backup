/* cbntfs.c -- NTFS $Bitmap reader (see cbntfs.h).
 *
 * Just enough NTFS to find the cluster allocation bitmap for compaction:
 * BPB parse -> read MFT record #6 ($Bitmap) with the update-sequence fixup ->
 * decode its $DATA (resident inline, or non-resident run list) into a RAM
 * bitmap. No file enumeration, no driver. Ported from the disk_spike.c probe. */

#include "cbntfs.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

int ntfs_is_ntfs(const uint8_t *vbr) {
    return memcmp(vbr + 3, "NTFS    ", 8) == 0;
}

int ntfs_parse(const uint8_t *vbr, ntfs_vol_t *v) {
    memset(v, 0, sizeof *v);
    v->bytes_per_sec = rd16(vbr + 0x0B);
    v->sec_per_clus  = vbr[0x0D];
    /* int13h reads/writes 512-byte sectors; require that for the volume too. */
    if (v->bytes_per_sec != 512 || v->sec_per_clus == 0) return -1;
    v->total_secs   = rd64(vbr + 0x28);
    v->mft_lcn      = rd64(vbr + 0x30);
    v->cluster_size = (uint64_t)v->bytes_per_sec * v->sec_per_clus;
    int8_t raw = (int8_t)vbr[0x40];             /* clusters/MFT-record, signed */
    if (raw < 0) v->mft_rec_size = 1u << (-raw);
    else         v->mft_rec_size = (uint32_t)raw * v->cluster_size;
    if (v->mft_rec_size == 0 || v->mft_rec_size > 8192) return -1;
    if (v->total_secs == 0) return -1;
    v->total_clusters = v->total_secs / v->sec_per_clus;
    return 0;
}

/* Apply the NTFS update-sequence (fixup) array in place over `rec`. */
static void ntfs_fixup(uint8_t *rec, uint32_t rec_size, unsigned bps) {
    unsigned usa_off = rd16(rec + 0x04);
    unsigned usa_cnt = rd16(rec + 0x06);
    if (usa_cnt < 2 || usa_off + usa_cnt * 2 > rec_size) return;
    for (unsigned i = 1; i < usa_cnt; i++) {
        unsigned sector_end = i * bps;
        if (sector_end < 2 || sector_end > rec_size) break;
        rec[sector_end - 2] = rec[usa_off + i * 2];
        rec[sector_end - 1] = rec[usa_off + i * 2 + 1];
    }
}

/* Read MFT record `n`. The MFT's first extent is always contiguous at mft_lcn
 * and covers metadata records 0..15, so $Bitmap (#6) is a straight offset from
 * mft_lcn -- no need to chase $MFT's own runs. Returns 0 / -1. */
static int ntfs_read_mft_rec(const drive_info_t *di, int drive, uint64_t vol_lba,
                             const ntfs_vol_t *v, uint32_t n, uint8_t *rec) {
    uint64_t byte = v->mft_lcn * v->cluster_size + (uint64_t)n * v->mft_rec_size;
    uint64_t lba = vol_lba + byte / 512;
    int secs = (int)(v->mft_rec_size / 512);
    if (secs < 1) secs = 1;
    int done = 0;
    while (done < secs) {
        int chunk = secs - done;
        if (chunk > XFER_SECTORS) chunk = XFER_SECTORS;
        if (read_lba(di, drive, lba + done, chunk, rec + done * 512) != 0) return -1;
        done += chunk;
    }
    if (memcmp(rec, "FILE", 4) != 0) return -1;
    ntfs_fixup(rec, v->mft_rec_size, v->bytes_per_sec);
    return 0;
}

int ntfs_load_bitmap(const drive_info_t *di, int drive, uint64_t vol_lba,
                     const ntfs_vol_t *v, uint8_t **bitmap_out) {
    static uint8_t rec[8192];
    if (ntfs_read_mft_rec(di, drive, vol_lba, v, 6, rec) != 0) return -1;

    uint64_t bm_bytes = (v->total_clusters + 7) / 8;
    uint8_t *bm = malloc(bm_bytes ? (size_t)bm_bytes : 1);
    if (!bm) return -1;
    memset(bm, 0, bm_bytes ? (size_t)bm_bytes : 1);
    uint64_t bytepos = 0;                       /* write cursor into bm */

    unsigned pos = rd16(rec + 0x14);            /* first attribute offset */
    int found = 0;
    while (pos + 16 <= v->mft_rec_size) {
        uint32_t type = rd32(rec + pos);
        if (type == 0xFFFFFFFF || type == 0) break;
        uint32_t alen = rd32(rec + pos + 4);
        if (alen < 16 || pos + alen > v->mft_rec_size) break;
        if (type == 0x80) {                     /* $DATA -- the bitmap itself */
            found = 1;
            if (!rec[pos + 8]) {                /* resident: content inline */
                uint32_t clen = rd32(rec + pos + 0x10);
                unsigned coff = rd16(rec + pos + 0x14);
                for (uint32_t i = 0; i < clen && bytepos < bm_bytes; i++)
                    bm[bytepos++] = rec[pos + coff + i];
            } else {                            /* non-resident: decode runs */
                unsigned rp = pos + rd16(rec + pos + 0x20);
                int64_t lcn = 0;
                uint8_t buf[XFER_BYTES];
                while (rp < pos + alen && bytepos < bm_bytes) {
                    uint8_t hdr = rec[rp++];
                    if (hdr == 0) break;
                    int lsz = hdr & 0x0F, osz = (hdr >> 4) & 0x0F;
                    if (lsz == 0) break;
                    uint64_t rlen = 0;
                    for (int i = 0; i < lsz; i++)
                        rlen |= (uint64_t)rec[rp + i] << (i * 8);
                    rp += lsz;
                    if (osz == 0) {             /* sparse run: those bitmap bytes
                                                * are 0 (clusters free) -- skip */
                        bytepos += rlen * v->cluster_size;
                        continue;
                    }
                    int64_t off = 0;
                    for (int i = 0; i < osz; i++)
                        off |= (int64_t)rec[rp + i] << (i * 8);
                    if (osz < 8 && (rec[rp + osz - 1] & 0x80))
                        for (int i = osz; i < 8; i++)
                            off |= (int64_t)0xFF << (i * 8);
                    rp += osz;
                    lcn += off;

                    uint64_t run_lba = vol_lba + (uint64_t)lcn * v->sec_per_clus;
                    uint64_t run_secs = rlen * v->sec_per_clus, ds = 0;
                    while (ds < run_secs && bytepos < bm_bytes) {
                        int chunk = (int)(run_secs - ds);
                        if (chunk > XFER_SECTORS) chunk = XFER_SECTORS;
                        if (read_lba(di, drive, run_lba + ds, chunk, buf) != 0) {
                            free(bm); return -1;
                        }
                        int got = chunk * 512;
                        for (int byi = 0; byi < got && bytepos < bm_bytes; byi++)
                            bm[bytepos++] = buf[byi];
                        ds += chunk;
                    }
                }
            }
            break;
        }
        pos += alen;
    }
    if (!found) { free(bm); return -1; }
    *bitmap_out = bm;
    return 0;
}

int ntfs_cluster_used(const uint8_t *bitmap, uint64_t total_clusters, uint64_t lcn) {
    if (lcn >= total_clusters) return 1;        /* tail / backup boot sector */
    return (bitmap[lcn >> 3] >> (lcn & 7)) & 1;
}
