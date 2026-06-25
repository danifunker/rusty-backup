/* cbntfs.h -- minimal NTFS $Bitmap reader for cb-dos backup/clone compaction.
 *
 * We do NOT implement an NTFS driver. For compaction we need exactly one thing:
 * the volume's cluster allocation bitmap, so we can image used clusters verbatim
 * and zero the free ones before gzip (same smart-compaction the FAT path does
 * from its FAT). The bitmap is the $DATA of $Bitmap (MFT record #6); reading it
 * means parsing the BPB, reading MFT #6 (with the fixup/update-sequence applied),
 * and decoding its $DATA runs. Lifted + extended from the proven disk_spike.c
 * NTFS probe (which validated these counts bit-exact vs a host scan).
 *
 * Convention: in the NTFS $Bitmap a SET bit = allocated/used (the opposite of the
 * Amiga filesystems, but the same as FAT's "non-zero entry = used" sense here).
 * On-DOS NTFS resize is out of scope (heavy lift) -- cb-dos images/restores NTFS
 * same-size and routes resize through the desktop's resize_ntfs_in_place. */
#ifndef CBNTFS_H
#define CBNTFS_H

#include "cbdisk.h"

typedef struct {
    unsigned bytes_per_sec;     /* 512 required (int13h works in 512-byte sectors) */
    unsigned sec_per_clus;
    uint64_t total_secs;        /* sectors in the NTFS volume (BPB) */
    uint64_t mft_lcn;
    uint64_t cluster_size;      /* bytes_per_sec * sec_per_clus */
    uint64_t total_clusters;    /* total_secs / sec_per_clus */
    uint32_t mft_rec_size;      /* bytes per MFT record */
} ntfs_vol_t;

/* True if `vbr` (a partition's first sector) is an NTFS volume boot record
 * (OEM id "NTFS    "). Distinguishes NTFS from the other type-0x07 filesystems
 * (exFAT "EXFAT   ", HPFS), which we do not handle. */
int ntfs_is_ntfs(const uint8_t *vbr);

/* Parse the NTFS BPB. 0 on success, -1 if unsupported (non-512 sectors, or
 * insane geometry). */
int ntfs_parse(const uint8_t *vbr, ntfs_vol_t *v);

/* Read $Bitmap (MFT #6) at the partition starting at vol_lba and materialise the
 * cluster allocation bitmap into a malloc'd buffer (ceil(total_clusters/8)
 * bytes; bit set = used). The caller frees *bitmap_out. 0 / -1. */
int ntfs_load_bitmap(const drive_info_t *di, int drive, uint64_t vol_lba,
                     const ntfs_vol_t *v, uint8_t **bitmap_out);

/* Is cluster `lcn` allocated? Clusters at or beyond total_clusters (a partial
 * tail cluster, or the volume's backup boot sector) report used -> imaged
 * verbatim rather than zeroed. */
int ntfs_cluster_used(const uint8_t *bitmap, uint64_t total_clusters, uint64_t lcn);

#endif /* CBNTFS_H */
