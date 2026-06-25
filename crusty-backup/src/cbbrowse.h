/* cbbrowse.h -- the browse engine shared by the `ls`/`get` commands and the TUI
 * browse screen. A FAT12/16/32 directory reader + file extractor with two
 * interchangeable byte backends:
 *   - a compacted `partition-N.gz` from a backup folder (zlib gzseek random
 *     access), or
 *   - a *live* FAT partition on a BIOS drive (int13h read_lba).
 * Whichever backend is open, the directory walk + extract code is identical --
 * the engine only cares how to read a byte range at a partition offset. The
 * whole first FAT is cached in RAM; directories + file data read on demand. No
 * scratch space, no full restore. */
#ifndef CBBROWSE_H
#define CBBROWSE_H

#include "cbdisk.h"
#include <zlib.h>

typedef struct {
    /* Read backend: a backup gz when gz != NULL, otherwise a live BIOS disk
     * (di/drive/part_lba). Exactly one is active for the volume's lifetime. */
    gzFile       gz;
    drive_info_t di;            /* live backend: drive geometry for int13h */
    int          drive;         /* live backend: BIOS drive number (0x80..) */
    uint64_t     part_lba;      /* live backend: partition start LBA */

    fatlay_t L;
    uint8_t *fat;
    uint32_t fat_bytes;
    uint32_t root_off, root_bytes, root_cluster;
} fatvol_t;

typedef struct {
    char     name[256];      /* long name if present, else 8.3 */
    uint8_t  attr;
    uint32_t first_cluster;
    uint32_t size;
} dirent_t;

#define CBK_ATTR_DIR 0x10

/* Pick the partition: explicit (>=0) or the first partition-N.gz present. */
int  cbk_default_part(const char *folder, int part);

/* Open partition-N.gz of `folder` for browsing (parses BPB, loads the FAT). */
int  cbk_open_vol(const char *folder, int part, fatvol_t *v);

/* Open a *live* FAT partition at `part_lba` on BIOS drive `drive` for browsing.
 * Read-only on the source disk. The caller must have already done xfer_init()
 * (the engine reuses the shared transfer buffer for its int13h reads). 0 / -1. */
int  cbk_open_vol_live(const drive_info_t *di, int drive, uint64_t part_lba, fatvol_t *v);

void cbk_close_vol(fatvol_t *v);

/* List a directory's entries into `out` (skips '.'/'..'). `cluster`+`fixed_root`
 * select the FAT12/16 root region, the FAT32 root cluster, or a subdir's first
 * cluster. Returns the count (<= max), or -1 on error. */
int  cbk_list_dir(fatvol_t *v, uint32_t cluster, int fixed_root, dirent_t *out, int max);

/* Extract one file's cluster chain to a DOS path. 0 / -1. */
int  cbk_extract(fatvol_t *v, const dirent_t *f, const char *dest);

/* Recursively extract a directory (by start cluster) into `dest_dir`
 * (created if absent). 0 on success, -1 if any file failed. */
int  cbk_extract_tree(fatvol_t *v, uint32_t cluster, const char *dest_dir);

#endif /* CBBROWSE_H */
