/* cbbrowse.h -- the backup-browse engine shared by the `ls`/`get` commands and
 * the TUI browse screen. A FAT12/16/32 directory reader + file extractor that
 * works straight out of a compacted `partition-N.gz` (zlib gzseek random access,
 * the whole first FAT cached in RAM). No scratch space, no full restore. */
#ifndef CBBROWSE_H
#define CBBROWSE_H

#include "cbdisk.h"
#include <zlib.h>

typedef struct {
    gzFile   gz;
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
