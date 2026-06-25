/* cbdefrag.h -- file-level FAT defragmentation for the `backup /DEFRAG` path
 * (Phase 5).
 *
 * Reorders a FAT12/16/32 volume's files + directories into contiguous runs
 * packed toward the front of the data area, then emits the result as a normal
 * compacted `partition-N.gz` -- same on-disk format, just defragmented (and
 * smaller, since the last used cluster drops to ~the used-cluster count). The
 * source disk is **read-only** throughout: defrag builds the relocated image in
 * the gzip stream, never writing the source, so a bug can only produce a bad
 * backup (caught by verification), never corrupt the source.
 *
 * Boot-aware: any IO.SYS / MSDOS.SYS / IBMBIO.COM / IBMDOS.COM / KERNEL.SYS in
 * the root is pinned first + contiguous at the data-area start (the DOS `SYS`
 * rule) so the restored disk still boots.
 *
 * Conservative: if the filesystem is not provably clean -- lost (unreferenced)
 * clusters, bad clusters, cross-linked chains, an unreadable directory, or out
 * of memory -- defrag declines (returns 1) and the caller images the volume
 * as-is. The gzip stream is untouched on decline, so the fallback is seamless.
 */
#ifndef CBDEFRAG_H
#define CBDEFRAG_H

#include "cbdisk.h"
#include <zlib.h>

/* Defrag the FAT volume at `start_lba` on `drive` and write the defragmented,
 * compacted partition image to the already-open gzip stream `gz`. `L` is the
 * parsed BPB and `fat` the whole first FAT (already loaded by the caller).
 * `label` is the progress-line label and `pr` the caller's progress meter
 * (defrag drives it). On success the imaged sector count is returned via
 * `*out_imaged_secs`.
 *
 * Returns 0 on success (defragged image fully written to gz),
 *         1 if defrag declined (gz untouched -- caller should image as-is),
 *        -1 on a hard error mid-emit (gz partially written -- abort the backup).
 */
int defrag_backup_fat(const drive_info_t *di, int drive, uint64_t start_lba,
                      const fatlay_t *L, uint8_t *fat, gzFile gz,
                      const char *label, progress_t *pr, uint32_t *out_imaged_secs);

#endif /* CBDEFRAG_H */
