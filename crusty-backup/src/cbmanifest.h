/* cbmanifest.h -- per-partition file manifest for the `backup` path (Phase 7f).
 *
 * Alongside the block image (partition-N.gz), backup emits a per-FAT-partition
 * `manifest-N.json` sidecar describing the filesystem at the file level: every
 * file/dir's path, size, write-mtime, attributes, and start cluster, plus a
 * `system` block fingerprinting the boot chain (the MBR boot code, the
 * partition's reserved/boot sectors, and the DOS system files in the root). See
 * docs/cb_dos_network_and_state.md §5.
 *
 * The manifest is **file-awareness + a change-detection gate**, not a restore
 * input: cb-dos restore is block-level (write_lba only), so directory entries --
 * and with them every mtime / attribute / archive bit -- round-trip verbatim
 * inside the image. A same-size backup -> restore -> backup is therefore already
 * a no-op, and re-building this manifest from the restored disk yields the byte-
 * identical document (the §5b "nothing changed" gate; the §5c int-21h attribute
 * replay a file-level tool would need is unnecessary here). The manifest rides
 * the folder / `.cbk` / network PUT as an ordinary member.
 *
 * Built by one read-only directory-tree walk over the live source, reusing the
 * cbbrowse FAT reader; the source disk is never written. FAT12/16/32 only (NTFS
 * has no on-DOS directory reader -- those partitions get no manifest). */
#ifndef CBMANIFEST_H
#define CBMANIFEST_H

#include "cbdisk.h"
#include <stdint.h>

/* Build the FAT partition at `start_lba` on BIOS `drive` into a JSON manifest.
 * The document is returned in a malloc'd buffer via *out_buf / *out_len (the
 * caller frees it). `mbr` is the 512-byte disk MBR (for the boot-code CRC).
 * `keep_swap` (the --keep-swap flag) controls only the swap files' `content`
 * field: when 0 they are flagged `content:zeroed` to match the zeroed payload,
 * when nonzero that flag is omitted (they were imaged verbatim). The caller must
 * already have called xfer_init() (the browse engine borrows the shared transfer
 * buffer for its int13h reads). Returns 0 on success, -1 on a hard error
 * (*out_buf untouched). */
int manifest_build_fat(const drive_info_t *di, int drive, uint64_t start_lba,
                       const uint8_t *mbr, int keep_swap, char **out_buf, uint32_t *out_len);

#endif /* CBMANIFEST_H */
