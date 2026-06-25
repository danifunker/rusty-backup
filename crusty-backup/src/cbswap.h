/* cbswap.h -- Level-1 swap/page-file exclusion for the backup compaction path
 * (Phase 7g, §6 of docs/cb_dos_network_and_state.md).
 *
 * Swap/page files churn every boot but carry no meaningful state. We identify
 * them by a strict allowlist (exact name + expected location + expected
 * attributes -- never a fuzzy match, so the DoubleSpace/DriveSpace/Stacker
 * container files are NEVER touched) and emit zeros for their content while
 * keeping the allocation intact: the file survives full-size, gzip crushes the
 * zeros, and the OS reinitializes swap on next boot. The manifest flags the same
 * files `volatile` / `content:zeroed` via cbswap_is_swap() so the change counter
 * keeps ignoring them and restore recreates them. */
#ifndef CBSWAP_H
#define CBSWAP_H

#include "cbbrowse.h"
#include <stdint.h>

typedef struct {
    char     name[16];   /* the matched 8.3 name (swap names are all 8.3) */
    uint32_t size;       /* file size in bytes (for the exclusion log) */
} cbswap_found_t;

/* Strict allowlist predicate (§6a): is the file (name, attr) at parent DOS path
 * `parent_path` ("" = root, "\WINDOWS", "\OS2\SYSTEM") a recognized swap/page
 * file? Shared by the compaction (which zeros its content) and the manifest
 * walk (which flags it volatile). Conservative -- name + location + attribs must
 * all match. */
int cbswap_is_swap(const char *name, uint8_t attr, const char *parent_path);

/* Walk the live volume `v` for allowlisted swap files; set each match's cluster-
 * chain bits in `bm` (bit i = cluster i, valid 2..nclusters+1) and record up to
 * `max_found` of them in `found`. Returns the number found (0 if none -- the
 * bitmap is left as the caller initialized it, so the caller may skip the mask).
 * `v` must already be open (cbk_open_vol_live). */
int cbswap_collect(fatvol_t *v, uint8_t *bm, uint32_t nclusters,
                   cbswap_found_t *found, int max_found);

#endif /* CBSWAP_H */
