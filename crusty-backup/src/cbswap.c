/* cbswap.c -- Level-1 swap/page-file exclusion (Phase 7g, §6).
 *
 * Identifies swap/page files by a strict allowlist (name + location + attribs)
 * and marks their cluster chains so the compaction zeros that content while
 * keeping the allocation. See cbswap.h. The allowlist is deliberately small and
 * exact: DBLSPACE/DRVSPACE/STACVOL are NOT here -- those ARE the filesystem, and
 * excluding them would destroy the volume (§6a). */

#include "cbswap.h"
#include "cbbrowse.h"

#include <stdlib.h>
#include <string.h>

#define ATTR_HIDDEN 0x02
#define ATTR_SYS    0x04

enum { LOC_ROOT = 1, LOC_WIN = 2, LOC_OS2SYS = 4 };

typedef struct { const char *name; unsigned loc; uint8_t need_attr; } swap_rule_t;

/* §6a. need_attr is required (matched as (attr & need_attr) == need_attr); 0
 * means "no attribute requirement" (the name+location is specific enough). */
static const swap_rule_t SWAP_RULES[] = {
    { "386SPART.PAR", LOC_ROOT,           ATTR_HIDDEN },             /* Win3.x perm swap */
    { "WIN386.SWP",   LOC_ROOT | LOC_WIN, 0 },                      /* Win3.x temp / Win9x */
    { "PAGEFILE.SYS", LOC_ROOT,           ATTR_HIDDEN | ATTR_SYS }, /* NT/2k/XP page file */
    { "HIBERFIL.SYS", LOC_ROOT,           ATTR_HIDDEN | ATTR_SYS }, /* NT hibernation */
    { "SWAPPER.DAT",  LOC_OS2SYS,         0 },                      /* OS/2 swap */
};
#define N_SWAP_RULES ((int)(sizeof SWAP_RULES / sizeof SWAP_RULES[0]))

/* Map a parent DOS path to its location bit (case-insensitive); 0 if not a
 * directory any swap file is expected in. "" = root. */
static unsigned loc_of(const char *parent) {
    if (parent[0] == 0)                  return LOC_ROOT;
    if (eq_ci(parent, "\\WINDOWS"))      return LOC_WIN;
    if (eq_ci(parent, "\\OS2\\SYSTEM"))  return LOC_OS2SYS;
    return 0;
}

int cbswap_is_swap(const char *name, uint8_t attr, const char *parent_path) {
    unsigned loc = loc_of(parent_path);
    if (!loc) return 0;
    for (int i = 0; i < N_SWAP_RULES; i++) {
        const swap_rule_t *r = &SWAP_RULES[i];
        if (!(r->loc & loc)) continue;
        if (!eq_ci(name, r->name)) continue;
        if ((attr & r->need_attr) != r->need_attr) continue;
        return 1;
    }
    return 0;
}

static uint32_t eoc_threshold(int bits) {
    return (bits == 12) ? 0x0FF8u : (bits == 16) ? 0xFFF8u : 0x0FFFFFF8u;
}

/* Set the cluster-chain bits for the file starting at `first` (bit i = cluster i,
 * valid 2..nclusters+1). A guard bounds a corrupt/looping chain. */
static void mark_chain(fatvol_t *v, uint8_t *bm, uint32_t nclusters, uint32_t first) {
    uint32_t eoc = eoc_threshold(v->L.fat_bits);
    uint32_t cl = first, guard = 0;
    while (cl >= 2 && cl < nclusters + 2 && cl < eoc && guard <= nclusters) {
        bm[cl >> 3] |= (uint8_t)(1u << (cl & 7));
        cl = fat_entry(v->fat, v->L.fat_bits, cl);
        guard++;
    }
}

/* Scan one directory's entries for allowlisted swap files; mark + record each. */
static void scan_dir(fatvol_t *v, uint32_t cluster, int fixed_root, const char *parent,
                     uint8_t *bm, uint32_t nclusters,
                     cbswap_found_t *found, int max_found, int *nf) {
    dirent_t *ents = malloc(sizeof(dirent_t) * 256);
    if (!ents) return;
    int n = cbk_list_dir(v, cluster, fixed_root, ents, 256);
    for (int i = 0; i < n; i++) {
        if (ents[i].attr & CBK_ATTR_DIR) continue;
        if (!cbswap_is_swap(ents[i].name, ents[i].attr, parent)) continue;
        mark_chain(v, bm, nclusters, ents[i].first_cluster);
        if (*nf < max_found) {
            strncpy(found[*nf].name, ents[i].name, sizeof found[*nf].name - 1);
            found[*nf].name[sizeof found[*nf].name - 1] = 0;
            found[*nf].size = ents[i].size;
            (*nf)++;
        }
    }
    free(ents);
}

/* First cluster of subdir `name` under `cluster` (case-insensitive), or 0. */
static uint32_t find_subdir(fatvol_t *v, uint32_t cluster, int fixed_root, const char *name) {
    dirent_t *ents = malloc(sizeof(dirent_t) * 256);
    if (!ents) return 0;
    int n = cbk_list_dir(v, cluster, fixed_root, ents, 256);
    uint32_t fc = 0;
    for (int i = 0; i < n; i++)
        if ((ents[i].attr & CBK_ATTR_DIR) && eq_ci(ents[i].name, name)) {
            fc = ents[i].first_cluster;
            break;
        }
    free(ents);
    return fc;
}

int cbswap_collect(fatvol_t *v, uint8_t *bm, uint32_t nclusters,
                   cbswap_found_t *found, int max_found) {
    int nf = 0;
    int root_fixed = !v->L.is_fat32;
    uint32_t root = v->root_cluster;

    scan_dir(v, root, root_fixed, "", bm, nclusters, found, max_found, &nf);

    uint32_t win = find_subdir(v, root, root_fixed, "WINDOWS");
    if (win >= 2)
        scan_dir(v, win, 0, "\\WINDOWS", bm, nclusters, found, max_found, &nf);

    uint32_t os2 = find_subdir(v, root, root_fixed, "OS2");
    if (os2 >= 2) {
        uint32_t sys = find_subdir(v, os2, 0, "SYSTEM");
        if (sys >= 2)
            scan_dir(v, sys, 0, "\\OS2\\SYSTEM", bm, nclusters, found, max_found, &nf);
    }
    return nf;
}
