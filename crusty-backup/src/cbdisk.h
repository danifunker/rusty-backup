/* cbdisk.h -- shared cb-dos disk engine.
 *
 * The single source of truth for the int13h / geometry / FAT-parse / FAT-resize
 * / MBR primitives that used to be copied inline in cbbackup.c, cbrestore.c and
 * cbclone.c. Now that those are command modules (cmd_backup/restore/clone) inside
 * one CRUSTYBK.EXE, the primitives live here and are linked once.
 *
 * Disk I/O is BIOS int 13h (LBA AH=42h/43h with a CHS AH=02h/03h fallback) via a
 * conventional-memory transfer buffer. All sectors are 512-byte BIOS sectors.
 */
#ifndef CBDISK_H
#define CBDISK_H

#include <stdint.h>

#define XFER_SECTORS 16
#define XFER_BYTES   (XFER_SECTORS * 512)

/* DOS-memory transfer buffer (real-mode reachable). xfer_init returns the
 * segment (>0) on success, <0 on failure. */
int  xfer_init(void);
void xfer_free(void);

/* Little-endian field helpers. */
uint16_t rd16(const uint8_t *p);
uint32_t rd32(const uint8_t *p);
uint64_t rd64(const uint8_t *p);
void     wr16(uint8_t *p, uint16_t v);
void     wr32(uint8_t *p, uint32_t v);

/* ----- drive geometry + sector I/O ---------------------------------- */

typedef struct { int present, ext; unsigned cyls, heads, spt; } drive_info_t;

void     drive_params(int drive, drive_info_t *di);   /* AH=08h geometry */
int      drive_has_ext(int drive);                    /* AH=41h LBA-extensions */
uint64_t drive_total_sectors(const drive_info_t *di, int drive); /* AH=48h / CHS */

/* <= XFER_SECTORS sectors per call. 0 on success, else the BIOS AH error code. */
int read_lba(const drive_info_t *di, int drive, uint64_t lba, int count, void *dst);
int write_lba(const drive_info_t *di, int drive, uint64_t lba, int count, const void *src);

/* Sector-multiple byte regions, chunked into XFER_SECTORS calls. 0 / -1. */
int load_region(const drive_info_t *di, int drive, uint64_t lba, uint32_t bytes, uint8_t *dst);
int store_region(const drive_info_t *di, int drive, uint64_t lba, uint32_t bytes, const uint8_t *src);

/* ----- FAT layout + resize ------------------------------------------ */

typedef struct {
    int      ok;
    unsigned bps, spc, reserved, num_fats, root_entries;
    uint32_t old_spf, old_total, root_dir_secs, first_data_sec, clusters;
    int      is_fat32, fat_bits;
} fatlay_t;

void     parse_fatlay(const uint8_t *bpb, fatlay_t *L);    /* ok=0 if not a FAT BPB */
uint32_t fat_entry(const uint8_t *fat, int bits, uint32_t n); /* 0 == free cluster */
int      is_fat_part_type(uint8_t t);                      /* MBR FAT12/16/32 types */
int      is_extended_type(uint8_t t);                      /* 0x05 | 0x0F | 0x85 */

/* ----- extended-partition / EBR chain ------------------------------- */

/* A logical partition discovered by walking an extended partition's EBR chain. */
typedef struct {
    uint64_t start_lba;   /* absolute LBA of the logical partition's data */
    uint64_t count;       /* size in sectors */
    uint64_t ebr_lba;     /* absolute LBA of this partition's EBR sector */
    uint8_t  type;        /* MBR partition type byte */
} logical_t;

/* Walk the EBR chain of the extended partition based at `ext_base`. Fills `out`
 * (up to `max`) with the logical partitions in chain order, absolute LBAs.
 * Returns the count (>=0), or -1 on a read error. Loop/overflow guarded. */
int walk_ebr_chain(const drive_info_t *di, int drive, uint64_t ext_base,
                   logical_t *out, int max);

/* Reconstruct + write the EBR chain for `n` logical partitions (the `starts`/
 * `counts`/`types` arrays, sorted ascending by start) to the disk, mirroring the
 * desktop's build_ebr_chain: first EBR at `ext_base`, each later EBR one sector
 * before its logical. Returns 0 / -1. */
int write_ebr_chain(const drive_info_t *di, int drive, uint64_t ext_base,
                    const uint64_t *starts, const uint64_t *counts,
                    const uint8_t *types, int n);

uint64_t max_fat_window(const fatlay_t *L);   /* largest window keeping a valid FAT */
uint64_t min_fat_window(const fatlay_t *L);   /* smallest window keeping a valid FAT */

/* Resize the FAT at part_lba (on drive) to new_total sectors in place. imaged_secs
 * is the end of meaningful data (everything past it is zero). Returns 1 if changed,
 * 0 if no-op/non-FAT, -1 on error. The C port of resize_fat_in_place (src/fs/fat.rs). */
int  fat_resize(const drive_info_t *di, int drive, uint64_t part_lba,
                uint32_t new_total, uint32_t imaged_secs);

/* Set the FAT16/32 clean-shutdown flags in FAT[1] (each copy). FAT12 has none. */
void set_clean_flags(const drive_info_t *di, int drive, uint64_t part_lba);

/* ----- transfer progress (live %, speed, ETA on the console) -------- */

/* A live one-line progress meter for a single partition's transfer. Updates in
 * place with \r (percentage, transferred/total MiB, MiB/s, ETA) using the BIOS
 * tick counter, throttled to a few updates a second. Suppressed when stdout is
 * not a console (so redirected output keeps its clean per-partition summaries),
 * so both the CLI and the TUI operation screen show it but pipes/files don't. */
typedef struct {
    uint64_t      total;        /* total bytes this transfer will move */
    unsigned long start_tick;   /* BIOS tick at begin */
    unsigned long last_tick;    /* last drawn tick (throttle) */
    int           tty;          /* 1 if stdout is a console */
    int           shown;        /* a line has been drawn (finish -> newline) */
    char          label[40];
} progress_t;

void progress_begin(progress_t *p, const char *label, uint64_t total_bytes);
void progress_update(progress_t *p, uint64_t done_bytes);   /* throttled redraw */
void progress_finish(progress_t *p);                        /* terminate the line */

/* ----- command-parser helpers --------------------------------------- */

/* If arg begins with prefix (case-insensitive) return the rest, else NULL. */
const char *switch_val(const char *arg, const char *prefix);
int         eq_ci(const char *a, const char *b);
uint64_t    round_up_512(uint64_t v);
/* Parse a comma/space list of indices (0-31) into a bitmask. 0 ok / -1 empty. */
int         parse_parts(const char *v, unsigned *mask);

#endif /* CBDISK_H */
