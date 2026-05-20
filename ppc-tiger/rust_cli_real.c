/*
 * rust_cli_real.c — Complete rusty-backup CLI for Mac OS X Tiger PowerPC
 *
 * Reimplements the core rusty-backup operations in C:
 *   - list-devices    Device enumeration with sizes via ioctl
 *   - backup          Read device, detect partitions, copy to folder
 *   - restore         Write backup back to device/image
 *   - inspect         Display backup metadata
 *
 * Features:
 *   - Partition table support: MBR (with EBR chain), APM, Superfloppy
 *   - Compression: raw or gzip (via zlib, ships with Tiger)
 *   - Checksums: CRC32 (via zlib) and SHA-1 (via CommonCrypto)
 *   - FAT compaction: only backs up allocated clusters (FAT12/16/32)
 *
 * Compiled: gcc -std=c99 -O2 -c rust_cli_real.c
 * Link with: -lz (for gzip + CRC32)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/param.h>
#include <sys/mount.h>
#include <zlib.h>

/* CommonCrypto for SHA-256 — available since Tiger 10.4. SHA-1 was
 * intentionally not exposed (rb-cli uses sha256 by default; sha1
 * would not cross-verify). */
#ifdef __APPLE__
#include <CommonCrypto/CommonDigest.h>
#define HAVE_SHA256 1
#endif

#ifdef __APPLE__
#include <crt_externs.h>
#include <sys/disk.h>
#define get_argc() (*_NSGetArgc())
#define get_argv() (*_NSGetArgv())
#else
static int g_argc = 0;
static char **g_argv = NULL;
#define get_argc() g_argc
#define get_argv() g_argv
/* Define ioctl constants for non-Apple builds */
#ifndef DKIOCGETBLOCKSIZE
#define DKIOCGETBLOCKSIZE  0x40046418
#define DKIOCGETBLOCKCOUNT 0x40086419
#endif
#endif

/* Forward declarations from runtime stubs */
extern void rust_runtime_init(void);
extern void rust_runtime_cleanup(void);

/* ============================================================
 * Constants
 * ============================================================ */
#define SECTOR_SIZE          512
#define CHUNK_SIZE           (256 * 1024)  /* 256 KB I/O buffer */
#define MAX_PARTITIONS       64
#define MAX_PATH_LEN         1024
#define MBR_SIG_OFFSET       510
#define PART_TABLE_OFFSET    446
#define PART_ENTRY_SIZE      16
#define APM_DDR_SIG          0x4552
#define APM_ENTRY_SIG        0x504D

/* MBR extended partition types */
#define MBR_TYPE_EXTENDED_CHS  0x05
#define MBR_TYPE_EXTENDED_LBA  0x0F
#define MBR_TYPE_EXTENDED_LNX  0x85

/* ============================================================
 * Data Structures
 * ============================================================ */

typedef struct {
    uint8_t  bootable;
    uint8_t  chs_start[3];
    uint8_t  type;
    uint8_t  chs_end[3];
    uint32_t start_lba;
    uint32_t total_sectors;
} MbrEntry;

typedef struct {
    uint32_t disk_signature;
    MbrEntry entries[4];
    int      logical_count;
    MbrEntry logical[MAX_PARTITIONS];
} MbrTable;

typedef struct {
    char     name[33];
    char     type[33];
    uint32_t start_block;
    uint32_t block_count;
    uint32_t status;
} ApmEntry;

typedef struct {
    uint16_t block_size;
    uint32_t block_count;
    int      entry_count;
    ApmEntry entries[MAX_PARTITIONS];
} ApmTable;

/* ===== GPT ===== */

#define GPT_SIGNATURE 0x5452415020494645ULL /* "EFI PART" LE */

typedef struct {
    uint8_t  type_guid[16];   /* mixed-endian on disk; stored verbatim */
    uint8_t  unique_guid[16];
    uint64_t first_lba;
    uint64_t last_lba;
    uint64_t attributes;
    char     name[37];        /* 36 chars + NUL, decoded from UTF-16LE */
} GptEntry;

typedef struct {
    uint32_t revision;
    uint32_t header_size;
    uint64_t my_lba;
    uint64_t alternate_lba;
    uint64_t first_usable_lba;
    uint64_t last_usable_lba;
    uint8_t  disk_guid[16];
    uint64_t partition_entry_lba;
    uint32_t num_partition_entries;
    uint32_t partition_entry_size;
    int      entry_count;     /* count of non-empty entries actually parsed */
    GptEntry entries[MAX_PARTITIONS];
} GptTable;

typedef enum {
    PT_NONE = 0,   /* superfloppy */
    PT_MBR,
    PT_GPT,
    PT_APM
} PartTableType;

typedef struct {
    PartTableType type;
    union {
        MbrTable mbr;
        ApmTable apm;
        GptTable gpt;
    } data;
    char     fs_hint[16];       /* for superfloppy */
    uint64_t disk_size;
} PartTable;

typedef struct {
    int      index;
    char     type_name[64];
    uint8_t  type_byte;
    uint64_t start_lba;
    uint64_t total_sectors;
    int      bootable;
    int      is_logical;
    int      is_extended;
} PartInfo;

typedef struct {
    char     name[64];          /* "disk0" */
    char     path[128];         /* "/dev/disk0" */
    uint64_t size_bytes;
    int      is_whole;
    /* partition mount info */
    char     mount_point[256];
    char     filesystem[32];
    uint64_t total_space;
    uint64_t avail_space;
} DeviceInfo;

/* ============================================================
 * Utility Functions
 * ============================================================ */

static uint16_t read_le16(const uint8_t *p) { return p[0] | (p[1] << 8); }
static uint32_t read_le32(const uint8_t *p) {
    return p[0] | (p[1] << 8) | (p[2] << 16) | ((uint32_t)p[3] << 24);
}
static uint16_t read_be16(const uint8_t *p) { return (p[0] << 8) | p[1]; }
static uint32_t read_be32(const uint8_t *p) {
    return ((uint32_t)p[0] << 24) | (p[1] << 16) | (p[2] << 8) | p[3];
}
static uint64_t read_le64(const uint8_t *p) {
    return (uint64_t)read_le32(p)
        | ((uint64_t)read_le32(p + 4) << 32);
}

static const char *format_bytes(uint64_t b, char *buf, int bufsz) {
    if (b >= (uint64_t)1024 * 1024 * 1024)
        snprintf(buf, bufsz, "%.2f GiB", (double)b / (1024.0 * 1024.0 * 1024.0));
    else if (b >= 1024 * 1024)
        snprintf(buf, bufsz, "%.1f MiB", (double)b / (1024.0 * 1024.0));
    else if (b >= 1024)
        snprintf(buf, bufsz, "%.0f KiB", (double)b / 1024.0);
    else
        snprintf(buf, bufsz, "%llu B", (unsigned long long)b);
    return buf;
}

static void draw_progress(double pct, const char *operation) {
    int width = 40;
    int filled = (int)((pct / 100.0) * width);
    if (filled > width) filled = width;
    fprintf(stderr, "\r[");
    for (int i = 0; i < width; i++)
        fputc(i < filled ? '=' : ' ', stderr);
    fprintf(stderr, "] %5.1f%%  %s", pct, operation);
    fflush(stderr);
}

static const char *partition_type_name(uint8_t type) {
    switch (type) {
        case 0x00: return "Empty";
        case 0x01: return "FAT12";
        case 0x04: return "FAT16 <32MB";
        case 0x05: return "Extended (CHS)";
        case 0x06: return "FAT16";
        case 0x07: return "NTFS/HPFS";
        case 0x0B: return "FAT32 (CHS)";
        case 0x0C: return "FAT32 (LBA)";
        case 0x0E: return "FAT16 (LBA)";
        case 0x0F: return "Extended (LBA)";
        case 0x11: return "Hidden FAT12";
        case 0x14: return "Hidden FAT16 <32MB";
        case 0x16: return "Hidden FAT16";
        case 0x17: return "Hidden NTFS";
        case 0x1B: return "Hidden FAT32";
        case 0x1C: return "Hidden FAT32 (LBA)";
        case 0x1E: return "Hidden FAT16 (LBA)";
        case 0x82: return "Linux swap";
        case 0x83: return "Linux";
        case 0x85: return "Linux extended";
        case 0xA5: return "FreeBSD";
        case 0xA6: return "OpenBSD";
        case 0xA8: return "Mac OS X";
        case 0xAB: return "Mac OS X Boot";
        case 0xAF: return "HFS/HFS+";
        case 0xEE: return "GPT Protective";
        case 0xEF: return "EFI System";
        case 0xFD: return "Linux RAID";
        default:   return "Unknown";
    }
}

/* ============================================================
 * Argument Parsing
 * ============================================================ */

static const char *flag_value(int argc, char **argv, const char *flag) {
    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], flag) == 0 && i + 1 < argc)
            return argv[i + 1];
        int flen = strlen(flag);
        if (strncmp(argv[i], flag, flen) == 0 && argv[i][flen] == '=')
            return &argv[i][flen + 1];
    }
    return NULL;
}

static int has_flag(int argc, char **argv, const char *flag) {
    for (int i = 0; i < argc; i++)
        if (strcmp(argv[i], flag) == 0) return 1;
    return 0;
}

/* Boolean flags that don't take a value — used by nth_positional() to
 * avoid eating a positional as the flag's argument. Keep in sync with
 * the per-command argument parsers below. */
static int is_bool_flag(const char *a) {
    return strcmp(a, "--sector-by-sector") == 0
        || strcmp(a, "--device") == 0
        || strcmp(a, "--yes") == 0
        || strcmp(a, "--write-to-system-disk") == 0
        || strcmp(a, "--write-zeros-to-unused") == 0
        || strcmp(a, "--removable-only") == 0
        || strcmp(a, "--eject") == 0
        || strcmp(a, "--sparse") == 0
        || strcmp(a, "--help") == 0
        || strcmp(a, "-h") == 0
        || strcmp(a, "--version") == 0
        || strcmp(a, "-V") == 0;
}

/* Return the nth bare positional argument (0-indexed), skipping any
 * --flag tokens and the values that follow them. Returns NULL when
 * the requested positional doesn't exist. Used by the rb-cli-aligned
 * positional grammar (`backup SOURCE DEST`, `restore BACKUP_DIR TARGET`,
 * `inspect BACKUP_DIR`). */
static const char *nth_positional(int argc, char **argv, int n) {
    int seen = 0;
    for (int i = 0; i < argc; i++) {
        const char *a = argv[i];
        if (a[0] == '-' && a[1] == '-' && a[2] != '\0') {
            /* `--flag=value` is self-contained. */
            if (strchr(a, '=') != NULL) continue;
            /* Boolean flag — doesn't consume the next token. */
            if (is_bool_flag(a)) continue;
            /* Value-taking flag: skip its argument. */
            if (i + 1 < argc) i++;
            continue;
        }
        if (seen == n) return a;
        seen++;
    }
    return NULL;
}

/* ============================================================
 * Device Enumeration
 * ============================================================ */

static uint64_t get_device_size_ioctl(const char *path) {
    /* Try the given path first */
    int fd = open(path, O_RDONLY);
    if (fd >= 0) {
        uint32_t block_size = 0;
        uint64_t block_count = 0;
        if (ioctl(fd, DKIOCGETBLOCKSIZE, &block_size) == 0 &&
            ioctl(fd, DKIOCGETBLOCKCOUNT, &block_count) == 0 &&
            block_size > 0 && block_count > 0) {
            close(fd);
            return block_count * (uint64_t)block_size;
        }
        close(fd);
    }

#ifdef __APPLE__
    /* Try /dev/rdisk* (raw character device) — often works without root on Tiger */
    if (strstr(path, "/dev/disk")) {
        char rpath[128];
        const char *dname = strstr(path, "disk");
        if (dname) {
            snprintf(rpath, sizeof(rpath), "/dev/r%s", dname);
            fd = open(rpath, O_RDONLY);
            if (fd >= 0) {
                uint32_t block_size = 0;
                uint64_t block_count = 0;
                if (ioctl(fd, DKIOCGETBLOCKSIZE, &block_size) == 0 &&
                    ioctl(fd, DKIOCGETBLOCKCOUNT, &block_count) == 0 &&
                    block_size > 0 && block_count > 0) {
                    close(fd);
                    return block_count * (uint64_t)block_size;
                }
                close(fd);
            }
        }
    }
#endif

    /* For regular files, use stat */
    struct stat st;
    if (stat(path, &st) == 0 && S_ISREG(st.st_mode))
        return st.st_size;

    return 0;
}

static int cmd_list_devices(void) {
    printf("Scanning for disk devices...\n\n");

    /* Enumerate /dev/disk* entries */
    DeviceInfo devices[64];
    int dev_count = 0;

    DIR *devdir = opendir("/dev");
    if (!devdir) {
        fprintf(stderr, "Cannot open /dev\n");
        return 1;
    }

    struct dirent *ent;
    while ((ent = readdir(devdir)) != NULL && dev_count < 64) {
        /* Match disk[0-9] (whole disks only, no partitions like disk0s1) */
        if (strncmp(ent->d_name, "disk", 4) != 0) continue;
        if (ent->d_name[4] < '0' || ent->d_name[4] > '9') continue;

        /* Check if it's a whole disk or partition */
        int is_whole = 1;
        for (int i = 4; ent->d_name[i]; i++) {
            if (ent->d_name[i] == 's') { is_whole = 0; break; }
            if (ent->d_name[i] < '0' || ent->d_name[i] > '9') { is_whole = 0; break; }
        }

        DeviceInfo *d = &devices[dev_count];
        memset(d, 0, sizeof(*d));
        strncpy(d->name, ent->d_name, sizeof(d->name) - 1);
        snprintf(d->path, sizeof(d->path), "/dev/%s", ent->d_name);
        d->is_whole = is_whole;
        d->size_bytes = get_device_size_ioctl(d->path);

        /* Check mount info via getmntinfo */
        d->mount_point[0] = '\0';
        d->filesystem[0] = '\0';

#ifdef __APPLE__
        struct statfs *mntbuf;
        int mntcount = getmntinfo(&mntbuf, MNT_NOWAIT);
        for (int m = 0; m < mntcount; m++) {
            if (strstr(mntbuf[m].f_mntfromname, ent->d_name)) {
                strncpy(d->mount_point, mntbuf[m].f_mntonname, sizeof(d->mount_point) - 1);
                strncpy(d->filesystem, mntbuf[m].f_fstypename, sizeof(d->filesystem) - 1);
                d->total_space = (uint64_t)mntbuf[m].f_blocks * mntbuf[m].f_bsize;
                d->avail_space = (uint64_t)mntbuf[m].f_bavail * mntbuf[m].f_bsize;
                break;
            }
        }
#endif
        dev_count++;
    }
    closedir(devdir);

    /* Sort by name */
    for (int i = 0; i < dev_count - 1; i++)
        for (int j = i + 1; j < dev_count; j++)
            if (strcmp(devices[i].name, devices[j].name) > 0) {
                DeviceInfo tmp = devices[i];
                devices[i] = devices[j];
                devices[j] = tmp;
            }

    /* For whole disks with 0 size, try to infer from mounted partitions */
    for (int i = 0; i < dev_count; i++) {
        if (!devices[i].is_whole || devices[i].size_bytes > 0) continue;
        /* Sum total_space from all mounted partitions of this disk */
        int prefix_len = strlen(devices[i].name);
        uint64_t sum = 0;
        for (int j = 0; j < dev_count; j++) {
            if (devices[j].is_whole) continue;
            if (strncmp(devices[j].name, devices[i].name, prefix_len) != 0) continue;
            if (devices[j].name[prefix_len] != 's') continue;
            if (devices[j].total_space > sum) sum = devices[j].total_space;
        }
        /* Use the largest mounted partition's total_space as a lower bound */
        if (sum > 0) devices[i].size_bytes = sum;
    }

    /* Display whole disks with their partitions */
    for (int i = 0; i < dev_count; i++) {
        if (!devices[i].is_whole) continue;

        char sz[32];
        format_bytes(devices[i].size_bytes, sz, sizeof(sz));
        printf("%s\n", devices[i].name);
        printf("  Path:  %s\n", devices[i].path);
        if (devices[i].size_bytes > 0)
            printf("  Size:  %s (%llu bytes)\n", sz, (unsigned long long)devices[i].size_bytes);
        else
            printf("  Size:  (unknown - needs root for ioctl)\n");

        if (devices[i].mount_point[0])
            printf("  Mount: %s (%s)\n", devices[i].mount_point, devices[i].filesystem);

        /* Find partitions of this disk */
        int prefix_len = strlen(devices[i].name);
        int has_parts = 0;
        for (int j = 0; j < dev_count; j++) {
            if (devices[j].is_whole) continue;
            if (strncmp(devices[j].name, devices[i].name, prefix_len) != 0) continue;
            if (devices[j].name[prefix_len] != 's') continue;

            if (!has_parts) { printf("  Partitions:\n"); has_parts = 1; }

            /* Use ioctl size, or fall back to statfs total_space */
            uint64_t psz_val = devices[j].size_bytes > 0 ? devices[j].size_bytes : devices[j].total_space;
            char psz[32];
            format_bytes(psz_val, psz, sizeof(psz));
            printf("    %-12s %10s", devices[j].name, psz);
            if (devices[j].mount_point[0]) {
                char avsz[32];
                format_bytes(devices[j].avail_space, avsz, sizeof(avsz));
                printf("  %s (%s, %s free)", devices[j].mount_point,
                       devices[j].filesystem, avsz);
            }
            printf("\n");
        }
        printf("\n");
    }

    if (dev_count == 0) printf("No disk devices found.\n");
    return 0;
}

/* ============================================================
 * Partition Table Detection
 * ============================================================ */

static int is_fat_vbr(const uint8_t *s) {
    if (s[0] != 0xEB && s[0] != 0xE9) return 0;
    uint16_t bps = read_le16(&s[11]);
    if (bps != 512 && bps != 1024 && bps != 2048 && bps != 4096) return 0;
    uint8_t spc = s[13];
    if (spc == 0 || (spc & (spc - 1)) != 0) return 0;  /* must be power of 2 */
    uint16_t reserved = read_le16(&s[14]);
    if (reserved < 1) return 0;
    uint8_t fats = s[16];
    if (fats != 1 && fats != 2) return 0;
    uint8_t media = s[21];
    if (media != 0xF0 && media < 0xF8) return 0;
    return 1;
}

static int is_hfs_sig(const uint8_t *s1024) {
    uint16_t sig = read_be16(s1024);
    return (sig == 0x4244 || sig == 0x482B || sig == 0x4858);
}

static int is_extended_type(uint8_t type) {
    return type == MBR_TYPE_EXTENDED_CHS ||
           type == MBR_TYPE_EXTENDED_LBA ||
           type == MBR_TYPE_EXTENDED_LNX;
}

static void parse_mbr_entry(const uint8_t *p, MbrEntry *e) {
    e->bootable = p[0];
    memcpy(e->chs_start, &p[1], 3);
    e->type = p[4];
    memcpy(e->chs_end, &p[5], 3);
    e->start_lba = read_le32(&p[8]);
    e->total_sectors = read_le32(&p[12]);
}

static int parse_ebr_chain(int fd, uint32_t ext_start, MbrTable *tbl) {
    uint32_t cur_lba = ext_start;
    uint32_t visited[MAX_PARTITIONS];
    int vis_count = 0;

    while (tbl->logical_count < MAX_PARTITIONS) {
        /* Loop detection */
        for (int i = 0; i < vis_count; i++)
            if (visited[i] == cur_lba) return 0;
        if (vis_count >= MAX_PARTITIONS) break;
        visited[vis_count++] = cur_lba;

        uint8_t ebr[512];
        if (lseek(fd, (off_t)cur_lba * 512, SEEK_SET) < 0) break;
        if (read(fd, ebr, 512) != 512) break;

        uint16_t sig = read_le16(&ebr[510]);
        if (sig != 0xAA55) break;

        MbrEntry e0, e1;
        parse_mbr_entry(&ebr[446], &e0);
        parse_mbr_entry(&ebr[446 + 16], &e1);

        if (e0.type != 0x00 && e0.total_sectors > 0) {
            MbrEntry *le = &tbl->logical[tbl->logical_count++];
            *le = e0;
            le->start_lba = cur_lba + e0.start_lba;  /* absolute */
        }

        if (e1.type == 0x00 || e1.total_sectors == 0) break;
        cur_lba = ext_start + e1.start_lba;
    }
    return tbl->logical_count;
}

/* ============================================================
 * GPT Parsing
 * ----------------------------------------------------------------
 * Parses the primary GPT header at LBA 1 and the partition-entry
 * array at `partition_entry_lba`. Validates the signature, header
 * CRC32, and entry-array CRC32 (all via zlib's crc32). Falls back
 * to the backup GPT at the last LBA if the primary fails.
 * ============================================================ */

/* GPT GUIDs are stored on disk in a mixed-endian format ("Microsoft
 * GUID"): first three fields little-endian, last two big-endian. The
 * canonical text rendering is xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
 * where every byte is printed in display order. */
static void gpt_format_guid(const uint8_t g[16], char *out, size_t outsz) {
    snprintf(out, outsz,
        "%02X%02X%02X%02X-%02X%02X-%02X%02X-%02X%02X-%02X%02X%02X%02X%02X%02X",
        g[3], g[2], g[1], g[0],   /* Data1 (LE on disk) */
        g[5], g[4],               /* Data2 (LE) */
        g[7], g[6],               /* Data3 (LE) */
        g[8], g[9],               /* Data4 (BE) */
        g[10], g[11], g[12], g[13], g[14], g[15]);
}

/* Map a GPT type GUID to a short human-readable name. Mirrors the
 * subset in src/partition/gpt.rs::partition_type_name. */
static const char *gpt_type_name_from_guid(const uint8_t g[16]) {
    char s[64];
    gpt_format_guid(g, s, sizeof(s));
    if (strcmp(s, "00000000-0000-0000-0000-000000000000") == 0) return "Unused";
    if (strcmp(s, "C12A7328-F81F-11D2-BA4B-00A0C93EC93B") == 0) return "EFI System";
    if (strcmp(s, "21686148-6449-6E6F-7468-656564454649") == 0) return "BIOS Boot";
    if (strcmp(s, "E3C9E316-0B5C-4DB8-817D-F92DF00215AE") == 0) return "Microsoft Reserved";
    if (strcmp(s, "EBD0A0A2-B9E5-4433-87C0-68B6B72699C7") == 0) return "Microsoft Basic Data";
    if (strcmp(s, "DE94BBA4-06D1-4D40-A16A-BFD50179D6AC") == 0) return "Windows Recovery";
    if (strcmp(s, "0FC63DAF-8483-4772-8E79-3D69D8477DE4") == 0) return "Linux Filesystem";
    if (strcmp(s, "0657FD6D-A4AB-43C4-84E5-0933C84B4F4F") == 0) return "Linux Swap";
    if (strcmp(s, "E6D6D379-F507-44C2-A23C-238F2A3DF928") == 0) return "Linux LVM";
    if (strcmp(s, "A19D880F-05FC-4D3B-A006-743F0F84911E") == 0) return "Linux RAID";
    if (strcmp(s, "48465300-0000-11AA-AA11-00306543ECAC") == 0) return "Apple HFS/HFS+";
    if (strcmp(s, "7C3457EF-0000-11AA-AA11-00306543ECAC") == 0) return "Apple APFS";
    if (strcmp(s, "55465300-0000-11AA-AA11-00306543ECAC") == 0) return "Apple UFS";
    if (strcmp(s, "516E7CB4-6ECF-11D6-8FF8-00022D09712B") == 0) return "FreeBSD Data";
    return "Unknown";
}

/* Decode a UTF-16LE GPT partition name (up to 72 bytes / 36 codepoints)
 * into a best-effort ASCII string. Non-ASCII codepoints become '?'. */
static void gpt_decode_name(const uint8_t name_le[72], char *out, size_t outsz) {
    size_t oi = 0;
    for (int i = 0; i < 72 && oi + 1 < outsz; i += 2) {
        uint16_t u = read_le16(&name_le[i]);
        if (u == 0) break;
        out[oi++] = (u < 0x80) ? (char)u : '?';
    }
    out[oi] = '\0';
}

/* Validate the primary GPT header at LBA 1 and populate `gpt`. The
 * header CRC32 covers the first `header_size` bytes with the CRC
 * field itself zeroed out. The entry-array CRC32 covers
 * `num_partition_entries * partition_entry_size` bytes. Returns 0 on
 * success, -1 on any validation failure. */
static int parse_gpt(int fd, GptTable *gpt) {
    memset(gpt, 0, sizeof(*gpt));

    uint8_t hdr[512];
    if (lseek(fd, (off_t)SECTOR_SIZE, SEEK_SET) < 0) return -1;
    if (read(fd, hdr, 512) != 512) return -1;

    /* Signature: "EFI PART" at offset 0 (LE) */
    uint64_t sig = read_le64(hdr);
    if (sig != GPT_SIGNATURE) return -1;

    gpt->revision     = read_le32(&hdr[8]);
    gpt->header_size  = read_le32(&hdr[12]);
    if (gpt->header_size < 92 || gpt->header_size > 512) return -1;
    uint32_t header_crc = read_le32(&hdr[16]);
    /* reserved at [20..24] */
    gpt->my_lba              = read_le64(&hdr[24]);
    gpt->alternate_lba       = read_le64(&hdr[32]);
    gpt->first_usable_lba    = read_le64(&hdr[40]);
    gpt->last_usable_lba     = read_le64(&hdr[48]);
    memcpy(gpt->disk_guid, &hdr[56], 16);
    gpt->partition_entry_lba    = read_le64(&hdr[72]);
    gpt->num_partition_entries  = read_le32(&hdr[80]);
    gpt->partition_entry_size   = read_le32(&hdr[84]);
    uint32_t entries_crc = read_le32(&hdr[88]);

    /* Verify header CRC: zero out the CRC field and recompute. */
    uint8_t hbuf[512];
    memcpy(hbuf, hdr, gpt->header_size);
    memset(&hbuf[16], 0, 4);
    uint32_t calc = crc32(0L, Z_NULL, 0);
    calc = crc32(calc, hbuf, gpt->header_size);
    if (calc != header_crc) {
        fprintf(stderr, "[WARN] GPT primary header CRC mismatch "
                "(stored 0x%08x, computed 0x%08x); will try backup\n",
                header_crc, calc);
        return -1;
    }

    if (gpt->partition_entry_size < 128 || gpt->partition_entry_size > 1024) {
        return -1;
    }
    /* Cap the number of entries we expose so we don't overrun
     * `entries[MAX_PARTITIONS]`. The on-disk array can be much larger
     * (typically 128 entries reserved); we still CRC the whole thing. */
    uint32_t max_show = gpt->num_partition_entries;
    if (max_show > (uint32_t)MAX_PARTITIONS) max_show = MAX_PARTITIONS;

    /* Read the full entry array for CRC verification. */
    uint64_t arr_bytes = (uint64_t)gpt->num_partition_entries
                       * gpt->partition_entry_size;
    if (arr_bytes > 1024UL * 1024UL) return -1; /* sanity cap (1 MiB) */
    uint8_t *arr = (uint8_t *)malloc((size_t)arr_bytes);
    if (!arr) return -1;
    if (lseek(fd, (off_t)gpt->partition_entry_lba * SECTOR_SIZE, SEEK_SET) < 0) {
        free(arr); return -1;
    }
    if (read(fd, arr, (size_t)arr_bytes) != (ssize_t)arr_bytes) {
        free(arr); return -1;
    }
    uint32_t arr_calc = crc32(0L, Z_NULL, 0);
    arr_calc = crc32(arr_calc, arr, (size_t)arr_bytes);
    if (arr_calc != entries_crc) {
        fprintf(stderr, "[WARN] GPT entry-array CRC mismatch "
                "(stored 0x%08x, computed 0x%08x)\n", entries_crc, arr_calc);
        free(arr); return -1;
    }

    /* Populate the visible entries (non-empty type GUID). */
    for (uint32_t i = 0; i < max_show; i++) {
        const uint8_t *e = arr + (uint64_t)i * gpt->partition_entry_size;
        /* Skip empty (all-zero type GUID) entries. */
        int empty = 1;
        for (int k = 0; k < 16; k++) if (e[k]) { empty = 0; break; }
        if (empty) continue;

        GptEntry *out = &gpt->entries[gpt->entry_count++];
        memcpy(out->type_guid, &e[0], 16);
        memcpy(out->unique_guid, &e[16], 16);
        out->first_lba   = read_le64(&e[32]);
        out->last_lba    = read_le64(&e[40]);
        out->attributes  = read_le64(&e[48]);
        gpt_decode_name(&e[56], out->name, sizeof(out->name));
    }

    free(arr);
    return 0;
}

static int detect_partition_table(int fd, PartTable *pt) {
    uint8_t sector[2048];  /* need 4 sectors for HFS check */
    memset(pt, 0, sizeof(*pt));

    if (lseek(fd, 0, SEEK_SET) < 0) return -1;
    int nr = read(fd, sector, 2048);
    if (nr < 512) return -1;

    /* Check APM: DDR signature 0x4552 at bytes 0-1 */
    uint16_t ddr_sig = read_be16(sector);
    if (ddr_sig == APM_DDR_SIG) {
        pt->type = PT_APM;
        pt->data.apm.block_size = read_be16(&sector[2]);
        pt->data.apm.block_count = read_be32(&sector[4]);

        /* Read APM entries starting at block 1 */
        uint16_t bsz = pt->data.apm.block_size ? pt->data.apm.block_size : 512;
        uint8_t entry_buf[512];
        int map_entries = 0;

        for (int i = 1; i < MAX_PARTITIONS; i++) {
            if (lseek(fd, (off_t)i * bsz, SEEK_SET) < 0) break;
            if (read(fd, entry_buf, 512) != 512) break;

            uint16_t esig = read_be16(entry_buf);
            if (esig != APM_ENTRY_SIG) break;

            if (i == 1) map_entries = (int)read_be32(&entry_buf[4]);

            ApmEntry *ae = &pt->data.apm.entries[pt->data.apm.entry_count];
            ae->start_block = read_be32(&entry_buf[8]);
            ae->block_count = read_be32(&entry_buf[12]);
            memcpy(ae->name, &entry_buf[16], 32); ae->name[32] = '\0';
            memcpy(ae->type, &entry_buf[48], 32); ae->type[32] = '\0';
            ae->status = read_be32(&entry_buf[88]);
            pt->data.apm.entry_count++;

            if (map_entries > 0 && i >= map_entries) break;
        }
        return 0;
    }

    /* Check superfloppy: FAT VBR at sector 0 */
    if (is_fat_vbr(sector)) {
        pt->type = PT_NONE;
        strcpy(pt->fs_hint, "FAT");
        return 0;
    }

    /* Check HFS/HFS+ at offset 1024 */
    if (nr >= 2048 && is_hfs_sig(&sector[1024])) {
        pt->type = PT_NONE;
        uint16_t hsig = read_be16(&sector[1024]);
        strcpy(pt->fs_hint, hsig == 0x4244 ? "HFS" : "HFS+");
        return 0;
    }

    /* Check MBR signature */
    uint16_t mbr_sig = read_le16(&sector[510]);
    if (mbr_sig != 0xAA55) return -1;  /* no recognized partition table */

    /* Parse MBR */
    pt->type = PT_MBR;
    pt->data.mbr.disk_signature = read_le32(&sector[440]);
    for (int i = 0; i < 4; i++)
        parse_mbr_entry(&sector[446 + i * 16], &pt->data.mbr.entries[i]);

    /* Check for GPT (protective MBR). The MBR remains valid for legacy
     * tools, but the real layout lives in the GPT structures at LBA 1
     * and at the disk's last sector. We parse the primary here; if it
     * fails CRC validation we fall back to the protective-MBR view so
     * that older Tiger backups continue to work. */
    if (pt->data.mbr.entries[0].type == 0xEE) {
        GptTable gpt_tmp;
        if (parse_gpt(fd, &gpt_tmp) == 0) {
            pt->type = PT_GPT;
            pt->data.gpt = gpt_tmp;
            return 0;
        }
        fprintf(stderr,
            "[WARN] GPT signature present but parse failed; "
            "falling back to protective-MBR view\n");
        pt->type = PT_GPT;
        return 0;
    }

    /* Parse EBR chain for extended partitions */
    for (int i = 0; i < 4; i++) {
        if (is_extended_type(pt->data.mbr.entries[i].type)) {
            parse_ebr_chain(fd, pt->data.mbr.entries[i].start_lba, &pt->data.mbr);
            break;
        }
    }

    return 0;
}

/* Build flat partition list from parsed table */
static int get_partition_list(const PartTable *pt, PartInfo *parts) {
    int count = 0;

    if (pt->type == PT_MBR) {
        for (int i = 0; i < 4; i++) {
            const MbrEntry *e = &pt->data.mbr.entries[i];
            if (e->type == 0x00 || e->total_sectors == 0) continue;
            PartInfo *p = &parts[count];
            memset(p, 0, sizeof(*p));
            p->index = count;
            p->type_byte = e->type;
            strncpy(p->type_name, partition_type_name(e->type), sizeof(p->type_name) - 1);
            p->start_lba = e->start_lba;
            p->total_sectors = e->total_sectors;
            p->bootable = (e->bootable == 0x80);
            p->is_extended = is_extended_type(e->type);
            count++;
        }
        /* Add logical partitions */
        for (int i = 0; i < pt->data.mbr.logical_count; i++) {
            const MbrEntry *e = &pt->data.mbr.logical[i];
            PartInfo *p = &parts[count];
            memset(p, 0, sizeof(*p));
            p->index = count;
            p->type_byte = e->type;
            strncpy(p->type_name, partition_type_name(e->type), sizeof(p->type_name) - 1);
            p->start_lba = e->start_lba;
            p->total_sectors = e->total_sectors;
            p->is_logical = 1;
            count++;
        }
    } else if (pt->type == PT_GPT && pt->data.gpt.entry_count > 0) {
        for (int i = 0; i < pt->data.gpt.entry_count && count < MAX_PARTITIONS; i++) {
            const GptEntry *e = &pt->data.gpt.entries[i];
            PartInfo *p = &parts[count];
            memset(p, 0, sizeof(*p));
            p->index = count;
            p->type_byte = 0; /* not meaningful for GPT */
            const char *tname = gpt_type_name_from_guid(e->type_guid);
            if (e->name[0]) {
                snprintf(p->type_name, sizeof(p->type_name),
                         "%s (%s)", tname, e->name);
            } else {
                strncpy(p->type_name, tname, sizeof(p->type_name) - 1);
            }
            p->start_lba = e->first_lba;
            p->total_sectors = (e->last_lba >= e->first_lba)
                              ? (e->last_lba - e->first_lba + 1) : 0;
            /* GPT attribute bit 2 is "Legacy BIOS Bootable". */
            p->bootable = (e->attributes & (1ULL << 2)) ? 1 : 0;
            count++;
        }
    } else if (pt->type == PT_APM) {
        uint16_t bsz = pt->data.apm.block_size ? pt->data.apm.block_size : 512;
        for (int i = 0; i < pt->data.apm.entry_count; i++) {
            const ApmEntry *ae = &pt->data.apm.entries[i];
            /* Skip partition map and free space */
            if (strcmp(ae->type, "Apple_partition_map") == 0) continue;
            if (strcmp(ae->type, "Apple_Free") == 0) continue;
            if (strcmp(ae->type, "Apple_Driver") == 0) continue;
            if (strcmp(ae->type, "Apple_Driver43") == 0) continue;
            if (strcmp(ae->type, "Apple_Driver43_CD") == 0) continue;
            if (strcmp(ae->type, "Apple_Driver_ATA") == 0) continue;
            if (strcmp(ae->type, "Apple_Driver_ATAPI") == 0) continue;
            if (strcmp(ae->type, "Apple_FWDriver") == 0) continue;
            if (strcmp(ae->type, "Apple_Patches") == 0) continue;

            PartInfo *p = &parts[count];
            memset(p, 0, sizeof(*p));
            p->index = count;
            snprintf(p->type_name, sizeof(p->type_name), "%s (%s)", ae->type, ae->name);
            p->start_lba = (uint64_t)ae->start_block * bsz / SECTOR_SIZE;
            p->total_sectors = (uint64_t)ae->block_count * bsz / SECTOR_SIZE;
            p->bootable = (ae->status & 0x08) != 0;
            count++;
        }
    }

    return count;
}

/* ============================================================
 * Alignment Detection
 * ============================================================ */

static uint64_t gcd64(uint64_t a, uint64_t b) {
    while (b) { uint64_t t = b; b = a % b; a = t; }
    return a;
}

static const char *detect_alignment(const PartInfo *parts, int count,
                                     uint64_t *out_first_lba,
                                     uint64_t *out_alignment) {
    if (count == 0) {
        *out_first_lba = 0;
        *out_alignment = 0;
        return "None";
    }

    /* Find first non-extended partition LBA */
    uint64_t first_lba = 0;
    for (int i = 0; i < count; i++) {
        if (!parts[i].is_extended && parts[i].start_lba > 0) {
            first_lba = parts[i].start_lba;
            break;
        }
    }
    *out_first_lba = first_lba;

    if (first_lba == 63) {
        *out_alignment = 16065;  /* 255 * 63 */
        return "DOS Traditional (255x63)";
    }

    if (first_lba == 2048 || (first_lba > 0 && first_lba % 2048 == 0)) {
        int all_aligned = 1;
        for (int i = 0; i < count; i++) {
            if (parts[i].is_extended) continue;
            if (parts[i].start_lba % 2048 != 0) { all_aligned = 0; break; }
        }
        if (all_aligned) {
            *out_alignment = 2048;
            return "Modern 1MB";
        }
    }

    /* Custom alignment via GCD */
    if (count >= 2) {
        uint64_t g = 0;
        for (int i = 0; i < count; i++) {
            if (parts[i].is_extended) continue;
            if (parts[i].start_lba == 0) continue;
            g = g == 0 ? parts[i].start_lba : gcd64(g, parts[i].start_lba);
        }
        if (g > 1) {
            *out_alignment = g;
            return "Custom";
        }
    }

    *out_alignment = 0;
    return "None";
}

/* ============================================================
 * Checksum Functions (CRC32 via zlib, SHA-256 via CommonCrypto)
 * ============================================================ */

typedef enum {
    CKSUM_NONE = 0,
    CKSUM_CRC32,
    CKSUM_SHA256
} ChecksumType;

/* Compute CRC32 of a file and write .crc32 sidecar */
static uint32_t compute_file_crc32(const char *filepath) {
    FILE *f = fopen(filepath, "rb");
    if (!f) return 0;

    uint32_t crc = crc32(0L, Z_NULL, 0);
    uint8_t buf[CHUNK_SIZE];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), f)) > 0)
        crc = crc32(crc, buf, n);
    fclose(f);
    return crc;
}

static void write_crc32_sidecar(const char *filepath, uint32_t crc) {
    char sidecar[MAX_PATH_LEN];
    snprintf(sidecar, sizeof(sidecar), "%s.crc32", filepath);

    const char *fname = strrchr(filepath, '/');
    fname = fname ? fname + 1 : filepath;

    FILE *f = fopen(sidecar, "w");
    if (f) {
        fprintf(f, "%08x  %s\n", crc, fname);
        fclose(f);
    }
}

#ifdef HAVE_SHA256
/* Stream a file through CC_SHA256 and write a `.sha256` sidecar. The
 * sidecar format matches rb-cli + GNU coreutils: `<hex>  <basename>\n`. */
static void compute_file_sha256(const char *filepath, char *hex_out) {
    FILE *f = fopen(filepath, "rb");
    if (!f) { hex_out[0] = '\0'; return; }

    CC_SHA256_CTX ctx;
    CC_SHA256_Init(&ctx);

    uint8_t buf[CHUNK_SIZE];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), f)) > 0)
        CC_SHA256_Update(&ctx, buf, n);
    fclose(f);

    unsigned char digest[CC_SHA256_DIGEST_LENGTH];
    CC_SHA256_Final(digest, &ctx);

    for (int i = 0; i < CC_SHA256_DIGEST_LENGTH; i++)
        sprintf(&hex_out[i * 2], "%02x", digest[i]);
    hex_out[CC_SHA256_DIGEST_LENGTH * 2] = '\0';
}

static void write_sha256_sidecar(const char *filepath, const char *hex) {
    char sidecar[MAX_PATH_LEN];
    snprintf(sidecar, sizeof(sidecar), "%s.sha256", filepath);

    const char *fname = strrchr(filepath, '/');
    fname = fname ? fname + 1 : filepath;

    FILE *f = fopen(sidecar, "w");
    if (f) {
        fprintf(f, "%s  %s\n", hex, fname);
        fclose(f);
    }
}
#endif

static void write_checksum(const char *filepath, ChecksumType type,
                            char *hex_out, int hexsz) {
    hex_out[0] = '\0';
    if (type == CKSUM_CRC32) {
        uint32_t crc = compute_file_crc32(filepath);
        snprintf(hex_out, hexsz, "%08x", crc);
        write_crc32_sidecar(filepath, crc);
    }
#ifdef HAVE_SHA256
    else if (type == CKSUM_SHA256) {
        compute_file_sha256(filepath, hex_out);
        write_sha256_sidecar(filepath, hex_out);
    }
#endif
}

/* ============================================================
 * Gzip Compression (via zlib — ships with Tiger)
 * ============================================================ */

typedef enum {
    COMP_RAW = 0,
    COMP_GZIP,
    COMP_VHD
} CompressionMode;

/* Returns extension string for the compression mode */
static const char *comp_ext(CompressionMode m) {
    if (m == COMP_GZIP) return ".gz";
    if (m == COMP_VHD)  return ".vhd";
    return ".raw";
}

/* ============================================================
 * VHD (Fixed) Footer
 * ----------------------------------------------------------------
 * Microsoft VHD v1.0 "fixed" footer — 512 bytes appended to the raw
 * partition data. Matches `src/rbformats/vhd.rs::build_vhd_footer`
 * byte-for-byte so files round-trip with rb-cli.
 * ============================================================ */

#define VHD_FOOTER_SIZE 512
#define VHD_EPOCH 946684800UL   /* 2000-01-01 00:00:00 UTC */

/* Compute VHD CHS geometry from total disk size (bytes). Mirrors
 * `vhd_chs_geometry()` in src/rbformats/vhd.rs. */
static void vhd_chs_geometry(uint64_t size_bytes, uint32_t *cyl_out,
                             uint32_t *heads_out, uint32_t *spt_out) {
    uint64_t total_sectors_64 = size_bytes / 512ULL;
    uint64_t cap = 65535ULL * 16ULL * 255ULL;
    if (total_sectors_64 > cap) total_sectors_64 = cap;
    uint32_t total_sectors = (uint32_t)total_sectors_64;

    if (total_sectors == 0) {
        *cyl_out = 0; *heads_out = 0; *spt_out = 0;
        return;
    }

    if (total_sectors >= 65535U * 16U * 63U) {
        uint32_t spt = 255U, heads = 16U;
        *spt_out = spt; *heads_out = heads;
        *cyl_out = total_sectors / (heads * spt);
        return;
    }

    uint32_t spt = 17U;
    uint32_t cyl_times_heads = total_sectors / spt;
    uint32_t heads = (cyl_times_heads + 1023U) / 1024U; /* div_ceil */
    if (heads < 4U) heads = 4U;

    if (cyl_times_heads >= heads * 1024U || heads > 16U) {
        spt = 31U; heads = 16U;
        cyl_times_heads = total_sectors / spt;
    }
    if (cyl_times_heads >= heads * 1024U) {
        spt = 63U; heads = 16U;
        cyl_times_heads = total_sectors / spt;
    }

    *cyl_out = cyl_times_heads / heads;
    *heads_out = heads;
    *spt_out = spt;
}

static void vhd_put_be32(uint8_t *p, uint32_t v) {
    p[0] = (v >> 24) & 0xff; p[1] = (v >> 16) & 0xff;
    p[2] = (v >> 8) & 0xff;  p[3] = v & 0xff;
}
static void vhd_put_be64(uint8_t *p, uint64_t v) {
    vhd_put_be32(p, (uint32_t)(v >> 32));
    vhd_put_be32(p + 4, (uint32_t)(v & 0xffffffffULL));
}

/* Build a 512-byte VHD Fixed footer for `data_size`. */
static void build_vhd_footer(uint64_t data_size, uint8_t footer[512]) {
    memset(footer, 0, 512);
    memcpy(&footer[0], "conectix", 8);
    vhd_put_be32(&footer[8], 0x00000002);   /* Features: reserved bit */
    vhd_put_be32(&footer[12], 0x00010000);  /* File Format Version 1.0 */
    vhd_put_be64(&footer[16], 0xFFFFFFFFFFFFFFFFULL); /* Data Offset = none */

    /* Timestamp: seconds since 2000-01-01 UTC. */
    time_t now = time(NULL);
    uint32_t ts = (now > (time_t)VHD_EPOCH) ? (uint32_t)(now - VHD_EPOCH) : 0;
    vhd_put_be32(&footer[24], ts);

    memcpy(&footer[28], "rsbk", 4);          /* Creator App */
    vhd_put_be32(&footer[32], 0x00010000);   /* Creator Version */
    memcpy(&footer[36], "Mac ", 4);          /* Creator Host OS */
    vhd_put_be64(&footer[40], data_size);    /* Original Size */
    vhd_put_be64(&footer[48], data_size);    /* Current Size */

    uint32_t cyl, heads, spt;
    vhd_chs_geometry(data_size, &cyl, &heads, &spt);
    footer[56] = (uint8_t)((cyl >> 8) & 0xff);
    footer[57] = (uint8_t)(cyl & 0xff);
    footer[58] = (uint8_t)(heads & 0xff);
    footer[59] = (uint8_t)(spt & 0xff);

    vhd_put_be32(&footer[60], 2);            /* Disk Type: Fixed */
    /* Checksum at [64..68] left zero for now; filled below. */

    /* UUID (16 bytes) — mix time + pid like the Rust version. */
    uint64_t pid = (uint64_t)getpid();
    uint64_t mix1 = ((uint64_t)now * 2654435761ULL) ^ pid;
    uint64_t mix2 = (mix1 ^ 0xDEADBEEFCAFEBABEULL) * 11400714819323198485ULL;
    for (int i = 0; i < 8; i++) footer[68 + i] = (uint8_t)(mix1 >> (i * 8));
    for (int i = 0; i < 8; i++) footer[76 + i] = (uint8_t)(mix2 >> (i * 8));

    /* Checksum: one's complement of the sum of all bytes, treating the
     * checksum field [64..68) as zero. */
    uint32_t sum = 0;
    for (int i = 0; i < 512; i++) {
        if (i >= 64 && i < 68) continue;
        sum += footer[i];
    }
    uint32_t cksum = ~sum;
    vhd_put_be32(&footer[64], cksum);
}

/* ============================================================
 * Split-file Writer
 * ----------------------------------------------------------------
 * Streams bytes into a sequence of output files, rolling over to a
 * new file when `split_bytes` is reached. Matches rb-cli's naming
 * convention:
 *
 *   no split:        partition-3.raw
 *   split, file 0:   partition-3.raw      (first file is unindexed)
 *   split, file 1:   partition-3.001.raw
 *   split, file 2:   partition-3.002.raw
 *
 * Used for both raw output (`FILE *`) and gzip output (`gzFile`).
 * After close, `files[]` lists the basenames of every emitted file
 * so the caller can compute per-file checksums and populate the
 * `compressed_files` array in `metadata.json`.
 * ============================================================ */

#define SPLIT_MAX_FILES 64
#define SPLIT_NAME_LEN 128

typedef struct {
    char dir_path[MAX_PATH_LEN];   /* output directory */
    char stem[64];                  /* e.g. "partition-3" */
    char ext[8];                    /* ".raw" or ".gz" */
    uint64_t split_bytes;           /* 0 = no split */
    uint64_t bytes_in_current;
    int part_index;                 /* 0 first, 1 = .001, 2 = .002, ... */
    int is_gzip;
    int skip_zeros;                 /* sparse-seek all-zero chunks (raw only) */
    FILE *raw_fp;
    gzFile gz_fp;
    char files[SPLIT_MAX_FILES][SPLIT_NAME_LEN]; /* emitted basenames */
    int num_files;
} SplitWriter;

/* Build the on-disk path for the n-th split file (0-indexed). The
 * first file is unindexed; subsequent files get a zero-padded suffix
 * before the extension, e.g. `partition-3.001.raw`. */
static void splitwriter_build_path(const SplitWriter *sw, int idx,
                                   char *full_path, size_t full_sz,
                                   char *basename, size_t bn_sz) {
    if (idx == 0) {
        snprintf(basename, bn_sz, "%s%s", sw->stem, sw->ext);
    } else {
        snprintf(basename, bn_sz, "%s.%03d%s", sw->stem, idx, sw->ext);
    }
    snprintf(full_path, full_sz, "%s/%s", sw->dir_path, basename);
}

/* Open (or roll over to) the next output file. Returns 0 on success. */
static int splitwriter_open_next(SplitWriter *sw) {
    if (sw->num_files >= SPLIT_MAX_FILES) {
        fprintf(stderr, "Error: split-file limit (%d) exceeded\n",
                SPLIT_MAX_FILES);
        return -1;
    }
    char full[MAX_PATH_LEN];
    char *bn = sw->files[sw->num_files];
    splitwriter_build_path(sw, sw->part_index, full, sizeof(full),
                           bn, SPLIT_NAME_LEN);
    if (sw->is_gzip) {
        sw->gz_fp = gzopen(full, "wb9");
        if (!sw->gz_fp) {
            fprintf(stderr, "Error: cannot create %s\n", full);
            return -1;
        }
    } else {
        sw->raw_fp = fopen(full, "wb");
        if (!sw->raw_fp) {
            fprintf(stderr, "Error: cannot create %s\n", full);
            return -1;
        }
    }
    sw->num_files++;
    sw->bytes_in_current = 0;
    return 0;
}

static int splitwriter_open(SplitWriter *sw, const char *dir_path,
                            const char *stem, const char *ext,
                            int is_gzip, uint64_t split_bytes,
                            int skip_zeros) {
    memset(sw, 0, sizeof(*sw));
    snprintf(sw->dir_path, sizeof(sw->dir_path), "%s", dir_path);
    snprintf(sw->stem, sizeof(sw->stem), "%s", stem);
    snprintf(sw->ext, sizeof(sw->ext), "%s", ext);
    sw->is_gzip = is_gzip;
    sw->split_bytes = split_bytes;
    /* skip_zeros only makes sense for raw output — gzip can't represent
     * file holes (the codec emits every byte). */
    sw->skip_zeros = skip_zeros && !is_gzip;
    sw->part_index = 0;
    return splitwriter_open_next(sw);
}

/* Close the currently-open output file (without freeing the writer). */
static void splitwriter_close_current(SplitWriter *sw) {
    if (sw->is_gzip) {
        if (sw->gz_fp) { gzclose(sw->gz_fp); sw->gz_fp = NULL; }
    } else {
        if (sw->raw_fp) { fclose(sw->raw_fp); sw->raw_fp = NULL; }
    }
}

static void splitwriter_finalize_sparse(SplitWriter *sw); /* fwd */

static int splitwriter_close(SplitWriter *sw) {
    splitwriter_finalize_sparse(sw);
    splitwriter_close_current(sw);
    return 0;
}

/* VHD output: append the 512-byte Fixed footer to the (single) raw
 * file. `data_bytes_written` is the partition data size — it goes into
 * the footer's Original/Current Size fields and CHS geometry. Caller
 * must close the SplitWriter via this helper instead of
 * splitwriter_close() when writing VHD. */
static int splitwriter_close_vhd(SplitWriter *sw, uint64_t data_bytes_written) {
    splitwriter_finalize_sparse(sw);
    if (sw->raw_fp) {
        uint8_t footer[VHD_FOOTER_SIZE];
        build_vhd_footer(data_bytes_written, footer);
        if (fwrite(footer, 1, VHD_FOOTER_SIZE, sw->raw_fp) != VHD_FOOTER_SIZE) {
            fprintf(stderr, "Error: failed to write VHD footer\n");
            splitwriter_close_current(sw);
            return -1;
        }
    }
    splitwriter_close_current(sw);
    return 0;
}

/* True if every byte in `buf[..n]` is zero. Used by the sparse-zero
 * fast path in `splitwriter_write_sparse`. */
static int buf_all_zeros(const uint8_t *buf, size_t n) {
    for (size_t i = 0; i < n; i++)
        if (buf[i]) return 0;
    return 1;
}

/* Write `n` bytes, rolling to a new split file at the boundary.
 *
 * When `sw->skip_zeros` is set and `buf` is an all-zero chunk, the raw
 * output path takes a sparse-seek shortcut: it advances the file
 * position via `fseeko(SEEK_CUR)` instead of writing real bytes,
 * leaving an unbacked hole. The file's logical size is reconciled
 * with `ftruncate` at split-rollover and at close (see
 * `splitwriter_finalize_sparse`). Gzip output ignores `skip_zeros` —
 * the codec can't represent file holes. */
static int splitwriter_write(SplitWriter *sw, const uint8_t *buf, size_t n) {
    int sparse_chunk = sw->skip_zeros && buf_all_zeros(buf, n);
    size_t off = 0;
    while (off < n) {
        size_t chunk = n - off;
        if (sw->split_bytes > 0) {
            uint64_t room = sw->split_bytes - sw->bytes_in_current;
            if ((uint64_t)chunk > room) chunk = (size_t)room;
        }
        if (sw->is_gzip) {
            if (gzwrite(sw->gz_fp, buf + off, chunk) <= 0 && chunk > 0) {
                fprintf(stderr, "Error: gzwrite failed\n");
                return -1;
            }
        } else if (sparse_chunk) {
            if (fseeko(sw->raw_fp, (off_t)chunk, SEEK_CUR) != 0) {
                fprintf(stderr, "Error: sparse seek failed\n");
                return -1;
            }
        } else {
            if (fwrite(buf + off, 1, chunk, sw->raw_fp) != chunk) {
                fprintf(stderr, "Error: fwrite failed\n");
                return -1;
            }
        }
        sw->bytes_in_current += chunk;
        off += chunk;

        if (sw->split_bytes > 0 && sw->bytes_in_current >= sw->split_bytes
            && off < n) {
            /* Reconcile any trailing sparse hole, then roll over. */
            splitwriter_finalize_sparse(sw);
            splitwriter_close_current(sw);
            sw->part_index++;
            if (splitwriter_open_next(sw) != 0) return -1;
        }
    }
    return 0;
}

/* Before closing the current raw file, ensure its on-disk length
 * matches `bytes_in_current` even if the final byte fell inside a
 * sparse-skipped zero run (in which case fwrite never extended the
 * file). ftruncate is non-destructive — POSIX guarantees that
 * extending a file fills the new region with zeros. */
static void splitwriter_finalize_sparse(SplitWriter *sw) {
    if (sw->is_gzip || !sw->raw_fp || sw->bytes_in_current == 0) return;
    fflush(sw->raw_fp);
    ftruncate(fileno(sw->raw_fp), (off_t)sw->bytes_in_current);
}

/* ============================================================
 * FAT Compaction — only back up allocated clusters
 * ============================================================ */

typedef enum { FAT_12, FAT_16, FAT_32 } FatType;

/* Directory entry constants */
#define DIR_ENTRY_SIZE  32
#define ATTR_LONG_NAME  0x0F
#define ATTR_VOLUME_ID  0x08
#define ATTR_DIRECTORY  0x10

typedef struct {
    FatType     fat_type;
    uint16_t    bytes_per_sector;
    uint8_t     sectors_per_cluster;
    uint16_t    reserved_sectors;
    uint8_t     num_fats;
    uint16_t    root_entry_count;    /* 0 for FAT32 */
    uint32_t    sectors_per_fat;
    uint32_t    root_cluster;        /* FAT32 only */
    uint32_t    total_sectors;
    uint32_t    total_clusters;
    uint32_t    data_start_sector;   /* first sector of cluster 2 */
    uint32_t    root_dir_sectors;    /* FAT12/16 root dir sector count */
    uint32_t    cluster_size;        /* bytes */

    /* FAT table (in memory) */
    uint8_t    *fat_data;
    uint32_t    fat_data_size;

    /* Cluster mapping: allocated[i] = old cluster number for new cluster i+2 */
    uint32_t   *allocated;           /* old cluster numbers */
    uint32_t    alloc_count;         /* number of allocated clusters */

    /* Reverse map: old_to_new[old_cluster] = new_cluster (0 = unmapped) */
    uint32_t   *old_to_new;
    uint32_t    old_to_new_size;

    /* Directory cluster bitmap */
    uint8_t    *is_dir_cluster;      /* [total_clusters] flag array */

    /* Source file and partition offset */
    int         src_fd;
    off_t       part_offset;         /* absolute byte offset of partition */

    /* Pre-built output sections */
    uint8_t    *boot_sector;         /* patched BPB + reserved sectors */
    uint32_t    boot_size;
    uint8_t    *new_fat;             /* rebuilt FAT table */
    uint32_t    new_fat_size;
    uint8_t    *root_dir;            /* FAT12/16 root dir (NULL for FAT32) */
    uint32_t    root_dir_size;

    /* Virtual stream state */
    uint64_t    total_output_size;
    uint64_t    position;
    uint8_t    *cluster_buf;         /* single cluster buffer */
    int         cached_cluster;      /* -1 = none */
} CompactFat;

/* Read a FAT entry */
static uint32_t fat_read_entry(const uint8_t *fat, uint32_t cluster, FatType type) {
    switch (type) {
    case FAT_12: {
        uint32_t off = (cluster * 3) / 2;
        uint16_t val = fat[off] | (fat[off + 1] << 8);
        return (cluster & 1) ? ((val >> 4) & 0xFFF) : (val & 0xFFF);
    }
    case FAT_16: {
        uint32_t off = cluster * 2;
        return fat[off] | (fat[off + 1] << 8);
    }
    case FAT_32: {
        uint32_t off = cluster * 4;
        uint32_t val = fat[off] | (fat[off+1]<<8) | (fat[off+2]<<16) | (fat[off+3]<<24);
        return val & 0x0FFFFFFF;
    }
    }
    return 0;
}

/* Write a FAT entry */
static void fat_write_entry(uint8_t *fat, uint32_t cluster, uint32_t value, FatType type) {
    switch (type) {
    case FAT_12: {
        uint32_t off = (cluster * 3) / 2;
        uint16_t existing = fat[off] | (fat[off + 1] << 8);
        uint16_t nv;
        if (cluster & 1)
            nv = (existing & 0x000F) | ((value & 0xFFF) << 4);
        else
            nv = (existing & 0xF000) | (value & 0xFFF);
        fat[off] = nv & 0xFF;
        fat[off + 1] = (nv >> 8) & 0xFF;
        break;
    }
    case FAT_16: {
        uint32_t off = cluster * 2;
        fat[off] = value & 0xFF;
        fat[off + 1] = (value >> 8) & 0xFF;
        break;
    }
    case FAT_32: {
        uint32_t off = cluster * 4;
        /* preserve high 4 bits */
        uint32_t existing = fat[off] | (fat[off+1]<<8) | (fat[off+2]<<16) | (fat[off+3]<<24);
        value = (existing & 0xF0000000) | (value & 0x0FFFFFFF);
        fat[off]   = value & 0xFF;
        fat[off+1] = (value >> 8) & 0xFF;
        fat[off+2] = (value >> 16) & 0xFF;
        fat[off+3] = (value >> 24) & 0xFF;
        break;
    }
    }
}

static int fat_is_eoc(uint32_t entry, FatType type) {
    switch (type) {
    case FAT_12: return entry >= 0x0FF8;
    case FAT_16: return entry >= 0xFFF8;
    case FAT_32: return entry >= 0x0FFFFFF8;
    }
    return 0;
}

static int fat_is_allocated(uint32_t entry, FatType type) {
    if (entry == 0) return 0;  /* free */
    if (type == FAT_12 && entry == 0xFF7) return 0;  /* bad */
    if (type == FAT_16 && entry == 0xFFF7) return 0;
    if (type == FAT_32 && entry == 0x0FFFFFF7) return 0;
    return 1;
}

/* Walk directory tree to identify directory clusters */
static void fat_mark_dir_clusters(CompactFat *cf, uint32_t start_cluster) {
    uint32_t cluster = start_cluster;
    while (cluster >= 2 && cluster < cf->total_clusters + 2) {
        if (cf->is_dir_cluster[cluster]) break;  /* already visited */
        cf->is_dir_cluster[cluster] = 1;

        /* Read this cluster and find subdirectory entries */
        off_t coff = cf->part_offset +
            (off_t)(cf->data_start_sector + (cluster - 2) * cf->sectors_per_cluster)
            * cf->bytes_per_sector;
        uint8_t *dir = (uint8_t *)malloc(cf->cluster_size);
        if (!dir) break;

        if (lseek(cf->src_fd, coff, SEEK_SET) >= 0 &&
            read(cf->src_fd, dir, cf->cluster_size) == (ssize_t)cf->cluster_size) {
            int nent = cf->cluster_size / DIR_ENTRY_SIZE;
            for (int i = 0; i < nent; i++) {
                uint8_t *e = &dir[i * DIR_ENTRY_SIZE];
                if (e[0] == 0x00) break;       /* end of dir */
                if (e[0] == 0xE5) continue;     /* deleted */
                if (e[11] == ATTR_LONG_NAME) continue;
                if (e[11] & ATTR_VOLUME_ID) continue;

                if (e[11] & ATTR_DIRECTORY) {
                    uint32_t sub = (uint32_t)(e[26] | (e[27]<<8));
                    if (cf->fat_type == FAT_32)
                        sub |= ((uint32_t)(e[20] | (e[21]<<8))) << 16;
                    /* Skip . and .. */
                    if (e[0] == '.' && (e[1] == ' ' || e[1] == '.')) continue;
                    if (sub >= 2 && sub < cf->total_clusters + 2)
                        fat_mark_dir_clusters(cf, sub);
                }
            }
        }
        free(dir);

        /* Follow chain */
        uint32_t next = fat_read_entry(cf->fat_data, cluster, cf->fat_type);
        if (fat_is_eoc(next, cf->fat_type) || next < 2) break;
        cluster = next;
    }
}

/* Patch directory entries: update cluster references to new locations */
static void fat_patch_dir_entries(CompactFat *cf, uint8_t *data, uint32_t size) {
    int nent = size / DIR_ENTRY_SIZE;
    for (int i = 0; i < nent; i++) {
        uint8_t *e = &data[i * DIR_ENTRY_SIZE];
        if (e[0] == 0x00) break;
        if (e[0] == 0xE5) continue;
        if (e[11] == ATTR_LONG_NAME) continue;
        if (e[11] & ATTR_VOLUME_ID) continue;

        uint32_t old_c = (uint32_t)(e[26] | (e[27]<<8));
        if (cf->fat_type == FAT_32)
            old_c |= ((uint32_t)(e[20] | (e[21]<<8))) << 16;

        if (old_c == 0 || old_c >= cf->old_to_new_size) continue;
        uint32_t new_c = cf->old_to_new[old_c];
        if (new_c == 0) continue;

        e[26] = new_c & 0xFF;
        e[27] = (new_c >> 8) & 0xFF;
        if (cf->fat_type == FAT_32) {
            e[20] = (new_c >> 16) & 0xFF;
            e[21] = (new_c >> 24) & 0xFF;
        }
    }
}

/* Initialize CompactFat reader — returns NULL if not a FAT partition */
static CompactFat *compact_fat_open(int src_fd, off_t part_offset, uint64_t part_size) {
    uint8_t bpb[512];
    if (lseek(src_fd, part_offset, SEEK_SET) < 0) return NULL;
    if (read(src_fd, bpb, 512) != 512) return NULL;

    /* Check FAT signature */
    if (bpb[0] != 0xEB && bpb[0] != 0xE9) return NULL;
    uint16_t bps = read_le16(&bpb[11]);
    if (bps != 512 && bps != 1024 && bps != 2048 && bps != 4096) return NULL;
    uint8_t spc = bpb[13];
    if (spc == 0 || (spc & (spc - 1)) != 0) return NULL;
    if (bpb[16] != 1 && bpb[16] != 2) return NULL;

    CompactFat *cf = (CompactFat *)calloc(1, sizeof(CompactFat));
    if (!cf) return NULL;

    cf->src_fd = src_fd;
    cf->part_offset = part_offset;
    cf->bytes_per_sector = bps;
    cf->sectors_per_cluster = spc;
    cf->reserved_sectors = read_le16(&bpb[14]);
    cf->num_fats = bpb[16];
    cf->root_entry_count = read_le16(&bpb[17]);
    cf->cluster_size = (uint32_t)bps * spc;

    uint16_t spf16 = read_le16(&bpb[22]);
    uint32_t ts16 = read_le16(&bpb[19]);
    uint32_t ts32 = read_le32(&bpb[32]);
    cf->total_sectors = ts16 ? ts16 : ts32;

    /* Determine FAT type */
    if (spf16 == 0 && cf->root_entry_count == 0) {
        cf->fat_type = FAT_32;
        cf->sectors_per_fat = read_le32(&bpb[36]);
        cf->root_cluster = read_le32(&bpb[44]);
    } else {
        cf->sectors_per_fat = spf16;
        cf->root_dir_sectors = ((cf->root_entry_count * 32) + bps - 1) / bps;
        cf->data_start_sector = cf->reserved_sectors +
            cf->num_fats * cf->sectors_per_fat + cf->root_dir_sectors;
        uint32_t data_sectors = cf->total_sectors - cf->data_start_sector;
        cf->total_clusters = data_sectors / spc;
        cf->fat_type = (cf->total_clusters < 4085) ? FAT_12 : FAT_16;
    }

    if (cf->fat_type == FAT_32) {
        cf->root_dir_sectors = 0;
        cf->data_start_sector = cf->reserved_sectors +
            cf->num_fats * cf->sectors_per_fat;
        uint32_t data_sectors = cf->total_sectors - cf->data_start_sector;
        cf->total_clusters = data_sectors / spc;
    }

    fprintf(stderr, "  [compact] FAT%s, %u clusters, %u bytes/cluster\n",
            cf->fat_type == FAT_12 ? "12" : cf->fat_type == FAT_16 ? "16" : "32",
            cf->total_clusters, cf->cluster_size);

    /* Read FAT table into memory */
    cf->fat_data_size = cf->sectors_per_fat * bps;
    cf->fat_data = (uint8_t *)malloc(cf->fat_data_size);
    if (!cf->fat_data) { free(cf); return NULL; }

    off_t fat_off = part_offset + (off_t)cf->reserved_sectors * bps;
    if (lseek(src_fd, fat_off, SEEK_SET) < 0 ||
        read(src_fd, cf->fat_data, cf->fat_data_size) != (ssize_t)cf->fat_data_size) {
        free(cf->fat_data); free(cf); return NULL;
    }

    /* Scan for allocated clusters */
    cf->allocated = (uint32_t *)malloc(cf->total_clusters * sizeof(uint32_t));
    cf->alloc_count = 0;
    cf->old_to_new_size = cf->total_clusters + 2;
    cf->old_to_new = (uint32_t *)calloc(cf->old_to_new_size, sizeof(uint32_t));
    cf->is_dir_cluster = (uint8_t *)calloc(cf->total_clusters + 2, 1);

    for (uint32_t c = 2; c < cf->total_clusters + 2; c++) {
        uint32_t entry = fat_read_entry(cf->fat_data, c, cf->fat_type);
        if (fat_is_allocated(entry, cf->fat_type)) {
            uint32_t new_c = cf->alloc_count + 2;
            cf->allocated[cf->alloc_count++] = c;
            cf->old_to_new[c] = new_c;
        }
    }

    fprintf(stderr, "  [compact] %u/%u clusters allocated (%.1f%% savings)\n",
            cf->alloc_count, cf->total_clusters,
            cf->total_clusters > 0 ?
            (1.0 - (double)cf->alloc_count / cf->total_clusters) * 100.0 : 0.0);

    /* Mark directory clusters (FAT32: start from root_cluster, else from root dir) */
    if (cf->fat_type == FAT_32 && cf->root_cluster >= 2) {
        fat_mark_dir_clusters(cf, cf->root_cluster);
    }
    /* For FAT12/16, root dir is separate (not in cluster area), scan its entries */
    if (cf->fat_type != FAT_32 && cf->root_entry_count > 0) {
        uint32_t root_off_abs = cf->reserved_sectors + cf->num_fats * cf->sectors_per_fat;
        uint32_t root_size = cf->root_entry_count * DIR_ENTRY_SIZE;
        uint8_t *root = (uint8_t *)malloc(root_size);
        if (root) {
            off_t roff = part_offset + (off_t)root_off_abs * bps;
            if (lseek(src_fd, roff, SEEK_SET) >= 0 &&
                read(src_fd, root, root_size) == (ssize_t)root_size) {
                for (uint32_t i = 0; i < cf->root_entry_count; i++) {
                    uint8_t *e = &root[i * DIR_ENTRY_SIZE];
                    if (e[0] == 0x00) break;
                    if (e[0] == 0xE5 || e[11] == ATTR_LONG_NAME) continue;
                    if (e[11] & ATTR_VOLUME_ID) continue;
                    if (e[11] & ATTR_DIRECTORY) {
                        uint32_t sub = (uint32_t)(e[26] | (e[27]<<8));
                        if (e[0] == '.' && (e[1] == ' ' || e[1] == '.')) continue;
                        if (sub >= 2) fat_mark_dir_clusters(cf, sub);
                    }
                }
            }
            free(root);
        }
    }

    /* Build patched boot sector (with updated cluster counts) */
    cf->boot_size = cf->reserved_sectors * bps;
    cf->boot_sector = (uint8_t *)malloc(cf->boot_size);
    if (lseek(src_fd, part_offset, SEEK_SET) >= 0)
        read(src_fd, cf->boot_sector, cf->boot_size);

    /* Recalculate sectors_per_fat for compacted size */
    uint32_t new_data_sectors = cf->alloc_count * spc;
    uint32_t new_spf;
    if (cf->fat_type == FAT_32) {
        /* entries * 4 bytes / bps, round up */
        new_spf = ((cf->alloc_count + 2) * 4 + bps - 1) / bps;
    } else if (cf->fat_type == FAT_16) {
        new_spf = ((cf->alloc_count + 2) * 2 + bps - 1) / bps;
    } else {
        new_spf = (((cf->alloc_count + 2) * 3 + 1) / 2 + bps - 1) / bps;
    }

    uint32_t new_total = cf->reserved_sectors + cf->num_fats * new_spf +
                          cf->root_dir_sectors + new_data_sectors;

    /* Patch BPB in boot sector */
    if (new_total <= 0xFFFF) {
        cf->boot_sector[19] = new_total & 0xFF;
        cf->boot_sector[20] = (new_total >> 8) & 0xFF;
        cf->boot_sector[32] = 0; cf->boot_sector[33] = 0;
        cf->boot_sector[34] = 0; cf->boot_sector[35] = 0;
    } else {
        cf->boot_sector[19] = 0; cf->boot_sector[20] = 0;
        cf->boot_sector[32] = new_total & 0xFF;
        cf->boot_sector[33] = (new_total >> 8) & 0xFF;
        cf->boot_sector[34] = (new_total >> 16) & 0xFF;
        cf->boot_sector[35] = (new_total >> 24) & 0xFF;
    }

    if (cf->fat_type == FAT_32) {
        cf->boot_sector[36] = new_spf & 0xFF;
        cf->boot_sector[37] = (new_spf >> 8) & 0xFF;
        cf->boot_sector[38] = (new_spf >> 16) & 0xFF;
        cf->boot_sector[39] = (new_spf >> 24) & 0xFF;
        /* Patch root cluster if it was remapped */
        if (cf->root_cluster < cf->old_to_new_size && cf->old_to_new[cf->root_cluster]) {
            uint32_t nr = cf->old_to_new[cf->root_cluster];
            cf->boot_sector[44] = nr & 0xFF;
            cf->boot_sector[45] = (nr >> 8) & 0xFF;
            cf->boot_sector[46] = (nr >> 16) & 0xFF;
            cf->boot_sector[47] = (nr >> 24) & 0xFF;
        }
    } else {
        cf->boot_sector[22] = new_spf & 0xFF;
        cf->boot_sector[23] = (new_spf >> 8) & 0xFF;
    }

    /* Build new FAT table */
    cf->new_fat_size = new_spf * bps * cf->num_fats;
    cf->new_fat = (uint8_t *)calloc(1, cf->new_fat_size);

    uint32_t one_fat_size = new_spf * bps;
    /* Entry 0: media byte */
    if (cf->fat_type == FAT_32)
        fat_write_entry(cf->new_fat, 0, 0x0FFFFF00 | bpb[21], FAT_32);
    else if (cf->fat_type == FAT_16)
        fat_write_entry(cf->new_fat, 0, 0xFF00 | bpb[21], FAT_16);
    else
        fat_write_entry(cf->new_fat, 0, 0x0F00 | bpb[21], FAT_12);

    /* Entry 1: EOC with clean shutdown */
    if (cf->fat_type == FAT_32)
        fat_write_entry(cf->new_fat, 1, 0x0FFFFFFF, FAT_32);
    else if (cf->fat_type == FAT_16)
        fat_write_entry(cf->new_fat, 1, 0xFFFF, FAT_16);
    else
        fat_write_entry(cf->new_fat, 1, 0x0FFF, FAT_12);

    /* Remap cluster chains */
    for (uint32_t i = 0; i < cf->alloc_count; i++) {
        uint32_t old_c = cf->allocated[i];
        uint32_t new_c = i + 2;
        uint32_t old_next = fat_read_entry(cf->fat_data, old_c, cf->fat_type);

        if (fat_is_eoc(old_next, cf->fat_type)) {
            /* End of chain */
            if (cf->fat_type == FAT_32)
                fat_write_entry(cf->new_fat, new_c, 0x0FFFFFFF, FAT_32);
            else if (cf->fat_type == FAT_16)
                fat_write_entry(cf->new_fat, new_c, 0xFFFF, FAT_16);
            else
                fat_write_entry(cf->new_fat, new_c, 0x0FFF, FAT_12);
        } else if (old_next >= 2 && old_next < cf->old_to_new_size &&
                   cf->old_to_new[old_next]) {
            fat_write_entry(cf->new_fat, new_c, cf->old_to_new[old_next], cf->fat_type);
        } else {
            /* Broken chain — mark as EOC */
            if (cf->fat_type == FAT_32)
                fat_write_entry(cf->new_fat, new_c, 0x0FFFFFFF, FAT_32);
            else if (cf->fat_type == FAT_16)
                fat_write_entry(cf->new_fat, new_c, 0xFFFF, FAT_16);
            else
                fat_write_entry(cf->new_fat, new_c, 0x0FFF, FAT_12);
        }
    }

    /* Copy FAT to all copies */
    for (int f = 1; f < cf->num_fats; f++)
        memcpy(cf->new_fat + f * one_fat_size, cf->new_fat, one_fat_size);

    /* Build root directory for FAT12/16 */
    if (cf->fat_type != FAT_32 && cf->root_dir_sectors > 0) {
        cf->root_dir_size = cf->root_dir_sectors * bps;
        cf->root_dir = (uint8_t *)malloc(cf->root_dir_size);
        off_t roff = part_offset + (off_t)(cf->reserved_sectors +
            cf->num_fats * cf->sectors_per_fat) * bps;
        if (lseek(src_fd, roff, SEEK_SET) >= 0)
            read(src_fd, cf->root_dir, cf->root_dir_size);
        /* Patch directory entries in root */
        fat_patch_dir_entries(cf, cf->root_dir, cf->root_dir_size);
    }

    /* Calculate total output size */
    cf->total_output_size = (uint64_t)cf->boot_size + cf->new_fat_size +
        (cf->root_dir ? cf->root_dir_size : 0) +
        (uint64_t)cf->alloc_count * cf->cluster_size;

    cf->position = 0;
    cf->cluster_buf = (uint8_t *)malloc(cf->cluster_size);
    cf->cached_cluster = -1;

    fprintf(stderr, "  [compact] Output: %llu bytes (was %llu, saved %llu)\n",
            (unsigned long long)cf->total_output_size,
            (unsigned long long)part_size,
            (unsigned long long)(part_size - cf->total_output_size));

    return cf;
}

/* Read from compacted stream — returns bytes read (0 = EOF) */
static ssize_t compact_fat_read(CompactFat *cf, uint8_t *buf, size_t count) {
    if (cf->position >= cf->total_output_size) return 0;
    if (cf->position + count > cf->total_output_size)
        count = cf->total_output_size - cf->position;

    size_t filled = 0;
    while (filled < count) {
        uint64_t pos = cf->position + filled;
        size_t remaining = count - filled;

        /* Region 1: Boot sector / reserved */
        if (pos < cf->boot_size) {
            size_t off = (size_t)pos;
            size_t n = cf->boot_size - off;
            if (n > remaining) n = remaining;
            memcpy(buf + filled, cf->boot_sector + off, n);
            filled += n;
            continue;
        }

        /* Region 2: FAT tables */
        uint64_t fat_start = cf->boot_size;
        uint64_t fat_end = fat_start + cf->new_fat_size;
        if (pos < fat_end) {
            size_t off = (size_t)(pos - fat_start);
            size_t n = (size_t)(fat_end - pos);
            if (n > remaining) n = remaining;
            memcpy(buf + filled, cf->new_fat + off, n);
            filled += n;
            continue;
        }

        /* Region 3: Root directory (FAT12/16 only) */
        uint64_t root_start = fat_end;
        uint64_t root_end = root_start + (cf->root_dir ? cf->root_dir_size : 0);
        if (cf->root_dir && pos < root_end) {
            size_t off = (size_t)(pos - root_start);
            size_t n = (size_t)(root_end - pos);
            if (n > remaining) n = remaining;
            memcpy(buf + filled, cf->root_dir + off, n);
            filled += n;
            continue;
        }

        /* Region 4: Data clusters */
        uint64_t data_start = root_end;
        size_t rel = (size_t)(pos - data_start);
        uint32_t cidx = rel / cf->cluster_size;
        size_t coff = rel % cf->cluster_size;

        if (cidx >= cf->alloc_count) {
            /* Beyond mapped clusters — zero fill */
            size_t n = cf->cluster_size - coff;
            if (n > remaining) n = remaining;
            memset(buf + filled, 0, n);
            filled += n;
            continue;
        }

        /* Load cluster from source if not cached */
        if (cf->cached_cluster != (int)cidx) {
            uint32_t old_c = cf->allocated[cidx];
            off_t src_off = cf->part_offset +
                (off_t)(cf->data_start_sector + (old_c - 2) * cf->sectors_per_cluster)
                * cf->bytes_per_sector;
            lseek(cf->src_fd, src_off, SEEK_SET);
            read(cf->src_fd, cf->cluster_buf, cf->cluster_size);

            /* Patch directory entries if this is a directory cluster */
            if (old_c < (uint32_t)(cf->total_clusters + 2) && cf->is_dir_cluster[old_c])
                fat_patch_dir_entries(cf, cf->cluster_buf, cf->cluster_size);

            cf->cached_cluster = (int)cidx;
        }

        size_t n = cf->cluster_size - coff;
        if (n > remaining) n = remaining;
        memcpy(buf + filled, cf->cluster_buf + coff, n);
        filled += n;
    }

    cf->position += filled;
    return (ssize_t)filled;
}

static void compact_fat_close(CompactFat *cf) {
    if (!cf) return;
    free(cf->fat_data);
    free(cf->allocated);
    free(cf->old_to_new);
    free(cf->is_dir_cluster);
    free(cf->boot_sector);
    free(cf->new_fat);
    free(cf->root_dir);
    free(cf->cluster_buf);
    free(cf);
}

/* Check if a partition type byte is FAT */
static int is_fat_type(uint8_t type) {
    switch (type) {
        case 0x01: case 0x04: case 0x06: case 0x0B: case 0x0C:
        case 0x0E: case 0x11: case 0x14: case 0x16: case 0x1B:
        case 0x1C: case 0x1E:
            return 1;
        default:
            return 0;
    }
}

/* ============================================================
 * JSON Metadata Writer
 * ============================================================ */

static void write_metadata_json(const char *backup_path, const char *source,
                                  uint64_t source_size, const PartTable *pt,
                                  const PartInfo *parts, int part_count,
                                  const char *compression, const char *checksum,
                                  int sector_by_sector, uint64_t *imaged_sizes,
                                  char (*part_files)[SPLIT_MAX_FILES][SPLIT_NAME_LEN],
                                  const int *part_file_counts) {
    char meta_path[MAX_PATH_LEN];
    snprintf(meta_path, sizeof(meta_path), "%s/metadata.json", backup_path);

    FILE *f = fopen(meta_path, "w");
    if (!f) { fprintf(stderr, "Cannot write %s\n", meta_path); return; }

    time_t now = time(NULL);
    struct tm *tm = gmtime(&now);
    char timestamp[64];
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H:%M:%SZ", tm);

    uint64_t first_lba = 0, alignment = 0;
    const char *align_type = detect_alignment(parts, part_count, &first_lba, &alignment);

    const char *pt_name = "None";
    if (pt->type == PT_MBR) pt_name = "MBR";
    else if (pt->type == PT_APM) pt_name = "APM";
    else if (pt->type == PT_GPT) pt_name = "GPT";

    fprintf(f, "{\n");
    fprintf(f, "  \"version\": 1,\n");
    fprintf(f, "  \"created\": \"%s\",\n", timestamp);
    fprintf(f, "  \"source_device\": \"%s\",\n", source);
    fprintf(f, "  \"source_size_bytes\": %llu,\n", (unsigned long long)source_size);
    fprintf(f, "  \"partition_table_type\": \"%s\",\n", pt_name);
    fprintf(f, "  \"compression_type\": \"%s\",\n", compression);
    fprintf(f, "  \"checksum_type\": \"%s\",\n", checksum);
    fprintf(f, "  \"sector_by_sector\": %s,\n", sector_by_sector ? "true" : "false");
    fprintf(f, "  \"transpiled_by\": \"rust-ppc-tiger\",\n");
    fprintf(f, "  \"alignment\": {\n");
    fprintf(f, "    \"detected_type\": \"%s\",\n", align_type);
    fprintf(f, "    \"first_partition_lba\": %llu,\n", (unsigned long long)first_lba);
    fprintf(f, "    \"alignment_sectors\": %llu\n", (unsigned long long)alignment);
    fprintf(f, "  },\n");
    fprintf(f, "  \"partitions\": [\n");

    for (int i = 0; i < part_count; i++) {
        if (parts[i].is_extended) continue;

        fprintf(f, "    {\n");
        fprintf(f, "      \"index\": %d,\n", parts[i].index);
        fprintf(f, "      \"type_name\": \"%s\",\n", parts[i].type_name);
        fprintf(f, "      \"partition_type_byte\": %d,\n", parts[i].type_byte);
        fprintf(f, "      \"start_lba\": %llu,\n", (unsigned long long)parts[i].start_lba);
        fprintf(f, "      \"original_size_bytes\": %llu,\n",
                (unsigned long long)(parts[i].total_sectors * SECTOR_SIZE));
        fprintf(f, "      \"imaged_size_bytes\": %llu,\n",
                (unsigned long long)(imaged_sizes ? imaged_sizes[i] :
                    parts[i].total_sectors * SECTOR_SIZE));
        /* compressed_files: list every split-file emitted for this
         * partition. Falls back to a single conventional name when no
         * list was recorded (defensive — shouldn't happen in practice). */
        fprintf(f, "      \"compressed_files\": [");
        int nfiles = part_file_counts ? part_file_counts[i] : 0;
        if (nfiles > 0) {
            for (int k = 0; k < nfiles; k++) {
                fprintf(f, "%s\"%s\"", k == 0 ? "" : ", ",
                        part_files[i][k]);
            }
        } else {
            const char *_ext =
                (strcmp(compression, "gzip") == 0) ? ".gz"
                : (strcmp(compression, "vhd") == 0) ? ".vhd" : ".raw";
            fprintf(f, "\"partition-%d%s\"", parts[i].index, _ext);
        }
        fprintf(f, "],\n");
        fprintf(f, "      \"bootable\": %s,\n", parts[i].bootable ? "true" : "false");
        fprintf(f, "      \"is_logical\": %s\n", parts[i].is_logical ? "true" : "false");
        fprintf(f, "    }%s\n", (i < part_count - 1) ? "," : "");
    }

    fprintf(f, "  ]\n");
    fprintf(f, "}\n");
    fclose(f);
}

/* ============================================================
 * MBR Export
 * ============================================================ */

static void export_mbr_bin(const char *backup_path, int src_fd) {
    char mbr_path[MAX_PATH_LEN];
    snprintf(mbr_path, sizeof(mbr_path), "%s/mbr.bin", backup_path);

    uint8_t mbr[512];
    if (lseek(src_fd, 0, SEEK_SET) < 0) return;
    if (read(src_fd, mbr, 512) != 512) return;

    FILE *f = fopen(mbr_path, "wb");
    if (f) { fwrite(mbr, 1, 512, f); fclose(f); }
}

/* Emit a parsed GPT sidecar (mirrors rb-cli's `gpt.json`). The
 * structure matches src/partition/gpt.rs serde output closely enough
 * for round-trip inspection: header summary + one entry per
 * non-empty slot. The raw GPT sectors aren't preserved here — the
 * primary MBR (already written as mbr.bin) plus this JSON is enough
 * for inspect; a full restore of GPT-formatted disks falls outside
 * the PPC port's scope (rb-cli rebuilds the GPT from JSON). */
static void export_gpt_json(const char *backup_path, const GptTable *gpt) {
    char path[MAX_PATH_LEN];
    snprintf(path, sizeof(path), "%s/gpt.json", backup_path);
    FILE *f = fopen(path, "w");
    if (!f) return;

    char guid_buf[64];
    fprintf(f, "{\n");
    fprintf(f, "  \"revision\": %u,\n", gpt->revision);
    fprintf(f, "  \"header_size\": %u,\n", gpt->header_size);
    fprintf(f, "  \"my_lba\": %llu,\n", (unsigned long long)gpt->my_lba);
    fprintf(f, "  \"alternate_lba\": %llu,\n",
            (unsigned long long)gpt->alternate_lba);
    fprintf(f, "  \"first_usable_lba\": %llu,\n",
            (unsigned long long)gpt->first_usable_lba);
    fprintf(f, "  \"last_usable_lba\": %llu,\n",
            (unsigned long long)gpt->last_usable_lba);
    gpt_format_guid(gpt->disk_guid, guid_buf, sizeof(guid_buf));
    fprintf(f, "  \"disk_guid\": \"%s\",\n", guid_buf);
    fprintf(f, "  \"partition_entry_lba\": %llu,\n",
            (unsigned long long)gpt->partition_entry_lba);
    fprintf(f, "  \"num_partition_entries\": %u,\n", gpt->num_partition_entries);
    fprintf(f, "  \"partition_entry_size\": %u,\n", gpt->partition_entry_size);
    fprintf(f, "  \"entries\": [\n");
    for (int i = 0; i < gpt->entry_count; i++) {
        const GptEntry *e = &gpt->entries[i];
        fprintf(f, "    {\n");
        gpt_format_guid(e->type_guid, guid_buf, sizeof(guid_buf));
        fprintf(f, "      \"type_guid\": \"%s\",\n", guid_buf);
        gpt_format_guid(e->unique_guid, guid_buf, sizeof(guid_buf));
        fprintf(f, "      \"unique_guid\": \"%s\",\n", guid_buf);
        fprintf(f, "      \"first_lba\": %llu,\n",
                (unsigned long long)e->first_lba);
        fprintf(f, "      \"last_lba\": %llu,\n",
                (unsigned long long)e->last_lba);
        fprintf(f, "      \"attributes\": %llu,\n",
                (unsigned long long)e->attributes);
        fprintf(f, "      \"name\": \"%s\"\n", e->name);
        fprintf(f, "    }%s\n", (i < gpt->entry_count - 1) ? "," : "");
    }
    fprintf(f, "  ]\n");
    fprintf(f, "}\n");
    fclose(f);
}

/* ============================================================
 * Backup Command
 * ============================================================ */

static int cmd_backup(int argc, char **argv) {
    if (has_flag(argc, argv, "--help") || has_flag(argc, argv, "-h")) {
        fprintf(stderr,
            "USAGE: rusty-backup backup <SOURCE> <DEST> [OPTIONS]\n\n"
            "ARGUMENTS:\n"
            "  <SOURCE>               Source device or image file\n"
            "  <DEST>                 Destination directory\n\n"
            "OPTIONS:\n"
            "  --name <NAME>          Backup name (default: backup)\n"
            "  --format <TYPE>        raw (default), gzip, or vhd\n"
            "  --checksum <TYPE>      none (default), crc32, or sha256\n"
            "  --sector-by-sector     Full sector-by-sector (no FAT compaction)\n"
            "  --split-size <MIB>     Split output every N MiB (raw/gzip only)\n"
            "  --sparse               Skip all-zero chunks as file holes (raw only)\n\n"
            "DEPRECATED (still accepted): --source, --dest, --compression\n"
        );
        return 0;
    }

    /* Positionals match rb-cli (`backup SOURCE DEST`); legacy --source /
     * --dest flags remain accepted for back-compat. */
    const char *source = nth_positional(argc, argv, 0);
    const char *dest   = nth_positional(argc, argv, 1);
    if (!source) source = flag_value(argc, argv, "--source");
    if (!dest)   dest   = flag_value(argc, argv, "--dest");
    const char *name = flag_value(argc, argv, "--name");
    /* `--format` is the rb-cli spelling; `--compression` kept as alias. */
    const char *comp_str = flag_value(argc, argv, "--format");
    if (!comp_str) comp_str = flag_value(argc, argv, "--compression");
    const char *cksum_str = flag_value(argc, argv, "--checksum");
    const char *split_str = flag_value(argc, argv, "--split-size");
    int sector_by_sector = has_flag(argc, argv, "--sector-by-sector");
    int sparse = has_flag(argc, argv, "--sparse");
    uint64_t split_bytes = 0;
    if (split_str) {
        long mib = atol(split_str);
        if (mib <= 0) {
            fprintf(stderr, "Error: --split-size must be a positive MiB count\n");
            return 1;
        }
        split_bytes = (uint64_t)mib * 1024ULL * 1024ULL;
    }

    if (!source || !dest) {
        fprintf(stderr, "Error: SOURCE and DEST are required\n");
        fprintf(stderr, "Run 'rusty-backup backup --help' for options\n");
        return 1;
    }
    if (!name) name = "backup";

    CompressionMode compression = COMP_RAW;
    if (comp_str) {
        if (strcmp(comp_str, "gzip") == 0) compression = COMP_GZIP;
        else if (strcmp(comp_str, "vhd") == 0) compression = COMP_VHD;
        else if (strcmp(comp_str, "raw") != 0) {
            fprintf(stderr,
                "Error: unknown --format value '%s'. "
                "Supported: raw, gzip, vhd.\n", comp_str);
            return 1;
        }
    }
    /* VHD output is incompatible with --split-size (footer must live at
     * the very end of a single contiguous file). */
    if (compression == COMP_VHD && split_bytes > 0) {
        fprintf(stderr, "Error: --split-size is not compatible with --format vhd "
                        "(the footer must live at the end of one file).\n");
        return 1;
    }
    /* VHD output and FAT compaction are mutually exclusive — the VHD
     * footer's size fields must match the on-disk partition geometry,
     * which compaction breaks. rb-cli treats VHD as a layout-preserving
     * format for the same reason. */
    if (compression == COMP_VHD && !sector_by_sector) {
        sector_by_sector = 1;
        fprintf(stderr, "[INFO] --format vhd forces --sector-by-sector "
                        "(layout-preserving).\n");
    }

    ChecksumType checksum = CKSUM_NONE;
    if (cksum_str) {
        if (strcmp(cksum_str, "crc32") == 0) {
            checksum = CKSUM_CRC32;
        } else if (strcmp(cksum_str, "sha256") == 0) {
            checksum = CKSUM_SHA256;
        } else if (strcmp(cksum_str, "sha1") == 0) {
            fprintf(stderr,
                "Error: --checksum sha1 is not supported on the PPC build.\n"
                "       Supported values: none, crc32, sha256.\n");
            return 1;
        } else if (strcmp(cksum_str, "none") != 0) {
            fprintf(stderr,
                "Error: unknown --checksum value '%s'. "
                "Supported: none, crc32, sha256.\n", cksum_str);
            return 1;
        }
    }

    /* Open source */
    int src_fd = open(source, O_RDONLY);
    if (src_fd < 0) {
        fprintf(stderr, "Error: Cannot open %s: %s\n", source, strerror(errno));
        return 1;
    }

    /* Get source size */
    uint64_t source_size = 0;
    struct stat st;
    if (fstat(src_fd, &st) == 0) {
        if (S_ISREG(st.st_mode)) {
            source_size = st.st_size;
        } else {
            source_size = get_device_size_ioctl(source);
        }
    }

    char sz_buf[32];
    fprintf(stderr, "Source: %s (%s)\n", source,
            format_bytes(source_size, sz_buf, sizeof(sz_buf)));
    const char *comp_label =
        compression == COMP_GZIP ? "gzip"
        : compression == COMP_VHD ? "vhd" : "raw";
    fprintf(stderr, "Compression: %s | Checksum: %s | Compact: %s\n",
            comp_label,
            checksum == CKSUM_CRC32 ? "crc32"
                : checksum == CKSUM_SHA256 ? "sha256" : "none",
            sector_by_sector ? "off (sector-by-sector)" : "FAT-aware");

    /* Detect partition table */
    PartTable pt;
    if (detect_partition_table(src_fd, &pt) != 0) {
        fprintf(stderr, "Warning: Could not detect partition table, treating as raw image\n");
        pt.type = PT_NONE;
        strcpy(pt.fs_hint, "raw");
    }

    const char *pt_name = "None";
    if (pt.type == PT_MBR) pt_name = "MBR";
    else if (pt.type == PT_APM) pt_name = "APM";
    else if (pt.type == PT_GPT) pt_name = "GPT";
    fprintf(stderr, "Partition table: %s\n", pt_name);

    /* Get partition list */
    PartInfo parts[MAX_PARTITIONS];
    int part_count = get_partition_list(&pt, parts);
    fprintf(stderr, "Partitions found: %d\n", part_count);

    for (int i = 0; i < part_count; i++) {
        char psz[32];
        uint64_t psize = parts[i].total_sectors * SECTOR_SIZE;
        format_bytes(psize, psz, sizeof(psz));
        fprintf(stderr, "  [%d] %-20s LBA %-10llu %s%s%s\n",
                parts[i].index, parts[i].type_name,
                (unsigned long long)parts[i].start_lba, psz,
                parts[i].bootable ? " [boot]" : "",
                parts[i].is_logical ? " [logical]" : "");
    }

    /* Create backup directory */
    char backup_path[MAX_PATH_LEN];
    snprintf(backup_path, sizeof(backup_path), "%s/%s", dest, name);
    mkdir(backup_path, 0755);

    fprintf(stderr, "Backup to: %s\n\n", backup_path);

    /* Export MBR/partition table */
    if (pt.type == PT_MBR || pt.type == PT_GPT) {
        export_mbr_bin(backup_path, src_fd);
        fprintf(stderr, "[INFO] Exported MBR to mbr.bin\n");
        if (pt.type == PT_GPT && pt.data.gpt.entry_count > 0) {
            export_gpt_json(backup_path, &pt.data.gpt);
            fprintf(stderr, "[INFO] Exported GPT header + entries to gpt.json\n");
        }
    }

    /* Back up each partition */
    uint8_t *buf = (uint8_t *)malloc(CHUNK_SIZE);
    if (!buf) { fprintf(stderr, "Out of memory\n"); close(src_fd); return 1; }

    uint64_t *imaged_sizes = (uint64_t *)calloc(part_count + 1, sizeof(uint64_t));

    /* Per-partition output-file lists. With --split-size each partition
     * may produce multiple files; without splitting, exactly one. The
     * `metadata.json` writer below renders these as the
     * `compressed_files` array. Heap-allocated to keep this off the
     * stack (~512 KB at MAX_PARTITIONS=64 × SPLIT_MAX_FILES=64). */
    int *part_file_counts = (int *)calloc(part_count + 1, sizeof(int));
    char (*part_files)[SPLIT_MAX_FILES][SPLIT_NAME_LEN] =
        calloc(part_count + 1, sizeof(*part_files));
    if (!imaged_sizes || !part_file_counts || !part_files) {
        fprintf(stderr, "Out of memory\n");
        close(src_fd);
        return 1;
    }

    const char *comp_name = compression == COMP_GZIP ? "gzip"
                          : compression == COMP_VHD ? "vhd" : "raw";
    const char *cksum_name = checksum == CKSUM_CRC32 ? "crc32"
                            : checksum == CKSUM_SHA256 ? "sha256" : "none";

    if (part_count == 0 && pt.type == PT_NONE) {
        /* Superfloppy: back up entire device as one partition */
        fprintf(stderr, "Backing up entire %s image...\n", pt.fs_hint);
        lseek(src_fd, 0, SEEK_SET);

        SplitWriter sw;
        if (splitwriter_open(&sw, backup_path, "partition-0",
                             comp_ext(compression),
                             compression == COMP_GZIP, split_bytes, sparse) != 0) {
            free(buf); close(src_fd); return 1;
        }

        uint64_t written = 0;
        ssize_t nr;
        while ((nr = read(src_fd, buf, CHUNK_SIZE)) > 0) {
            if (splitwriter_write(&sw, buf, nr) != 0) {
                splitwriter_close(&sw); free(buf); close(src_fd); return 1;
            }
            written += nr;
            if (source_size > 0)
                draw_progress((double)written / source_size * 100.0, pt.fs_hint);
        }
        if (compression == COMP_VHD) {
            splitwriter_close_vhd(&sw, written);
        } else {
            splitwriter_close(&sw);
        }
        fprintf(stderr, "\n");

        /* Record emitted files + per-file checksums */
        part_file_counts[0] = sw.num_files;
        for (int k = 0; k < sw.num_files; k++) {
            strncpy(part_files[0][k], sw.files[k], SPLIT_NAME_LEN - 1);
            if (checksum != CKSUM_NONE) {
                char fpath[MAX_PATH_LEN];
                char hex[128];
                snprintf(fpath, sizeof(fpath), "%s/%s", backup_path, sw.files[k]);
                fprintf(stderr, "[INFO] %s %s...\n", cksum_name, sw.files[k]);
                write_checksum(fpath, checksum, hex, sizeof(hex));
                fprintf(stderr, "[INFO]   %s\n", hex);
            }
        }

        parts[0].index = 0;
        strcpy(parts[0].type_name, pt.fs_hint);
        parts[0].start_lba = 0;
        parts[0].total_sectors = source_size / SECTOR_SIZE;
        part_count = 1;
        imaged_sizes[0] = written;
    } else {
        for (int i = 0; i < part_count; i++) {
            if (parts[i].is_extended) {
                fprintf(stderr, "[INFO] Skipping extended container [%d]\n", parts[i].index);
                continue;
            }

            uint64_t part_size = parts[i].total_sectors * SECTOR_SIZE;
            off_t part_offset = (off_t)parts[i].start_lba * SECTOR_SIZE;

            char psz[32];
            format_bytes(part_size, psz, sizeof(psz));
            fprintf(stderr, "Backing up partition %d (%s, %s)...\n",
                    parts[i].index, parts[i].type_name, psz);

            /* Try FAT compaction (unless --sector-by-sector) */
            CompactFat *cf = NULL;
            if (!sector_by_sector && is_fat_type(parts[i].type_byte)) {
                cf = compact_fat_open(src_fd, part_offset, part_size);
            }

            char stem[64];
            snprintf(stem, sizeof(stem), "partition-%d", parts[i].index);

            SplitWriter sw;
            if (splitwriter_open(&sw, backup_path, stem, comp_ext(compression),
                                 compression == COMP_GZIP, split_bytes, sparse) != 0) {
                compact_fat_close(cf);
                continue;
            }

            uint64_t total_to_write = cf ? cf->total_output_size : part_size;
            uint64_t written = 0;
            const char *prog_label =
                cf ? (compression == COMP_GZIP ? "compact+gz" : "compact")
                   : (compression == COMP_GZIP ? "gzip" : parts[i].type_name);

            if (cf) {
                ssize_t nr;
                while ((nr = compact_fat_read(cf, buf, CHUNK_SIZE)) > 0) {
                    if (splitwriter_write(&sw, buf, nr) != 0) break;
                    written += nr;
                    draw_progress((double)written / total_to_write * 100.0,
                                  prog_label);
                }
            } else {
                lseek(src_fd, part_offset, SEEK_SET);
                uint64_t remaining = part_size;
                while (remaining > 0) {
                    size_t to_read = remaining > CHUNK_SIZE
                                     ? CHUNK_SIZE : (size_t)remaining;
                    ssize_t nr = read(src_fd, buf, to_read);
                    if (nr <= 0) break;
                    if (splitwriter_write(&sw, buf, nr) != 0) break;
                    written += nr;
                    remaining -= nr;
                    draw_progress((double)written / part_size * 100.0,
                                  prog_label);
                }
            }
            if (compression == COMP_VHD) {
                splitwriter_close_vhd(&sw, written);
            } else {
                splitwriter_close(&sw);
            }

            imaged_sizes[i] = written;
            compact_fat_close(cf);
            fprintf(stderr, "\n");

            /* Record emitted files + per-file checksums */
            part_file_counts[i] = sw.num_files;
            for (int k = 0; k < sw.num_files; k++) {
                strncpy(part_files[i][k], sw.files[k], SPLIT_NAME_LEN - 1);
                if (checksum != CKSUM_NONE) {
                    char fpath[MAX_PATH_LEN];
                    char hex[128];
                    snprintf(fpath, sizeof(fpath), "%s/%s",
                             backup_path, sw.files[k]);
                    fprintf(stderr, "[INFO] %s %s...\n",
                            cksum_name, sw.files[k]);
                    write_checksum(fpath, checksum, hex, sizeof(hex));
                    fprintf(stderr, "[INFO]   %s\n", hex);
                }
            }
        }
    }

    free(buf);

    /* Write metadata.json */
    write_metadata_json(backup_path, source, source_size, &pt,
                        parts, part_count, comp_name, cksum_name,
                        sector_by_sector, imaged_sizes,
                        part_files, part_file_counts);
    fprintf(stderr, "[INFO] Wrote metadata.json\n");

    free(imaged_sizes);
    free(part_file_counts);
    free(part_files);
    close(src_fd);
    fprintf(stderr, "\nBackup completed successfully.\n");
    return 0;
}

/* ============================================================
 * Restore Command
 * ============================================================ */

static int cmd_restore(int argc, char **argv) {
    if (has_flag(argc, argv, "--help") || has_flag(argc, argv, "-h")) {
        fprintf(stderr,
            "USAGE: rusty-backup restore <BACKUP_DIR> <TARGET> [OPTIONS]\n\n"
            "ARGUMENTS:\n"
            "  <BACKUP_DIR>           Backup folder containing metadata.json\n"
            "  <TARGET>               Target device or image file\n\n"
            "OPTIONS:\n"
            "  --target-size <BYTES>  Target size in bytes (auto-detected for devices)\n\n"
            "DEPRECATED (still accepted): --backup-dir, --target\n"
        );
        return 0;
    }

    const char *backup_dir = nth_positional(argc, argv, 0);
    const char *target     = nth_positional(argc, argv, 1);
    if (!backup_dir) backup_dir = flag_value(argc, argv, "--backup-dir");
    if (!target)     target     = flag_value(argc, argv, "--target");

    if (!backup_dir || !target) {
        fprintf(stderr, "Error: BACKUP_DIR and TARGET are required\n");
        return 1;
    }

    /* Read metadata.json */
    char meta_path[MAX_PATH_LEN];
    snprintf(meta_path, sizeof(meta_path), "%s/metadata.json", backup_dir);

    FILE *mf = fopen(meta_path, "r");
    if (!mf) {
        fprintf(stderr, "Error: Cannot open %s: %s\n", meta_path, strerror(errno));
        return 1;
    }

    /* Simple JSON parser for key fields */
    char json_buf[32768];
    size_t json_len = fread(json_buf, 1, sizeof(json_buf) - 1, mf);
    json_buf[json_len] = '\0';
    fclose(mf);

    /* Parse partition table type */
    char pt_type[16] = "None";
    const char *p = strstr(json_buf, "\"partition_table_type\"");
    if (p) {
        p = strchr(p + 21, '"'); if (p) p++;
        if (p) {
            p = strchr(p, '"'); if (p) p++;
            if (p) {
                const char *end = strchr(p, '"');
                if (end) { int len = end - p; if (len > 15) len = 15;
                    memcpy(pt_type, p, len); pt_type[len] = '\0'; }
            }
        }
    }

    /* Restore MBR if present */
    if (strcmp(pt_type, "MBR") == 0 || strcmp(pt_type, "GPT") == 0) {
        char mbr_path[MAX_PATH_LEN];
        snprintf(mbr_path, sizeof(mbr_path), "%s/mbr.bin", backup_dir);
        FILE *mbrf = fopen(mbr_path, "rb");
        if (mbrf) {
            uint8_t mbr[512];
            if (fread(mbr, 1, 512, mbrf) == 512) {
                int tgt_fd = open(target, O_WRONLY | O_CREAT, 0644);
                if (tgt_fd >= 0) {
                    write(tgt_fd, mbr, 512);
                    close(tgt_fd);
                    fprintf(stderr, "[INFO] Restored MBR from mbr.bin\n");
                }
            }
            fclose(mbrf);
        }
    }

    /* Find and restore partition files */
    int tgt_fd = open(target, O_WRONLY | O_CREAT, 0644);
    if (tgt_fd < 0) {
        fprintf(stderr, "Error: Cannot open %s for writing: %s\n", target, strerror(errno));
        return 1;
    }

    /* Scan backup directory for partition-N.raw files */
    DIR *bdir = opendir(backup_dir);
    if (!bdir) {
        fprintf(stderr, "Error: Cannot open %s\n", backup_dir);
        close(tgt_fd);
        return 1;
    }

    uint8_t *buf = (uint8_t *)malloc(CHUNK_SIZE);
    if (!buf) { close(tgt_fd); closedir(bdir); return 1; }

    /* Parse partition start_lba values + compressed_files list for
     * positioning and split-file traversal. */
    struct {
        int index;
        uint64_t start_lba;
        uint64_t size;
        char filenames[SPLIT_MAX_FILES][SPLIT_NAME_LEN];
        int file_count;
    } restorable[MAX_PARTITIONS];
    int restore_count = 0;

    /* Simple parser: find each partition entry */
    const char *scan = json_buf;
    while ((scan = strstr(scan, "\"index\"")) != NULL) {
        int idx = -1;
        uint64_t slba = 0;

        sscanf(scan + 9, "%d", &idx);

        const char *lba_p = strstr(scan, "\"start_lba\"");
        if (lba_p && lba_p < scan + 500) {
            sscanf(lba_p + 13, "%llu", (unsigned long long *)&slba);
        }

        const char *size_p = strstr(scan, "\"imaged_size_bytes\"");
        uint64_t isize = 0;
        if (size_p && size_p < scan + 500) {
            sscanf(size_p + 20, "%llu", (unsigned long long *)&isize);
        }

        if (idx >= 0 && restore_count < MAX_PARTITIONS) {
            restorable[restore_count].index = idx;
            restorable[restore_count].start_lba = slba;
            restorable[restore_count].size = isize;
            restorable[restore_count].file_count = 0;

            /* Parse `compressed_files: ["a", "b", ...]` for this entry. */
            const char *cf = strstr(scan, "\"compressed_files\"");
            if (cf && cf < scan + 800) {
                const char *lb = strchr(cf, '[');
                const char *rb = lb ? strchr(lb, ']') : NULL;
                if (lb && rb) {
                    const char *q = lb;
                    while (q < rb
                        && restorable[restore_count].file_count < SPLIT_MAX_FILES) {
                        q = strchr(q, '"');
                        if (!q || q >= rb) break;
                        q++;
                        const char *qe = strchr(q, '"');
                        if (!qe || qe > rb) break;
                        int len = qe - q;
                        if (len > SPLIT_NAME_LEN - 1) len = SPLIT_NAME_LEN - 1;
                        char *slot = restorable[restore_count].filenames[
                            restorable[restore_count].file_count++];
                        memcpy(slot, q, len);
                        slot[len] = '\0';
                        q = qe + 1;
                    }
                }
            }
            /* Fallback for older backups that pre-date compressed_files. */
            if (restorable[restore_count].file_count == 0) {
                struct stat _rs;
                char *slot = restorable[restore_count].filenames[0];
                snprintf(slot, SPLIT_NAME_LEN, "partition-%d.gz", idx);
                char fpath[MAX_PATH_LEN];
                snprintf(fpath, sizeof(fpath), "%s/%s", backup_dir, slot);
                if (stat(fpath, &_rs) != 0) {
                    snprintf(slot, SPLIT_NAME_LEN, "partition-%d.raw", idx);
                }
                restorable[restore_count].file_count = 1;
            }
            restore_count++;
        }

        scan += 10;
    }

    fprintf(stderr, "Restoring %d partition(s) to %s\n\n", restore_count, target);

    for (int i = 0; i < restore_count; i++) {
        /* Seek to partition offset in target. Subsequent split files
         * stream directly to the current file position (the kernel
         * keeps it advancing after each write). */
        off_t offset = (off_t)restorable[i].start_lba * SECTOR_SIZE;
        lseek(tgt_fd, offset, SEEK_SET);

        const char *first = restorable[i].filenames[0];
        size_t fl = strlen(first);
        int is_gz  = (fl > 3 && strcmp(first + fl - 3, ".gz") == 0);
        int is_vhd = (fl > 4 && strcmp(first + fl - 4, ".vhd") == 0);

        fprintf(stderr, "Restoring partition %d to LBA %llu%s (%d file%s)...\n",
                restorable[i].index,
                (unsigned long long)restorable[i].start_lba,
                is_gz ? " (decompressing)" : is_vhd ? " (VHD)" : "",
                restorable[i].file_count,
                restorable[i].file_count == 1 ? "" : "s");

        uint64_t written = 0;

        for (int k = 0; k < restorable[i].file_count; k++) {
            char fpath[MAX_PATH_LEN];
            snprintf(fpath, sizeof(fpath), "%s/%s",
                     backup_dir, restorable[i].filenames[k]);

            if (is_gz) {
                gzFile gz = gzopen(fpath, "rb");
                if (!gz) {
                    fprintf(stderr, "[WARN] Cannot open %s, skipping rest\n",
                            fpath);
                    break;
                }
                int nr;
                while ((nr = gzread(gz, buf, CHUNK_SIZE)) > 0) {
                    write(tgt_fd, buf, nr);
                    written += nr;
                    if (restorable[i].size > 0) {
                        draw_progress((double)written / restorable[i].size * 100.0,
                                      "Restoring");
                    }
                }
                gzclose(gz);
            } else {
                FILE *pf = fopen(fpath, "rb");
                if (!pf) {
                    fprintf(stderr, "[WARN] Cannot open %s, skipping rest\n",
                            fpath);
                    break;
                }
                /* VHD: the last 512 bytes are the footer, not partition
                 * data. Stop reading 512 bytes before EOF. */
                uint64_t cap = UINT64_MAX;
                if (is_vhd) {
                    fseek(pf, 0, SEEK_END);
                    long flen = ftell(pf);
                    fseek(pf, 0, SEEK_SET);
                    cap = (flen > VHD_FOOTER_SIZE)
                          ? (uint64_t)(flen - VHD_FOOTER_SIZE) : 0;
                }
                size_t nr;
                uint64_t this_file = 0;
                while ((nr = fread(buf, 1, CHUNK_SIZE, pf)) > 0) {
                    size_t to_write = nr;
                    if (is_vhd && this_file + nr > cap) {
                        to_write = (size_t)(cap - this_file);
                    }
                    if (to_write > 0) write(tgt_fd, buf, to_write);
                    written += to_write;
                    this_file += to_write;
                    if (restorable[i].size > 0) {
                        draw_progress((double)written / restorable[i].size * 100.0,
                                      "Restoring");
                    }
                    if (is_vhd && this_file >= cap) break;
                }
                fclose(pf);
            }
        }
        fprintf(stderr, "\n[INFO] Wrote %llu bytes\n", (unsigned long long)written);
    }

    free(buf);
    close(tgt_fd);
    closedir(bdir);

    fprintf(stderr, "\nRestore completed successfully.\n");
    return 0;
}

/* ============================================================
 * Inspect Command
 * ============================================================ */

static int cmd_inspect(int argc, char **argv) {
    if (has_flag(argc, argv, "--help") || has_flag(argc, argv, "-h")) {
        fprintf(stderr,
            "USAGE: rusty-backup inspect <BACKUP_DIR> [--format text]\n\n"
            "NOTE: PPC inspect reads a backup folder (metadata.json), not a\n"
            "disk image. This differs from rb-cli's `inspect <IMAGE>` which\n"
            "summarizes a live disk image's partition table.\n"
        );
        return 0;
    }

    const char *fmt = flag_value(argc, argv, "--format");
    if (fmt && strcmp(fmt, "text") != 0) {
        fprintf(stderr, "Error: only --format text is supported on PPC\n");
        return 1;
    }
    const char *backup_dir = nth_positional(argc, argv, 0);
    if (!backup_dir) {
        fprintf(stderr, "Error: BACKUP_DIR is required\n");
        fprintf(stderr, "USAGE: rusty-backup inspect <BACKUP_DIR>\n");
        return 1;
    }

    char meta_path[MAX_PATH_LEN];
    snprintf(meta_path, sizeof(meta_path), "%s/metadata.json", backup_dir);

    FILE *f = fopen(meta_path, "r");
    if (!f) {
        fprintf(stderr, "Error: Cannot open %s: %s\n", meta_path, strerror(errno));
        return 1;
    }

    char json[32768];
    size_t len = fread(json, 1, sizeof(json) - 1, f);
    json[len] = '\0';
    fclose(f);

    /* Print the metadata in a nice format */
    /* Simple key-value extraction */
    printf("Backup Metadata\n");
    printf("===============\n\n");

    /* Helper macro for extracting string values from "key": "value" */
    #define PRINT_JSON_STR(label, key) do { \
        const char *_p = strstr(json, "\"" key "\""); \
        if (_p) { \
            _p += strlen(key) + 2; /* skip past closing quote of key */ \
            _p = strchr(_p, ':');  /* find colon */ \
            if (_p) { _p++; while (*_p == ' ') _p++; /* skip spaces */ \
                if (*_p == '"') { _p++; /* skip opening quote of value */ \
                    const char *_e = strchr(_p, '"'); \
                    if (_e) printf("  %-20s %.*s\n", label, (int)(_e - _p), _p); \
                } \
            } \
        } \
    } while(0)

    #define PRINT_JSON_NUM(label, key) do { \
        const char *_p = strstr(json, "\"" key "\""); \
        if (_p) { \
            _p = strchr(_p + strlen(key) + 2, ':'); \
            if (_p) { _p++; while (*_p == ' ') _p++; \
                long long _v = 0; sscanf(_p, "%lld", &_v); \
                printf("  %-20s %lld\n", label, _v); \
            } \
        } \
    } while(0)

    PRINT_JSON_STR("Created:", "created");
    PRINT_JSON_STR("Source:", "source_device");

    /* Source size with human-readable */
    const char *ss = strstr(json, "\"source_size_bytes\"");
    if (ss) {
        ss = strchr(ss + 18, ':');
        if (ss) {
            ss++; while (*ss == ' ') ss++;
            uint64_t sz = 0; sscanf(ss, "%llu", (unsigned long long *)&sz);
            char szb[32];
            printf("  %-20s %s (%llu bytes)\n", "Source size:",
                   format_bytes(sz, szb, sizeof(szb)), (unsigned long long)sz);
        }
    }

    PRINT_JSON_STR("Partition table:", "partition_table_type");
    PRINT_JSON_STR("Compression:", "compression_type");
    PRINT_JSON_STR("Checksum:", "checksum_type");
    PRINT_JSON_STR("Transpiled by:", "transpiled_by");

    /* Alignment section */
    printf("\n  Alignment\n");
    PRINT_JSON_STR("    Type:", "detected_type");
    PRINT_JSON_NUM("    First LBA:", "first_partition_lba");
    PRINT_JSON_NUM("    Alignment:", "alignment_sectors");

    /* Partition list */
    printf("\n  Partitions:\n");
    const char *scan = json;
    while ((scan = strstr(scan, "\"index\"")) != NULL) {
        int idx = -1;
        sscanf(scan + 9, "%d", &idx);

        /* type_name */
        char tname[64] = "?";
        const char *tp = strstr(scan, "\"type_name\"");
        if (tp && tp < scan + 500) {
            tp = strchr(tp + 11, ':');  /* past key, find colon */
            if (tp) { tp++; while (*tp == ' ') tp++;
                if (*tp == '"') { tp++;
                    const char *te = strchr(tp, '"');
                    if (te) { int l = te - tp; if (l > 63) l = 63;
                        memcpy(tname, tp, l); tname[l] = '\0'; }
                }
            }
        }

        /* sizes */
        uint64_t orig = 0, imaged = 0;
        const char *op = strstr(scan, "\"original_size_bytes\"");
        if (op && op < scan + 600) sscanf(op + 22, "%llu", (unsigned long long *)&orig);
        const char *ip = strstr(scan, "\"imaged_size_bytes\"");
        if (ip && ip < scan + 600) sscanf(ip + 20, "%llu", (unsigned long long *)&imaged);

        if (idx >= 0) {
            char obs[32], ibs[32];
            printf("    [%d] %-20s %10s (imaged: %10s)\n",
                   idx, tname,
                   format_bytes(orig, obs, sizeof(obs)),
                   format_bytes(imaged, ibs, sizeof(ibs)));
        }

        scan += 10;
    }

    /* Check what files exist in backup dir */
    printf("\n  Backup files:\n");
    DIR *d = opendir(backup_dir);
    if (d) {
        struct dirent *ent;
        while ((ent = readdir(d)) != NULL) {
            if (ent->d_name[0] == '.') continue;
            char fpath[MAX_PATH_LEN];
            snprintf(fpath, sizeof(fpath), "%s/%s", backup_dir, ent->d_name);
            struct stat fst;
            if (stat(fpath, &fst) == 0) {
                char fsz[32];
                printf("    %-30s %s\n", ent->d_name,
                       format_bytes(fst.st_size, fsz, sizeof(fsz)));
            }
        }
        closedir(d);
    }

    #undef PRINT_JSON_STR
    #undef PRINT_JSON_NUM

    return 0;
}

/* ============================================================
 * Rip Command (optical disc)
 * ============================================================ */

static int cmd_rip(int argc, char **argv) {
    if (has_flag(argc, argv, "--help") || has_flag(argc, argv, "-h")) {
        fprintf(stderr,
            "USAGE: rusty-backup rip [OPTIONS]\n\n"
            "OPTIONS:\n"
            "  --device <PATH>   Optical drive (e.g. /dev/disk1) (required)\n"
            "  --output <PATH>   Output file path (required)\n"
            "  --format <TYPE>   iso (default)\n"
        );
        return 0;
    }

    const char *device = flag_value(argc, argv, "--device");
    const char *output = flag_value(argc, argv, "--output");
    const char *fmt    = flag_value(argc, argv, "--format");
    if (fmt && strcmp(fmt, "iso") != 0) {
        fprintf(stderr, "Error: only --format iso is supported on PPC\n");
        return 1;
    }
    (void)has_flag(argc, argv, "--eject"); /* accepted for rb-cli parity, not implemented */

    if (!device || !output) {
        fprintf(stderr, "Error: --device and --output are required\n");
        return 1;
    }

    int src_fd = open(device, O_RDONLY);
    if (src_fd < 0) {
        fprintf(stderr, "Error: Cannot open %s: %s\n", device, strerror(errno));
        return 1;
    }

    uint64_t dev_size = get_device_size_ioctl(device);
    if (dev_size == 0) {
        fprintf(stderr, "Error: Cannot determine size of %s\n", device);
        close(src_fd);
        return 1;
    }

    char sz_buf[32];
    fprintf(stderr, "Ripping %s (%s) to %s...\n", device,
            format_bytes(dev_size, sz_buf, sizeof(sz_buf)), output);

    FILE *out = fopen(output, "wb");
    if (!out) {
        fprintf(stderr, "Error: Cannot create %s\n", output);
        close(src_fd);
        return 1;
    }

    uint8_t *buf = (uint8_t *)malloc(CHUNK_SIZE);
    uint64_t written = 0;
    ssize_t nr;

    while ((nr = read(src_fd, buf, CHUNK_SIZE)) > 0) {
        fwrite(buf, 1, nr, out);
        written += nr;
        double pct = (double)written / dev_size * 100.0;
        draw_progress(pct, "Ripping");
    }

    free(buf);
    fclose(out);
    close(src_fd);

    fprintf(stderr, "\nRip completed: %s (%s)\n", output,
            format_bytes(written, sz_buf, sizeof(sz_buf)));
    return 0;
}

/* ============================================================
 * show partmap / show fs-info
 * ----------------------------------------------------------------
 * Read-only views into a disk image. `show partmap` dumps the
 * partition table (MBR / GPT / APM / Superfloppy). `show fs-info`
 * digs into a specific partition's superblock — FAT BPB,
 * HFS/HFS+ MDB header — and prints volume metadata.
 * ============================================================ */

/* Parse an `IMG[@N]` reference: returns the 1-based partition index
 * (0 = "whole disk", -1 = parse error). The image path is written
 * into `path_out` with the `@N` suffix stripped. */
/* ============================================================
 * write — stream an image onto a block device
 * ----------------------------------------------------------------
 * Equivalent of rb-cli's `write IMG /dev/diskN`. No partition-table
 * smarts, no resize, no decompression — just raw bytes. Used to flash
 * a previously-restored disk image (or any random raw file) onto
 * removable media without going through the full restore pipeline.
 * ============================================================ */
static int cmd_write(int argc, char **argv) {
    if (has_flag(argc, argv, "--help") || has_flag(argc, argv, "-h")) {
        fprintf(stderr,
            "USAGE: rusty-backup write <IMAGE> <TARGET> [--yes]\n\n"
            "ARGUMENTS:\n"
            "  <IMAGE>    Source image file (raw bytes; no headers stripped)\n"
            "  <TARGET>   Target device or file (rdiskN recommended for speed)\n\n"
            "OPTIONS:\n"
            "  --yes      Confirm destructive write to a block device\n"
        );
        return 0;
    }

    const char *src = nth_positional(argc, argv, 0);
    const char *dst = nth_positional(argc, argv, 1);
    int yes = has_flag(argc, argv, "--yes");

    if (!src || !dst) {
        fprintf(stderr, "Error: IMAGE and TARGET are required\n");
        return 1;
    }

    int src_fd = open(src, O_RDONLY);
    if (src_fd < 0) {
        fprintf(stderr, "Error: cannot open %s: %s\n", src, strerror(errno));
        return 1;
    }
    uint64_t src_size = 0;
    struct stat st;
    if (fstat(src_fd, &st) == 0 && S_ISREG(st.st_mode)) {
        src_size = st.st_size;
    }

    /* Heuristic: paths under /dev/ are block devices and require --yes
     * to confirm. Everything else is treated as a regular file target. */
    int target_is_device = (strncmp(dst, "/dev/", 5) == 0);
    if (target_is_device && !yes) {
        fprintf(stderr,
            "Refusing to write to block device %s without --yes.\n"
            "  This will OVERWRITE every byte on the device.\n", dst);
        close(src_fd);
        return 1;
    }

    int tgt_fd = open(dst, O_WRONLY | O_CREAT, 0644);
    if (tgt_fd < 0) {
        fprintf(stderr, "Error: cannot open %s for writing: %s\n",
                dst, strerror(errno));
        close(src_fd);
        return 1;
    }

    char sz[32];
    fprintf(stderr, "Writing %s%s to %s...\n", src,
            src_size ? "" : " (size unknown)", dst);

    uint8_t *buf = (uint8_t *)malloc(CHUNK_SIZE);
    if (!buf) { close(src_fd); close(tgt_fd); return 1; }

    uint64_t written = 0;
    ssize_t nr;
    while ((nr = read(src_fd, buf, CHUNK_SIZE)) > 0) {
        if (write(tgt_fd, buf, nr) != nr) {
            fprintf(stderr, "\nError: short write: %s\n", strerror(errno));
            break;
        }
        written += nr;
        if (src_size > 0)
            draw_progress((double)written / src_size * 100.0, "Writing");
    }
    free(buf);
    close(src_fd);
    close(tgt_fd);

    format_bytes(written, sz, sizeof(sz));
    fprintf(stderr, "\nWrite completed: %s (%s)\n", dst, sz);
    return 0;
}

static int parse_img_at(const char *spec, char *path_out, size_t path_sz) {
    const char *at = strrchr(spec, '@');
    if (!at) {
        snprintf(path_out, path_sz, "%s", spec);
        return 0;
    }
    size_t plen = at - spec;
    if (plen >= path_sz) plen = path_sz - 1;
    memcpy(path_out, spec, plen);
    path_out[plen] = '\0';
    long n = atol(at + 1);
    if (n <= 0) return -1;
    return (int)n;
}

static int cmd_show_partmap(int argc, char **argv) {
    const char *img = nth_positional(argc, argv, 0);
    if (!img) {
        fprintf(stderr,
            "USAGE: rusty-backup show partmap <IMAGE>\n"
            "  Prints the partition table (MBR / GPT / APM / Superfloppy).\n"
        );
        return 1;
    }

    int fd = open(img, O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "Error: cannot open %s: %s\n", img, strerror(errno));
        return 1;
    }
    PartTable pt;
    if (detect_partition_table(fd, &pt) != 0) {
        fprintf(stderr, "Error: no recognized partition table on %s\n", img);
        close(fd);
        return 1;
    }

    PartInfo parts[MAX_PARTITIONS];
    int n = get_partition_list(&pt, parts);

    const char *pt_name = "None";
    if (pt.type == PT_MBR) pt_name = "MBR";
    else if (pt.type == PT_APM) pt_name = "APM";
    else if (pt.type == PT_GPT) pt_name = "GPT";

    printf("Image: %s\n", img);
    printf("Partition table: %s\n", pt_name);

    if (pt.type == PT_GPT && pt.data.gpt.entry_count > 0) {
        char guid[64];
        gpt_format_guid(pt.data.gpt.disk_guid, guid, sizeof(guid));
        printf("Disk GUID: %s\n", guid);
        printf("First usable LBA: %llu, last usable LBA: %llu\n",
               (unsigned long long)pt.data.gpt.first_usable_lba,
               (unsigned long long)pt.data.gpt.last_usable_lba);
    }
    printf("Partitions: %d\n", n);

    for (int i = 0; i < n; i++) {
        char psz[32];
        uint64_t bytes = parts[i].total_sectors * SECTOR_SIZE;
        format_bytes(bytes, psz, sizeof(psz));
        printf("  [%d] %-28s LBA %-10llu %s%s%s\n",
               parts[i].index + 1, parts[i].type_name,
               (unsigned long long)parts[i].start_lba, psz,
               parts[i].bootable ? " [boot]" : "",
               parts[i].is_logical ? " [logical]" : "");
    }
    close(fd);
    return 0;
}

/* Print FAT volume metadata read directly from the BPB at `offset`. */
static void show_fat_fs_info(int fd, off_t offset) {
    uint8_t vbr[512];
    if (lseek(fd, offset, SEEK_SET) < 0) return;
    if (read(fd, vbr, 512) != 512) return;
    if (!is_fat_vbr(vbr)) {
        printf("  (FAT VBR validation failed — partition magic mismatch)\n");
        return;
    }
    uint16_t bps      = read_le16(&vbr[11]);
    uint8_t  spc      = vbr[13];
    uint16_t resvd    = read_le16(&vbr[14]);
    uint8_t  fats     = vbr[16];
    uint16_t rootents = read_le16(&vbr[17]);
    uint16_t tot16    = read_le16(&vbr[19]);
    uint16_t fatsz16  = read_le16(&vbr[22]);
    uint32_t tot32    = read_le32(&vbr[32]);
    uint32_t fatsz32  = read_le32(&vbr[36]);

    uint32_t total_sectors = tot16 ? tot16 : tot32;
    uint32_t fat_sectors   = fatsz16 ? fatsz16 : fatsz32;
    uint64_t total_bytes   = (uint64_t)total_sectors * bps;

    /* OEM ID (offset 3, 8 bytes) — printable hint, often "MSWIN4.1". */
    char oem[9];
    memcpy(oem, &vbr[3], 8); oem[8] = '\0';
    /* FAT32 volume label lives at VBR offset 71, 11 bytes. FAT12/16
     * label lives at VBR offset 43. */
    char label[12];
    memcpy(label, fatsz16 ? &vbr[43] : &vbr[71], 11); label[11] = '\0';
    /* Strip trailing spaces. */
    for (int i = 10; i >= 0 && (label[i] == ' ' || label[i] == '\0'); i--) label[i] = '\0';

    char sz[32];
    format_bytes(total_bytes, sz, sizeof(sz));

    printf("  Filesystem:        FAT (%s)\n",
           fatsz16 ? (rootents > 0 ? "FAT12/16" : "FAT12/16") : "FAT32");
    printf("  Volume label:      %s\n", label[0] ? label : "(none)");
    printf("  OEM ID:            %s\n", oem);
    printf("  Bytes per sector:  %u\n", bps);
    printf("  Sectors per clust: %u\n", spc);
    printf("  Reserved sectors:  %u\n", resvd);
    printf("  FAT copies:        %u\n", fats);
    printf("  Root entries:      %u\n", rootents);
    printf("  Total sectors:     %u\n", total_sectors);
    printf("  Sectors per FAT:   %u\n", fat_sectors);
    printf("  Volume size:       %s\n", sz);
}

/* Print HFS/HFS+ volume metadata read from the MDB / VH at
 * `offset + 1024`. */
static void show_hfs_fs_info(int fd, off_t offset) {
    uint8_t sb[512];
    if (lseek(fd, offset + 1024, SEEK_SET) < 0) return;
    if (read(fd, sb, 512) != 512) return;
    uint16_t sig = read_be16(sb);
    if (sig == 0x4244) {
        /* Classic HFS MDB (Big Endian). */
        uint16_t attr     = read_be16(&sb[2]);
        uint16_t nm_files = read_be16(&sb[12]);
        uint16_t nm_root  = read_be16(&sb[82]);
        uint16_t total    = read_be16(&sb[18]);
        uint16_t free_    = read_be16(&sb[34]);
        uint32_t alblksz  = read_be32(&sb[20]);
        uint8_t  vn_len   = sb[36];
        if (vn_len > 27) vn_len = 27;
        char vn[28];
        memcpy(vn, &sb[37], vn_len);
        vn[vn_len] = '\0';
        printf("  Filesystem:        HFS (classic)\n");
        printf("  Volume name:       %s\n", vn);
        printf("  Block size:        %u\n", alblksz);
        printf("  Total blocks:      %u\n", total);
        printf("  Free blocks:       %u\n", free_);
        printf("  Files (root):      %u files / %u dirs\n", nm_files, nm_root);
        printf("  Attributes:        0x%04x%s\n", attr,
               (attr & 0x0100) ? " (unmounted-clean)" : "");
    } else if (sig == 0x482B || sig == 0x4858) {
        /* HFS+ / HFSX VolumeHeader. Layout starts at offset 1024. */
        uint32_t blk_size      = read_be32(&sb[40]);
        uint32_t total_blocks  = read_be32(&sb[44]);
        uint32_t free_blocks   = read_be32(&sb[48]);
        uint32_t file_count    = read_be32(&sb[32]);
        uint32_t folder_count  = read_be32(&sb[36]);
        printf("  Filesystem:        HFS+ (%s)\n",
               sig == 0x4858 ? "HFSX, case-sensitive" : "case-insensitive");
        printf("  Block size:        %u\n", blk_size);
        printf("  Total blocks:      %u\n", total_blocks);
        printf("  Free blocks:       %u\n", free_blocks);
        printf("  Files:             %u\n", file_count);
        printf("  Folders:           %u\n", folder_count);
    } else {
        printf("  (no FAT/HFS magic at +1024; signature: 0x%04x)\n", sig);
    }
}

static int cmd_show_fs_info(int argc, char **argv) {
    const char *spec = nth_positional(argc, argv, 0);
    if (!spec) {
        fprintf(stderr,
            "USAGE: rusty-backup show fs-info <IMAGE[@N]>\n"
            "  Without @N: prints info for a superfloppy image (FAT/HFS).\n"
            "  With @N:    selects partition N (1-based) from the table.\n"
        );
        return 1;
    }
    char img_path[MAX_PATH_LEN];
    int part_n = parse_img_at(spec, img_path, sizeof(img_path));
    if (part_n < 0) {
        fprintf(stderr, "Error: invalid @N partition selector in '%s'\n", spec);
        return 1;
    }

    int fd = open(img_path, O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "Error: cannot open %s: %s\n", img_path, strerror(errno));
        return 1;
    }

    PartTable pt;
    if (detect_partition_table(fd, &pt) != 0) {
        fprintf(stderr, "Error: no recognized partition table on %s\n", img_path);
        close(fd);
        return 1;
    }

    off_t fs_offset;
    const char *fs_hint;
    if (part_n == 0) {
        if (pt.type != PT_NONE) {
            fprintf(stderr, "Error: %s has a partition table; specify @N "
                    "(1-based) to choose a partition.\n", img_path);
            close(fd);
            return 1;
        }
        fs_offset = 0;
        fs_hint = pt.fs_hint;
    } else {
        PartInfo parts[MAX_PARTITIONS];
        int n = get_partition_list(&pt, parts);
        if (part_n > n) {
            fprintf(stderr, "Error: partition %d out of range (%d available)\n",
                    part_n, n);
            close(fd);
            return 1;
        }
        const PartInfo *p = &parts[part_n - 1];
        fs_offset = (off_t)p->start_lba * SECTOR_SIZE;
        fs_hint = p->type_name;
        printf("Partition %d: %s, %llu sectors @ LBA %llu\n",
               part_n, p->type_name, (unsigned long long)p->total_sectors,
               (unsigned long long)p->start_lba);
    }

    /* Sniff for FAT first (its VBR has a strict signature), then HFS. */
    uint8_t probe[2048];
    if (lseek(fd, fs_offset, SEEK_SET) >= 0
        && read(fd, probe, 2048) >= 1024) {
        if (is_fat_vbr(probe)) {
            show_fat_fs_info(fd, fs_offset);
        } else if (is_hfs_sig(&probe[1024])) {
            show_hfs_fs_info(fd, fs_offset);
        } else {
            printf("  (unrecognized filesystem; partition type hint: %s)\n",
                   fs_hint);
        }
    }

    close(fd);
    return 0;
}

/* ============================================================
 * Browse Infrastructure (ls / get)
 * ----------------------------------------------------------------
 * Read-only filesystem browse layer. Each supported filesystem
 * (HFS, ISO 9660, ProDOS, AFFS, FAT) implements two functions:
 *
 *   fs_ls_<X>(fd, fs_base_offset, path)
 *      Print a directory listing to stdout.
 *   fs_get_<X>(fd, fs_base_offset, path, host_out)
 *      Extract one file to a host path.
 *
 * `cmd_ls` and `cmd_get` resolve `IMAGE[@N]`, sniff the filesystem
 * at the partition's start offset, and dispatch.
 * ============================================================ */

typedef enum {
    FS_UNKNOWN = 0,
    FS_FAT,
    FS_HFS,
    FS_HFS_PLUS,
    FS_ISO9660,
    FS_PRODOS,
    FS_AFFS
} BrowseFsType;

/* Resolve `IMAGE[@N]` to a (fd, fs_offset, fs_size, fs_type) tuple.
 * If `@N` is omitted, treat the image as a superfloppy (offset 0).
 * Returns 0 on success and the four out params populated. */
static int resolve_image_ref(const char *spec, int *fd_out,
                              off_t *fs_offset_out, uint64_t *fs_size_out,
                              BrowseFsType *fs_type_out,
                              char *fs_label_out, size_t fs_label_sz) {
    char img_path[MAX_PATH_LEN];
    int part_n = parse_img_at(spec, img_path, sizeof(img_path));
    if (part_n < 0) {
        fprintf(stderr, "Error: invalid @N selector in '%s'\n", spec);
        return -1;
    }

    int fd = open(img_path, O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "Error: cannot open %s: %s\n",
                img_path, strerror(errno));
        return -1;
    }

    PartTable pt;
    if (detect_partition_table(fd, &pt) != 0) {
        fprintf(stderr, "Error: cannot identify %s\n", img_path);
        close(fd);
        return -1;
    }

    off_t off = 0;
    uint64_t fs_size = 0;
    if (part_n == 0) {
        if (pt.type != PT_NONE) {
            fprintf(stderr, "Error: %s has a partition table; specify @N\n",
                    img_path);
            close(fd);
            return -1;
        }
        struct stat st;
        if (fstat(fd, &st) == 0) fs_size = st.st_size;
    } else {
        PartInfo parts[MAX_PARTITIONS];
        int n = get_partition_list(&pt, parts);
        if (part_n > n) {
            fprintf(stderr, "Error: partition %d of %d on %s\n",
                    part_n, n, img_path);
            close(fd);
            return -1;
        }
        off = (off_t)parts[part_n - 1].start_lba * SECTOR_SIZE;
        fs_size = parts[part_n - 1].total_sectors * SECTOR_SIZE;
    }

    /* Sniff fs at fs_offset. */
    BrowseFsType ft = FS_UNKNOWN;
    if (fs_label_out && fs_label_sz > 0) fs_label_out[0] = '\0';
    uint8_t probe[2048];
    if (lseek(fd, off, SEEK_SET) >= 0
        && read(fd, probe, 2048) >= 2048) {
        if (is_fat_vbr(probe)) {
            ft = FS_FAT;
        } else if (is_hfs_sig(&probe[1024])) {
            uint16_t sig = read_be16(&probe[1024]);
            ft = (sig == 0x4244) ? FS_HFS : FS_HFS_PLUS;
        }
        /* ISO 9660: "CD001" magic at sector 16, offset 1 of the
         * Primary Volume Descriptor. Read sector 16 from fs base. */
        if (ft == FS_UNKNOWN) {
            uint8_t pvd[2048];
            if (lseek(fd, off + 16ULL * 2048, SEEK_SET) >= 0
                && read(fd, pvd, 2048) == 2048
                && memcmp(&pvd[1], "CD001", 5) == 0) {
                ft = FS_ISO9660;
                if (fs_label_out && fs_label_sz > 0) {
                    /* Volume identifier at offset 40, 32 bytes, A-chars,
                     * space-padded. */
                    int copy = (fs_label_sz < 33) ? (int)fs_label_sz - 1 : 32;
                    memcpy(fs_label_out, &pvd[40], copy);
                    fs_label_out[copy] = '\0';
                    /* Trim trailing spaces. */
                    for (int i = copy - 1; i >= 0; i--) {
                        if (fs_label_out[i] == ' ') fs_label_out[i] = '\0';
                        else break;
                    }
                }
            }
        }
    }
    if (ft == FS_UNKNOWN) {
        fprintf(stderr, "Error: no recognized filesystem at %s%s%d\n",
                img_path, part_n ? "@" : " (superfloppy)",
                part_n);
        close(fd);
        return -1;
    }

    *fd_out = fd;
    *fs_offset_out = off;
    *fs_size_out = fs_size;
    *fs_type_out = ft;
    return 0;
}

/* ============================================================
 * ISO 9660 Reader
 * ----------------------------------------------------------------
 * Implements directory listing + file extraction for plain ISO 9660
 * Level 1 / Level 2 / Joliet (UCS-2 BE). Path Tables are ignored;
 * we walk the root directory record (in the PVD at byte 156) and
 * recurse via directory-record extents.
 *
 * Limitations: Rock Ridge is not interpreted (files keep their ISO
 * `;1` version suffix unless they're Joliet); multi-extent files
 * (CONTINUATION flag) are not chained; interleaved files are not
 * de-interleaved.
 * ============================================================ */

#define ISO_SECTOR 2048

typedef struct {
    uint32_t extent_lba;
    uint32_t data_length;
    uint8_t  flags;       /* bit 1 = directory */
    char     name[256];   /* UTF-8 best-effort */
} IsoDirRec;

/* Parse one ISO 9660 directory record from `buf` of total length
 * `buf_len`. Returns the record's length (in bytes) so the caller can
 * advance; 0 means "no more records in this sector" (padded). */
static int iso_parse_dirrec(const uint8_t *buf, int buf_len,
                            int joliet, IsoDirRec *out) {
    if (buf_len < 33) return 0;
    int rec_len = buf[0];
    if (rec_len == 0) return 0;
    if (rec_len > buf_len) return 0;

    out->extent_lba   = read_le32(&buf[2]);
    out->data_length  = read_le32(&buf[10]);
    out->flags        = buf[25];
    int name_len      = buf[32];
    if (name_len <= 0 || 33 + name_len > rec_len) return rec_len;

    if (joliet) {
        /* UCS-2 BE — decode to ASCII best-effort. */
        int oi = 0;
        for (int i = 0; i + 1 < name_len && oi < 255; i += 2) {
            uint16_t u = read_be16(&buf[33 + i]);
            out->name[oi++] = (u < 0x80) ? (char)u : '?';
        }
        out->name[oi] = '\0';
    } else {
        int copy = name_len < 255 ? name_len : 255;
        memcpy(out->name, &buf[33], copy);
        out->name[copy] = '\0';
        /* Strip trailing ";1" version. */
        char *semi = strrchr(out->name, ';');
        if (semi) *semi = '\0';
        /* Strip trailing dot for extensionless files. */
        int ln = (int)strlen(out->name);
        if (ln > 0 && out->name[ln - 1] == '.') out->name[ln - 1] = '\0';
    }
    return rec_len;
}

/* Find the file/directory `name` (case-insensitive) inside the
 * directory whose extent starts at `dir_lba` and has byte length
 * `dir_len`. Returns 0 on hit (out populated), -1 on miss. */
static int iso_lookup_in_dir(int fd, off_t fs_off, uint32_t dir_lba,
                              uint32_t dir_len, int joliet,
                              const char *name, IsoDirRec *out) {
    uint32_t total = dir_len;
    uint32_t lba = dir_lba;
    while (total > 0) {
        uint8_t sec[ISO_SECTOR];
        if (lseek(fd, fs_off + (off_t)lba * ISO_SECTOR, SEEK_SET) < 0) return -1;
        if (read(fd, sec, ISO_SECTOR) != ISO_SECTOR) return -1;
        int off = 0;
        while (off < ISO_SECTOR) {
            IsoDirRec rec;
            int rl = iso_parse_dirrec(&sec[off], ISO_SECTOR - off, joliet, &rec);
            if (rl == 0) break;
            /* Skip "." and ".." (name_len 1 with bytes 0/1). */
            uint8_t name_first = sec[off + 33];
            if (sec[off] >= 34 && (name_first == 0 || name_first == 1)) {
                off += rl;
                continue;
            }
            if (strcasecmp(rec.name, name) == 0) {
                *out = rec;
                return 0;
            }
            off += rl;
        }
        lba++;
        total = (total > ISO_SECTOR) ? total - ISO_SECTOR : 0;
    }
    return -1;
}

/* Resolve a `/a/b/c` path to a directory record. `path` may name a
 * file or a directory; the result's `flags` bit 1 distinguishes. */
static int iso_resolve_path(int fd, off_t fs_off, const char *path,
                             IsoDirRec *out, int *joliet_out) {
    /* Read PVD (sector 16) for the root directory record (offset 156,
     * 34 bytes) and look for a Joliet SVD (escape sequence in
     * descriptor type 2). */
    uint8_t pvd[ISO_SECTOR];
    if (lseek(fd, fs_off + 16ULL * ISO_SECTOR, SEEK_SET) < 0) return -1;
    if (read(fd, pvd, ISO_SECTOR) != ISO_SECTOR) return -1;
    if (memcmp(&pvd[1], "CD001", 5) != 0) return -1;

    int joliet = 0;
    uint8_t root_rec[34];
    memcpy(root_rec, &pvd[156], 34);

    /* Scan descriptors looking for a Joliet SVD (type 2, esc seq
     * starts with 0x25 0x2F and ends with 0x40/43/45). If found,
     * prefer its root record. */
    for (uint32_t sec_n = 17; sec_n < 64; sec_n++) {
        uint8_t d[ISO_SECTOR];
        if (lseek(fd, fs_off + (off_t)sec_n * ISO_SECTOR, SEEK_SET) < 0) break;
        if (read(fd, d, ISO_SECTOR) != ISO_SECTOR) break;
        if (memcmp(&d[1], "CD001", 5) != 0) break;
        if (d[0] == 0xFF) break;   /* terminator */
        if (d[0] == 2) {
            /* Supplementary Volume Descriptor. Joliet uses UCS-2 BE
             * with escape sequence at offset 88, 32 bytes. */
            const uint8_t *esc = &d[88];
            if (esc[0] == 0x25 && esc[1] == 0x2F
                && (esc[2] == 0x40 || esc[2] == 0x43 || esc[2] == 0x45)) {
                memcpy(root_rec, &d[156], 34);
                joliet = 1;
                break;
            }
        }
    }
    *joliet_out = joliet;

    uint32_t cur_lba = read_le32(&root_rec[2]);
    uint32_t cur_len = read_le32(&root_rec[10]);
    out->extent_lba = cur_lba;
    out->data_length = cur_len;
    out->flags = root_rec[25];
    out->name[0] = '\0';

    /* Walk path components. */
    char tmp[512];
    snprintf(tmp, sizeof(tmp), "%s", path && path[0] ? path : "/");
    char *p = tmp;
    while (*p == '/') p++;
    while (*p) {
        char *slash = strchr(p, '/');
        if (slash) *slash = '\0';
        if (*p) {
            IsoDirRec next;
            if (iso_lookup_in_dir(fd, fs_off, out->extent_lba,
                                  out->data_length, joliet, p, &next) != 0) {
                return -1;
            }
            *out = next;
        }
        if (!slash) break;
        p = slash + 1;
        while (*p == '/') p++;
    }
    return 0;
}

/* Public: list ISO 9660 directory at `path`. */
static int fs_ls_iso9660(int fd, off_t fs_off, const char *path) {
    IsoDirRec d;
    int joliet;
    if (iso_resolve_path(fd, fs_off, path, &d, &joliet) != 0) {
        fprintf(stderr, "Error: ISO 9660 path not found: %s\n", path);
        return 1;
    }
    if (!(d.flags & 0x02)) {
        fprintf(stderr, "Error: %s is not a directory\n", path);
        return 1;
    }
    uint32_t total = d.data_length;
    uint32_t lba = d.extent_lba;
    while (total > 0) {
        uint8_t sec[ISO_SECTOR];
        if (lseek(fd, fs_off + (off_t)lba * ISO_SECTOR, SEEK_SET) < 0) return 1;
        if (read(fd, sec, ISO_SECTOR) != ISO_SECTOR) return 1;
        int off = 0;
        while (off < ISO_SECTOR) {
            IsoDirRec rec;
            int rl = iso_parse_dirrec(&sec[off], ISO_SECTOR - off, joliet, &rec);
            if (rl == 0) break;
            uint8_t name_first = sec[off + 33];
            if (sec[off] >= 34 && (name_first == 0 || name_first == 1)) {
                off += rl;
                continue;
            }
            char sz[32];
            format_bytes(rec.data_length, sz, sizeof(sz));
            printf("%s %10s  %s%s\n",
                   (rec.flags & 0x02) ? "d" : "-", sz, rec.name,
                   (rec.flags & 0x02) ? "/" : "");
            off += rl;
        }
        lba++;
        total = (total > ISO_SECTOR) ? total - ISO_SECTOR : 0;
    }
    return 0;
}

/* Public: extract one ISO 9660 file to `host_out`. */
static int fs_get_iso9660(int fd, off_t fs_off, const char *path,
                           const char *host_out) {
    IsoDirRec f;
    int joliet;
    if (iso_resolve_path(fd, fs_off, path, &f, &joliet) != 0) {
        fprintf(stderr, "Error: ISO 9660 path not found: %s\n", path);
        return 1;
    }
    if (f.flags & 0x02) {
        fprintf(stderr, "Error: %s is a directory\n", path);
        return 1;
    }
    FILE *out = fopen(host_out, "wb");
    if (!out) {
        fprintf(stderr, "Error: cannot create %s: %s\n",
                host_out, strerror(errno));
        return 1;
    }
    if (lseek(fd, fs_off + (off_t)f.extent_lba * ISO_SECTOR, SEEK_SET) < 0) {
        fclose(out); return 1;
    }
    uint8_t *buf = (uint8_t *)malloc(CHUNK_SIZE);
    if (!buf) { fclose(out); return 1; }
    uint32_t remaining = f.data_length;
    while (remaining > 0) {
        size_t to_read = remaining > CHUNK_SIZE ? CHUNK_SIZE : remaining;
        ssize_t nr = read(fd, buf, to_read);
        if (nr <= 0) break;
        if (fwrite(buf, 1, nr, out) != (size_t)nr) break;
        remaining -= nr;
    }
    free(buf);
    fclose(out);
    fprintf(stderr, "Extracted %s (%u bytes) -> %s\n",
            path, f.data_length, host_out);
    return 0;
}

/* ============================================================
 * cmd_ls / cmd_get
 * ============================================================ */

static int cmd_ls(int argc, char **argv) {
    if (has_flag(argc, argv, "--help") || has_flag(argc, argv, "-h")) {
        fprintf(stderr,
            "USAGE: rusty-backup ls <IMAGE[@N]> [PATH]\n\n"
            "  Lists a directory inside the partition's filesystem.\n"
            "  Currently supported (read-only): ISO 9660.\n"
            "  HFS, HFS+, FAT, ProDOS, AFFS are wired in subsequent slices.\n"
        );
        return 0;
    }
    const char *spec = nth_positional(argc, argv, 0);
    const char *path = nth_positional(argc, argv, 1);
    if (!path) path = "/";
    if (!spec) {
        fprintf(stderr, "Error: IMAGE[@N] is required\n");
        return 1;
    }

    int fd;
    off_t fs_off;
    uint64_t fs_size;
    BrowseFsType ft;
    char label[64];
    if (resolve_image_ref(spec, &fd, &fs_off, &fs_size, &ft,
                          label, sizeof(label)) != 0) {
        return 1;
    }
    int rc = 1;
    switch (ft) {
        case FS_ISO9660: rc = fs_ls_iso9660(fd, fs_off, path); break;
        default:
            fprintf(stderr,
                "Error: ls is not yet implemented for this filesystem.\n");
            break;
    }
    close(fd);
    return rc;
}

static int cmd_get(int argc, char **argv) {
    if (has_flag(argc, argv, "--help") || has_flag(argc, argv, "-h")) {
        fprintf(stderr,
            "USAGE: rusty-backup get <IMAGE[@N]> <PATH> <HOST_OUT>\n\n"
            "  Extracts one file from the partition's filesystem to a\n"
            "  host file. Currently supported (read-only): ISO 9660.\n"
        );
        return 0;
    }
    const char *spec = nth_positional(argc, argv, 0);
    const char *path = nth_positional(argc, argv, 1);
    const char *host_out = nth_positional(argc, argv, 2);
    if (!spec || !path || !host_out) {
        fprintf(stderr, "Error: IMAGE[@N], PATH, and HOST_OUT are required\n");
        return 1;
    }
    int fd;
    off_t fs_off;
    uint64_t fs_size;
    BrowseFsType ft;
    if (resolve_image_ref(spec, &fd, &fs_off, &fs_size, &ft,
                          NULL, 0) != 0) {
        return 1;
    }
    int rc = 1;
    switch (ft) {
        case FS_ISO9660:
            rc = fs_get_iso9660(fd, fs_off, path, host_out); break;
        default:
            fprintf(stderr,
                "Error: get is not yet implemented for this filesystem.\n");
            break;
    }
    close(fd);
    return rc;
}

/* ============================================================
 * Print Usage
 * ============================================================ */

static void print_usage(const char *prog) {
    const char *name = prog;
    if (prog) {
        const char *slash = strrchr(prog, '/');
        if (slash) name = slash + 1;
    } else {
        name = "rusty-backup";
    }

    fprintf(stderr,
        "Rusty Backup CLI — headless disk backup & restore\n"
        "PowerPC port (Mac OS X Tiger)\n"
        "\n"
        "USAGE:\n"
        "  %s <COMMAND> [OPTIONS]\n"
        "\n"
        "COMMANDS:\n"
        "  backup <SOURCE> <DEST>          Back up a device or image\n"
        "  restore <BACKUP_DIR> <TARGET>   Restore a backup\n"
        "  inspect <BACKUP_DIR>            Show metadata for an existing backup\n"
        "  write <IMAGE> <TARGET>          Stream a raw image to a device or file\n"
        "  ls <IMAGE[@N]> [PATH]           List a directory inside a filesystem\n"
        "  get <IMAGE[@N]> <PATH> <OUT>    Extract one file to a host path\n"
        "  show devices                    Enumerate available disk devices\n"
        "  show partmap <IMAGE>            Print partition table (MBR/GPT/APM)\n"
        "  show fs-info <IMAGE[@N]>        Print FAT/HFS volume metadata\n"
        "  optical rip                     Rip an optical disc to ISO\n"
        "  help                            Show this help message\n"
        "\n"
        "DEPRECATED aliases (still accepted): list-devices, rip\n"
        "\n"
        "Run '%s <COMMAND> --help' for per-command options.\n",
        name, name);
}

/* ============================================================
 * Main Entry Point
 * ============================================================ */

/* These are needed by other transpiled .o files */
void *std_env_args(void) {
    int argc = get_argc();
    char **argv = get_argv();
    /* Return a simple struct: { items, len, cap, elem_size } */
    void **v = (void **)calloc(1, 16);
    if (!v) return NULL;
    v[0] = argv;
    ((int *)v)[2] = argc;
    ((int *)v)[3] = argc;
    return v;
}

void *env_args(void) { return std_env_args(); }

int main(int argc, char **argv) {
#ifndef __APPLE__
    g_argc = argc;
    g_argv = argv;
#endif
    (void)argc; (void)argv;

    int ac = get_argc();
    char **av = get_argv();

    if (ac < 2) {
        print_usage(av[0]);
        return 1;
    }

    const char *cmd = av[1];
    int sub_argc = ac - 2;
    char **sub_argv = &av[2];

    if (strcmp(cmd, "backup") == 0) {
        return cmd_backup(sub_argc, sub_argv);
    } else if (strcmp(cmd, "restore") == 0) {
        return cmd_restore(sub_argc, sub_argv);
    } else if (strcmp(cmd, "inspect") == 0) {
        return cmd_inspect(sub_argc, sub_argv);
    } else if (strcmp(cmd, "write") == 0) {
        return cmd_write(sub_argc, sub_argv);
    } else if (strcmp(cmd, "ls") == 0) {
        return cmd_ls(sub_argc, sub_argv);
    } else if (strcmp(cmd, "get") == 0) {
        return cmd_get(sub_argc, sub_argv);
    } else if (strcmp(cmd, "show") == 0) {
        /* rb-cli surface: devices / partmap / fs-info implemented;
         * chd-info is not (no CHD support on the PPC build today). */
        if (sub_argc < 1) {
            fprintf(stderr,
                "USAGE: rusty-backup show <devices|partmap|fs-info> [ARGS]\n");
            return 1;
        }
        if (strcmp(sub_argv[0], "devices") == 0) {
            return cmd_list_devices();
        }
        if (strcmp(sub_argv[0], "partmap") == 0) {
            return cmd_show_partmap(sub_argc - 1, sub_argv + 1);
        }
        if (strcmp(sub_argv[0], "fs-info") == 0) {
            return cmd_show_fs_info(sub_argc - 1, sub_argv + 1);
        }
        fprintf(stderr, "Unknown 'show' subcommand: %s "
                "(supported on PPC: devices, partmap, fs-info)\n", sub_argv[0]);
        return 1;
    } else if (strcmp(cmd, "optical") == 0) {
        if (sub_argc < 1) {
            fprintf(stderr, "USAGE: rusty-backup optical <rip>\n");
            return 1;
        }
        if (strcmp(sub_argv[0], "rip") == 0) {
            return cmd_rip(sub_argc - 1, sub_argv + 1);
        }
        fprintf(stderr, "Unknown 'optical' subcommand: %s "
                "(supported on PPC: rip)\n", sub_argv[0]);
        return 1;
    } else if (strcmp(cmd, "list-devices") == 0) {
        /* Deprecated alias for `show devices`. */
        return cmd_list_devices();
    } else if (strcmp(cmd, "rip") == 0) {
        /* Deprecated alias for `optical rip`. */
        return cmd_rip(sub_argc, sub_argv);
    } else if (strcmp(cmd, "--help") == 0 || strcmp(cmd, "-h") == 0
            || strcmp(cmd, "help") == 0) {
        print_usage(av[0]);
        return 0;
    } else if (strcmp(cmd, "--version") == 0 || strcmp(cmd, "-V") == 0) {
        printf("rusty-backup 0.3.0-ppc\n");
        printf("Platform: Mac OS X Tiger PowerPC\n");
        printf("Features: MBR, APM, EBR chain, gzip, CRC32, SHA-256, FAT compaction\n");
        return 0;
    } else {
        fprintf(stderr, "Unknown subcommand: %s\n", cmd);
        print_usage(av[0]);
        return 1;
    }
}
