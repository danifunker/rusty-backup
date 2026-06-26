/* cmd_inspect.c -- the `inspect` command: list BIOS hard drives + partitions.
 *
 * Scans drives 0x80..0x87 (AH=08h/41h/48h), and for each present disk dumps its
 * MBR primary partitions. Read-only; handy for finding the source/target drive
 * numbers to pass to backup / restore / clone. */

#include "cbdisk.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

int cmd_inspect(int argc, char **argv) {
    setvbuf(stdout, NULL, _IOLBF, 0);
    int only = -1;
    if (argc >= 2) only = (int)strtol(argv[1], NULL, 16);

    if (xfer_init() < 0) { printf("DOS memory alloc failed\n"); return 1; }

    int found = 0;
    for (int d = 0x80; d <= 0x87; d++) {
        if (only >= 0 && d != only) continue;
        drive_info_t di;
        drive_params(d, &di);
        if (!di.present) continue;
        di.ext = drive_has_ext(d);
        uint64_t secs = drive_total_sectors(&di, d);
        printf("drive 0x%02X: %u cyl %u head %u spt, ext=%s, %lu sectors (%lu MB)\n",
               d, di.cyls, di.heads, di.spt, di.ext ? "yes" : "no",
               (unsigned long)secs, (unsigned long)(secs / 2048));
        found = 1;

        uint8_t mbr[512];
        if (read_lba(&di, d, 0, 1, mbr) != 0) { printf("  (MBR read failed)\n"); continue; }
        if (mbr[510] != 0x55 || mbr[511] != 0xAA) { printf("  (no MBR signature)\n"); continue; }
        int any = 0;
        for (int i = 0; i < 4; i++) {
            const uint8_t *e = mbr + 446 + i * 16;
            if (e[4] == 0 || rd32(e + 12) == 0) continue;
            uint32_t lba = rd32(e + 8), cnt = rd32(e + 12);
            printf("  slot %d: type 0x%02X %-7s lba %lu  %lu MB%s\n",
                   i, e[4], is_fat_part_type(e[4]) ? "FAT" : "non-FAT",
                   (unsigned long)lba, (unsigned long)(cnt / 2048),
                   (e[0] & 0x80) ? "  [boot]" : "");
            any = 1;
        }
        if (!any) printf("  (no partitions)\n");
    }
    if (!found) printf("no BIOS hard drives found\n");
    xfer_free();
    return found ? 0 : 1;
}
