/*
 * crustybk.c -- crusty-backup (cb-dos) unified front-end.
 *
 * One executable. Run bare, it opens the keyboard-only text UI: a list of the
 * real BIOS hard drives (and their MBR partitions), with a function-key action
 * bar that drives Backup / Restore / Clone. Run with a subcommand
 * (backup|restore|clone|inspect), it dispatches straight to that command for
 * scripting. Both front-ends sit on the shared cbdisk engine + the cmd_* modules.
 *
 * The TUI list is double-buffered (each frame composed in an off-screen cell
 * buffer, pushed in ONE ScreenUpdate blit -- no conio flicker). The operation
 * flows (param entry + the command's own progress output) run on a plain text
 * screen, then return to the menu.
 *
 * Builds for MS-DOS with i586-pc-msdosdjgpp-gcc.
 */

#include <conio.h>
#include <pc.h>
#include <go32.h>
#include <dpmi.h>
#include <sys/farptr.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cbdisk.h"
#include "cbbrowse.h"

/* The scriptable commands, each its own module over the shared cbdisk engine. */
extern int cmd_backup(int argc, char **argv);
extern int cmd_restore(int argc, char **argv);
extern int cmd_clone(int argc, char **argv);
extern int cmd_inspect(int argc, char **argv);
extern int cmd_ls(int argc, char **argv);
extern int cmd_get(int argc, char **argv);

#define CB_VERSION "0.2.0"
#define CB_URL     "github.com/danifunker/rusty-backup"

/* Extended-key scancodes returned after getch() yields 0. */
#define K_UP    72
#define K_DOWN  80
#define K_LEFT  75
#define K_F1    59
#define K_F2    60
#define K_F3    61
#define K_F4    62
#define K_F5    63
#define K_F6    64
#define K_F10   68
#define K_ESC   27
#define K_ENTER 13

#define ATTR(fg, bg) (((bg) << 4) | (fg))
enum { C_BLACK, C_BLUE, C_GREEN, C_CYAN, C_RED, C_MAGENTA, C_BROWN, C_LTGRAY,
       C_DKGRAY, C_LTBLUE, C_LTGREEN, C_LTCYAN, C_LTRED, C_LTMAGENTA,
       C_YELLOW, C_WHITE };

#define A_PAGE   ATTR(C_LTGRAY, C_BLUE)
#define A_TITLE  ATTR(C_BLUE,   C_LTGRAY)
#define A_DISK   ATTR(C_WHITE,  C_BLUE)
#define A_SEL    ATTR(C_BLACK,  C_LTGRAY)
#define A_STATUS ATTR(C_YELLOW, C_BLACK)
#define A_ACTION ATTR(C_BLACK,  C_CYAN)
#define A_ABOUT  ATTR(C_WHITE,  C_RED)
#define A_ABOUT2 ATTR(C_YELLOW, C_RED)

#define G_UP    0x18
#define G_DOWN  0x19
#define G_PTR   0x10
#define G_TL    0xC9
#define G_TR    0xBB
#define G_BL    0xC8
#define G_BR    0xBC
#define G_H     0xCD
#define G_V     0xBA

/* ---- enumerated disks + the flat browse list --------------------------- */

typedef struct {
    int      drive;        /* 0x80.. */
    int      ext, has_mbr;
    unsigned cyls, heads, spt;
    uint64_t sectors;
    uint8_t  mbr[512];
} disk_t;

static disk_t disks[8];
static int    ndisks;

typedef struct {
    int  is_disk;
    int  disk;             /* index into disks[] */
    int  slot;             /* MBR slot (partition rows only) */
    char name[16];
    char detail[64];
} Item;

static Item items[40];
static int  nitems;

static int rows, cols;
static unsigned short backbuf[80 * 50];
static char status_msg[128];

/* Scan BIOS drives 0x80..0x87 and rebuild the disk + item lists. Self-contained
 * around the transfer buffer so it never overlaps a command's own xfer_init. */
static void scan_disks(void)
{
    ndisks = 0;
    if (xfer_init() < 0) { nitems = 0; return; }
    for (int d = 0x80; d <= 0x87 && ndisks < 8; d++) {
        drive_info_t di;
        drive_params(d, &di);
        if (!di.present) continue;
        di.ext = drive_has_ext(d);
        disk_t *dk = &disks[ndisks++];
        memset(dk, 0, sizeof *dk);
        dk->drive = d; dk->ext = di.ext;
        dk->cyls = di.cyls; dk->heads = di.heads; dk->spt = di.spt;
        dk->sectors = drive_total_sectors(&di, d);
        if (read_lba(&di, d, 0, 1, dk->mbr) == 0 &&
            dk->mbr[510] == 0x55 && dk->mbr[511] == 0xAA)
            dk->has_mbr = 1;
    }
    xfer_free();

    nitems = 0;
    for (int i = 0; i < ndisks; i++) {
        disk_t *dk = &disks[i];
        Item *it = &items[nitems++];
        it->is_disk = 1; it->disk = i; it->slot = -1;
        sprintf(it->name, "Disk 0x%02X", dk->drive);
        sprintf(it->detail, "%lu MB   %s", (unsigned long)(dk->sectors / 2048),
                dk->has_mbr ? "MBR" : "no partition table");
        if (!dk->has_mbr) continue;
        for (int s = 0; s < 4 && nitems < 40; s++) {
            const uint8_t *e = dk->mbr + 446 + s * 16;
            if (e[4] == 0 || rd32(e + 12) == 0) continue;
            Item *p = &items[nitems++];
            p->is_disk = 0; p->disk = i; p->slot = s;
            sprintf(p->name, "Part %d", s);
            sprintf(p->detail, "type 0x%02X %-3s  %lu MB%s", e[4],
                    is_fat_part_type(e[4]) ? "FAT" : "---",
                    (unsigned long)(rd32(e + 12) / 2048),
                    (e[0] & 0x80) ? "  [boot]" : "");
        }
    }
}

/* ---- back-buffer primitives -------------------------------------------- */

static void cell(int x, int y, int attr, int ch)
{
    if (x >= 0 && x < cols && y >= 0 && y < rows)
        backbuf[y * cols + x] = (unsigned short)((attr << 8) | (ch & 0xFF));
}
static void put(int x, int y, int attr, const char *s)
{
    for (int i = 0; s[i]; i++) cell(x + i, y, attr, (unsigned char)s[i]);
}
static void put_row(int y, int attr, const char *s)
{
    for (int x = 0; x < cols; x++) cell(x, y, attr, ' ');
    if (s) put(1, y, attr, s);
}
static void put_center(int y, int left, int width, int attr, const char *s)
{
    int len = (int)strlen(s);
    put(left + (width - len) / 2, y, attr, s);
}
static unsigned long bios_ticks(void) { return _farpeekl(_dos_ds, 0x46c); }

/* ---- main list frame --------------------------------------------------- */

static void render(int sel)
{
    char line[96];
    for (int y = 0; y < rows; y++) put_row(y, A_PAGE, NULL);

    put_row(0, A_TITLE, "crusty-backup (cb-dos)");
    put(cols - (int)strlen("v" CB_VERSION) - 1, 0, A_TITLE, "v" CB_VERSION);

    put_row(rows - 2, A_STATUS, status_msg);
    put_row(rows - 1, A_ACTION,
            " \x18\x19  F2 Backup  F3 Restore  F4 Clone  F6 Browse  "
            "F5 Rescan  F1 About  F10 Quit ");

    if (nitems == 0) {
        put(4, 3, A_DISK, "No BIOS hard drives found (need at least a second drive).");
    }
    for (int i = 0; i < nitems; i++) {
        int attr = (i == sel) ? A_SEL : (items[i].is_disk ? A_DISK : A_PAGE);
        int list_x = items[i].is_disk ? 4 : 7;     /* partitions indented */
        snprintf(line, sizeof(line), "%-9s %s", items[i].name, items[i].detail);
        int y = 2 + i;
        if (i == sel) { put_row(y, attr, NULL); cell(1, y, attr, G_PTR); }
        put(list_x, y, attr, line);
    }
    ScreenUpdate(backbuf);
}

/* ---- DOS-red About box ------------------------------------------------- */

static void overlay_about(void)
{
    const int bw = 60, bh = 12;
    int bx = (cols - bw) / 2, by = (rows - bh) / 2, r;

    for (int y = 0; y < bh; y++)
        for (int x = 0; x < bw; x++) cell(bx + x, by + y, A_ABOUT, ' ');
    for (int x = 1; x < bw - 1; x++) {
        cell(bx + x, by, A_ABOUT, G_H);
        cell(bx + x, by + bh - 1, A_ABOUT, G_H);
    }
    for (int y = 1; y < bh - 1; y++) {
        cell(bx, by + y, A_ABOUT, G_V);
        cell(bx + bw - 1, by + y, A_ABOUT, G_V);
    }
    cell(bx, by, A_ABOUT, G_TL);
    cell(bx + bw - 1, by, A_ABOUT, G_TR);
    cell(bx, by + bh - 1, A_ABOUT, G_BL);
    cell(bx + bw - 1, by + bh - 1, A_ABOUT, G_BR);

    r = by + 2;
    put_center(r++, bx, bw, A_ABOUT,  "crusty-backup  (cb-dos)");
    put_center(r++, bx, bw, A_ABOUT2, "Version " CB_VERSION);
    r++;
    put_center(r++, bx, bw, A_ABOUT,  "C-based DOS backup / restore / clone");
    r++;
    put_center(r++, bx, bw, A_ABOUT,  "More info and downloads:");
    put_center(r++, bx, bw, A_ABOUT2, CB_URL);
    put_center(by + bh - 2, bx, bw, A_ABOUT, "Press Space or Enter to dismiss");
}

static void show_about(int auto_secs)
{
    unsigned long start = bios_ticks();
    unsigned long limit = (unsigned long)(auto_secs * 182 + 5) / 10;
    overlay_about();
    ScreenUpdate(backbuf);
    for (;;) {
        if (kbhit()) {
            int k = getch();
            if (k == 0) getch();
            else if (k == ' ' || k == K_ENTER || k == K_ESC) break;
        }
        if (auto_secs > 0 && (bios_ticks() - start) >= limit) break;
        __dpmi_yield();
    }
}

/* ---- operation flows (plain text screen) ------------------------------- */

static void op_begin(const char *title)
{
    textattr(ATTR(C_LTGRAY, C_BLACK));
    clrscr();
    _setcursortype(_NORMALCURSOR);
    printf("=== %s ===\n\n", title);
}
static void op_end(void)
{
    printf("\n-- Press any key to return to the menu --");
    int k = getch(); if (k == 0) getch();
    _setcursortype(_NOCURSOR);
}

/* Read a line into buf (echoed). Returns 1 on Enter, 0 on Esc. */
static int read_line(const char *prompt, char *buf, int cap)
{
    printf("%s", prompt);
    fflush(stdout);
    int n = 0;
    for (;;) {
        int k = getch();
        if (k == 0) { getch(); continue; }
        if (k == K_ENTER) { buf[n] = 0; printf("\n"); return 1; }
        if (k == K_ESC)   { buf[n] = 0; return 0; }
        if (k == '\b') { if (n > 0) { n--; printf("\b \b"); fflush(stdout); } continue; }
        if (k >= 32 && k < 127 && n < cap - 1) { buf[n++] = (char)k; putch((char)k); }
    }
}

/* Single-key size-policy picker. Returns the /SIZE switch, or NULL on Esc.
 * For CUSTOM it also fills custom_sw with "/CUSTOM:<bytes>". */
static const char *pick_size(char *custom_sw, int cap)
{
    printf("Size: [O]riginal  [M]inimum  [E]ntire  [C]ustom   (Esc cancels): ");
    fflush(stdout);
    for (;;) {
        int k = getch();
        if (k == 0) { getch(); continue; }
        if (k == K_ESC) { printf("\n"); return NULL; }
        if (k == 'o' || k == 'O') { printf("Original\n"); return "/SIZE:ORIGINAL"; }
        if (k == 'm' || k == 'M') { printf("Minimum\n");  return "/SIZE:MINIMUM"; }
        if (k == 'e' || k == 'E') { printf("Entire\n");   return "/SIZE:ENTIRE"; }
        if (k == 'c' || k == 'C') {
            char b[24];
            printf("Custom\n");
            if (!read_line("  Size in bytes: ", b, sizeof b) || !b[0]) return NULL;
            snprintf(custom_sw, cap, "/CUSTOM:%s", b);
            return "/SIZE:CUSTOM";
        }
    }
}

static int confirm_erase(int drive)
{
    printf("\nThis ERASES disk 0x%02X.  Type Y to proceed: ", drive);
    fflush(stdout);
    int k = getch(); if (k == 0) getch();
    printf("%c\n", (k >= 32 && k < 127) ? k : ' ');
    return (k == 'y' || k == 'Y');
}

static void do_backup(int drive)
{
    char dest[80], dh[12];
    op_begin("Backup");
    printf("Source: disk 0x%02X\n\n", drive);
    if (!read_line("Destination folder (e.g. D:\\BK\\MYDISK): ", dest, sizeof dest) || !dest[0]) {
        printf("\nCancelled.\n"); op_end(); return;
    }
    mkdir(dest, 0777);                       /* create it if absent (ok if exists) */
    sprintf(dh, "%02X", drive);
    char *av[] = { "backup", dest, dh, NULL };
    printf("\n");
    cmd_backup(3, av);
    op_end();
}

static void do_restore(int drive)
{
    char folder[80], dh[12], custom[32] = "";
    op_begin("Restore");
    printf("Target: disk 0x%02X\n\n", drive);
    if (!read_line("Source backup folder (e.g. D:\\BK\\MYDISK): ", folder, sizeof folder) || !folder[0]) {
        printf("\nCancelled.\n"); op_end(); return;
    }
    const char *sz = pick_size(custom, sizeof custom);
    if (!sz) { printf("\nCancelled.\n"); op_end(); return; }
    if (!confirm_erase(drive)) { printf("\nCancelled.\n"); op_end(); return; }
    sprintf(dh, "%02X", drive);
    char *av[7]; int ac = 0;
    av[ac++] = "restore"; av[ac++] = folder; av[ac++] = dh; av[ac++] = "/Y";
    av[ac++] = (char *)sz;
    if (custom[0]) av[ac++] = custom;
    av[ac] = NULL;
    printf("\n");
    cmd_restore(ac, av);
    op_end();
}

static void do_clone(int src_drive)
{
    char sh[12], th[12], custom[32] = "";
    op_begin("Clone");
    printf("Source: disk 0x%02X\n\nAvailable targets:\n", src_drive);
    int shown = 0;
    for (int i = 0; i < ndisks; i++)
        if (disks[i].drive != src_drive) {
            printf("  %d) disk 0x%02X   %lu MB\n", i, disks[i].drive,
                   (unsigned long)(disks[i].sectors / 2048));
            shown = 1;
        }
    if (!shown) { printf("  (none -- need a second drive)\n"); op_end(); return; }

    char pick[8];
    if (!read_line("\nTarget disk number (Esc cancels): ", pick, sizeof pick) || !pick[0]) {
        printf("\nCancelled.\n"); op_end(); return;
    }
    int ti = atoi(pick);
    if (ti < 0 || ti >= ndisks || disks[ti].drive == src_drive) {
        printf("\nInvalid target.\n"); op_end(); return;
    }
    int tgt = disks[ti].drive;
    const char *sz = pick_size(custom, sizeof custom);
    if (!sz) { printf("\nCancelled.\n"); op_end(); return; }
    if (!confirm_erase(tgt)) { printf("\nCancelled.\n"); op_end(); return; }
    sprintf(sh, "%02X", src_drive);
    sprintf(th, "%02X", tgt);
    char *av[7]; int ac = 0;
    av[ac++] = "clone"; av[ac++] = sh; av[ac++] = th; av[ac++] = "/Y";
    av[ac++] = (char *)sz;
    if (custom[0]) av[ac++] = custom;
    av[ac] = NULL;
    printf("\n");
    cmd_clone(ac, av);
    op_end();
}

/* ---- browse a backup: navigate + mark + extract ------------------------ */

typedef struct { uint32_t cluster; int fixed_root; char name[40]; } bframe_t;

static void build_path(char *out, int cap, bframe_t *stack, int depth)
{
    out[0] = '\\'; out[1] = 0;
    for (int i = 1; i <= depth; i++) {
        if (strcmp(out, "\\") != 0) strncat(out, "\\", cap - (int)strlen(out) - 1);
        strncat(out, stack[i].name, cap - (int)strlen(out) - 1);
    }
}

static void render_browse(const char *pathstr, dirent_t *ents, int nent,
                          const char *marks, int sel, int top, int nmark)
{
    char line[96];
    for (int y = 0; y < rows; y++) put_row(y, A_PAGE, NULL);

    snprintf(line, sizeof line, "Browse  %.80s", pathstr);
    put_row(0, A_TITLE, line);
    put(cols - (int)strlen("v" CB_VERSION) - 1, 0, A_TITLE, "v" CB_VERSION);

    int visible = rows - 4;
    if (nent == 0) put(4, 2, A_DISK, "(empty directory)");
    for (int i = 0; i < visible && top + i < nent; i++) {
        int idx = top + i;
        int attr = (idx == sel) ? A_SEL : A_PAGE;
        char box = marks[idx] ? 'X' : ' ';
        if (ents[idx].attr & CBK_ATTR_DIR)
            snprintf(line, sizeof line, "[%c] %-40.40s  <DIR>", box, ents[idx].name);
        else
            snprintf(line, sizeof line, "[%c] %-40.40s  %lu", box, ents[idx].name,
                     (unsigned long)ents[idx].size);
        int y = 2 + i;
        if (idx == sel) { put_row(y, attr, NULL); cell(1, y, attr, G_PTR); }
        put(4, y, attr, line);
    }
    if (nent > visible) { cell(cols - 2, 2, A_PAGE, G_UP); cell(cols - 2, rows - 3, A_PAGE, G_DOWN); }

    snprintf(line, sizeof line, "%d item%s, %d marked", nent, nent == 1 ? "" : "s", nmark);
    put_row(rows - 2, A_STATUS, line);
    put_row(rows - 1, A_ACTION,
            " \x18\x19 Move  Enter Open  Bksp Up  Space Mark  F2 Extract  Esc Back ");
    ScreenUpdate(backbuf);
}

/* Extract the marked entries to a destination directory the user types. */
static void browse_extract(fatvol_t *v, dirent_t *ents, int nent, char *marks)
{
    int nmark = 0;
    for (int i = 0; i < nent; i++) if (marks[i]) nmark++;
    if (nmark == 0) { strcpy(status_msg, "Nothing marked (Space to mark)."); return; }

    char dest[80];
    op_begin("Extract");
    printf("Extracting %d item%s.\n\n", nmark, nmark == 1 ? "" : "s");
    if (!read_line("Destination folder (e.g. C:\\OUT): ", dest, sizeof dest) || !dest[0]) {
        printf("\nCancelled.\n"); op_end(); return;
    }
    mkdir(dest, 0777);
    printf("\n");
    int ok = 0, fail = 0;
    for (int i = 0; i < nent; i++) {
        if (!marks[i]) continue;
        char child[340];
        snprintf(child, sizeof child, "%.80s\\%.255s", dest, ents[i].name);
        if (ents[i].attr & CBK_ATTR_DIR) {
            printf("dir  %s\\\n", ents[i].name);
            if (cbk_extract_tree(v, ents[i].first_cluster, child) == 0) ok++; else fail++;
        } else {
            printf("file %s\n", ents[i].name);
            if (cbk_extract(v, &ents[i], child) == 0) ok++; else fail++;
        }
    }
    printf("\nDone: %d ok, %d failed.\n", ok, fail);
    op_end();
}

static void do_browse(void)
{
    char folder[80], pn[8];
    op_begin("Browse a backup");
    if (!read_line("Backup folder (e.g. C:\\BK): ", folder, sizeof folder) || !folder[0]) {
        printf("\nCancelled.\n"); op_end(); return;
    }
    pn[0] = 0;
    read_line("Partition number (Enter = first): ", pn, sizeof pn);
    int part = pn[0] ? atoi(pn) : -1;
    part = cbk_default_part(folder, part);

    fatvol_t vol;
    if (cbk_open_vol(folder, part, &vol) != 0) { op_end(); return; }
    _setcursortype(_NOCURSOR);

    static dirent_t ents[256];
    static char marks[256];
    bframe_t stack[20];
    int depth = 0, sel = 0, top = 0;
    stack[0].cluster = vol.root_cluster;
    stack[0].fixed_root = !vol.L.is_fat32;
    strcpy(stack[0].name, "\\");
    int nent = cbk_list_dir(&vol, stack[0].cluster, stack[0].fixed_root, ents, 256);
    if (nent < 0) nent = 0;
    memset(marks, 0, sizeof marks);

    for (;;) {
        if (sel >= nent) sel = nent > 0 ? nent - 1 : 0;
        if (sel < 0) sel = 0;
        int visible = rows - 4;
        if (sel < top) top = sel;
        if (sel >= top + visible) top = sel - visible + 1;

        char pathstr[120];
        build_path(pathstr, sizeof pathstr, stack, depth);
        int nmark = 0;
        for (int i = 0; i < nent; i++) if (marks[i]) nmark++;
        render_browse(pathstr, ents, nent, marks, sel, top, nmark);

        int key = getch();
        int reload = 0;
        if (key == 0) {
            key = getch();
            switch (key) {
            case K_UP:   if (sel > 0) sel--; break;
            case K_DOWN: if (sel < nent - 1) sel++; break;
            case K_LEFT: if (depth > 0) { depth--; reload = 1; } break;
            case K_F2:   browse_extract(&vol, ents, nent, marks);
                         _setcursortype(_NOCURSOR); break;
            default: break;
            }
        } else if (key == K_ENTER) {
            if (nent > 0 && (ents[sel].attr & CBK_ATTR_DIR) && depth < 19) {
                depth++;
                stack[depth].cluster = ents[sel].first_cluster;
                stack[depth].fixed_root = 0;
                strncpy(stack[depth].name, ents[sel].name, sizeof stack[depth].name - 1);
                stack[depth].name[sizeof stack[depth].name - 1] = 0;
                reload = 1;
            }
        } else if (key == ' ') {
            if (nent > 0) marks[sel] = !marks[sel];
        } else if (key == '\b') {
            if (depth > 0) { depth--; reload = 1; }
        } else if (key == K_ESC) {
            break;
        }
        if (reload) {
            nent = cbk_list_dir(&vol, stack[depth].cluster, stack[depth].fixed_root, ents, 256);
            if (nent < 0) nent = 0;
            sel = 0; top = 0;
            memset(marks, 0, sizeof marks);
        }
    }
    cbk_close_vol(&vol);
}

/* ---- text-UI entry ----------------------------------------------------- */

static int tui_main(void)
{
    int sel = 0, key, running = 1;

    cols = ScreenCols();
    rows = ScreenRows();
    if (cols <= 0 || cols > 80) cols = 80;
    if (rows <= 0 || rows > 50) rows = 25;

    _setcursortype(_NOCURSOR);
    scan_disks();
    strcpy(status_msg, "Pick a disk, then F2 Backup / F3 Restore / F4 Clone. F1 About.");

    render(sel);
    show_about(6);

    while (running) {
        if (sel >= nitems) sel = nitems > 0 ? nitems - 1 : 0;
        render(sel);

        key = getch();
        if (key == 0) {
            key = getch();
            int drive = (nitems > 0) ? disks[items[sel].disk].drive : -1;
            switch (key) {
            case K_UP:   if (sel > 0)          sel--; break;
            case K_DOWN: if (sel < nitems - 1) sel++; break;
            case K_F1:   show_about(0); break;
            case K_F2:
                if (drive < 0) break;
                do_backup(drive);
                scan_disks();
                snprintf(status_msg, sizeof status_msg, "Returned from Backup.");
                break;
            case K_F3:
                if (drive < 0) break;
                do_restore(drive);
                scan_disks();
                snprintf(status_msg, sizeof status_msg, "Returned from Restore.");
                break;
            case K_F4:
                if (drive < 0) break;
                do_clone(drive);
                scan_disks();
                snprintf(status_msg, sizeof status_msg, "Returned from Clone.");
                break;
            case K_F6:
                do_browse();
                strcpy(status_msg, "Returned from Browse.");
                break;
            case K_F5:
                scan_disks();
                strcpy(status_msg, "Rescanned drives.");
                break;
            case K_F10:  running = 0; break;
            default: break;
            }
        } else if (key == K_ESC) {
            running = 0;
        }
    }

    textattr(ATTR(C_LTGRAY, C_BLACK));
    clrscr();
    _setcursortype(_NORMALCURSOR);
    cputs("cb-dos exited.\r\n");
    return 0;
}

/* ---- dispatcher: one exe, TUI by default, subcommands for scripting ----- */

static void usage(void)
{
    printf("crusty-backup (cb-dos) v" CB_VERSION "\n");
    printf("usage: CRUSTYBK [command] [args]\n");
    printf("  (no command)   launch the text UI\n");
    printf("  backup  <dest-dir> [drive-hex] [/PARTS:i,j]\n");
    printf("  restore <folder> <drive-hex> /Y [/SIZE:mode] [/CUSTOM:bytes] [/PARTS:i,j]\n");
    printf("  clone   <src-hex> <tgt-hex> /Y [/SIZE:mode] [/CUSTOM:bytes] [/PARTS:i,j]\n");
    printf("  inspect [drive-hex]   list BIOS hard drives + partitions\n");
    printf("  ls      <folder> [N] [path]            list files in a backup\n");
    printf("  get     <folder> [N] <path> <dest>     extract one file from a backup\n");
    printf("  /SIZE modes: ORIGINAL (default), MINIMUM, ENTIRE, CUSTOM\n");
}

int main(int argc, char **argv)
{
    if (argc >= 2) {
        const char *cmd = argv[1];
        if (eq_ci(cmd, "backup"))  return cmd_backup(argc - 1, argv + 1);
        if (eq_ci(cmd, "restore")) return cmd_restore(argc - 1, argv + 1);
        if (eq_ci(cmd, "clone"))   return cmd_clone(argc - 1, argv + 1);
        if (eq_ci(cmd, "inspect")) return cmd_inspect(argc - 1, argv + 1);
        if (eq_ci(cmd, "ls"))      return cmd_ls(argc - 1, argv + 1);
        if (eq_ci(cmd, "get"))     return cmd_get(argc - 1, argv + 1);
        if (eq_ci(cmd, "help") || eq_ci(cmd, "/?") ||
            eq_ci(cmd, "-h")   || eq_ci(cmd, "--help")) { usage(); return 0; }
        printf("unknown command: %s\n\n", cmd);
        usage();
        return 2;
    }
    return tui_main();
}
