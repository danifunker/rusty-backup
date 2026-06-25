/*
 * crusty-backup (cb-dos) -- text-UI proof of concept.
 *
 * Validates the lightweight, keyboard-only TUI: a multi-select disk/partition
 * list with a function-key action bar pinned to the bottom, plus a DOS-red
 * "About" box. No mouse, no framework. See docs/cb_dos.md, Phase 0a.
 *
 * Model:
 *   Up/Down  move the cursor
 *   Space    mark / unmark (toggle) the current item
 *   Enter    execute on the marked items
 *   F1       About box (version + project URL); auto-shown at startup
 *   F2/F3    Backup / Restore (stubs)
 *   F10/Esc  quit
 *
 * Rendering is double-buffered: each frame is composed in an off-screen cell
 * buffer and pushed to text-mode video memory in ONE ScreenUpdate() blit, so
 * there is no cell-by-cell conio flicker. Input is blocking getch(), so we only
 * redraw on a keypress.
 *
 * Builds for MS-DOS with i586-pc-msdosdjgpp-gcc.
 */

#include <conio.h>
#include <pc.h>
#include <go32.h>
#include <dpmi.h>
#include <sys/farptr.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cbdisk.h"   /* eq_ci for the subcommand dispatch */

/* The scriptable commands, each its own module over the shared cbdisk engine. */
extern int cmd_backup(int argc, char **argv);
extern int cmd_restore(int argc, char **argv);
extern int cmd_clone(int argc, char **argv);
extern int cmd_inspect(int argc, char **argv);

#define CB_VERSION "0.1.0-poc"
#define CB_URL     "github.com/danifunker/rusty-backup"

/* Extended-key scancodes returned after getch() yields 0. */
#define K_UP    72
#define K_DOWN  80
#define K_LEFT  75
#define K_RIGHT 77
#define K_F1    59
#define K_F2    60
#define K_F3    61
#define K_F10   68
#define K_ESC   27
#define K_ENTER 13

/* VGA text attribute byte: (background << 4) | foreground. */
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
#define A_ABOUT  ATTR(C_WHITE,  C_RED)    /* classic DOS red box */
#define A_ABOUT2 ATTR(C_YELLOW, C_RED)

/* CP437 glyphs. */
#define G_UP    0x18
#define G_DOWN  0x19
#define G_PTR   0x10    /* right-pointing selector */
#define G_TL    0xC9    /* box corners / edges */
#define G_TR    0xBB
#define G_BL    0xC8
#define G_BR    0xBC
#define G_H     0xCD
#define G_V     0xBA

typedef struct {
    const char *name;
    const char *detail;
    int         is_disk;
} Item;

static Item items[] = {
    { "Disk 0", "IDE  512 MB CompactFlash",      1 },
    { "Part 1", "FAT16   bootable      200 MB",  0 },
    { "Part 2", "FAT32                 300 MB",  0 },
    { "Disk 1", "IDE  2 GB CompactFlash",        1 },
    { "Part 1", "NTFS                  2.0 GB",  0 },
};
#define NITEMS ((int)(sizeof(items) / sizeof(items[0])))

static int  rows, cols;
static int  marked[NITEMS];
static int  parent[NITEMS];   /* index of the disk each row belongs to */
static unsigned short backbuf[80 * 50];
static char status_msg[128];

/* ---- back-buffer primitives -------------------------------------------- */

static void cell(int x, int y, int attr, int ch)
{
    if (x >= 0 && x < cols && y >= 0 && y < rows)
        backbuf[y * cols + x] = (unsigned short)((attr << 8) | (ch & 0xFF));
}

static void put(int x, int y, int attr, const char *s)
{
    int i;
    for (i = 0; s[i]; i++)
        cell(x + i, y, attr, (unsigned char)s[i]);
}

static void put_row(int y, int attr, const char *s)
{
    int x;
    for (x = 0; x < cols; x++)
        cell(x, y, attr, ' ');
    if (s)
        put(1, y, attr, s);
}

static void put_center(int y, int left, int width, int attr, const char *s)
{
    int len = (int)strlen(s);
    int x = left + (width - len) / 2;
    put(x, y, attr, s);
}

/* ---- BIOS tick timer (0040:006C, ~18.2065 Hz) -------------------------- */

static unsigned long bios_ticks(void)
{
    return _farpeekl(_dos_ds, 0x46c);
}

/* ---- main list frame --------------------------------------------------- */

static void render(int sel)
{
    int i, y;
    char line[96];

    for (y = 0; y < rows; y++)
        put_row(y, A_PAGE, NULL);

    put_row(0, A_TITLE, "crusty-backup (cb-dos)  -  text-UI POC");
    put(cols - (int)strlen("v" CB_VERSION) - 1, 0, A_TITLE, "v" CB_VERSION);

    put_row(rows - 2, A_STATUS, status_msg);
    put_row(rows - 1, A_ACTION,
            " \x18\x19 Move  Space Mark  Enter Run  F1 About  "
            "F2 Backup  F3 Restore  F10 Quit ");

    for (i = 0; i < NITEMS; i++) {
        int attr = (i == sel) ? A_SEL : (items[i].is_disk ? A_DISK : A_PAGE);
        int list_x = items[i].is_disk ? 4 : 6;   /* partition boxes indented +2 */
        char box;
        /* A marked disk covers its partitions: they show '-' (included). */
        if (items[i].is_disk)
            box = marked[i] ? 'X' : ' ';
        else
            box = marked[parent[i]] ? '-' : (marked[i] ? 'X' : ' ');
        snprintf(line, sizeof(line), "[%c] %-8s %s",
                 box, items[i].name, items[i].detail);
        y = 2 + i;
        if (i == sel) {
            put_row(y, attr, NULL);
            cell(1, y, attr, G_PTR);     /* cursor, with a small left margin */
        }
        put(list_x, y, attr, line);      /* disk boxes at col 4, partitions +2 */
    }

    if (NITEMS > rows - 3) {             /* scroll hints when list overflows */
        cell(cols - 2, 2, A_PAGE, G_UP);
        cell(cols - 2, rows - 3, A_PAGE, G_DOWN);
    }

    ScreenUpdate(backbuf);
}

/* ---- DOS-red About box ------------------------------------------------- */

static void overlay_about(void)
{
    const int bw = 60, bh = 12;
    int bx = (cols - bw) / 2;
    int by = (rows - bh) / 2;
    int x, y, r;

    for (y = 0; y < bh; y++)            /* fill */
        for (x = 0; x < bw; x++)
            cell(bx + x, by + y, A_ABOUT, ' ');

    for (x = 1; x < bw - 1; x++) {      /* horizontal edges */
        cell(bx + x, by, A_ABOUT, G_H);
        cell(bx + x, by + bh - 1, A_ABOUT, G_H);
    }
    for (y = 1; y < bh - 1; y++) {      /* vertical edges */
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
    put_center(r++, bx, bw, A_ABOUT,  "C-based DOS backup / restore for rusty-backup");
    r++;
    put_center(r++, bx, bw, A_ABOUT,  "More info and downloads:");
    put_center(r++, bx, bw, A_ABOUT2, CB_URL);
    r++;
    put_center(by + bh - 2, bx, bw, A_ABOUT, "Press Space or Enter to dismiss");
}

/* Show the About box. auto_secs > 0 also dismisses after that many seconds. */
static void show_about(int auto_secs)
{
    unsigned long start = bios_ticks();
    unsigned long limit = (unsigned long)(auto_secs * 182 + 5) / 10; /* ticks */

    overlay_about();
    ScreenUpdate(backbuf);

    for (;;) {
        if (kbhit()) {
            int k = getch();
            if (k == 0)
                getch();                 /* swallow extended scancode */
            else if (k == ' ' || k == K_ENTER || k == K_ESC)
                break;
        }
        if (auto_secs > 0 && (bios_ticks() - start) >= limit)
            break;
        __dpmi_yield();                  /* be nice to the host */
    }
}

/* ---- mock Backup / Restore screens ------------------------------------- */

/* Draw a progress bar of `w` cells at (x,y), filled to `pct` percent. */
static void draw_bar(int x, int y, int w, int pct)
{
    int fill = w * pct / 100, i;
    char tail[8];
    for (i = 0; i < w; i++)
        cell(x + i, y,
             (i < fill) ? ATTR(C_LTGREEN, C_BLUE) : ATTR(C_DKGRAY, C_BLUE),
             (i < fill) ? 0xDB : 0xB0);       /* full block / light shade */
    snprintf(tail, sizeof(tail), " %3d%%", pct);
    put(x + w, y, A_DISK, tail);
}

/* Animate a mock progress bar with elapsed/ETA and a MB counter on the line
 * below it. `total_mb` is the (mock) amount of data this operation moves. */
static void mock_progress(int x, int y, int w, int total_mb)
{
    unsigned long start = bios_ticks();
    const unsigned long dur = 73;            /* ~4 s at 18.2 Hz */
    char info[80];

    for (;;) {
        unsigned long el  = bios_ticks() - start;
        unsigned long rem = (el < dur) ? (dur - el) : 0;
        int pct  = (el >= dur) ? 100 : (int)(el * 100UL / dur);
        int el_s = (int)(el / 18), eta_s = (int)(rem / 18);
        int cur  = total_mb * pct / 100;

        draw_bar(x, y, w, pct);
        snprintf(info, sizeof(info),
                 "Elapsed %d:%02d   ETA %d:%02d   %d / %d MB",
                 el_s / 60, el_s % 60, eta_s / 60, eta_s % 60, cur, total_mb);
        put_row(y + 1, A_PAGE, NULL);
        put(x, y + 1, A_PAGE, info);
        ScreenUpdate(backbuf);

        if (pct >= 100)
            break;
        __dpmi_yield();
    }
}

/* Gather the effective selection (marked disks + standalone marked parts). */
static int collect_selection(char out[][40], int max)
{
    int i, n = 0;
    for (i = 0; i < NITEMS && n < max; i++) {
        if (items[i].is_disk) {
            if (marked[i])
                snprintf(out[n++], 40, "%s  (whole disk)", items[i].name);
        } else if (!marked[parent[i]] && marked[i]) {
            snprintf(out[n++], 40, "%s on %s", items[i].name, items[parent[i]].name);
        }
    }
    return n;
}

/* Modal Backup (is_restore=0) / Restore (is_restore=1) screen.
 * Returns 1 if the user asked to quit the whole app (F10), else 0 (Esc/back). */
static int op_screen(int is_restore)
{
    /* Backup modes (opt) and the mock size figures each implies. */
    static const char *modes[] = { "Smart compact (used data only)",
                                   "Sector-by-sector (every sector)" };
    static const char *used_s[] = { "312 MB", "2.0 GB" };
    static const char *est_s[]  = { "~140 MB", "~900 MB" };
    static const char *need_s[] = { "~150 MB", "~950 MB" };
    /* Restore sizing options (opt). */
    static const char *sizing[] = { "Minimum  (shrink each partition to used)",
                                    "Original (same size as source)",
                                    "Custom   (set per-partition)" };

    const char *op = is_restore ? "Restore" : "Backup";
    const int bar_x = 14, bar_y = 18, bar_w = 40;
    const int nopt = is_restore ? 3 : 2;
    char sel[8][40];
    int nsel = collect_selection(sel, 8);
    int done = 0, opt = 0, total_mb, i, y, key;

    for (;;) {
        for (y = 0; y < rows; y++)
            put_row(y, A_PAGE, NULL);
        put_row(0, A_TITLE, "crusty-backup (cb-dos)  -  text-UI POC");
        put(cols - (int)strlen("v" CB_VERSION) - 1, 0, A_TITLE, "v" CB_VERSION);

        put(2, 2, A_DISK, "Operation:");
        put(14, 2, A_PAGE, op);

        if (is_restore) {
            put(2, 4, A_DISK, "Backup source:");
            put(4, 5, A_PAGE, "D:\\BACKUPS\\MYDISK   (2 partitions, gzip)");
            put(2, 7, A_DISK, "Restore to:");
            put(14, 7, A_PAGE, "Disk 1   (IDE  2 GB CompactFlash)");
            put(2, 9, A_DISK, "Sizing:");
            put(14, 9, A_PAGE, sizing[opt]);
            put(58, 9, A_STATUS, "<- ->");
            put(2, 11, A_DISK, "Decompress:");
            put(14, 11, A_PAGE, "gzip, streamed on the fly");
            total_mb = 312;
        } else {
            put(2, 4, A_DISK, "Source (marked items):");
            if (nsel == 0) {
                put(4, 5, A_PAGE, "(nothing marked -- press Esc and mark items)");
            } else {
                for (i = 0; i < nsel; i++)
                    put(4, 5 + i, A_PAGE, sel[i]);
            }
            put(2, 11, A_DISK, "Mode:");
            put(14, 11, A_PAGE, modes[opt]);
            put(58, 11, A_STATUS, "<- ->");
            put(2, 12, A_DISK, "Destination:");
            put(14, 12, A_PAGE, "D:\\BACKUPS\\MYDISK");
            put(2, 13, A_DISK, "Compression:");
            put(14, 13, A_PAGE, "gzip (DEFLATE), streamed on the fly");
            put(2, 14, A_DISK, "Used data:");
            put(14, 14, A_PAGE, used_s[opt]);
            put(30, 14, A_DISK, "Est. backup:");
            put(44, 14, A_PAGE, est_s[opt]);
            put(2, 15, A_DISK, "Space needed:");
            put(14, 15, A_PAGE, need_s[opt]);
            put(24, 15, A_PAGE, "free at destination (compressed)");
            total_mb = opt ? 2048 : 312;
        }

        put(2, bar_y, A_DISK, "Progress:");
        draw_bar(bar_x, bar_y, bar_w, done ? 100 : 0);

        if (done)
            put_row(rows - 2, A_STATUS,
                    is_restore ? "Restore complete (mock).  Esc to go back."
                               : "Backup complete (mock).  Esc to go back.");
        else
            put_row(rows - 2, A_STATUS,
                    "Enter to start (mock), <- -> change option, Esc to go back.");

        put_row(rows - 1, A_ACTION, " Enter Start   <- -> Option   Esc Back   F10 Quit ");
        ScreenUpdate(backbuf);

        key = getch();
        if (key == 0) {
            key = getch();
            switch (key) {
            case K_F10:  return 1;
            case K_LEFT:  if (opt > 0)        { opt--; done = 0; } break;
            case K_RIGHT: if (opt < nopt - 1) { opt++; done = 0; } break;
            default: break;
            }
        } else if (key == K_ENTER) {
            mock_progress(bar_x, bar_y, bar_w, total_mb);
            done = 1;
        } else if (key == K_ESC) {
            return 0;
        }
    }
}

/* ---- text-UI entry ----------------------------------------------------- */

static int tui_main(void)
{
    int sel = 0, key, running = 1, i, n;

    cols = ScreenCols();
    rows = ScreenRows();
    if (cols <= 0 || cols > 80) cols = 80;
    if (rows <= 0 || rows > 50) rows = 25;

    _setcursortype(_NOCURSOR);
    strcpy(status_msg, "Space marks items, Enter runs on marked, F1 for About.");

    /* Map each partition to the disk above it (a disk maps to itself). */
    for (i = 0, n = 0; i < NITEMS; i++) {
        if (items[i].is_disk) n = i;
        parent[i] = n;
    }

    render(sel);
    show_about(8);                       /* auto-shown, 8s or key to dismiss */

    while (running) {
        render(sel);

        key = getch();
        if (key == 0) {                  /* extended key */
            key = getch();
            switch (key) {
            case K_UP:   if (sel > 0)          sel--; break;
            case K_DOWN: if (sel < NITEMS - 1) sel++; break;
            case K_F1:   show_about(0); break;
            case K_F2:
                if (op_screen(0)) running = 0;
                else strcpy(status_msg, "Returned from Backup screen.");
                break;
            case K_F3:
                if (op_screen(1)) running = 0;
                else strcpy(status_msg, "Returned from Restore screen.");
                break;
            case K_F10:  running = 0; break;
            default: break;
            }
        } else {
            switch (key) {
            case ' ':    /* toggle mark */
                if (!items[sel].is_disk && marked[parent[sel]]) {
                    snprintf(status_msg, sizeof(status_msg),
                             "%s is included via %s -- unmark the disk to pick partitions.",
                             items[sel].name, items[parent[sel]].name);
                } else {
                    marked[sel] = !marked[sel];
                    snprintf(status_msg, sizeof(status_msg), "%s: %s",
                             marked[sel] ? "Marked" : "Unmarked", items[sel].name);
                }
                break;
            case K_ENTER:  /* execute on the effective selection */
                for (i = 0, n = 0; i < NITEMS; i++) {
                    if (items[i].is_disk) {
                        if (marked[i]) n++;                 /* whole disk */
                    } else if (!marked[parent[i]] && marked[i]) {
                        n++;                                /* standalone partition */
                    }
                }
                if (n == 0)
                    strcpy(status_msg, "Nothing marked. Space to mark items, then Enter to run.");
                else
                    snprintf(status_msg, sizeof(status_msg),
                             "Execute: would process %d marked item(s) (stub).", n);
                break;
            case K_ESC:  running = 0; break;
            default: break;
            }
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
        if (eq_ci(cmd, "help") || eq_ci(cmd, "/?") ||
            eq_ci(cmd, "-h")   || eq_ci(cmd, "--help")) { usage(); return 0; }
        printf("unknown command: %s\n\n", cmd);
        usage();
        return 2;
    }
    return tui_main();
}
