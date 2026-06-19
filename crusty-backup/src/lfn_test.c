/* cb-dos Phase 0b -- long-filename (LFN) write spike.
 *
 * cb-dos must write the desktop tool's native filenames verbatim
 * (metadata.json, partition-N.gz, ...). On DOS that needs the LFN API
 * (int 21h AX=71xxh), which FreeDOS exposes via doslfn and which
 * dosbox-x's integrated DOS provides natively.
 *
 * This spike:
 *   1. queries LFN support + limits         (int 21h AX=71A0h)
 *   2. creates a long-named file            (int 21h AX=716Ch)
 *   3. writes to it / closes it             (int 21h AH=40h / 3Eh)
 *
 * It calls the raw API through DPMI (not DJGPP's auto-LFN open()) so the
 * exact mechanism cb-dos depends on is explicit. Output goes to the
 * screen and C:\LFN.LOG; the created file lands on the mounted drive, so
 * a host check of the folder confirms the long name round-tripped.
 *
 * NOTE: a pass under dosbox-x's integrated DOS proves the API call only.
 * Real MS-DOS needs doslfn loaded; FreeDOS likewise. Validate the
 * deployment environment with a booted-FreeDOS run separately.
 *
 * Build:  make            (adds build/lfn_test.exe)
 * Run:    LFNTEST
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdint.h>
#include <dpmi.h>
#include <go32.h>
#include <sys/movedata.h>

static FILE *g_log;
static void slog(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt); vprintf(fmt, ap); va_end(ap);
    if (g_log) { va_start(ap, fmt); vfprintf(g_log, fmt, ap); va_end(ap); fflush(g_log); }
}

/* Conventional-memory scratch block laid out as:
 *   0x000  long filename (ASCIZ)
 *   0x100  file content
 *   0x200  root path "C:\" (ASCIZ)  -- for 71A0h
 *   0x300  filesystem-name return buffer (32 bytes)
 */
#define OFF_NAME 0x000
#define OFF_DATA 0x100
#define OFF_PATH 0x200
#define OFF_FSNM 0x300
#define BLOCK_BYTES 1024

static int g_seg, g_sel;

static int put_str(unsigned off, const char *s) {     /* copy ASCIZ to low mem */
    int n = strlen(s) + 1;
    dosmemput(s, n, g_seg * 16 + off);
    return n;
}

/* int 21h AX=71A0h -- get volume information (LFN capability + limits). */
static int lfn_volume_info(void) {
    __dpmi_regs r;
    put_str(OFF_PATH, "C:\\");
    memset(&r, 0, sizeof r);
    r.x.ax = 0x71A0;
    r.x.ds = g_seg; r.x.dx = OFF_PATH;     /* root path */
    r.x.es = g_seg; r.x.di = OFF_FSNM;     /* fs-name buffer */
    r.x.cx = 32;                            /* buffer size */
    __dpmi_int(0x21, &r);
    if (r.x.flags & 1) {
        slog("LFN not available (AX=71A0h failed, ax=0x%04X)\n", r.x.ax);
        return 0;
    }
    char fsname[33];
    dosmemget(g_seg * 16 + OFF_FSNM, 32, fsname);
    fsname[32] = 0;
    slog("LFN supported. fs '%s'  flags 0x%04X  max-file %u  max-path %u\n",
         fsname, r.x.bx, r.x.cx, r.x.dx);
    slog("  (flags bit14 = LFN functions: %s)\n",
         (r.x.bx & 0x4000) ? "yes" : "no");
    return 1;
}

/* int 21h AX=716Ch -- extended create/open with a long name.
 * Returns a DOS file handle, or -1 on error. */
static int lfn_create(const char *longname, int *action_out) {
    __dpmi_regs r;
    put_str(OFF_NAME, longname);
    memset(&r, 0, sizeof r);
    r.x.ax = 0x716C;
    r.x.bx = 0x0002;        /* access: read/write */
    r.x.cx = 0x0020;        /* attribute: archive (for newly created) */
    r.x.dx = 0x0012;        /* action: create-if-new (0x10) | truncate-if-exists (0x02) */
    r.x.di = 0;             /* no alias hint */
    r.x.ds = g_seg; r.x.si = OFF_NAME;
    __dpmi_int(0x21, &r);
    if (r.x.flags & 1) {
        slog("716Ch create failed (ax=0x%04X)\n", r.x.ax);
        return -1;
    }
    if (action_out) *action_out = r.x.cx;   /* 1=opened 2=created 3=truncated */
    return r.x.ax;                            /* file handle */
}

/* int 21h AH=40h -- write `len` bytes from low-mem OFF_DATA to handle. */
static int dos_write(int handle, const char *data, int len) {
    __dpmi_regs r;
    dosmemput(data, len, g_seg * 16 + OFF_DATA);
    memset(&r, 0, sizeof r);
    r.h.ah = 0x40;
    r.x.bx = handle;
    r.x.cx = len;
    r.x.ds = g_seg; r.x.dx = OFF_DATA;
    __dpmi_int(0x21, &r);
    if (r.x.flags & 1) { slog("write failed (ax=0x%04X)\n", r.x.ax); return -1; }
    return r.x.ax;          /* bytes actually written */
}

static void dos_close(int handle) {
    __dpmi_regs r;
    memset(&r, 0, sizeof r);
    r.h.ah = 0x3E;
    r.x.bx = handle;
    __dpmi_int(0x21, &r);
}

int main(void) {
    g_log = fopen("LFN.LOG", "w");
    slog("cb-dos LFN write spike (Phase 0b)\n");
    slog("=================================\n");

    int para = (BLOCK_BYTES + 15) >> 4;
    g_seg = __dpmi_allocate_dos_memory(para, &g_sel);
    if (g_seg < 0) {
        slog("FATAL: no DOS memory for scratch block\n");
        if (g_log) fclose(g_log);
        return 1;
    }

    int have_lfn = lfn_volume_info();

    /* The kind of name cb-dos must round-trip: spaces + mixed case + a
     * long stem that would be mangled to 8.3 without LFN. */
    const char *name = "Crusty Backup Long Name partition-1.gz";
    const char *body = "cb-dos LFN round-trip test payload.\r\n";

    int action = 0;
    int h = lfn_create(name, &action);
    if (h >= 0) {
        const char *what = action == 2 ? "created" :
                           action == 3 ? "truncated" :
                           action == 1 ? "opened" : "?";
        slog("716Ch ok: handle %d, action=%s\n", h, what);
        int w = dos_write(h, body, strlen(body));
        slog("wrote %d bytes\n", w);
        dos_close(h);
        slog("created long-named file: \"%s\"\n", name);
    } else {
        slog("could not create the long-named file\n");
    }

    if (!have_lfn)
        slog("WARNING: LFN unsupported here; name was likely mangled to 8.3\n");

    __dpmi_free_dos_memory(g_sel);
    slog("done.\n");
    if (g_log) fclose(g_log);
    return 0;
}
