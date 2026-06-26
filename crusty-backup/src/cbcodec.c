/* cbcodec.c -- gzip (zlib) + LZ4-frame (liblz4) compressed streams (see
 * cbcodec.h). The gzip path is a thin wrapper over zlib's gzFile; the LZ4 path
 * drives the LZ4F streaming frame API with a fixed-size bounce buffer (so a
 * partition of any size streams without holding it in RAM). */

#include "cbcodec.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <zlib.h>
#include <lz4frame.h>

#include "cbdisk.h"   /* eq_ci */

/* Feed LZ4F_compressUpdate at most this many input bytes per call; the output
 * bounce buffer is sized to LZ4F_compressBound() of this. 64 KB matches the
 * default LZ4 frame block. */
#define LZ4_IN_CHUNK 65536u
/* Compressed-input read granularity for the decoder. */
#define LZ4_INBUF    65536u

int codec_from_name(const char *name) {
    if (!name) return -1;
    if (eq_ci(name, "gzip")) return CODEC_GZIP;
    if (eq_ci(name, "lz4"))  return CODEC_LZ4;
    return -1;
}
int codec_for_file(const char *name) {
    if (!name) return -1;
    if (strstr(name, ".lz4")) return CODEC_LZ4;   /* check .lz4 before .gz */
    if (strstr(name, ".gz"))  return CODEC_GZIP;
    return -1;
}
const char *codec_name(int codec) { return codec == CODEC_LZ4 ? "lz4" : "gzip"; }
const char *codec_ext(int codec)  { return codec == CODEC_LZ4 ? "lz4" : "gz"; }

/* ----- writer ------------------------------------------------------------- */

struct cbw {
    int        codec;
    gzFile     gz;          /* CODEC_GZIP */
    FILE      *fp;          /* CODEC_LZ4 */
    LZ4F_cctx *cctx;        /* CODEC_LZ4 */
    uint8_t   *out;         /* CODEC_LZ4 bounce buffer */
    size_t     outcap;
};

cbw_t *cbw_open(const char *path, int codec) {
    cbw_t *w = calloc(1, sizeof *w);
    if (!w) return NULL;
    w->codec = codec;
    if (codec == CODEC_GZIP) {
        w->gz = gzopen(path, "wb6");
        if (!w->gz) { free(w); return NULL; }
        return w;
    }
    w->fp = fopen(path, "wb");
    if (!w->fp) { free(w); return NULL; }
    if (LZ4F_isError(LZ4F_createCompressionContext(&w->cctx, LZ4F_getVersion())))
        { fclose(w->fp); free(w); return NULL; }
    LZ4F_preferences_t prefs;
    memset(&prefs, 0, sizeof prefs);     /* defaults: fast level, 64 KB blocks */
    w->outcap = LZ4F_compressBound(LZ4_IN_CHUNK, &prefs);
    if (w->outcap < LZ4F_HEADER_SIZE_MAX) w->outcap = LZ4F_HEADER_SIZE_MAX;
    w->out = malloc(w->outcap);
    if (!w->out) { LZ4F_freeCompressionContext(w->cctx); fclose(w->fp); free(w); return NULL; }
    size_t h = LZ4F_compressBegin(w->cctx, w->out, w->outcap, &prefs);
    if (LZ4F_isError(h) || fwrite(w->out, 1, h, w->fp) != h) {
        LZ4F_freeCompressionContext(w->cctx); free(w->out); fclose(w->fp); free(w);
        return NULL;
    }
    return w;
}

int cbw_write(cbw_t *w, const void *buf, int len) {
    if (w->codec == CODEC_GZIP)
        return (gzwrite(w->gz, buf, len) == len) ? 0 : -1;
    const uint8_t *p = (const uint8_t *)buf;
    while (len > 0) {
        size_t chunk = (size_t)len > LZ4_IN_CHUNK ? LZ4_IN_CHUNK : (size_t)len;
        size_t n = LZ4F_compressUpdate(w->cctx, w->out, w->outcap, p, chunk, NULL);
        if (LZ4F_isError(n)) return -1;
        if (n && fwrite(w->out, 1, n, w->fp) != n) return -1;
        p += chunk; len -= (int)chunk;
    }
    return 0;
}

int cbw_close(cbw_t *w) {
    int rc = 0;
    if (w->codec == CODEC_GZIP) {
        if (gzclose(w->gz) != Z_OK) rc = -1;
    } else {
        size_t n = LZ4F_compressEnd(w->cctx, w->out, w->outcap, NULL);
        if (LZ4F_isError(n) || fwrite(w->out, 1, n, w->fp) != n) rc = -1;
        LZ4F_freeCompressionContext(w->cctx);
        if (fclose(w->fp) != 0) rc = -1;
        free(w->out);
    }
    free(w);
    return rc;
}

/* ----- reader ------------------------------------------------------------- */

struct cbr {
    int        codec;
    gzFile     gz;          /* CODEC_GZIP */
    FILE      *fp;          /* CODEC_LZ4 */
    LZ4F_dctx *dctx;        /* CODEC_LZ4 */
    uint8_t   *in;          /* CODEC_LZ4 compressed-input buffer */
    size_t     incap, inlen, inpos;
    int        eof_in;      /* read all compressed bytes from fp */
    int        frame_done;  /* LZ4F_decompress signalled end of frame */
};

cbr_t *cbr_open(const char *path, int codec) {
    cbr_t *r = calloc(1, sizeof *r);
    if (!r) return NULL;
    r->codec = codec;
    if (codec == CODEC_GZIP) {
        r->gz = gzopen(path, "rb");
        if (!r->gz) { free(r); return NULL; }
        return r;
    }
    r->fp = fopen(path, "rb");
    if (!r->fp) { free(r); return NULL; }
    if (LZ4F_isError(LZ4F_createDecompressionContext(&r->dctx, LZ4F_getVersion())))
        { fclose(r->fp); free(r); return NULL; }
    r->incap = LZ4_INBUF;
    r->in = malloc(r->incap);
    if (!r->in) { LZ4F_freeDecompressionContext(r->dctx); fclose(r->fp); free(r); return NULL; }
    return r;
}

int cbr_read(cbr_t *r, void *buf, int len) {
    if (r->codec == CODEC_GZIP)
        return gzread(r->gz, buf, len);
    uint8_t *out = (uint8_t *)buf;
    size_t produced = 0;
    while (produced < (size_t)len && !r->frame_done) {
        if (r->inpos >= r->inlen) {                 /* refill compressed input */
            if (r->eof_in) break;
            size_t got = fread(r->in, 1, r->incap, r->fp);
            r->inlen = got; r->inpos = 0;
            if (got == 0) { r->eof_in = 1; break; }
        }
        size_t dstSize = (size_t)len - produced;
        size_t srcSize = r->inlen - r->inpos;
        size_t hint = LZ4F_decompress(r->dctx, out + produced, &dstSize,
                                      r->in + r->inpos, &srcSize, NULL);
        if (LZ4F_isError(hint)) return -1;
        produced += dstSize;
        r->inpos += srcSize;
        if (hint == 0) r->frame_done = 1;
        if (dstSize == 0 && srcSize == 0) break;    /* no progress -> stop */
    }
    return (int)produced;
}

void cbr_close(cbr_t *r) {
    if (!r) return;
    if (r->codec == CODEC_GZIP) {
        if (r->gz) gzclose(r->gz);
    } else {
        if (r->dctx) LZ4F_freeDecompressionContext(r->dctx);
        if (r->fp) fclose(r->fp);
        free(r->in);
    }
    free(r);
}
