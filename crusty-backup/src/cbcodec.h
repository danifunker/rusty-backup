/* cbcodec.h -- a tiny compressed-stream abstraction so the cb-dos backup +
 * restore paths can use either codec for a `partition-N.<ext>` member:
 *
 *   - gzip / DEFLATE  (zlib gzFile)         -> `partition-N.gz`   (the default)
 *   - LZ4 frame       (liblz4 LZ4F)         -> `partition-N.lz4`  (`/CODEC:LZ4`)
 *
 * LZ4 is much cheaper than DEFLATE on a slow 486 CPU at a lower ratio; gzip
 * stays the default. The frame is the standard LZ4 frame format, so a
 * `.lz4` member written here is byte-for-byte interchangeable with the
 * desktop's lz4_flex (`rb-cli backup --format lz4`).
 *
 * The reader (`cbr_*`) mirrors zlib's gzread contract exactly (returns bytes
 * produced, 0 = EOF, -1 = error) so it drops into the existing restore loops;
 * the writer (`cbw_*`) mirrors gzwrite/gzclose. Random access (gzseek, used by
 * the browse engine) is gzip-only -- LZ4 frames aren't seekable, so browsing an
 * lz4 backup isn't supported (restore it first). */
#ifndef CBCODEC_H
#define CBCODEC_H

#define CODEC_GZIP 0
#define CODEC_LZ4  1

/* "gzip"/"lz4" (metadata compression_type) -> codec id, or -1 if unknown. */
int         codec_from_name(const char *name);
/* Detect the codec from a member filename's extension (".gz"/".lz4"), or -1. */
int         codec_for_file(const char *name);
const char *codec_name(int codec);   /* "gzip" / "lz4" (for metadata) */
const char *codec_ext(int codec);    /* "gz"   / "lz4" (for the filename)  */

/* ----- compressed writer ----- */
typedef struct cbw cbw_t;
cbw_t *cbw_open(const char *path, int codec);          /* NULL on failure */
int    cbw_write(cbw_t *w, const void *buf, int len);  /* 0 ok / -1 error */
int    cbw_close(cbw_t *w);                            /* flush+finish; 0 / -1 */

/* ----- compressed reader ----- */
typedef struct cbr cbr_t;
cbr_t *cbr_open(const char *path, int codec);          /* NULL on failure */
int    cbr_read(cbr_t *r, void *buf, int len);         /* bytes; 0 EOF; -1 err */
void   cbr_close(cbr_t *r);

#endif /* CBCODEC_H */
