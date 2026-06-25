/* cbnet.h -- networked-backup transport for cb-dos (phase 7b/7c).
 *
 * The DOS side of the Family-B chunk PUT: `CRUSTYBK BACKUP` streams a disk image
 * *straight to* an `rb-cli serve` daemon, which assembles a frozen `.cbk`
 * container under its serve root. Nothing is written to a local DOS disk -- the
 * partition is imaged block-by-block over int13h, smart-compacted, and
 * compressed into independent gzip-member *spans* that are sent as chunks as they
 * are produced. So a vintage box with no spare storage can back itself up over
 * the wire. See docs/cb_dos_network_and_state.md (§2 the chunk container, §2c the
 * source-span gzip-member shape) and src/remote/protocol.rs for the wire form.
 *
 * Wire framing (all multi-byte fields big-endian) -- must match protocol.rs:
 *   handshake (8 bytes each way): ['R''B''K''0'][ver:u16][caps:u16]
 *   PUT header:  ['R''B''K''P'][name_len:u16][name][member_count:u16]
 *   per member:  ['R''B''K''M'][kind:u8][name_len:u16][name][chunk_count:u32]
 *   per chunk:   [src_offset:u64][len:u32][crc32:u32][payload:len]  <- ack:u8
 *   result:      [status:u8][cbk_size:u64]
 *
 * A "Gz" member (a partition) is sent as `chunk_count` independent gzip members,
 * each covering up to CBNET_SPAN bytes of uncompressed source; their payloads
 * concatenated on the host are a valid multi-member `.gz`. A "Raw" member
 * (mbr.bin / metadata.json) is one chunk of verbatim bytes the host gzip-wraps
 * when it packs. Uses WATT-32 BSD sockets + zlib deflate; link both into
 * CRUSTYBK.EXE. */
#ifndef CBNET_H
#define CBNET_H

#include <stdint.h>

/* Uncompressed bytes per gzip-member span. Bounds DOS memory (one span's input
 * + compressed output buffer); also the resume granularity (phase 7d). Smaller
 * than the desktop's 4 MiB so a low-RAM 486 stays comfortable. */
#define CBNET_SPAN (1u << 20) /* 1 MiB */

typedef struct cbnet cbnet_t;

/* Parse rb://HOST[:PORT]/NAME into host/port/name. 0 ok / -1 malformed. Defaults:
 * port 7341, NAME = "MYDISK". Shared by `backup rb://` and `restore rb://`. */
int cbnet_parse_url(const char *url, char *host, int hostcap, unsigned short *port, char *name,
                    int namecap);

/* Bring up WATT-32, connect to host:port, do the Family-B handshake, send the
 * PUT header (the container becomes `<cbk_name>.cbk` on the daemon), and read
 * back the daemon's resume map. `fingerprint` is the cheap §4 source fingerprint
 * the daemon uses to decide whether a reconnect may resume a prior partial
 * transfer for this `cbk_name`. `member_count` = the total members that will
 * follow. NULL on failure (a message is printed). */
cbnet_t *cbnet_start(const char *host, unsigned short port, const char *cbk_name,
                     unsigned long fingerprint, int member_count);

/* Send a Raw member (a small verbatim file: mbr.bin / metadata.json) as one
 * chunk. Raw members are always re-sent fresh (not resumed). 0 ok / -1 error. */
int cbnet_raw_member(cbnet_t *n, const char *name, const void *buf, uint32_t len);

/* Begin a Gz member (a partition). `imaged_bytes` is the exact uncompressed
 * length that will be fed via cbnet_part_write -- it fixes the chunk (span)
 * count up front. On resume the daemon may already hold the first spans:
 * `*committed_out` receives how many spans (chunks) to SKIP, so the caller
 * seeks the source to `(*committed_out) * CBNET_SPAN` and feeds only the rest.
 * 0 / -1. */
int cbnet_part_begin(cbnet_t *n, const char *name, int logical_id, uint64_t imaged_bytes,
                     uint32_t *committed_out);
/* Feed the member's (smart-compacted) block stream, starting at the resume
 * offset. Cut into gzip-member spans and sent as chunks at each CBNET_SPAN
 * boundary. 0 / -1. */
int cbnet_part_write(cbnet_t *n, const void *buf, uint32_t len);
/* Finish the member: flush the final span. The daemon owns the partition's gz
 * checksum (it can't be recomputed by the client across a resume), so nothing is
 * returned. 0 / -1. */
int cbnet_part_end(cbnet_t *n);

/* All members sent: read the daemon's result frame. 0 ok (sets *cbk_size), -1 on
 * a NAK / short read / non-zero status. */
int cbnet_finish(cbnet_t *n, unsigned long long *cbk_size);

/* ----- GET: restore over the wire (pull a `.cbk`'s members) ----- */

/* Connect + handshake + send a GET request for `cbk_name` + read the member list.
 * NULL on failure (no such container, or transport error). */
cbnet_t *cbnet_start_get(const char *host, unsigned short port, const char *cbk_name);

/* Fetch a small Raw member (metadata.json / mbr.bin) fully into `buf`. Returns
 * the byte count, -2 if the member doesn't exist, or -1 on error. */
int cbnet_get_raw(cbnet_t *n, const char *name, void *buf, int cap);

/* Begin streaming a Gz member (a partition); the bytes are gunzipped as read.
 * 0 ok / -2 not found / -1 error. Read it via cbnet_get_member_read until it
 * returns 0 (EOF), then cbnet_get_member_end before the next member. */
int cbnet_get_member_begin(cbnet_t *n, const char *name);
int cbnet_get_member_read(cbnet_t *n, void *buf, int len); /* bytes / 0 EOF / -1 */
int cbnet_get_member_end(cbnet_t *n);                      /* finish the member; 0 / -1 */

/* Send the "done" marker (empty member request) so the daemon closes cleanly. */
void cbnet_get_done(cbnet_t *n);

/* Close the socket and free buffers. */
void cbnet_close(cbnet_t *n);

#endif /* CBNET_H */
