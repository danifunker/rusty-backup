/* cbnet.c -- DOS-side Family-B chunk PUT transport (see cbnet.h).
 *
 * WATT-32 BSD sockets for the wire + zlib deflate for the per-span gzip members.
 * The producer never holds a whole partition in RAM: it accumulates one
 * CBNET_SPAN of uncompressed source, compresses it into an independent gzip
 * member, sends it as a chunk, and recycles the buffer. */

#include "cbnet.h"

#include <tcp.h> /* WATT-32: sock_init(), sock_init_err() */
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

/* Must match src/remote/protocol.rs. */
static const unsigned char RB_MAGIC[4] = {'R', 'B', 'K', '0'};
static const unsigned char PUT_MAGIC[4] = {'R', 'B', 'K', 'P'};
static const unsigned char MEMBER_MAGIC[4] = {'R', 'B', 'K', 'M'};
static const unsigned char RESUME_MAGIC[4] = {'R', 'B', 'K', 'R'};
#define RB_PROTO_VER 2
#define CAP_FAMILY_B 0x0002
#define PUT_ACK 0x06

#define MAX_RESUME 16 /* distinct Gz members the daemon can report resumable */

struct cbnet {
    int sock;
    int net_up; /* sock_init succeeded */
    /* PUT: span buffers */
    unsigned char *inbuf;  /* uncompressed span accumulator (CBNET_SPAN) */
    uint32_t inlen;        /* bytes currently in inbuf */
    unsigned char *outbuf; /* compressed span */
    uint32_t outcap;
    /* current Gz member */
    int in_member;
    unsigned long span_uoff; /* uncompressed offset of the current span */
    /* resume map from the daemon (Gz members it already holds chunks for) */
    struct {
        char name[28];
        uint32_t committed;
    } resume[MAX_RESUME];
    int nresume;
    /* GET (restore): a member byte stream the daemon frames as [u32 BE n][n]*
     * terminated by [u32 0]; for a .gz member we inflate it (multi-member gzip). */
    unsigned char *framebuf; /* current frame's bytes from the socket */
    int framecap, framelen, framepos;
    int member_eof;          /* read the [u32 0] frame terminator */
    int member_inflate;      /* 1 = gz (inflate), 0 = raw passthrough */
    z_stream inf;            /* inflate state for a .gz member */
    int inf_active;          /* inflateInit2 done, inflateEnd pending */
};

int cbnet_parse_url(const char *url, char *host, int hostcap, unsigned short *port, char *name,
                    int namecap) {
    if (strncmp(url, "rb://", 5) != 0)
        return -1;
    const char *p = url + 5;
    const char *slash = strchr(p, '/');
    const char *colon = strchr(p, ':');
    const char *hostend;
    *port = 7341;
    if (colon && (!slash || colon < slash)) {
        hostend = colon;
        *port = (unsigned short)atoi(colon + 1);
    } else {
        hostend = slash ? slash : p + strlen(p);
    }
    int hl = (int)(hostend - p);
    if (hl <= 0 || hl >= hostcap)
        return -1;
    memcpy(host, p, hl);
    host[hl] = 0;
    if (slash && slash[1]) {
        if ((int)strlen(slash + 1) >= namecap)
            return -1;
        strcpy(name, slash + 1);
    } else {
        strncpy(name, "MYDISK", namecap - 1);
        name[namecap - 1] = 0;
    }
    return 0;
}

/* committed chunk count the daemon reported for `name`, or 0 if none. */
static uint32_t resume_committed(const cbnet_t *n, const char *name) {
    for (int i = 0; i < n->nresume; i++)
        if (strcmp(n->resume[i].name, name) == 0)
            return n->resume[i].committed;
    return 0;
}

/* ----- big-endian field writers ----- */
static void be16(unsigned char *p, unsigned v) {
    p[0] = (unsigned char)(v >> 8);
    p[1] = (unsigned char)v;
}
static void be32(unsigned char *p, unsigned long v) {
    p[0] = (unsigned char)(v >> 24);
    p[1] = (unsigned char)(v >> 16);
    p[2] = (unsigned char)(v >> 8);
    p[3] = (unsigned char)v;
}
static void be64(unsigned char *p, unsigned long long v) {
    for (int i = 0; i < 8; i++)
        p[i] = (unsigned char)((v >> (56 - 8 * i)) & 0xFF);
}

/* recv() may return short; loop until all n bytes arrive. 0 on success. */
static int recv_all(int s, unsigned char *buf, int n) {
    int got = 0;
    while (got < n) {
        int r = recv(s, buf + got, n - got, 0);
        if (r <= 0)
            return -1;
        got += r;
    }
    return 0;
}

/* send() may accept a short count; loop until all n bytes go out. 0 on success. */
static int send_all(int s, const unsigned char *buf, int n) {
    int sent = 0;
    while (sent < n) {
        int r = send(s, buf + sent, n - sent, 0);
        if (r <= 0)
            return -1;
        sent += r;
    }
    return 0;
}

/* Send one chunk header + payload, then wait for the stop-and-go ack. 0 / -1. */
static int send_chunk(cbnet_t *n, unsigned long long src_offset, const unsigned char *payload,
                      uint32_t len, unsigned long crc) {
    unsigned char ch[16];
    be64(ch, src_offset);
    be32(ch + 8, len);
    be32(ch + 12, crc);
    if (send_all(n->sock, ch, 16) != 0)
        return -1;
    if (len && send_all(n->sock, payload, (int)len) != 0)
        return -1;
    unsigned char ack;
    if (recv_all(n->sock, &ack, 1) != 0 || ack != PUT_ACK)
        return -1;
    return 0;
}

/* Compress inbuf[0..inlen] into a gzip member and ship it as one chunk. The
 * chunk's crc32 is the wire-integrity check of the compressed payload (the
 * daemon owns the whole-member gz checksum, so we don't accumulate it). */
static int flush_span(cbnet_t *n) {
    z_stream s;
    memset(&s, 0, sizeof s);
    /* windowBits 15 + 16 = a gzip wrapper (an independent gzip member). */
    if (deflateInit2(&s, 6, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK)
        return -1;
    s.next_in = n->inbuf;
    s.avail_in = n->inlen;
    s.next_out = n->outbuf;
    s.avail_out = n->outcap;
    int rc = deflate(&s, Z_FINISH);
    uint32_t clen = n->outcap - s.avail_out;
    deflateEnd(&s);
    if (rc != Z_STREAM_END)
        return -1; /* span didn't fit outbuf (shouldn't happen: sized > bound) */

    unsigned long crc = crc32(0L, n->outbuf, clen);
    if (send_chunk(n, (unsigned long long)n->span_uoff, n->outbuf, clen, crc) != 0)
        return -1;

    n->span_uoff += n->inlen;
    n->inlen = 0;
    return 0;
}

/* Read the daemon's RBKR resume map into n->resume. 0 / -1. */
static int read_resume_map(cbnet_t *n) {
    unsigned char hdr[6];
    if (recv_all(n->sock, hdr, 6) != 0)
        return -1;
    if (memcmp(hdr, RESUME_MAGIC, 4) != 0)
        return -1;
    int count = (hdr[4] << 8) | hdr[5];
    n->nresume = 0;
    for (int i = 0; i < count; i++) {
        unsigned char nl[2];
        if (recv_all(n->sock, nl, 2) != 0)
            return -1;
        int len = (nl[0] << 8) | nl[1];
        char name[64];
        if (len >= (int)sizeof name) /* skip an over-long name we wouldn't match */
            return -1;
        if (len && recv_all(n->sock, (unsigned char *)name, len) != 0)
            return -1;
        name[len] = 0;
        unsigned char cc[4];
        if (recv_all(n->sock, cc, 4) != 0)
            return -1;
        uint32_t committed = ((uint32_t)cc[0] << 24) | ((uint32_t)cc[1] << 16) |
                             ((uint32_t)cc[2] << 8) | cc[3];
        if (n->nresume < MAX_RESUME && len < (int)sizeof n->resume[0].name) {
            strcpy(n->resume[n->nresume].name, name);
            n->resume[n->nresume].committed = committed;
            n->nresume++;
        }
    }
    return 0;
}

/* Bring up WATT-32, connect to host:port, and do the Family-B handshake. NULL on
 * failure (a message is printed). Shared by the PUT (backup) and GET (restore)
 * entry points. */
static cbnet_t *cbnet_connect(const char *host, unsigned short port) {
    cbnet_t *n = calloc(1, sizeof *n);
    if (!n)
        return NULL;
    n->sock = -1;

    int rc = sock_init();
    if (rc != 0) {
        fprintf(stderr, "network init failed: %s\n", sock_init_err(rc));
        cbnet_close(n);
        return NULL;
    }
    n->net_up = 1;

    n->sock = socket(AF_INET, SOCK_STREAM, 0);
    if (n->sock < 0) {
        fprintf(stderr, "socket() failed\n");
        cbnet_close(n);
        return NULL;
    }
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof sa);
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    sa.sin_addr.s_addr = inet_addr(host);
    if (sa.sin_addr.s_addr == INADDR_NONE) {
        fprintf(stderr, "bad IP address: %s (use dotted-quad)\n", host);
        cbnet_close(n);
        return NULL;
    }
    printf("connecting to backup agent %s:%u ...\n", host, (unsigned)port);
    if (connect(n->sock, (struct sockaddr *)&sa, sizeof sa) < 0) {
        fprintf(stderr, "connect failed (is 'rb-cli serve' running there?)\n");
        cbnet_close(n);
        return NULL;
    }

    unsigned char hello[8], reply[8];
    memcpy(hello, RB_MAGIC, 4);
    be16(hello + 4, RB_PROTO_VER);
    be16(hello + 6, 0);
    if (send_all(n->sock, hello, 8) != 0 || recv_all(n->sock, reply, 8) != 0 ||
        memcmp(reply, RB_MAGIC, 4) != 0) {
        fprintf(stderr, "handshake failed (not an rb daemon?)\n");
        cbnet_close(n);
        return NULL;
    }
    if (!((((unsigned)reply[6] << 8) | reply[7]) & CAP_FAMILY_B))
        fprintf(stderr, "warning: agent does not advertise the backup stream (old daemon?)\n");
    return n;
}

cbnet_t *cbnet_start(const char *host, unsigned short port, const char *cbk_name,
                     unsigned long fingerprint, int member_count) {
    cbnet_t *n = cbnet_connect(host, port);
    if (!n)
        return NULL;
    n->outcap = CBNET_SPAN + CBNET_SPAN / 16 + 1024; /* > deflateBound(SPAN) */
    n->inbuf = malloc(CBNET_SPAN);
    n->outbuf = malloc(n->outcap);
    if (!n->inbuf || !n->outbuf) {
        fprintf(stderr, "out of memory for span buffers\n");
        cbnet_close(n);
        return NULL;
    }

    /* PUT header: RBKP, name, fingerprint (u32), member_count (u16). */
    int nlen = (int)strlen(cbk_name);
    unsigned char ph[8];
    memcpy(ph, PUT_MAGIC, 4);
    be16(ph + 4, (unsigned)nlen);
    unsigned char fp[4];
    be32(fp, fingerprint);
    unsigned char mc[2];
    be16(mc, (unsigned)member_count);
    if (send_all(n->sock, ph, 6) != 0 ||
        send_all(n->sock, (const unsigned char *)cbk_name, nlen) != 0 ||
        send_all(n->sock, fp, 4) != 0 || send_all(n->sock, mc, 2) != 0) {
        fprintf(stderr, "sending PUT header failed\n");
        cbnet_close(n);
        return NULL;
    }

    /* Daemon's resume map (which members it already holds, and how far). */
    if (read_resume_map(n) != 0) {
        fprintf(stderr, "reading resume map failed\n");
        cbnet_close(n);
        return NULL;
    }
    if (n->nresume > 0)
        printf("agent has partial data for %d member(s) -- resuming\n", n->nresume);
    return n;
}

/* Send a member header (RBKM, kind, name, chunk_count). 0 / -1. */
static int send_member_header(cbnet_t *n, int kind, const char *name, uint32_t chunk_count) {
    int nlen = (int)strlen(name);
    unsigned char hdr[8];
    memcpy(hdr, MEMBER_MAGIC, 4);
    hdr[4] = (unsigned char)kind;
    be16(hdr + 5, (unsigned)nlen);
    unsigned char cc[4];
    be32(cc, chunk_count);
    if (send_all(n->sock, hdr, 7) != 0 || send_all(n->sock, (const unsigned char *)name, nlen) != 0 ||
        send_all(n->sock, cc, 4) != 0)
        return -1;
    return 0;
}

int cbnet_raw_member(cbnet_t *n, const char *name, const void *buf, uint32_t len) {
    if (send_member_header(n, 1 /*Raw*/, name, 1) != 0)
        return -1;
    unsigned long crc = crc32(0L, (const unsigned char *)buf, len);
    return send_chunk(n, 0ULL, (const unsigned char *)buf, len, crc);
}

int cbnet_part_begin(cbnet_t *n, const char *name, int logical_id, uint64_t imaged_bytes,
                     uint32_t *committed_out) {
    (void)logical_id;
    uint32_t chunk_count = (uint32_t)((imaged_bytes + CBNET_SPAN - 1) / CBNET_SPAN);
    if (chunk_count == 0)
        chunk_count = 1; /* always at least one (possibly empty) span */
    /* The daemon may already hold the first `committed` spans (a resumed
     * transfer); skip them and start emitting at span `committed`. */
    uint32_t committed = resume_committed(n, name);
    if (committed > chunk_count)
        committed = chunk_count; /* defensive: never under-send */
    if (send_member_header(n, 0 /*Gz*/, name, chunk_count) != 0)
        return -1;
    n->in_member = 1;
    n->inlen = 0;
    n->span_uoff = (unsigned long)committed * CBNET_SPAN;
    if (committed_out)
        *committed_out = committed;
    return 0;
}

int cbnet_part_write(cbnet_t *n, const void *buf, uint32_t len) {
    const unsigned char *p = (const unsigned char *)buf;
    while (len > 0) {
        uint32_t room = CBNET_SPAN - n->inlen;
        uint32_t take = len < room ? len : room;
        memcpy(n->inbuf + n->inlen, p, take);
        n->inlen += take;
        p += take;
        len -= take;
        if (n->inlen == CBNET_SPAN && flush_span(n) != 0)
            return -1;
    }
    return 0;
}

int cbnet_part_end(cbnet_t *n) {
    /* Flush a trailing partial span. (An exact-multiple member already flushed
     * its last full span at the boundary, leaving inlen == 0.) */
    if (n->inlen > 0 && flush_span(n) != 0)
        return -1;
    n->in_member = 0;
    return 0;
}

int cbnet_finish(cbnet_t *n, unsigned long long *cbk_size) {
    unsigned char res[9];
    if (recv_all(n->sock, res, 9) != 0) {
        fprintf(stderr, "no result frame (daemon closed early?)\n");
        return -1;
    }
    if (res[0] != 0) {
        fprintf(stderr, "daemon reported PUT failure (status %u)\n", (unsigned)res[0]);
        return -1;
    }
    if (cbk_size) {
        unsigned long long sz = 0;
        for (int i = 0; i < 8; i++)
            sz = (sz << 8) | res[1 + i];
        *cbk_size = sz;
    }
    return 0;
}

/* ----- GET: restore over the wire ----- */

static const unsigned char GET_MAGIC[4] = {'R', 'B', 'K', 'G'};
#define FRAME_INIT 65536

/* Send a member-fetch request: u16 name_len (BE) + name. An empty name is the
 * "done" marker. 0 / -1. */
static int send_member_request(cbnet_t *n, const char *name) {
    int nlen = name ? (int)strlen(name) : 0;
    unsigned char nl[2];
    be16(nl, (unsigned)nlen);
    if (send_all(n->sock, nl, 2) != 0)
        return -1;
    if (nlen && send_all(n->sock, (const unsigned char *)name, nlen) != 0)
        return -1;
    return 0;
}

/* Read the next member-stream frame into framebuf. Sets member_eof on the [u32 0]
 * terminator. 0 / -1. */
static int fill_frame(cbnet_t *n) {
    unsigned char hdr[4];
    if (recv_all(n->sock, hdr, 4) != 0)
        return -1;
    uint32_t len = ((uint32_t)hdr[0] << 24) | ((uint32_t)hdr[1] << 16) | ((uint32_t)hdr[2] << 8) |
                   hdr[3];
    if (len == 0) {
        n->member_eof = 1;
        n->framelen = 0;
        n->framepos = 0;
        return 0;
    }
    if ((int)len > n->framecap) {
        unsigned char *nb = realloc(n->framebuf, len);
        if (!nb)
            return -1;
        n->framebuf = nb;
        n->framecap = (int)len;
    }
    if (recv_all(n->sock, n->framebuf, (int)len) != 0)
        return -1;
    n->framelen = (int)len;
    n->framepos = 0;
    return 0;
}

cbnet_t *cbnet_start_get(const char *host, unsigned short port, const char *cbk_name) {
    cbnet_t *n = cbnet_connect(host, port);
    if (!n)
        return NULL;
    n->framebuf = malloc(FRAME_INIT);
    if (!n->framebuf) {
        fprintf(stderr, "out of memory\n");
        cbnet_close(n);
        return NULL;
    }
    n->framecap = FRAME_INIT;

    /* GET request: RBKG + name. */
    int nlen = (int)strlen(cbk_name);
    unsigned char gh[6];
    memcpy(gh, GET_MAGIC, 4);
    be16(gh + 4, (unsigned)nlen);
    if (send_all(n->sock, gh, 6) != 0 ||
        send_all(n->sock, (const unsigned char *)cbk_name, nlen) != 0) {
        fprintf(stderr, "sending GET request failed\n");
        cbnet_close(n);
        return NULL;
    }
    /* GET-open reply: status(1); on ok member_count(2) + names we don't need. */
    unsigned char st;
    if (recv_all(n->sock, &st, 1) != 0) {
        fprintf(stderr, "no GET reply\n");
        cbnet_close(n);
        return NULL;
    }
    if (st != 0) {
        fprintf(stderr, "agent has no container '%s'\n", cbk_name);
        cbnet_close(n);
        return NULL;
    }
    unsigned char cc[2];
    if (recv_all(n->sock, cc, 2) != 0) {
        cbnet_close(n);
        return NULL;
    }
    int count = (cc[0] << 8) | cc[1];
    for (int i = 0; i < count; i++) { /* skip the advertised member names */
        unsigned char ml[2];
        if (recv_all(n->sock, ml, 2) != 0) {
            cbnet_close(n);
            return NULL;
        }
        int ln = (ml[0] << 8) | ml[1];
        while (ln > 0) {
            unsigned char tmp[64];
            int t = ln < (int)sizeof tmp ? ln : (int)sizeof tmp;
            if (recv_all(n->sock, tmp, t) != 0) {
                cbnet_close(n);
                return NULL;
            }
            ln -= t;
        }
    }
    return n;
}

/* Request a member and read its status byte. 0 found / 1 not-found / -1 error. */
static int begin_member_stream(cbnet_t *n, const char *name) {
    if (send_member_request(n, name) != 0)
        return -1;
    unsigned char st;
    if (recv_all(n->sock, &st, 1) != 0)
        return -1;
    n->member_eof = 0;
    n->framelen = 0;
    n->framepos = 0;
    return st == 0 ? 0 : 1;
}

int cbnet_get_raw(cbnet_t *n, const char *name, void *buf, int cap) {
    int s = begin_member_stream(n, name);
    if (s < 0)
        return -1;
    if (s == 1)
        return -2; /* not found */
    n->member_inflate = 0;
    int got = 0;
    unsigned char *out = (unsigned char *)buf;
    while (!n->member_eof) {
        if (fill_frame(n) != 0)
            return -1;
        if (n->member_eof)
            break;
        if (got + n->framelen > cap) {
            fprintf(stderr, "member %s too large for buffer\n", name);
            return -1;
        }
        memcpy(out + got, n->framebuf, n->framelen);
        got += n->framelen;
    }
    return got;
}

int cbnet_get_member_begin(cbnet_t *n, const char *name) {
    int s = begin_member_stream(n, name);
    if (s < 0)
        return -1;
    if (s == 1)
        return -2;
    memset(&n->inf, 0, sizeof n->inf);
    if (inflateInit2(&n->inf, 15 + 16) != Z_OK) /* 15+16 = gzip, multi-member */
        return -1;
    n->inf_active = 1;
    n->member_inflate = 1;
    return 0;
}

int cbnet_get_member_read(cbnet_t *n, void *buf, int len) {
    if (!n->inf_active)
        return -1;
    n->inf.next_out = (Bytef *)buf;
    n->inf.avail_out = (uInt)len;
    while (n->inf.avail_out > 0) {
        if (n->inf.avail_in == 0) {
            if (n->member_eof)
                break; /* no more compressed input */
            if (fill_frame(n) != 0)
                return -1;
            if (n->member_eof)
                break;
            n->inf.next_in = n->framebuf;
            n->inf.avail_in = (uInt)n->framelen;
        }
        int rc = inflate(&n->inf, Z_NO_FLUSH);
        if (rc == Z_STREAM_END) {
            /* End of one gzip member; a multi-member `.gz` continues with the
             * next. Reset and keep going while input remains / more frames. */
            if (n->inf.avail_in > 0 || !n->member_eof) {
                if (inflateReset(&n->inf) != Z_OK)
                    return -1;
                continue;
            }
            break;
        }
        if (rc != Z_OK)
            return -1;
    }
    return len - (int)n->inf.avail_out;
}

int cbnet_get_member_end(cbnet_t *n) {
    /* Drain to the frame terminator if the caller stopped early. */
    while (!n->member_eof)
        if (fill_frame(n) != 0)
            break;
    if (n->inf_active) {
        inflateEnd(&n->inf);
        n->inf_active = 0;
    }
    n->member_inflate = 0;
    return 0;
}

void cbnet_get_done(cbnet_t *n) {
    send_member_request(n, NULL); /* empty-name "done" marker */
}

void cbnet_close(cbnet_t *n) {
    if (!n)
        return;
    if (n->inf_active)
        inflateEnd(&n->inf);
    if (n->sock >= 0)
        closesocket(n->sock);
    free(n->inbuf);
    free(n->outbuf);
    free(n->framebuf);
    free(n);
}
