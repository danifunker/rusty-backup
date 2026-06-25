/*
 * cb-dos networked-backup PUT client -- chunk protocol + container (phase 7b).
 *
 * Streams a native backup *folder* (the metadata.json + mbr.bin + partition-N.gz
 * tree that `CRUSTYBK BACKUP` writes to a DOS disk) to an `rb-cli serve` daemon
 * over the Family-B chunk protocol. The daemon assembles the members into a
 * frozen `.cbk` container under its serve root -- byte-identical to a desktop
 * `pack_folder_to_cbk` of the same folder. See docs/cb_dos_network_and_state.md
 * (the chunk-PUT half of phase 7b) and src/remote/protocol.rs for the wire form.
 *
 * Wire contract -- all multi-byte fields big-endian:
 *   handshake (8 bytes each way, phase 7a):
 *     ->  [ 'R' 'B' 'K' '0' ][ version:u16 ][ caps:u16 ]
 *     <-  [ 'R' 'B' 'K' '0' ][ version:u16 ][ caps:u16 ]
 *   then the PUT:
 *     ->  [ 'R' 'B' 'K' 'P' ][ name_len:u16 ][ name ][ member_count:u16 ]
 *     per member:
 *       ->  [ 'R' 'B' 'K' 'M' ][ kind:u8 ][ name_len:u16 ][ name ][ chunk_count:u32 ]
 *       per chunk (stop-and-go):
 *         ->  [ src_offset:u64 ][ len:u32 ][ crc32:u32 ][ payload:len ]
 *         <-  [ ack:u8 ]        (0x06 ACK / 0x15 NAK)
 *     <-  [ status:u8 ][ cbk_size:u64 ]   (status 0 = ok)
 *
 * This sends one chunk per member (cb-dos images each partition as a single
 * gzip member); per-span chunking for finer resume is a later phase (7d).
 *
 * Build: needs the WATT-32 DJGPP package -- run `sh net/fetch-watt32.sh`, then
 *     make -C crusty-backup net          # -> build/netput.exe
 *
 * Run on the DOS box (after the packet driver + WATTCP.CFG -- see
 * docs/cb_dos_networking.md). LFN must be active (DOSLFN) so the long member
 * names (partition-0.gz.crc32) survive enumeration:
 *     NETPUT <agent-ip> <port> <backup-folder> [container-name]
 *     NETPUT 10.0.2.2 7341 C:\BK MYDISK
 */

#include <tcp.h> /* WATT-32: sock_init(), sock_init_err() */
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Must match src/remote/protocol.rs. */
static const unsigned char RB_MAGIC[4] = {'R', 'B', 'K', '0'};
static const unsigned char PUT_MAGIC[4] = {'R', 'B', 'K', 'P'};
static const unsigned char MEMBER_MAGIC[4] = {'R', 'B', 'K', 'M'};
#define RB_PROTOCOL_VERSION 2
#define CAP_FAMILY_B 0x0002
#define PUT_ACK 0x06
#define PUT_NAK 0x15

#define MAX_MEMBERS 64
#define IOBUF 8192

/* ----- standard CRC32 (IEEE 802.3 / zlib / crc32fast): matches the daemon ----- */
static unsigned long crc_table[256];
static void crc_init(void) {
    for (unsigned i = 0; i < 256; i++) {
        unsigned long c = i;
        for (int k = 0; k < 8; k++)
            c = (c & 1) ? (0xEDB88320UL ^ (c >> 1)) : (c >> 1);
        crc_table[i] = c;
    }
}
static unsigned long crc_update(unsigned long crc, const unsigned char *buf, int n) {
    for (int i = 0; i < n; i++)
        crc = crc_table[(crc ^ buf[i]) & 0xFF] ^ (crc >> 8);
    return crc;
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

/* Join a folder + name with a single backslash (folder may already end in one). */
static void join_path(char *out, const char *folder, const char *name) {
    int n = (int)strlen(folder);
    if (n > 0 && (folder[n - 1] == '\\' || folder[n - 1] == '/'))
        sprintf(out, "%s%s", folder, name);
    else
        sprintf(out, "%s\\%s", folder, name);
}

/* kind byte: 0 = Gz (a *.gz member, sent verbatim), 1 = Raw (host gzip-wraps). */
static int member_kind(const char *name) {
    int n = (int)strlen(name);
    if (n >= 3) {
        const char *e = name + n - 3;
        if ((e[0] == '.') && (e[1] == 'g' || e[1] == 'G') && (e[2] == 'z' || e[2] == 'Z'))
            return 0;
    }
    return 1;
}

/* Send one member: header + a single chunk holding the whole file. 0 on success. */
static int put_member(int s, const char *folder, const char *name) {
    char path[160];
    join_path(path, folder, name);

    FILE *fp = fopen(path, "rb");
    if (!fp) {
        fprintf(stderr, "  cannot open %s\n", path);
        return -1;
    }

    /* Pass 1: size + CRC32 (the crc precedes the payload on the wire). */
    unsigned char io[IOBUF];
    unsigned long size = 0, crc = 0xFFFFFFFFUL;
    size_t got;
    while ((got = fread(io, 1, sizeof io, fp)) > 0) {
        size += (unsigned long)got;
        crc = crc_update(crc, io, (int)got);
    }
    crc ^= 0xFFFFFFFFUL;

    /* Member header: RBKM, kind, name, chunk_count = 1. */
    int nlen = (int)strlen(name);
    unsigned char hdr[8];
    memcpy(hdr, MEMBER_MAGIC, 4);
    hdr[4] = (unsigned char)member_kind(name);
    be16(hdr + 5, (unsigned)nlen);
    if (send_all(s, hdr, 7) != 0 || send_all(s, (const unsigned char *)name, nlen) != 0) {
        fclose(fp);
        return -1;
    }
    unsigned char cc[4];
    be32(cc, 1UL);
    if (send_all(s, cc, 4) != 0) {
        fclose(fp);
        return -1;
    }

    /* Chunk header: src_offset = 0, len, crc32. */
    unsigned char ch[16];
    be64(ch, 0ULL);
    be32(ch + 8, size);
    be32(ch + 12, crc);
    if (send_all(s, ch, 16) != 0) {
        fclose(fp);
        return -1;
    }

    /* Pass 2: stream the payload. */
    rewind(fp);
    unsigned long sent = 0;
    while ((got = fread(io, 1, sizeof io, fp)) > 0) {
        if (send_all(s, io, (int)got) != 0) {
            fclose(fp);
            return -1;
        }
        sent += (unsigned long)got;
    }
    fclose(fp);
    if (sent != size) {
        fprintf(stderr, "  %s shrank mid-send (%lu != %lu)\n", name, sent, size);
        return -1;
    }

    /* Stop-and-go ack. */
    unsigned char ack;
    if (recv_all(s, &ack, 1) != 0) {
        fprintf(stderr, "  no ack for %s\n", name);
        return -1;
    }
    if (ack != PUT_ACK) {
        fprintf(stderr, "  daemon NAK'd %s (0x%02X)\n", name, ack);
        return -1;
    }
    printf("  sent %s (%lu bytes, crc %08lx)\n", name, size, crc);
    return 0;
}

int main(int argc, char **argv) {
    const char *host, *folder, *name = "MYDISK";
    unsigned short port;
    int s;
    struct sockaddr_in sa;
    unsigned char hello[8], reply[8];
    char members[MAX_MEMBERS][32];
    int nmembers = 0;

    if (argc < 4) {
        fprintf(stderr, "usage: %s <agent-ip> <port> <backup-folder> [container-name]\n", argv[0]);
        fprintf(stderr, "  streams a CRUSTYBK backup folder to an rb-cli serve daemon as a .cbk.\n");
        fprintf(stderr, "  e.g. %s 10.0.2.2 7341 C:\\BK MYDISK\n", argv[0]);
        return 2;
    }
    host = argv[1];
    port = (unsigned short)atoi(argv[2]);
    folder = argv[3];
    if (argc >= 5)
        name = argv[4];

    crc_init();

    /* Enumerate the backup folder's regular files (LFN names need DOSLFN). */
    DIR *d = opendir(folder);
    if (!d) {
        fprintf(stderr, "cannot open folder %s\n", folder);
        return 1;
    }
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.')
            continue; /* skip "." / ".." */
        char path[160];
        join_path(path, folder, de->d_name);
        struct stat st;
        if (stat(path, &st) != 0 || (st.st_mode & S_IFDIR))
            continue; /* only regular files */
        if (nmembers >= MAX_MEMBERS) {
            fprintf(stderr, "too many files in %s (max %d)\n", folder, MAX_MEMBERS);
            closedir(d);
            return 1;
        }
        if ((int)strlen(de->d_name) >= (int)sizeof members[0]) {
            fprintf(stderr, "member name too long: %s\n", de->d_name);
            closedir(d);
            return 1;
        }
        strcpy(members[nmembers++], de->d_name);
    }
    closedir(d);
    if (nmembers == 0) {
        fprintf(stderr, "no files in %s (is it a backup folder?)\n", folder);
        return 1;
    }
    printf("folder %s: %d member%s -> %s.cbk on %s:%u\n", folder, nmembers,
           nmembers == 1 ? "" : "s", name, host, (unsigned)port);

    /* Bring up WATT-32 (packet driver + IP from WATTCP.CFG / DHCP). */
    int rc = sock_init();
    if (rc != 0) {
        fprintf(stderr, "network init failed: %s\n", sock_init_err(rc));
        return 1;
    }

    s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0) {
        fprintf(stderr, "socket() failed\n");
        return 1;
    }
    memset(&sa, 0, sizeof sa);
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    sa.sin_addr.s_addr = inet_addr(host);
    if (sa.sin_addr.s_addr == INADDR_NONE) {
        fprintf(stderr, "bad IP address: %s (use dotted-quad)\n", host);
        closesocket(s);
        return 1;
    }
    printf("connecting to backup agent %s:%u ...\n", host, (unsigned)port);
    if (connect(s, (struct sockaddr *)&sa, sizeof sa) < 0) {
        fprintf(stderr, "connect failed (is 'rb-cli serve' running there?)\n");
        closesocket(s);
        return 1;
    }

    /* Binary Family-B handshake (phase 7a). */
    memcpy(hello, RB_MAGIC, 4);
    hello[4] = (RB_PROTOCOL_VERSION >> 8) & 0xFF;
    hello[5] = RB_PROTOCOL_VERSION & 0xFF;
    hello[6] = 0;
    hello[7] = 0;
    if (send_all(s, hello, 8) != 0 || recv_all(s, reply, 8) != 0) {
        fprintf(stderr, "handshake failed\n");
        closesocket(s);
        return 1;
    }
    if (memcmp(reply, RB_MAGIC, 4) != 0) {
        fprintf(stderr, "reply is not an rb daemon (bad magic)\n");
        closesocket(s);
        return 1;
    }
    {
        unsigned caps = (unsigned)((reply[6] << 8) | reply[7]);
        if (!(caps & CAP_FAMILY_B))
            fprintf(stderr, "warning: agent does not advertise the backup stream (old daemon?)\n");
    }

    /* PUT header: RBKP, name, member_count. */
    {
        int nlen = (int)strlen(name);
        unsigned char ph[8];
        memcpy(ph, PUT_MAGIC, 4);
        be16(ph + 4, (unsigned)nlen);
        if (send_all(s, ph, 6) != 0 || send_all(s, (const unsigned char *)name, nlen) != 0) {
            fprintf(stderr, "sending PUT header failed\n");
            closesocket(s);
            return 1;
        }
        unsigned char mc[2];
        be16(mc, (unsigned)nmembers);
        if (send_all(s, mc, 2) != 0) {
            fprintf(stderr, "sending member count failed\n");
            closesocket(s);
            return 1;
        }
    }

    /* Members, each one stop-and-go chunk. */
    for (int i = 0; i < nmembers; i++) {
        if (put_member(s, folder, members[i]) != 0) {
            fprintf(stderr, "PUT failed on %s\n", members[i]);
            closesocket(s);
            return 1;
        }
    }

    /* Result: status + cbk_size. */
    unsigned char res[9];
    if (recv_all(s, res, 9) != 0) {
        fprintf(stderr, "no result frame (daemon closed early?)\n");
        closesocket(s);
        return 1;
    }
    closesocket(s);
    if (res[0] != 0) {
        fprintf(stderr, "daemon reported PUT failure (status %u)\n", (unsigned)res[0]);
        return 1;
    }
    {
        unsigned long long sz = 0;
        for (int i = 0; i < 8; i++)
            sz = (sz << 8) | res[1 + i];
        printf("PUT complete: %s.cbk assembled on the agent (%llu bytes).\n", name, sz);
    }
    return 0;
}
