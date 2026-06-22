/*
 * cb-dos networked-backup spike -- Family B handshake hello-world (phase 7a).
 *
 * Connects to an `rb-cli serve` daemon (the "backup agent" running on the modern
 * box), sends the binary Family-B Hello, and prints the daemon's reply. This
 * proves the transport -- WATT-32 BSD sockets over a packet driver -- plus the
 * JSON-free handshake the 486 will speak, BEFORE any disk streaming. No int13h
 * disk I/O yet; that is phase 7b+ (the chunk producer). See
 * docs/cb_dos_network_and_state.md.
 *
 * Wire contract -- must match src/remote/protocol.rs (read_handshake /
 * write_binary_hello). 8 bytes each way, all fields big-endian:
 *     ->  [ 'R' 'B' 'K' '0' ][ version:u16 ][ caps:u16 ]
 *     <-  [ 'R' 'B' 'K' '0' ][ version:u16 ][ caps:u16 ]
 * The leading magic is how the daemon tells a JSON-free Family-B client from a
 * JSON Family-F frame on the same port.
 *
 * Build: needs the WATT-32 DJGPP package -- run `sh net/fetch-watt32.sh`, then
 *     make -C crusty-backup net          # -> build/nethello.exe
 *
 * Run on the DOS box (after loading your NIC's packet driver + a WATTCP.CFG with
 * DHCP or a static IP -- see docs/cb_dos_networking.md):
 *     NETHELLO 192.168.1.50              (default port 7341)
 *     NETHELLO 192.168.1.50 7341
 */

#include <tcp.h> /* WATT-32: sock_init(), sock_init_err() */
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Must match RB_HELLO_MAGIC / PROTOCOL_VERSION in src/remote/protocol.rs. */
static const unsigned char RB_MAGIC[4] = {'R', 'B', 'K', '0'};
#define RB_PROTOCOL_VERSION 2
#define RB_DEFAULT_PORT 7341
#define CAP_FAMILY_F 0x0001
#define CAP_FAMILY_B 0x0002

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

int main(int argc, char **argv) {
    const char *host;
    unsigned short port = RB_DEFAULT_PORT;
    int s, rc;
    struct sockaddr_in sa;
    unsigned char hello[8], reply[8];
    unsigned short reply_ver, reply_caps;

    if (argc < 2) {
        fprintf(stderr, "usage: %s <agent-ip> [port]\n", argv[0]);
        fprintf(stderr, "  connects to an rb-cli serve daemon and says hello.\n");
        return 2;
    }
    host = argv[1];
    if (argc >= 3)
        port = (unsigned short)atoi(argv[2]);

    /* Bring up WATT-32: packet driver + IP (DHCP / static from WATTCP.CFG).
       Aborts here (with its own message) if no driver or no IP. */
    rc = sock_init();
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
        fprintf(stderr, "bad IP address: %s (use dotted-quad, e.g. 192.168.1.50)\n", host);
        closesocket(s);
        return 1;
    }

    printf("Connecting to backup agent %s:%u ...\n", host, (unsigned)port);
    if (connect(s, (struct sockaddr *)&sa, sizeof sa) < 0) {
        fprintf(stderr, "connect failed (is 'rb-cli serve' running there?)\n");
        closesocket(s);
        return 1;
    }

    /* Binary Family-B Hello: magic + our version + caps (none advertised yet). */
    memcpy(hello, RB_MAGIC, 4);
    hello[4] = (RB_PROTOCOL_VERSION >> 8) & 0xFF;
    hello[5] = RB_PROTOCOL_VERSION & 0xFF;
    hello[6] = 0;
    hello[7] = 0;
    if (send_all(s, hello, 8) != 0) {
        fprintf(stderr, "sending hello failed\n");
        closesocket(s);
        return 1;
    }

    if (recv_all(s, reply, 8) != 0) {
        fprintf(stderr, "no/short hello reply (daemon closed the connection?)\n");
        closesocket(s);
        return 1;
    }
    closesocket(s);

    if (memcmp(reply, RB_MAGIC, 4) != 0) {
        fprintf(stderr, "reply is not an rb daemon (bad magic)\n");
        return 1;
    }
    reply_ver = (unsigned short)((reply[4] << 8) | reply[5]);
    reply_caps = (unsigned short)((reply[6] << 8) | reply[7]);

    printf("Connected. Agent protocol v%u, capabilities 0x%04X%s%s.\n", (unsigned)reply_ver,
           (unsigned)reply_caps, (reply_caps & CAP_FAMILY_F) ? " [file]" : "",
           (reply_caps & CAP_FAMILY_B) ? " [backup-stream]" : "");
    if (!(reply_caps & CAP_FAMILY_B))
        printf("Note: agent does not yet advertise the backup stream (7a handshake only).\n");
    return 0;
}
