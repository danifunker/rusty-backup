/*
 * poll-shim-test.c -- exercise the `poll` override in shim/ppc-compat.c.
 *
 * Link this against the shim and run it on a terminal:
 *
 *   gcc -o /tmp/poll-shim-test poll-shim-test.c ../shim/ppc-compat.c
 *   /tmp/poll-shim-test          # via ssh -tt, or at the console
 *
 * The call goes through the *undecorated* `_poll`, which is what mrustc's
 * generated C references (it is compiled without the SDK headers, so it never
 * sees Leopard's `$UNIX2003` alias). Calling plain `poll()` from a file that
 * did include <poll.h> may reach `_poll$UNIX2003` instead and quietly miss the
 * override - the same trap the fcntl entry documents.
 *
 * Three cases, and all three matter:
 *   1. a tty            - the bug: real poll says POLLNVAL, shim must not
 *   2. a pipe with data - the common path must still work (and not go through
 *                         select, which would be a behaviour change)
 *   3. a closed fd      - a real POLLNVAL must survive; the shim must not
 *                         paper over an actual caller error
 */

#include <errno.h>
#include <poll.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

extern int rb_test_poll(struct pollfd *, nfds_t, int) __asm__("_poll");

static int failures;

static void check(const char *what, int cond, const char *detail)
{
	printf("  %-38s %s   %s\n", what, cond ? "PASS" : "FAIL", detail);
	if (!cond)
		failures++;
}

int main(void)
{
	struct pollfd pfd;
	char buf[64];
	int p[2], fd, rc;

	printf("== 1. tty (the Leopard bug) ==\n");
	if (!isatty(0)) {
		printf("  stdin is not a tty - rerun under `ssh -tt`; skipping\n");
	} else {
		pfd.fd = 0;
		pfd.events = POLLIN;
		pfd.revents = 0;
		errno = 0;
		rc = rb_test_poll(&pfd, 1, 120);
		sprintf(buf, "rc=%d revents=0x%04x errno=%d", rc, pfd.revents, errno);
		check("poll(tty, 120ms) times out cleanly",
		      rc == 0 && pfd.revents == 0, buf);
	}

	printf("== 2. pipe with data pending ==\n");
	if (pipe(p) != 0) {
		printf("  pipe() failed: %s\n", strerror(errno));
		return 1;
	}
	if (write(p[1], "x", 1) != 1) {
		printf("  write() failed: %s\n", strerror(errno));
		return 1;
	}
	pfd.fd = p[0];
	pfd.events = POLLIN;
	pfd.revents = 0;
	errno = 0;
	rc = rb_test_poll(&pfd, 1, 0);
	sprintf(buf, "rc=%d revents=0x%04x", rc, pfd.revents);
	check("poll(pipe) reports readable", rc == 1 && (pfd.revents & POLLIN),
	      buf);
	close(p[0]);
	close(p[1]);

	printf("== 3. closed descriptor (POLLNVAL must survive) ==\n");
	fd = dup(0);
	if (fd < 0) {
		printf("  dup() failed: %s\n", strerror(errno));
		return 1;
	}
	close(fd);
	pfd.fd = fd;
	pfd.events = POLLIN;
	pfd.revents = 0;
	errno = 0;
	rc = rb_test_poll(&pfd, 1, 0);
	sprintf(buf, "rc=%d revents=0x%04x", rc, pfd.revents);
	check("poll(closed fd) still reports POLLNVAL",
	      rc == 1 && (pfd.revents & POLLNVAL), buf);

	printf("\n%s (%d failure%s)\n", failures ? "FAILED" : "all good",
	       failures, failures == 1 ? "" : "s");
	return failures ? 1 : 0;
}
