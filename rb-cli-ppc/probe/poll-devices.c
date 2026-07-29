/*
 * poll-devices.c -- exactly which descriptors can Leopard's poll(2) handle?
 *
 * The `rb-cli tui` failure led to a claim that poll() "does not work on
 * character devices" on Darwin 9. That was extrapolated from a single
 * observation (POLLNVAL on an ssh pty), which is not enough to justify
 * overriding a libc entry point. This enumerates descriptor kinds and reports
 * what poll says about each, next to what select says about the same fd.
 *
 * POSIX: a descriptor that is open must never yield POLLNVAL. Any row where
 * poll answers POLLNVAL (0x0020) while select accepts the same fd is a kernel
 * bug, not a usage error.
 *
 *   gcc -o /tmp/poll-devices poll-devices.c -lutil && /tmp/poll-devices
 *
 * Run under `ssh -tt` (or at the console) so the tty rows are meaningful.
 */

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdio.h>
#include <string.h>
#include <sys/event.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <util.h>

/* kqueue's verdict on the same fd: mio registers EVFILT_READ with EV_RECEIPT,
 * which forces an EV_ERROR return carrying errno in `data` (0 on success).
 * Returns that errno, or -1 if the kevent call itself failed. */
static int kqueue_verdict(int fd)
{
	struct kevent ev;
	int kq, rc;

	kq = kqueue();
	if (kq < 0)
		return -1;
	EV_SET(&ev, fd, EVFILT_READ, EV_ADD | EV_CLEAR | EV_RECEIPT, 0, 0, NULL);
	rc = kevent(kq, &ev, 1, &ev, 1, NULL);
	close(kq);
	if (rc < 0)
		return -1;
	return (int)ev.data;
}

/* Ask poll, select and kqueue about the same descriptor; print all three. */
static void probe(const char *label, int fd, const char *note)
{
	struct pollfd pfd;
	fd_set rd;
	struct timeval tv;
	int prc, src, perr, serr, kerr;
	struct stat st;
	const char *kind = "?";

	if (fd < 0) {
		printf("  %-22s  (could not open: %s)\n", label, strerror(errno));
		return;
	}

	if (fstat(fd, &st) == 0) {
		if (S_ISCHR(st.st_mode))
			kind = "chr";
		else if (S_ISFIFO(st.st_mode))
			kind = "fifo";
		else if (S_ISSOCK(st.st_mode))
			kind = "sock";
		else if (S_ISREG(st.st_mode))
			kind = "reg";
		else if (S_ISBLK(st.st_mode))
			kind = "blk";
	}

	pfd.fd = fd;
	pfd.events = POLLIN;
	pfd.revents = 0;
	errno = 0;
	prc = poll(&pfd, 1, 50);
	perr = errno;

	FD_ZERO(&rd);
	FD_SET(fd, &rd);
	tv.tv_sec = 0;
	tv.tv_usec = 50000;
	errno = 0;
	src = select(fd + 1, &rd, NULL, NULL, &tv);
	serr = errno;

	kerr = kqueue_verdict(fd);

	printf("  %-22s %-5s poll: rc=%-3d revents=0x%04x%s   select: rc=%-3d errno=%-3d  kqueue: %-22s  %s\n",
	       label, kind, prc, pfd.revents,
	       (pfd.revents & POLLNVAL) ? " POLLNVAL" : "        ", src, serr,
	       kerr == 0 ? "ok"
			 : (kerr < 0 ? "kevent() failed"
				     : (kerr == ENOTSUP ? "ENOTSUP (45)"
						       : strerror(kerr))),
	       note);
	if (prc < 0)
		printf("  %-22s   poll errno=%d (%s)\n", "", perr, strerror(perr));
}

int main(void)
{
	int fd, p[2], sv[2], master, slave;
	char name[128];

	printf("== inherited stdin ==\n");
	probe("fd 0 (stdin)", 0, isatty(0) ? "isatty=1" : "isatty=0");

	printf("== terminals ==\n");
	fd = open("/dev/tty", O_RDWR);
	probe("/dev/tty", fd, "controlling terminal");
	if (fd >= 0)
		close(fd);

	/* A pty we make ourselves, so the answer cannot be blamed on ssh. */
	if (openpty(&master, &slave, name, NULL, NULL) == 0) {
		probe("fresh pty slave", slave, "openpty(), no data pending");
		probe("fresh pty master", master, "openpty()");
		/* Now with data actually waiting: a correct poll says POLLIN. */
		if (write(master, "x", 1) == 1) {
			usleep(50000);
			probe("fresh pty slave", slave, "1 byte pending -> want POLLIN");
		}
		close(slave);
		close(master);
	} else {
		printf("  openpty() failed: %s\n", strerror(errno));
	}

	printf("== other character devices ==\n");
	fd = open("/dev/null", O_RDONLY);
	probe("/dev/null", fd, "always ready");
	if (fd >= 0)
		close(fd);
	fd = open("/dev/zero", O_RDONLY);
	probe("/dev/zero", fd, "always ready");
	if (fd >= 0)
		close(fd);
	fd = open("/dev/random", O_RDONLY);
	probe("/dev/random", fd, "always ready");
	if (fd >= 0)
		close(fd);

	printf("== non-character descriptors (control group) ==\n");
	fd = open("/etc/hosts", O_RDONLY);
	probe("regular file", fd, "always ready");
	if (fd >= 0)
		close(fd);
	if (pipe(p) == 0) {
		probe("pipe (empty)", p[0], "want timeout");
		if (write(p[1], "x", 1) == 1)
			probe("pipe (1 byte)", p[0], "want POLLIN");
		close(p[0]);
		close(p[1]);
	}
	if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0) {
		probe("unix socket (empty)", sv[0], "want timeout");
		if (write(sv[1], "x", 1) == 1)
			probe("unix socket (1 byte)", sv[0], "want POLLIN");
		close(sv[0]);
		close(sv[1]);
	}

	printf("\ndone\n");
	return 0;
}
