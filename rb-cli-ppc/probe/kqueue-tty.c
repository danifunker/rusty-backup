/*
 * kqueue-tty.c -- replay crossterm's terminal-event init, syscall by syscall.
 *
 * `rb-cli tui` dies on Leopard with "error: Failed to initialize input reader".
 * That message is crossterm's, and it is a *lost* error: `InternalEventReader`
 * builds its event source with `UnixInternalEventSource::new().ok()`, so
 * whatever the kernel said is dropped on the floor and only resurfaces as that
 * one string when `poll()` finds `source == None`.
 *
 * This program makes the same calls in the same order, and prints errno for
 * each. Build target: crossterm 0.28 + mio 1.0 (the mio event source, which is
 * what the PowerPC build resolves - there is no `filedescriptor` crate in the
 * dependency graph).
 *
 *   crossterm  tty_fd()                     -> isatty(0), else open /dev/tty
 *   mio        Poll::new()                  -> kqueue() + FD_CLOEXEC
 *   mio        registry.register(tty)       -> kevent EV_ADD|EV_CLEAR|EV_RECEIPT
 *   sig-hook   Signals::new([SIGWINCH])     -> socketpair + O_NONBLOCK + sigaction
 *   mio        registry.register(signals)   -> kevent on the read end
 *
 * Must run on a terminal to be meaningful: `ssh -tt`, or at the console.
 *
 *   gcc -o /tmp/kqueue-tty kqueue-tty.c && /tmp/kqueue-tty
 */

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/event.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

static void report(const char *what, long rc)
{
	if (rc < 0)
		printf("  %-34s = %-6ld FAIL errno=%d (%s)\n", what, rc, errno,
		       strerror(errno));
	else
		printf("  %-34s = %-6ld ok\n", what, rc);
}

/* mio registers with EV_RECEIPT, which forces EV_ERROR on every returned
 * change with data == 0 for success. mio then scans the returned events and
 * fails the registration if any carries a non-zero data. Report both halves:
 * the kevent() return *and* the per-change error mio would actually read. */
static void register_fd(int kq, int fd, const char *label)
{
	struct kevent ev;
	int rc;

	EV_SET(&ev, fd, EVFILT_READ, EV_ADD | EV_CLEAR | EV_RECEIPT, 0, 0, NULL);
	errno = 0;
	rc = kevent(kq, &ev, 1, &ev, 1, NULL);
	printf("  kevent(%-14s EV_RECEIPT) = %-3d errno=%-3d flags=0x%04x data=%ld%s\n",
	       label, rc, errno, ev.flags, (long)ev.data,
	       (rc >= 0 && (long)ev.data == 0) ? "  <- mio sees success"
					       : "  <- mio sees FAILURE");

	/* Same registration without EV_RECEIPT: if this one works and the
	 * receipt form does not, the flag is the whole problem. */
	EV_SET(&ev, fd, EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, NULL);
	errno = 0;
	rc = kevent(kq, &ev, 1, NULL, 0, NULL);
	printf("  kevent(%-14s plain     ) = %-3d errno=%-3d (%s)\n", label, rc,
	       errno, rc < 0 ? strerror(errno) : "ok");
}

int main(void)
{
	int tty, kq, rc, sp[2];
	struct sigaction sa;

	printf("== crossterm tty_fd() ==\n");
	printf("  isatty(0)                          = %d\n", isatty(0));
	if (isatty(0)) {
		tty = 0;
		printf("  using STDIN_FILENO\n");
	} else {
		tty = open("/dev/tty", O_RDWR);
		report("open(/dev/tty, O_RDWR)", tty);
		if (tty < 0)
			return 1;
	}

	printf("== mio Poll::new() ==\n");
	errno = 0;
	kq = kqueue();
	report("kqueue()", kq);
	if (kq < 0)
		return 1;
	errno = 0;
	rc = fcntl(kq, F_SETFD, FD_CLOEXEC);
	report("fcntl(kq, F_SETFD, FD_CLOEXEC)", rc);

	printf("== mio register(tty, READABLE) ==\n");
	register_fd(kq, tty, "tty");

	printf("== signal-hook Signals::new([SIGWINCH]) ==\n");
	errno = 0;
	rc = socketpair(AF_UNIX, SOCK_STREAM, 0, sp);
	report("socketpair(AF_UNIX, SOCK_STREAM)", rc);
	if (rc == 0) {
		errno = 0;
		report("fcntl(sp[0], F_SETFL, O_NONBLOCK)",
		       fcntl(sp[0], F_SETFL, O_NONBLOCK));
		errno = 0;
		report("fcntl(sp[1], F_SETFL, O_NONBLOCK)",
		       fcntl(sp[1], F_SETFL, O_NONBLOCK));
		/* signal-hook probes send() first to decide write vs send. */
		errno = 0;
		rc = send(sp[1], "", 0, 0);
		printf("  send(sp[1], 0 bytes)               = %-3d errno=%d (%s)\n",
		       rc, errno, rc < 0 ? strerror(errno) : "ok");

		memset(&sa, 0, sizeof(sa));
		sa.sa_handler = SIG_IGN;
		errno = 0;
		report("sigaction(SIGWINCH)", sigaction(SIGWINCH, &sa, NULL));

		printf("== mio register(signal pipe, READABLE) ==\n");
		register_fd(kq, sp[0], "signal-pipe");
	}

	/* If kqueue refuses the tty, the alternative is crossterm's `use-dev-tty`
	 * event source, which polls instead. That is only a fix if poll(2) works
	 * on a tty here - old macOS was notorious for poll() misbehaving on
	 * character devices, which is why so much software preferred select(). */
	printf("== crossterm use-dev-tty: poll(2) on the tty ==\n");
	{
		struct pollfd pfd;
		pfd.fd = tty;
		pfd.events = POLLIN;
		pfd.revents = 0;
		errno = 0;
		rc = poll(&pfd, 1, 120); /* expect 0 (timeout, no keys typed) */
		printf("  poll(tty, POLLIN, 120ms)           = %-3d errno=%-3d revents=0x%04x%s\n",
		       rc, errno, pfd.revents,
		       (rc < 0 || (pfd.revents & POLLNVAL)) ? "  <- BROKEN"
							    : "  <- usable");
	}

	printf("== select(2) on the tty (fallback of last resort) ==\n");
	{
		fd_set rfds;
		struct timeval tv;
		FD_ZERO(&rfds);
		FD_SET(tty, &rfds);
		tv.tv_sec = 0;
		tv.tv_usec = 120000;
		errno = 0;
		rc = select(tty + 1, &rfds, NULL, NULL, &tv);
		printf("  select(tty, 120ms)                 = %-3d errno=%-3d (%s)\n",
		       rc, errno, rc < 0 ? strerror(errno) : "usable");
	}

	printf("\ndone\n");
	return 0;
}
