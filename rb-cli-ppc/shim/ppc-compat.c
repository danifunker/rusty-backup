/*
 * ppc-compat.c -- the handful of libc/libm entry points that Rust's libstd
 * references but Mac OS X 10.4/10.5 on PowerPC does not export.
 *
 * This is deliberately tiny. Anything broader (device enumeration, raw disk
 * I/O) belongs in the hand-written platform shell, not here; this file exists
 * only so the *standard library* links. Each entry documents which SDK first
 * shipped the real function, so entries can be dropped if the floor ever moves.
 *
 * Built and linked by scripts/ppc-cc-remote.py (see PPC_SHIM).
 */

#include <dlfcn.h>
#include <errno.h>
#include <math.h>
#include <poll.h>
#include <stdarg.h>
#include <sys/fcntl.h>
#include <sys/ioctl.h>
#include <sys/select.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <unistd.h>

/*
 * Leopard's <math.h> only declares `lgamma_r` under _REENTRANT-ish feature
 * macros that this translation unit does not opt into, so declare it here
 * rather than perturb the feature-macro state for the whole file.
 */
extern double lgamma_r(double, int *);

/*
 * `lgammaf_r` -- referenced by `core`/`std`'s `f32::ln_gamma`.
 *
 * Leopard's libm exports `lgamma`, `lgammaf` and the reentrant `lgamma_r`, but
 * not the float reentrant form (it arrived later). Widening to double and back
 * is what the platforms that do ship it effectively do for this function, and
 * `ln_gamma` is an unstable API that nothing in the engine calls -- this exists
 * purely to satisfy the linker.
 */
float lgammaf_r(float x, int *signgamp)
{
    return (float)lgamma_r((double)x, signgamp);
}

/*
 * `fcntl` -- intercepted for one command, `F_DUPFD_CLOEXEC` (67), which
 * arrived in Mac OS X 10.7. Everything else is forwarded untouched.
 *
 * This is the one entry here that *overrides* a function the OS does export,
 * rather than supplying a missing one, so it earns an explanation.
 * `File::try_clone` is `fcntl(fd, F_DUPFD_CLOEXEC, 0)` in Rust 1.74's
 * `sys/unix/fd.rs`, with no fallback for kernels that lack the command - and
 * on Leopard it fails with ENOTTY, so *every* `try_clone` fails:
 *
 *     error: backup failed: failed to clone local source handle:
 *            Inappropriate ioctl for device (os error 25)
 *
 * Measured on 10.5.8: F_DUPFD_CLOEXEC -> -1 ENOTTY, while plain F_DUPFD
 * followed by ioctl(FIOCLEX) succeeds - which is exactly what the atomic
 * command was introduced to collapse into one step. The window between the
 * two calls only matters if another thread execs in between, which is a
 * trade this target has no way to avoid.
 *
 * The principled fix is a patch to std's `duplicate()`, and if the PowerPC
 * stdlib is ever rebuilt for another reason it should go in there and this
 * entry should be dropped. It lives here because patching std invalidates
 * every crate downstream of it - the 797 MB engine included - and this is a
 * link-line change instead.
 */
#ifndef F_DUPFD_CLOEXEC
#define F_DUPFD_CLOEXEC 67
#endif

/*
 * The asm label is load-bearing, and so is the odd C name.
 *
 * Leopard's <sys/fcntl.h> aliases `fcntl` to the conformance variant
 * `_fcntl$UNIX2003`, while libstd - compiled from mrustc's C, without those
 * headers' feature macros - calls plain `_fcntl`. Defining `fcntl` the obvious
 * way therefore exports the decorated name, the two never meet, and the
 * override silently does nothing: the failure looks identical to not having
 * written this at all. Nor can a second `__asm__` label on `fcntl` fix it -
 * the header's declaration comes first and its label is the one that sticks.
 * So the definition gets a name of its own and asks for `_fcntl` explicitly.
 */
int rb_compat_fcntl(int fd, int cmd, ...) __asm__("_fcntl");

int rb_compat_fcntl(int fd, int cmd, ...)
{
    static int (*real_fcntl)(int, int, ...);
    va_list ap;
    void *arg;
    int nfd, saved;

    va_start(ap, cmd);
    arg = va_arg(ap, void *);
    va_end(ap);

    if (real_fcntl == 0) {
        real_fcntl = (int (*)(int, int, ...))dlsym(RTLD_NEXT, "fcntl");
    }

    if (cmd == F_DUPFD_CLOEXEC) {
        if (real_fcntl != 0) {
            nfd = real_fcntl(fd, cmd, arg);
        } else {
            nfd = syscall(SYS_fcntl, fd, cmd, arg);
        }
        if (nfd >= 0) {
            return nfd;
        }
        if (errno != ENOTTY && errno != EINVAL) {
            return -1;
        }
        /* The command is unsupported here; do it in two steps instead. */
        nfd = (real_fcntl != 0) ? real_fcntl(fd, F_DUPFD, arg)
                                : (int)syscall(SYS_fcntl, fd, F_DUPFD, arg);
        if (nfd < 0) {
            return -1;
        }
        if (ioctl(nfd, FIOCLEX) < 0) {
            saved = errno;
            close(nfd);
            errno = saved;
            return -1;
        }
        return nfd;
    }

    if (real_fcntl != 0) {
        return real_fcntl(fd, cmd, arg);
    }
    return (int)syscall(SYS_fcntl, fd, cmd, arg);
}

/*
 * `poll` -- intercepted because Leopard's does not work on character devices.
 *
 * That is a broad claim, so it is measured rather than asserted:
 * `probe/poll-devices.c` asks poll, select and kqueue about the same
 * descriptor, across kinds. On 10.5.8:
 *
 *   descriptor kind                        poll        kqueue     select
 *   stdin/tty, /dev/tty, pty master+slave  POLLNVAL    ENOTSUP    correct
 *   /dev/null, /dev/zero, /dev/random      POLLNVAL    ENOTSUP    correct
 *   regular file, fifo, unix socket        correct     ok         correct
 *
 * Every S_ISCHR descriptor, nothing else. Not an artifact of how the fd was
 * obtained - a pty created in-process by openpty() fails identically - and not
 * a "nothing to read" confusion: a pty slave with a byte already waiting still
 * answers POLLNVAL rather than POLLIN. POSIX is explicit that an open
 * descriptor must never yield POLLNVAL, so this is the kernel, not the caller.
 *
 * This is the second override in this file rather than a missing symbol, and it
 * earns its place because it is not one crate's problem: anything that waits
 * for a keypress goes through poll. `rb-cli tui` dies with "Failed to
 * initialize input reader", and rustyline's line editor (`src/tty/unix.rs`)
 * waits the same way.
 *
 * kqueue is not an escape - the table above shows EVFILT_READ returning ENOTSUP
 * (45) on the same descriptors, which is what breaks crossterm's default (mio)
 * event source and is why the PowerPC build selects crossterm's `use-dev-tty`
 * source. That leaves select(2) as the only readiness primitive Leopard offers
 * for a terminal, so poll is reimplemented on top of it.
 *
 * Conservative on purpose: the real poll runs first and its answer is kept
 * unless it claims POLLNVAL for a descriptor that `F_GETFD` says is open. Only
 * then do we redo the wait with select. Sockets and pipes - where this poll
 * behaves - are therefore untouched, and the emulation's rougher edges (no
 * distinct POLLHUP; POLLPRI approximated by select's exceptfds) apply only to
 * the case that was already broken.
 */
static int rb_compat_poll_via_select(struct pollfd *fds, nfds_t nfds, int timeout)
{
    fd_set rd, wr, ex;
    struct timeval tv;
    struct timeval *ptv;
    int maxfd = -1;
    int rc;
    int ready = 0;
    nfds_t i;

    FD_ZERO(&rd);
    FD_ZERO(&wr);
    FD_ZERO(&ex);

    for (i = 0; i < nfds; i++) {
        fds[i].revents = 0;
        if (fds[i].fd < 0) {
            continue;
        }
        /* select cannot express a descriptor past the set's fixed width; say
         * so rather than smash the stack the way FD_SET would. */
        if (fds[i].fd >= FD_SETSIZE) {
            errno = EINVAL;
            return -1;
        }
        if (fds[i].events & POLLIN) {
            FD_SET(fds[i].fd, &rd);
        }
        if (fds[i].events & POLLOUT) {
            FD_SET(fds[i].fd, &wr);
        }
        FD_SET(fds[i].fd, &ex);
        if (fds[i].fd > maxfd) {
            maxfd = fds[i].fd;
        }
    }

    if (timeout < 0) {
        ptv = 0; /* block indefinitely */
    } else {
        tv.tv_sec = timeout / 1000;
        tv.tv_usec = (timeout % 1000) * 1000;
        ptv = &tv;
    }

    rc = select(maxfd + 1, &rd, &wr, &ex, ptv);
    if (rc <= 0) {
        return rc; /* timeout (0) or error (-1, errno already set) */
    }

    for (i = 0; i < nfds; i++) {
        if (fds[i].fd < 0) {
            continue;
        }
        if (FD_ISSET(fds[i].fd, &rd)) {
            fds[i].revents |= POLLIN;
        }
        if (FD_ISSET(fds[i].fd, &wr)) {
            fds[i].revents |= POLLOUT;
        }
        if (FD_ISSET(fds[i].fd, &ex)) {
            fds[i].revents |= POLLPRI;
        }
        if (fds[i].revents != 0) {
            ready++;
        }
    }
    return ready;
}

int rb_compat_poll(struct pollfd *fds, nfds_t nfds, int timeout) __asm__("_poll");

int rb_compat_poll(struct pollfd *fds, nfds_t nfds, int timeout)
{
    static int (*real_poll)(struct pollfd *, nfds_t, int);
    int rc;
    nfds_t i;

    if (real_poll == 0) {
        real_poll = (int (*)(struct pollfd *, nfds_t, int))dlsym(RTLD_NEXT, "poll");
    }
    if (real_poll == 0) {
        return rb_compat_poll_via_select(fds, nfds, timeout);
    }

    rc = real_poll(fds, nfds, timeout);
    if (rc <= 0) {
        return rc;
    }

    /* A POLLNVAL on a descriptor that is demonstrably open is the Leopard bug,
     * not the caller's mistake. Redo the whole wait with select; a genuinely
     * closed fd keeps poll's verdict. */
    for (i = 0; i < nfds; i++) {
        if ((fds[i].revents & POLLNVAL) && fds[i].fd >= 0
            && fcntl(fds[i].fd, F_GETFD) != -1) {
            return rb_compat_poll_via_select(fds, nfds, timeout);
        }
    }
    return rc;
}
