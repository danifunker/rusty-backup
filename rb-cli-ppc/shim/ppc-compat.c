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
#include <stdarg.h>
#include <sys/fcntl.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
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
