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

#include <dirent.h>
#include <dlfcn.h>
#include <errno.h>
#include <math.h>
#include <poll.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/fcntl.h>
#include <sys/ioctl.h>
#include <sys/resource.h>
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
 * Leopard's headers alias these to their `$UNIX2003` conformance variants,
 * which Tiger does not export - so a shim compiled here would itself fail to
 * bind on 10.4, which is a fine way to break the very compatibility layer
 * meant to provide it. Ask for the plain names, which both systems have.
 * (`nm -u` on the shim object is the check: no `$UNIX2003` may appear.)
 */
extern int rb_sys_close(int) __asm__("_close");
extern int rb_sys_select(int, fd_set *, fd_set *, fd_set *, struct timeval *)
	__asm__("_select");
extern char *rb_sys_realpath(const char *, char *) __asm__("_realpath");

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
 * `realpath$DARWIN_EXTSN` -- 10.5+. Rust's `libc` hardcodes this name with
 * `#[link_name]`, so `fs::canonicalize` binds it whatever the deployment target,
 * and Tiger exports only plain `realpath`. The difference that matters is the
 * NULL second argument: the EXTSN variant allocates, Tiger's requires a caller
 * buffer, and `canonicalize` passes NULL - so forwarding blindly would hand the
 * legacy implementation a null pointer to write through.
 */
char *rb_compat_realpath_extsn(const char *, char *) __asm__("_realpath$DARWIN_EXTSN");

char *rb_compat_realpath_extsn(const char *path, char *resolved)
{
    char *buf, *out;
    int saved;

    if (resolved)
        return rb_sys_realpath(path, resolved);
    if ((buf = malloc(PATH_MAX)) == 0) {
        errno = ENOMEM;
        return 0;
    }
    if ((out = rb_sys_realpath(path, buf)) == 0) {
        saved = errno;
        free(buf);
        errno = saved;
        return 0;
    }
    return out;
}

/*
 * `clock$UNIX2003` -- 10.5+. The two variants disagree about their unit, not
 * just their name: 10.5 defines CLOCKS_PER_SEC as 1000000 for the conformance
 * entry point, while Tiger's plain `clock` counts __DARWIN_CLK_TCK (100/sec).
 * Forwarding would under-report CPU time by 10000x, silently. Take the value
 * from getrusage instead, which is exact and means the same thing on both.
 */
clock_t rb_compat_clock_unix2003(void) __asm__("_clock$UNIX2003");

clock_t rb_compat_clock_unix2003(void)
{
    struct rusage ru;

    if (getrusage(RUSAGE_SELF, &ru) != 0)
        return (clock_t)-1;
    return (clock_t)((ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) * 1000000
                     + ru.ru_utime.tv_usec + ru.ru_stime.tv_usec);
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
            rb_sys_close(nfd);
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

/* =========================================================================
 * The `$INODE64` family: making one binary run on both 10.4 and 10.5.
 *
 * Tiger exports *no* `$INODE64` symbols. libstd binds nine of them
 * (`stat`, `fstat`, `lstat`, `statfs`, `fstatfs`, `getmntinfo`, `readdir`,
 * `readdir_r`, and `opendir$INODE64$UNIX2003`), so on 10.4 there is nothing
 * for them to resolve to.
 *
 * Rather than rebuild libc and the standard library with a 10.4 arch file -
 * which invalidates every crate downstream, the 797 MB engine included - each
 * of those symbols is *defined here* and dispatched at runtime:
 *
 *   Leopard: the real `$INODE64` entry point exists; forward to it.
 *   Tiger:   call the plain entry point, which fills the *legacy* struct, and
 *            convert that to the 64-bit-inode struct libc's Rust declarations
 *            describe.
 *
 * The lookup is `RTLD_NEXT`, never `RTLD_DEFAULT`: these definitions live in
 * the main executable, so a default-scope lookup would find *this* function
 * and recurse forever.
 *
 * Layouts measured by `probe/inode64-layout.c` on the machine, not read out of
 * a header by eye:
 *
 *            legacy (Tiger)                 64-bit-inode (what Rust expects)
 *   stat     96 B, st_ino @4 (4B)           108 B, st_ino @8 (8B), st_mode @4
 *   statfs   272 B, counts 4B, name 90 B    2168 B, counts 8B, name 1024 B
 *   dirent   264 B, d_namlen @7 (1B)        1048 B, d_namlen @18 (2B)
 * ========================================================================= */

/* The 64-bit-inode structs, written out explicitly so the layout cannot drift
 * with whichever feature macros this file is compiled under. The static
 * assertions below are the safety net if it ever does. */
struct rb_stat64 {
	dev_t st_dev;                      /* @0   */
	mode_t st_mode;                    /* @4   */
	nlink_t st_nlink;                  /* @6   */
	uint64_t st_ino;                   /* @8   */
	uid_t st_uid;                      /* @16  */
	gid_t st_gid;                      /* @20  */
	dev_t st_rdev;                     /* @24  */
	struct timespec st_atimespec;      /* @28  */
	struct timespec st_mtimespec;      /* @36  */
	struct timespec st_ctimespec;      /* @44  */
	struct timespec st_birthtimespec;  /* @52  */
	off_t st_size;                     /* @60  */
	blkcnt_t st_blocks;                /* @68  */
	blksize_t st_blksize;              /* @76  */
	uint32_t st_flags;                 /* @80  */
	uint32_t st_gen;                   /* @84  */
	int32_t st_lspare;                 /* @88  */
	int64_t st_qspare[2];              /* @92  */
};

struct rb_statfs64 {
	uint32_t f_bsize;                  /* @0    */
	int32_t f_iosize;                  /* @4    */
	uint64_t f_blocks;                 /* @8    */
	uint64_t f_bfree;                  /* @16   */
	uint64_t f_bavail;                 /* @24   */
	uint64_t f_files;                  /* @32   */
	uint64_t f_ffree;                  /* @40   */
	fsid_t f_fsid;                     /* @48   */
	uid_t f_owner;                     /* @56   */
	uint32_t f_type;                   /* @60   */
	uint32_t f_flags;                  /* @64   */
	uint32_t f_fssubtype;              /* @68   */
	char f_fstypename[16];             /* @72   */
	char f_mntonname[1024];            /* @88   */
	char f_mntfromname[1024];          /* @1112 */
	uint32_t f_reserved[8];            /* @2136 */
};

struct rb_dirent64 {
	uint64_t d_ino;                    /* @0  */
	uint64_t d_seekoff;                /* @8  */
	uint16_t d_reclen;                 /* @16 */
	uint16_t d_namlen;                 /* @18 */
	uint8_t d_type;                    /* @20 */
	char d_name[1024];                 /* @21 */
};

/* Compile-time guards. The left-hand sizes are this file's own structs; the
 * `struct stat` / `struct statfs` / `struct dirent` ones assert that the SDK
 * gave us the *legacy* shapes, i.e. that nothing has defined
 * _DARWIN_USE_64_BIT_INODE for this translation unit - which would silently
 * make every conversion below a no-op copy of the wrong layout. */
typedef char rb_assert_stat64[(sizeof(struct rb_stat64) == 108) ? 1 : -1];
typedef char rb_assert_statfs64[(sizeof(struct rb_statfs64) == 2168) ? 1 : -1];
typedef char rb_assert_dirent64[(sizeof(struct rb_dirent64) == 1048) ? 1 : -1];
typedef char rb_assert_stat_legacy[(sizeof(struct stat) == 96) ? 1 : -1];
typedef char rb_assert_statfs_legacy[(sizeof(struct statfs) == 272) ? 1 : -1];
typedef char rb_assert_dirent_legacy[(sizeof(struct dirent) == 264) ? 1 : -1];

/* Does this system have the 64-bit-inode entry points at all? Decided once,
 * from a symbol Leopard has and Tiger does not.
 *
 * `RB_COMPAT_FORCE_LEGACY=1` forces the Tiger answer on a system that has
 * them. That exists because the conversion code below is otherwise unreachable
 * on the only machine available to test it: Leopard always takes the forward
 * branch, so a bug in the legacy path would sit undetected until someone booted
 * 10.4. With the override, the same binary can be run both ways on one machine
 * and the results compared - which is how `probe/inode64-diff.c` checks the
 * conversions field by field. */
static int rb_have_inode64(void)
{
	static int cached = -1;
	if (cached < 0) {
		const char *force = getenv("RB_COMPAT_FORCE_LEGACY");
		if (force != 0 && force[0] == '1') {
			cached = 0;
		} else {
			/* Ask libSystem by name: RTLD_NEXT also finds the bundled legacy-support, which exports stat$INODE64 on 10.4. */
			void *sys = dlopen("/usr/lib/libSystem.B.dylib", RTLD_LAZY);
			cached = sys != 0 && dlsym(sys, "stat$INODE64") != 0;
		}
	}
	return cached;
}

static void rb_conv_stat(const struct stat *in, struct rb_stat64 *out)
{
	memset(out, 0, sizeof(*out));
	out->st_dev = in->st_dev;
	out->st_mode = in->st_mode;
	out->st_nlink = in->st_nlink;
	out->st_ino = (uint64_t)in->st_ino;
	out->st_uid = in->st_uid;
	out->st_gid = in->st_gid;
	out->st_rdev = in->st_rdev;
	out->st_atimespec = in->st_atimespec;
	out->st_mtimespec = in->st_mtimespec;
	out->st_ctimespec = in->st_ctimespec;
	/* Legacy has no birthtime. Leave it zeroed rather than inventing one:
	 * Rust surfaces it as `created()`, and a wrong date is worse than an
	 * epoch that reads as "unknown". */
	out->st_size = in->st_size;
	out->st_blocks = in->st_blocks;
	out->st_blksize = in->st_blksize;
	out->st_flags = in->st_flags;
	out->st_gen = in->st_gen;
}

static void rb_conv_statfs(const struct statfs *in, struct rb_statfs64 *out)
{
	memset(out, 0, sizeof(*out));
	out->f_bsize = in->f_bsize;
	out->f_iosize = in->f_iosize;
	out->f_blocks = in->f_blocks;
	out->f_bfree = in->f_bfree;
	out->f_bavail = in->f_bavail;
	out->f_files = in->f_files;
	out->f_ffree = in->f_ffree;
	out->f_fsid = in->f_fsid;
	out->f_owner = in->f_owner;
	out->f_type = in->f_type;
	out->f_flags = in->f_flags;
	/* The legacy string fields are shorter than the 64-bit ones (15/90 vs
	 * 16/1024), so copy by the *source* length and rely on the zeroed
	 * destination to terminate. */
	memcpy(out->f_fstypename, in->f_fstypename, sizeof(in->f_fstypename));
	memcpy(out->f_mntonname, in->f_mntonname, sizeof(in->f_mntonname));
	memcpy(out->f_mntfromname, in->f_mntfromname, sizeof(in->f_mntfromname));
}

static void rb_conv_dirent(const struct dirent *in, struct rb_dirent64 *out)
{
	memset(out, 0, sizeof(*out));
	out->d_ino = (uint64_t)in->d_ino;
	out->d_reclen = sizeof(struct rb_dirent64);
	out->d_namlen = in->d_namlen;
	out->d_type = in->d_type;
	memcpy(out->d_name, in->d_name, sizeof(in->d_name));
	out->d_name[sizeof(in->d_name)] = '\0';
}

int rb_compat_stat64(const char *, struct rb_stat64 *) __asm__("_stat$INODE64");
int rb_compat_lstat64(const char *, struct rb_stat64 *) __asm__("_lstat$INODE64");
int rb_compat_fstat64(int, struct rb_stat64 *) __asm__("_fstat$INODE64");
int rb_compat_statfs64(const char *, struct rb_statfs64 *) __asm__("_statfs$INODE64");
int rb_compat_fstatfs64(int, struct rb_statfs64 *) __asm__("_fstatfs$INODE64");

int rb_compat_stat64(const char *path, struct rb_stat64 *buf)
{
	static int (*real)(const char *, struct rb_stat64 *);
	struct stat legacy;

	if (rb_have_inode64()) {
		if (real == 0) {
			real = (int (*)(const char *, struct rb_stat64 *))dlsym(
				RTLD_NEXT, "stat$INODE64");
		}
		/* A missing symbol falls through to the conversion below rather than calling null. */
		if (real != 0)
			return real(path, buf);
	}
	if (stat(path, &legacy) != 0) {
		return -1;
	}
	rb_conv_stat(&legacy, buf);
	return 0;
}

int rb_compat_lstat64(const char *path, struct rb_stat64 *buf)
{
	static int (*real)(const char *, struct rb_stat64 *);
	struct stat legacy;

	if (rb_have_inode64()) {
		if (real == 0) {
			real = (int (*)(const char *, struct rb_stat64 *))dlsym(
				RTLD_NEXT, "lstat$INODE64");
		}
		/* A missing symbol falls through to the conversion below rather than calling null. */
		if (real != 0)
			return real(path, buf);
	}
	if (lstat(path, &legacy) != 0) {
		return -1;
	}
	rb_conv_stat(&legacy, buf);
	return 0;
}

int rb_compat_fstat64(int fd, struct rb_stat64 *buf)
{
	static int (*real)(int, struct rb_stat64 *);
	struct stat legacy;

	if (rb_have_inode64()) {
		if (real == 0) {
			real = (int (*)(int, struct rb_stat64 *))dlsym(RTLD_NEXT,
								      "fstat$INODE64");
		}
		/* A missing symbol falls through to the conversion below rather than calling null. */
		if (real != 0)
			return real(fd, buf);
	}
	if (fstat(fd, &legacy) != 0) {
		return -1;
	}
	rb_conv_stat(&legacy, buf);
	return 0;
}

int rb_compat_statfs64(const char *path, struct rb_statfs64 *buf)
{
	static int (*real)(const char *, struct rb_statfs64 *);
	struct statfs legacy;

	if (rb_have_inode64()) {
		if (real == 0) {
			real = (int (*)(const char *, struct rb_statfs64 *))dlsym(
				RTLD_NEXT, "statfs$INODE64");
		}
		/* A missing symbol falls through to the conversion below rather than calling null. */
		if (real != 0)
			return real(path, buf);
	}
	if (statfs(path, &legacy) != 0) {
		return -1;
	}
	rb_conv_statfs(&legacy, buf);
	return 0;
}

int rb_compat_fstatfs64(int fd, struct rb_statfs64 *buf)
{
	static int (*real)(int, struct rb_statfs64 *);
	struct statfs legacy;

	if (rb_have_inode64()) {
		if (real == 0) {
			real = (int (*)(int, struct rb_statfs64 *))dlsym(
				RTLD_NEXT, "fstatfs$INODE64");
		}
		/* A missing symbol falls through to the conversion below rather than calling null. */
		if (real != 0)
			return real(fd, buf);
	}
	if (fstatfs(fd, &legacy) != 0) {
		return -1;
	}
	rb_conv_statfs(&legacy, buf);
	return 0;
}

/*
 * `getmntinfo$INODE64` -- the one that returns an *array*, so the conversion
 * needs somewhere to put it. libc's contract is that the buffer belongs to the
 * library and the caller neither frees nor holds it across calls, so a single
 * grown-on-demand allocation matches the real thing's behaviour.
 */
int rb_compat_getmntinfo64(struct rb_statfs64 **, int) __asm__("_getmntinfo$INODE64");

int rb_compat_getmntinfo64(struct rb_statfs64 **mntbufp, int flags)
{
	static int (*real)(struct rb_statfs64 **, int);
	static struct rb_statfs64 *converted;
	static int converted_cap;
	struct statfs *legacy;
	int n, i;

	if (rb_have_inode64()) {
		if (real == 0) {
			real = (int (*)(struct rb_statfs64 **, int))dlsym(
				RTLD_NEXT, "getmntinfo$INODE64");
		}
		/* A missing symbol falls through to the conversion below rather than calling null. */
		if (real != 0)
			return real(mntbufp, flags);
	}

	n = getmntinfo(&legacy, flags);
	if (n <= 0) {
		return n;
	}
	if (n > converted_cap) {
		struct rb_statfs64 *grown =
			realloc(converted, (size_t)n * sizeof(*converted));
		if (grown == 0) {
			errno = ENOMEM;
			return 0; /* getmntinfo reports failure as 0 */
		}
		converted = grown;
		converted_cap = n;
	}
	for (i = 0; i < n; i++) {
		rb_conv_statfs(&legacy[i], &converted[i]);
	}
	*mntbufp = converted;
	return n;
}

/*
 * `readdir$INODE64` / `readdir_r$INODE64`.
 *
 * The non-reentrant form may return a pointer to storage it owns, which is
 * what the real one does too, so one static buffer is faithful. `readdir_r`
 * takes the caller's buffer and is genuinely reentrant.
 */
struct rb_dirent64 *rb_compat_readdir64(DIR *) __asm__("_readdir$INODE64");
int rb_compat_readdir64_r(DIR *, struct rb_dirent64 *, struct rb_dirent64 **)
	__asm__("_readdir_r$INODE64");

struct rb_dirent64 *rb_compat_readdir64(DIR *dirp)
{
	static struct rb_dirent64 *(*real)(DIR *);
	static struct rb_dirent64 converted;
	struct dirent *legacy;

	if (rb_have_inode64()) {
		if (real == 0) {
			real = (struct rb_dirent64 * (*)(DIR *))
				dlsym(RTLD_NEXT, "readdir$INODE64");
		}
		/* A missing symbol falls through to the conversion below rather than calling null. */
		if (real != 0)
			return real(dirp);
	}
	errno = 0;
	legacy = readdir(dirp);
	if (legacy == 0) {
		return 0;
	}
	rb_conv_dirent(legacy, &converted);
	return &converted;
}

int rb_compat_readdir64_r(DIR *dirp, struct rb_dirent64 *entry,
			  struct rb_dirent64 **result)
{
	static int (*real)(DIR *, struct rb_dirent64 *, struct rb_dirent64 **);
	struct dirent legacy;
	struct dirent *legacy_result;
	int rc;

	if (rb_have_inode64()) {
		if (real == 0) {
			real = (int (*)(DIR *, struct rb_dirent64 *,
					struct rb_dirent64 **))
				dlsym(RTLD_NEXT, "readdir_r$INODE64");
		}
		/* A missing symbol falls through to the conversion below rather than calling null. */
		if (real != 0)
			return real(dirp, entry, result);
	}
	rc = readdir_r(dirp, &legacy, &legacy_result);
	if (rc != 0) {
		return rc;
	}
	if (legacy_result == 0) {
		*result = 0;
		return 0;
	}
	rb_conv_dirent(&legacy, entry);
	*result = entry;
	return 0;
}

/*
 * `opendir` -- intercepted so it returns a DIR the 64-bit-inode `readdir` can
 * actually read.
 *
 * libstd links a crossed pair on this target, which `nm -u` shows plainly:
 *
 *     _opendir            (legacy)
 *     _readdir$INODE64    (64-bit inode)
 *
 * Those are two ABIs. A DIR opened by the legacy call is not one that
 * `readdir$INODE64` can walk, and the failure mode is silence: no error, no
 * garbage, just **zero entries**.
 *
 * It hid for a long time because it is filesystem-dependent. Measured by
 * `probe/opendir-abi.c` on 10.5.8:
 *
 *     /usr/lib   legacy pair -> 392 entries    matched pair -> 392 entries
 *     /dev       legacy pair ->   0 entries    matched pair -> 313 entries
 *
 * So HFS+ tolerates the crossed pair and devfs does not - which is why an
 * earlier check that `read_dir` "works" (it counted /usr/lib correctly) was
 * true and still missed this, and why the symptom that surfaced it was
 * `enumerate_devices` finding no disks rather than anything about directories.
 *
 * The matching call is `opendir$INODE64$UNIX2003`. The doubled suffix matters:
 * searching for a plain `opendir$INODE64` finds nothing and reads as "10.5 has
 * no 64-bit opendir", which is the wrong conclusion.
 *
 * `closedir` needs no equivalent - it has no `$INODE64` variant, so it is
 * version-agnostic. `rewinddir` / `seekdir` / `telldir` do have one each; libstd
 * does not reference them today, and they are left alone rather than
 * speculatively overridden.
 */
DIR *rb_compat_opendir(const char *path) __asm__("_opendir");

DIR *rb_compat_opendir(const char *path)
{
	static DIR *(*real)(const char *);

	/* Both lookups go through RTLD_NEXT: `_opendir` is *this* function, so
	 * a default-scope lookup - or simply calling `opendir()` and letting
	 * the header pick a name - can land back here and recurse.
	 *
	 * This *must* follow the same `rb_have_inode64()` decision the readdir
	 * entries make, or the pair is crossed again and directories read as
	 * empty. Choosing purely by "does the 64-bit symbol exist" is not
	 * enough: under RB_COMPAT_FORCE_LEGACY on a 10.5 machine the symbol
	 * still exists, so opendir took the 64-bit branch while readdir took
	 * the legacy one - and /dev came back empty exactly as it did before
	 * any of this was fixed. probe/inode64-diff.c caught that. */
	if (real == 0) {
		if (rb_have_inode64()) {
			real = (DIR * (*)(const char *))
				dlsym(RTLD_NEXT, "opendir$INODE64$UNIX2003");
		}
		if (real == 0) {
			/* Tiger: no 64-bit variant exists. The plain DIR is
			 * correct there, because this file's readdir$INODE64
			 * converts the legacy entries it yields. */
			real = (DIR * (*)(const char *))dlsym(RTLD_NEXT, "opendir");
		}
	}
	return real(path);
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

    rc = rb_sys_select(maxfd + 1, &rd, &wr, &ex, ptv);
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
            && rb_compat_fcntl(fds[i].fd, F_GETFD) != -1) {
            return rb_compat_poll_via_select(fds, nfds, timeout);
        }
    }
    return rc;
}
