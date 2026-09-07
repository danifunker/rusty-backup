/*
 * sol9-compat.c -- the libc entry points the rusty-backup engine references
 * that Solaris 9 does not export.
 *
 * Deliberately tiny, and deliberately here rather than in ../src: the engine
 * is shared with every other build, and a target this old should pay for its
 * own gaps. Same contract as rb-cli-ppc/shim/ppc-compat.c.
 *
 * Built and linked by scripts/build-sol9.sh (see SOL9_SHIM).
 */

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/sockio.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <stddef.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <dlfcn.h>
#include <stdarg.h>

/*
 * `getifaddrs` / `freeifaddrs` -- referenced by remote/service.rs's
 * local_ipv4_addrs(), which is gated on `cfg(unix)` and so is compiled for
 * this target.
 *
 * Both arrived in Solaris 11; nothing in Solaris 9's libc, libsocket or libnsl
 * defines them. The libc *crate* declares them for solarish all the same
 * (libc/src/unix/solarish/mod.rs), so without this file the engine compiles
 * clean and dies at the final link -- after the whole transpile.
 *
 * SIOCGLIFNUM / SIOCGLIFCONF is what Solaris used before getifaddrs existed,
 * and is still the documented way to enumerate interfaces here.
 *
 * IPv4 only: the caller filters on AF_INET and reads a sockaddr_in, so
 * enumerating AF_INET6 would only build nodes it discards.
 */

/*
 * Solaris' <net/if.h> defines `ifa_dstaddr` as a macro for its own kernel-side
 * `struct ifaddr`, which rewrites our field name into `ifa_ifu.ifu_dstaddr`
 * and will not compile. Nothing here wants the macro.
 */
#undef ifa_dstaddr
#undef ifa_broadaddr

/* Solaris 9 has no <ifaddrs.h>. This must match libc's solarish definition. */
struct ifaddrs {
	struct ifaddrs *ifa_next;
	char *ifa_name;
	unsigned long ifa_flags;
	struct sockaddr *ifa_addr;
	struct sockaddr *ifa_netmask;
	struct sockaddr *ifa_dstaddr;
	void *ifa_data;
};

/*
 * The layout is the contract with the libc crate, and a disagreement would be
 * silent -- the caller would read ifa_addr from the wrong offset. Seven
 * pointer-sized fields on LP64: 56 bytes. Measured, not assumed (the PowerPC
 * port's lesson).
 */
typedef char rb_assert_ifaddrs[(sizeof(struct ifaddrs) == 56) ? 1 : -1];

/*
 * One allocation per interface, with the strings and sockaddrs it points at
 * inside it, so freeifaddrs is a plain walk-and-free. `ifa` is first so the
 * node address and the struct address are the same pointer.
 */
struct rb_ifa_node {
	struct ifaddrs ifa;
	char name[LIFNAMSIZ];
	struct sockaddr_storage addr;
	struct sockaddr_storage netmask;
};

void freeifaddrs(struct ifaddrs *ifap)
{
	struct ifaddrs *cur = ifap;

	while (cur != NULL) {
		struct ifaddrs *next = cur->ifa_next;
		free(cur);
		cur = next;
	}
}

int getifaddrs(struct ifaddrs **ifap)
{
	struct lifnum ln;
	struct lifconf lc;
	struct lifreq *lifr;
	struct ifaddrs *head = NULL, *tail = NULL;
	char *buf = NULL;
	size_t bufsize;
	int s, i, count, saved_errno;

	if (ifap == NULL) {
		errno = EINVAL;
		return -1;
	}
	*ifap = NULL;

	s = socket(AF_INET, SOCK_DGRAM, 0);
	if (s < 0)
		return -1;

	memset(&ln, 0, sizeof(ln));
	ln.lifn_family = AF_INET;
	ln.lifn_flags = 0;
	if (ioctl(s, SIOCGLIFNUM, &ln) < 0)
		goto fail;
	if (ln.lifn_count <= 0) {
		/* No interfaces is a legitimate answer, not an error. */
		(void)close(s);
		return 0;
	}

	/*
	 * The count can grow between the two ioctls (an interface plumbed in
	 * the gap), so ask for headroom; SIOCGLIFCONF reports what it filled.
	 */
	bufsize = (size_t)(ln.lifn_count + 4) * sizeof(struct lifreq);
	buf = malloc(bufsize);
	if (buf == NULL)
		goto fail;
	memset(buf, 0, bufsize);

	memset(&lc, 0, sizeof(lc));
	lc.lifc_family = AF_INET;
	lc.lifc_flags = 0;
	lc.lifc_len = (int)bufsize;
	lc.lifc_buf = buf;
	if (ioctl(s, SIOCGLIFCONF, &lc) < 0)
		goto fail;

	lifr = (struct lifreq *)lc.lifc_buf;
	count = lc.lifc_len / (int)sizeof(struct lifreq);

	for (i = 0; i < count; i++) {
		struct rb_ifa_node *node;
		struct lifreq req;

		node = malloc(sizeof(*node));
		if (node == NULL)
			goto fail;
		memset(node, 0, sizeof(*node));

		(void)strncpy(node->name, lifr[i].lifr_name, LIFNAMSIZ - 1);
		(void)memcpy(&node->addr, &lifr[i].lifr_addr, sizeof(node->addr));

		node->ifa.ifa_name = node->name;
		node->ifa.ifa_addr = (struct sockaddr *)&node->addr;

		/*
		 * Flags and netmask each need their own ioctl, keyed by name.
		 * A failure here is not fatal: the address is the part the
		 * caller uses, so leave the field zeroed and keep the entry.
		 */
		memset(&req, 0, sizeof(req));
		(void)strncpy(req.lifr_name, lifr[i].lifr_name, LIFNAMSIZ - 1);
		if (ioctl(s, SIOCGLIFFLAGS, &req) == 0)
			node->ifa.ifa_flags = (unsigned long)req.lifr_flags;

		memset(&req, 0, sizeof(req));
		(void)strncpy(req.lifr_name, lifr[i].lifr_name, LIFNAMSIZ - 1);
		if (ioctl(s, SIOCGLIFNETMASK, &req) == 0) {
			(void)memcpy(&node->netmask, &req.lifr_addr,
			    sizeof(node->netmask));
			node->ifa.ifa_netmask =
			    (struct sockaddr *)&node->netmask;
		}

		if (head == NULL)
			head = &node->ifa;
		else
			tail->ifa_next = &node->ifa;
		tail = &node->ifa;
	}

	free(buf);
	(void)close(s);
	*ifap = head;
	return 0;

fail:
	saved_errno = errno;
	freeifaddrs(head);
	free(buf);
	(void)close(s);
	errno = saved_errno;
	return -1;
}

/*
 * ===========================================================================
 * Part 2: the openat(2) family, getrandom(2), and the flag-taking fd calls.
 *
 * Fourteen more entry points the final link asked for, every one of them
 * probed absent from this sysroot's libc rather than assumed. They arrive
 * from four directions:
 *
 *   filetime    utimensat            fs/fork_export.rs -- LIVE
 *   mio         pipe2                crossterm's event source -- LIVE
 *   getrandom   getrandom            tempfile names, zip AES salt -- LIVE
 *   nix/rustix  everything else      paths rusty-backup never runs
 *
 * The dead ones still have to resolve. A stub that lies would be worse than
 * the link error it replaces, so each is either a faithful emulation or an
 * honest ENOSYS -- nothing here quietly does the wrong thing.
 * ===========================================================================
 */

/* Solaris 9 has none of these; the values are Solaris 11's, and the libc crate's. */
#ifndef O_CLOEXEC
#define O_CLOEXEC	0x800000
#endif
#ifndef AT_EACCESS
#define AT_EACCESS	0x4
#endif
#ifndef AT_SYMLINK_FOLLOW
#define AT_SYMLINK_FOLLOW 0x2000
#endif
#ifndef UTIME_NOW
#define UTIME_NOW	(-1L)
#endif
#ifndef UTIME_OMIT
#define UTIME_OMIT	(-2L)
#endif
#ifndef GRND_NONBLOCK
#define GRND_NONBLOCK	0x0001
#endif
#ifndef GRND_RANDOM
#define GRND_RANDOM	0x0002
#endif

/*
 * Solaris spells AT_FDCWD 0xffd19553, which the preprocessor types as unsigned;
 * comparing it against an int fd is correct by conversion but warns. Name the
 * int form once -- it is what the caller passes (libc's AT_FDCWD is a c_int).
 */
#define RB_AT_FDCWD	((int)AT_FDCWD)

/*
 * The *at() calls take a directory fd Solaris 9 cannot honour. Its only
 * at-family primitive is openat(2) -- present since 9 for extended attributes,
 * and the one symbol of this set the sysroot does define -- and openat alone
 * cannot express a directory-relative mkdir, link, chmod or stat. The
 * alternative is a save-cwd/fchdir/restore dance that races every other thread
 * in the process, which is not worth doing silently for code paths that never
 * run here.
 *
 * So resolve the two cases that need no directory fd at all -- AT_FDCWD, and
 * an absolute path, which ignores the fd by definition -- and refuse the rest.
 */
static int rb_at_plain(int fd, const char *path)
{
	if (path == NULL) {
		errno = EINVAL;
		return 0;
	}
	if (fd == RB_AT_FDCWD || path[0] == '/')
		return 1;
	errno = ENOSYS;
	return 0;
}

/* O_NONBLOCK and O_CLOEXEC after the fact, for the *2 / *4 variants. */
static int rb_set_fd_flags(int fd, int flags)
{
	int cur;

	if (flags & O_NONBLOCK) {
		cur = fcntl(fd, F_GETFL, 0);
		if (cur < 0 || fcntl(fd, F_SETFL, cur | O_NONBLOCK) < 0)
			return -1;
	}
	if (flags & O_CLOEXEC) {
		cur = fcntl(fd, F_GETFD, 0);
		if (cur < 0 || fcntl(fd, F_SETFD, cur | FD_CLOEXEC) < 0)
			return -1;
	}
	return 0;
}

/*
 * `getrandom` -- Solaris 11.3. Nine has only the CPRNG devices, which it does
 * have: <sys/random.h> is in this sysroot, and /dev/random and /dev/urandom
 * have been standard since 9 (8 needed the SUNWski patch).
 *
 * The fd is opened per call rather than cached. Caching it safely would need
 * pthread_once, and both call sites are cold -- a temp-file name and an AES
 * salt, not a stream cipher.
 */
ssize_t getrandom(void *buf, size_t buflen, unsigned int flags)
{
	const char *dev;
	unsigned char *p = (unsigned char *)buf;
	size_t got = 0;
	int fd, oflags, saved_errno;

	if (buf == NULL && buflen != 0) {
		errno = EFAULT;
		return -1;
	}
	if (buflen == 0)
		return 0;

	dev = (flags & GRND_RANDOM) ? "/dev/random" : "/dev/urandom";
	oflags = O_RDONLY;
	if (flags & GRND_NONBLOCK)
		oflags |= O_NONBLOCK;

	fd = open(dev, oflags);
	if (fd < 0)
		return -1;

	while (got < buflen) {
		ssize_t n = read(fd, p + got, buflen - got);

		if (n > 0) {
			got += (size_t)n;
			continue;
		}
		if (n == 0) {
			/* A CPRNG device should never EOF; do not spin on it. */
			errno = EIO;
			break;
		}
		if (errno == EINTR)
			continue;
		break;
	}

	saved_errno = errno;
	(void)close(fd);
	if (got == 0) {
		errno = saved_errno;
		return -1;
	}
	return (ssize_t)got;
}

/*
 * `utimensat` -- Solaris 10, and the one live *at() call here: filetime routes
 * solaris through utimensat(AT_FDCWD, ...) and fs/fork_export.rs calls it.
 *
 * utimes(2) is the Solaris 9 equivalent and takes microseconds, so the
 * nanosecond field is truncated -- which costs nothing, because the caller
 * builds its stamps with FileTime::from_unix_time(secs, 0).
 *
 * UTIME_OMIT means "leave this one alone" and utimes() cannot say that, so the
 * existing value is read back with stat() and rewritten. AT_SYMLINK_NOFOLLOW
 * has no answer at all -- Solaris 9 has no lutimes(3C) -- so it is refused
 * rather than quietly stamping the link's target instead.
 */
int utimensat(int dirfd, const char *path, const struct timespec times[2],
    int flag)
{
	struct timeval tv[2];
	struct stat st;
	int i;

	if (!rb_at_plain(dirfd, path))
		return -1;
	if (flag & AT_SYMLINK_NOFOLLOW) {
		errno = ENOSYS;
		return -1;
	}
	if (times == NULL)
		return utimes((char *)path, NULL);

	if (times[0].tv_nsec == UTIME_OMIT || times[1].tv_nsec == UTIME_OMIT) {
		if (stat(path, &st) < 0)
			return -1;
	}

	for (i = 0; i < 2; i++) {
		long nsec = times[i].tv_nsec;

		if (nsec == UTIME_OMIT) {
			tv[i].tv_sec = (i == 0) ? st.st_atime : st.st_mtime;
			tv[i].tv_usec = 0;
		} else if (nsec == UTIME_NOW) {
			struct timeval now;

			if (gettimeofday(&now, NULL) < 0)
				return -1;
			tv[i] = now;
		} else {
			if (nsec < 0 || nsec > 999999999L) {
				errno = EINVAL;
				return -1;
			}
			tv[i].tv_sec = times[i].tv_sec;
			tv[i].tv_usec = (suseconds_t)(nsec / 1000);
		}
	}
	return utimes((char *)path, tv);
}

/*
 * `dirfd` -- Solaris 10. Nine's DIR is a plain struct whose first member is the
 * descriptor under either definition <dirent.h> selects (dd_fd bare, d_fd
 * under _POSIX_C_SOURCE); this file compiles as the former. Asserted rather
 * than trusted, because a wrong offset returns a plausible-looking integer.
 */
typedef char rb_assert_dirfd[(offsetof(DIR, dd_fd) == 0) ? 1 : -1];

int dirfd(DIR *dirp)
{
	if (dirp == NULL) {
		errno = EINVAL;
		return -1;
	}
	return dirp->dd_fd;
}

/* ---- the rest of the *at() family: nix and rustix dead code -------------- */

int faccessat(int fd, const char *path, int amode, int flag)
{
	if (!rb_at_plain(fd, path))
		return -1;
	if (flag & AT_EACCESS) {
		/* No eaccess(3C) on Solaris 9; real- vs effective-uid differ. */
		errno = ENOSYS;
		return -1;
	}
	return access(path, amode);
}

int fchmodat(int fd, const char *path, mode_t mode, int flag)
{
	if (!rb_at_plain(fd, path))
		return -1;
	if (flag & AT_SYMLINK_NOFOLLOW) {
		/* No lchmod(3C); permissions on the link itself are unreachable. */
		errno = ENOSYS;
		return -1;
	}
	return chmod(path, mode);
}

/*
 * Solaris 9's link(2) follows a symbolic link in path1, so it can express
 * AT_SYMLINK_FOLLOW and cannot express its absence. Refusing flag 0 rather
 * than silently giving it follow semantics is the whole point of this file.
 */
int linkat(int ofd, const char *opath, int nfd, const char *npath, int flag)
{
	if (!rb_at_plain(ofd, opath) || !rb_at_plain(nfd, npath))
		return -1;
	if ((flag & AT_SYMLINK_FOLLOW) == 0) {
		errno = ENOSYS;
		return -1;
	}
	return link(opath, npath);
}

int mkdirat(int fd, const char *path, mode_t mode)
{
	if (!rb_at_plain(fd, path))
		return -1;
	return mkdir(path, mode);
}

int mkfifoat(int fd, const char *path, mode_t mode)
{
	if (!rb_at_plain(fd, path))
		return -1;
	return mkfifo(path, mode);
}

int mknodat(int fd, const char *path, mode_t mode, dev_t dev)
{
	if (!rb_at_plain(fd, path))
		return -1;
	return mknod(path, mode, dev);
}

ssize_t readlinkat(int fd, const char *path, char *buf, size_t bufsiz)
{
	if (!rb_at_plain(fd, path))
		return -1;
	return readlink(path, buf, bufsiz);
}

/* Note the argument order: the directory fd is the *second* parameter here. */
int symlinkat(const char *target, int nfd, const char *linkpath)
{
	if (!rb_at_plain(nfd, linkpath))
		return -1;
	return symlink(target, linkpath);
}

/* ---- the flag-taking fd calls ------------------------------------------- */

int accept4(int fd, struct sockaddr *addr, socklen_t *addrlen, int flags)
{
	int s, saved_errno;

	s = accept(fd, addr, addrlen);
	if (s < 0)
		return -1;
	if (rb_set_fd_flags(s, flags) < 0) {
		saved_errno = errno;
		(void)close(s);
		errno = saved_errno;
		return -1;
	}
	return s;
}

/*
 * dup3 differs from dup2 in exactly two ways, and both matter: equal fds are
 * an error rather than a no-op, and O_CLOEXEC is the only flag it accepts.
 */
int dup3(int src, int dst, int flags)
{
	int cur, saved_errno;

	if (src == dst || (flags & ~O_CLOEXEC) != 0) {
		errno = EINVAL;
		return -1;
	}
	if (dup2(src, dst) < 0)
		return -1;
	if (flags & O_CLOEXEC) {
		cur = fcntl(dst, F_GETFD, 0);
		if (cur < 0 || fcntl(dst, F_SETFD, cur | FD_CLOEXEC) < 0) {
			saved_errno = errno;
			(void)close(dst);
			errno = saved_errno;
			return -1;
		}
	}
	return dst;
}

/*
 * `pipe2` -- live on every TUI wake-up. mio's Waker is a self-pipe wherever
 * there is no eventfd, and crossterm 0.28's event source is built on mio.
 */
int pipe2(int fildes[2], int flags)
{
	int saved_errno;

	if (fildes == NULL) {
		errno = EFAULT;
		return -1;
	}
	if (pipe(fildes) < 0)
		return -1;
	if (rb_set_fd_flags(fildes[0], flags) < 0 ||
	    rb_set_fd_flags(fildes[1], flags) < 0) {
		saved_errno = errno;
		(void)close(fildes[0]);
		(void)close(fildes[1]);
		errno = saved_errno;
		return -1;
	}
	return 0;
}

/*
 * `fcntl` interposer, for F_DUPFD_CLOEXEC only.
 *
 * Rust's File::try_clone() is fcntl(fd, F_DUPFD_CLOEXEC, 0). libc spells that
 * 47 for solaris, but the command arrived in Solaris 10 -- nine's fcntl knows
 * only F_DUPFD (0) and answers EINVAL. The call lives inside libstd, which
 * mrustc built from rustc's own vendored libc, so it cannot be reached by
 * patching this crate's dependencies; interposing here is what is left.
 *
 * Found by running `rb-cli backup` on the Blade: every backup failed with
 * "failed to clone local source handle: Invalid argument (os error 22)".
 * Nothing at link time hints at it -- the symbol exists, only the command is
 * too new -- so this is a runtime-only gap of exactly the kind the parity
 * gates exist to catch.
 *
 * Everything other than F_DUPFD_CLOEXEC is delegated to the real fcntl. The
 * third argument is read as a void* and passed through: fcntl's variants take
 * an int or a pointer, both of which arrive in the same argument register on
 * SPARC V9, and commands taking no third argument ignore it.
 */
#define RB_F_DUPFD_CLOEXEC 47

int fcntl(int fd, int cmd, ...)
{
	static int (*real_fcntl)(int, int, ...);
	va_list ap;
	void *arg;
	int rc;

	if (real_fcntl == NULL) {
		real_fcntl = (int (*)(int, int, ...))dlsym(RTLD_NEXT, "fcntl");
		if (real_fcntl == NULL) {
			errno = ENOSYS;
			return -1;
		}
	}

	va_start(ap, cmd);
	arg = va_arg(ap, void *);
	va_end(ap);

	if (cmd != RB_F_DUPFD_CLOEXEC) {
		return real_fcntl(fd, cmd, arg);
	}

	/* Duplicate, then set the close-on-exec flag the atomic command would
	 * have set. Not atomic against a concurrent exec, which is the price
	 * of the command not existing. */
	rc = real_fcntl(fd, F_DUPFD, arg);
	if (rc < 0) {
		return -1;
	}
	if (real_fcntl(rc, F_SETFD, (void *)(long)FD_CLOEXEC) < 0) {
		int saved = errno;
		(void)close(rc);
		errno = saved;
		return -1;
	}
	return rc;
}
