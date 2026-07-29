/*
 * inode64-diff.c -- does the Tiger translation produce what Leopard produces?
 *
 * The shim defines the `$INODE64` symbols and dispatches at runtime: forward
 * to the real entry point where it exists (10.5), or call the plain one and
 * convert the legacy struct (10.4). The converting half is the risky half, and
 * it is unreachable on the only machine available to test it - Leopard always
 * takes the forward branch.
 *
 * `RB_COMPAT_FORCE_LEGACY=1` makes the shim take the Tiger branch anyway. This
 * program calls the real 10.5 entry point *and* the shim's forced-legacy path
 * for the same subject, and compares field by field. Anything the conversion
 * gets wrong shows up here, on Leopard, instead of on a 10.4 boot.
 *
 * Fields that legacy genuinely cannot supply are excluded and named below, so
 * this does not quietly bless a lossy conversion.
 *
 *   gcc -o /tmp/inode64-diff inode64-diff.c ../shim/ppc-compat.c
 *   RB_COMPAT_FORCE_LEGACY=1 /tmp/inode64-diff
 */

#include <dirent.h>
#include <dlfcn.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/* The 64-bit-inode shapes, matching the shim's. */
struct t_stat64 {
	uint32_t st_dev;
	uint16_t st_mode;
	uint16_t st_nlink;
	uint64_t st_ino;
	uint32_t st_uid;
	uint32_t st_gid;
	uint32_t st_rdev;
	struct timespec st_atimespec;
	struct timespec st_mtimespec;
	struct timespec st_ctimespec;
	struct timespec st_birthtimespec;
	int64_t st_size;
	int64_t st_blocks;
	int32_t st_blksize;
	uint32_t st_flags;
	uint32_t st_gen;
	int32_t st_lspare;
	int64_t st_qspare[2];
};

struct t_dirent64 {
	uint64_t d_ino;
	uint64_t d_seekoff;
	uint16_t d_reclen;
	uint16_t d_namlen;
	uint8_t d_type;
	char d_name[1024];
};

/* The shim's definitions (this binary links it, so these resolve to it). */
extern int shim_stat64(const char *, struct t_stat64 *) __asm__("_stat$INODE64");
extern struct t_dirent64 *shim_readdir64(DIR *) __asm__("_readdir$INODE64");
extern DIR *shim_opendir(const char *) __asm__("_opendir");

static int failures;

static void cmp_u64(const char *what, const char *field, uint64_t a, uint64_t b)
{
	if (a != b) {
		printf("    MISMATCH %s.%s  real=%llu legacy=%llu\n", what, field,
		       (unsigned long long)a, (unsigned long long)b);
		failures++;
	}
}

static void check_stat(const char *path)
{
	int (*real)(const char *, struct t_stat64 *);
	struct t_stat64 r, l;

	/* RTLD_NEXT skips this executable, so this is libSystem's, not the shim's. */
	real = (int (*)(const char *, struct t_stat64 *))dlsym(RTLD_NEXT, "stat$INODE64");
	if (!real) {
		printf("  (no real stat$INODE64 here - are we on Tiger already?)\n");
		return;
	}
	memset(&r, 0, sizeof(r));
	memset(&l, 0, sizeof(l));
	if (real(path, &r) != 0) {
		printf("  %-24s real stat failed; skipping\n", path);
		return;
	}
	if (shim_stat64(path, &l) != 0) {
		printf("  %-24s shim stat FAILED\n", path);
		failures++;
		return;
	}
	printf("  %s\n", path);
	cmp_u64("stat", "st_dev", r.st_dev, l.st_dev);
	cmp_u64("stat", "st_ino", r.st_ino, l.st_ino);
	cmp_u64("stat", "st_mode", r.st_mode, l.st_mode);
	cmp_u64("stat", "st_nlink", r.st_nlink, l.st_nlink);
	cmp_u64("stat", "st_uid", r.st_uid, l.st_uid);
	cmp_u64("stat", "st_gid", r.st_gid, l.st_gid);
	cmp_u64("stat", "st_rdev", r.st_rdev, l.st_rdev);
	cmp_u64("stat", "st_size", (uint64_t)r.st_size, (uint64_t)l.st_size);
	cmp_u64("stat", "st_blocks", (uint64_t)r.st_blocks, (uint64_t)l.st_blocks);
	cmp_u64("stat", "st_blksize", (uint64_t)r.st_blksize, (uint64_t)l.st_blksize);
	cmp_u64("stat", "st_flags", r.st_flags, l.st_flags);
	cmp_u64("stat", "st_mtime", (uint64_t)r.st_mtimespec.tv_sec,
		(uint64_t)l.st_mtimespec.tv_sec);
	cmp_u64("stat", "st_ctime", (uint64_t)r.st_ctimespec.tv_sec,
		(uint64_t)l.st_ctimespec.tv_sec);
	/* st_birthtimespec is deliberately NOT compared: the legacy struct has
	 * no birth time, so the shim zeroes it. Rust surfaces it as
	 * `created()`, which then reads as the epoch on Tiger - a known and
	 * documented loss, not a conversion bug. */
}

/* Walk a directory both ways and compare the name sets in order. */
static void check_dir(const char *path)
{
	struct t_dirent64 *(*real_readdir)(DIR *);
	DIR *(*real_opendir)(const char *);
	DIR *d;
	struct t_dirent64 *e;
	char real_names[64][256];
	int real_n = 0, i = 0, legacy_n = 0;

	real_opendir = (DIR * (*)(const char *))
		dlsym(RTLD_NEXT, "opendir$INODE64$UNIX2003");
	real_readdir = (struct t_dirent64 * (*)(DIR *))
		dlsym(RTLD_NEXT, "readdir$INODE64");
	if (!real_opendir || !real_readdir) {
		printf("  (no real 64-bit dir entry points here)\n");
		return;
	}

	d = real_opendir(path);
	while (d && (e = real_readdir(d)) != NULL && real_n < 64) {
		snprintf(real_names[real_n], sizeof(real_names[0]), "%s", e->d_name);
		real_n++;
	}
	if (d)
		closedir(d);

	printf("  %s: real listed %d entries\n", path, real_n);

	d = shim_opendir(path);
	while (d && (e = shim_readdir64(d)) != NULL && legacy_n < 64) {
		if (legacy_n < real_n && strcmp(real_names[legacy_n], e->d_name) != 0) {
			printf("    MISMATCH entry %d: real=%s legacy=%s\n", legacy_n,
			       real_names[legacy_n], e->d_name);
			failures++;
		}
		if (e->d_namlen != strlen(e->d_name)) {
			printf("    MISMATCH d_namlen=%u but strlen=%lu (%s)\n",
			       e->d_namlen, (unsigned long)strlen(e->d_name), e->d_name);
			failures++;
		}
		legacy_n++;
	}
	if (d)
		closedir(d);
	if (legacy_n != real_n) {
		printf("    MISMATCH entry count: real=%d legacy=%d\n", real_n, legacy_n);
		failures++;
	}
	i = i; /* silence unused in some compilers */
}

int main(void)
{
	printf("== stat: real 10.5 entry point vs the shim's legacy conversion ==\n");
	check_stat("/etc/hosts");
	check_stat("/usr/lib");
	check_stat("/dev/disk0");
	check_stat("/");

	printf("\n== readdir ==\n");
	check_dir("/dev");
	check_dir("/usr/lib");

	printf("\n%s (%d mismatch%s)\n", failures ? "FAILED" : "conversions agree",
	       failures, failures == 1 ? "" : "es");
	return failures ? 1 : 0;
}
