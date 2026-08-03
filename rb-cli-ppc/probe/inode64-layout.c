/*
 * inode64-layout.c -- the legacy and 64-bit-inode layouts, side by side.
 *
 * Tiger (10.4) exports no `$INODE64` symbols at all, so a binary built like
 * ours - libstd binds `stat$INODE64`, `readdir$INODE64`, `getmntinfo$INODE64`
 * - has nothing to bind to there. The plan is a translation shim: call the
 * plain entry point, which fills the *legacy* struct, and convert it to the
 * 64-bit-inode struct that libc's Rust declarations describe.
 *
 * That conversion is only as good as the two layouts, so measure both rather
 * than reading them out of a header by eye. Compile this file twice:
 *
 *   gcc                             -o /tmp/lay-legacy inode64-layout.c
 *   gcc -D_DARWIN_USE_64_BIT_INODE  -o /tmp/lay-64     inode64-layout.c
 *
 * and diff. Run against the 10.4u SDK too, to confirm Tiger's legacy struct is
 * the same one Leopard's legacy entry points fill:
 *
 *   gcc -isysroot /Developer/SDKs/MacOSX10.4u.sdk -o /tmp/lay-104 inode64-layout.c
 */

#include <dirent.h>
#include <stddef.h>
#include <stdio.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/types.h>

#define F(type, field) \
	printf("  %-10s %-16s @ %-5lu size %lu\n", #type, #field, \
	       (unsigned long)offsetof(struct type, field), \
	       (unsigned long)sizeof(((struct type *)0)->field))

int main(void)
{
#ifdef _DARWIN_USE_64_BIT_INODE
	printf("== mode: _DARWIN_USE_64_BIT_INODE (what libc's Rust decls describe) ==\n");
#else
	printf("== mode: legacy (what Tiger's plain entry points fill) ==\n");
#endif

	printf("struct stat: sizeof = %lu\n", (unsigned long)sizeof(struct stat));
	F(stat, st_dev);
	F(stat, st_ino);
	F(stat, st_mode);
	F(stat, st_nlink);
	F(stat, st_uid);
	F(stat, st_gid);
	F(stat, st_rdev);
	F(stat, st_size);
	F(stat, st_blocks);
	F(stat, st_blksize);
	F(stat, st_flags);
	F(stat, st_gen);

	printf("\nstruct statfs: sizeof = %lu\n", (unsigned long)sizeof(struct statfs));
	F(statfs, f_bsize);
	F(statfs, f_iosize);
	F(statfs, f_blocks);
	F(statfs, f_bfree);
	F(statfs, f_bavail);
	F(statfs, f_files);
	F(statfs, f_ffree);
	F(statfs, f_fstypename);
	F(statfs, f_mntonname);
	F(statfs, f_mntfromname);

	printf("\nstruct dirent: sizeof = %lu\n", (unsigned long)sizeof(struct dirent));
	F(dirent, d_ino);
	F(dirent, d_reclen);
	F(dirent, d_namlen);
	F(dirent, d_type);
	F(dirent, d_name);

	return 0;
}
