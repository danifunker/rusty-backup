/*
 * opendir-abi.c -- which `opendir` pairs with `readdir$INODE64` on Leopard?
 *
 * `enumerate_devices` returned an empty list on 10.5 while the same
 * enumeration in C (probe/devlist.c) listed /dev correctly. `nm -u` on the
 * binary shows the pairing libstd links:
 *
 *     _opendir            (legacy)
 *     _readdir$INODE64    (64-bit inode)
 *
 * On Darwin those are two ABIs. `readdir$INODE64` yields the 64-bit `dirent`
 * (8-byte d_ino, d_seekoff, 1024-byte d_name); the legacy pair yields the old
 * one. The DIR handle has to be opened by the matching call, and on 10.5 the
 * matching call is **`opendir$INODE64$UNIX2003`** - a combined suffix, and the
 * reason a search for plain `opendir$INODE64` comes up empty and looks like
 * "there is no 64-bit opendir here".
 *
 * A crossed pair does not error. It returns zero entries, so every directory
 * on the system reads as empty.
 *
 *   gcc -D_DARWIN_USE_64_BIT_INODE -o /tmp/opendir-abi opendir-abi.c
 *   /tmp/opendir-abi
 */

#include <dirent.h>
#include <stdio.h>
#include <string.h>

/* With _DARWIN_USE_64_BIT_INODE, `struct dirent` here is the 64-bit form —
 * the same one libc's Rust declaration describes. */
extern DIR *opendir_legacy(const char *) __asm__("_opendir");
extern DIR *opendir_inode64(const char *) __asm__("_opendir$INODE64$UNIX2003");
extern struct dirent *readdir_inode64(DIR *) __asm__("_readdir$INODE64");

static int count(DIR *d, const char *first_wanted)
{
	struct dirent *e;
	int total = 0, empty = 0, found = 0;

	if (!d) {
		printf("      opendir failed\n");
		return -1;
	}
	while ((e = readdir_inode64(d)) != NULL) {
		total++;
		if (e->d_name[0] == '\0')
			empty++;
		else if (first_wanted && strcmp(e->d_name, first_wanted) == 0)
			found = 1;
		if (total < 4 && e->d_name[0] != '\0')
			printf("      d_ino=%-10llu d_reclen=%-5u name=%s\n",
			       (unsigned long long)e->d_ino, e->d_reclen, e->d_name);
	}
	closedir(d);
	printf("      -> %d entries, %d nameless%s\n", total, empty,
	       first_wanted ? (found ? ", found the probe entry" : ", probe entry MISSING")
			    : "");
	return total;
}

static void compare(const char *dir, const char *wanted)
{
	printf("  %s\n", dir);
	printf("    legacy opendir + readdir$INODE64   (what libstd links today)\n");
	count(opendir_legacy(dir), wanted);
	printf("    opendir$INODE64$UNIX2003 + readdir$INODE64   (matched pair)\n");
	count(opendir_inode64(dir), wanted);
}

int main(void)
{
	printf("sizeof(struct dirent) = %lu (64-bit-inode form)\n\n",
	       (unsigned long)sizeof(struct dirent));
	compare("/dev", "disk0");
	printf("\n");
	compare("/usr/lib", NULL);
	printf("\ndone\n");
	return 0;
}
