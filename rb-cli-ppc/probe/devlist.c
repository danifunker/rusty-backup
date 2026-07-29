/*
 * devlist.c -- can we enumerate disks on Leopard without IOKit?
 *
 * `os-stub` returns an empty device list, so the TUI shows "No disks
 * detected" and every device-driven flow (inspect / backup / restore) has
 * nothing to work on. The real `os/macos.rs` gets this from IOKit +
 * DiskArbitration through `objc2-*`, which cannot transpile for PowerPC.
 *
 * Everything the engine's `DiskDevice` actually needs is reachable from plain
 * POSIX, and this proves each piece on the real machine before any of it is
 * written in Rust (where the first compile is an 80-minute round trip):
 *
 *   whole disks + partitions   readdir("/dev")
 *   size                       ioctl(DKIOCGETBLOCKSIZE / DKIOCGETBLOCKCOUNT)
 *   mounts, fs type, free      getmntinfo(3)
 *   which disk is the system   the mount whose f_mntonname is "/"
 *
 * What is *not* reachable this way: removable/ejectable, bus protocol and the
 * marketing media name. Those are IOKit-only and stay empty.
 *
 * The struct layout matters as much as the calls: libc's `statfs` is bound to
 * the 64-bit-inode ABI here (the binary references _statfs$INODE64), so this
 * prints the offsets the Rust side will assume, to be diffed against
 * rb-cli-ppc/probe/ppc-10.5.tsv rather than trusted.
 *
 *   gcc -D_DARWIN_USE_64_BIT_INODE -o /tmp/devlist devlist.c && /tmp/devlist
 *   sudo /tmp/devlist        # sizes need read access to the raw device
 */

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/disk.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/param.h>
#include <unistd.h>

/* "disk12" -> whole disk; "disk12s3" -> a partition of it. */
static int is_disk_name(const char *n, int *whole)
{
	int i = 0, digits = 0;
	if (strncmp(n, "disk", 4) != 0)
		return 0;
	i = 4;
	while (n[i] >= '0' && n[i] <= '9') {
		i++;
		digits++;
	}
	if (digits == 0)
		return 0;
	if (n[i] == '\0') {
		*whole = 1;
		return 1;
	}
	if (n[i] != 's')
		return 0;
	i++;
	digits = 0;
	while (n[i] >= '0' && n[i] <= '9') {
		i++;
		digits++;
	}
	*whole = 0;
	return digits > 0 && n[i] == '\0';
}

static void size_of(const char *bsd)
{
	char path[64];
	uint32_t block_size = 0;
	uint64_t block_count = 0;
	int fd;

	/* The raw device is the one worth reading from; fall back to the
	 * buffered node, which is what a non-root run can sometimes open. */
	snprintf(path, sizeof(path), "/dev/r%s", bsd);
	fd = open(path, O_RDONLY);
	if (fd < 0) {
		snprintf(path, sizeof(path), "/dev/%s", bsd);
		fd = open(path, O_RDONLY);
	}
	if (fd < 0) {
		printf("    size: cannot open (%s)\n", strerror(errno));
		return;
	}
	if (ioctl(fd, DKIOCGETBLOCKSIZE, &block_size) < 0)
		printf("    DKIOCGETBLOCKSIZE failed: %s\n", strerror(errno));
	if (ioctl(fd, DKIOCGETBLOCKCOUNT, &block_count) < 0)
		printf("    DKIOCGETBLOCKCOUNT failed: %s\n", strerror(errno));
	close(fd);
	if (block_size)
		printf("    size: %llu bytes (%llu x %u) via %s\n",
		       (unsigned long long)block_count * block_size,
		       (unsigned long long)block_count, block_size, path);
}

int main(void)
{
	DIR *d;
	struct dirent *e;
	struct statfs *mnt;
	int n, i;
	char whole_names[64][32];
	int whole_count = 0;

	printf("== struct statfs layout (must match libc's Rust view) ==\n");
	printf("  sizeof            = %lu\n", (unsigned long)sizeof(struct statfs));
	printf("  f_bsize     @ %-4lu f_blocks    @ %-4lu f_bavail  @ %lu\n",
	       (unsigned long)offsetof(struct statfs, f_bsize),
	       (unsigned long)offsetof(struct statfs, f_blocks),
	       (unsigned long)offsetof(struct statfs, f_bavail));
	printf("  f_fstypename@ %-4lu f_mntonname @ %-4lu f_mntfromname @ %lu\n",
	       (unsigned long)offsetof(struct statfs, f_fstypename),
	       (unsigned long)offsetof(struct statfs, f_mntonname),
	       (unsigned long)offsetof(struct statfs, f_mntfromname));

	printf("\n== whole disks from readdir(\"/dev\") ==\n");
	d = opendir("/dev");
	if (!d) {
		printf("  opendir failed: %s\n", strerror(errno));
		return 1;
	}
	while ((e = readdir(d)) != NULL) {
		int whole = 0;
		if (!is_disk_name(e->d_name, &whole) || !whole)
			continue;
		if (whole_count < 64)
			snprintf(whole_names[whole_count++], 32, "%s", e->d_name);
	}
	closedir(d);
	for (i = 0; i < whole_count; i++) {
		printf("  /dev/%s\n", whole_names[i]);
		size_of(whole_names[i]);
	}

	printf("\n== mounts from getmntinfo(3) ==\n");
	n = getmntinfo(&mnt, MNT_NOWAIT);
	if (n <= 0) {
		printf("  getmntinfo returned %d (%s)\n", n, strerror(errno));
		return 1;
	}
	for (i = 0; i < n; i++) {
		unsigned long long avail =
			(unsigned long long)mnt[i].f_bavail * mnt[i].f_bsize;
		printf("  %-20s on %-24s %-8s free=%lluMB%s\n",
		       mnt[i].f_mntfromname, mnt[i].f_mntonname,
		       mnt[i].f_fstypename, avail / (1024 * 1024),
		       strcmp(mnt[i].f_mntonname, "/") == 0 ? "  <- system disk"
							   : "");
	}

	printf("\ndone\n");
	return 0;
}
