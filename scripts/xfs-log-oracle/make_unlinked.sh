#!/bin/bash
set -e
name=$1; opts=$2; img=/w/$name.img
rm -f $img; truncate -s 300M $img; mkfs.xfs -q $opts $img
mkdir -p /mnt/x; mount -o loop $img /mnt/x
echo keep > /mnt/x/keep.txt
dd if=/dev/zero of=/mnt/x/open.bin bs=64k count=10 status=none
sync; xfs_io -x -c "shutdown -f" /mnt/x 2>/dev/null || true
umount /mnt/x; mount -o loop $img /mnt/x
exec 3</mnt/x/open.bin
rm /mnt/x/open.bin
echo more > /mnt/x/more.txt
sync
xfs_io -x -c "shutdown -f" /mnt/x
exec 3<&-
umount /mnt/x
cp $img /w/$name.dirty.img
mount -o loop $img /mnt/x; (cd /mnt/x && find . | sort > /w/$name.tree.txt; find . -type f | sort | xargs md5sum > /w/$name.md5.txt); umount /mnt/x
mv $img /w/$name.recovered.img
xfs_repair -n /w/$name.recovered.img >/dev/null 2>&1 && echo "$name: kernel-recovered clean"
