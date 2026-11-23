#!/bin/sh
# usage: make_dirty.sh NAME "MKFS OPTIONS"
set -e
name=$1; opts=$2
img=/w/$name.img
rm -f $img; truncate -s 300M $img
mkfs.xfs -q $opts $img
mkdir -p /mnt/x
mount -o loop $img /mnt/x
# Settle the fresh volume to disk, then make changes that only the log will hold.
sync; xfs_io -x -c "shutdown -f" /mnt/x 2>/dev/null || true
umount /mnt/x; mount -o loop $img /mnt/x
xfs_io -x -c "freeze" -c "thaw" /mnt/x
mkdir -p /mnt/x/dir/sub
echo hello > /mnt/x/hello.txt
seq 1 20000 > /mnt/x/dir/numbers.txt
dd if=/dev/urandom of=/mnt/x/dir/sub/blob.bin bs=64k count=40 status=none
ln -s hello.txt /mnt/x/link
for i in $(seq 1 60); do echo "file $i" > /mnt/x/dir/f$i.txt; done
mv /mnt/x/dir/f1.txt /mnt/x/renamed.txt
rm /mnt/x/dir/f2.txt /mnt/x/dir/f3.txt
# Flush the log to disk but leave the metadata un-checkpointed: what a crash leaves.
xfs_io -x -c "shutdown -f" /mnt/x
umount /mnt/x
cp $img /w/$name.dirty.img
# The kernel's own recovery, as the reference.
mount -o loop $img /mnt/x
(cd /mnt/x && find . | sort > /w/$name.tree.txt && find . -type f | sort | xargs md5sum > /w/$name.md5.txt)
umount /mnt/x
mv $img /w/$name.recovered.img
xfs_repair -n /w/$name.recovered.img >/dev/null 2>&1 && echo "$name: kernel-recovered image repairs clean"
xfs_repair -n /w/$name.dirty.img 2>&1 | grep -iE "log|dirty" | head -2
