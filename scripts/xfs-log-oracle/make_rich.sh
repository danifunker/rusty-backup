#!/bin/sh
# usage: make_rich.sh NAME "MKFS OPTIONS"
set -e
name=$1; opts=$2
img=/w/$name.img
rm -f $img; truncate -s 300M $img
mkfs.xfs -q $opts $img
mkdir -p /mnt/x
mount -o loop $img /mnt/x
mkdir -p /mnt/x/big /mnt/x/keep
for i in $(seq 1 1500); do echo "k$i" > /mnt/x/big/entry_with_a_long_name_$i; done
echo data > /mnt/x/keep/old.txt
sync; xfs_io -x -c "shutdown -f" /mnt/x 2>/dev/null || true
umount /mnt/x; mount -o loop $img /mnt/x
# Everything below lives only in the log.
for i in $(seq 1 1200); do rm /mnt/x/big/entry_with_a_long_name_$i; done
xfs_io -f -c "truncate 0" /mnt/x/frag.bin
for i in $(seq 0 99); do xfs_io -c "pwrite -q $((i*8192)) 4096" -c fsync /mnt/x/frag.bin; done
setfattr -n user.small -v hello /mnt/x/keep/old.txt
setfattr -n user.other -v world /mnt/x/keep/old.txt
mknod /mnt/x/keep/null c 1 3
ln /mnt/x/keep/old.txt /mnt/x/keep/hard.txt
dd if=/dev/urandom of=/mnt/x/keep/blob bs=64k count=20 status=none
truncate -s 100000 /mnt/x/keep/blob
rm /mnt/x/keep/old.txt
sync
xfs_io -x -c "shutdown -f" /mnt/x
umount /mnt/x
cp $img /w/$name.dirty.img
mount -o loop $img /mnt/x
(cd /mnt/x && find . | sort > /w/$name.tree.txt && find . -type f | sort | xargs md5sum > /w/$name.md5.txt && getfattr -d -R . 2>/dev/null > /w/$name.xattr.txt; ls -ln keep > /w/$name.ls.txt)
umount /mnt/x
mv $img /w/$name.recovered.img
xfs_repair -n /w/$name.recovered.img >/dev/null 2>&1 && echo "$name: kernel-recovered image repairs clean"
xfs_repair -n /w/$name.dirty.img 2>&1 | grep -iE "log|dirty" | head -2
xfs_logprint /w/$name.dirty.img > /w/$name.logprint.txt 2>&1 || true
