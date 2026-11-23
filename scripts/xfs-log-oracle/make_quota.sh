#!/bin/bash
# usage: make_quota.sh NAME "MKFS OPTIONS" "MOUNT OPTIONS"
set -e
name=$1; opts=$2; mo=$3; img=/w/$name.img
rm -f $img; truncate -s 300M $img; mkfs.xfs -q $opts $img
mkdir -p /mnt/x; mount -o loop,$mo $img /mnt/x
mkdir /mnt/x/d
for u in 1001 1002 1003; do mkdir /mnt/x/d/u$u; chown $u:$((u+1000)) /mnt/x/d/u$u; done
dd if=/dev/zero of=/mnt/x/d/u1001/a bs=64k count=4 status=none; chown 1001:2001 /mnt/x/d/u1001/a
sync; xfs_io -x -c "shutdown -f" /mnt/x 2>/dev/null || true
umount /mnt/x; mount -o loop,$mo $img /mnt/x
xfs_quota -x -c "report -ugb" /mnt/x > /w/$name.qbefore.txt 2>&1 || true
# Only the log holds these.
for i in 1 2 3; do dd if=/dev/zero of=/mnt/x/d/u1002/f$i bs=32k count=$i status=none; chown 1002:2002 /mnt/x/d/u1002/f$i; done
rm /mnt/x/d/u1001/a
echo x > /mnt/x/d/u1003/t; chown 1003:2003 /mnt/x/d/u1003/t
chown 1003:2003 /mnt/x/d/u1002/f1
xfs_quota -x -c "limit -u bsoft=1m bhard=2m 1002" /mnt/x
sync
xfs_io -x -c "shutdown -f" /mnt/x
umount /mnt/x
cp $img /w/$name.dirty.img
mount -o loop,$mo $img /mnt/x
xfs_quota -x -c "report -ugbin" /mnt/x > /w/$name.quota.txt 2>&1
(cd /mnt/x && find . | sort > /w/$name.tree.txt; find . -type f | sort | xargs md5sum > /w/$name.md5.txt)
umount /mnt/x
mv $img /w/$name.recovered.img
xfs_repair -n /w/$name.recovered.img >/dev/null 2>&1 && echo "$name: kernel-recovered clean"
xfs_logprint /w/$name.dirty.img > /w/$name.logprint.txt 2>&1 || true
