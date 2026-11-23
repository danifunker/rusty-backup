#!/bin/bash
# usage: check_quota.sh NAME "MOUNT OPTIONS"  (expects /w/NAME.ours.img)
name=$1; mo=$2; img=/w/$name.ours.img; mkdir -p /mnt/y
xfs_repair -n $img > /w/$name.ours.repair.txt 2>&1; echo "$name: xfs_repair -n exit $?"
dmesg -C
mount -o loop,$mo $img /mnt/y || { echo mount failed; dmesg | tail -3; exit; }
dmesg | grep -iE "quotacheck|recover|clean mount" | sed 's/^\[[ 0-9.]*\] //'
xfs_quota -x -c "report -ugbin" /mnt/y > /w/$name.ours.quota.txt 2>&1
(cd /mnt/y && find . | sort > /w/$name.ours.tree.txt; find . -type f | sort | xargs md5sum > /w/$name.ours.md5.txt)
umount /mnt/y
sed -i "s|/mnt/y|/mnt/x|" /w/$name.ours.quota.txt
for k in quota tree md5; do cmp -s /w/$name.$k.txt /w/$name.ours.$k.txt && echo "  $k: same" || { echo "  $k: DIFFERENT"; diff /w/$name.$k.txt /w/$name.ours.$k.txt | head -8; }; done
