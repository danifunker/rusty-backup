#!/bin/sh
# usage: check_ours.sh NAME  (expects /w/NAME.ours.img)
name=$1; img=/w/$name.ours.img
mkdir -p /mnt/y
xfs_repair -n $img > /w/$name.ours.repair.txt 2>&1; echo "$name: xfs_repair -n exit $?"
dmesg -C 2>/dev/null
if mount -o loop $img /mnt/y; then
  (cd /mnt/y && find . | sort > /w/$name.ours.tree.txt && find . -type f | sort | xargs md5sum > /w/$name.ours.md5.txt; [ -f /w/$name.xattr.txt ] && getfattr -d -R . 2>/dev/null > /w/$name.ours.xattr.txt; [ -f /w/$name.ls.txt ] && ls -ln keep > /w/$name.ours.ls.txt)
  umount /mnt/y
  dmesg 2>/dev/null | grep -iE "recover|xfs" | head -5
  for k in tree md5 xattr ls; do [ -f /w/$name.$k.txt ] && { cmp -s /w/$name.$k.txt /w/$name.ours.$k.txt && echo "  $k: same" || echo "  $k: DIFFERENT"; }; done
else echo "$name: mount failed"; dmesg | tail -5; fi
