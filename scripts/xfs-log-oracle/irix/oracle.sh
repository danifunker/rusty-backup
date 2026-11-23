#!/bin/sh
# usage: oracle.sh BOOTDISK TESTDISK OUTPREFIX — boot IRIX from BOOTDISK with TESTDISK as unit 3,
# check it, mount it, list it, unmount it
R=$(cd "$(dirname "$0")" && pwd); boot=$1; disk=$2; out=$3
(cd $R && ./launch.sh "$boot" "$disk" > $R/iris-stdout.log 2>&1 &)
$R/ci status --wait-ready 120 >/dev/null 2>&1; $R/ci start >/dev/null 2>&1
timeout 1200 $R/ci boot >/dev/null 2>&1; timeout 300 $R/ci login >/dev/null 2>&1
$R/ci run 'xfs_repair -n /dev/rdsk/dks0d3s0 2>&1 | tail -4' > $out.repair-before.txt 2>&1
$R/ci run 'mkdir -p /t; echo BEFORE=`grep -c "XFS recovery" /var/adm/SYSLOG`; mount -t xfs /dev/dsk/dks0d3s0 /t; echo MOUNT=$?; sleep 2; echo AFTER=`grep -c "XFS recovery" /var/adm/SYSLOG`' > $out.mount.txt 2>&1
$R/ci run 'cd /t && find . -print | sort | while read f; do ls -ldni "$f"; done' > $out.ls.txt 2>&1
$R/ci run 'cd /t && find . -type f -print | sort | xargs sum -r' > $out.sum.txt 2>&1
$R/ci run 'cd /; umount /t; echo UMOUNT=$?; xfs_repair -n /dev/rdsk/dks0d3s0 2>&1 | tail -4' > $out.after.txt 2>&1
$R/ci run 'grep -n "XFS recovery" /var/adm/SYSLOG | tail -3' > $out.syslog.txt 2>&1
$R/ci serial-send "$(printf '\r/etc/shutdown -y -g0 -i0\r')" >/dev/null 2>&1
timeout 600 $R/ci serial-wait 'Okay to power off' --timeout 600 >/dev/null 2>&1; $R/ci quit >/dev/null 2>&1
echo "done $out"
