set -e
K=$(ls /lib/modules); R=/tmp/ir; mkdir -p $R/bin $R/mods $R/proc $R/sys $R/dev
cp /bin/busybox $R/bin/busybox; cp /opt/vm-init $R/init; chmod +x $R/init
: > $R/mods/order
for m in virtio_pci 9pnet_virtio 9p; do
  { modprobe -S $K --show-depends $m || true; } | awk '$1=="insmod"{print $2}' | while read f; do
    b=$(basename $f | sed -E 's/\.ko(\.xz|\.zst)?$//')
    grep -qx "$b" $R/mods/order && continue
    case $f in *.xz) xz -dc $f > $R/mods/$b.ko;; *.zst) zstd -dc $f > $R/mods/$b.ko;; *) cp $f $R/mods/$b.ko;; esac
    echo $b >> $R/mods/order
  done
done
cat $R/mods/order
(cd $R && find . | cpio -o -H newc | gzip) > /opt/initrd.gz
cp /boot/vmlinuz-$K /opt/vmlinuz
