  # Create raw image directly
  dd if=/dev/zero of=/tmp/hfsplus.img bs=1M count=4
  newfs_hfs -v "test_hfsplus" /tmp/hfsplus.img
  # Mount the raw image
  hdiutil attach -imagekey diskimage-class=CRawDiskImage /tmp/hfsplus.img -mountpoint /tmp/mnt_hfsplus
  echo -n "Hello, hfsplus!" > /tmp/mnt_hfsplus/hello.txt
  mkdir -p /tmp/mnt_hfsplus/subdir
  echo -n "nested file" > /tmp/mnt_hfsplus/subdir/nested.txt
  hdiutil detach /tmp/mnt_hfsplus
  zstd -19 /tmp/hfsplus.img -o test_hfsplus.img.zst

