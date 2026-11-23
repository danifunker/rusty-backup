#!/bin/sh
rm -f /w/vmjob.out
timeout 1500 qemu-system-aarch64 -M virt -cpu max -m 2048 -smp 4 -nographic -no-reboot -nic none \
  -kernel /opt/vmlinuz -initrd /opt/initrd.gz -append "console=ttyAMA0 panic=-1" \
  -virtfs local,path=/,mount_tag=root,security_model=passthrough,id=root > /w/console.log 2>&1
cat /w/vmjob.out 2>/dev/null || tail -30 /w/console.log
