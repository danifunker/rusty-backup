FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends xfsprogs util-linux attr \
    qemu-system-arm linux-image-arm64 busybox-static cpio kmod && rm -rf /var/lib/apt/lists/*
COPY mkinitrd.sh vm-init /opt/
RUN sh /opt/mkinitrd.sh
