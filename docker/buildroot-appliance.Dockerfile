# Build the rusty-backup Buildroot appliance image — a bootable minimal Linux
# (kernel + rootfs) with rb-cli baked in for backup AND restore. See
# docs/linux_486_appliance.md and buildroot/.
#
# rb-cli goes in as a STATIC binary via a rootfs overlay, so Buildroot only has
# to supply the kernel + a busybox userland — it doesn't build Rust.
#
# Prereq: build the static binary once —
#   docker run --rm -v "$PWD":/src rb-cross-i586-musl
#
# Build the image, then build the appliance (default CMD runs buildroot/build.sh;
# outputs buildroot/output/{bzImage,rootfs.ext2}). Buildroot's own build runs as
# root here (FORCE_UNSAFE_CONFIGURE) which is fine in a throwaway container:
#   docker build -t rb-buildroot - < docker/buildroot-appliance.Dockerfile
#   docker run  --rm -v "$PWD":/work rb-buildroot
#
# Boot + smoke-test in qemu (serial console):
#   qemu-system-i386 -M pc -m 256 -nographic \
#     -kernel buildroot/output/bzImage \
#     -drive file=buildroot/output/rootfs.ext2,format=raw,if=ide \
#     -drive file=<a-test-disk.img>,format=raw,if=ide \
#     -append "root=/dev/sda rw console=ttyS0"

FROM debian:bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential wget cpio unzip rsync bc file python3 sed git \
        libncurses-dev ca-certificates gawk \
    && rm -rf /var/lib/apt/lists/*

# A recent Buildroot LTS branch.
RUN git clone --depth 1 -b 2024.02.x https://github.com/buildroot/buildroot.git /opt/buildroot

ENV FORCE_UNSAFE_CONFIGURE=1
WORKDIR /work
CMD ["sh", "/work/buildroot/build.sh"]
