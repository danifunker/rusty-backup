# Docker build images

Each retro/cross build that's awkward on a dev box (especially macOS / Apple
Silicon) has its **own** Dockerfile here. They copy nothing — the repo is
bind-mounted at run time (`-v "$PWD":/src`), and each image's **default command
builds**, so a build is a one-liner. Linux cross artifacts land in
`./target-cross/` (gitignored); the cb-dos `.exe`s land in `crusty-backup/build/`.

| Dockerfile | Output | Link | Runs on | Verified |
|---|---|---|---|---|
| `cross-i586.Dockerfile` | `rb-cli` i586 | dynamic (glibc) | Pentium+ Linux | ✅ runs |
| `cross-i586-musl.Dockerfile` | `rb-cli` i586 | **static** (musl) | Pentium+ Linux | ✅ runs |
| `cross-i486.Dockerfile` | `rb-cli` i486 | dynamic (glibc) | 486+ Linux¹ | ✅ builds |
| `cb-dos.Dockerfile` | DOS `.exe`s | — | DOS (486+) | ✅ runs (DOSBox-X) |

¹ i486 *codegen* (no CMPXCHG8B), but Debian's 32-bit glibc is i686-baseline, so
the binary needs an **i486 rootfs** (Buildroot/musl) to actually run on bare 486
hardware — see below.

```sh
# build the toolchain image, then build the artifact (default CMD):
docker build -t rb-cross-i586-musl - < docker/cross-i586-musl.Dockerfile
docker run  --rm -v "$PWD":/src rb-cross-i586-musl
# -> ./target-cross/i586-unknown-linux-musl/release/rb-cli  (static, 15 MB)

# cb-dos is amd64 (DJGPP is x86_64; runs under qemu on arm64):
docker build --platform linux/amd64 -t rb-cb-dos - < docker/cb-dos.Dockerfile
docker run  --rm --platform linux/amd64 -v "$PWD":/src rb-cb-dos
```

The atomics/CPU rationale (custom target specs in `../targets/`, `max-atomic-width`,
the cpuid caveat) is in [`../docs/linux_486_build.md`](../docs/linux_486_build.md).

## i586 vs i486 — and why "static" alone isn't enough for a 486

The 486 lacks `CMPXCHG8B` (a Pentium instruction). Two things have to be right
for a binary to run on bare 486 hardware:

1. **Codegen** — *our* code + `std` must be compiled with `cpu=i486` (no
   CMPXCHG8B; 64-bit atomics lowered to libatomic libcalls). `cross-i486` does
   this via nightly `-Z build-std` + `../targets/i486-unknown-linux-gnu.json`.
2. **libc** — the C library it links must also be 486-clean. Debian's 32-bit
   glibc is **i686**-baseline, so even an i486-codegen binary linked against it
   faults on a 486.

**Demonstrated:** the `cross-i586-musl` static binary builds perfectly and runs
under `qemu-i386 -cpu pentium`, but under `qemu-i386 -cpu 486` it dies with
`Illegal instruction` — i586 codegen hit an instruction the 486 doesn't have.
(This also shows `qemu-i386 -cpu 486` is a genuine 486-compat check, not just a
CPUID cosmetic.)

So:
- **Pentium-class machines** → `cross-i586-musl` (static, drop-and-run). Done.
- **True 486 hardware** → needs i486 codegen **and** an i486 libc. `cross-i486`
  gives the codegen; the libc/rootfs is the remaining piece (below). A fully
  static true-486 binary (i486 + musl built for i486) is possible but needs a
  hand-populated `build-std` musl sysroot — Buildroot is the more maintainable
  route.

## Running / testing — Buildroot and emulators

> **Q: Is there a Buildroot i486 we can run, or an emulator?** Yes to both.

### `qemu-i386 -cpu <model>` (user-mode) — for static binaries

A static binary needs no rootfs; qemu-user runs it directly while presenting a
chosen CPU. Baked into the musl image:

```sh
docker run --rm -v "$PWD":/src rb-cross-i586-musl \
  qemu-i386 -cpu pentium /src/target-cross/i586-unknown-linux-musl/release/rb-cli --version
# (… -cpu 486 faults, as shown above — that's the point)
```

### Buildroot + `qemu-system-i386 -cpu 486` — a real i486 userland

This is the right path for a **true 486** target (it gives you both an i486 libc
and a bootable rootfs to test the dynamic `cross-i486` binary):

- Buildroot: `make qemu_x86_defconfig`, then `menuconfig` → **Target
  Architecture → i486** (`BR2_x86_i486`); optionally switch libc to musl. Output:
  a kernel + i486 rootfs.
- Boot: `qemu-system-i386 -cpu 486 -kernel output/images/bzImage \
  -drive file=output/images/rootfs.ext2,format=raw -append "root=/dev/sda …"`,
  copy in the `rb-cli` binary, run it.

Buildroot is a long one-off build, so it isn't wired into a Dockerfile here yet;
it's also the natural way to ship a bootable "Linux backup appliance" image.

### 86Box / real hardware — highest fidelity

86Box (already used for cb-dos) emulates actual 486 chipsets far more strictly
than qemu's TCG — the best stand-in for real hardware short of a real 486DX2/DX4,
where the cpuid-less early-486 edge case must finally be confirmed.
