# `sparcv9-sun-solaris2.9` cross toolchain

A container holding a GCC that can compile mrustc's C output for **64-bit SPARC
Solaris 9**, plus mrustc's own build dependencies, so one image does both halves of a
cross build.

## Why this exists

Solaris 9 sits in a gap that no package can fill:

| | |
|---|---|
| Newest GCC in any OpenCSW Solaris 9 catalog | 4.6.4 |
| Newest GCC that supports Solaris 9 at all | 4.9.x, and only with `--enable-obsolete` |
| Needed for `<stdatomic.h>` in mrustc's output | 4.9 |
| Needed for `__builtin_{add,sub,mul}_overflow` | 5.0 |

GCC obsoleted Solaris 9 in 4.9 and deleted the port in the next release, which shipped
as GCC 5 — so 4.9.4 is the ceiling, and it is missing the overflow builtins. mrustc
closes that gap from its side: the `sparcv9-sun-solaris2.9` target sets
`emulate-overflow-intrinsics`, and `src/trans/codegen_c.cpp` emits type-suffixed
`__builtin_*_overflow_u32`-style helpers instead of the type-generic builtins.

GCC 4.9 needs `-std=gnu++98 -fpermissive` to build at all; with those it builds under a
current host GCC too (verified on 13.3), so the Debian 11 (GCC 10) pin is for
reproducibility rather than necessity. Building on the Blade itself was never an option —
every translation unit would compile on a 1.6GHz UltraSPARC IIIi.

## The sysroot

No sysroot ships here: it is a copy of a licensed Solaris 9 install's headers and
libraries, and Solaris 9 predates OpenSolaris by three years so no free substitute
exists. Whether a copy may be redistributed depends on the licence yours came
under. Building one from scratch: `Notes/SolarisSysroot.md` in the mrustc tree. Supply
one from a live Solaris 9 SPARC install:

```sh
SOL9_HOST=user@192.168.99.176 ./build.sh
```

or drop your own `sysroot.tar.gz` (rooted at `/`, containing `usr/include`, `usr/lib`
and `usr/ccs/lib`) next to this file and run `./build.sh`.

The image also patches one header. Solaris 9's `<sys/int_limits.h>` defines
`INTPTR_MAX` and `UINTPTR_MAX` as *empty* macros: before C99 they were existence
flags rather than values, and GCC copies the header into `include-fixed` without
fixing it. Any C99 code doing `#if UINTPTR_MAX == ...` then fails with "operator
'==' has no left operand" — mbedTLS does exactly that.

Solaris 9 ships **no `crt1.o`/`crti.o`/`crtn.o`** — on Solaris those come with the
compiler rather than the OS, and the package database has no entry for them. That is
expected and harmless: GCC builds its own from `libgcc/config/sparc/sol2-c1.S`.

Pull the sysroot as root. Parts of `/usr/lib` are not world-readable, and a partial
sysroot fails much later and far less clearly.

## Using it

```sh
docker run --rm -v "$PWD:/work" -w /work mrustc-sol9-cross \
    make -f minicargo.mk LIBS \
        MRUSTC_TARGET=sparcv9-sun-solaris2.9 \
        OVERRIDE_SUFFIX=-solaris \
        STD_ENV_ARCH=sparc64
```

mrustc invokes `<target>-gcc`, so `sparcv9-sun-solaris2.9-gcc` is what it picks up off
`PATH`. `CC_sparcv9_sun_solaris2_9` overrides that (`-` becomes `_`), as does `CC`.

`STD_ENV_ARCH=sparc64` is needed because `std::env::consts::ARCH` is otherwise derived
from the first component of the target triple, which is `sparcv9`.

## Deploying to the target

Solaris 9 has no `libgcc_s.so.1` of its own, and Rust's `unwind` crate asks for it by name
(`#[link(name = "gcc_s")]`), which defeats `-static-libgcc`. Install the toolchain's copy
on the target once:

```sh
scp /opt/sol9/sparcv9-sun-solaris2.9/lib/sparcv9/libgcc_s.so.1 HOST:/tmp/
ssh HOST 'sudo cp /tmp/libgcc_s.so.1 /usr/lib/sparcv9/ && sudo chmod 755 /usr/lib/sparcv9/libgcc_s.so.1'
```

The 64-bit library lives under `lib/sparcv9/`; `lib/` holds the 32-bit one, following the
Solaris multilib layout rather than GCC's usual one.

## Notes

- The compiler defaults to 64-bit: `sparcv9-*-solaris2*` selects `sparc/default-64.h`.
  32-bit remains available through the usual multilib, and the target spec passes
  `-m64 -mcpu=v9` explicitly anyway — 32-bit SPARC has no `__int128`.
- GNU `as`/`ld` are used, since Solaris' own are not available to a cross build. The
  target spec already avoids the GNU-only linker options Solaris `ld` lacks, so nothing
  regresses when linking natively on the target instead.
