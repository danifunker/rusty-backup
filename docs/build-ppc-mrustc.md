# Building rb-cli for PowerPC Mac OS X via mrustc

Practical build notes for transpiling the rusty-backup engine to C with
[mrustc](https://github.com/thepowersgang/mrustc) and compiling it natively on
a PowerPC Mac (Tiger 10.4 / Leopard 10.5). This is the *how*; the *why* and the
scope decisions live in [`native_osx_10_dot_3.md`](native_osx_10_dot_3.md).

Companion script: [`../scripts/build-ppc.sh`](../scripts/build-ppc.sh).

Status (2026-07-24): **host libstd + full dependency-graph resolution work; the
host transpile of the engine is the next thing to validate; the PPC half is
blocked on a `powerpc-apple-darwin` libc.** See "Known blockers" at the bottom.

## The two-machine model

mrustc is a *transpiler* — Rust in, C99 out. It never emits a PowerPC binary.
A PowerPC-Darwin C compiler does that, and there's no sane cross-gcc for it on
modern macOS, so the C is compiled *on the G4*.

```
   Modern Mac (fast)                         G4 / PowerPC Tiger
   ----------------------------------        --------------------------------
   Rust  --mrustc-->  C99  (+ record         C99  --gcc-mp-10-->  PowerPC
          the compile command, don't run)          Mach-O binary, runs here
```

The dividing line is the `.c` files. Rust and mrustc never run on the G4; gcc
and the finished binary never run on the Mac. The one deliberate exception:
the **host** path builds a native this-Mac `rb-cli` straight through mrustc as
proof the whole engine transpiles before we involve PowerPC at all.

## What transpiles vs. what stays hand-written C

- **Transpiles:** the whole `rb-cli-ppc` engine — every filesystem driver,
  partition tables, backup/restore, and the ratatui TUI. (Not FAT-only.)
- **Stays out:** `chd` (libchdman is C++), the egui GUI, and the `os/` platform
  layer (device enumeration via objc2 targets modern macOS, not Tiger — that's
  a hand-C shell on the G4, reusing `ppc-tiger/`).

## Prerequisites

### Modern Mac
- An mrustc clone **with the two parser patches** (see below), on branch
  `rb-cli-vintage-build` (fork: `github.com/danifunker/mrustc`).
- The rustc **1.74.0** source: `make RUSTCSRC RUSTC_VERSION=1.74.0` in the
  mrustc dir (downloads `rustc-1.74.0-src/`).
- A modern `cargo` (for `cargo vendor` only).

### G4 (PowerPC Tiger/Leopard)
- MacPorts **gcc10** (`gcc-mp-10`) — needed for mrustc's C11 `<stdatomic.h>`;
  stock Xcode-2 gcc-4.0 will not do.
- MacPorts **legacy-support** — backfills `clock_gettime` etc. that Tiger's
  libSystem lacks (usually pulled in as a gcc10 dependency already).

## The two required mrustc patches

minicargo's minimal TOML/manifest parser can't read rusty-backup's real
`Cargo.toml` set. Both fixes are committed on `rb-cli-vintage-build`:

1. **`tools/common/toml.{cpp,h}`** — the TOML parser only accepted string array
   elements and three escape sequences. It aborted on the parent `Cargo.toml`'s
   `[package.metadata.deb/rpm/aur]` sections (array-of-array / array-of-inline-
   table `assets`) and on multi-line-string line-continuations. Added nested
   `[...]`/`{...}` skipping, integer/bool array elements, and the full escape
   set. (minicargo parses the *parent* manifest, not just `rb-cli-vintage`'s —
   fixing the parser leaves both Cargo.tomls untouched.)
2. **`tools/minicargo/manifest.cpp`** — accept `crate-type = ["lib"]` (cargo's
   alias for the default rlib; `serde_yml` declares it).

Both are candidate upstream PRs.

### Plus: a macOS build-script override set

mrustc ships `script-overrides/stable-1.74.0-{linux,windows}` but **not
`-macos`**. `build-ppc.sh` (stage `overrides`) creates it, derived from the
linux set — only `build_libc.txt` (first cfg `freebsd11` -> `darwin`) and the
`STD_ENV_ARCH` in `build_std.txt` differ.

### And: `MRUSTC_TARGET_VER`

Every mrustc/minicargo invocation must run with `MRUSTC_TARGET_VER=1.74`
exported, or it silently parses in **1.29 mode**. The Makefile does *not* set it
for the `LIBS` target — the script does.

## Running it

```sh
# defaults: MRUSTC_DIR=~/repos/mrustc  RB_DIR=~/repos/rusty-backup
scripts/build-ppc.sh            # runs the host path end to end
scripts/build-ppc.sh hostlibs   # or one stage at a time
```

Stages, in order:

| stage | what | cost | status |
|------|------|------|--------|
| `mrustc`   | build mrustc + minicargo (verifies the patches) | mins | ok |
| `overrides`| create the macOS 1.74 override set | instant | ok |
| `hostlibs` | transpile+compile host libcore/alloc/std | ~mins, heavy | **ok** (33 MB libstd.c -> .o) |
| `vendor`   | `cargo vendor` the 240+ deps | mins | ok |
| `host`     | transpile+build a native `rb-cli` | heavy | **next to validate** |
| `ppclibs`  | build PowerPC libstd | — | **blocked (libc)** |
| `ppc`      | emit PPC C (deferred codegen) | — | after `ppclibs` |

The `host` stage is the real test: it feeds all 244 crates (your engine
included) through mrustc for the first time. Per-crate lowering errors, if any,
surface here.

## The G4 half (once `ppc` emits C)

`stage_ppc` writes C + records the compile command *without running a compiler*
(`MINICARGO_DEFER_CODEGEN=1`). Ship it over and compile natively:

```sh
scp -r ~/repos/mrustc/output-rb-ppc  you@g4.local:~/rb-ppc
# on the G4:
cd ~/rb-ppc
gcc-mp-10 *.c -o rb-cli \
    -I/opt/local/include/LegacySupport -L/opt/local/lib -lMacportsLegacySupport
./rb-cli --help
```
Finalize the exact `-I/-L/-l` flags from the compile command mrustc recorded
(swap its compiler for `gcc-mp-10`, add legacy-support).

## rb-cli-ppc deviations (mrustc workarounds)

`rb-cli-ppc/Cargo.toml` and `scripts/build-ppc.sh` carry these, each working
around a specific mrustc gap (all documented at their site):

- **zip** `deflate` -> `deflate-flate2` — drops `zopfli` -> `bumpalo` (mrustc
  can't infer `Bump::shrink`). Same DEFLATE via flate2.
- **crc** — `patch_crc_vendor` in build-ppc.sh turbofishes `Digest::<uN,Table<L>>::new`
  in all 5 width files (mrustc can't infer the const-generic impl params).
- **env_logger** `0.11` -> `=0.10.2` — drops `jiff` (0.11's timestamp backend;
  const-generic gap). Same logging via humantime.
- **YAML** (`serde_yml` -> `libyml`) — an mrustc macro-expansion gap
  (`TOK_RWORD_AS`); to be dropped via an engine feature-gate.

## Known blockers (in order)

1. ~~minicargo can't parse the manifest~~ — fixed (the two patches).
2. ~~no macOS 1.74 override set~~ — fixed (stage `overrides`).
3. ~~mrustc const-generic `CallPath` abort~~ (crc) — worked around (turbofish).
   The general mrustc fix (inferring const-generic impl params from the result
   type in `visit_call_populate_cache`) is deep and unfinished.
4. **The engine's diverse long tail** — the whole-engine transpile hits ~6
   distinct mrustc gaps + target-specific noise (see `native_osx_10_dot_3.md` §
   "Where this stands"). The scope-down is to transpile the **portable engine**
   and **exclude `os/`** — which drops `libc`/`nix`/`objc2` (the platform layer,
   hand-C on PPC anyway) and most of the pain. NOT FAT-only; all filesystems.
5. **`powerpc-apple-darwin` libc** (for the real target) — no arch file exists
   (the `libc` crate postdates PPC Macs; rust-ppc-tiger has none — it's a
   separate hand-C-runtime approach, not liftable). It must be **created**, and
   the ground truth is on hand: build it from the **MacOSX10.4u SDK** on a real
   PowerPC Mac (see below).
6. **C++ deps** stay off forever (`chd` = libchdman). Already excluded.

## Building the `powerpc-apple-darwin` libc arch file

A real PowerPC Mac (Power Mac G5, Leopard 10.5.8) provides everything:

- **`MacOSX10.4u.sdk`** under `/Developer/SDKs/` (universal, includes PPC) — the
  authoritative C definitions for the Tiger target. Also 10.3.9 / 10.2.8 SDKs
  and the running system's `/usr/include`.
- **Native `gcc-4.0.1` / `gcc-4.2.1`** (`powerpc-apple-darwin9`, `arch: ppc`) —
  enough to compile probe programs (no C11 needed for probes).

Method (don't eyeball the headers — big-endian struct alignment is where you get
silently-wrong field offsets): compile small C probes on real PPC that emit
`sizeof` / `offsetof` / `alignof` and constant values for every type the Rust
`libc` unix layer needs, then generate the `apple`/`powerpc` arch file to match.
`bindgen` against the 10.4u SDK is a faster first draft but finicky with old
SDKs + a ppc target, so probe-and-validate on hardware is the trustworthy finish.
(For compiling mrustc's *emitted* C on this box you still need MacPorts gcc for
C11 `<stdatomic.h>` — the stock 4.0/4.2 lack it. The probes don't.)
