# Building rb-cli for PowerPC Mac OS X via mrustc

Practical build notes for transpiling the rusty-backup engine to C with
[mrustc](https://github.com/thepowersgang/mrustc) and compiling it natively on
a PowerPC Mac (Tiger 10.4 / Leopard 10.5). This is the *how*; the *why* and the
scope decisions live in [`native_osx_10_dot_3.md`](native_osx_10_dot_3.md).

Companion scripts: [`../scripts/build-ppc.sh`](../scripts/build-ppc.sh) (driver),
[`../scripts/ppc-cc-remote.py`](../scripts/ppc-cc-remote.py) (the remote C
compiler), [`../scripts/ppc-libc-probe.py`](../scripts/ppc-libc-probe.py) and
[`../scripts/ppc-libc-compare.py`](../scripts/ppc-libc-compare.py) (libc ground
truth).

Status (2026-07-25): **the full Rust standard library - core, alloc, std,
panic_unwind, test, libc - builds for `powerpc-apple-darwin` and links into a
running PowerPC Mach-O binary.** Getting there took five mrustc fixes and two
rustc-source patches, all listed below. The remaining work is *correctness*, not
*buildability*: several `libc` struct definitions describe modern macOS and are
wrong for 10.4/10.5, which is a silent-at-compile-time, wrong-at-runtime class of
bug. See "The libc situation" below.

## The two-machine model

mrustc is a *transpiler* - Rust in, C99 out. It never emits a PowerPC binary.
A PowerPC-Darwin C compiler does that, and there's no sane cross-gcc for it on
a modern host, so the C is compiled *on the Mac*.

```
   This machine (fast)                       PowerPC Mac (over ssh)
   ----------------------------------        --------------------------------
   Rust  --mrustc-->  C99            --->    C99  --gcc10-->  PowerPC Mach-O
```

You do **not** run this in two manual halves.
[`scripts/ppc-cc-remote.py`](../scripts/ppc-cc-remote.py) *is* the C compiler as
far as mrustc is concerned: point `CC_powerpc_apple_darwin` at it and every
codegen step ships its `.c` over ssh, runs gcc there, and copies the `.o` back.
minicargo's dependency graph, parallelism and incremental rebuilds keep working
across the two machines. mrustc passes its arguments in a response file
(`cc @cmdfile`); the wrapper handles that form.

The wrapper also owns the platform's link line, which is not obvious:

| flag | why |
|---|---|
| `-latomic` | 32-bit PowerPC has no lock-free 8-byte atomic; the `__atomic_*_8` calls mrustc emits for `AtomicU64` live in libatomic |
| `-lMacportsLegacySupport` | `pthread_setname_np` is 10.6+ |
| `-lgcc_s.1` | `_Unwind_GetIPInfo`, used by std's DWARF personality routine |
| `rb-cli-ppc/shim/ppc-compat.o` | `lgammaf_r`, which Leopard's libm lacks (it has `lgamma_r` and `lgammaf`, just not the float reentrant form) |

`_Unwind_GetIPInfo` is in Leopard's `libgcc_s.1` but **not** in
`libgcc_s.10.4`, so a binary that must run on Tiger needs gcc10's own unwinder
or `panic=abort`. That is an open item.

## What transpiles vs. what stays hand-written C

- **Transpiles:** the whole `rb-cli-ppc` engine — every filesystem driver,
  partition tables, backup/restore, and the ratatui TUI. (Not FAT-only.)
- **Stays out:** `chd` (libchdman is C++), the egui GUI, and the `os/` platform
  layer (device enumeration via objc2 targets modern macOS, not Tiger — that's
  a hand-C shell on the G4, reusing `ppc-tiger/`).

## Prerequisites

### This machine (the transpile host)
- An mrustc clone **with the fixes below**, on branch `rb-cli-vintage-build`
  (fork: `github.com/danifunker/mrustc`).
- The rustc **1.74.0** source: `make RUSTCSRC RUSTC_VERSION=1.74.0` in the
  mrustc dir (downloads `rustc-1.74.0-src/` and applies
  `rustc-1.74.0-src.patch`, which carries the two stdlib fixes below).
- A modern `cargo` (for `cargo vendor` only).
- ssh key auth to the PowerPC Mac, and `rsync`.

### The PowerPC Mac
Verified on a dual-G5 (PowerPC 970, 4 GB) running Leopard 10.5.8:
- **gcc 10.5.0** at `/opt/local/libexec/gcc10-bootstrap/bin/gcc`
  (`powerpc-apple-darwin9`) - required for mrustc's C11 `<stdatomic.h>`; stock
  Xcode gcc-4.0/4.2 lack it. It also supplies `libatomic` and emulated TLS.
- **MacportsLegacySupport** at `/opt/local/lib` - backfills `pthread_setname_np`,
  `clock_gettime` and friends.
- `/Developer/SDKs/MacOSX10.4u.sdk` (and 10.3.9 / 10.5) for the libc probes.

For scale: libcore's 29.7 MB of emitted C compiles on the G5 in ~104 seconds, so
a full stdlib build is minutes, not hours.

## mrustc fixes this needed

All committed on `rb-cli-vintage-build`; each is a candidate upstream PR.

**Manifest parsing** (pre-existing, needed to read rusty-backup's Cargo.toml set):

1. **`tools/common/toml.{cpp,h}`** - the TOML parser only accepted string array
   elements and three escape sequences. It aborted on the parent `Cargo.toml`'s
   `[package.metadata.deb/rpm/aur]` sections (array-of-array / array-of-inline-
   table `assets`) and on multi-line-string line-continuations. **Merged
   upstream.**
2. **`tools/minicargo/manifest.cpp`** - accept `crate-type = ["lib"]` (cargo's
   alias for the default rlib; `serde_yml` declares it).

**PowerPC support** (this round):

3. **`src/parse/lex.cpp`** - an unrecognised integer-literal suffix was a hard
   error. rustc's lexer accepts an arbitrary identifier there and only rejects it
   when the literal is *evaluated*, so a suffixed literal that only appears in a
   `macro_rules!` matcher is legal. `core_arch`'s PowerPC intrinsics select
   `impl_vec_trait!` arms with bare `2b` / `3b` / `4b` tokens, which made libcore
   unbuildable for **every** `powerpc*` target. Now the suffix is emitted as a
   following ident token; matcher and invocation split identically, so macro
   matching is unaffected.
4. **`src/hir_conv/constant_evaluation.cpp`** - added the `arith_offset` and
   `ptr_guaranteed_cmp` intrinsics. Both are reached through 32-bit-only paths in
   `core::slice::ascii`, so this affects any 32-bit target, not just PowerPC.
5. **`src/trans/target.cpp`** - `ARCH_POWERPC` declared no 64-bit atomic, which
   cfg's `AtomicU64` out of libcore; libstd's `sys::unix::time` uses it
   unconditionally on macOS, so libstd could not build. 32-bit PowerPC has no
   lock-free 8-byte atomic instruction, but it does not need one: `_Atomic
   uint64_t` lowers to `__atomic_*_8` calls that libatomic implements with a
   lock. Also added `-l atomic` to the target's linker options.
6. **`tools/minicargo/build.cpp`** - `debug_assertions` was unconditional; added
   `MINICARGO_NO_DEBUG_ASSERTIONS`. Required here, for the reason in "The
   alignment problem" below.

**rustc-source patches** (in `rustc-1.74.0-src.patch`):

7. **`library/std/src/sys/unix/fs.rs`** - `macos_weak` is compiled for every
   non-aarch64 macOS target but only defines the `fdopendir` weak symbol for x86
   and x86_64. On any other 32-bit macOS target neither arm is emitted, so the
   name resolves to the enclosing function and `.get()` has no applicable method.
   10.4 has no `$INODE64` variants at all, so the plain symbol is the correct one
   there; on Tiger the weak lookup returns `None` and `remove_dir_all` falls back
   to its non-`openat` path, which is the intent of the module.
8. **`library/std/src/sys/unix/thread_parking/mod.rs`** - std's Darwin thread
   parker is built on libdispatch (`dispatch_semaphore_*`), which is 10.6+.
   `powerpc-apple-darwin` is by definition 10.4/10.5, so it falls through to the
   generic pthread parker.

### Plus: a macOS/PowerPC build-script override set

mrustc ships `script-overrides/stable-1.74.0-{linux,windows}` but not `-macos`.
`build-ppc.sh` creates both `-macos` and `-macos-powerpc`; they differ only in
`STD_ENV_ARCH`. The `-powerpc` variant matters because minicargo.mk picks
`OVERRIDE_SUFFIX` from the **host** OS, so a cross build from Linux would
otherwise use the linux set (whose `build_libc.txt` declares `freebsd11`).

### And: `MRUSTC_TARGET_VER`

Every mrustc/minicargo invocation must run with `MRUSTC_TARGET_VER=1.74`
exported, or it silently parses in **1.29 mode**. The Makefile does *not* set it
for the `LIBS` target - the script does.

## Running it

```sh
export PPC_HOST=admin@192.168.99.116        # the PowerPC Mac
scripts/build-ppc.sh                        # the host path, end to end
scripts/build-ppc.sh ppclibs                # PowerPC stdlib
scripts/build-ppc.sh ppc                    # PowerPC rb-cli
scripts/build-ppc.sh probe                  # libc ground truth from the SDKs
```

Stages, in order:

| stage | what | status |
|------|------|--------|
| `mrustc`   | build mrustc + minicargo (verifies the patches) | ok |
| `overrides`| create the macOS 1.74 override sets | ok |
| `hostlibs` | transpile+compile host libcore/alloc/std | ok |
| `vendor`   | `cargo vendor` the 240+ deps | ok |
| `hostc`    | emit the engine's C on this machine (deferred codegen) | ok |
| `host`     | transpile+build a native `rb-cli` | to validate |
| `ppclibs`  | **PowerPC libcore/alloc/std/panic_unwind/test/libc** | **ok** |
| `ppc`      | PowerPC `rb-cli` | next |
| `probe`    | capture libc ground truth from the 10.4u / 10.5 SDKs | ok |

Building the PowerPC stdlib by hand, if you want to skip the driver:

```sh
cd ~/repos/mrustc
PPC_HOST=admin@g5.local \
PPC_SHIM=~/repos/rusty-backup/rb-cli-ppc/shim/ppc-compat.c \
CC_powerpc_apple_darwin=~/repos/rusty-backup/scripts/ppc-cc-remote.py \
MRUSTC_TARGET_VER=1.74 MINICARGO_DEFER_CODEGEN=1 MINICARGO_NO_DEBUG_ASSERTIONS=1 \
make -f minicargo.mk LIBS RUSTC_VERSION=1.74.0 \
     MRUSTC_TARGET=powerpc-apple-darwin \
     OVERRIDE_SUFFIX=-macos-powerpc PARLEVEL=3
```

and then a hello-world against it:

```sh
bin/mrustc hello.rs -o output-1.74.0-powerpc-apple-darwin/hello \
    --target powerpc-apple-darwin -L output-1.74.0-powerpc-apple-darwin -O
ssh $PPC_HOST './ppc-xbuild/output-1.74.0-powerpc-apple-darwin/hello'
```

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

## The alignment problem (the one real design wart)

Darwin/PowerPC uses the **"power" alignment ABI**: a struct's alignment follows
its *first* member, so an 8-byte member that is not first is only 4-aligned.
Rust's `repr(C)` uses the max-member rule. mrustc already models the PowerPC rule
(`src/trans/target.cpp`), and it has to: mrustc emits every struct as a plain C
struct and lets the C compiler lay it out, so mrustc's model must match gcc's or
the `sizeof_assert` / `alignof_assert` typedefs it emits fail to compile. That is
also a useful safety net - a layout disagreement is a compile error, not silent
corruption.

The cost is that the rule applies to Rust's own types too, and there a field can
land at an offset that does not satisfy `align_of::<FieldTy>()`.
`std::thread::Inner` is the concrete casualty: mrustc puts its `ThreadId` (a
`NonZeroU64`, align 8) at offset 84, and `ptr::write`'s
`assert_unsafe_precondition!` then aborts **every** program inside `std::rt::init`,
before `main`:

```
thread panicked while processing panic. aborting.
#8  ZRIG2cD8std..thread6Thread0g3new0g
#21 ZRG2cD8std0_0_02rt4init0g
  "unsafe precondition(s) violated: ptr::write requires that the pointer
   argument is aligned and non-null"
```

The generated C is still *correct* - gcc knows the member's real alignment and
emits matching accesses - so the current answer is to build the PowerPC stdlib
with `MINICARGO_NO_DEBUG_ASSERTIONS=1`, which compiles those checks out.

That is a workaround, not a fix. Restricting the PowerPC rule to `repr(C)` was
tried and does not work: gcc then disagrees with mrustc about Rust types and
libcore fails its own `alignof_assert`. A real fix means making mrustc stop
delegating layout to the C compiler for `repr(Rust)` types - emitting explicit
padding members plus a forced `__attribute__((aligned))` - which is a much larger
change. 32-bit PowerPC is the only arch where this arises: it is the sole target
with 32-bit pointers but 8-byte-aligned `u64`.

## The libc situation

The old plan assumed a `powerpc-apple-darwin` `libc` arch file had to be written
from scratch. It does not: libc's `unix/bsd/apple/b32` module is arch-agnostic,
and libc **compiles for this target unchanged**. `target_arch = "powerpc"` also
happens to dodge the `$UNIX2003` `link_name` overrides, which are gated on
`target_arch = "x86"`.

What still needs checking is struct *layout*, and that is the dangerous class:
it compiles cleanly and is wrong at runtime. mrustc's `sizeof_assert` /
`alignof_assert` typedefs only compare mrustc's layout against gcc's layout of
*mrustc's own declaration* - never against the SDK's real struct.

So measure it. `scripts/ppc-libc-probe.py` compiles `sizeof` / `alignof` /
`offsetof` probes on the real machine against a real SDK;
`scripts/ppc-libc-compare.py` diffs that against what libc's Rust declarations
lay out, modelling the same power-alignment rule mrustc uses. Results are checked
in under `rb-cli-ppc/probe/` (`.tsv` = raw measurements, `.report.txt` = the
diff).

Two configuration details matter, and getting either wrong produces a page of
false findings:

- **Probe with `-D_DARWIN_USE_64_BIT_INODE`** (the script's default). libc binds
  `stat$INODE64`, `fstat$INODE64`, `opendir$INODE64`, `readdir$INODE64` (see
  libc's `src/unix/mod.rs`), so the 64-bit-inode layout is the one its struct
  definitions must match. Probing without it measures the legacy struct and
  reports `stat`, `statfs` and `dirent` as broken when they are not.
- **Model the power-alignment rule on the Rust side too**, as the compare script
  does - otherwise every struct with a non-leading 8-byte field looks wrong.

### Result

| | exact | name-only | real mismatch |
|---|---|---|---|
| **10.5** | 86 | 6 | 8 |
| **10.4u** | 78 | 6 | 10 |

"Name-only" means sizes, alignments and every offset agree and libc merely calls
a field something the header does not (`st_atime_nsec` for `st_atimespec.tv_nsec`).

On **10.5 `stat` and `dirent` are correct**, and this is confirmed end-to-end -
a PowerPC binary reports `/etc/hosts` as 236 bytes with the right mode, ino, uid,
gid, nlink, mtime, blocks and dev, and counts `/usr/lib` at 390 entries, both
matching `stat -f` on the machine. The eight real 10.5 mismatches are `statfs`,
`passwd`, `ipc_perm`, `semid_ds`, `shmid_ds`, `rt_metrics`, `malloc_zone_t`,
`vnode_info` - SysV IPC, routing and malloc-zone internals, none of which the
engine's file paths touch. `passwd` is the one worth fixing early (`home_dir`).

**10.4 is a different story, and the blocker there is not layout.** Tiger's
libSystem exports **zero** `$INODE64` symbols, so a binary linked the way this
one is cannot even launch on 10.4 - the dynamic linker fails on `_stat$INODE64`
before `main`. Fixing that means a genuine PowerPC/10.4 arch file that binds the
plain symbols *and* declares the legacy structs: `stat` is 96 bytes there with a
4-byte `st_ino` at offset 4 and no birthtime (vs 108), `statfs` 272 (vs 2168) and
`dirent` 264 (vs 1048). All three are measured and checked in.

Other 10.4-only findings from the same probes:

- No `<spawn.h>`, `<copyfile.h>` or `<libproc.h>` - all 10.5 additions.
- No `daemon$1050`, which libc's `cfg_attr(not(target_arch = "aarch64"))` would
  otherwise bind.
- 62 `$UNIX2003` symbols vs Leopard's 173. `ppc` and `i386` are identical here on
  both SDKs, so none of this is PowerPC-specific.

The probe runner can only exercise arches the machine can execute, so `i386`
probes cannot be captured on a PowerPC Mac; the useful axis is 10.4u vs 10.5,
and both are checked in.

## Where this stands

Done:

1. ~~minicargo can't parse the manifest~~ - fixed (fixes 1-2).
2. ~~no macOS 1.74 override set~~ - fixed (`build-ppc.sh`).
3. ~~mrustc const-generic `CallPath` abort~~ (crc) - worked around with a
   turbofish patch on the vendored source; the general mrustc fix (inferring
   const-generic impl params from the result type) is deep and unfinished.
4. ~~libcore won't build for `powerpc-apple-darwin`~~ - fixed (fixes 3-4).
5. ~~no `powerpc-apple-darwin` libc~~ - libc *compiles* for this target
   unchanged, and on 10.5 its `stat`/`dirent` are correct (verified end to end).
   See "The libc situation".
6. ~~libstd won't build~~ - fixed (fixes 5-8).
7. ~~nothing links~~ - fixed (the wrapper's link line + `ppc-compat.c`).

Open, in rough priority order:

1. **Tiger (10.4) support** is the big one, and it is a *symbol* problem before
   it is a layout problem: 10.4 has no `$INODE64` symbols, so today's binaries
   cannot launch there. That needs a `powerpc-apple-darwin` arch file binding the
   plain symbols and declaring 10.4's legacy `stat` / `statfs` / `dirent`, all of
   which are measured in `rb-cli-ppc/probe/`. `_Unwind_GetIPInfo` is also absent
   from `libgcc_s.10.4`, so 10.4 needs gcc10's unwinder or `panic=abort`.
   Everything so far is built and run on 10.5.
2. **The alignment workaround.** `MINICARGO_NO_DEBUG_ASSERTIONS=1` is a
   workaround; see "The alignment problem" for what a real fix costs.
3. **Eight libc structs are wrong even on 10.5** - `statfs`, `passwd`,
   `ipc_perm`, `semid_ds`, `shmid_ds`, `rt_metrics`, `malloc_zone_t`,
   `vnode_info`. None are on the engine's file paths; `passwd` (`home_dir`) is
   the one worth doing first.
4. **The engine itself** (`scripts/build-ppc.sh ppc`) - the stdlib is the hard
   part and it is done, but the 244-crate graph has its own long tail (see the
   deviations above).
5. **C++ deps stay off forever** (`chd` = libchdman). Already excluded.
