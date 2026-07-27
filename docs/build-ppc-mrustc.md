# Building rb-cli for PowerPC Mac OS X via mrustc

Practical build notes for transpiling the rusty-backup engine to C with
[mrustc](https://github.com/thepowersgang/mrustc) and compiling it natively on
a PowerPC Mac (Tiger 10.4 / Leopard 10.5). This is the *how*; the *why* and the
scope decisions live in [`native_osx_10_dot_3.md`](native_osx_10_dot_3.md).

Companion scripts: [`../scripts/build-ppc.sh`](../scripts/build-ppc.sh) (driver),
[`../scripts/ppc-cc-remote.py`](../scripts/ppc-cc-remote.py) (the remote C
compiler) and [`../scripts/ppc-ar-remote.py`](../scripts/ppc-ar-remote.py) (the
remote archiver), [`../scripts/ppc-libc-probe.py`](../scripts/ppc-libc-probe.py)
and [`../scripts/ppc-libc-compare.py`](../scripts/ppc-libc-compare.py) (libc
ground truth).

Status (2026-07-27): **the full Rust standard library - core, alloc, std,
panic_unwind, test, libc - builds for `powerpc-apple-darwin` and links into a
running PowerPC Mach-O binary, and every one of the engine's 380 crates now
compiles**, including the engine itself (an 81 MB object from a 797 MB
translation unit). Only the final `rb-cli` link is outstanding.

Getting here took twenty-four mrustc fixes and two rustc-source patches, all
listed below.

Two classes of remaining work, and they are different in kind:

- **Buildability** - a per-crate tail of mrustc gaps. Each has been small and
  local, and they fail loudly.
- **Correctness** - several `libc` struct definitions describe modern macOS and
  are wrong for 10.4/10.5. That is the silent-at-compile-time,
  wrong-at-runtime class, and the only defence is measurement. See "The libc
  situation" below, and "The alignment problem" for the same lesson applied to
  the layout model.

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

### The remote archiver

mrustc only ever needs a compiler, but the engine's dependency graph contains
`-sys` crates, and *their* build scripts compile a C source tree of their own
through [cc-rs](https://docs.rs/cc). cc-rs looks up `CC_<triple>` by the same
convention mrustc does, so it picks up `ppc-cc-remote.py` for free - but then it
wants an **archiver**, and that one cannot be borrowed from the host: the host
`ar` writes a System V symbol table where Apple's linker wants a Mach-O
`__.SYMDEF`. So [`scripts/ppc-ar-remote.py`](../scripts/ppc-ar-remote.py) is
`AR_powerpc_apple_darwin`, and the archive is built where its objects were.

Measured on the G5 rather than assumed: Leopard's `/usr/bin/ar` takes `cq` and
`s` and rejects `D` (`ar: illegal option -- D`). That is exactly the probe cc-rs
makes - it tries `sD`, and on failure retries `s` with `ZERO_AR_DATE=1` - so a
non-zero exit from this wrapper is a legitimate answer and is passed straight
back rather than turned into an error. The result is a `current ar archive
random library`, which is what Apple's `ld` wants.

Two things about `ppc-cc-remote.py` exist only because of these crates:

- It mirrors *directories*, not just files. An **include** directory
  (`-I` / `-isystem` / `-iquote` / `-idirafter`) goes over recursively -
  bzip2-sys passes `-I bzip2-1.0.8`, and shipping the named `.c` alone leaves
  every `#include` unresolved. A **`-L`** directory is different: only the
  archives `-l` can resolve (`*.a`, `*.dylib`, `*.so*`) are shipped. mrustc puts
  `-L <stdlib output dir>` on every link and that directory is 75 MB of
  .c/.o/.rlib whose objects the link already names explicitly, so mirroring it
  wholesale is pure waste.
- It does **not** mirror blindly. `PPC_LDFLAGS` names paths that exist on the
  *Mac* (`-L/opt/local/lib`), and some of those prefixes exist on the build host
  too with entirely different contents, so anything under a system prefix is
  passed through as a remote path. An include tree over 64 MB is a loud failure
  rather than a silent giant rsync - that guard is what caught the stdlib
  directory being mirrored, before it was split out as a `-L` case.
- Relative paths are mirrored against the **cwd**, not the remote root.
  minicargo runs each build script with its cwd set to that crate's directory,
  so `src/foo.c` from two different crates would otherwise land on the same
  remote path and one crate would quietly be compiled from the other's source.

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

**Layout model** (see "The alignment problem" for the measurements behind these):

9. **`src/trans/codegen_c.cpp`** - `has_manual_align` compared against the largest
   *natural* field alignment, so it emitted no forcing attribute for a struct the
   C compiler would derive lower. (`libc`'s `tcp_connection_info`.)
10. **`src/trans/{target,codegen_c}.cpp`** - the member-alignment cap was applied
    unconditionally. It must skip members whose alignment is explicitly requested,
    as gcc's does, and that exemption propagates outward - including from the
    alignment attributes mrustc itself puts on unions. (`lzfse_rust`'s `FseCore`,
    `BTreeMap`'s `LeafNode`.)
11. **`src/trans/target.cpp`** - a niche enum took its size and alignment from
    per-variant layouts computed before the tag field was added, over-stating both.
    (`Result<u64, Error>`.)

**Parser** (both reached through `bitflags` 2.13, so neither is PowerPC-specific):

12. **`src/parse/root.cpp`** - `Parse_Impl_Item` handled an interpolated *function*
    only; an associated `const` arrives as `AST::Item::Static` and hit a `TODO`.
13. **`src/parse/root.cpp`** - attributes written in front of an interpolated item
    were dropped rather than transferred onto it, so a `#[cfg]` that should have
    removed the item did nothing.

**Build orchestration (minicargo):**

14. **`tools/minicargo/manifest.cpp`** - cargo creates the implicit
    `foo = ["dep:foo"]` feature for an optional dependency only when `[features]`
    does not name `dep:foo` itself; minicargo created it unconditionally. `rustix`
    has an optional `alloc` dependency (really `rustc-std-workspace-alloc`,
    referenced only as `dep:alloc` from `rustc-dep-of-std`) *and* a plain
    `alloc = []` feature, so merging the two made enabling the ordinary feature
    pull in the std-workspace shim. The guard already existed in the source but was
    commented out, and written as a global switch rather than cargo's
    per-dependency rule; this implements the per-dependency form.

15. **`tools/minicargo/build.cpp`** - under `MINICARGO_DEFER_CODEGEN` a dependency
    has to be repointed at the deferred codegen job. The guard doing that read
    `if( d[d.size()-1] != ')' )`, meaning "not already suffixed" - but a host
    crate's job name ends in `(host)`, so host dependencies kept pointing at the
    transpile job and could be linked against before their object file existed.
    Presented as an intermittent race; see the traps above.

**Target description:**

16. **`src/trans/target.cpp`** - every `*-apple-darwin` target declared
    `target_env = "gnu"`. rustc leaves it empty on Apple platforms, and it matters:
    `#[cfg(target_env = "gnu")]` was selecting glibc-specific code on a system with
    no glibc. `nix` picked its Linux `SigevThreadId` match arm that way and then
    failed on `libc::SIGEV_THREAD_ID`. Fixed for all five macOS targets. Note this
    is a **cfg** change, so it invalidates the PowerPC stdlib - see the traps above.
    (mrustc declares `"gnu"` for the BSD targets too, which is wrong for the same
    reason; left alone here as it is untested and out of scope.)

**Cross-build correctness (minicargo):**

17. **`tools/minicargo/build.cpp`** - a build script is native code, so minicargo
    builds it, runs it, and captures its stdout under `get_output_dir(true)` (the
    `host/` subdirectory when cross compiling) and runs it exactly once per
    package. But the crate compile that consumes the sources it *generated* derived
    `OUT_DIR` from its own host-ness, so on a cross build the two disagreed: the
    script wrote to `<out>/host/build_<pkg>/` and the crate was told to read
    `<out>/build_<pkg>/`. `get_output_dir` ignores its argument when not cross
    compiling, which is why every host stage passed and only PowerPC hit it -
    `crc32c` generates its CRC tables in `build.rs` and died on `Unable to open
    file '.../sw.table'` with no hint that a directory prefix was the problem.
    Both sides now go through one `get_build_script_out_dir()` accessor.

**Lifetimes:**

18. **`src/hir_typeck/{common.cpp,monomorph.hpp}`, `src/hir_expand/lifetime_infer.cpp`** -
    three asserts, one cause: a lifetime parameter whose index the recorded param
    list does not cover aborted the compile, in cases where that list was simply
    never populated. `radix_trie` hit it through `trait TrieCommon<'a, K, V>`, a
    trait that declares a lifetime on *itself* and uses it from a default method
    body; `compact_str` hit the higher-ranked equivalent one binder in, through
    `for<'a> &'a C: IntoIterator<Item = &'a I>`. Such a lifetime is now passed
    through rather than aborting - which is the treatment the surrounding code had
    already settled on twice: `Monomorphiser::monomorph_lifetime` has the same HRTB
    range check commented out with a TODO noting the params are not reliably in
    range once binders nest, and `sanity_check_lft` no longer rejects an HRL
    outright. Deliberately limited to lifetimes: the neighbouring `get_type` and
    `get_value` still assert, because an unresolved type or const genuinely breaks
    codegen, whereas mrustc erases lifetimes before codegen and does no borrow
    checking. Note this cannot change a crate that already compiled - an assert
    that did not fire has no effect - so it does **not** invalidate the prebuilt
    standard libraries.

19. **`src/hir_conv/constant_evaluation.cpp`** - `Expander::visit_path_params`
    evaluates const-generic arguments, and to find the type of each it calls
    `m_get_params`, a `std::function` that whichever enclosing *path* visitor is
    responsible for installing. The expression visitor nested in
    `Expander::visit_expr` forwards `visit_path` (which installs it) and
    `visit_path_params` (which needs it) but not `visit_generic_path` - so the
    default `ExprVisitorDef::visit_generic_path` ran instead and went straight to
    `visit_path_params` with the function still empty. The result was
    `std::bad_function_call`: `terminate` with no span, no phase, and nothing in
    the crate's debug log past `Constant Evaluate: V V V`, surfacing only as
    "Process was terminated with signal 6". Worth knowing as a *shape*: an mrustc
    abort with no diagnostic at all is a C++ exception escaping, and
    `gdb -ex 'break std::__throw_bad_function_call()' -ex run -ex bt` on the
    failing command names the line in one go. (Found on `quote` 1.0.47.)

20. **`src/parse/root.cpp`** - `Parse_Trait_Item` had no case for
    `TOK_INTERPOLATED_ITEM` at all, so an item reaching a *trait* body as an
    already-parsed `$item:item` fragment was a hard error. Fixes 12-13 grew that
    case for impl blocks (via `bitflags`); a trait body can hold one too.
    `crossterm`'s `stylize_method!` is the case - it emits each method through
    `calculated_docs!`, whose expansion is `$(#[doc = $doc] $item)*`, i.e. an
    attribute in front of an interpolated function, straight into `trait
    Stylize`. Simpler than the impl version, since a trait item *is* an
    `AST::Named<AST::Item>` and the fragment is just handed back; attributes are
    transferred onto it for the same reason as fix 13.

21. **`tools/minicargo/{build,jobs}.cpp`** - a **regression from fix 15**, plus
    two things that made it far harder to read than it should have been.
    `make_dep_codegen` appends `" (codegen)"` to every entry in a crate's
    dependency list under deferred codegen, and that list also holds the crate's
    own build-script *run* job - which has no codegen job, being a host binary
    built and run directly. The dependency became `quote v1.0.47 (script run)
    (codegen)`, which nothing ever announces, and 91 jobs deadlocked behind it.
    Fix 15 caused this: the original test (`not already ending in ')'`) excluded
    those names by accident, and correcting it for `(host)` removed that
    protection. It stayed hidden because it only fires when a build script
    actually has a job that run - most have cached output and `bs_job_name` comes
    back empty. Alongside it:

    - the deadlock now **lists** the waiting jobs and each unmet dependency.
      "Nothing runnable or running, but jobs are still waiting" names neither the
      job nor the dependency, and a job-name mismatch is exactly what it is most
      likely to be.
    - it now sets `failed`. `run()` returns `!failed`, so a deadlock exited
      **zero** - minicargo reported success and `build-ppc.sh` announced an
      `rb-cli` that had never been linked. `stage_ppc` now also checks for the
      binary, which `stage_host` always did.

**The proc-macro bridge** (three gaps, all reached through ratatui's tree):

22. **`lib/libproc_macro/src/lex.rs`** - the escape arm of the string-literal
    lexer had *no cases at all*, so every escape hit the fallback panic. That
    panic runs inside the proc-macro child, so the compiler never sees an error,
    only the child going away: `Unexpected EOF while reading from child process`.
    `indoc`'s `formatdoc!` (via `instability`) emits a doc string containing `\n`.
    Now handles `\n \r \t \0 \\ \' \"`, `\xNN`, `\u{...}` and the end-of-line
    continuation, plus the two missing char-literal escapes next to it. **Lives
    in the plugin-side library**, so `libproc_macro` must be rebuilt and every
    plugin relinked for the fix to take effect - `bin/minicargo --output-dir
    output-<ver> lib/libproc_macro`.

23. **`src/expand/proc_macro.cpp`** - `visit_item` had no `Trait` case, so an
    attribute macro on a trait aborted with "TODO: visit_item - Trait". Because
    that kills the compiler mid-conversation the plugin then reports its own
    "Unexpected EOF", so it presents as two unrelated failures. ratatui puts
    `#[instability::unstable(..)]` on `WidgetRef` and `StatefulWidgetRef`.
    `visit_trait` mirrors `visit_impl`; two things arise only here:
    trait items carry no visibility of their own (mrustc records them as `pub`,
    and emitting it yields `trait X { pub fn .. }`, which the plugin's parser
    rejects), and a trait method *declaration* has no body, where
    `visit_function` dereferenced `fcn.code()` unconditionally. Associated types
    are emitted un-bounded only; their bounds are stored as `Self: ...`, which is
    not the shape the declaration site needs, so a bounded one is a loud TODO
    rather than a silently dropped bound.

A note on reading these: **an mrustc abort with no diagnostic at all is a C++
exception escaping**, and an "Unexpected EOF while reading from child process" is
usually the *plugin* dying, not the bridge. For the first,
`gdb -ex 'break std::__throw_bad_function_call()' -ex run -ex bt` on the failing
command names the line immediately. For the second, the child's own panic message
appears in the log a few lines *above* the mrustc error - the compiler's message
is the consequence, not the cause.

**Layout, again:**

24. **`src/trans/codegen_c.cpp`** - a type whose alignment is 1 is now emitted
    inside a `#pragma pack(1)` region even when it is not `repr(packed)` itself,
    because gcc ignores the pack applied where a *member's* type was defined and
    re-derives from that member's natural alignment. See "`#pragma pack` does not
    propagate to a containing type" above for the measurements. Restricted to
    align 1, where Rust guarantees a tight layout, so `pack(1)` provably cannot
    move a field and is a no-op wherever gcc already agreed. The `MaybeUninit`
    union wrapping the same type is fixed transitively - which matters, since its
    `aligned(1)` could never have lowered it. Verified with
    `scripts/ppc-layout-probe.py`: libzip 5 mismatches -> 0 across 1728 structs,
    and the whole PowerPC stdlib rebuilds clean. **This is a layout change, so it
    invalidates the PowerPC stdlib** - see the traps.

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
| `ppc`      | PowerPC `rb-cli` | **all 380 crates compile**; final link outstanding |
| `probe`    | capture libc ground truth from the 10.4u / 10.5 SDKs | ok |

### Traps in the build loop

Six that have each cost real time:

- **A layout change invalidates the PowerPC stdlib.** `minicargo`'s own rebuild
  check *does* compare against the compiler binary's timestamp
  (`outfile_needs_rebuild`, `tools/minicargo/build.cpp`), so the `ppc` stage
  rebuilds itself after `make`. `minicargo.mk`'s `LIBS` target does not, so the
  **stdlib** silently stays as it was. That happened: the stdlib in
  `output-1.74.0-powerpc-apple-darwin` predated both alignment fixes, and the
  engine was being compiled against a newer layout model than the libs it links
  with. **After touching `src/trans/target.cpp` or `src/trans/codegen_c.cpp`,
  `rm -rf` the PowerPC lib output directory and re-run `ppclibs` before `ppc`.**
- **A killed build leaves a 0-byte `build_<crate>.txt` that minicargo trusts.**
  The build-script runner's stdout is captured by shell redirection, which
  truncates the file *before* the process starts. If that run then dies, the empty
  file survives, looks up to date, and the crate is compiled with **no** cfgs at
  all. For `libc` that silently drops `libc_core_cvoid`, so `libc::c_void` becomes
  a distinct type from `core::ffi::c_void` and the failure lands two crates later
  in `getrandom` with no hint of where it came from. If a crate fails
  inexplicably, check `output-rb-ppc/host/build_<crate>.txt` for zero length.
- **mrustc reports itself as `rustc 1.29.100` unless `MRUSTC_TARGET_VER` is set.**
  Build scripts version-gate on that. `libc`'s emits 6 cfgs at 1.29 and 14 at
  1.74 - among the missing eight is `libc_core_cvoid` again. This is the same
  trap as the previous bullet with a different cause and the identical symptom.
- **Don't `tail` the build log.** Redirect to a file and grep it. A run reported
  as three failing assertions actually had four; the fourth was below a `tail -30`
  cutoff and cost a session's worth of wrong hypotheses.
- **An "intermittent race" around host binaries was a real ordering bug** - fixed,
  but worth knowing because it wore three different disguises before it was
  diagnosed: `Unable to run process ... Permission denied` on a build-script
  runner, `Unable to open crate '<x>' at path .../lib<x>-plugin` for a plugin
  whose `.c` had been emitted, and finally `ld: cannot find ...rlib.o` with two
  named host crates, which gave it away. Under `MINICARGO_DEFER_CODEGEN` a
  dependency has to be repointed at the codegen job; the guard doing that skipped
  any name already ending in `)`, and a host crate's job name ends in `(host)`.
  See fix 15 below. If something like this reappears, running the emitted
  `<output>-codegen.sh` by hand tells you immediately whether the step itself is
  broken or merely mis-sequenced.
- **An aborted build leaves `.rlib` files with no `.o`, and minicargo trusts
  them.** Same family as the 0-byte `build_<crate>.txt` above: the transpile job
  completed, the deferred codegen job never ran, and on the next run the `.rlib`
  looks up to date so no codegen job is scheduled - the link then fails on a
  missing `.rlib.o` for a crate the log never even mentions. (`build.cpp` has a
  standing `TODO: Codegen should re-run if the output file from it is missing`.)
  To find them:

  ```sh
  cd output-rb-ppc && for d in . host; do for f in $d/*.rlib; do
      [ -e "$f.o" ] || echo "stale: $f"; done; done
  ```

  **Repair it by running the codegen script, not by deleting the `.rlib`.**
  mrustc already emitted one next to the artifact:

  ```sh
  sh output-rb-ppc/lib<crate>-<tag>.rlib-codegen.sh    # produces the missing .o
  ```

  Deleting the `.rlib` also works, but it makes that crate dirty and therefore
  everything downstream of it - and if the crate is anywhere under the engine,
  "downstream" includes the engine's 797 MB translation unit, which is hours.
  That mistake was made here for a *43 KB* `zstd-safe` object.

  **How this gets created in the first place is worth knowing, because it is easy
  to do to yourself:** running a single crate's mrustc command by hand to test a
  fix - which is the normal debugging loop - produces the `.rlib` and, under
  `MINICARGO_DEFER_CODEGEN`, only *emits* the codegen script rather than running
  it. So every standalone crate test leaves exactly this footprint. Run the
  check above afterwards, or just run the emitted script too.

  **And the symptom is misleading.** A missing object is not remapped by
  `ppc-cc-remote.py` (it only mirrors paths that exist locally), so the local
  path is passed to the Mac verbatim - where `/home` is an autofs mount that
  answers `Input/output error` rather than `No such file`:

  ```
  gcc: error: /home/dani/repos/mrustc/output-rb-ppc/libzstd_safe-....rlib.o: Input/output error
  ```

  That reads like a disk fault on a path that looks perfectly reasonable. It is a
  missing file. Check the link's inputs exist locally before suspecting hardware:

  ```sh
  tr ' ' '\n' < output-rb-ppc/rb-cli_cmd.txt | tr -d '"' | grep '\.o$' \
    | while read f; do [ -f "$f" ] || echo "MISSING: $f"; done
  ```

## The engine is one 797 MB translation unit

mrustc emits **one `.c` per crate**, and there is no way to ask it for more -
there is no codegen-units concept anywhere in `src/trans/`. For the engine crate
that one file is **797 MB**, and it is the single most expensive thing in this
build by a wide margin.

Two consequences, both of which have bitten:

**It exhausts a 32-bit compiler.** gcc's peak memory scales with the whole unit's
IR, and `cc1` here is a 32-bit binary (`Mach-O executable ppc`), so at `-O1` it
ran out of its own *address space* - about 3.5 GB on Darwin - after 25 minutes:

```
cc1: out of memory allocating 65536 bytes            (at ~2.9 GB RSS)
```

**Swap is not the constraint, and raising it is wasted effort.** Darwin's
`dynamic_pager` grows swap on demand: it went 64 MB -> 1 GB unaided during one of
these runs, with 45 GB of disk free. A 32-bit process cannot address more no
matter how much backing store exists.

`ppc-cc-remote.py` therefore gives an oversized unit its own flags, and only it -
every other crate keeps what mrustc asked for:

| flag | why |
|---|---|
| `-O0` | the significant one. At `-O1` gcc runs inter-procedural passes that need many function bodies live at once; at `-O0` it emits each function and releases it |
| `--param ggc-min-expand=10` | collect far more often than the default "let the heap grow 30% between collections" |
| `--param ggc-min-heapsize=32768` | and start doing so early |

Result: an **81 MB `Mach-O ppc_7400` object**, peaking around 2.3 GB with RSS
*falling* during the run as the collector reclaimed. Tunable with
`PPC_BIG_TU_BYTES` / `PPC_BIG_TU_ARGS`; `PPC_BIG_TU_BYTES=0` disables the
special-casing entirely.

**Note the engine is consequently built unoptimised.** That is the right trade
for having a binary at all, but it is a knob to revisit once it runs.

**It makes anything that dirties the engine cost hours.** One compile is 70
minutes with a warm page cache and closer to 3 hours without. So the dependency
graph matters: anything the engine depends on, if invalidated, drags the engine
with it. Deleting a 43 KB `zstd-safe` artifact cost three hours here for exactly
that reason (see the traps above), and taking the first `APP_VERSION` stamp on an
already-built tree cost another three.

If this ceiling returns, in increasing order of cost: squeeze harder on flags
(`--param ggc-min-expand=0`, `-fno-var-tracking`); split the crate across
translation units, which is a **genuine mrustc feature** and not a flag; or build
a 64-bit `cc1` (the G5 is a PPC970 and Leopard has a 64-bit userland, so a
`ppc64` gcc would lift the ceiling while still emitting 32-bit target code - but
that means rebuilding the bootstrap toolchain on a 2005 machine).

## Version stamping

The release pipeline sets `RELEASE_VERSION` to `date -u +"%Y-%m-%d-%H-%M"` and
`build.rs` turns it into `cargo:rustc-env=APP_VERSION=<it>`. That whole path
works unchanged under mrustc - minicargo parses `cargo:rustc-env` and passes it
to the crate compile - so `build-ppc.sh` only has to set the variable.

What does **not** carry over is the staleness guard. `build.rs` protects itself
with `rerun-if-env-changed=RELEASE_VERSION`, and **minicargo does not implement
rerun-if-env-changed at all**: once `build_rb-cli-ppc-*.txt` looks current the
script never re-runs, so `APP_VERSION` would be pinned to whatever the first
build stamped. The version is therefore recorded in `<output>/.release-version`
and the build-script output dropped only when it genuinely changes.

Dropping it re-transpiles the engine (hours), so the stamp is taken once and
reused, and the *first* stamp on an existing tree deliberately adopts the value
without invalidating anything - there is nothing stale to correct when nothing
recorded a version before.

```sh
scripts/build-ppc.sh ppc                                   # stamp once, then reuse
RELEASE_VERSION=2026-07-27-04-50 scripts/build-ppc.sh ppc  # set explicitly
rm <output>/.release-version                               # take a fresh stamp
```

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
- **zstd-sys** — `patch_zstd_sys_vendor` drops `features = ["parallel"]` from its
  `cc` build-dependency. It is the only crate in the graph asking for it, and
  `parallel` is what compiles cc's `src/parallel/` — the only `async` in the
  crate, and mrustc emits no `Future` impl for an async block. cc keeps a serial
  arm behind `#[cfg(not(feature = "parallel"))]`, and cc never reaches the
  PowerPC binary, so the only cost is that the C compiles one file at a time.
- **signal-hook** — `patch_signal_hook_vendor` spells the one call to
  `AddSignal::add_signal` as UFCS. The trait takes an arbitrary self type
  (`self: Arc<Self>`) and the call used method syntax on a trait object, which
  mrustc cannot resolve. Not avoidable by dropping a feature: crossterm's
  `events` needs signal-hook-mio, which genuinely imports
  `signal_hook::iterator::backend`.
- **signal-hook-mio** — `patch_signal_hook_mio_vendor` turns
  `use $pipe as Pipe;` into `type Pipe = $pipe;` inside
  `implement_signals_with_pipe!`. mrustc cannot parse a `use` whose path is an
  interpolated fragment followed by `as` — having consumed the path it insists on
  `::` (`Unexpected token TOK_RWORD_AS, expected TOK_DOUBLE_COLON`). `Pipe` is
  only ever used as a type here (`SignalDelivery<Pipe, E>`, `Pipe::pair()`), so
  the alias binds it identically. **Same `TOK_RWORD_AS` family as the libyml gap
  that keeps `yaml` off for this target** — see the open items; one parser fix
  would plausibly clear both.
- **instability** — `patch_instability_vendor` rewrites its three
  `indoc::formatdoc!` doc strings as plain `format!` with the string already
  unindented (byte-identical output). A proc macro that *forwards* a token from
  its input loses that token's hygiene context crossing mrustc's proc-macro
  bridge, and `formatdoc!` re-emits its trailing arguments verbatim, so
  `formatdoc!{"...{}.", version.trim_start_matches('v')}` produced a `format!`
  whose `version` no longer resolved to the `if let Some(ref version)` binding
  around it (`Couldn't find variable name 'version'`). `format!` is a builtin, so
  this takes the proc macro out of the picture. See the open items - the bridge
  is the real bug.
- **rustyline** `default-features = false` (keeping `with-file-history`,
  `with-dirs`) — drops `custom-bindings`, the only thing pulling `radix_trie`.
  The engine uses `DefaultEditor`, `ReadlineError` and the history calls, none of
  which are gated on it.
- **serde_json** `>=1, <1.0.147` — 1.0.147 swapped its float formatter from `ryu`
  to `zmij`, and zmij aborts mrustc (`Mismatched types - f64 and f32`). Identical
  JSON; only the shortest-round-trip float printer differs.
- **hashbrown** `default-features = false` — the entry is a version pin for
  indexmap, but as a direct dependency it also enabled 0.14's default `ahash`,
  which pulls `zerocopy`, whose const-generic `HasField` impls mrustc
  mis-selects. indexmap asks for `default-features = false` itself.
- **chrono** — `patch_chrono_vendor` in build-ppc.sh turbofishes
  `DateTime::<Utc>::UNIX_EPOCH` at its two use sites. Nothing in
  `DateTime::UNIX_EPOCH.naive_utc()` pins `Tz`; rustc resolves it because only
  `impl DateTime<Utc>` declares that associated const, but mrustc cannot infer an
  impl's type parameter from which impl happens to carry the const. Same class of
  gap as the crc turbofish.
- **rustversion** — `patch_rustversion_vendor` makes it identify the compiler from
  the last line of `rustc --version` that actually starts with `rustc `, rather
  than the last line full stop. Real rustc prints one line; mrustc prints four,
  with the version banner first and informational lines after. Fixing this in
  mrustc is not obviously safe - `libc`'s build script parses from the *start* of
  the same output and mrustc's own comments note that `autoconfig` looks for the
  `release:` line, so neither reordering nor trimming is free.
- **YAML** (`serde_yml` -> `libyml`) - an mrustc macro-expansion gap
  (`TOK_RWORD_AS` at `scanner.rs:1937`). **Done:** the engine gates YAML output
  behind a `yaml` feature, on by default everywhere else, and this build leaves it
  off. `serde_yml`/`libyml` leave the dependency graph entirely, `--format yaml`
  is hidden from `--help` and rejected, and JSON carries the identical schema.

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

### What the cap actually applies to

"Members after the first are capped to 4" is the summary, not the rule. The rule
gcc implements is in `stor-layout.c:place_field`:

```c
#ifdef ADJUST_FIELD_ALIGN
  if (! DECL_USER_ALIGN (field))
    desired_align = ADJUST_FIELD_ALIGN (field, TREE_TYPE (field), desired_align);
#endif
```

`ADJUST_FIELD_ALIGN` is where the Darwin/PowerPC cap lives, and it is skipped for
any member whose alignment was **asked for explicitly**. So the cap applies to
*natural* alignment only. gcc tracks "explicit" as `TYPE_USER_ALIGN`, propagates
it from an array's element type to the array, and ORs it into a struct from
**every** member - unconditionally, whether or not that member's alignment is what
set the struct's.

Three consequences, all **measured on the G5** (see the probe below), all of which
mrustc now models:

| | gcc |
|---|---|
| `struct { u16; struct{u64;u32;}; }` - interior aggregate, natural align 8 | 20/**4** - capped |
| `struct { u16; struct{...} __attribute__((aligned(8))); }` | 16/**8** - exempt |
| ...through an array wrapper and one more struct, two levels down | 48/**8** - still exempt |

The last one is the shape mrustc emits, and it is why `lzfse_rust`'s `FseCore`
failed: it embeds a `repr(align(8))` `VEntry` array 808 bytes in, so gcc gives the
whole struct align 8 where mrustc said 4.

The counter-intuitive one is that unconditional OR. These two differ only in that
one has a member carrying `aligned(2)` - far *below* the 4-byte cap, and not what
gives either struct its alignment:

```c
struct att2        { uint16_t x; } __attribute__((__aligned__(2)));
struct contaminated { uint64_t lead; struct att2 tag; };   /* 16/8 */
struct clean        { uint64_t lead; uint16_t   tag; };   /* 16/8 */
```

Standalone they are identical, 16/8. Interior, `contaminated` comes out **24/8**
and `clean` **20/4** - the `aligned(2)` member has permanently exempted its whole
enclosing struct from the cap. mrustc reproduces this (`TypeRepr::user_align` is
set by *any* user-aligned member, not only one that raises the alignment).

**mrustc creates user-alignment itself, and has to account for it.** `codegen_c`
pins every union's alignment with an explicit attribute rather than letting the C
compiler derive it - without that, `MaybeUninit<u128>` comes out 1-aligned because
its first variant is the unit type. That attribute is user-alignment as far as gcc
is concerned, so it exempts the union *and every aggregate containing it* from the
cap. `BTreeMap`'s `LeafNode<String, Metric>` is the case: it holds a
`[MaybeUninit<Metric>; 11]` 140 bytes in, and gcc makes the whole node **320/8**
where mrustc's model said 316/4. So `TypeRepr::user_align` is set for every union,
not only for types the *Rust* source marked `repr(align)`.

Note what does **not** work as an escape: `__attribute__((aligned(4)))` on the
enclosing struct does not pull it back down. Measured - `{u16; u16; struct{union
aligned(8) [2];};}` is 24/8 with or without an `aligned(4)` on the outer struct,
and a plain `{u64; u32;}` marked `aligned(4)` is still 16/8. gcc's "aligned can
only increase" holds here; the alignment has to come out of the model correctly.

### `#pragma pack` does not propagate to a containing type

The mirror image of the cap, and the one that bites `repr(packed)` Rust types.
gcc derives a containing type's alignment from its member's **natural**
alignment, ignoring any `#pragma pack` that was in force where that member's type
was *defined*. Measured on the G5, with `#pragma pack(1) struct p4 { uint32_t a;
uint16_t b; }` (itself a correct 6/1):

| | gcc |
|---|---|
| `struct { p4 v; }` | 8/**4** - the pack is ignored, and the size padded |
| `struct { uint8_t t; p4 v; }` | 8/**4** |
| `struct { p4 v; }` **defined inside** `pack(1)` | 6/**1** |

So a type that merely *contains* a packed type has to be emitted inside a pack
region itself; `aligned(1)` cannot express it, by the "aligned can only increase"
rule above. This is what `ManuallyDrop<T>` and `MaybeUninit<T>` are - transparent
wrappers that inherit `T`'s align 1 - and zip's `#[repr(packed, C)]` block headers
go through both. mrustc said 30/1 for `ManuallyDrop<ZipLocalEntryBlock>`, which is
right; gcc said 32/4.

The trap for the *model* is that mrustc's `c_max_align` is built from each
member's own alignment, and a packed member honestly reports 1 - so this looks
like a case where C already agrees, and neither the pack pragma nor
`has_manual_align` fires. See fix 24.

Two mrustc bugs came out of this, both fixed on `rb-cli-vintage-build`:

1. **`make_type_repr_struct__inner` capped user-aligned members.** Now exempt, and
   `TypeRepr::user_align` propagates outward through struct, union, enum and array
   exactly as gcc's `TYPE_USER_ALIGN` does - seeded both by Rust's `repr(align(N))`
   and by mrustc's own union pinning. `codegen_c.cpp`'s decision about whether to
   emit a forcing `__attribute__((aligned))` uses the same test.
2. **`make_type_repr_enum`'s niche path over-stated size and alignment.** It takes
   `max_align` from per-variant layouts built *before* the tag field is added, so a
   payload that will not be first in the final layout was laid out as though it
   were, and escaped the cap. `Result<u64, Error>` came out 16/8 where the emitted
   C - a union of the final variant structs, both 12/4 - is 12/4. It now takes the
   answer from the final variant layouts, guarded on the capping ABI so no other
   target moves.

### Measuring it: `scripts/ppc-layout-probe.py`

The `sizeof_assert` / `alignof_assert` typedefs only ever say *that* mrustc and gcc
disagree, never *what gcc computed*. For a nested aggregate that is not enough to
work from, and guessing here produces a subtle miscompile rather than a loud
failure.

[`scripts/ppc-layout-probe.py`](../scripts/ppc-layout-probe.py) gets gcc's real
numbers. Appending a `main()` that prints `sizeof` does not work - the emitted
translation unit refers to libcore symbols and will not link - so it never links at
all. Each probe is a deliberately ill-typed initialiser whose diagnostic carries
the number:

```c
#line 1 "PROBE|size|FseCore"
char (*p)[sizeof(struct s_..._FseCore0g)] = 1;
/* warning: initialization of 'char (*)[7976]' from 'int' ... */
```

A `#line` directive tags each probe, `-fsyntax-only` skips codegen, and a 5 MB
translation unit answers in seconds. Every struct in the file with an assertion is
probed and diffed against what mrustc claimed:

```sh
PPC_HOST=admin@192.168.99.116 \
  scripts/ppc-layout-probe.py ~/repos/mrustc/output-rb-ppc/liblzfse_rust-0_2_1.rlib.c

struct                          mrustc      gcc         verdict
...fse8fse_core7FseCore0g       7976/4      7976/8      ALIGN MISMATCH
667 struct(s) probed, 4 mismatch(es).
```

`--synthetic` skips the input file and compiles the hand-written ABI cases in the
table above instead, which is the quickest way to re-derive the rule on new
hardware or a new gcc.

A note on reading build output: **do not `tail` it.** The run that found this had
been reported as three failing assertions; it was four, and the fourth (the
`Result<u64, Error>` enum) was simply below the `tail -30` cutoff. Redirect to a
file and grep.

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

### Caveat: `repr(C)` structs that contain a union

Pinning union alignment (see "The alignment problem") makes a union
user-aligned in the emitted C, which exempts any struct containing it from the
member-alignment cap. For a `repr(C)` struct that is *also* declared by a system
header, that is a real hazard: mrustc and gcc will agree with each other and
both disagree with the SDK. It is bounded, though - `libc`'s Apple module
declares only four unions (`__c_anonymous_ifk_data`, `__c_anonymous_ifr_ifru`,
`__c_anonymous_ifc_ifcu`, `semun`), reaching `ifreq`, `ifconf`, `ifkpi` and the
SysV IPC structs. None are on the engine's file paths, and none are in the
verified probe set - `ppc-libc-compare.py` already lists 17 structs it cannot
resolve because of unions and function pointers, and these are among them.
Worth revisiting if raw-socket or IPC code ever crosses.

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
   workaround for the power rule applying to Rust's own types; see "The alignment
   problem" for what a real fix costs. (The *model* bugs described there are
   fixed - this is the separate, remaining wart.)
3. **Eight libc structs are wrong even on 10.5** - `statfs`, `passwd`,
   `ipc_perm`, `semid_ds`, `shmid_ds`, `rt_metrics`, `malloc_zone_t`,
   `vnode_info`. None are on the engine's file paths; `passwd` (`home_dir`) is
   the one worth doing first.
4. **The engine itself** (`scripts/build-ppc.sh ppc`), which resolves **404
   crates**. Cleared since the last revision:

   - ~~`libyml`'s `TOK_RWORD_AS` macro-expansion gap~~ - YAML sits behind a
     default-on `yaml` feature that this build leaves off. `libyml` no longer
     appears in the build at all.
   - ~~host `libc` 0.2.189's `new::linux::can::j1939`~~ - a manifest pin drops the
     `nix 0.31` dependency that dragged in a post-`src/new/` libc, so the build
     uses 0.2.155.
   - ~~`lzfse_rust`'s `FseCore` / `LzfseDecoder` / `LzfseRingDecoder` alignof
     assertions~~ and ~~`Result<u64, Error>`'s sizeof assertion~~ - two mrustc
     layout-model bugs; see "The alignment problem".
   - ~~`bitflags` 2.13 aborting mrustc through `nix`'s `libc_bitflags!`~~ - two
     parser bugs in `Parse_Impl_Item`'s interpolated-item path: an associated
     `const` arrives as `AST::Item::Static` and hit a `TODO`, and attributes
     written in front of an interpolated item were dropped, so the `#[cfg]` that
     should have removed a Linux-only flag constant did nothing.
   - ~~`nix` reaching for `libc::SIGEV_THREAD_ID`~~ - `target_env` was `"gnu"` on
     every Apple target, so glibc-only code was cfg'd *in*.

   - ~~`cc`'s `async fn` build-command runner~~ - `zstd-sys` is the only crate in
     the graph that asks for cc's `parallel` feature, and cc gates the whole
     `src/parallel/` module (the only `async` in the crate) on it. Dropping the
     feature from the vendored manifest deletes the problem; cc is a
     build-dependency and never reaches the PowerPC binary, so the only cost is
     that its C compiles serially. Cheaper than pinning cc back, and it keeps
     `native-zstd` in the build.
   - ~~the `-sys` crates could not compile C for the target~~ - cc-rs picks up
     `CC_powerpc_apple_darwin` for free, but it needed `-I` *directories*
     mirrored and an `AR_powerpc_apple_darwin`. See "The remote archiver".
   - ~~minicargo pointed a cross-compiled crate at the wrong `OUT_DIR`~~ - fix 17.
   - ~~`radix_trie` and `compact_str` lifetime aborts~~ - fix 18. radix_trie is
     also out of the graph now (rustyline's `custom-bindings`, which the engine
     does not use, was the only thing pulling it).
   - ~~`signal-hook`'s `Arc<Self>` receiver on a trait object~~ - the one call
     site is spelled UFCS by `patch_signal_hook_vendor`.
   - ~~`zmij` and `zerocopy`~~ - both arrived transitively and neither was wanted:
     serde_json is held below the release that swapped `ryu` for `zmij`, and
     hashbrown's `ahash` default (the only thing pulling zerocopy) is off.
   - ~~`quote` aborting with no diagnostic at all~~ - fix 19.
   - ~~`zeroize_derive`'s `#![crate_type = "proc-macro"]`~~ - fix 21.
   - ~~`crossterm`'s attribute macro on a trait, and `indoc`'s escaped doc
     string~~ - fixes 22-23.
   - ~~`zip` resolving `flate2` as a relative path~~ - not an mrustc bug at all:
     `deflate-flate2` does not enable the optional `flate2` dependency (that is
     `deflate`'s job, via `flate2/rust_backend`), so the manifest needed the
     implicit `flate2` feature adding. It would have failed the same way under
     real cargo.
   - ~~`zstd-safe`'s deref-coercion abort~~ - `ptr_mut(&mut *output)` spells out
     what mrustc could not infer.
   - ~~`ManuallyDrop`/`MaybeUninit` over `repr(packed)` failing their layout
     assertions~~ - fix 24.

   **Every crate now compiles.** What is left is the final link. Each blocker has
   fallen into one of three buckets, in increasing order of what it costs to fix:
   a feature or version change in `rb-cli-ppc/Cargo.toml`, a `patch_*_vendor`
   rewrite in `build-ppc.sh`, or a change to mrustc itself. **Prefer the first two
   while the frontier is still moving.** A change to mrustc's source updates the
   compiler binary, and minicargo's `outfile_needs_rebuild` compares against it -
   so every mrustc fix invalidates all several-hundred crates already built and
   costs a full rebuild, where a manifest or vendor change is incremental. Batch
   the mrustc work.
5. **C++ deps stay off forever** (`chd` = libchdman). Already excluded.
6. **Enum and union alignment is unasserted.** mrustc emits an `alignof_assert`
   for every struct but only a `sizeof_assert` for enums and unions (in `libtest`:
   2,027 struct alignment assertions, 0 for its 691 enums and 81 unions). On a
   target where the alignment rules are this subtle that is a real hole in the
   safety net - an enum whose alignment mrustc gets wrong is caught only through
   the size it rounds up to, or through an enclosing struct. Emitting the missing
   assertions is a small codegen change and the obvious next hardening step.
7. **`TOK_RWORD_AS` in the parser - now worth fixing.** Two crates have hit it:
   `libyml`, which is why the `yaml` feature is off for this target, and
   `signal-hook-mio`'s `use $pipe as Pipe;`, where mrustc consumes an
   interpolated path fragment and then demands `::` rather than accepting `as`
   (`Unexpected token TOK_RWORD_AS, expected TOK_DOUBLE_COLON`). Both are worked
   around today - YAML by a feature gate, signal-hook-mio by a type alias - but
   the second occurrence makes the parser the right place to fix it, and doing so
   would plausibly let `yaml` come back on. Confirm the two are the same root
   cause first: libyml presents as "Unused tokens at the end of macro expansion"
   at `scanner.rs:1937` rather than as a parse error, so it may be a different
   manifestation.
8. **A proc macro loses the hygiene of tokens it forwards.** An identifier taken
   from a proc macro's input and re-emitted in its output comes back with an
   empty hygiene context and no longer resolves to the binding it named at the
   call site. `instability` hit this through `indoc::formatdoc!`, which re-emits
   its trailing arguments verbatim: `formatdoc!{"...{}.", version.trim_start_matches('v')}`
   became a `format!` that could not see the enclosing `if let Some(ref version)`
   binding. Confirmed with `MRUSTC_DEBUG=Expand` - the expansion is correct
   token-for-token, and the forwarded ident is the only one carrying
   `/*Rust2021 /**/*/`. Worked around by rewriting the call sites, because the
   fix is in the bridge's hygiene handling and is a much larger change. This is
   worth watching: derive macros mostly emit *new* code referring to types, which
   is why nothing else in ~380 crates has tripped it, but any macro that forwards
   a caller's local will.
