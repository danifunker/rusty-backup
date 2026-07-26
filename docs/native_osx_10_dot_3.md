# Native rb-cli for Mac OS X 10.3 / 10.4 / 10.5 (PowerPC)

Status: **IN PROGRESS - the hard part is done.** The Rust standard library builds
for `powerpc-apple-darwin` and runs on real hardware, so the engine can target
real `std` and the `no_std` carve-out this plan was originally built around is
off the table. Scope is deliberately limited to `rb-cli`; the GUI is a separate
question (see the end). See [`build-ppc-mrustc.md`](build-ppc-mrustc.md) for the
runnable build.

## Where this stands (2026-07-25)

**The Rust standard library builds and runs on PowerPC.** `core`, `alloc`, `std`,
`panic_unwind`, `panic_abort`, `test`, `libc`, `hashbrown`, `compiler_builtins`
and `std_detect` all transpile for `powerpc-apple-darwin`, compile to PowerPC
Mach-O, and link into a binary that executes on a Power Mac G5 under Leopard
10.5.8. This was the top risk in section 3 and section 10, and it is retired.

**The two-machine pipeline is automatic.** There is no usable
`powerpc-apple-darwin` cross-gcc, so
[`scripts/ppc-cc-remote.py`](../scripts/ppc-cc-remote.py) *is* the C compiler as
far as mrustc is concerned: it ships each emitted `.c` to the Mac over ssh, runs
MacPorts gcc 10.5.0 there, and copies the `.o` back. minicargo's dependency
graph, parallelism and incremental rebuilds keep working across both machines.
libcore's 29.7 MB of C compiles on the G5 in ~104 seconds.

**Eight fixes were needed**, all recorded in
[`build-ppc-mrustc.md`](build-ppc-mrustc.md) and committed to the mrustc fork's
`rb-cli-vintage-build` branch: four mrustc compiler fixes (an over-strict
integer-literal-suffix lexer that made libcore unbuildable for every `powerpc*`
target; two missing const-eval intrinsics reached only on 32-bit targets; union
alignment being left to the C compiler), one target-descriptor fix (declaring
64-bit atomics, which libatomic provides, plus `-l atomic`), one minicargo knob,
and two rustc-stdlib patches (macOS-only code that assumed x86 or assumed
libdispatch). Several are not PowerPC-specific and are candidate upstream PRs.

**The libc premise was wrong, in a good way.** This plan said a
`powerpc-apple-darwin` libc arch file "must be created". In fact `libc`'s
`unix/bsd/apple/b32` module is arch-agnostic and **compiles for this target
unchanged**, and `target_arch = "powerpc"` even selects the right symbol variants
by accident (the `$UNIX2003` / `$INODE64` overrides are gated on
`target_arch = "x86"`, and 10.4 has none of those symbols). What is wrong is
struct *layout*: libc's `apple` module describes modern macOS.

**How wrong is now measured, not guessed.**
[`scripts/ppc-libc-probe.py`](../scripts/ppc-libc-probe.py) compiles
`sizeof`/`alignof`/`offsetof` probes on the real G5 against the real SDKs, and
[`scripts/ppc-libc-compare.py`](../scripts/ppc-libc-compare.py) diffs the result
against libc's Rust declarations. Ground truth for 10.4u and 10.5 is checked in
under `rb-cli-ppc/probe/`.

On **10.5, 86 structs match exactly, 6 differ only in field naming, and 8 are
genuinely wrong** - `statfs`, `passwd`, `ipc_perm`, `semid_ds`, `shmid_ds`,
`rt_metrics`, `malloc_zone_t`, `vnode_info`. `stat` and `dirent` are *correct*,
confirmed end to end: a PowerPC binary reports `/etc/hosts` at 236 bytes with the
right mode/ino/uid/gid/nlink/mtime/blocks/dev and counts `/usr/lib` at 390
entries, both matching `stat -f` on the machine.

**Tiger is the real gap, and it is a symbol problem before a layout one.** 10.4's
libSystem exports **zero** `$INODE64` symbols, so a binary linked as today's is
cannot even launch there - the dynamic linker fails on `_stat$INODE64` before
`main`. A `powerpc-apple-darwin` arch file has to bind the plain symbols and
declare 10.4's legacy structs (`stat` 96 bytes with a 4-byte `st_ino`, `statfs`
272, `dirent` 264 - all measured). `_Unwind_GetIPInfo` is likewise absent from
`libgcc_s.10.4`.

**The alignment model is now measured rather than inferred.** Darwin/PowerPC's
"power" alignment ABI (a struct's alignment follows its first member; later
members are capped to 4) disagrees with Rust's `repr(C)` rule, and mrustc has to
model it because it delegates layout to the C compiler. Getting that model right
turned out to hinge on a detail the summary hides: gcc applies the cap to
*natural* alignment only, skipping any member whose alignment was requested
explicitly, and that exemption propagates outward through arrays and enclosing
aggregates. mrustc creates such alignment itself, too, by pinning every union's
alignment with an attribute. Three mrustc bugs came out of that, all fixed and
all verified against gcc 10.5 on the G5 with
[`scripts/ppc-layout-probe.py`](../scripts/ppc-layout-probe.py), which reads
gcc's real `sizeof`/`__alignof__` out of its own diagnostics rather than guessing.
See "The alignment problem" in the build doc.

**One wart is still open.** The power rule applies to Rust's own types, where a
field can land at an offset its own `align_of` rejects -
`std::thread::Inner`'s `ThreadId` is the case, and `ptr::write`'s
`assert_unsafe_precondition!` then aborts every program inside `std::rt::init`.
The stdlib is built with `MINICARGO_NO_DEBUG_ASSERTIONS=1`; the generated C is
correct, only the assertion disagrees. Restricting the rule to `repr(C)` was
tried and fails differently.

**The engine build runs, and the blockers have changed character.** Both things
this plan predicted needed *engine* changes, and both are done:

- ~~`libyml` - the known `TOK_RWORD_AS` macro-expansion gap~~ **done**: YAML output
  sits behind a default-on `yaml` feature that this build leaves off, so
  `serde_yml`/`libyml` leave the dependency graph. Verified: `libyml` no longer
  appears in the build at all.
- ~~`libc` 0.2.189 built for the host - `new::linux::can::j1939`~~ **done**: a
  manifest pin drops the `nix 0.31` dependency whose `libc >= 0.2.186` requirement
  dragged in a post-`src/new/` libc. The build uses 0.2.155, which predates it.

Everything hit since has been an **mrustc** bug rather than an engine or
dependency problem - six of them so far, listed under phase 2 - and each has been
small and local. That is the shape this plan expected for the tail, but the tail's
length is still unknown.

**Revised direction:** finish the engine transpile (section 8, phase 2), then link
and smoke-test `rb-cli` on 10.5. Tiger is a separate, well-scoped follow-up
(phase 5). The `os/` platform layer stays out and remains hand-C, as section 5
describes.

> **Sections 1-12 were rewritten on 2026-07-25** against the working build. The
> `no_std` carve-out, the FAT-only filesystem scope and the hand-C ABI boundary
> that earlier revisions were organised around are all **gone** - they existed to
> dodge a libstd port that turned out to work. Anything describing them is
> historical and is marked as such.

## 1. Goal

Ship a native PowerPC `rb-cli` that runs on Mac OS X Tiger (10.4) and Leopard
(10.5), **single-sourcing the engine logic from the existing Rust** instead of
hand-porting it to C a second time.

We already have a hand-written C port in [`ppc-tiger/`](../ppc-tiger/) that works
(verified on a dual-G4 running 10.4.11). This plan is the *next evolution* of
that port: replace the hand-C engine with **the real engine, transpiled to C via
[mrustc](https://github.com/thepowersgang/mrustc)**, so the whole thing - not
just the bug-prone kernels - stops drifting from the desktop build. Only the
platform layer (`os/`: device enumeration, raw disk IO) stays hand-written C.

This is a rewrite of *how the engine gets there*, not of what the CLI does.

**On 10.3 (Panther):** earlier revisions listed it alongside 10.4 and 10.5.
Treat it as aspirational rather than targeted. Everything is currently built and
run on 10.5, 10.4 is a scoped follow-up with measured ground truth (section 8,
phase 5), and nothing has been checked against Panther at all - it predates even
the `$UNIX2003` conformance symbols, and MacPorts gcc10 does not target it. The
hand-C `ppc-tiger/` port remains the answer there.

## 2. Why C at all (recap)

Modern rustc/LLVM dropped `powerpc-apple-darwin` years ago - you cannot
`--target` your way onto these machines. The escape hatch is to emit **C99** and
compile it with a PPC-Darwin C toolchain. mrustc's default backend emits exactly
that C, and **`powerpc-apple-darwin` is a built-in mrustc target**
(`src/trans/target.cpp`, `ARCH_POWERPC` / `CodegenMode::Gnu11`).

**Toolchain - NOT stock gcc-4.0.** mrustc-emitted C requires `<stdatomic.h>`
(C11). Stock Tiger/Xcode gcc-4.0 and Leopard's gcc-4.2 do not have it. The build
box therefore uses **MacPorts gcc 10.5.0**, which on the G5 lives at
`/opt/local/libexec/gcc10-bootstrap/bin/gcc` (`powerpc-apple-darwin9`) - note
that path, not `/opt/local/bin/gcc-mp-10`; MacPorts is only partly extracted
there and `port` itself is not installed. gcc10 also supplies `libatomic` and
emulated TLS, both of which are load-bearing. **MacportsLegacySupport**
(`/opt/local/lib`) backfills `pthread_setname_np` and friends.

This is all confirmed working, not projected: libcore's 29.7 MB of emitted C
compiles on the G5 in ~104 seconds, and the resulting binaries run.

## 3. The version problem (resolved)

> This section drove the whole original plan toward a 1.54 / edition-2018
> `no_std` core. Every claim that pushed it there has since been checked and
> found wrong. The corrections are kept because they are the reason the plan
> changed shape twice.

We build on **rustc 1.96, edition 2021**; mrustc bootstraps from the **1.74.0**
source, and that is what the PowerPC target uses.

**CORRECTION 1 (2026-07-23) - the version wall is far lower than first written.**
The earlier revision claimed "1.54 safest, 1.74 partial, 1.90+ actively broken on
PPC" and concluded the core must target the **1.54 language subset**. That
reflected older second-hand PPC-Darwin experience, not upstream mrustc. **1.74 is
a supported baseline** - one minor version above
[`rb-cli-vintage/`](../rb-cli-vintage/), which already compiles the *entire shared
engine* at rustc 1.73 / edition 2021.

For scale, the abandoned 1.54 plan would have cost, across `src/` (340k lines,
406 files): **1,530** inline-format-arg sites (1.58), **450** `let ... else`
(1.65), **355** `div_ceil` (1.73), **22** `is_some_and` (1.70), plus dropping the
tree to edition 2018. Library gaps are shimmable -
[`src/rust173_compat.rs`](../src/rust173_compat.rs) already does that for 1.87's
`is_multiple_of` - but **syntax and edition gaps are not**. Do not go down this
path.

**CORRECTION 2 (2026-07-23) - proc macros are NOT a hard stop.** mrustc
implements them (`src/expand/proc_macro.cpp`, 2,094 lines: it builds the macro
crate for the host and drives it over a token-stream pipe). The 108
`Serialize`/`Deserialize` derives, 93 `clap` derives and 4 `thiserror` in this
tree are not fatal. Confirmed in practice - the PowerPC engine build gets 93
crates deep, derives and all.

**CORRECTION 3 (2026-07-25) - libstd for `powerpc-apple-darwin` works.** This was
the last remaining blocker and the one the `no_std` core existed to dodge. It is
true that PowerPC-Darwin is not on mrustc's CI-tested libstd list, and that a
target descriptor is the cheap part. But the port turned out to be eight fixes,
not a project: see "Where this stands" above. `core`, `alloc`, `std`,
`panic_unwind`, `panic_abort`, `test` and `libc` all build and run.

### What survives from this section

Two constraints, both still real:

1. **C++ dependencies are a hard stop.** `libchdman-rs` is C++; mrustc cannot
   help. Any transpiled configuration must leave `chd` off, as
   `rb-cli-vintage`'s feature set already does.
2. **`-sys` crates compile real C for the target**, so they inherit the C
   toolchain. `zstd-sys` is the one in our graph; it has not been reached yet
   (the build stops earlier), so treat it as unproven.

## 4. Target scope

> **Rewritten 2026-07-25.** Earlier revisions scoped this to five verbs and
> FAT12/16/32 only. That was a consequence of the `no_std` carve-out - hand-porting
> filesystem kernels one at a time is expensive, so you pick one. With real `std`
> there is no carve-out and no per-driver cost, so the scope is now "the engine,
> minus what genuinely cannot cross".

### In scope - the portable engine

Everything `rb-cli-vintage` builds: **every filesystem driver**, partition tables
(MBR / GPT / APM / RDB), backup, restore, inspect, browse-view editing, fsck,
the image builders, the ratatui TUI. These are ordinary Rust over byte buffers
and they transpile like any other crate.

### Out of scope - and why

| Excluded | Reason |
|---|---|
| `src/os/` (device enumeration, raw disk IO, elevation) | `objc2-*` targets modern macOS frameworks; `nix` is Linux-only. This is the platform layer, and it is hand-C on PowerPC (section 5). Excluding it also drops `libc`/`nix` from the graph, which removes a current build blocker. |
| `chd` / `libchdman-rs` | C++. mrustc cannot help, at all, ever. |
| The egui GUI | eframe/wgpu will not transpile and would not run on 10.3-10.5 GPUs anyway (section 12). |
| YAML output (`serde_yml` -> `libyml`) | An mrustc macro-expansion gap (`TOK_RWORD_AS`). Gated out via the default-on `yaml` feature, which `rb-cli-ppc` leaves off; JSON carries the identical schema. |

The first PowerPC `rb-cli` therefore operates on **disk-image files**.
Raw-device IO arrives with the hand-C platform shell, not before.

### Dependency deviations

`rb-cli-ppc/Cargo.toml` is a third top-level package (sibling of
`rb-cli-vintage`) that reuses `../src` but carries the deviations mrustc forces,
so `rb-cli-vintage` stays pristine for the x86_64-10.7 / Win7 builds. Each is
documented at its site; the current set is `zip`'s deflate backend (drops
`zopfli` -> `bumpalo`), `env_logger` pinned to 0.10 (drops `jiff`), and a
turbofish patch applied to vendored `crc`. See
[`build-ppc-mrustc.md`](build-ppc-mrustc.md).

## 5. Architecture

> **Rewritten 2026-07-25.** The previous version of this section described a
> `#![no_std]` `rusty-backup-core` crate exposing a C ABI to a hand-written C
> shell, with `metadata.json` hand-emitted in the core and no serde. That design
> existed solely because libstd for this target was assumed unusable. It is not,
> so none of it is needed. It is recorded in git history if the assumption ever
> reverses.

```
    ../src  (the shared engine, unchanged, real std, edition 2021)
        |
        |  rb-cli-ppc/Cargo.toml  -- same engine, mrustc-forced dep deviations
        |
        v
    mrustc --target powerpc-apple-darwin  -->  C99
        |
        |  scripts/ppc-cc-remote.py  (stands in as the C compiler, over ssh)
        v
    gcc 10.5.0 on the PowerPC Mac  -->  PowerPC Mach-O
        |
        +-- linked against: libSystem, libatomic, MacportsLegacySupport,
        |                   libgcc_s.1, rb-cli-ppc/shim/ppc-compat.o
        v
    rb-cli  (PowerPC, operates on disk images)

    [later] hand-written C platform shell for os/: device enumeration,
            raw disk IO, elevation -- reusing ppc-tiger/
```

**The engine is not modified for PowerPC.** That is the point of the whole
exercise: a desktop bug-fix in a filesystem driver propagates for free. The only
PowerPC-specific code in this repo is build tooling
([`scripts/`](../scripts/)), the dependency manifest
([`rb-cli-ppc/Cargo.toml`](../rb-cli-ppc/Cargo.toml)), and one small C file of
libc/libm stand-ins
([`rb-cli-ppc/shim/ppc-compat.c`](../rb-cli-ppc/shim/ppc-compat.c)).

### The platform boundary

The split is no longer "pure computation vs I/O" - the engine does its own file
I/O through `std::fs`, which works. The split is now just **`os/`**: enumerating
physical disks, opening `/dev/rdiskN`, unmounting, asking for elevation. That
is a small, genuinely platform-specific surface, it is what `ppc-tiger/` already
implements in C, and it is the natural seam.

## 6. What actually has to change

> **Rewritten 2026-07-25.** The previous coupling audit catalogued what it would
> take to make `partition/` and `fs/fat.rs` `no_std`-clean (strip `std::io`
> bounds, swap `HashMap` for `BTreeMap`, drop serde and `thiserror`). None of
> that is needed now; `std`, serde, `thiserror` and `HashMap` all work.

The remaining work is not in the engine's logic at all. It is:

1. ~~**A feature gate that excludes `os/`**~~ **Done, as a platform-leaf swap.**
   The measured surface turned out to be small enough that replacing all of `os/`
   was unnecessary: `os/mod.rs`'s `SectorAlignedReader`/`Writer`, `TempFileGuard`,
   `ElevatedSource`, `DeviceWriteHandle` and `get_file_size` are already portable
   Rust, and `wakelock` already ships a no-op arm. Only `os/macos.rs` is hostile
   (IOKit/DiskArbitration via `objc2`, ~50 `libc` calls), and only **9** of its
   items are referenced. So the `os-stub` feature swaps just that file for
   `os/macos_stub.rs` via `#[path]`, leaving the portable trunk and all 51 call
   sites untouched. `objc2-*` leaves the graph.

   Note the diagnosis this corrected: the host-`libc` abort was **not** caused by
   `os/`. It came from `cc`/`jobserver`, the build-dependencies of `bzip2-sys` and
   `zstd-sys`, and `libc` cannot leave this build's graph at all - it is used
   outside `os/` by `remote/service.rs`, `cli/verbs/tui_app.rs` and
   (macOS-gated) `fs/resource_fork.rs`. The fix was a manifest pin: `nix 0.31` is
   declared linux-only and never compiled for this target, but a target-agnostic
   lockfile let its `libc >= 0.2.186` requirement drag in `src/new/`, which mrustc
   cannot resolve through its glob re-export. Dropping that dep pins libc to
   0.2.155, which predates `src/new/` entirely.
2. ~~**A feature gate that excludes YAML output**, for the `libyml` macro gap.~~
   **Done.** `yaml` is a default-on feature; `rb-cli-ppc` leaves it off, which
   drops `serde_yml`/`libyml` from the graph, hides `yaml` from `--format`, and
   leaves JSON carrying the identical schema. The `Yaml` enum variant stays (so
   the verbs' `Json | Yaml` match arms are untouched) and is marked
   `#[value(skip)]` when the feature is off.
3. **The hand-C platform shell**, later, once there is a working image-only
   `rb-cli` to attach it to.

Items 1 and 2 are the only changes that touch `src/`, and both are additive
feature gates rather than rewrites - consistent with keeping the shared engine
single-sourced. Item 2 landed as four lines of `#[cfg]` in two files plus the
manifests, which is the shape to aim for with item 1.

## 7. Build pipeline

Fully automated across two machines; see
[`build-ppc-mrustc.md`](build-ppc-mrustc.md) for the runnable commands.

```sh
export PPC_HOST=admin@<the PowerPC Mac>
scripts/build-ppc.sh ppclibs    # PowerPC core/alloc/std/panic_unwind/test/libc
scripts/build-ppc.sh ppc        # PowerPC rb-cli
scripts/build-ppc.sh probe      # re-capture libc ground truth from the SDKs
```

The "cross vs on-device" question earlier revisions left open is **settled: both,
automatically.** mrustc runs on the fast machine; `scripts/ppc-cc-remote.py` is
registered as `CC_powerpc_apple_darwin`, so every codegen step ships its `.c`
over ssh, compiles it with gcc10 on the Mac, and copies the `.o` back. minicargo's
dependency graph, parallelism and incremental rebuilds all keep working across
the boundary, and there is no manual "scp the output and compile it by hand"
step. The wrapper also owns the platform link line and builds the shim.

## 8. Phased plan

> **Rewritten 2026-07-25.** Phases 0-4 in earlier revisions were about carving
> out and validating a `no_std` core, kernel by kernel. That work is moot.

**Phase 0 - toolchain spike. DONE.** mrustc builds, targets
`powerpc-apple-darwin`, and its emitted C compiles with gcc10 on the G5. The
`<stdatomic.h>` / C11 hurdle is cleared. This was the go/no-go gate and it is
green.

**Phase 1 - PowerPC standard library. DONE.** `core`, `alloc`, `std`,
`panic_unwind`, `panic_abort`, `test`, `libc`, `hashbrown`, `compiler_builtins`,
`std_detect` - 20 PowerPC Mach-O objects. Verified by running a binary on the G5
that exercises `println!`, `BTreeMap`, iterators, `AtomicU64`, threads,
`fs::metadata` and `read_dir`, with the filesystem results checked field-by-field
against `stat -f` on the machine. Cost: eight fixes (see "Where this stands").

**Phase 2 - finish the engine transpile.** *(in progress)* Both scope-downs from
section 6 have landed - YAML is feature-gated out and `os-stub` replaces the macOS
platform leaf - and the host-`libc` blocker is fixed by a manifest pin. The
blockers met since are all mrustc bugs, each small and local, and each fixed on
`rb-cli-vintage-build`:

- **The alignment model** (three bugs). The "power" ABI's member-alignment cap
  applies to *natural* alignment only; gcc exempts anything explicitly aligned, and
  that exemption propagates outward - including from the alignment attributes
  mrustc itself puts on unions. Separately, a niche enum took its size and
  alignment from variant layouts computed before the tag was added. Between them
  these accounted for `lzfse_rust`'s `FseCore` / `LzfseDecoder` /
  `LzfseRingDecoder`, `Result<u64, Error>` and `BTreeMap`'s `LeafNode`.

  The previous revision of this plan said the guesswork loop was not converging and
  that a proper harness was needed. That harness is
  [`scripts/ppc-layout-probe.py`](../scripts/ppc-layout-probe.py): it never links,
  it reads gcc's real `sizeof`/`__alignof__` out of its own diagnostics, and it
  settled the question in one run. Every rule above is measured, not inferred - and
  one of them (an `aligned(2)` member permanently exempting its enclosing struct
  from a 4-byte cap) is counter-intuitive enough that guessing would have got it
  wrong.
- **Two parser bugs**, both reached through `bitflags` 2.13 and neither
  PowerPC-specific: an interpolated associated `const` in an `impl` hit a `TODO`,
  and attributes written in front of an interpolated item were dropped, so a
  `#[cfg]` that should have removed an item did nothing.
- **One target-description bug**: every `*-apple-darwin` target declared
  `target_env = "gnu"`, cfg'ing glibc-only code *in* on a platform with no glibc.

Expect a further tail. Deliverable: every crate in the PowerPC configuration
transpiles and compiles.

**Phase 3 - link and smoke-test on 10.5.** Get `rb-cli` linked, then exercise it
against disk images on the G5: `inspect`, `backup`, `restore`, a browse-view
listing, an `fsck`. Compare output against the desktop build on the same images -
byte-identical `metadata.json` and checksums is the bar. Deliverable: a native
PowerPC `rb-cli` doing real work on Leopard.

**Phase 4 - the hand-C platform shell.** Reattach `os/` as C: device enumeration,
`/dev/rdiskN`, unmount, elevation, reusing `ppc-tiger/`. Deliverable:
`list-devices` and backing up a physical CF card.

**Phase 5 - Tiger (10.4).** A `powerpc-apple-darwin` `libc` arch file that binds
the plain (non-`$INODE64`) symbols and declares 10.4's legacy `stat` (96 bytes),
`statfs` (272) and `dirent` (264) - all measured and checked in under
`rb-cli-ppc/probe/`. Also resolve `_Unwind_GetIPInfo`, absent from
`libgcc_s.10.4`: either gcc10's own unwinder or `panic=abort`. Deliverable: the
same binary running on `/Volumes/MacOS TigerLNX`.

**Phase 6 - clean-up and upstreaming.** The alignment wart (section 10), the
cosmetic `STD_ENV_ARCH` bug, and PRs for the mrustc fixes that are not
PowerPC-specific.

Phases 2-4 are independently useful; 5 is the one that satisfies the original
10.4 goal. Nothing here removes the hand-C `ppc-tiger/` port, which still works.

## 9. Decision gate - answered

Earlier revisions ended with a real question: the hand-C `ppc-tiger/` port
already works, so is this worth doing? The gate was written assuming the prize
was single-sourcing *two kernels* (partition parsing and FAT compaction) at the
cost of a `no_std` carve-out - a thin prize when that logic is stable.

**That is no longer the trade.** With libstd working, the prize is the **whole
engine** - every filesystem driver, every verb - with no carve-out and no
engine-side rewrite. The earlier text called that "a categorically better deal",
and it is now the actual situation rather than a hypothetical.

The gate is therefore closed in favour of continuing. The remaining cost is
build-tooling work and a per-crate mrustc tail, not a parallel implementation to
maintain.

## 10. Risks / open questions

- **The "power" alignment wart - the one real design problem.** Darwin/PowerPC
  gives a struct the alignment of its *first* member, so an 8-byte member that is
  not first is only 4-aligned. mrustc models this and must, because it delegates
  layout to the C compiler and emits `sizeof`/`alignof` static assertions that
  fail otherwise. But the rule then also applies to Rust's own types, where a
  field can land at an offset its own `align_of` rejects -
  `std::thread::Inner`'s `ThreadId` is the concrete case, and `ptr::write`'s
  `assert_unsafe_precondition!` aborts every program inside `std::rt::init`
  because of it. Worked around with `MINICARGO_NO_DEBUG_ASSERTIONS=1`; the
  generated C is correct, only the assertion disagrees. Restricting the rule to
  `repr(C)` was tried and fails differently. A real fix means mrustc emitting
  explicit padding plus forced alignment for `repr(Rust)` types instead of
  delegating - a much larger change. See "The alignment problem" in the build doc.
- **The per-crate mrustc tail.** 188 of 404 crates so far. Each gap met to date
  has been small and local (a turbofish, a feature swap, a dependency pin), but
  the tail length is genuinely unknown until phase 2 runs to completion.
- **`zstd-sys` is unproven.** It compiles real C for the target and has not been
  reached yet. If it fights, the `native-zstd` feature can come off.
- **C++ dependencies remain a hard stop.** `chd` stays excluded permanently.
- **10.4 vs 10.5 is a symbol problem, not just a layout one.** Tiger exports zero
  `$INODE64` symbols, so today's binaries cannot launch there at all. Phase 5.
- **Eight libc structs are wrong even on 10.5** - `statfs`, `passwd`, `ipc_perm`,
  `semid_ds`, `shmid_ds`, `rt_metrics`, `malloc_zone_t`, `vnode_info`. None are on
  the engine's file paths; `passwd` (`home_dir`) is the one worth doing first.
- **`std::env::consts::ARCH` reports `x86_64`.** Cosmetic: minicargo.mk sets
  `STD_ENV_ARCH` from the host triple, overriding the build-script override set.
  One-line fix, noted so nobody mistakes it for something deeper.
- **Allocator pressure** on a G3/G4 with limited RAM - keep the streaming block
  model, never buffer a whole partition. Unchanged from the original plan and
  still worth watching.
- ~~libstd for `powerpc-apple-darwin`~~ - resolved, see section 3.
- ~~Proc macros~~ - resolved, see section 3.
- ~~Cross vs on-device build ergonomics~~ - resolved, see section 7.
- ~~HashMap iteration order changing output~~ - moot; `HashMap` is used as-is,
  there is no `BTreeMap` swap.

## 11. Prior art / references

- [`ppc-tiger/`](../ppc-tiger/) — the working hand-C port. `rust_cli_real.c`
  (CLI, 2,251 lines), `rust_runtime_v2.c` (runtime stubs), `malloc_wrapper.c`
  (PIC/allocator fix), `build.sh`. The C shell here is ~80% reusable.
- [rust-ppc-tiger](https://github.com/Scottcjn/rust-ppc-tiger) — the transpiler
  reference used to guide the original hand port.
- [mrustc](https://github.com/thepowersgang/mrustc) — the Rust->C transpiler this
  plan depends on.
- PowerPC build tooling in this repo: [`scripts/build-ppc.sh`](../scripts/build-ppc.sh)
  (driver), [`scripts/ppc-cc-remote.py`](../scripts/ppc-cc-remote.py) (the remote
  C compiler), [`scripts/ppc-libc-probe.py`](../scripts/ppc-libc-probe.py) and
  [`scripts/ppc-libc-compare.py`](../scripts/ppc-libc-compare.py) (libc ground
  truth), [`rb-cli-ppc/`](../rb-cli-ppc/) (manifest, shim, captured probe data).

## 12. GUI (out of scope here, noted for later)

The egui GUI does **not** come across - eframe/wgpu won't transpile and wouldn't
run on 10.4/10.5 GPUs regardless. A native GUI is possible but is a **hand-written
Carbon frontend** (HIToolbox + Navigation Services, as in
`ppc-tiger/rusty_backup_gui.c`) driving the transpiled engine. That's a separate
plan.

One thing this section used to insist on is now moot: it said the core's ABI
"should be designed with a progress **callback** boundary from day one so the GUI
can drive long ops on a worker thread without a retrofit". That was because the
GUI would have had to reach the `no_std` core through a hand-written C ABI. There
is no such ABI - a Carbon frontend would link the transpiled engine directly and
use its ordinary Rust progress interfaces, exactly as the CLI does. Nothing needs
deciding early.
