# Native rb-cli for Mac OS X 10.3 / 10.4 / 10.5 (PowerPC)

Status: **PLAN ONLY — not started.** A design to revisit later. Scope is
deliberately limited to `rb-cli`; the GUI is a separate question (see the end).

> **§3 was materially corrected on 2026-07-23** against the mrustc source. Two
> claims that shaped this plan — that PPC needs the rustc 1.54 baseline, and that
> proc macros are a hard stop — turned out to be wrong. The single-`no_std`-core
> architecture in §5 was a consequence of those claims, so **read §3 before
> treating §5 as settled.**

## 1. Goal

Ship a native PowerPC `rb-cli` that runs on Mac OS X Panther (10.3), Tiger
(10.4), and Leopard (10.5), **single-sourcing the engine logic from the existing
Rust** instead of hand-porting it to C a second time.

We already have a hand-written C port in [`ppc-tiger/`](../ppc-tiger/) that works
(verified on a dual-G4 running 10.4.11). This plan is the *next evolution* of
that port: replace the hand-C engine with a **Rust core transpiled to C via
[mrustc](https://github.com/thepowersgang/mrustc)**, so the bug-prone kernels
(partition parsing, FAT compaction remap, checksums) stop drifting from the
desktop build. The platform shell stays hand-written C.

This is a rewrite of *how the engine gets there*, not of what the CLI does.

## 2. Why C at all (recap)

Modern rustc/LLVM dropped `powerpc-apple-darwin` years ago — you cannot
`--target` your way onto these machines. The escape hatch is to emit **C99** and
compile it with a PPC-Darwin C toolchain. mrustc's default backend emits exactly
that C, and **`powerpc-apple-darwin` is a built-in mrustc target** (confirmed at
`src/trans/target.cpp:632` in the local clone `/Users/dani/repos/mrustc`).

**Toolchain gotcha — NOT stock gcc-4.0.** mrustc-emitted C requires
`<stdatomic.h>` (C11). Stock Tiger/Xcode-2.x `gcc-4.0` does **not** have it, so
the mrustc path needs **MacPorts gcc + MacportsLegacySupport**, not the system
compiler. (The hand-C `ppc-tiger/` port uses stock `gcc-4.0.1`; the mrustc path
cannot.) `@catap` maintains a working MacPorts port of this exact chain — proof
the toolchain works end-to-end.

## 3. The version problem (and the discipline it forces)

> **Corrected 2026-07-23** by reading the local mrustc clone
> (`/Users/dani/repos/mrustc`) directly, instead of the second-hand summary this
> section previously rested on. **Two load-bearing claims here were wrong** — they
> are what pushed this plan toward a 1.54 / edition-2018 `no_std` core. Both
> corrections are inline below.

We build on **rustc 1.96, edition 2021**. mrustc's own README states its
baselines:

- **1.90.0** — currently tested to *fully bootstrap*, binary-equal against
  1.91.1. This is upstream's headline version.
- **1.19.0 / 1.29.0 / 1.39.0 / 1.54.0 / 1.74.0** — also supported, "might still
  bootstrap (assuming the right environment)".
- mrustc's default `rust-version` is **1.29.0**, and the tree ships
  `rustc-1.74.0-{src.patch,overrides.toml}` and `rustc-1.90.0-*` next to the
  older ones.

**CORRECTION 1 — the version wall is far lower than previously written here.**
The earlier revision claimed "1.54 safest, 1.74 partial, 1.90+ actively broken on
PPC" and concluded the core must target the **1.54 language subset**. That table
reflects `@catap`'s older PPC-Darwin experience, not upstream mrustc today.
**1.74 is a supported baseline** — and it is one minor version above
[`rb-cli-vintage/`](../rb-cli-vintage/), which already compiles the *entire shared
engine* at **rustc 1.73 / edition 2021**. That is a vastly smaller gap than
edition 2018.

For scale, here is what the abandoned 1.54 plan would have cost, measured across
`src/` (340k lines, 406 files): **1,530** inline-format-arg sites (1.58), **450**
`let … else` (1.65), **355** `div_ceil` (1.73), **22** `is_some_and` (1.70), plus
dropping the whole tree to edition 2018. Library gaps are shimmable —
[`src/rust173_compat.rs`](../src/rust173_compat.rs) already does exactly that for
1.87's `is_multiple_of` — but **syntax and edition gaps are not**. Do not go down
this path.

**CORRECTION 2 — proc macros are NOT a hard stop.** The earlier revision stated
"Proc macros are a hard stop" and used it to justify never transpiling the app.
mrustc implements them: `src/expand/proc_macro.cpp` is **2,094 lines** (it builds
the macro crate for the host and drives it over a token-stream pipe). The 108
`Serialize`/`Deserialize` derives, 93 `clap` derives and 4 `thiserror` in this
tree are therefore not automatically fatal.

### The gap that actually matters: libstd for `powerpc-apple-darwin`

mrustc's README lists its **CI-tested libstd targets**: x86-64 Linux GNU and
x86-64 Windows MSVC, with x86-64 / arm64 macOS as secondary.
**PowerPC-Darwin is not on that list.**

The *target spec* exists and is well-formed — `src/trans/target.cpp:632` defines
`powerpc-apple-darwin` as `ARCH_POWERPC`, `CodegenMode::Gnu11`, with
`m_emulated_i128 = true` (set for 32-bit PPC; clear on the `powerpc64-apple-darwin`
entry directly below it). But a target descriptor is the cheap part. A working
`std` — libc bindings, threads, filesystem, process — on Mach-O/PPC is the
project, and nobody upstream validates it.

Ordered blockers, then:

1. **libstd on `powerpc-apple-darwin`** — untested upstream. This is the real
   work, and it is precisely what `no_std` was proposed to dodge.
2. **A PPC-Darwin C toolchain** — MacPorts gcc + MacportsLegacySupport, not stock
   `gcc-4.0.1` (see §2).
3. **`-sys` crates** — `zstd-sys` compiles real C for the target, so it inherits
   blocker 2. Note that `rb-cli-vintage`'s feature set
   (`native-zstd,remote,tui`) already excludes `chd`: **libchdman is C++**, which
   mrustc cannot help with at all.

### What this means for the plan below

The `no_std` core in §5 is **no longer forced** by proc macros. It may still be
the right shape — it sidesteps blocker 1, the expensive one — but it is now a
*choice*, and the alternative deserves a fair hearing: point mrustc at rustc-1.74
and try to build `rb-cli-vintage` roughly as-is.

**Cheapest next experiment**, and it costs no changes to this repo: build mrustc,
point it at the 1.74 source, and try `--target powerpc-apple-darwin` on a
hello-world. That answers the single unknown which decides between the two
shapes. If libstd collapses immediately, the `no_std` core is vindicated; if it
survives, the whole `no_std` restructuring may be unnecessary.

## 4. Target scope

### In scope — the MVP command set (proven by the hand-C port)

| Command | Notes |
|---|---|
| `list-devices` | enumerate disks (platform C: `rdisk`, ioctl, `statfs`) |
| `backup` | MBR/EBR/APM + superfloppy; FAT compaction; gzip; crc32/sha1 |
| `restore` | reconstruct disk from backup dir; handles `.gz` |
| `inspect` | read `metadata.json`, print partition summary |
| `rip` | optical disc -> `.iso` |

### Filesystem scope — **FAT12/16/32 only**

The vintage CF/SD use case is DOS/Win9x, which is FAT. The desktop build has ~70
fs drivers; **none of the others come across.** FAT compaction (skip
unallocated clusters, remap cluster chains, patch BPB/dir entries) is the one
filesystem kernel worth single-sourcing — it is subtle and has bitten us before
(the FAT-below-4085-cluster truncation bug). Everything non-FAT is a
sector-by-sector raw copy.

### Explicitly OUT of scope

The other ~42 rb-cli verbs and their machinery: `fsck` (all filesystems),
`archive`/`sit`/`binhex`, `serve`/remote, optical authoring, `expand`/`grow`/
`shrink`/`resize`/`repack`, `bless`/`make-bootable`, `convert`, all the
`new-*` image builders, browse-view editing (`ls`/`cp`/`rm`/`put`/`get`), CHD
(libchdman is a C++ dep we will not transpile), zstd. If a later phase wants one,
it gets added deliberately, not by default.

## 5. Architecture

> **Conditional on §3.** This shape was chosen when proc macros looked like a
> hard stop and 1.54 looked mandatory. Neither holds. It remains a reasonable
> design *if* mrustc's `powerpc-apple-darwin` libstd proves unusable — which is
> the experiment §3 ends with. If libstd works, transpiling `rb-cli-vintage`
> largely as-is is the cheaper route and this whole section is moot.

```
rusty-backup-core     (#![no_std] + alloc, 1.74 subset, deps ~= none)
        |   mrustc -> C99 -> gcc-4.0.1
        |   exposes a stable C ABI (extern "C")
        v
+-------------------------------+
| hand-written C platform shell |
|   - argv parsing              |
|   - device enumeration/IO     |  <- ppc-tiger/rust_cli_real.c territory
|   - file IO, gzip (zlib)      |
|   - sha1/crc32 (CommonCrypto) |
|   - malloc GlobalAlloc bridge |  <- ppc-tiger/malloc_wrapper.c
+-------------------------------+
        v
   rb-cli-ppc  (~single Mach-O, PPC)
```

### The C ABI boundary

The core is pure computation over buffers. It does **no I/O itself** — the C
shell reads bytes, hands slices to the core, and writes what the core returns.
Rough surface (to be firmed up):

- `partition table parse`: `(bytes, len) -> parsed layout struct`
- `fat compaction plan`: `(fat region bytes, bpb) -> {clusters to copy, remap
  table, patched BPB/dir bytes}` — the C side then streams only those clusters
- `metadata emit`: core produces the `metadata.json` byte buffer (we hand-roll
  JSON in the core — no serde)
- `checksum`: prefer delegating crc32/sha1 to CommonCrypto in C; only vendor a
  no_std impl if we want it single-sourced

Progress/streaming stays entirely on the C side (loops over sectors, calls the
core per-region). No callbacks needed for the CLI — this matters more for a
future GUI.

## 6. Coupling audit (what actually has to change)

Findings from the current tree:

- **`partition/mbr.rs`, `apm.rs`** — already parse `&[u8; 512]` / `&[u8]`
  slices. Nearly no_std-ready. The one hostile spot is `parse_ebr_chain(reader:
  &mut (impl Read + Seek))` — replace the `std::io` bound with a local minimal
  `trait ByteSource { fn read_at(off, buf) }` or just pre-read the EBR sectors on
  the C side and pass slices.
- **`fs/fat.rs`** — uses `std::collections::HashMap`/`HashSet` (swap to
  `alloc::collections::BTreeMap`/`BTreeSet` or vendor `hashbrown`) and
  `std::io::{Read,Seek,Write}` (replace with slice-based API). `VecDeque`,
  `Vec`, `String` are all in `alloc` — fine.
- **serde** — 23 files derive `Serialize`. The core cannot use it. `metadata.json`
  is hand-emitted in the core (ppc-tiger already hand-writes this exact format in
  C, so there is a reference).
- **`error.rs`** — `thiserror` (proc macro) won't transpile; the core uses a
  plain `enum` + hand-written `Display`, or returns integer codes across the ABI.
- Everything under `os/`, `backup/` orchestration, `restore/`, `device.rs` is
  I/O-bound and **stays in the C shell**, not the core.

The core crate is therefore a *carve-out*, mostly of `partition/` (the pure
parts) + `fs/fat.rs` (compaction math), rewritten to no_std. Estimated core
surface: low thousands of LOC, not the 285k of the full tree.

## 7. Build pipeline

1. `cargo` (host, 1.96) builds `rusty-backup-core` normally as a lib for CI
   testing — it must compile and pass unit tests on the desktop too.
2. mrustc (local clone at `/Users/dani/repos/mrustc`, pinned build) transpiles
   `rusty-backup-core` + `core`/`alloc` to C, targeting `powerpc-apple-darwin`.
3. Transfer the emitted C + the hand-written shell C to a Tiger box (or a
   `powerpc-apple-darwin8` cross-toolchain if we can stand one up).
4. **MacPorts gcc + MacportsLegacySupport** (NOT stock `gcc-4.0` — mrustc's C
   needs C11 `<stdatomic.h>`) compiles + links against zlib + CommonCrypto.
   `@catap`'s MacPorts port is the reference for getting this toolchain up.
5. Output: `rb-cli-ppc` Mach-O. Reuse `ppc-tiger/build.sh` as a starting point
   for the shell's build, adjusting the compiler from stock gcc to MacPorts gcc.

Open: whether to run mrustc on-device or cross. ppc-tiger built on-device; a
cross-mrustc + cross-gcc pipeline on the Mac dev box would be far nicer for
iteration but is more setup.

## 8. Phased plan

- **Phase 0 — spike.** Build the local mrustc (`/Users/dani/repos/mrustc`) with
  the `powerpc-apple-darwin` target, pin the exact rustc baseline (start 1.54)
  and core-crate edition, transpile a trivial `no_std` crate with one function,
  compile the emitted C with **MacPorts gcc + MacportsLegacySupport**, run on
  Tiger. Proves the toolchain (including the `<stdatomic.h>`/C11 hurdle)
  end-to-end before touching real code. **Go/no-go gate.**
- **Phase 1 — core crate skeleton.** Create `rusty-backup-core` (`no_std` +
  `alloc`, malloc allocator). Move MBR/APM/EBR parsing in, strip `std::io`.
  Unit-tested on host at 1.96 *and* transpiled+run on Tiger. Deliverable:
  `inspect` of a raw MBR image works natively via the core.
- **Phase 2 — FAT compaction kernel.** Port `fs/fat.rs` compaction math into the
  core (BTreeMap, slice API). Deliverable: `backup` with FAT compaction produces
  a byte-identical plan to the desktop build (differential test against the Rust
  original).
- **Phase 3 — metadata + backup/restore round-trip.** Hand-roll `metadata.json`
  emit in the core; wire the C shell's backup/restore loops to the core.
  Deliverable: backup -> restore -> checksum match on Tiger, matching the desktop
  metadata format.
- **Phase 4 — parity pass.** `list-devices`, `rip`, gzip, crc32/sha1 wired
  through (mostly reused from the existing ppc-tiger C). Deliverable: the 5-verb
  MVP at parity with `ppc-tiger/` — but now with the engine single-sourced.

Each phase is independently shippable and reverts cleanly to the hand-C port if
we stop.

## 9. Decision gate — is this worth doing?

The hand-C `ppc-tiger/` port already works. This effort only pays off if:

- The **engine kernels churn** on the desktop (partition/FAT logic changes) and
  we're tired of re-porting them by hand, **and**
- We value **single provenance** for the subtle bits (FAT compaction remap,
  cluster-floor edge cases) so a desktop bug-fix propagates to PPC for free.

If the FAT/partition logic is effectively frozen, **keep the hand-C port** —
mrustc setup + no_std carve-out is real work for little marginal gain. The
tie-breaker is Phase 0: if standing up mrustc is painful, that alone argues for
staying hand-C.

> **The payoff changes if libstd works (§3).** This gate was written assuming the
> only option was carving out a `no_std` core — i.e. paying real effort to
> single-source just the FAT/partition kernels, which is a thin prize when that
> logic is stable. If mrustc's `powerpc-apple-darwin` libstd turns out usable,
> the prize is not two kernels but **the whole of `rb-cli-vintage`** — every
> filesystem driver, every verb — with no carve-out at all. That is a
> categorically better deal, and it is why the hello-world experiment should run
> before this gate is answered.

## 10. Risks / open questions

- **libstd for `powerpc-apple-darwin` — the top risk, and previously understated
  here.** mrustc's README lists its CI-tested libstd targets (x86-64 Linux /
  Windows, secondary x86-64 + arm64 macOS); **PPC-Darwin is absent.** The target
  spec at `src/trans/target.cpp:632` is well-formed, but that is the descriptor,
  not a std port. The earlier text called Darwin output "lower risk than it
  looks" on the grounds that `@catap`'s MacPorts port runs mrustc-emitted C on
  PPC — note that establishes the *C toolchain* works, which is blocker 2, and
  says nothing about whether libstd builds and runs for this target. Settle this
  first (§3's hello-world experiment); everything else is downstream of it.
- **Language baseline** in whatever we transpile. Against the **1.74** baseline
  (not 1.54 — see §3) `partition/` + `fat.rs` are old-school byte code and
  almost certainly fine; `rb-cli-vintage` already builds the whole engine at
  1.73. Inventory only if the `no_std` core route is actually taken.
- **C++ dependencies are a genuine hard stop** (unlike proc macros, which are
  not). `libchdman-rs` is C++; mrustc cannot help. Any transpiled configuration
  must leave `chd` off, as `rb-cli-vintage`'s feature set already does.
- **Cross vs on-device build** ergonomics (Section 7).
- **HashMap iteration-order** differences if we swap to BTreeMap could change
  output byte-for-byte — must be checked in the Phase 2 differential test.
- **Allocator pressure** — no_std + malloc bridge on a G3 with limited RAM;
  keep the streaming block model, never buffer a whole partition.

## 11. Prior art / references

- [`ppc-tiger/`](../ppc-tiger/) — the working hand-C port. `rust_cli_real.c`
  (CLI, 2,251 lines), `rust_runtime_v2.c` (runtime stubs), `malloc_wrapper.c`
  (PIC/allocator fix), `build.sh`. The C shell here is ~80% reusable.
- [rust-ppc-tiger](https://github.com/Scottcjn/rust-ppc-tiger) — the transpiler
  reference used to guide the original hand port.
- [mrustc](https://github.com/thepowersgang/mrustc) — the Rust->C transpiler this
  plan depends on.
- Desktop engine to carve from: `src/partition/{mbr,apm,mod}.rs`,
  `src/fs/fat.rs`, `src/backup/`, `src/restore/`.

## 12. GUI (out of scope here, noted for later)

The egui GUI does **not** come across — eframe/wgpu won't transpile and wouldn't
run on 10.3-10.5 GPUs regardless. A native GUI is possible but is a **hand-written
Carbon frontend** (HIToolbox + Navigation Services, as in
`ppc-tiger/rusty_backup_gui.c`) calling the same core over the C ABI. That's a
separate plan; the core's ABI should be designed with a progress **callback**
boundary from day one so the GUI can drive long ops on a worker thread without a
retrofit.
