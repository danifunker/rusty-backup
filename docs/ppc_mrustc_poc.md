# PPC port via mrustc — PARKED POC plan

**Status:** Parked. Not on the active queue. The C port in `ppc-tiger/`
continues to be the production PowerPC target for the foreseeable
future; see `ppc-tiger/README.md` for what's shipping there today.

**Why this doc exists:** A long-term goal of rusty-backup is to **stay
in Rust**, with one codebase that all targets share. The C port works
but drifts from the Rust source — bugs get fixed in one and forgotten
in the other, features land in one and lag in the other. mrustc is the
only realistic path to building our Rust filesystem code as native
PowerPC binaries for Mac OS X Tiger / Leopard. This plan captures what
a 4-to-6-week POC of that approach would look like, so we can pick it
up when there's time.

**Not part of the regular pipeline.** When/if revived, this is a
side-project spike with explicit go/no-go gates.

---

## Why mrustc

`rustc` does not target `powerpc-apple-darwin`. The official target was
unofficial through ~Rust 1.10, removed because (a) Apple dropped PPC at
10.6, (b) no maintainer remained, (c) LLVM's PowerPC Mach-O backend
bit-rotted. Reviving it would be 6–12 months of LLVM-internals work for
one experienced engineer. That's not a realistic path.

[**mrustc**](https://github.com/thepowersgang/mrustc) is an alternative
Rust compiler written in C++. It reads real Rust source, lowers to its
own MIR, and **emits ugly-but-correct C99** that any decent C compiler
can build. Originally designed to bootstrap rustc on platforms without
a binary. Maintained — last commits 2026-04.

The critical finding from the local clone (`/Users/dani/repos/mrustc`):
**`powerpc-apple-darwin` is a built-in target** (see
`src/trans/target.cpp:632`). Real users — `@catap` via MacPorts — are
producing PPC Mac binaries through it today. We are not blazing a
trail; we're walking one that's already been worn.

The catches:

1. mrustc-emitted C requires `<stdatomic.h>` (C11). Tiger's stock
   `gcc-4.0.1` does not have it. Workaround: use **MacPorts gcc** on
   the Tiger box (MacPorts ships up to gcc-12 for Tiger PPC via the
   `MacportsLegacySupport` shim that catap's port already uses).
2. mrustc cannot handle proc-macros. Any crate using `syn` /
   `proc-macro2` will fail. That rules out `thiserror`, `serde`
   derives, `clap` derive, `tracing`, and transitively any GUI crate
   (`eframe`, `egui` pull syn in).
3. mrustc's type-checker has known bugs that bite certain crates
   (`rayon`, `crc`, `syn 2.x`). When you hit one, options are
   (a) restructure your code, (b) file an issue and wait.

---

## Scoped target: CLI only, PPC-relevant subset

This is **not** a port of all of rusty-backup. It is a port of the
slice that matters on a PowerPC Mac OS X box.

### Verbs to include

| Verb | Include? | Notes |
|---|---|---|
| `backup` | yes | core use case |
| `restore` | yes | core use case |
| `inspect` | yes | small, useful |
| `show devices` / `show partmap` / `show fs-info` | yes | parity with C port |
| `ls` / `get` | yes (stretch) | parity with C port |
| `put` / `mkdir` / `rm` / `write` / `chmeta` / `setrsrc` / `bless` | no | edit verbs; defer |
| `convert` / `expand` / `grow` / `shrink` / `resize` | no | layout transforms; defer |
| `fsck` / `new` | no | complex; defer |
| `optical` | no | needs `opticaldiscs` + `cd_da_reader` |
| `batch` / `batch_template` / `terminal` | no | needs `rustyline` |
| `completions` / `config` | no | meta verbs |

### Filesystems to include

| FS | Include? | Reasoning |
|---|---|---|
| HFS | yes | Tiger system disks |
| HFS+ | yes | Tiger system disks, modern Macs |
| ext2/3/4 (read-only) | yes | Linux PPC partitions |
| FAT12/16/32 | yes (stretch) | small, useful for cross-mount disks |
| AFFS, PFS3, SFS | no | Amiga; not a PPC use case |
| ProDOS, EFS | no | retro; ppc-tiger C handles these |
| NTFS, exFAT, btrfs | no | not relevant to PPC owners |

### Partition tables

APM (Tiger uses this), MBR (Linux PPC partitions), GPT (modern PPC
Macs). RDB / SGI VH stay in C-port-only territory.

### Output formats

| Format | Include? | Reason |
|---|---|---|
| raw | yes | trivial |
| gzip | yes | proven via `flate2` |
| VHD | yes | one-shot footer write, marginal cost — keep |
| zstd / zeekstd | no | C dep on `libzstd`, marginal benefit over gzip |
| CHD | no | `libchdman-rs` pulls MAME's CHD lib; not portable |
| bzip2 | no | obscure, drop |

### Dependencies — what survives the cut

**Drop entirely:**
- All GUI: `eframe`, `egui`, `rfd`, `image`
- All CHD: `libchdman-rs`
- All optical: `opticaldiscs`, `cd-da-reader`
- All web: `reqwest`, `webbrowser`
- All macOS native: `objc2-io-kit`, `objc2-disk-arbitration`, the rest of `objc2-*`
- Interactive shells: `rustyline`
- Build helpers: `clap_mangen`, `clap_complete`, `embed-resource`, `winres`
- Alternate metadata: `serde_yml`, `csv`
- Alternate compressors: `zstd`, `zeekstd`, `bzip2`
- `chrono` (replace with `std::time::SystemTime`)
- `tempfile` (replace with manual paths)
- `nix` (Tiger predates most of what it wraps)
- `windows`, `winres` (PPC isn't Windows)

**Replace with hand-written code:**
- `clap` derive → ~200 lines of manual arg parsing, keeping the
  current `rb-cli` positional grammar
- `thiserror` → hand-written `impl std::error::Error` (mechanical;
  ~50 enum variants in `error.rs` + filesystem-specific errors)
- `serde` derives → hand-written `to_json` / `from_json` for the few
  structs in metadata (BackupMetadata, PartitionMetadata, etc.)
- `serde_json` → keep if mrustc accepts it (no proc-macros of its
  own); else hand-write a minimal JSON emitter

**Keep as-is if mrustc accepts them:**
- `byteorder` — pure, no macros
- `log` — minimal
- `crc32fast`, `crc32c` — pure
- `sha2` — pure, big
- `flate2` — wraps C zlib; should be fine
- `anyhow` — macro-light, but verify; else replace with
  `Box<dyn Error>`

### Line-count estimate

| Module | LoC (current Rust) |
|---|---|
| `src/partition/{apm,mbr,gpt}.rs` + shared types | ~2,000 |
| `src/fs/{fat,hfs,hfsplus,ext}.rs` + traits | ~20,000 |
| `src/backup/` (subset) | ~2,000 |
| `src/restore/` (subset) | ~1,000 |
| `src/rbformats/{raw,gzip,vhd}.rs` | ~1,100 |
| `src/error.rs` (rewritten without thiserror) | ~300 |
| `src/cli/` (subset, no clap derive) | ~3,500 |
| **Total Rust to port through mrustc** | **~30,000 lines** |

That's roughly 6× smaller than the full crate. Still big.

---

## The two-phase POC

### Phase 0 — toolchain spike (2-3 days)

**Goal:** prove the mrustc → gcc-on-Tiger → working PPC binary chain
end-to-end with a trivial program.

**Steps:**
1. Build mrustc on this dev Mac (modern macOS host, needs gcc 5.4+).
2. Pull `rustc-1.54.0-src.tar.gz` (catap's confirmed-working version).
   1.74 has open issues; 1.90 actively broken on PPC.
3. Run `make -f minicargo.mk LIBS RUSTC_TARGET=powerpc-apple-darwin`.
   This builds `libcore` / `liballoc` / `libstd` as C source.
4. Get a Tiger box (or a Tiger VM via QEMU) with **MacPorts** installed
   and gcc-7 or newer + `MacportsLegacySupport`.
5. Cross-build a hello-world Rust program: mrustc emits C on the host,
   transfer C + the libstd outputs to Tiger, compile with MacPorts gcc.
6. Run the resulting binary on Tiger.

**Decision point:**
- ✅ Hello-world runs on Tiger → continue to Phase 1.
- ❌ Toolchain doesn't link / atomics broken / mrustc can't build
  libstd for PPC → stop. Mrustc path is closed; stay with C in
  `ppc-tiger/`.

### Phase 1 — single-module vertical slice (1 week)

**Goal:** compile one of our actual filesystem modules through mrustc
and run its tests on Tiger.

**Steps:**
1. Pick the smallest module that exercises non-trivial code:
   **`src/partition/mbr.rs`**. Small (~700 lines), no FS dependencies,
   exercises `byteorder`, struct serialization, error handling.
2. Strip `thiserror::Error` derive from `FilesystemError`; write
   `impl std::error::Error` by hand. Verify it still compiles with
   stock rustc and passes existing tests on dev Mac.
3. Compile through mrustc → C → PPC binary on Tiger. Run a minimal
   harness that parses a known MBR-on-disk fixture and prints the
   partition table.
4. Compare output bit-for-bit with what stock rustc produces on the
   dev Mac.

**Decision point:**
- ✅ Output matches → mrustc actually compiles our Rust correctly.
  Commit. Continue to Phase 2.
- ❌ mrustc asserts / output diverges → file upstream bug, assess
  whether we can route around it. If two attempts in a row hit bugs
  that need maintainer fixes, stop.

### Phase 2 — incremental FS ports (2-4 weeks)

**Goal:** port the chosen filesystem subset, one at a time, with PPC
test passes for each.

**Order (cheapest → most ambitious):**
1. APM, GPT (similar to MBR)
2. FAT (no proc-macros once derives are stripped, well-tested)
3. ext (read-only; we already have the substrate planned in C
   anyway)
4. HFS (big, classic, BE-native so endianness is free)
5. HFS+ (depends on HFS; tackles the trickier extent / B-tree code)

**Each port:**
- Strip derives, vendor any deps that pull `syn`, replace with
  hand-written equivalents.
- Get it green with stock rustc on dev Mac (regression check).
- Compile through mrustc, run on Tiger, compare against C port's
  output on the same fixtures.

**Wire up CLI:**
- Minimum verbs: `backup`, `restore`, `show devices`, `show fs-info`.
- Hand-rolled positional arg parser matching current `rb-cli`
  grammar.
- Output formats: raw, gzip, VHD.

**Decision point at end of Phase 2:**
- Sufficient parity with `ppc-tiger/` C port → declare the mrustc
  path the new PPC build target, deprecate the C code (or keep it as
  a fallback for environments without MacPorts).
- Coverage gap too large → keep both, mrustc as experimental.

---

## Estimated effort

| Phase | Wall time | Risk |
|---|---|---|
| 0: toolchain | 2-3 days | Medium — atomics / MacPorts gcc issues likely |
| 1: one module | 1 week | Medium — mrustc bugs hit ~30% of attempts |
| 2: full subset | 2-4 weeks | High — accumulated bugs, derive-stripping work |
| **Total when active** | **4-6 weeks** | |

Realistic interpretation: when picked up, expect **~6 weeks of
intermittent work**, not 6 weeks of focused work. mrustc bugs may
require waiting on upstream fixes.

---

## Why not now

User-confirmed reasons for parking:

1. **C port is already producing working binaries** on the Tiger box.
2. **No deadline pressure** for one-codebase parity.
3. **Active work elsewhere** in the main Rust codebase (HFS expand-block-size,
   Amiga filesystems, EFS, etc.) needs attention.

When to revive: when the C port hits a feature that's genuinely
painful to reimplement in C (a candidate is the HFS+ extent-tree
write path — non-trivial in either language, and being able to share
the Rust implementation would be material).

---

## Open questions to resolve when revived

1. **Tiger VM via QEMU**: Does `qemu-system-ppc -M mac99` with a Tiger
   install image actually work on Apple Silicon for iteration? Or do
   we need a real PPC Mac for every test cycle? Speed matters a lot.
2. **MacPorts gcc availability on Tiger**: Confirm which gcc version
   the user's Tiger box currently has via MacPorts, or whether they
   need to set it up. catap's port uses gcc-7+ in practice.
3. **rustc version targeting**: 1.54 is the safest catap-confirmed
   choice. Our codebase uses edition 2021 features that *should* be
   in 1.54, but anything from edition 2024 or recent stdlib additions
   (`let-else` is 1.65, `is_some_and` is 1.70) need a sweep.
4. **`anyhow` vs `Box<dyn Error>`**: Test in Phase 0. If `anyhow`
   builds, we save substantial rewriting.
5. **JSON emitter**: Hand-write or vendor a no-derive crate like
   `tinyjson`? Tinyjson is ~600 LoC, no deps, no macros — likely
   mrustc-friendly.

---

## What stays in the C port

Even if mrustc succeeds, the following stay in `ppc-tiger/` C code
forever (or until someone explicitly ports them):

- Amiga filesystems (AFFS, PFS3, SFS) and RDB
- SGI EFS and SGI Volume Header parsing
- ProDOS browser
- ISO 9660 (incl. Joliet) browser
- DC42 / 2MG image wrappers
- The Carbon GUI (`rusty_backup_gui.c`)

These are PPC-side-only features, not present (or different) in the
main Rust build, so there's no "one codebase" benefit to porting them.
The C port keeps living in its own folder as the home for
PPC-Tiger-specific weirdness.
