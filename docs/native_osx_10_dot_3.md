# Native rb-cli for Mac OS X 10.3 / 10.4 / 10.5 (PowerPC)

Status: **PLAN ONLY — not started.** A design to revisit later. Scope is
deliberately limited to `rb-cli`; the GUI is a separate question (see the end).

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

We build on **rustc 1.96, edition 2021**. mrustc supports older baselines, and
on **PPC specifically** the confirmed-good range is narrower than mrustc's
general ceiling:

- **rustc 1.54** — safest `@catap`-confirmed baseline on `powerpc-apple-darwin`.
- **rustc 1.74** — partial on PPC.
- **rustc 1.90+** — actively broken on PPC.

So the core targets the **1.54 language subset** (not 1.74) to stay on the
confirmed-good baseline. Two layers of gap:

- **Language/std gap (manageable).** Edition 2021 needs rustc >= 1.56, so if we
  pin the core to the 1.54 baseline the core crate itself likely wants
  **edition 2018**. Whatever the pin, the discipline is the same: the core uses
  only features available at the chosen baseline. We keep developing the desktop
  on 1.96 (backward-compatible, daily builds unaffected); mrustc consumes the
  same source at the older baseline. This is worth nailing down in Phase 0 —
  pick the exact rustc pin and edition for the core crate.
- **Dependency graph (the real wall).** mrustc must compile everything
  transitively. Our `[dependencies]` is 112 crates — eframe/egui/wgpu, every
  proc-macro (`serde_derive`, `clap` derive, `thiserror`), build scripts, `nix`,
  the `windows` crate, `libchdman-rs`. **None of that transpiles.** Proc macros
  are a hard stop.

Conclusion: **we never transpile the app.** We transpile a tiny, `#![no_std]`,
near-zero-dependency **core** and hand-write everything else in C. `no_std` also
sidesteps mrustc's weak Darwin *std* port entirely — we only need `core` +
`alloc` with a malloc-backed `GlobalAlloc` (which is literally what
[`ppc-tiger/malloc_wrapper.c`](../ppc-tiger/malloc_wrapper.c) already is).

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

## 10. Risks / open questions

- **rustc 1.54 baseline vs any feature we can't live without** in the carved
  core. Need to inventory the exact language features `partition/` + `fat.rs`
  use against the 1.54 baseline. (Likely fine — they're old-school byte code.)
- **mrustc Darwin/Mach-O output maturity.** Lower risk than it looks: `@catap`'s
  MacPorts port already runs mrustc's C on PPC via MacPorts gcc, so the chain is
  known-good. Phase 0 re-confirms it for our crate + the C11/`<stdatomic.h>`
  dependency.
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
