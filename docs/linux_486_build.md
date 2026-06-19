# Building `rb-cli` for 486-class Linux

This is the **Linux** sibling of the DOS-native `cb-dos` offshoot (see
[`cb_dos.md`](cb_dos.md)). Both let you back up a vintage machine *from the
machine itself*; they are complementary, not alternatives:

| | `cb-dos` (DOS) | `rb-cli` on Linux |
|---|---|---|
| OS on the box | none (boots from a floppy) | a 486-capable Linux distro |
| Language | C / DJGPP | Rust (this codebase) |
| FS coverage | FAT/NTFS/exFAT (hand-written) | **everything `rb-cli` supports** (FAT/NTFS/exFAT/HFS/ext/Amiga/…) |
| Disk access | BIOS `int 13h` | `/dev/sdX`, `/dev/hdX` |
| Effort | a C program | a cross-build of an existing binary |

The appeal of the Linux path: it reuses the *entire* Rust engine — every
filesystem, the whole backup/restore pipeline — with no reimplementation. The
cost is that the target box must run Linux (≥ ~8–16 MB RAM realistically), and
the Rust toolchain has to be coaxed onto a 486.

> **Status: scoping + toolchain prep.** The `rb-cli-mini` build
> (`--no-default-features`) and the pure-Rust flate2 backend (below) are done
> and verified. The actual cross-link belongs on a Linux/CI host (a Mac dev box
> has no i586/i486 glibc sysroot). Nothing here has been run on real 486
> hardware yet.

---

## Two tiers — pick your CPU floor

There are two meaningfully different targets, and they cost different amounts of
effort. **Decide which hardware you actually need to support.**

### Tier 1 — i586 (Pentium and up) — *easy, stable Rust*

`i586-unknown-linux-gnu` is a built-in tier-2 target with a **prebuilt std**.
It assumes the Pentium baseline, so 64-bit atomics use the native `CMPXCHG8B`
instruction. **This does *not* run on a real 486** — only Pentium-class and
newer. If your "retro Linux" box is a Pentium/P5/P55C, this is all you need.

```sh
rustup target add i586-unknown-linux-gnu
cargo build --release --bin rb-cli \
  --no-default-features \
  --target i586-unknown-linux-gnu
```

### Tier 2 — i486 (true 486) — *custom target + nightly build-std*

A real i486 lacks `CMPXCHG8B` (Pentium, 1993), so 64-bit atomics can't be done
in a single instruction. There is no built-in i486 Rust target, so you supply a
**custom target spec** and build `std` from source.

`targets/i486-unknown-linux-gnu.json` (committed here) is the i586 spec with two
changes: `"cpu": "i486"` and `"max-atomic-width": 32`. The latter is the whole
trick — see [Atomics](#atomics-you-dont-skip-them-you-lower-them).

```sh
# nightly is required for -Z build-std (no prebuilt std for a custom target)
rustup toolchain install nightly
rustup component add rust-src --toolchain nightly

cargo +nightly build --release --bin rb-cli \
  --no-default-features \
  -Z build-std=std,panic_abort \
  --target targets/i486-unknown-linux-gnu.json
```

To **regenerate / sanity-check** the spec against your rustc version:

```sh
rustc +nightly -Z unstable-options \
  --target i586-unknown-linux-gnu --print target-spec-json \
  > /tmp/i586.json
# then edit: "cpu" -> "i486", "max-atomic-width" -> 32, llvm-target -> i486-...
```

---

## Atomics: you don't *skip* them, you *lower* them

The only thing that genuinely breaks on a 486 is **64-bit atomics**.

- A 32-bit target's `AtomicU64`/`AtomicI64` need an 8-byte atomic compare-and-swap
  = `CMPXCHG8B`, which is a **Pentium (i586)** instruction.
- The 486 *does* have `LOCK CMPXCHG` (4-byte), `XADD`, and `LOCK` prefixes, so
  **all ≤32-bit atomics are native and fine** — `Arc` refcounts, `AtomicUsize`
  (32-bit here), `Once`, allocator internals.

`"max-atomic-width": 32` in the target spec tells LLVM the CPU can't do 8-byte
atomics natively, so it emits **libcalls** (`__atomic_load_8`,
`__atomic_compare_exchange_8`, …) for the 64-bit ones instead of `CMPXCHG8B`.
Those calls are satisfied by **libatomic** (ships with GCC), which implements
them with a global lock table — correct on a single-core 486. Combined with
`"cpu": "i486"` (LLVM then knows it has no `cx8`), no `CMPXCHG8B` is emitted
anywhere.

You may need to add `-latomic` explicitly when linking, depending on the
sysroot's gcc spec:

```sh
RUSTFLAGS="-C link-arg=-latomic" cargo +nightly build ... # as above
```

### Caveat: `cpuid` on *original* 486 steppings

Several always-on crates do **runtime CPU-feature detection** via `cpuid` and
fall back to software when SSE/CRC instructions are absent: `crc32fast`,
`crc32c`, `sha2`, `aes`. The original i486 (and 486SX) predate `cpuid`, which
arrived on later **486DX2/DX4** steppings. On a 486 *with* `cpuid` (the common
Linux-capable ones) these crates correctly detect "no SSE" and use their
portable paths — fine. On a *no-`cpuid`* 486, the detection itself faults.

If you must support a no-`cpuid` 486, force the software paths (e.g. patch
`std_detect`'s cache to report no features, or vendor the crates with their
SIMD paths disabled). For typical DX2/DX4 hardware this is a non-issue.

---

## Dependencies: what's pure Rust and what isn't

The decoupling work is already in `Cargo.toml`. The slim build
(`--no-default-features`) drops the whole GUI/network/CHD/optical stack
(eframe, egui, glow, winit, rfd/GTK, reqwest, tokio, libchdman, opticaldiscs).
What remains is almost entirely pure Rust.

**Native (C) libraries in the slim build — there are only two, and one is
already handled:**

| C dep | Pulled by | Status for 486 |
|---|---|---|
| `libz-ng-sys` (zlib-ng) | `flate2` | **Solved.** flate2's baseline is now the pure-Rust `rust_backend` (miniz_oxide); the C `zlib-ng` is behind the desktop-only `native-zlib` feature, which the slim build doesn't enable. So flate2 is pure Rust here — and that's exactly the gzip path cb-dos/486 wants. |
| `zstd-sys` (libzstd) | `zstd`, `zeekstd` | **Open follow-up** (see below). Cross-compiles fine as C, but isn't pure Rust. |

Everything else resolves to pure Rust: `bzip2` → `libbz2-rs-sys` (a Rust port),
`dirs-sys`, `zlib-rs`, `miniz_oxide`, `image`, `rustyline`, `crossterm`, the
RustCrypto hashes/ciphers, etc.

### Follow-up: gate `zstd` behind a feature

To make the slim build **100% pure Rust** (no C cross-toolchain needed at all),
`zstd` + `zeekstd` need to live behind a cargo feature so the 486 build can drop
them. This is **not done yet**: `zstd`/`Zstd` is referenced across ~40 files and
`CompressionType::Zstd` is an enum variant matched in ~10 exhaustive sites plus
the `metadata.json` (de)serialization, so gating it cleanly (without breaking
the "zero warnings / `cargo test --lib` green" rules) is its own focused change,
not a one-liner. Tracked as a separate task.

Until then, the i586/i486 build still compiles `zstd-sys` from C source, which
needs the cross **C** toolchain you already need for linking anyway — so this
blocks *purity*, not *feasibility*.

---

## Sysroot / cross-toolchain

Linking a Rust binary for i586/i486-linux-gnu needs a 32-bit **glibc sysroot**
and a cross `gcc`/`ld` (also the home of `libatomic` for Tier 2). Options:

- **`cross`** (Docker-based) — simplest if an i586 image exists; a custom
  Dockerfile based on a multilib gcc works.
- A **multilib** gcc on an x86-64 Linux host (`gcc -m32` + `libc6-dev-i386` +
  `libatomic1`) and point Cargo's linker at it.
- A **Buildroot** sysroot built for the exact target board.

Set the linker in `.cargo/config.toml`:

```toml
[target.i586-unknown-linux-gnu]
linker = "i586-linux-gnu-gcc"   # or "gcc" with -m32 via the spec's pre-link-args

[target.'cfg(all())']
# Tier 2 may also need: rustflags = ["-C", "link-arg=-latomic"]
```

---

## The target distro

Modern mainstream distros dropped i486 (Debian is i686-only). Realistic bases
for a 486:

- A custom **Buildroot** / Linux-From-Scratch rootfs (recommended — you control
  the kernel arch and libc).
- Very old **Slackware** / **TinyCore** lineages.

Boot media is heavier than cb-dos's single floppy (kernel + initrd + rootfs on
a CF/SD or small disk), which is the main practical tradeoff vs the DOS path.

---

## Recap / checklist

- [x] `rb-cli-mini` builds with `--no-default-features` (no GUI/CHD/optical).
- [x] flate2 baseline = pure-Rust `rust_backend`; C `zlib-ng` behind
      `native-zlib` (desktop default only).
- [x] `targets/i486-unknown-linux-gnu.json` committed (`cpu=i486`,
      `max-atomic-width=32`).
- [ ] Gate `zstd`/`zeekstd` behind a feature (→ 100% pure-Rust slim build).
- [ ] Stand up an i586 cross-build in CI (stable, easiest tier).
- [ ] Stand up the i486 nightly `build-std` cross-build + link `libatomic`.
- [ ] Boot + smoke-test on real 486 (DX2/DX4) hardware.
