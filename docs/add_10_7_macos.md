# Building rb-cli for vintage Intel Macs (down to macOS 10.7)

Scope: a headless **`rb-cli`** binary for old **64-bit Intel** Macs. The GUI is
out — `eframe` → `winit` + `wgpu`/`glow` needs a Metal-capable, modern-ish macOS,
so old Intel hardware is CLI-only. This is the macOS sibling of the exotic-target
work in [`docs/linux_486_build.md`](linux_486_build.md) (i486/i586 appliance) and
the armv7/MiSTer slim build.

## The two hard cliffs

Intel Macs span 2006–2020. Rust draws two hard lines through that range:

1. **32-bit Core Solo/Duo (2006–07, capped at 10.6.8)** — needs
   `i386`/`i686-apple-darwin`, which Rust **removed years ago**
   (`i686-apple-darwin` went away around Rust 1.42). No modern-Rust build exists.
   That is a C-offshoot situation (à la `cb-dos`), **not** rb-cli. Out of scope.

2. **64-bit Intel** — the floor is set by Rust's prebuilt `std` for
   `x86_64-apple-darwin`, which is compiled for **macOS 10.12 (Sierra)**.

Confirmed on this repo's toolchain (Homebrew rustc 1.96.0):

```
$ rustc --print deployment-target --target x86_64-apple-darwin
MACOSX_DEPLOYMENT_TARGET=10.12
```

Modern rustc does not merely default to 10.12 — it **actively refuses** to go
lower. Passing a smaller target yields:

```
warning: deployment target in MACOSX_DEPLOYMENT_TARGET was set to 10.7,
         but the minimum supported by `rustc` is 10.12
```

It warns and clamps, so a 1.96 binary is stamped 10.12 and **won't load on
Lion** regardless of the env var.

## Tiers

| Tier | macOS range | Toolchain | Effort |
| ---- | ----------- | --------- | ------ |
| **A — Sierra+** | 10.12 (2016) → 12 Monterey (last Intel macOS) | stable, prebuilt std | one env var |
| **B — Lion–El Capitan** | 10.7 – 10.11 | pinned old Rust **or** nightly build-std + polyfill | hard, best-effort |

**Tier A is the whole game for "vintage Intel."** It covers every Intel Mac
anyone realistically still backs up disks on (2016+):

```bash
MACOSX_DEPLOYMENT_TARGET=10.12 \
  cargo build --release --bin rb-cli \
  --target x86_64-apple-darwin \
  --no-default-features --features pure-zstd,remote
```

Slim (`--no-default-features`) for the same reason as MiSTer/486: `chd`/`optical`
pull in the `libchdman-rs` / `opticaldiscs` **prebuilts, compiled against a modern
macOS SDK**, which won't honor an old floor. `pure-zstd` keeps zstd pure-Rust;
`native-zlib` stays off so flate2 uses its pure-Rust `rust_backend`. You get
FAT/HFS/HFS+/exFAT/NTFS/Amiga/etc. read+write+backup (zstd/raw/vhd) — everything
except CHD and physical optical.

## Why 10.7 (Tier B) is hard

The baseline jumped **straight from 10.7 to 10.12 in Rust 1.74** (Nov 2023). The
documented trigger: std started calling **`clock_gettime`**, which macOS didn't
add until **10.12 Sierra** (a couple of other 10.12-era primitives are involved
too, but `clock_gettime` is the canonical one). A 1.96 binary references a symbol
Lion's `libSystem` doesn't export, so it fails at **dyld load time — before
`main`**.

Consequence: **10.7, 10.8, 10.9, 10.10, 10.11 are all equally hard** — they're
all "below 10.12." Picking 10.9 or 10.11 buys nothing. The real choice is binary:
do you cross the 10.12 line or not?

## Path A — pin Rust 1.73.0 (recommended for Tier B)

**1.73.0 is the last toolchain whose prebuilt `x86_64-apple-darwin` std targets
10.7** and uses the old `mach_absolute_time`/pthread code paths — no
`clock_gettime`. That means **no `build-std`, no nightly, no polyfills**. CI:

```yaml
  build-macos-vintage:
    runs-on: macos-latest          # arm64 host cross-compiles x86_64 fine
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@1.73.0
        with:
          targets: x86_64-apple-darwin
      - name: Build (vintage Intel, 10.7 floor)
        env:
          MACOSX_DEPLOYMENT_TARGET: "10.7"
        run: |
          cargo +1.73.0 build --release --bin rb-cli \
            --target x86_64-apple-darwin \
            --no-default-features --features pure-zstd,remote
```

The one real cost is **dependency MSRV archaeology**. The current `Cargo.lock`
resolved against 1.96; some crates will demand newer-than-1.73. Reactive loop:

```bash
cargo +1.73.0 build ...            # fails: "package X requires rustc 1.80"
cargo update -p X --precise <older-version>   # pin down to a 1.73-OK release
# repeat until it builds
```

Usual MSRV offenders to watch: `pure-zstd` (`libzstd-bitexact-rs`), `crossterm`,
`time`, and anything on `edition2024`. If a crate has **no** 1.73-compatible
version that still carries the features we need, that's a genuine wall → Path B.
Keep these pins isolated (a dedicated branch / `rust-toolchain.toml` / separate
lockfile) so they don't drag the main toolchain backward.

## Path B — nightly + custom target + polyfill (only if Path A's deps won't cooperate)

The macOS analog of `targets/i486-unknown-linux-gnu.json` — best-effort, expect
CI iteration:

1. **Custom target JSON** (`targets/x86_64-apple-darwin-10.7.json`) overriding the
   min-OS version below rustc's 10.12 clamp — the target spec is where the floor
   lives.
2. **Nightly + `-Z build-std=std,panic_abort`** to recompile std against the
   lowered target.
3. **A `clock_gettime` polyfill** (a `.c`/`.o` linked in that implements
   `clock_gettime` over `gettimeofday`/`mach_absolute_time`) so the weak-linked
   symbol resolves on Lion instead of crashing — plus anything else the symbol
   dump below turns up.

More maintenance; keeps the modern dep tree. Reach for it only if 1.73 pinning
hits a wall.

## Ground-truth loop (run on the artifact, either path)

A **dependency** — not just std — can also call a 10.12 symbol. After any vintage
build, enumerate the gap:

```bash
otool -L rb-cli                                # dylib deps: all should be /usr/lib/libSystem etc.
otool -l rb-cli | grep -A3 LC_BUILD_VERSION    # confirm minos = 10.7 (not clamped to 10.12)
nm -u rb-cli | sort                            # every undefined import
# diff against a real 10.7 libSystem symbol list; clock_gettime / os_unfair_lock*
# / __ulock_* showing up = still broken.
```

Clean of 10.12-era symbols + `minos` reads 10.7 ⇒ it'll run on Lion.

## Signing / notarization gotcha

The current macOS CI job signs + notarizes with **hardened runtime**. Do **not**
apply that to the vintage artifact:

- Notarization/Gatekeeper enforcement only exists on **10.14.5+/10.15+** — a
  Sierra/Lion user never sees it.
- Hardened-runtime + stapling assumes newer-OS behavior and can make the binary
  **fail to launch** on the old OS.

So for the vintage variant: **ad-hoc sign (`codesign -s -`) or ship unsigned**,
and tell users to `xattr -dr com.apple.quarantine ./rb-cli` on first run.

## Local prototyping caveat

This repo's Homebrew rustc is **stable-only, single-toolchain**, ships only the
host (arm64) std, and actively refuses sub-10.12 targets — so none of Tier B can
be prototyped locally as-is. To work on it locally, install `rustup`
(`brew install rustup` → `rustup-init`), then either
`rustup toolchain install 1.73.0` (Path A) or a nightly + the `rust-src`
component (Path B). Otherwise it's a CI-only exercise.

## Recommendation

Ship **Tier A** now (slim `x86_64-apple-darwin` rb-cli, `MACOSX_DEPLOYMENT_TARGET
=10.12`, ad-hoc signed) as a step on the existing Intel macOS job — near-zero
maintenance, covers 2016+. Treat **Tier B (10.7–10.11)** as a follow-up branch
like i486: try Path A (Rust 1.73.0) first; fall back to Path B only if a
dependency has no 1.73-compatible version. Skip 32-bit entirely.
