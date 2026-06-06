# rb-cli on MiSTer (`armv7-unknown-linux-gnueabihf`)

Idea: cross-compile a stripped-down `rb-cli` for the MiSTer's
on-board Linux so the Wave-2 `[!] ref` user-side boot-test parks can
be validated in-place on the device instead of round-tripping
through scp + a host machine.

Target environment (per the user's MiSTer):

```
Linux 5.15.1-MiSTer #6 SMP Thu Jul 4 01:01:19 UTC 2024 armv7l GNU/Linux
```

— Intel Cyclone V SoC, dual-core Cortex-A9 ~800 MHz, glibc Buildroot
rootfs (~256 MB), USB / SD writable storage under `/media/fat/`.

This file is the **idea capture** for a later session. Nothing has
been built yet.

---

## Why this matters

The `[!]` markers still open on Wave-2 spine rows after the
2026-06-05 close-out are:

| Core    | Open user-side parks                        |
|---------|---------------------------------------------|
| X68000  | `ref` (Workflow A+C+B), `write-verified`, `gui` |
| Archie  | `ref` (Workflow A+C), `write-verified`, `gui` |
| QL      | `ref` (Workflow A+C; sQLux passed, hardware doesn't), `gui` |
| Altair  | `gui`                                       |

Workflow A+C is "build a test image on a host, scp to MiSTer, mount
in the core, verify the file is visible and readable." Workflow B
reverses it ("write from inside the core, power down, pull SD, check
host-side with rb-cli").

Today the only way to drive these workflows is:

```
host:  rb-cli put / get / ls    →    scp    →    MiSTer core mount
```

Every iteration is a round-trip. If `rb-cli` ran *on* the MiSTer
itself, the loop collapses to:

```
MiSTer shell:  rb-cli put / get / ls    →    core mount (same SD)
```

— closer to the way real users work, and the same binary is then
usable for ad-hoc inspection / extraction of any of the dozen+
disc-image formats rusty-backup understands without leaving the
device.

It also gives us a closer-to-real-hardware oracle for the **Archie
HDD resize** work that's currently deferred (OPEN-WORK §7) — if
rb-cli is on the MiSTer, a resize test means "shrink/grow the HDF,
unplug, plug back in, boot the core" rather than booting RPCEmu on a
host.

---

## Blockers (concrete, all surveyed 2026-06-05)

### 1. `libchdman-rs` with the `prebuilt` feature

`Cargo.toml` line 66:

```toml
libchdman-rs = { version = "0.288.1", features = ["prebuilt"] }
```

The `prebuilt` feature pulls a precompiled CHD core; upstream's
prebuilt matrix doesn't currently include `armv7-unknown-linux-
gnueabihf`. So the link step will fail.

**Resolutions, easiest first:**

- **(A) Drop CHD for the MiSTer build** — gate `libchdman-rs`
  behind a `chd` feature flag, off by default for a new
  `minimal-cli` build profile. ADFS / QXL.WIN / .d88 / .adf / .hdf /
  .vhd / .img / .zst all still work without CHD. The use cases
  where MiSTer-side CHD matters are vanishingly small (Cortex-A9
  is too slow for CHD compression to be useful anyway).
- **(B) Build CHD from source on the host cross-toolchain** —
  `libchdman-rs` is a Rust wrapper around the MAME CHD core; with
  the C++ source vendored it should cross-compile via `cc` + a
  proper ARM gnu++ toolchain. Half a day of work to validate.

**Recommendation: (A).** Keep the host build as-is; the on-device
build trades CHD for portability.

### 2. GUI deps drift into rb-cli via the workspace lib

The rb-cli binary lives at `src/bin/rb_cli.rs` and pulls everything
from `src/lib.rs`. The lib re-exports the GUI module unconditionally,
so building rb-cli still resolves:

- `eframe = "0.34"` (egui's eframe runner)
- `egui = "0.34"`
- `rfd = { version = "0.17", features = ["gtk3"] }`

On MiSTer Buildroot, GTK3 dev libs are not present, so `rfd` fails
at link. egui itself is pure Rust but pulls in `glow` (OpenGL via
GLES2 bindings) which on Linux wants `libGL` + `libEGL` headers —
also probably absent.

**Resolution: feature-gate the GUI**. Introduce a `gui` feature in
the lib `Cargo.toml`, off by default for the rb-cli build:

```toml
[features]
default = ["gui", "chd"]
gui = ["dep:eframe", "dep:egui", "dep:rfd"]
chd = ["dep:libchdman-rs"]
minimal-cli = []  # nothing
```

And make `src/gui/`, `src/main.rs`, and the rb-cli code paths that
currently call into GUI types compile out under `#[cfg(feature =
"gui")]` / `#[cfg(not(feature = "gui"))]`.

The split has to be careful — rb-cli today uses a handful of model
types (`InspectTab::*`, `BrowseView::*`) that live in the GUI module
even though they're logically core. Those need to move down to
`src/model/` before the cut.

### 3. Native C-lib dependencies — cross-toolchain setup

Workspace deps that have C / C++ components:

| Crate                | C component                              |
|----------------------|------------------------------------------|
| `flate2` (zlib-ng)   | zlib-ng (cmake + cc)                     |
| `zstd-sys`           | zstd (cc)                                |
| `bzip2-sys`          | bzip2 (cc)                               |
| `sha2`               | optional asm; pure-Rust path works on arm |
| `reqwest`            | rustls (pure Rust) or openssl-sys        |
| `crc32fast`          | optional simd; portable fallback works   |
| `aes`                | pure Rust                                |
| `image`              | optional PNG via image-png (pure Rust)   |

All of these are well-known to cross-compile under
`cross` (https://github.com/cross-rs/cross) with the default
`armv7-unknown-linux-gnueabihf` image. The lone risk is
`zlib-ng` (depends on which cmake the image ships); the fallback is
switching `flate2`'s default features from `zlib-ng` to the
pure-Rust `rust_backend`.

For `reqwest`: switch features from default (openssl-sys via
native-tls) to `rustls-tls` so there's no openssl-sys dep.

### 4. `opticaldiscs` / `cd-da-reader` (drives feature)

`opticaldiscs = { version = "0.4.2", features = ["drives"] }` and
`cd-da-reader` (git fork). These touch real CD drives via system
APIs (Linux: `libcdio` or `ioctl`). MiSTer has no optical drive
attached, but the build still compiles them — and `libcdio` may or
may not be present in the Buildroot rootfs.

**Resolution: gate behind `optical` feature.** Default on for host
builds, off for `minimal-cli`. Same shape as `chd`.

---

## Proposed cuts

Introduce three workspace features:

```toml
[features]
default = ["gui", "chd", "optical"]
gui = ["dep:eframe", "dep:egui", "dep:rfd"]
chd = ["dep:libchdman-rs"]
optical = ["dep:opticaldiscs", "dep:cd-da-reader"]
minimal-cli = []  # nothing — only the CLI binary and its core deps
```

`rb-cli` becomes buildable via:

```
cargo build --bin rb-cli --no-default-features
```

On the host this is a fast headless build (no glow / GTK link); on
the MiSTer cross-target it's the only path that resolves.

Reqwest also flips:

```toml
reqwest = { version = "0.13", default-features = false,
            features = ["blocking", "json", "rustls-tls"] }
```

(host build doesn't need `native-tls` either — saves the
openssl-sys dep for everyone.)

---

## Build incantation (target session)

```sh
# Linux x64 host with Docker + cross:
cargo install cross --git https://github.com/cross-rs/cross

# From the rusty-backup repo root:
cross build --target armv7-unknown-linux-gnueabihf \
            --bin rb-cli \
            --release \
            --no-default-features

# Output: target/armv7-unknown-linux-gnueabihf/release/rb-cli
file target/armv7-unknown-linux-gnueabihf/release/rb-cli
# expect: ELF 32-bit LSB executable, ARM, EABI5 version 1 (SYSV),
#         dynamically linked (loader /lib/ld-linux-armhf.so.3)

# Sanity check it links against glibc 2.x (MiSTer ships 2.31+):
arm-linux-gnueabihf-objdump -p target/.../rb-cli \
  | grep -i 'NEEDED\|GLIBC_'

# Deploy:
scp target/armv7-unknown-linux-gnueabihf/release/rb-cli \
    root@mister.local:/media/fat/Scripts/rb-cli
ssh root@mister.local 'chmod +x /media/fat/Scripts/rb-cli'

# Smoke test (replace with a real image already on the SD):
ssh root@mister.local '/media/fat/Scripts/rb-cli inspect /media/fat/games/Archie/CROS42.hdf'
```

Strip the binary for size:

```sh
arm-linux-gnueabihf-strip target/.../rb-cli
# Expect ~5-8 MB stripped (vs. ~30-40 MB unstripped Rust release).
```

---

## What this would unblock

Driving each Wave-2 user-side `[!] ref` mark to `[x]` becomes
practical (single-device workflow):

| Core    | On-device workflow                                           |
|---------|--------------------------------------------------------------|
| X68000  | `rb-cli put <foo.d88> X68000/games.d88 BAR` → mount core → `dir A:` → `type BAR` |
| Archie  | `rb-cli put CROS42.hdf Apps/test "hello"` → reboot Archie core → `*Type test` |
| QL      | `rb-cli put WIN.win win1_/BAR "..."` → boot QL → `DIR win1_` → `LOAD win1_BAR` |
| Altair  | `rb-cli ls disc.dsk` → boot CP/M → `DIR` cross-check         |

Each is now a single-shell loop instead of a host ↔ device shuffle.

The same binary is also useful for **anyone using the MiSTer to play
with vintage disc images** — inspect, browse, extract individual
files — without needing a separate computer. That's beyond the
Wave-2 scope but the natural fallout.

---

## Estimated effort

Rough ranges, all assuming the feature-flag plumbing goes cleanly:

| Phase                                                | Effort           |
|------------------------------------------------------|------------------|
| Carve `gui`/`chd`/`optical` features in Cargo.toml   | 1 session (~2h)  |
| Find + relocate GUI-coupled model types out of `src/gui/` | 1-2 sessions     |
| `cargo build --no-default-features` clean on x64 host| 1 session        |
| `cross build --target armv7-...` clean               | 1 session        |
| Smoke-test on MiSTer hardware (user-side)            | 1 session        |
| Wire CI matrix entry for armv7 release artifact      | 1 session (opt.) |

Realistic landing: 3-5 focused sessions for "buildable + tested",
plus optional CI. The first session (feature plumbing) is the
biggest unknown — depends on how much GUI / core coupling has crept
in.

---

## Open questions for the picking-up session

1. **How tangled is `src/model/` vs `src/gui/`?** A grep through
   `src/bin/rb_cli.rs` for `gui::` and `model::` references will
   tell us how big the cleanup is before the feature flag works.
2. **Does the rb-cli command surface need anything from `libchdman-
   rs`?** If `rb-cli backup --to chd` is a thing, the `--no-chd`
   build needs to error out cleanly on that verb rather than
   silently producing a corrupt file.
3. **Distribution channel?** Drop the armv7 binary into the regular
   GitHub release matrix, or keep it ad-hoc? The MiSTer audience
   is small enough that ad-hoc is probably fine for v1.
4. **glibc floor?** MiSTer's Buildroot tracks recent glibc; check
   what `ldd --version` reports on a live MiSTer before fixing a
   minimum target.
5. **Static-link alternative?** `armv7-unknown-linux-musleabihf`
   would produce a fully static binary that runs without worrying
   about glibc compatibility. Trade-off: musl + Rust + zstd-sys
   sometimes has performance issues. Not worth it unless the
   glibc story turns out to be painful.

---

## Status

Idea captured 2026-06-05 after the Wave-2 Archie engine close-out.
**Shipped 2026-06-06**:

- `Cargo.toml` carries three optional features — `gui` (eframe / egui
  / rfd / reqwest / webbrowser), `chd` (libchdman-rs), and `optical`
  (opticaldiscs / cd-da-reader). `default = ["gui", "chd", "optical"]`
  keeps the desktop release a single binary; the slim build is
  `cargo build --bin rb-cli --no-default-features`.
- `reqwest` flipped from `native-tls` to `rustls`, so the host build
  no longer drags in `openssl-sys`.
- Module-level gates throughout `src/`: `src/main.rs` (gui),
  `src/update.rs`, `src/model/{update_runner,chd_expand_runner}`,
  `src/optical/`, `src/cli/verbs/optical.rs`,
  `src/rbformats/{chd,chd_edit,chd_options}`,
  `src/backup/single_file_chd`. Runtime stubs in
  `src/rbformats/mod.rs` keep call sites (e.g. `BackupConfig::chd_options`,
  `BrowseSession::chd_edit_session`) compiling; calls into the stubs
  return a clear "this binary was built without the `chd` feature"
  error.
- CI workflow `.github/workflows/release.yml` ships a new
  `build-rb-cli-mini-armv7` job that uses `cross` to produce
  `rb-cli-mini-armv7-linux-<version>.tar.gz` as a release artifact.
- README has a `rb-cli-mini` section with the build incantation and
  the included/excluded feature matrix.

Remaining open: smoke-test the produced armv7 binary on a real MiSTer
device (the X68000 / Archie / QL Workflow A+B+C parks in §7 of
OPEN-WORK) — this requires hardware access and is tracked there
rather than here.
