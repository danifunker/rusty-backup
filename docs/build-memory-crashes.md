# Build crashes: memory pressure during debug builds

**Status (2026-09-02): root cause identified, mitigations landed and verified
on Linux (see Verification). Windows still unverified.** Track further findings here.

## Symptom

A debug build (`cargo build`, `cargo test`, `cargo build --all-targets`) kills
the terminal it runs in. On Linux the Claude Code session, the tmux pane or the
whole GNOME Terminal tab disappears mid-build; twice it also took gnome-shell
down. The same builds are reported to crash on the Windows development machine.

It looked like a bad test firing from the pre-commit hook. It is not. The hook
never runs tests (it ran `cargo fmt`, then `cargo check` and `cargo clippy`
with `--all-targets`), and nothing added in the last month is pathological
for the compiler. The tree simply outgrew a 16 GB machine at the default
debug settings.

## Evidence (m900, 6 cores, 16 GB RAM, 4 GB swap, rustc 1.95.0)

`journalctl --user` shows systemd-oomd, the userspace OOM killer Ubuntu ships
enabled, killing whole cgroups. Every kill lands on a terminal scope, which is
why the *session* dies rather than one rustc process:

```
Sep 01 20:39:54 vte-spawn-5207....scope: systemd-oomd killed 29 process(es) in this unit.
Sep 01 21:34:37 snap.code.code-9eb5....scope: systemd-oomd killed 2 process(es) in this unit.
Sep 01 21:34:53 snap.code.code-ba1a....scope: systemd-oomd killed 14 process(es) in this unit.
Sep 01 21:35:09 tmux-spawn-8b82....scope: systemd-oomd killed 25 process(es) in this unit.
Sep 01 22:33:14 vte-spawn-adb7....scope: systemd-oomd killed 29 process(es) in this unit.
Sep 01 23:50:18 vte-spawn-10bd....scope: systemd-oomd killed some process(es) in this unit.
Sep 01 23:50:34 org.gnome.Shell@x11.service: systemd-oomd killed 3 process(es) in this unit.
Sep 02 03:03:49 org.gnome.Shell@x11.service: systemd-oomd killed 3 process(es) in this unit.
Sep 02 03:04:05 vte-spawn-b3a1....scope: systemd-oomd killed 33 process(es) in this unit.
Sep 02 03:04:05 systemd-oomd: Killed .../vte-spawn-b3a1....scope due to memory pressure
    for /user.slice/user-1000.slice/user@1000.service being 95.27% > 50.00%
    for > 20s with reclaim activity
```

The kernel OOM killer never fired (`journalctl -k` is clean). systemd-oomd
acts on *pressure* (PSI), not on a hard limit, and its unit of action is the
cgroup. A terminal tab, a tmux server and a VS Code window are each one scope,
so the killer takes the terminal, its shell, Claude Code and cargo together.

The build artifacts date the kills to the **link phase of the test binaries**:

| artifact (`target/debug/deps`)              | size   | written        | note |
|---------------------------------------------|--------|----------------|------|
| `rusty_backup-9880d63a4e4696d4` (lib tests)  | 839 MB | Sep 02 03:01   | linked 3 min before the 03:04 kill |
| `rusty_backup-15795cb95792ca2c` (lib tests)  | 506 MB | Sep 01 23:45   | 5 min before the 23:50 kill |
| `librusty_backup-e70e9262d2ff9778.rlib`      | 634 MB | Sep 02 03:01   | the library every test binary links |
| `rb_cli-1b177b1943aa30ee`                    | 461 MB | Sep 02 03:01   | |
| `remote_filesystem-…`, `filesystem_e2e-…`, `fs_e2e_suite-…`, `wave2_dispatch_e2e-…` | 385-442 MB each | Sep 01 23:32-23:49 | integration tests |
| `wave2_dispatch_e2e-….tmp900a5ec`            | 386 MB | Sep 01 21:35   | linker temp file abandoned at the 21:35:09 kill |
| `commander_archive-…`                        | 349 MB | Sep 02 03:03   | linked while `sgi_efs_v1_e2e` and `daemon_service` were also mid-compile (0-byte `.rmeta` placeholders at 03:02) |

Totals: `target/` was 49 GB (`deps` 20 GB, `examples` 16 GB, `incremental`
11 GB) on a disk with 24 GB free. The abandoned `.tmp` file is the linker
being shot mid-write.

Why the binaries are that big: the crate is one library of 396,507 lines
(`src/`) with 3,499 `#[test]` functions compiled into a single test binary,
plus 19 integration-test crates, 2 bins and 76 examples (68 auto-discovered
under `examples/` and 8 declared under `example/`). `Cargo.toml` never had a
`[profile]` section, so every debug build used full debuginfo (`debug = 2`),
`opt-level = 0` and one job per core. Every one of those ~100 binaries links
the 634 MB rlib with its complete DWARF. Six such links in flight at once,
each holding hundreds of MB of debuginfo plus rustc still generating code for
the next targets, is what crossed 16 GB.

What was ruled out:

- **A pathological test.** No `include_bytes!` larger than the 16 MB CJK font
  that has been there since June; no giant array literals; no test allocates
  more than 100 MB. The lib-test binary grew because the tree grew: ~212
  commits and several new filesystem drivers (BFS, OFS, UFS newfs, EFS,
  HPFS, NeXT and Solaris labels) in the last four weeks.
- **The pre-commit hook running tests.** It does not. It did run two full
  type-check passes over the crate (`cargo check --all-targets` then `cargo
  clippy --all-targets`), which is slow and memory-heavy but no kill lines up
  with a commit; every kill lines up with a link.

## What changed (commit on `bugfixer1`)

1. **`Cargo.toml`: `[profile.dev] debug = "line-tables-only"`.** Debug and
   test builds keep file:line for backtraces and panics but drop the type and
   variable DWARF that made up most of every binary. `profile.test` inherits
   it. Release builds and CI (which builds `--release` everywhere) are
   untouched. The vintage builds (`rb-cli-vintage/`) use their own manifest,
   and minicargo (the PowerPC mrustc path) ignores `[profile]` sections.
   Need locals in a debugger for one run: `CARGO_PROFILE_DEV_DEBUG=2 cargo
   build`.
2. **`.cargo/config.toml`: `[build] jobs = 4`.** Caps concurrent rustc and
   linker processes; rustc's LLVM codegen threads draw from the same
   jobserver, so this bounds the whole build's concurrency. Override per run
   with `cargo -j N` or `CARGO_BUILD_JOBS=N`. GitHub's hosted runners have
   four cores, so CI is unaffected.
3. **`.cargo-husky/hooks/pre-commit`: dropped the `cargo check --all-targets`
   step.** Clippy is check plus lints and fails on the same type errors, so
   the separate pass was a second full walk of the crate per commit for no
   extra coverage. Halves hook time and removes one memory peak per commit.

## Verification (m900, 2026-09-02, after `rm -rf target/debug`)

`cargo test --no-run` at the committed settings, run in its own transient
scope (`systemd-run --user --scope`) with a 5-second sampler:

| measure | before (Sep 1-2) | after |
|---|---|---|
| lib-test binary | 839 MB | 385 MB |
| `librusty_backup` rlib | 634 MB | 334 MB |
| `rb-cli` | 461 MB | 216 MB |
| integration test binaries | 350-440 MB | 144-197 MB |
| `target/debug/examples` (76 binaries) | 16 GB | 7.0 GB |
| `target/debug/deps` | 20 GB | 6.0 GB |
| `target/debug/incremental` | 11 GB | 2.7 GB |
| `target/debug` total | 46 GB | 17 GB |
| peak anonymous memory in the build scope | not measured | 1.96 GB |
| peak system-wide "used" (`free`, incl. ~2.8 GB desktop baseline) | not measured | 4.65 GB |
| peak page cache charged to the scope | not measured | 7.7 GB |
| user-slice memory pressure `full avg10` (oomd's trigger, limit 50%) | 95.27% at the kills | 0.07% |
| systemd-oomd events during the run | one per build | none |
| wall time, main crate + all test/example targets, `-j4` | not measured | 222 s |
| compiler warnings | 0 | 0 |

Two things the measurement itself taught:

- **Do not verify with a `MemoryMax` cap on the scope.** The first attempt ran
  under `MemoryMax=11G` and was oomd-killed at 10.2 GB *while still compiling
  dependencies*: only ~1.5 GB of that was anonymous memory, the rest was page
  cache from the artifacts being written, and forcing the cgroup to reclaim
  that cache produced 84% pressure, which is the exact signal oomd acts on.
  Uncapped, the same build never exceeded 0.07%.
- **Reclaim stall is the trigger, not raw allocation.** The original builds
  wrote tens of GB of debuginfo-laden artifacts while several linkers held
  hundreds of MB each; the kernel's writeback and cache reclaim stalls are what
  pushed the user slice over 50% for 20 s. Shrinking every artifact by half or
  more attacks that directly, which is why the peak came down so far.

Still open:

- [x] Same build on the Windows machine survives (2026-09-02, Windows 11,
      32 GB RAM, 65 GB commit limit, `target\debug` deleted first, Defender
      real-time scanning still covering `target` because adding the
      exclusion needs an elevated shell). `cargo test --no-run` finished in
      395 s with exit 0. A 2-second sampler saw committed memory peak
      8.8 GB above the 30.5 GB baseline, and the cargo / rustc / link.exe
      family peak at 7.4 GB working set and 7.7 GB private bytes, with at
      most 10 such processes alive. Lib-test binary 97 MB plus a 185 MB
      PDB (the Linux 385 MB figure carries its DWARF inside the ELF), the
      bin-test binary 49 MB plus 73 MB, `rb-cli.exe` 79 MB, 27 test
      executables, 75 example binaries (2.0G), `deps` 4.6G, `incremental` 2.8G,
      `target\debug` 9.5 GB in all. Nothing was killed and the desktop
      stayed responsive; the MSVC PDB mechanism above is therefore contained
      by the same debuginfo trim.
- [x] Same build on the Mac (2026-09-05, Apple silicon, macOS 26, no
      `target/` at all beforehand): `cargo test --no-run` finished in
      152 s wall / 464 s user under `/usr/bin/time -l`, which reported
      2.35 GB as the largest resident set of any single child (the lib
      rustc) and 261 MB for cargo itself. Lib-test binary 62 MB,
      `target/debug` 7.0 GB. Nothing was killed. Activity Monitor was not
      watched; the rusage figure is per process, not the sum.
- [x] `cargo clippy --all-targets -- -D warnings` (the hook's pass, same
      scope and sampler): green, 0 warnings, 158 s. Peak anonymous memory
      3.9 GB in the scope, system-wide used 6.6 GB, user-slice pressure
      peaked at 8.8% for one sample, no oomd event. Clippy is the heavier
      of the two workloads for anonymous memory (3.9 GB vs 2.0 GB for the
      build), the build is heavier for page cache. Neither is near the
      killer's threshold any more.

## If it still happens

Further levers, in the order to try them:

- `split-debuginfo = "unpacked"` in `[profile.dev]` keeps DWARF in `.dwo` /
  `.o` files so the linker never sees it. Linux and macOS only; cargo drops
  the flag on MSVC where `packed` is the sole option.
- `cargo test --no-run -j2` for the first build after a big change; raise
  again once the incremental cache exists.
- Stop building the 68 scouts under `examples/` on every `cargo test` and
  `--all-targets` run: `autoexamples = false` plus explicit `[[example]]`
  entries for the ones that matter, or move the one-off probes to
  `regression-tests/`.
- Split the 400k-line crate into a workspace (fs drivers, formats, GUI) so
  the lib-test binary is no longer one 3,499-test monolith.
- `target/` hygiene: 49 GB of stale artifacts on a 24 GB-free disk is its own
  crash vector. `cargo clean` or `rm -rf target/debug` before a profile change,
  since old fingerprints are never reused.
- Two cargo invocations at once (a session's `cargo test` plus a manual build
  in another terminal) do not share a jobserver; that doubles the peak and no
  config setting prevents it.
- Machine-level: raise the swap file, or give systemd-oomd a higher pressure
  limit for the terminal scope (`/etc/systemd/oomd.conf.d/`), both of which
  need root.
