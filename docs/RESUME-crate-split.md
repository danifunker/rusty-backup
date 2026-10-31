# Resume: crate split — Alan's crates first, then Step 3 onward

Paste the prompt below into a new Claude Code session in a checkout of
`rusty-backup` on branch `native-frontends`.

---

You're continuing the **crate split** of rusty-backup on branch
`native-frontends`. The plan and its history are in `docs/crate_split_plan.md`.
Read it first, along with `docs/license_audit.md`, CLAUDE.md and CONTRIBUTING.md.

## Where it stands (2026-10-30)

- **Steps 0–2 are done.** The workspace is `crates/*`. Six crates exist:
  `disk-core`, `fs-8bit`, `fs-cpm`, `fs-apple2`, `fs-qnx` and `fs-xerox`.
  The old `crate::fs::*` paths are `pub use` shims, so the app still compiles
  unchanged.
- **On crates.io:** `disk-core` 0.1.0 only.
  - `fs-8bit`, `fs-cpm`, `fs-apple2` and `fs-qnx` are ready to publish but held,
    because Step 3 will probably grow `disk-core` to 0.2 (see "Publishing order").
  - `qnx2fs` 0.3.1, a separate repo, now carries the QNX 2 adapter behind its
    `disk-core` feature.
- **Held back from Step 2:** `adfs` and `qdos` / `qdos_mdv` (licence, D2 in
  `license_audit.md`), and `lif`. `lif`'s replace test needs its private fields
  and the app's `fs::replace`.

## The user's rules

- **Only reusable format code goes into a crate.** An adapter over an external
  engine stays in the app (`optical_fs` over `opticaldiscs`), or goes into that
  engine's own crate (`qnx2fs::adapter`). No crates "for the fun of it".
- **Minimal code changes when moving code.** That means `git mv`, path fixes,
  notices and comment corrections. Rewrites are separate follow-ups.
- **The licence follows the code.** Derived code moves into a crate or sub-crate
  under the upstream licence. Clean code is MIT.
- **Engine code must compile on Rust 1.73.** CLAUDE.md has the details. The check:

      T=$HOME/.rustup/toolchains/1.73.0-<host-triple>/bin
      PATH=$T:$PATH RUSTC=$T/rustc $T/cargo check --manifest-path rb-cli-vintage/Cargo.toml \
        --bin rb-cli --no-default-features \
        --features native-zstd,remote,tui,rust173-polyfill,yaml --ignore-rust-version

- **Commit to the checked-out branch** (`native-frontends`); never create
  branches. Use one commit per coherent slice, and don't ask permission between
  slices.
- **The pre-commit hook's whole-tree clippy fails on about 70 lint hits that are
  already on `main`.** Run `cargo fmt --all` by hand and commit with `--no-verify`.
- **Publishing goes through GitHub Actions only, never a local `cargo publish`.**
  Push a tag `<crate>-v<version>` (for example `fs-xerox-v0.1.0`) whose version
  matches the crate's `Cargo.toml`.
  - `.github/workflows/publish-crates.yml` checks the tag and publishes with the
    `CARGO_REGISTRY_TOKEN` repository secret, which is already set.
  - Run `cargo publish -p <crate> --dry-run` locally first. A dirty working tree
    fails it.
  - Add every new crate to both lists in the workflow: the tag patterns and the
    manual-run choices.

## Start here: Alan Steremberg's crates

Alan agreed to MIT ("Any license is fine"), and **there is no publishing hold**.
`fs-xerox` and `dsk-clonezilla` carry
`Copyright (c) 2026 Dani Sarfati, Alan Steremberg`, with Alan in `authors`.

1. **Publish `fs-xerox` 0.1.0.**
   - `publish = false` is already gone, the workflow lists it, and
     `cargo publish -p fs-xerox --dry-run` passes (16 files, 74 KiB compressed).
   - Push the tag `fs-xerox-v0.1.0`, watch the run with `gh run watch`, and
     confirm the crate on crates.io.
   - It depends on `disk-core` 0.1 from crates.io, which is fine.
2. **Create `crates/dsk-clonezilla`** from `src/clonezilla/`: `partclone.rs`,
   `metadata.rs`, `metadata_scan.rs`, `block_cache.rs` and `mod.rs`, about 3k lines.
   - **What couples it to the app:**
     - `crate::compat::io_other` and `crate::rust173_compat::IntIsMultipleOf`.
       `disk-core` already has both (`disk_core::io_other`, `disk_core::rust173`).
     - `crate::rbformats::zstd_compat::decoder` in `partclone.rs`. That switches
       between native and pure-Rust zstd by app feature, so the crate needs its
       own `native-zstd` / `pure-zstd` features, forwarded from all four
       manifests, or something equally minimal.
   - Check whether `metadata_scan.rs` is reusable format code or app glue. If it's
     app glue, it stays in the app.
   - **Callers:** `rbformats/{export,vhd}.rs`, `cli/verbs/restore.rs`,
     `gui/{inspect_tab,browse_view,restore_tab}.rs` and
     `model/{export_runner,backup_loader,status,browse_session,cache_runner,commander_source}.rs`.
     Keep `src/clonezilla/mod.rs` as a `pub use dsk_clonezilla::*` shim so none
     of them change.
   - **Crate setup:** MIT, the co-copyright `LICENSE`, `rust-version = "1.73"`, and
     a `rust173-polyfill` feature forwarded to `disk-core`. Add the crate to the
     path dependencies and `rust173-polyfill` lists of the root `Cargo.toml` and
     `rb-cli-vintage/`, `rb-cli-ppc/` and `rb-cli-sol9/`. Their lockfiles need
     refreshing too.
   - **Gate:** `cargo test` (all of it), clippy on the crate, the 1.73 vintage
     check, and the publish dry run.
   - Then add it to the workflow and publish `dsk-clonezilla-v0.1.0`.
   - Update `docs/crate_split_plan.md` §9 to say Step 5's `dsk-clonezilla` is done
     early.

## Then: the rest of the plan

Follow `docs/crate_split_plan.md` §9:

- **Step 3**, the big families:
  - `fs-amiga` (`MIT AND BSD-4-Clause` for the pfs3aio-derived code)
  - `fs-unix`
  - `fs-xfs` (GPL-2.0)
  - `fs-squashfs`
  - `fs-pc` (with `hpfs` editing as a GPL sub-crate)
  - `fs-mac` (with `hfs_unicode` as a GPL sub-crate)
  - `fs-apfs`
- **Step 4:** `part-tables`.
- **Step 5:** the other `dsk-*` crates.
- **Step 6:** `arc-mac` (MIT) and `arc-mac-codecs` (LGPL-2.1-or-later, D1), and
  `fs-tar`.
- **Step 7:** remove the shims and write the per-crate READMEs.

## Publishing order

Hold the `fs-*` publishes until Step 3 settles `disk-core`, then publish
`disk-core` 0.2 and the families together. The exceptions are `fs-xerox` and
`dsk-clonezilla`, which go out now on `disk-core` 0.1. Bump them later if they
need 0.2.

## The PowerPC / Solaris side (only if a slice breaks those builds)

- **Where they build:** the PPC and Sol9 builds transpile on **m900** (an x86_64
  Linux box) with mrustc in `~/repos/mrustc`, which is on branch
  `ppc-build-2026-09`. PPC compiles over ssh on the real G5 (`PPC_HOST=g5`).
  - Use an agent-forwarded ssh (`ssh -A m900`); m900's key has a passphrase.
  - The build command is
    `RB_DIR=<worktree> MRUSTC_DIR=~/repos/mrustc PPC_HOST=g5 scripts/build-ppc.sh ppc`.
    **`RB_DIR` is mandatory.** Without it the script quietly builds
    `~/repos/rusty-backup`, a different tree.
- **A new crate changes the vendor set**, so run `scripts/build-ppc.sh vendor`
  first.
- **Verify the result:**
  `PPC_HOST=g5 LOCAL_BIN=target/release/rb-cli scripts/ppc-smoke.sh <remote-binary>`
  must report zero mismatches.
  - m900's disk is nearly full, so build the desktop `target/release/rb-cli` on
    the Mac. `ssh g5` works from there too.
- **Engine split units:** `scripts/ppc-cc-remote.py` now sizes the units itself
  and refuses any over 16 MiB of `__text`. See `docs/build-ppc-mrustc.md`,
  "Scattered relocations and the 16 MiB ceiling".
- **mrustc fork branches:** `ppc-minicargo-fixes` (upstream PR
  thepowersgang/mrustc#450), `sparc-solaris10` (one squashed commit, which the user
  will send upstream) and `sparc-solaris9` (ours, stacked on `sparc-solaris10`).
  See `docs/build-sol9-mrustc.md`, "mrustc: the fork's branches".
- **Housekeeping:** never run two cargo builds at once on m900. If a build hits
  ENOSPC, run `cargo clean` straight away.
