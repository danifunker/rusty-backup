# Resume: crate split — Steps 4–7

Paste the prompt below into a new Claude Code session in a checkout of
`rusty-backup` on branch `native-frontends` (on dani's box that's the
`~/repos/rusty-backup-native-frontends` worktree).

---

You're continuing the **crate split** of rusty-backup on branch
`native-frontends`. The plan and its history are in `docs/crate_split_plan.md`.
Read it first, along with `docs/license_audit.md`, CLAUDE.md and CONTRIBUTING.md.

## Where it stands (2026-10-31)

- **Steps 0–3 are done.** The old `crate::fs::*` and
  `crate::clonezilla::*` paths are `pub use` shims, so the app compiles unchanged.
  The crates under `crates/`:
  - Steps 0–2: `disk-core`, `fs-8bit`, `fs-cpm`, `fs-apple2`, `fs-qnx` and `fs-xerox`.
  - `dsk-clonezilla`, done early (Step 5).
  - Step 3: `fs-amiga`, `fs-unix`, `fs-xfs` (GPL-2.0-only), `fs-squashfs`,
    `fs-pc`, `fs-hpfs` (GPL-2.0-only), `fs-apfs`, `fs-mac` and `fs-mac-unicode`
    (GPL-2.0-only).
  - `docs/crate_split_plan.md` §9 has one bullet per slice, saying what moved,
    what was held back and why.
- **`disk-core` grew, backwards-compatibly:** `name_legalize`, `stream_pipe`,
  `raw_name`, `attrs` and `sgi`, plus a `serde` feature. Its version is still
  0.1.0 in the tree.
- **On crates.io:** `disk-core` 0.1.0 only. **Nothing was published this
  session.** The auto-mode classifier refused the tag push, so publishing
  needs Dani.
  - `fs-xerox` and `dsk-clonezilla` are ready now. Both pass
    `cargo publish --dry-run` and depend only on `disk-core` 0.1.0 APIs:
    `git tag fs-xerox-v0.1.0 && git push origin fs-xerox-v0.1.0`, and the same
    for `dsk-clonezilla-v0.1.0`.
  - The Step 3 crates use the new `disk-core` modules, so `disk-core` has to go
    out first, as 0.1.1 or 0.2.
- **Every slice passed this gate:**
  - `cargo test --workspace`: 3,743 passed, 0 failed.
  - Clippy `-D warnings` on each new crate and on each of its features alone.
  - The Rust 1.73 vintage `rb-cli` check.
  - The slim build: `--no-default-features --features pure-zstd`.
- **Not re-run:** the PPC / Sol9 mrustc builds. Each slice adds a path crate, so
  re-vendor first (see the last section).
- **Still held back:**
  - `adfs` and `qdos` / `qdos_mdv` (licence, D2) and `lif` (its test), all from Step 2.
  - `ofs*`, the old BeOS filesystem. It needs the Mac type/creator table in
    `hfs_common::resolve_create_type_creator`, so it waits for `fs-mac`.
  - `sun2tape*` / `tar_section`. They need the Unix-`compress` codec in
    `macarchive::stuffit_lzw` and `mem_archive::open_tar`, so they wait for
    Step 6.

## This machine (dani's Linux box), read before building

- **The disk is small.** A default debug `cargo test --workspace` fills it (20 GB
  of target) and fails at link time with ENOSPC. Run the gate with debuginfo
  off, for that run only, which takes about 5 GB:
  `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --workspace`.
  Never change `Cargo.toml` for it.
- **There are no GTK3 dev headers,** so the `gui` feature's rfd/gtk3 fails in
  `atk-sys`.
  - To build or test, temporarily switch rfd to `features = ["xdg-portal"]` in
    `Cargo.toml`.
  - Afterwards, switch it back to `["gtk3"]` and run `cargo metadata` so
    `Cargo.lock` gets the gtk crates back.
  - Never commit the swap. Check that `git diff Cargo.lock` shows only your crate.
- **zstd versions:** a new crate with `zstd = ">=0.13, <0.15"` locks to 0.13.3
  in the root `Cargo.lock`, because `nod`, via opticaldiscs, pins 0.13. To keep
  the desktop on one version:
  1. Set the crate's requirement to `"0.14"`.
  2. Run `cargo metadata`.
  3. Restore the range and run `cargo metadata` again.
- **1.73 toolchain:** `~/.rustup/toolchains/1.73.0-x86_64-unknown-linux-gnu/bin`.
  - Refresh the PPC / Sol9 lockfiles with that cargo's `cargo metadata --manifest-path rb-cli-{ppc,sol9}/Cargo.toml`.
  - It errors on an edition-2024 crate *after* writing the lock, which is fine.

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

## Start here: Step 4, `part-tables`

Step 3 is done, including `fs-mac` and its GPL sub-crate `fs-mac-unicode`; see
`crate_split_plan.md` §9 for what moved and what stayed. Still held back from
earlier steps: `adfs`, `qdos`, `lif`, `ofs*`, `sun2tape*` / `tar_section` and
`lisa`.

- **`ofs*` needs a decision first.** `hfs_common::resolve_create_type_creator`
  and its type/creator table are now in `fs-mac`. Either `fs-unix` depends on
  `fs-mac`, or that table moves to `disk-core`.
- **Solaris 9 / PPC builds:** mrustc's minicargo has no `[patch]` support.
  `scripts/build-{sol9,ppc}.sh` run it through `minicargo_run`, which hides the
  `[patch.crates-io]` table. `rb-cli-ppc/patches/qnx2fs.py` points the vendored
  qnx2fs at `crates/disk-core`. Since then, CI's Sol9 job gets through all the
  path crates.
- **Check what a commit picks up.** `git commit` takes the whole index, so a
  `git mv` staged for the next slice rides along. Stage per slice, or commit
  with an explicit pathspec.

**How each slice was done (scripts are easy to recreate):**

1. `git mv` the modules into `crates/<name>/src/`.
2. Rewrite paths with a regex pass. `super::X` / `crate::fs::X` become:
   - `disk_core::X` for a disk-core module
   - `crate::X` for a module in the same crate
   - `disk_core::filesystem::CompactResult` for `CompactResult`

   Also: `crate::rust173_compat::` → `disk_core::rust173::`; `crate::compat::io_other` →
   `disk_core::io_other`; `crate::partition::format_size` →
   `disk_core::display::format_size`.
   In tests: `crate::rbformats::zstd_compat::decoder(` →
   `zstd::stream::read::Decoder::new(`, with zstd as a dev-dependency, and the
   relative `"tests/fixtures/..."` paths → `CARGO_MANIFEST_DIR/../../tests/fixtures/...`.
3. Write the one-line shims, plus a `pub use` in the four manifests and a
   rust173-polyfill forward. Add both workflow entries.
4. Let `cargo check --all-targets` on the app find the `pub(crate)` items the
   app uses, and make those `pub`.
5. Run the gate, update the plan doc, `cargo fmt --all`, and commit with `--no-verify`.

## Then: the rest of the plan

Follow `docs/crate_split_plan.md` §9:

- **Step 4:** `part-tables`. `disk_core::sgi` already holds the SGI
  byte-order helpers it will share with `fs-unix`.
- **Step 5:** the other `dsk-*` crates, then `lisa`.
- **Step 6:** `arc-mac` (MIT) and `arc-mac-codecs` (LGPL-2.1-or-later, D1), and
  `fs-tar`. Then `sun2tape*` / `tar_section`.
- **Step 7:** remove the shims and write the per-crate READMEs. CLAUDE.md and
  `docs/export_pipelines.md` already point the Amiga paths at the crate.

## Publishing order

Publishing goes through tags (see the rules above); this session could not push tags, so nothing beyond `disk-core` 0.1.0 is on crates.io yet.

1. **Now, on `disk-core` 0.1.0:** `fs-xerox`, `dsk-clonezilla`, `fs-8bit`, `fs-cpm`, `fs-apple2`, `fs-qnx`, `fs-apfs`, `fs-hpfs`, `fs-mac-unicode`, `dsk-flux`, `dsk-apple`, `dsk-dmg`, `dsk-floppy` and `dsk-vm`. Checked 2026-10-31 by building each against the published 0.1.0.
2. **`disk-core` 0.1.1:** the tree is at 0.1.1. The additions (`name_legalize`, `stream_pipe`, `raw_name`, `attrs`, `sgi`, the `serde` feature) are backwards compatible.
3. **After 0.1.1, these require it:** `fs-amiga`, `fs-mac`, `fs-pc`, `fs-squashfs`, `fs-unix`, `fs-xfs` and `part-tables`.
4. **Last:** `dsk-ghost` (after `fs-pc`), then `fs-mac` (after `fs-mac-unicode` and `dsk-apple`).

`.github/workflows/publish-crates.yml` lists every crate. Run `cargo publish -p <crate> --dry-run` on a clean tree before each tag.

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

