# RESUME: native front-ends, end of Phase 2 (handoff 2026-11-04)

Branch **`native-frontends`**, checked out in `~/repos/rusty-backup` on m900. Stay on it:
no other branches, no worktrees, no PR or merge into `main` unless the user asks.

## Paste this into the new session

> Read `docs/RESUME-native-frontends-phase2.md`, then CONTRIBUTING.md § Architecture policy
> (MVC), `docs/native_frontends_plan.md` § Phase 2 "Done" and § Phases 3-4, and
> `docs/build_disk_plan.md` § 6 (decisions). Check `git status` and
> `git log origin/native-frontends..HEAD`. Then ask me which comes first: the interim
> release from `main`, Build Disk, or Phase 3 / 4.

## Where things stand

- **Phase 2 is done** (slices 1-29) apart from the macOS / Windows GUI smoke:
  - Every egui screen and dialog and the whole TUI drive the controllers in
    `src/controller/`; `scripts/check-boundaries.sh` enforces 0 engine references in both.
  - The TUI has the GUI's feature set. The only exceptions are the CD-DA player (GUI only)
    and Build Disk. Every other gap in `docs/platform_matrix.md` is a terminal limit.
  - GUI file pickers are async (slice 23).
- **Pushed and green:** everything through 4e0ee6a6; CI green incl. Solaris 9 / mrustc.
- **Full fixture suite: 461/461** at 4e0ee6a6 (release rb-cli, 26.5 min). Recipe:
  `scratchpad/full-regress.sh` in the old session; it builds a release rb-cli into
  `/run/media/dani/BigDisk/rb-build/target-nf-release` and runs
  `rb-regress run --require-clean --scratch-root /run/media/dani/BigDisk/rb-build/regress-scratch`.
- **Published 2026-11-04:** part-tables 0.1.1, arc-mac 0.1.3, dsk-dmg 0.1.1, fs-mac 0.1.3,
  rb-fsexporter 0.1.3, fs-pc 0.1.2, opticaldiscs 0.16.2, libchdman-rs 0.289.4 (its release workflow now has a
  `skip_macos` input for GitHub macOS runner outages; test on `dani@dani-dev.local` first).
- **Issues #98-#104** closed (silently). Fixtures for them and the NT4 TSE disk (R-079) are
  in `/run/media/dani/BigDisk/rb-fixtures` and on the NAS (`smb://daninas.local/software`,
  mount it from the file manager; it needs the saved password).

## Next (the user picks the order)

1. **GUI smoke on macOS and Windows** (needs the user): async pickers, the Inspect / Restore
   dialogs moved in Phase 2. Then Phase 2 is closed.
2. **Interim release: done 2026-11-05** as v2026-11-05-11-36. PR #96 (7c8f80d9) failed on
   main; PR #107 (fbe7c9ec) fixed ppc32 `AtomicU64` plus the Windows / macOS bugs and tests
   the first full-target run found. The push to `main` builds every target and auto-releases.
   Branch pushes now build every target too (no ci-targets file); add one back only for a
   long run of branch work, and remove it before the next merge. Ask before anything else
   goes into `main`.
3. **Build Disk** (`docs/build_disk_plan.md`): decisions are in § 6. New `rb-cli build`
   verb (`new` kept a few months), its own top-level section in the app, presets for the
   fixed builders, streaming writers for AFFS / PFS3 / SFS / HPFS / BFS with a shown cap
   until then, image-file or device targets. Bootable work is in
   `docs/bootable_disks_backlog.md`, not in scope.
4. **Phase 4 (GTK4) or Phase 3 (AppKit)** per `native_frontends_plan.md`.

## Open bugs

- R-081 (tiny CHDs aborted) is FIXED in libchdman-rs 0.289.4; the MAME bug itself is not
  reported upstream yet.
- `ntfs_fsck`'s "N files checked" count still starts at record 24 (R-079 Left).

## Rules that bite

- **Build output goes to BigDisk:** `CARGO_TARGET_DIR=/run/media/dani/BigDisk/rb-build/target-nf-dev`,
  `TMPDIR=/run/media/dani/BigDisk/rb-tmp/tmp`. Root disk has ~6 GB free. Never run two
  cargo builds at once (OOM-killed sessions before).
- **Engine code stays on Rust 1.73**; vintage check: from `rb-cli-vintage`,
  `cargo +1.73.0 build --bin rb-cli --no-default-features --features native-zstd,remote,tui,rust173-polyfill,yaml --ignore-rust-version`
  with its own target dir.
- **mrustc (Solaris 9 CI):** write deref conversions out (`.as_str()`, `.as_path()`,
  `.as_slice()`, `*k` in closures); no proc-macros; no relative `include_str!` in inline mods.
- **TUI tests:** never assert on wrapped dialog text next to a temp path (CI's longer paths
  wrap differently); assert on controller state or message payloads.
- **Comments** one line, two at most. No Unicode glyphs in UI strings. `rustfmt` touched
  files only, never `cargo fmt` on the tree.
- **Commits:** the pre-commit hook does `git add -u`; commit with explicit paths and
  `--no-verify`.
- **Crate fixes** ship with a version bump, a CHANGELOG line, all four lockfiles
  (root, vintage, ppc, sol9; edit only the crate's entry) and a publish
  (`gh workflow run publish-crates.yml --ref native-frontends -f dry_run=false`).
- **Never `pkill -f`** with a pattern that matches your own shell; anchor it.
- **Hands off DECAlphaStation200-MiSTer files**, including its /tmp session folders.

## GUI preview

Debug builds are copied to `/run/media/dani/BigDisk/rb-tmp/gui-preview/` and run from
`/run/media/dani/BigDisk/rb-tmp/gui-test/` with `setsid -f <binary>`. Copy to a new name
when an older preview is still running (a running binary can't be overwritten).
