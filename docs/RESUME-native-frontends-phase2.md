# RESUME: native front-ends, Phase 2 (handoff 2026-11-02)

Branch **`native-frontends`**, checked out in `~/repos/rusty-backup` on m900. Stay on it:
no other branches, no worktrees, no PR or merge into `main` unless the user asks.

## Paste this into the new session

> Read `docs/RESUME-native-frontends-phase2.md`, then CONTRIBUTING.md § Architecture policy
> (MVC) and `docs/phase2_commander_plan.md` from "Decisions (answered 2026-11-02)" to the
> end. Check `git status` and `git log origin/native-frontends..HEAD`: local commits wait
> for my OK before any push. Then start **Next steps, item 1 (slice 19: one extractor in a
> crate)**, one agent at a time, verifying and committing each step.

## Where things stand

- **Phase 2 (view-models / controllers): slices 1-18b done.** The GUI, the TUI and the CLI
  drive shared controllers in `src/controller/`. All of `src/gui` is enforced at 0 engine
  references by `scripts/check-boundaries.sh`; 20 references remain, all in
  `src/cli/verbs/tui_app.rs`.
- **Last session's work:** Inspect tab, Backup, Restore, the disk writer and partition window,
  the Inspect dialogs, Optical, the rest of the egui shell, the disc browser on the shared
  browser pane (slice 18), and slice 18b:
  - `opticaldiscs` 0.16.1 (published) fixes hybrid discs listing every Mac file twice.
  - Picker format names are italic.
- **Push state:** everything through `3119ea34` is pushed, and CI was green on every slice
  through 18, including the Solaris 9 / mrustc job. The docs commit that adds this file is
  **local only**: the user wants to give the OK before the next push.
- **Not yet run on Phase 2 code:** the full fixture regression suite. The last full run was
  450/450 at `7fc076cd`.

## Next steps (detail in `docs/phase2_commander_plan.md` § Next steps)

1. **Slice 19:** one extractor, in a new MIT crate. It replaces `model::extract`, the disc
   walker, and possibly `rb-cli optical extract`. The four byte changes the user accepted
   are listed in slice 18's Decisions; everything else must stay byte-identical. Version,
   publish, and update all four lockfiles.
2. **Slice 20:** the TUI's last 20 references, then enforce the whole TUI.
3. **TUI parity gaps:** add each one, or record it as N/A in `docs/platform_matrix.md`.
4. **Pickers off the UI thread.**
5. **Full regression suite** (about 95 min) plus macOS and Windows GUI smoke tests, then close
   Phase 2.
6. **Phase 4 (GTK4) or Phase 3 (AppKit).**

## Rules that bit last session

- **Build output goes to BigDisk.** Use
  `CARGO_TARGET_DIR=/run/media/dani/BigDisk/rb-build/target-nf-dev` for every cargo
  command, including `git commit` (the pre-commit hook builds).
  - The root disk filled up on 2026-11-02 and truncated a source file mid-write.
  - Check `df -h /` before big work.
  - Never run two cargo builds at once.
- **mrustc (the Solaris 9 CI job) only:**
  - Write deref conversions out (`.as_str()`, `.as_path()`, `.as_slice()`, `.as_ref()`).
  - Never put a relative `include_str!` / `include_bytes!` inside an inline `mod {}`.
  - Read the job's log with `grep "error:0"`.
- **Engine code stays on Rust 1.73.** Comments are one line, two at most. No Unicode
  glyphs in UI strings.
- **Don't run `cargo fmt` on the whole tree.** Run `rustfmt` on touched files only.
- **The pre-commit hook runs `git add -u`.** When other changes are in the tree, commit
  with explicit paths and `--no-verify`.
- **Never chain `pkill -f <pattern>` in a shell whose own command line contains the
  pattern:** it kills the shell. Anchor the pattern (`'^/path/binary$'`).
- **Crate fixes ship with a version bump and a publish.**
  - `crates/*`: `gh workflow run publish-crates.yml --ref native-frontends`.
  - opticaldiscs (danifunker/opticaldiscs-rs, clone at
    `/run/media/dani/BigDisk/rb-build/opticaldiscs-rs`): its own Release workflow.
  - Ask before publishing.

## GUI preview

A debug build is copied to `/run/media/dani/BigDisk/rb-tmp/gui-preview/rusty-backup` and
run from `/run/media/dani/BigDisk/rb-tmp/gui-test/`. That folder holds `fat-preview.img`,
`hfs.img` and two optical fixtures: `optical.hfs.opentransport.cd.iso` (a hybrid HFS/ISO
disc) and `optical.iso9660.joliet.cd.iso`.

To relaunch:

```bash
setsid -f /run/media/dani/BigDisk/rb-tmp/gui-preview/rusty-backup
```
