# Resume: regression fixes

Paste this into a fresh session to continue.

---

Continuing regression fixes on rusty-backup (branch: `regression-fixes`, 6
commits ahead of `origin/regression-fixes`, **nothing pushed**).

## STATE

- Suite: **246 pass / 24 xfail / 0 fail** on Windows. macOS and Linux are at
  `9fe84e3` (235/26/0) and have **not** run since — six commits of drift.
- 12 findings fixed (R-004, R-006, R-007, R-009, R-010, R-014, R-017, R-018,
  R-025, R-026, R-027, R-034), 24 open. `data/known-failures.toml` holds 24
  entries, each citing one.
- `main` is at merge commit `48cee1f`; this branch is ahead of it.

## FIRST, BEFORE ANY NEW WORK

**Verify the six unpushed commits on macOS and Linux.** They touch shared
code — `exit::CodedError`, `PartitionContext::type_name`, the `optical`
verbs — and only Windows has run them. Push, then on each host:

    git fetch origin regression-fixes && git reset --hard origin/regression-fixes
    cargo build --release --bin rb-cli
    cargo build --release --manifest-path regression-tests/runner/Cargo.toml   # BOTH binaries
    cd regression-tests && ./runner/target/release/rb-regress run

Expect 246/24/0 and **zero XPASS** on both. An XPASS means a finding I closed
was platform-specific and closed too broadly — that is exactly how R-025 was
caught.

Rebuilding `rb-regress` as well as `rb-cli` is not optional: skipping it once
already produced two false XPASS on macOS under a correct-looking sha. The
runner now warns when its own sources are newer than the binary.

## USE THE TOOLS, NOT THE MARKDOWN

    rb-regress fixtures     # corpus: 90 catalogued, all sha256-verified
    rb-regress validate     # manifests + bug list consistency
    rb-regress run          # the matrix
    rb-regress consolidate  # across hosts

`fixture_root` is local disk (`regression-tests/fixtures`), synced once from
`corpus_source`. A run never touches the network. Control: move `fixtures/`
aside and every corpus-backed case must report `skip-fixture`.

## WHAT IS READY TO FIX

From `docs/regression-fix-prompt.md`, which tranches all 24 by whether they
can actually be acted on. Ready now, no decision and no hardware needed:

- **R-005** — no error envelope under `--format json`. Cross-cutting: the
  format is a per-verb arg and the error path in `main` cannot see it. The
  `exit::CodedError` machinery added for R-004 is the half that already
  exists; `status.code` should come from `code_for`.
- **R-001 / R-002** — doc drift. Both would be caught permanently by the
  source-parity test `Regression_Bugs.md` lists under "Not covered".
- **R-021, R-023, R-022** — the heavy ones, in value order. Silent no-ops and
  silent data loss: `resize --size` reports success and changes nothing;
  `repack` exits 0 having lost every file; HPFS sector-by-sector round-trip is
  not byte-identical.

## DO NOT START THESE WITHOUT AN ANSWER

Four are decisions for the maintainer, not bugs to fix. Ask first:

1. **R-003** — implement `ls --format`, or correct the docs that claim it?
2. **R-016** — is "backup refuses non-flat containers" a defect or an
   unimplemented feature? Four red cases hang on the answer.
3. **R-035** — `.cbk` embeds the producing host's absolute path. Keep,
   normalise, or record a device identity instead?
4. **R-011** — should copy-protected G64 dumps open at all?

## BLOCKED, NOT FORGOTTEN

- **R-015, R-012** are upstream in `opticaldiscs`. A fixed 0.15.0 exists in
  the maintainer's working tree, unpublished. When it lands: bump the pin and
  re-run `optical.cue.unpadded-track-number` and
  `optical.cdda.no-data-track-opens` — both red on purpose, and they will flip
  to XPASS. `docs/opticaldiscs-upstream-prompt.md` has the detail.
- **R-020** (every AFFS volume we write is unmountable on a real Amiga) needs
  an emulator or hardware oracle. All 62 emulator / MiSTer-core oracles are
  `skip-manual`, so no automated run can confirm a fix. Teaching `verify` to
  drive FS-UAE is the harness feature that unblocks it.
- MiSTer's `rb-cli` is from 2026-07-27 and must be redeployed before its 12
  core oracles mean anything.

## FEATURE WORK QUEUED

`docs/missing_features_from_regression.md`, F-005 through F-007:

- **F-005** — GUI cannot extract a single file. Small: `browse_view.rs`
  already calls `read_file` in three places. Must surface the filesystem
  selector or the GUI can never reach both sides of a hybrid disc.
- **F-006** — IRIX support disks. **Needs scope** — three readings recorded,
  one of which cannot be verified without hardware.
- **F-007** — no optical fixture has nested directories, so
  `--path DIR --recursive` is implemented and unverified. Fixtures already
  catalogued; only the case is missing.

## CONVENTIONS THAT MATTER

- Cases assert **intended** behaviour, so they are red until fixed. Never make
  one pass by asserting broken behaviour — add it to `known-failures.toml`
  citing a finding instead. XPASS catches a stale entry.
- If a case turns out to assert the wrong thing, change it **deliberately and
  say so**. `cli.exit.missing-image-file` pinned exit 1 as "current
  documented-free behaviour" and contradicted the contract in `exit.rs`; it
  was corrected to 3 rather than weakening the fix.
- `platforms = ["windows"]` on a `[[known]]` entry scopes a platform-specific
  finding. Without it, the other platforms report a false XPASS.
- **Always run a control before believing a diagnosis.** Two wrong root causes
  this session died to one: `FILE_SHARE_DELETE` looked like the R-025 fix and
  changed nothing, and the `__MACOSX` sidecar looked like the R-027 cause and
  was already filtered. A test that isolates the mechanism settles it in one
  run.
- Engine code (`src/`) must compile on Rust 1.73 — your `cargo build` will not
  catch a violation. See CONTRIBUTING.md.
- Comments are one line, two at most. No Unicode glyphs in UI or log strings.
- Pre-commit runs `clippy --all-targets -- -D warnings` and does `git add -u`,
  which bundles every modified file — stash-dance for per-phase commits.
- Commit per phase (3-5 a session). **Never push without being asked.**
- Nothing private in the repo: corpus paths, machines and addresses live in
  gitignored `regression-tests/local.toml` only.
- Windows: use `C:\Windows\System32\OpenSSH\ssh.exe`, not Git Bash ssh, with
  `-o IdentitiesOnly=no`. linuxbox needs `-A` for anything touching GitHub;
  the Mac has its own key. macOS commands need `zsh -lc`. Export
  `MSYS_NO_PATHCONV=1` for any `rb-cli` call with a `/` path.
