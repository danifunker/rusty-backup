# Resume: regression fixes

Paste this into a fresh session to continue.

---

Continuing regression fixes on rusty-backup (branch: `regression-fixes`,
pushed and verified on all three hosts at `c6e66fd`).

## STATE

- Suite: **259 pass / 19 xfail / 0 fail**, zero XPASS, on Windows, macOS and
  Linux — all three measured at `93a6d53`, with the OS/2 fixture present on
  all three.
- **Counts below this line were last true on 2026-08-09 and are not now.** As
  of 2026-08-14 the live tally in
  [`Regression_Bugs.md`](Regression_Bugs.md) is 31 fixed, 2 not-a-defect,
  2 reclassified as feature gaps, and **two findings open** — R-039 (EFS free
  list) and R-011 (blocked on fixtures) — plus R-019, accepted. R-020 and
  R-038 both closed 2026-08-14. `data/known-failures.toml` is down to 4
  entries, all F-008. Read the table there, not this paragraph.
- R-016 is no longer a defect: it was reclassified as
  [F-008](missing_features_from_regression.md#f-008), and `rb-regress validate`
  now accepts an `F-nnn` citation as well as an `R-nnn` one.
- `main` is at merge commit `48cee1f`; this branch is ahead of it and has
  **not** been merged.

## THE FOUR DECISIONS ARE NOW THREE ANSWERS AND ONE QUESTION

Do not re-ask the first three.

1. **R-003** — decided: *implement* `ls --format`, not correct the docs.
   Shipped.
2. **R-016** — decided: an unimplemented *feature*. Now F-008.
3. **R-035** — decided: *normalise* the path to a leaf. Shipped.
4. **R-011** — **still open.** Should copy-protected G64 dumps open at all?
   Asserting either way prejudges it, so only the working half is pinned.

## USE THE TOOLS, NOT THE MARKDOWN

    rb-regress fixtures     # corpus: 91 catalogued, all sha256-verified
    rb-regress validate     # manifests + bug list consistency
    rb-regress run          # the matrix
    rb-regress consolidate  # across hosts

`fixture_root` is local disk (`regression-tests/fixtures`), synced once from
`corpus_source`. A run never touches the network. Control: move `fixtures/`
aside and every corpus-backed case must report `skip-fixture`.

## WHAT IS READY TO FIX

From [`regression-fix-prompt.md`](regression-fix-prompt.md), which tranches the
remainder. The three highest-value ones are done (R-021, R-022, R-023), so what
is left needs investigation before it needs a fix:

- **R-008b / R-008a** — `new volume affs --size 4M` panics, exit 101, no file.
  A panic with no output is the worst remaining failure mode, and R-008a shares
  the fix: volumes above 4066 blocks have uncovered tail blocks.
- **R-024** — one `put` into a fresh 3 MB AFFS volume makes `fsck --checkonly`
  report errors. Data reads back fine, so the damage is to allocation
  structures. Three distinct AFFS bugs — R-008 is the formatter, R-024 the
  editor, R-020 the root block. Do not conflate them. (All three closed by
  2026-08-14; kept here because the "do not conflate" advice is still the
  right way to read the AFFS entries.)
- **R-033** — a QL Microdrive `.mdv` fails at MBR detection although its own
  probe matches it exactly. **Very likely the same shape as R-022**, which was
  a bare volume falling through to the MBR parse because no probe claimed it.
  Read the R-022 fix first; this may be twenty minutes.
- **R-013, R-028, R-029, R-030, R-031, R-032** — tranche C. The symptom is
  recorded, the cause is not. Each needs a scoped investigation, and the
  investigation is the deliverable.

## BLOCKED, NOT FORGOTTEN

- **R-015, R-012** are upstream in `opticaldiscs`. A fixed 0.15.0 exists in the
  maintainer's working tree, unpublished. When it lands: bump the pin and
  re-run `optical.cue.unpadded-track-number` and
  `optical.cdda.no-data-track-opens` — both red on purpose, both will flip to
  XPASS. `docs/opticaldiscs-upstream-prompt.md` has the detail.
- ~~**R-020** needs an emulator or hardware oracle.~~ **Unblocked and closed
  2026-08-14.** The harness feature this asked for exists:
  `oracles/fsuae/affs_mount.py` drives FS-UAE, and Kickstart 3.1 mounts our
  volume Read/Write. The remaining 61 emulator / MiSTer-core oracles are still
  `skip-manual` — this unblocked one of them, not the class.
- **R-025** is Windows-only and correctly scoped with `platforms = ["windows"]`.
- MiSTer's `rb-cli` is from 2026-07-27 and must be redeployed before its 12
  core oracles mean anything.

## FIXTURE ADMITTED 2026-08-09

`fs.hpfs.os2-warp45.hd` — a real OS/2 Warp 4.52 install, monolithicSparse VMDK,
MBR type-0x07 HPFS at LBA 63, 136 MB zstd in the annex. It closes the HPFS gap
R-022 left and carries two things nothing else in the corpus does:

- **A partitioned HPFS volume.** R-022's control (the probe must not hijack an
  ao486-shaped disk) was a hand-synthesized MBR; it is now
  `fs.detect.hpfs-partitioned-stays-mbr`.
- **A real non-flat container.** `backup` refuses it exactly as F-008
  describes, so that gap is not an artifact of how the synthetic containers
  were built.

`read.hpfs.os2-warp45` reads it end to end — 4722 files, 471 dirs, long names
with spaces, fsck clean.

Note the original drop is still at `new/OS2 Warp 4.52.zip` on the NAS; the
annex copy is independent, so the drop can be deleted whenever.

## HOW THE CORPUS REACHES THE OTHER HOSTS

Not by `rb-regress fixtures --sync`. **linuxbox and the Mac have
`corpus_source` commented out** — neither can reach the distribution share, and
their `local.toml` says so. The corpus was scp'd to them from this Windows box,
and that is still the mechanism.

So admitting an annex fixture is three steps per host, not one:

1. `scp` the file into `regression-tests/fixtures-large/`.
2. Append the catalogue row to that host's `regression-tests/fixture-map.tsv`
   — gitignored, so it does **not** arrive with `git pull`. This is the step
   that is easy to miss: without it the file is present and the case still
   reports `skip-fixture`.
3. `rb-regress fixtures` on the host to confirm `N catalogued - N verified, 0
   missing, 0 CORRUPT`.

Windows `scp.exe` cannot resolve an MSYS `/tmp/...` path; stage anything you
are copying at a real Windows path first.

A case whose fixture is missing degrades to `skip-fixture`, not a failure — so
a forgotten sync looks like a green run with a smaller number. Compare the pass
count across hosts, not just the fail count.

## FEATURE WORK QUEUED

`docs/missing_features_from_regression.md`, F-005 through F-008:

- **F-005** — GUI cannot extract a single file. Small: `browse_view.rs` already
  calls `read_file` in three places. Must surface the filesystem selector or the
  GUI can never reach both sides of a hybrid disc.
- **F-006** — IRIX support disks. **Needs scope** — three readings recorded, one
  of which cannot be verified without hardware.
- **F-007** — no optical fixture has nested directories, so
  `--path DIR --recursive` is implemented and unverified. Fixtures already
  catalogued; only the case is missing.
- **F-008** — `backup` reads only flat-layout sources. `inspect` already opens
  all four containers, so the decoding exists and `backup` simply takes a
  different route to the bytes. Routing it through the same path is the whole
  feature. `backup.container.inspect-reads-what-backup-cannot` is green and
  pins the asymmetry — read it first.

## CONVENTIONS THAT MATTER

- Cases assert **intended** behaviour, so they are red until fixed. Never make
  one pass by asserting broken behaviour — add it to `known-failures.toml`
  citing a finding instead. XPASS catches a stale entry.
- If a case turns out to assert the wrong thing, change it **deliberately and
  say so**. Three precedents now: `cli.exit.missing-image-file` pinned exit 1
  and contradicted `exit.rs`; `cli.envelope.error-envelope-on-failure` pinned
  the same 1 for the same reason; `resize.repack.keeps-data` built a plain FAT
  volume for a verb that is Human68k-only. All three were corrected, not
  weakened.
- **Doubt the diagnosis, not just the code.** Two findings this session were
  filed with the wrong cause. R-023 said "every file is gone" — nothing was
  lost, a FAT long filename was dropped. R-022 said "not byte-identical" — the
  backup was empty. Reproduce and *measure* before fixing; both had a
  one-command control (count the non-zero bytes; list the backup folder).
- **Always run a control before believing a diagnosis.** R-023's control was
  repacking a *real* Human68k volume, which worked perfectly and proved the
  clone sound and the input wrong. R-022's was a synthesized MBR-partitioned
  HPFS disk, proving the new probe does not hijack partitioned disks.
- `platforms = ["windows"]` on a `[[known]]` entry scopes a platform-specific
  finding. Without it, the other platforms report a false XPASS.
- A `[[known]]` entry may cite an `R-nnn` defect **or** an `F-nnn` feature gap.
  Before 2026-08-09 only the former validated, which is part of why a feature
  gap looked like it had to be filed as a bug.
- `tests/doc_parity.rs` guards README / CONTRIBUTING claims against the source.
  It is a `cargo test`, not an `rb-regress` case, because the claim is *about*
  the binary rather than something the binary does.
- Engine code (`src/`) must compile on Rust 1.73 — your `cargo build` will not
  catch a violation. See CONTRIBUTING.md.
- Comments are one line, two at most. No Unicode glyphs in UI or log strings.
- Pre-commit runs `clippy --all-targets -- -D warnings` and does `git add -u`,
  which bundles every modified file — stash-dance for per-phase commits.
- Commit per phase (3-5 a session). **Never push without being asked.**
- **After a push, check CI and drive it green.** `gh run list` / `gh run view
  <id>`. A green `rb-regress run` is not a green pipeline, and this was learned
  the hard way — a push on 2026-08-09 turned Release red and nobody noticed
  until it was mentioned in passing. Three specifics:
  - CI's test step is **`cargo test --release`**, not `cargo test`. Run that
    before pushing. (macOS x64 skips tests — it is cross-compiled — so only the
    arm64 macOS job exercises them there.)
  - **`gh run list` saying "success" does not mean every job passed.** Jobs
    marked `continue-on-error: true` — the MiSTer `rb-cli-mini` build is one —
    fail silently without reddening the run. Read the job list from
    `gh run view <id>`. The mini build was broken for a full day that way.
  - **Non-default feature sets are not covered locally.** The MiSTer one is
    `cargo check --bin rb-cli --no-default-features --features
    chd,pure-zstd,remote,optical,tui`. Anything touching zstd must go through
    `crate::rbformats::zstd_compat`, never the `zstd` crate directly, or it
    compiles on the desktop and breaks there.
- **Platform-dependent std APIs are the other thing local runs miss.**
  `Path::file_name` treats `\` as a separator only on Windows, which is how a
  Windows-path assertion passed locally and failed on every Unix job.
- Nothing private in the repo: corpus paths, machines and addresses live in
  gitignored `regression-tests/local.toml` only.
- Windows: use `C:\Windows\System32\OpenSSH\ssh.exe`, not Git Bash ssh, with
  `-o IdentitiesOnly=no`, and `GIT_SSH_COMMAND` for a push. **Fetching on the
  remote hosts is easiest over HTTPS** (`git fetch
  https://github.com/danifunker/rusty-backup.git regression-fixes`) — the repo
  is public, linuxbox's own key needs `-A` forwarding and the Mac's key needs
  an agent that non-interactive ssh does not have. macOS commands need
  `zsh -lc`. Export `MSYS_NO_PATHCONV=1` for any `rb-cli` call with a `/` path.
- Rebuilding `rb-regress` as well as `rb-cli` is not optional: skipping it once
  already produced two false XPASS on macOS under a correct-looking sha. The
  runner warns when its own sources are newer than the binary.
