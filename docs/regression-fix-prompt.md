# Regression Fix Prompt

A handoff for fixing the defects in [`Regression_Bugs.md`](Regression_Bugs.md).
Paste a tranche into a fresh session; each is independently shippable.

**Readiness, honestly.** 30 findings are open. 11 are specified well enough to
fix without further investigation. 9 more have an unambiguous symptom and a
red case but need real engine work. 8 cannot be turned into a fix prompt yet —
the symptom is known and the cause is not, so a prompt would be guessing. 2 are
decisions rather than defects and must not be quietly resolved by whoever
picks them up.

One prompt for all 30 would be the wrong shape. The tranches below are sized
to be reviewable.

---

## Ground rules — these override anything a tranche implies

1. **Never make a case pass by asserting the broken behaviour.** Cases assert
   *intended* behaviour and are red on purpose. If a case turns out to assert
   the wrong thing, say so and change it deliberately; do not weaken it to get
   green.
2. **Each fix must turn its named case green and leave every other case
   unchanged.** `rb-regress run` is 226–228 pass / 33–35 xfail / 0 fail on
   Windows, macOS and Linux at `c3e1984`. A fix that trades one red for
   another is not a fix.
3. **Remove the entry from `regression-tests/data/known-failures.toml`** when a
   finding goes green, and strike the row through in `Regression_Bugs.md`,
   keeping the original report below a **FIXED** note.
4. **Engine code (`src/`) must compile on Rust 1.73.** Your `cargo build` will
   not catch a violation, and clippy's autofixes (`is_multiple_of`,
   `io::Error::other`, `iter::repeat_n`) all postdate it. Route through
   `crate::compat` / `crate::rust173_compat`. See CONTRIBUTING.md.
5. **Comments are one line, two at the absolute most.** Say *why*.
6. **The pre-commit hook runs `clippy --all-targets -- -D warnings`.** It also
   does `git add -u`, which bundles every modified file — stash-dance if you
   want per-phase commits.
7. **Run the doc parity pass** (README tables, `docs/full_MiSTer_support_status.md`,
   `DISK_IMAGE_EXTS`) for anything user-facing. CLAUDE.md § Pre-commit
   documentation sync has the checklist.
8. **Verify on more than one platform where the finding is platform-shaped.**
   R-025 taught this: it is Windows-only, and until it was scoped the other two
   platforms reported false XPASS.

---

## Tranche A — fully specified, mechanical

Eleven findings. Each has a repro, an expected behaviour, and a red case. No
investigation needed; the work is deciding the exact wording and doing it.

| Finding | Case that must go green | What to do |
|---|---|---|
| R-006 | `fs.new-volume.prodos-default-name` | Default volume name `rusty-backup` contains `-`, which ProDOS forbids, so the verb always fails with defaults. Change the default (per-fs, or sanitise), and fix the message — it says "rename the file" when the offending string is the *volume* name. |
| R-004 | `cli.exit.{csv,tsv}-on-nested-verb-is-usage-error`, `shrink.rejects-non-chd-output` | CSV/TSV rejection exits 1; documented as 2. Usage errors are 2. |
| R-005 | `cli.envelope.error-envelope-on-failure` | No error envelope is emitted under `--format json`. Failures must produce the same envelope shape as successes. |
| R-003 | `cli.envelope.ls-supports-format` | **Decision.** Docs claim `ls` supports `--format`; it does not. Either implement it or correct the docs. Do not assume — see Decisions below. |
| R-010 | `cli.flags.inspect-accepts-fs-type` | `inspect` has no `--fs-type`, so CP/M images cannot be inspected. `ls` already accepts it (`cli.flags.ls-accepts-fs-type` is green) — mirror that. |
| R-026 | `subcmd.show.partmap` | `show partmap` cannot read an SGI disk that `inspect` reads fine. Two code paths disagree; make `show` use the one that works. |
| R-027 | `read.apfs.apple-gpt` | A Finder-made `.zip` holding one `.dmg` is rejected as ambiguous because `__MACOSX/._*` counts as a second candidate. Ignore the AppleDouble sidecar. Every zip made on a Mac has one. |
| R-034 | `edit.readonly.{lisa,alto}-refuses-a-write` | Refusing a write to a read-only FS reports the type as `unknown` and exits 1. Refusing is correct; the type should be the one `ls`/`inspect` just reported, and the exit code 4. **Check whether this also fixes R-031** — same shape. |
| R-015 | `optical.cue.unpadded-track-number` | A `.cue` with `TRACK 1` (unpadded) is rejected. Accept it. |
| R-012 | `optical.cdda.no-data-track-opens` | `optical info` rejects any disc with no data track (pure CD-DA). `optical.cdda.mixed-mode-still-opens` is the green working-half — keep it green. |
| R-001, R-002 | none (doc drift) | README partition-table list is missing AHDI and X68000; `src/fs/README.md` still lists ext as "planned". Both would be caught by the source-parity test noted below. |

**Worth doing while in here:** R-001/R-002/R-018 are all documentation drifting
from code, and all three were found by hand. A source-parity test — comparing
the README tables against the `PartitionTable` enum and the `fs/mod.rs`
dispatch — would guard the whole class. It is currently listed as "Not
covered" in `Regression_Bugs.md`.

---

## Tranche B — clear symptom, real engine work

Nine findings. The symptom is unambiguous and reproducible; the fix is not
mechanical. Take these one at a time.

**Highest value first — these three are silent data loss or silent no-ops,
the worst failure shape in a tool whose job is moving data:**

- **R-021** (`resize.to-explicit-size`) — `resize --size 16M` on an 8M volume
  prints "resize complete", exits 0, changes nothing. Nothing downstream has
  any reason to check.
- **R-023** (`resize.repack.keeps-data`) — `repack` exits 0 and every file in
  the volume is gone. Human68k.
- **R-022** (`roundtrip.hpfs.raw`) — `backup --sector-by-sector` then `restore`
  is not byte-identical for HPFS. FAT, NTFS, ext4, HFS, minix3, EFS and ProDOS
  all survive the same path, so it is HPFS-specific, not a backup-format bug.

**Then:**

- **R-008b** (`fs.new-volume.affs.{4m,32m}`) — `new volume affs --size 4M`
  panics, exit 101, no file produced. R-008a (`fs.new-volume.affs`,
  `...bitmap-boundary-plus-one`) shares the fix: volumes above 4066 blocks have
  uncovered tail blocks.
- **R-024** (`edit.affs.put-get`) — one `put` into a fresh 3 MB AFFS volume
  makes `fsck --checkonly` report errors. Data reads back fine, so the damage
  is to allocation structures. This is the editor; R-008 is the formatter;
  R-020 is the root block. Three distinct AFFS bugs — do not conflate them.
- **R-025** (`subcmd.squashfs.put-rebuilds`, `meta.xattr.set-list-rm`,
  both `platforms = ["windows"]`) — the rebuild-and-replace path fails its
  rename on Windows. Confirmed passing on macOS and Linux.
- **R-032** (`edit.sfs.put-get`) — SFS `put` fails on any volume with a
  multi-leaf extent btree, i.e. any real one. The driver is documented as
  single-leaf-only, so this is the known ceiling being hit, not a surprise.
- **R-033** (`read.qdos.microdrive`) — a QL Microdrive `.mdv` fails at MBR
  detection although its own probe matches it exactly. Detection ordering.
- **R-016** (`backup.container.{chd,vhd-dynamic,qcow2,vmdk-sparse}`) —
  **Decision.** `backup` accepts only flat-layout sources.
  `backup.container.inspect-reads-what-backup-cannot` is green and proves
  `inspect` opens exactly what `backup` refuses. Is this a defect or an
  unimplemented feature? See Decisions.

---

## Tranche C — cannot be prompted yet

Eight findings where the symptom is recorded and the cause is not. Writing a
fix prompt for these would be guessing. Each needs a scoped investigation
first, and the investigation is the deliverable.

| Finding | Case | What the investigation has to establish |
|---|---|---|
| R-020 | none — hand-verified | Every AFFS volume we write is "Not a DOS disk" on a real Amiga, at every size. Working hypothesis: root block `header_key` must be 0 and we write the block number. **Unconfirmed.** The fix must land in **both** the formatter and `affs_fsck`, which currently agree with each other and are both wrong. Needs an emulator or hardware oracle to confirm — see Blocked below. |
| R-030 | `edit.real.affs-workbench13` | A real Workbench 1.3 AFFS volume cannot be opened at all — read, fsck and write alike. Establish whether this is OFS-vs-FFS, an older root-block layout, or the same root cause as R-020. |
| R-029 | `edit.real.efs-small` | EFS computes block addresses far outside the image; `fsck` fails on an unmodified volume. Find where the address computation diverges from the on-disk geometry. |
| R-013 | `fs.detect.ufs-{solaris-entry-types,no-absurd-sizes}` | Solaris UFS directories are reported as files, one with a garbage size. Likely endianness or cylinder-group layout. |
| R-028 | `edit.apple-dos.put-get` | Apple DOS 3.3 reports three sizes for one file: 104 in, 512 by `ls`, 256 by `get`. Establish which is right and which two are wrong. |
| R-031 | `edit.real.apple-dos-invaders` | A real Apple DOS 3.3 disk detects as `unknown` although our own output does not. **Try R-034's fix first** — same shape, and one fix may cover both. |
| R-035 | none — a `parity` finding | `.cbk` embeds `source_device`, the producing host's absolute path, so it is not reproducible across machines and leaks the host's directory layout. **Decision** before any fix: keep the field, normalise it, or record a device identity instead. Note `expect_divergence` masks byte *ranges* and cannot express a divergence that changes the file's *length*. |
| R-011 | `fmt.g64.standard-dump-opens` (working half only) | G64 decoding fails on copy-protected / patched dumps. **Deliberately undecided** — whether it *should* succeed is an open question, and asserting either way prejudges it. Decide scope before writing anything. |

---

## Decisions that must not be quietly resolved

Four. Each changes what the fix is, so they belong to the maintainer, not to
whoever picks up the ticket.

1. **R-003** — implement `ls --format`, or correct the docs?
2. **R-016** — is "backup refuses non-flat containers" a defect or an
   unimplemented feature? It is currently filed as a defect with four red
   cases; if it is a feature, it belongs in
   `missing_features_from_regression.md` and the cases should move to a
   capability list.
3. **R-035** — keep, normalise, or replace `source_device`?
4. **R-011** — should copy-protected G64 dumps open at all?

---

## Blocked on something other than effort

- **R-020** needs an emulator or hardware oracle. All 62 emulator and
  MiSTer-core oracles resolve to `skip-manual`: `verify` cannot invoke them,
  so no automated run can confirm a fix. Either book MiSTer time, or teach
  `verify` to drive FS-UAE — which is the next harness feature regardless.
- The MiSTer's `rb-cli` is from 2026-07-27 and must be redeployed before its
  12 core oracles mean anything.

---

## How to verify a fix

```bash
rb-regress run --filter <case-id>          # the named case, now green
rb-regress run                             # everything else unchanged
rb-regress validate                        # bug list still consistent
```

Then on the other two hosts, because a platform-shaped bug is not visible from
one machine:

```bash
scripts/regress-all.sh
```

Baseline at `c3e1984`, all three hosts, corpus verified 90/90:

```
windows   226 pass / 35 xfail / 0 fail
macos     228 pass / 33 xfail / 0 fail
linux     228 pass / 33 xfail / 0 fail
```

The two-case gap is R-025, correctly scoped to Windows.
