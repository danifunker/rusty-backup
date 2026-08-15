# Regression Fix Prompt

A handoff for fixing the defects in [`Regression_Bugs.md`](Regression_Bugs.md).
Paste a tranche into a fresh session; each is independently shippable.

**Readiness, honestly.** Written when 30 findings were open. **Two remain as of
2026-08-14** — R-039 (EFS free list, filed after this document was written) and
R-011 (blocked on fixtures, not on effort). Everything this document was built
to hand off has been handed off and done.

Tranche C — the eight where "the symptom is known and the cause is not", the
state that does not clear on its own — cleared. Six were fixed, two turned out
not to be defects, and R-011 is the only row still live. That happened between
2026-08-10 and 2026-08-14, so if you are reading this expecting work, read
[`Regression_Bugs.md`](Regression_Bugs.md) first: this file is now mostly a
record of how the tranche was worked rather than a queue.

Struck-through rows are kept rather than deleted: the "what to do" column
records what each fix turned on, and two of them turned on the *report being
wrong*, which is the most reusable thing in this document.

The tranches below are sized to be reviewable.

---

## Ground rules — these override anything a tranche implies

1. **Never make a case pass by asserting the broken behaviour.** Cases assert
   *intended* behaviour and are red on purpose. If a case turns out to assert
   the wrong thing, say so and change it deliberately; do not weaken it to get
   green.
2. **Each fix must turn its named case green and leave every other case
   unchanged.** `rb-regress run` is **254 pass / 19 xfail / 0 fail** on
   Windows, macOS and Linux at `c6e66fd`. A fix that trades one red for
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

**Empty as of 2026-08-09** except the two that are blocked upstream. Eleven
findings started here; nine are fixed and struck through below, kept because
the "what to do" column records the decision each one turned on.

| Finding | Case that must go green | What to do |
|---|---|---|
| ~~R-006~~ **FIXED** | `fs.new-volume.prodos-default-name` | Default volume name `rusty-backup` contains `-`, which ProDOS forbids, so the verb always fails with defaults. Change the default (per-fs, or sanitise), and fix the message — it says "rename the file" when the offending string is the *volume* name. |
| ~~R-004~~ **FIXED** | `cli.exit.{csv,tsv}-on-nested-verb-is-usage-error`, `shrink.rejects-non-chd-output` | CSV/TSV rejection exits 1; documented as 2. Usage errors are 2. |
| ~~R-005~~ **FIXED** | `cli.envelope.error-envelope-on-failure` | No error envelope is emitted under `--format json`. Failures must produce the same envelope shape as successes. |
| ~~R-003~~ **FIXED** | `cli.envelope.ls-supports-format` | ~~**Decision.**~~ Decided: implement. Docs claim `ls` supports `--format`; it does not. Either implement it or correct the docs. Do not assume — see Decisions below. |
| ~~R-010~~ **FIXED** | `cli.flags.inspect-accepts-fs-type` | `inspect` has no `--fs-type`, so CP/M images cannot be inspected. `ls` already accepts it (`cli.flags.ls-accepts-fs-type` is green) — mirror that. |
| ~~R-026~~ **FIXED** | `subcmd.show.partmap` | `show partmap` cannot read an SGI disk that `inspect` reads fine. Two code paths disagree; make `show` use the one that works. |
| ~~R-027~~ **FIXED** | `read.apfs.apple-gpt` | A Finder-made `.zip` holding one `.dmg` is rejected as ambiguous because `__MACOSX/._*` counts as a second candidate. Ignore the AppleDouble sidecar. Every zip made on a Mac has one. |
| ~~R-034~~ **FIXED** | `edit.readonly.{lisa,alto}-refuses-a-write` | Refusing a write to a read-only FS reports the type as `unknown` and exits 1. Refusing is correct; the type should be the one `ls`/`inspect` just reported, and the exit code 4. **Check whether this also fixes R-031** — same shape. |
| R-015 **blocked upstream** | `optical.cue.unpadded-track-number` | A `.cue` with `TRACK 1` (unpadded) is rejected. Accept it. |
| R-012 **blocked upstream** | `optical.cdda.no-data-track-opens` | `optical info` rejects any disc with no data track (pure CD-DA). `optical.cdda.mixed-mode-still-opens` is the green working-half — keep it green. |
| ~~R-001, R-002~~ **FIXED** | `tests/doc_parity.rs` (three tests) | README partition-table list is missing AHDI and X68000; `src/fs/README.md` still lists ext as "planned". Both would be caught by the source-parity test noted below. |

**That source-parity test now exists.** `tests/doc_parity.rs` covers R-001,
R-002 and R-018 — the README partition-table list against
`PartitionTable::ALL_TYPE_NAMES`, `src/fs/README.md` against a capability table
growing back, and CONTRIBUTING.md's vintage feature list against the workflow's.
It is a `cargo test`, not an `rb-regress` case, because the claim is *about* the
binary rather than something the binary does.

---

## Tranche B — clear symptom, real engine work

Nine findings. The symptom is unambiguous and reproducible; the fix is not
mechanical. Take these one at a time.

**The three highest-value ones are done (2026-08-09) — and two of the three
were filed with the wrong cause, which is the lesson worth carrying:**

- ~~**R-021**~~ — fixed. Not a no-op: it warned and proceeded, leaving a
  filesystem describing twice the blocks its container held. `resize` now grows
  the file when the volume *is* the file, and refuses otherwise.
- ~~**R-023**~~ — fixed. **Nothing was lost.** A FAT long filename was dropped
  because `repack`, documented Human68k-only, accepted a plain FAT volume.
  Scope guard added.
- ~~**R-022**~~ — fixed. **Not a fidelity bug.** A bare HPFS volume was
  detected as an empty MBR, so `backup` wrote no partition file at all and
  exited 0. A detection probe closed it.

Reproduce and *measure* before fixing. Both wrong diagnoses had a one-command
control: count the non-zero bytes; list the backup folder.

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
  **Very likely the same shape as R-022**, which was a bare volume falling
  through to the MBR parse because no probe in `detect_superfloppy` claimed it.
  Read that fix first.
- ~~**R-016**~~ — **decided 2026-08-09: an unimplemented feature, not a
  defect.** Moved to
  [F-008](missing_features_from_regression.md#f-008); the four cases keep their
  assertions and now cite F-008, which `rb-regress validate` accepts as of the
  same date. No longer in this tranche.

---

## Tranche C — cannot be prompted yet

Eight findings where the symptom is recorded and the cause is not. Writing a
fix prompt for these would be guessing. Each needs a scoped investigation
first, and the investigation is the deliverable.

| Finding | Case | What the investigation has to establish |
|---|---|---|
| ~~R-020~~ | `oracles/fsuae/affs_mount.py` | ~~Every AFFS volume we write is "Not a DOS disk" on a real Amiga.~~ **CLOSED 2026-08-14.** The hypothesis was right: `header_key` had to be 0, and the bitmap was a block short of the geometry. Both fixed by a190182; Kickstart 3.1 then mounted the volume Read/Write. The half of the prediction that was *wrong* is worth keeping — the fix did **not** need to land in `affs_fsck`, which never inspected `header_key` at all. |
| ~~R-030~~ | `edit.real.affs-workbench13` | ~~A real Workbench 1.3 AFFS volume cannot be opened at all.~~ **CLOSED 2026-08-10** — neither OFS-vs-FFS nor a root-block layout difference: the root block was being located from the end of the *file* rather than the partition. Related to R-020 but not the same root cause. |
| ~~R-029~~ | `edit.real.efs-small` | ~~EFS computes block addresses far outside the image.~~ **NOT A DEFECT, 2026-08-10** — the fixture is a deliberate 4 MB prefix capture, and the case asked it to do what a prefix cannot. |
| ~~R-013~~ | `fs.detect.ufs-{solaris-entry-types,no-absurd-sizes}` | ~~Solaris UFS directories reported as files.~~ **CLOSED 2026-08-10** — cylinder-group layout, as guessed: UFS1's rotational cylinder-group offset was ignored. Not endianness. |
| ~~R-028~~ | `edit.apple-dos.put-get` | ~~Apple DOS 3.3 reports three sizes for one file.~~ **CLOSED 2026-08-10** — the length lives in a type-B header and was not being stored; all three now agree. |
| ~~R-031~~ | `edit.real.apple-dos-invaders` | ~~A real Apple DOS 3.3 disk detects as `unknown`.~~ **NOT A DEFECT, 2026-08-10** — the disk carries no filesystem at all, so `Unknown` is the correct answer. R-034's fix was unrelated. |
| ~~R-035~~ | none — a `parity` finding | ~~`.cbk` embeds the producing host's absolute path.~~ **CLOSED 2026-08-09** — decided and shipped: the path is normalised to a device leaf. |
| R-011 | `fmt.g64.standard-dump-opens` (working half only) | G64 decoding fails on copy-protected / patched dumps. **Deliberately undecided** — whether it *should* succeed is an open question, and asserting either way prejudges it. Decide scope before writing anything. |

---

## Decisions that must not be quietly resolved

Four were open. Three are now answered; each changed what the fix was, which
is why they belonged to the maintainer rather than to whoever picked up the
ticket.

1. ~~**R-003**~~ — **decided: implement the flag**, not correct the doc. `ls`
   is the most script-facing verb in the CLI. Shipped 2026-08-09.
2. ~~**R-016**~~ — **decided: an unimplemented feature.** Moved to
   [F-008](missing_features_from_regression.md#f-008), cases retagged,
   `validate` taught to accept an F-nnn citation. 2026-08-09.
3. ~~**R-035**~~ — **decided: normalise the path** to a device leaf rather than
   keeping the absolute path or inventing a device identity.
4. **R-011** — should copy-protected G64 dumps open at all? **Still open.**

---

## Blocked on something other than effort

- ~~**R-020** needs an emulator or hardware oracle.~~ **Unblocked 2026-08-14.**
  The suggested route was the one taken: `verify` was taught to drive FS-UAE,
  via `oracles/fsuae/affs_mount.py`. No MiSTer time was needed. The other
  ~60 emulator and MiSTer-core oracles are still `skip-manual` — this
  unblocked one, not the class.
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
