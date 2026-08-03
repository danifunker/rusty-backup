# Runbook — how to run a regression

Accurate as of commit `bb20c16`. Everything below has been executed; nothing
here is aspirational. Where a thing does not exist yet it says so plainly,
because a runbook that describes unimplemented features is worse than no
runbook.

---

## What exists today, in one line

`rb-regress run` executes **68 cases across tiers 0-2 on one machine**. That
is rb-cli checked against itself and against reference fixtures. **No
third-party verification runs yet** — see § The run/verify split.

---

## Prerequisites

1. **A clean git tree.** Required if you use `--require-clean` (you should).
   Results are attributed to a commit, and a dirty tree means no commit
   describes what was tested.
2. **rb-cli built from that tree.** Build it as part of the run — this is
   what makes the recorded sha trustworthy. See § Build provenance.
3. **Fixtures — optional.** Tiers 0-1 need none. Tier 2 resolves 45 fixtures
   from `tests/fixtures/` via `repo:` catalogue rows, so a bare clone works
   with no network. The NAS corpus adds the rest.

---

## The short version

```bash
cargo build --release --bin rb-cli
cd regression-tests/runner
cargo run --release -- run --rb-cli ../../target/release/rb-cli --tiers 0-2 --require-clean
```

On Windows the binary is `../../target/release/rb-cli.exe`.

With the fixture corpus mounted, add:

```bash
--fixture-root //NAS/share/rb-fixtures/fixtures
```

Expect roughly: **68 cases, ~52 pass, ~16 fail**, under a minute. Every
failure maps to a finding in `docs/Regression_Bugs.md` — none are unknown.

---

## What you get

A bundle under `regression-tests/runs/<stamp>-<host>-<platform>/`:

| File | Contents |
|------|----------|
| `summary.md` | Read this first. Counts, then failures clustered by group. |
| `results.jsonl` | One self-contained line per case, appended as the run proceeds |
| `env.json` | Host, platform, build label, git sha, branch, clean flag |
| `failures/<case-id>/` | `cmd.txt` (runnable as-is), `stdout`, `stderr`, `assertions.json` |
| `missing-fixtures.md` | Fixture IDs referenced but not resolvable — the shopping list |
| `oracle-skips.md` | External tools a case asked for and did not find |

A failing case captures enough to reproduce **without the harness** — if you
have to reconstruct the command from a manifest, that is a bug in the capture.

---

## Seeing where things stand

```bash
cargo run --release -- consolidate
```

Merges every `results.jsonl` under `runs/` and reports progress by verdict,
by platform, and by group worst-first, then lists failing cases. This is the
"how far have we got" view, and it works across hosts and across months
because every result line carries its own `run_id`, `git_sha` and
`rb_version`.

Point it at a different tree to consolidate a regression spread across hosts:

```bash
cargo run --release -- consolidate //NAS/share/rb-fixtures/regressions/2026-08
```

It groups by sha and **warns rather than averaging** if results span several
builds — a combined figure across two builds describes no program that ever
existed.

---

## Other commands

```bash
rb-regress list --tiers 0-2     # what would run, without running it
rb-regress validate             # parse every manifest, report problems
rb-regress plan                 # who produces / verifies what, and the gaps
rb-regress query <name>         # ask the registry a question
rb-regress export --check       # verify the committed JSON snapshot is current
```

`query` with no argument lists the named queries. The useful ones:
`unverified-writes`, `unfixtured-reads`, `coverage`, `platform-pins`.

---

## Build provenance

`rb-cli` does not bake its commit into `--version` — that would mean editing
`build.rs`, which is engine code and deliberately out of scope for this
suite. So the harness derives the sha from the working tree, and records it
as e.g. `rb-cli 0.1.0+g3eb322a`.

That substitution is only sound because of two rules **together**:

1. The tree is clean (`--require-clean`), so a commit describes the source.
2. **rb-cli is built at run time from that tree.**

The second is the load-bearing one. If the harness merely used an `rb-cli`
it happened to find, the sha would describe the checkout rather than the
binary — and on any host where a stale binary had been copied in, the label
would be confidently wrong. Building it as part of the run makes the label
true by construction. Do not skip the build step.

---

## The run/verify split

**Today there is no split, because verification is not implemented.**

`run` executes cases. A case is a sequence of rb-cli invocations with
assertions on exit codes, JSON envelope fields, and produced files. All 68
current cases are tiers 0-2:

- **tier 0** (23 cases) — rb-cli runs, exit codes and envelope shape hold
- **tier 1** (26 cases) — rb-cli builds a volume and reads it back. A smoke
  test only; per `README.md`, this proves nothing about format correctness
  because a bug on both sides cancels out
- **tier 2** (19 cases) — read third-party reference fixtures

Tiers 3-7 have **no case manifests**, and the case schema has **no oracle
step** — there is no `[[case.oracle]]` block in `manifest.rs`. So nothing
currently hands an artifact to `fsck.ext4`, `chdman` or a MiSTer core.

### How the split is designed to work

**Produce runs everywhere; only verification is OS-specific.**

rb-cli is cross-platform, so every host can generate the whole artifact set.
That matters because **the producer is the thing under test** — rb-cli on
macOS and rb-cli on Windows are different builds taking different code paths,
so you want all three artifacts, not one. An earlier design produced on a
single host and shipped the result to whoever held the oracle; that tested
one build and called it coverage.

Verification is the only OS-specific half. It walks the artifact tree as a
**queue**, checks whatever it has an oracle for, and records a reason for
everything it skips. It does not care which OS produced what.

```
regressions/<id>/
  artifacts/<producer-os>/<format-id>/
      image.<ext>
      meta.json    # format, producer os + sha, source fixture, argv, sha256
  verifications/<verifier-os>/
      <format-id>.<producer-os>.json
  parity/
      produce.json     # cross-OS byte compare
      read.json        # same fixture read on N hosts
```

Two independent commands, neither needing to know about the other:

```bash
rb-regress produce --out regressions/2026-08/artifacts/$OS
rb-regress verify  --artifacts regressions/2026-08/artifacts                    --out regressions/2026-08/verifications/$OS
```

So the Mac verifies Windows-produced HFS, the MiSTer core verifies
Linux-produced AFFS, and nothing coordinates. Verification does **not** belong
in the case schema — it is a separate pass over artifacts driven by
`oracles.toml` plus local capability.

### Two checks that need no oracle at all

Producing on every OS gives two cross-checks for free, and they catch the
class of bug a cross-platform regression exists to find:

- **Producer parity** — same format, same arguments, three OSes. The bytes
  should match, or it is a finding. The OSes check each other.
- **Read parity** — same pre-validated fixture, `inspect --format json` on
  three OSes. Outputs must match.

Producer parity needs one wrinkle handled. Measured 2026-08-02:

| Builder | Same command twice |
|---------|--------------------|
| FAT, NTFS, ProDOS | byte-identical |
| HFS, ext, HFS+ | 6-13 bytes differ (embedded creation timestamps) |

So parity cannot be a naked sha compare. The fix is self-calibrating: produce
each artifact **twice on the same machine**, diff to discover the volatile
byte ranges empirically, then compare across machines with those offsets
masked. No per-format table to maintain, and any difference outside the
volatile set is a genuine cross-OS divergence.

### What is missing to make that real

1. `produce` — walk the registry, build every artifact rb-cli can, write
   `meta.json` beside each.
2. Volatile-range discovery (produce twice, diff) and the parity comparison.
3. `verify` — walk the artifact queue, run the oracles this host has, record
   verdicts and skip reasons.
4. Read-parity comparison across hosts.

(1) and (2) are useful on a single machine immediately — they catch
determinism regressions — and become the cross-OS check the moment a second
host runs them. (3) needs no remote execution to start: a host can verify
artifacts it produced itself, and artifact trees can be synced by any means,
including the NAS.

---

## Known limitations, so nothing surprises you

- **Tiers 3-7 do not exist.** `--tiers 0-6` selects the same 68 cases as
  `--tiers 0-2`.
- **`--device-allowlist` does not exist.** `HARDWARE.md` describes it;
  `--allow-hardware` is a flag but no hardware cases are written, so it
  currently gates nothing.
- **`formats.toml` covers 121 formats; cases cover far fewer.** The registry
  knows the surface; the case matrix has not caught up.
- **65 read gaps** — formats with no third-party reference fixture. See
  `GAPS.md`; that is a sourcing problem, not a design one.
- **`COVERAGE.md`, `VERIFICATION-MATRIX.md` and `GAPS.md` are hand-written**
  and partly duplicate the registry. Some rows are already stale relative to
  `data/*.toml`; trust the registry (`query`, `plan`) over the prose.
