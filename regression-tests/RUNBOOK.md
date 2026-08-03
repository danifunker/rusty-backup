# Runbook — how to run a regression

Accurate as of commit `3eb322a`. Everything below has been executed; nothing
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

Point it at a different tree to consolidate a campaign:

```bash
cargo run --release -- consolidate //NAS/share/rb-fixtures/runs
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

The separation is **by host capability, not by process**. `plan` already
computes it:

```
STAGE 1  produce    rb-cli writes an artifact          (a host that can build)
STAGE 2  transfer   the artifact crosses machines      (only when needed)
STAGE 3  verify     an oracle judges it                (the host holding it)
```

One agent binary claims whatever job kinds its host can do. Windows produces
and runs `chdman`/`ghostexp`; the Linux box mostly verifies what Windows
produced; the MiSTer HPS is plumbing plus `fsck.*`, while the *core* is the
authoritative oracle for Amiga, X68000 and Atari formats.

Run `rb-regress plan` to see the current shape — 61 verify jobs across six
hosts, 41 of which need an artifact to cross a machine boundary.

### What is missing to make that real

1. An oracle step in the case schema (`[[case.oracle]]` with tool, role,
   args, expected exit).
2. Local oracle execution — enough for `chdman`, `qemu-img`, `7z` on the
   machine that produced the artifact.
3. Remote execution and artifact transfer (ssh / wsl), which is what turns
   `plan`'s stage 2 and 3 into something that runs.
4. Case manifests for tiers 3-6.

(1) and (2) together would cover a large share of the container matrix on a
single machine, because `chdman` and `qemu-img` are already present and
proven. (3) is the bigger piece.

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
