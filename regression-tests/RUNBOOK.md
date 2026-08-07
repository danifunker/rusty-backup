# Runbook — how to run a regression

Accurate as of commit `b87d688`. Everything below has been executed; nothing
here is aspirational. Where a thing does not exist yet it says so plainly,
because a runbook that describes unimplemented features is worse than no
runbook.

---

## What exists today, in one line

`rb-regress run` executes **87 cases across tiers 0-2 on one machine**. That
is rb-cli checked against itself and against reference fixtures.

`rb-regress produce` builds **35 artifacts** and `rb-regress parity` compares
them across producer OSes. Neither needs an oracle. **`verify` does not exist
yet** — no third-party tool is invoked by anything. See § The run/verify split.

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

Expect roughly: **87 cases, ~69 pass, ~18 fail**, under a minute. Every
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
rb-regress produce              # build every artifact rb-cli can, into artifacts/<os>
rb-regress parity               # compare those artifacts across producer OSes
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

**`produce` and `parity` exist. `verify` does not — no third-party tool is
invoked by anything yet.**

`run` executes cases. A case is a sequence of rb-cli invocations with
assertions on exit codes, JSON envelope fields, and produced files. All 87
current cases are tiers 0-2:

- **tier 0** — rb-cli runs, exit codes and envelope shape hold
- **tier 1** — rb-cli builds a volume and reads it back. A smoke test only;
  per `README.md`, this proves nothing about format correctness because a bug
  on both sides cancels out
- **tier 2** — read third-party reference fixtures

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
rb-regress verify --artifacts regressions/2026-08/artifacts --out regressions/2026-08/verifications/$OS
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

### What exists now

`produce` and `parity` shipped 2026-08-07.

```bash
rb-regress produce --rb-cli ../../target/release/rb-cli   # -> artifacts/<os>/
rb-regress parity                                          # compares artifacts/*/
```

`produce` walks `data/produce.toml` — the runnable argv behind each format's
`builder` in `formats.toml` — and writes `artifacts/<os>/<format-id>/` holding
the image and a self-contained `meta.json` (argv, sha256, producer OS + host,
rb-cli build label, git sha, volatile ranges). Any `builder` with no recipe is
listed at the end of the run, so an absent format is never mistaken for a
covered one. **35 of 55 builders have recipes**; `produce` names the other 20
every time it runs.

The recipe file also carries the two shapes that needed more than an argv: a
`pre` step, for the container formats that convert an image rather than format
a volume and so need a source first, and `produces`, for `convert` — which
takes a destination *folder* and names the output after its input.

`parity` compares every unordered pair of producer OSes for each format, so
three OSes give three comparisons and a single odd one out stays visible
rather than being averaged away. It exits 1 on any divergence.

`verify` and read-parity are still missing. `verify` needs no remote execution
to start: a host can verify artifacts it produced itself, and artifact trees
sync by any means, including the NAS. What it lacks is a runnable check per
(oracle, format) pair — `oracles.toml` records `evidence` strings like
`qemu-img info -> raw`, which describe a check rather than being one, so that
file needs the same treatment `produce.toml` just gave `formats.toml`.

### Volatile-range discovery is timing-sensitive, and that bites

The two produce runs are **two passes over the whole recipe set at least three
seconds apart**, not two back-to-back runs of one recipe. Measured 2026-08-07
on Windows:

| pairing | HFS | HFS+ | ext2 | ProDOS | NTFS |
|---------|----:|-----:|-----:|-------:|-----:|
| back-to-back | 0 | 0 | 4 | 0 | 0 |
| 3 seconds apart | 6 | 10 | 13 | **0** | 0 |
| 65 seconds apart | 6 | 10 | 13 | **1** | 0 |

A fast builder writes both copies inside one clock tick, the embedded
timestamp does not move, and the range is never discovered. That does not fail
the run — it makes `parity` report a false divergence on the *next* host,
which is worse.

ProDOS is why the gap is now **65 seconds** rather than 3. It stamps the
volume directory header to the *minute*, so a 3-second pair never crosses a
boundary and the field reads as deterministic. The first three-OS parity run
duly reported `fs.prodos` differing by one byte between Windows and the other
two — byte 0x41E, minute 45 against minute 46. A false finding, and the
expensive kind. 65 seconds guarantees a minute boundary; finding one byte of
the field is enough, because the adjacency rule below then covers the hour and
date bytes beside it.

Discovery still only finds a **lower bound**. Two samples a minute apart move
the low bytes of a time field and not the high ones, so hosts producing days
apart can differ in bytes this never saw vary. `parity` handles that by
reporting a divergence within 8 bytes of a known volatile range as *adjacent*
and printing the count, rather than either calling it a finding or silently
widening the mask. A field coarser than a minute — an hour-granularity stamp —
is out of reach of this method entirely and must be declared.

### Declaring a difference that discovery can never find

Some differences are stable on any one host and vary only by *which* host
built the artifact, so no amount of local sampling finds them. VHD's Creator
Host OS is the case in hand: macOS writes `"Mac "`, Windows and Linux write
`"Wi2k"` (R-019). A recipe declares those:

```toml
[[recipe.expect_divergence]]
from_end = 476     # footer is the last 512 bytes; +0x24 is Creator Host OS
len = 4
reason = "..."
```

`at` gives an absolute offset, `from_end` an offset back from EOF — footers
are anchored to the end, so an absolute offset silently points at the wrong
bytes the moment a recipe's `--size` changes. Ranges resolve against the real
artifact at produce time and land in `meta.json`, so `parity` never needs the
recipe file.

Keep them narrow. The exemption covers the 4 bytes, not the footer and not the
format; a byte one past the range is still a finding; and `parity` prints the
reason on every match that used one. An unexplained exemption is just a
quieter blind spot.

---

## Known limitations, so nothing surprises you

- **Tiers 3-7 do not exist.** `--tiers 0-6` selects the same 87 cases as
  `--tiers 0-2`.
- **`verify` does not exist.** `produce` and `parity` do; nothing runs an
  oracle.
- **20 builders have no produce recipe** and `fmt.vmdk-flat` cannot get one
  yet — monolithicFlat is a descriptor plus a sibling `-flat.vmdk` extent, and
  comparing the 301-byte descriptor alone would read as coverage. Multi-file
  artifacts need supporting first.
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
