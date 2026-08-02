# Run Reports and Triage

## Where a run lands

```
\\NAS\share\rb-fixtures\runs\<utc>-<host>-<platform>\
```

`<utc>` is `YYYYMMDD-HHMMSS`, `<host>` the machine name, `<platform>` one of
`windows`, `linux`, `macos`, `vintage-win7`, `vintage-macos107`, `ppc`,
`mister`. A monthly regression produces one directory per platform; they are
siblings and get compared after the fact.

## Bundle contents

```
<run>/
  summary.md              human-readable top sheet; read this first
  results.jsonl           one JSON object per case, the machine record
  env.json                host + toolchain + rb-cli version + git sha
  missing-fixtures.md     the shopping list (see FIXTURES.md)
  oracle-skips.md         cases skipped because a third-party tool was absent
  checklists/             generated manual verification checklists (tier 7)
  failures/
    <case-id>/
      cmd.txt             the exact argv, one arg per line, copy-pasteable
      exit.txt            observed exit code and the expected one
      stdout.txt
      stderr.txt
      assertions.json     which assertion failed, expected vs observed
      fixture.txt         fixture ID, sha256, and resolved path on this host
      artifacts/          what the case produced, capped (see below)
  compare/
    vs-previous.md        deltas against the previous run on this platform
```

## The rule about failing cases

A failing case captures **everything needed to reproduce it without the
harness**. The `cmd.txt` must be runnable as-is. If a reader has to
reconstruct the invocation from the manifest, the capture is incomplete.

## Artifact capture policy

Failure artifacts are the expensive part. The policy:

- Capture artifacts up to a per-case cap (default 32 MiB), zstd-compressed.
- Over the cap, capture the first and last 1 MiB plus the full sha256, and
  note the truncation in `assertions.json`.
- Never capture the *input* fixture — record its ID and sha256 instead, since
  the corpus already holds it.
- Scratch directories for failing cases are preserved; passing cases clean up.

## `results.jsonl` record

One object per case, appended as the run proceeds so a killed run still
leaves usable data:

```json
{
  "case_id": "fs.fat16.read.inspect-json",
  "tier": 2,
  "verdict": "fail",
  "fixture_id": "fs.fat16.dos622.hd",
  "duration_ms": 412,
  "exit_code": 1,
  "expected_exit": 0,
  "failed_assertions": [
    {"op": "json_path", "selector": "result.partitions[0].fs_type",
     "expected": "FAT16", "observed": "FAT12"}
  ],
  "artifacts": ["artifacts/out.img.zst"],
  "platform": "windows",
  "rb_cli_version": "..."
}
```

Appending as it goes matters: the first full regression is expected to be
long and may well be interrupted.

## `summary.md`

Ordered for triage, not for completeness:

1. **Headline** — counts by verdict, wall-clock, platform, rb-cli version.
2. **New failures** — failing here, passing in the previous run on this
   platform. Highest-value section; this is the regression signal.
3. **Still failing** — known-red cells, with the run count since first seen.
4. **Newly passing** — fixed since last run. Confirms the triage loop.
5. **Failures by axis** — grouped by filesystem / format / verb, so a single
   root cause shows up as a cluster rather than forty unrelated lines.
6. **Skips** — fixture gaps, absent tools, platform-inapplicable.
7. **Slowest cases** — keeps the monthly run from quietly growing unbounded.

Section 5 is what makes a several-hundred-failure first run tractable: forty
failures that all say "exFAT" are one bug, and the summary should say so
rather than making a human notice it.

## Triage workflow

1. Read `summary.md` §2 (new failures) and §5 (clusters).
2. For each cluster, open one representative `failures/<case-id>/` and run
   `cmd.txt` by hand.
3. Confirmed bugs get promoted to GitHub issues manually, with the failure
   directory attached. The harness never files issues itself — a first run
   would open hundreds.
4. Accepted limitations get recorded in `COVERAGE.md` § Deliberate exclusions
   so the next run does not re-litigate them.
5. Fix, re-run, repeat (`PLAN.md` phase 12).

## Comparison against the previous run

The runner reads the most recent bundle for the same platform and diffs by
`case_id`. `compare/vs-previous.md` carries four lists: new failures, newly
passing, newly skipped (usually a fixture went missing), and cases that
disappeared entirely (usually a manifest edit).

If no previous run exists, the comparison is skipped and the summary says so
rather than claiming everything is new.

## In-emulator and on-hardware results

Tier 7 results arrive out of band — a human runs an emulator, or a MiSTer
reports back. Those land through a separate ingest step that appends to the
same `results.jsonl` with `"source": "manual"` or `"source": "mister"`, so
the summary covers automated and human-verified cases in one sheet. See
`EMULATORS.md`.
