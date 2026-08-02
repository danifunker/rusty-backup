# Rusty Backup Regression Suite

A full-surface, cross-platform regression harness for `rb-cli`. It exists to
answer one question on a monthly cadence:

> Does every filesystem, container format, partition scheme, conversion path
> and CLI verb still do the right thing on Windows, Linux and macOS — and does
> the output still work on the real emulator or the real machine?

This is deliberately **not** the `cargo test` suite. `cargo test` is the
per-commit gate: fast, hermetic, synthetic fixtures committed to the repo.
This suite is the periodic gate: slow, fixture-heavy, cross-platform, and it
**reports** rather than fails.

---

## The prime directive: report, never abort

A regression run must always reach the end. A broken case is data, not a
crash. Every case resolves to exactly one verdict:

| Verdict          | Meaning                                                        |
|------------------|----------------------------------------------------------------|
| `pass`           | All assertions held.                                            |
| `fail`           | An assertion did not hold. Repro bundle captured.               |
| `skip-fixture`   | The case's fixture is not in the catalog yet. Logged as a gap.  |
| `skip-platform`  | Case is not applicable to this OS.                              |
| `skip-tool`      | A required external oracle (e.g. `chdman`) is absent.           |
| `skip-hardware`  | Hardware case, and hardware execution was not enabled.          |
| `error`          | The harness itself failed (timeout, panic, unreadable manifest).|

`skip-fixture` is a first-class outcome, not a failure. The corpus is expected
to be incomplete for a long time; the run tells us exactly what is missing so
the gaps can be filled deliberately.

---

## Layout

```
regression-tests/
  README.md          <- you are here
  PLAN.md            <- master phased tracker; the source of truth for progress
  COVERAGE.md        <- the enumerated surface + tier assignment + status
  FIXTURES.md        <- fixture policy, ID scheme, NAS catalog format
  EMULATORS.md       <- tiered emulator / on-hardware verification design
  HARDWARE.md        <- physical backup/restore design + safety interlocks
  REPORTING.md       <- run bundle format and triage workflow
  runner/            <- the harness itself (standalone Rust crate)
  cases/             <- declarative TOML case manifests, grouped by tier
  scripts/           <- fixture inventory + corpus sync helpers
```

Nothing under `regression-tests/` is wired into the main `Cargo.toml`. The
runner is a separate crate so it can be built for a test host without
building the GUI, and so it can later ride the mrustc 1.73 path to the
vintage and PPC targets.

---

## Tier model

Cases are grouped into tiers by what they need. Lower tiers run everywhere
with no setup; higher tiers need progressively more.

| Tier | Name                  | Needs                        | Notes |
|:----:|-----------------------|------------------------------|-------|
| 0    | Harness self-test     | nothing                      | `rb-cli` runs, exit codes and JSON envelope are the shape we expect. |
| 1    | Self-consistency smoke| nothing                      | `rb-cli new` builds an image, `rb-cli` reads it back. **Proves nothing about format correctness** — see below. Catches crashes and round-trip regressions only. |
| 2    | Read + inspect        | reference corpus             | `inspect`, `show`, `ls`, `du`, `fsck` over **third-party-produced** images. This is where read correctness is actually established. |
| 3    | Mutation round-trip   | reference corpus             | `put`/`get`/`rm`/`mkdir`/`chmod`/`xattr` -> re-read -> `fsck` clean. Always on a scratch copy. Output must also clear tier 6. |
| 4    | Conversion matrix     | reference corpus             | `convert`, `repack`, `floppy convert`, `optical convert` across the container matrix. |
| 5    | Backup / restore      | reference corpus             | `backup` -> `restore` -> byte-compare, across every output format and resize mode. |
| 6    | External oracle       | third-party tools            | `fsck_msdos`, `fsck.ext4`, `chdman`, `qemu-img`, `xorriso`, `unsquashfs`, `cpmtools`, mounting on the real OS. **The only valid verification of anything we write.** |
| 7    | Emulator / hardware   | emulators, scratch media     | Does the artifact actually boot? See `EMULATORS.md` and `HARDWARE.md`. |

### Why tier 1 is not coverage

An earlier draft of this plan treated `rb-cli new` as the highest-leverage
tier, on the reasoning that a builder removes the need to host a fixture.
That reasoning is wrong, and the mistake is worth stating plainly so nobody
reintroduces it:

**If rusty-backup writes the image and rusty-backup reads it back, any bug
present on both sides cancels out.** The test goes green, the bytes are
wrong, and no emulator or real machine would touch the result. A
byte-order slip, an off-by-one in a bitmap, a misplaced superblock field —
each is invisible to a round-trip against ourselves and obvious to the
vendor's own tool.

So tier 1 keeps a narrow, honest job: catching panics, regressions and
round-trip breakage. It is a smoke test. It never counts as evidence that a
format is implemented correctly, and a case in tier 1 may never be the only
coverage for a filesystem or container.

**Correctness comes from two directions, and every format needs both:**

- **Read path** — a reference image produced by somebody else (tier 2).
- **Write path** — our output validated by somebody else's tool (tier 6) or
  loaded by a real emulator or machine (tier 7).

That makes tier 6 load-bearing rather than a nice-to-have. It is the only
thing standing behind everything rusty-backup writes.

---

## Running

```bash
cargo run --manifest-path regression-tests/runner/Cargo.toml -- --help
```

Typical monthly run (see `PLAN.md` for current status of each flag):

```bash
rb-regress run --tiers 0-6 --report-root //NAS/share/rb-fixtures/runs
```

Hardware and emulator tiers are opt-in and never run by default:

```bash
rb-regress run --tiers 7 --allow-hardware --device-allowlist ./local-devices.toml
```

---

## Non-goals

- **Not a per-commit gate.** Monthly, or before a release.
- **Not a replacement for `cargo test`.** Unit and integration tests stay
  where they are; this suite exercises the shipped binary.
- **Not a fixture host.** Fixtures live on the NAS. Neither the fixture
  files nor their resolved paths are committed. See `FIXTURES.md`.
