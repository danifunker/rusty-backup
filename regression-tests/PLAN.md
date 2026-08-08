# Regression Suite — Master Plan

Source of truth for regression-suite progress. Update the status marks here
in the same commit as the work. Everything else in `regression-tests/` is
detail hung off one of these phases.

Status marks: `[ ]` not started · `[~]` in progress · `[x]` done · `[!]` blocked

---

## Decision log

Settled 2026-08-01, at the outset:

| Decision | Choice | Why |
|----------|--------|-----|
| Runner implementation | Standalone **Rust crate** (`regression-tests/runner`) | One codebase for all platforms, single static binary with no runtime deps on the test host, and it can ride the existing mrustc 1.73 path to the vintage / PPC targets later. Rejected: Python (absent on 10.7 / PPC), per-platform shell scripts (two codebases that drift). |
| Case definition | Declarative **TOML manifests** under `cases/`, referencing fixtures by **logical ID** | Keeps the matrix as data, not code. Lets the repo describe thousands of cases without committing a single fixture path. |
| Emulator verification | **Tiered** — automate what is scriptable, generate a manual checklist for the rest | MiSTer (SSH) and headless QEMU/MAME can assert automatically; WinUAE / Basilisk II / 86Box cannot, so they get a per-run checklist with screenshot slots. |
| Hardware backup/restore | **Design now, execute later** | Cases and safety interlocks get authored and committed; execution stays gated behind `--allow-hardware` plus a gitignored device allowlist until scratch media is set aside. |
| Bug reporting | **Report bundle on the NAS**, no automatic GitHub issues | A first full run may surface hundreds of failures. Triage from the summary, promote to issues by hand. |
| Fixture hosting | A working copy on local disk (`fixture_root`), synced once from a distribution source (`corpus_source`) | Both live in gitignored `local.toml`. Fixtures and their paths never enter git; see FIXTURES.md § Local first. |

---

## Phase 0 — Design and scaffolding

- [x] Confirm reachability of fixture sources (NAS `Software`, `ConsoleGames`, `C:\Temp`, MiSTer)
- [x] Enumerate the true test surface from code, not from memory (see `COVERAGE.md`)
- [x] Settle the six design decisions above
- [x] Write `README.md`, `PLAN.md`
- [x] Write `COVERAGE.md`, `FIXTURES.md`, `EMULATORS.md`, `HARDWARE.md`, `REPORTING.md`
- [x] `.gitignore` entries for fixture paths, local host config, run bundles

**Note on MiSTer: access resolved 2026-08-02.** The board answers at
`mister.local` and accepts a dedicated key as `root`. The earlier
"Permission denied (publickey,password,keyboard-interactive)" was not a
missing key — `~/.ssh/config` sets `IdentitiesOnly yes` under `Host *`, so
with no `IdentityFile` named for the board ssh offered nothing at all. Naming
the identity explicitly connects first try.

Host, user and identity file now live in the gitignored `local.toml`
(template committed as `local.toml.example`). No password channel needed, and
Phase 9 is unblocked.

---

## Phase 1 — Runner MVP (tiers 0 and 1)

- [x] Crate skeleton, `--help`, arg parsing (`run` / `list` / `validate`)
- [x] TOML case schema + loader with clear diagnostics on a malformed manifest
- [x] Process driver: run `rb-cli`, capture stdout/stderr/exit code, hard timeout
- [x] JSON envelope parser keyed to `schema_version` (`src/cli/output.rs`)
- [x] Assertion vocabulary — `exit`, `envelope`, `json_equals`, `json_exists`,
      `stdout_contains`, `stdout_matches`, `stderr_empty`, `files_exist`,
      `files_identical`, `file_sha256`, `fsck_clean`, `timeout`
- [x] Verdict model + the seven outcomes from `README.md`
- [x] Scratch-directory management; failing cases keep their scratch for triage
- [x] Report bundle writer (`REPORTING.md` format), appended as the run proceeds
- [x] Tier 0 cases: `--version`, `--help` across verbs, exit-code contract, envelope shape
- [x] Tier 1 cases: `new volume` across all 14 builders + size sweeps, each
      followed by `inspect --format json` and `fsck --checkonly`
- [ ] Comparison against the previous run (`compare/vs-previous.md`)
- [ ] Tier 1 coverage for the remaining builders (`new floppy`, `new hd`,
      `new hd sgi-efs`, `new hd x68k`, `optical new sgi-efs`)

**Status:** first run executed on Windows 2026-08-01 — **50 cases, 38 pass,
12 fail, 0 skip**, with zero fixtures present. The twelve failures are six
distinct findings (R-003 through R-008), all written up in `COVERAGE.md`.

The exit criterion was "a green tier-0+1 run". It is not green, and that is
the correct outcome: the suite found real bugs on its first execution,
including a panic. Phase 12's triage loop starts here rather than after the
full matrix is authored.

Deps ended up as `serde`, `serde_json`, `toml`, `regex`, `sha2` — `regex`
backs `stdout_matches` and `sha2` backs `file_sha256`. Crate is pinned to
`rust-version = "1.73"` to keep the Phase 11 vintage/PPC path open.

---

## Phase 2 — Fixture corpus and catalog

- [x] Inventory scanner over all four sources, matching `DISK_IMAGE_EXTS`
      (`scripts/inventory-fixtures.ps1`)
- [x] Scan `C:\Temp` — 710 candidates / 217 GiB across 26 extensions
- [x] Scan `\\NAS\share` — 5,693 candidates / 1,745 GiB across
      36 extensions (2,739 `.d88`, 1,570 `.iso`, 269 `.chd`, 259 `.d64`,
      plus `.woz` `.hfe` `.scp` `.hfv` `.gho` `.mdx` `.toast`)
- [x] Scan `\\NAS\games` — 21,793 candidates / 4,701 GiB.
      Overwhelmingly console media (6,214 `.chd` = 2,525 GiB; 1,149 `.iso` =
      1,932 GiB) with no fixture value, but it does carry gap-fillers:
      **155 `.adf` + 11 `.hdf`** (the Amiga gap), 140 `.cdi`, 12 `.scp`,
      15 `.imd`, 103 `.mdx`, 6 `.woz`
- [x] Scan MiSTer `/media/fat/games` — access resolved (see Phase 0 note).
      Richest single source for core-authoritative media: **2,738 `.d88`**
      (X68000 / PC-98 floppies), 259 `.d64`, 240 `.chd`, 74 `.dsk`,
      60 `.vhd`, 60 `.g64`, 55 `.hda`, 44 `.img`, 27 `.hdf`, 19 `.adf`.
      These are the files the real cores actually load, which makes them the
      best oracles we have for tier 7

**Scan totals: 28,196 candidates / ~6.7 TB across the three reachable
sources.** Which settles the question of approach — the corpus has to be
*derived* from what the matrix needs, never curated from what exists.
- [ ] Triage scan output: candidate -> logical ID, or reject with reason
- [x] Define and freeze the logical-ID scheme (`FIXTURES.md`)
- [x] Derive the **minimum corpus** — what can be synthesized, what the repo
      already carries, and what genuinely has to be hosted (`FIXTURES.md`
      § Minimum corpus)
- [x] Build `fixture-map.repo.tsv` — 45 IDs onto the 3.3 MB of fixtures
      already committed under `tests/fixtures/`, resolved via a `repo:`
      prefix, working with **no NAS and no network**
- [x] Runner materialises `.zst` fixtures in process into a per-run cache
      (no external `zstd` binary; decompressed once per run, not per case)
- [x] Consolidate the corpus into `rb-fixtures/fixtures/` — **53 fixtures,
      9.7 MB**, all verified to open before admission
- [x] Build `fixture-map.tsv` on the NAS (53 rows, sha256 per file)
- [x] Publish the policy and gap report alongside it
      (`README-fixture-policy.md`, `missing-fixtures.md`, `scans/`)
- [x] Verify the runner resolves against the NAS corpus end to end
- [ ] Close the remaining gaps — see `GAPS.md` (6 filesystems, ~13
      containers, 8 optical; GPT/APM/Sun unverified)
- [ ] Minimisation pass, once optical fixtures start arriving
- [x] Record the gap list — `missing-fixtures.md` per run, plus the standing
      table in `FIXTURES.md`

**Corpus position.** The inventory found ~6,400 candidates / ~2 TB. Almost
none of it should be admitted:

- **45 IDs come free with the clone** (`tests/fixtures/`, 3.3 MB), all
  produced by third-party formatters — audited, none built by rb-cli.
- **~26 filesystems and ~18 containers we can *write*** via `new volume` /
  `new floppy` / `new hd` / `squashfs create` / `convert`.

  **Superseded 2026-08-02.** An earlier revision counted that second bullet
  as "needs no fixture at all". That was wrong: being able to write a format
  is not evidence of writing it correctly, and a round-trip against our own
  reader hides any bug present on both sides. Each of those 26 + 18 now needs
  a reference fixture for the read path *and* a tier-6 oracle for the write
  path. See `README.md` § "Why tier 1 is not coverage" and
  `VERIFICATION-MATRIX.md`.

  Net effect: the corpus requirement went **up**. The offset is that most of
  the oracle half hosts no bytes.

**Exit criterion:** unresolved IDs degrade to `skip-fixture`, never to an
error — met. Tier 2 runs today on a bare clone.

---

## Phase 3 — Tier 2, read and inspect

- [~] Read cases for the no-builder filesystems, from repo fixtures
      (`cases/tier2/read-repo-fixtures.toml`, 19 cases) — surfaced R-009/R-010
- [ ] One read case per filesystem in the `COVERAGE.md` filesystem table (42)
- [ ] One read case per container format (~48)
- [ ] One case per optical filesystem (12)
- [ ] One case per partition scheme (8, including superfloppy)
- [ ] `inspect` / `show partmap` / `show fs-info` / `ls` / `du` / `fsck` per applicable target
- [ ] `--format json|yaml|csv|tsv` shape checks, including the documented CSV/TSV rejection on nested verbs

---

## Phase 4 — Tier 3, mutation round-trips

- [x] `put` -> `ls` -> `get` -> byte-compare, per editable filesystem
- [x] `mkdir` / `rm` / `setvolname` / `chmod` / `chown` / `chmeta` / `xattr` / `setrsrc`
- [x] Mac-specific: resource forks, type/creator, BinHex, MacBinary
- [~] Amiga-specific: protection bits done; **file comments have no CLI surface
      at all**, so the case cannot be written — see GAPS.md § Open
- [x] `fsck` must come back clean after every mutation
- [ ] Free-space and duplicate-name preflight behaviour
- [x] Negative cases: writing to a read-only filesystem must exit non-zero with
      the right code — R-034, red until fixed

---

## Phase 5 — Tier 4, conversion matrix

Tier 4 exists now (`cases/tier4/`) and holds three cases. Everything below is
still the plan. Note `produce` builds 53 artifacts across these formats, so
the *writers* are exercised even where a conversion case is not.

- [~] Container N x M conversion via `convert` / `repack` — 3 of N
- [ ] `floppy convert` across the floppy container family
- [ ] `optical convert` across optical containers
- [x] Round-trip identity assertion where the conversion is lossless
- [ ] Documented-lossy conversions assert the expected loss, not identity —
      blocked on enumerating the losses per format pair; formats.toml does not
      record them
- [x] `expand` / `shrink` / `resize` / `grow`, including `--to-hfv` and the 2047 MB HFV ceiling

---

## Phase 6 — Tier 5, backup and restore

Tier 5 exists now (`cases/tier5/`). The round-trip and container-source cases
were authored into tiers 3 and 4 and were re-tiered here on 2026-08-08; case
IDs were left alone, because renaming one is a breaking change to every
manifest that references it.

- [~] `backup` in every output format (raw, zstd, VHD fixed/dynamic, single-file CHD)
- [ ] Both backup layouts: per-partition and `layout: "single-file-chd"`
- [ ] `restore` at original / minimum / custom sizes
- [ ] Alignment preservation across all four alignment modes
- [ ] Checksum verification, both CRC32 and SHA256
- [ ] `--defrag`, compact-space, and smart-sizing paths
- [ ] Round-trip byte-compare of restored image against source

---

## Phase 7 — Tier 6, external oracles

**Promoted: this is now load-bearing, not a cross-check.** Per the
provenance rule, an oracle is the only valid verification of anything
rusty-backup writes. Full plan in `VERIFICATION-MATRIX.md`.

- [x] Build the verification matrix — per-format oracle, per-host availability
- [x] Audit oracle availability on Windows / WSL / MiSTer (macOS still unaudited)
- [x] Prove the pattern: `qemu-img` accepts all six containers we write
- [ ] Install `chdman` everywhere — CHD has **no oracle on any host** today
- [ ] Stand up one full Linux box with `linux-modules-extra` (unlocks
      hfs/hfsplus/affs/minix/jfs/ntfs3/ufs mounts)
- [ ] `apt install cpmtools` — closes the nine-DPB CP/M sub-axis
- [ ] Wire `ghostexp.exe` as the GHO oracle (present on this box, Ghost 11.5)
- [ ] Pick an Apple-floppy oracle (AppleCommander is cross-platform)
- [ ] Tool-presence probing, degrading to `skip-tool`
- [ ] `fsck_msdos` / `fsck_exfat` / `fsck.ext4` / `xfs_repair` cross-checks
- [ ] `chdman info` / `qemu-img check` / `7z t` / `unsquashfs -s` / `xorriso` / `cpmtools`
- [ ] Record oracle disagreements as findings even when our own fsck is clean

---

## Phase 8 — Cross-platform

- [ ] Linux host run
- [ ] macOS host run
- [ ] Windows host run (primary development host)
- [ ] Per-platform expected-difference table — what is legitimately allowed to differ
- [ ] Raw-device and permission paths that only exist on one OS

---

## Phase 9 — Tier 7, emulator verification

- [x] MiSTer SSH access working (key-based, config in `local.toml`)
- [ ] MiSTer over SSH: deploy, launch core, capture result (automated)
- [ ] Headless QEMU with serial-log assertion (automated)
- [ ] MAME / chdman-backed cores (automated where a scriptable exit exists)
- [ ] Generated manual checklists for WinUAE, Basilisk II, SheepShaver, 86Box, PCem, Arculator, and friends
- [ ] Screenshot capture slots and a result-ingest path back into the run bundle

See `EMULATORS.md`.

---

## Phase 10 — Hardware backup and restore

- [ ] Device allowlist format and the refuse-unless-listed interlock
- [ ] Backup from physical device -> restore to the same device -> verify
- [ ] Cross-media restore (smaller and larger target)
- [ ] Bad-sector handling
- [ ] Elevation paths per OS
- [ ] **Execution deferred** until dedicated scratch media exists

See `HARDWARE.md`.

---

## Phase 11 — Vintage and PPC targets

- [ ] Confirm the runner builds under the Rust 1.73 floor
- [ ] `rb-cli-vintage` (macOS 10.7 / Windows 7) run
- [ ] `rb-cli-ppc` run
- [ ] Reduced tier set appropriate to those hosts

---

## Phase 12 — The triage loop

- [ ] First full regression run
- [ ] Triage the bundle; every failure becomes a tracked item
- [ ] Fix, re-run, repeat until the matrix is green or every red cell has a
      documented, accepted reason
- [ ] Fold the accepted-reason list into `COVERAGE.md` so a future run does
      not re-litigate known limitations
