# rb-cli HFS catalog corruption — RESOLVED (stale `$PATH` binary, NOT HEAD)

> ## RETRACTION (2026-06-28) — read this first
>
> The original headline of this doc — *"current rb-cli HEAD 1b22518 GENERATES the
> corruption"* — is **WRONG**. The corruption was produced by a **stale, pre-fix
> rb-cli on `$PATH`**, never by 1b22518. HEAD's catalog fix is correct and is now
> validated end-to-end on the real build.
>
> **Root cause of the mix-up.** MacAtrium's `RbCli` resolved the binary from
> `$PATH` whenever `~/.macatrium.json` set `rb_cli` to the bare name `"rb-cli"`
> (it did) — and that bare name **silently overrode** the build config's absolute
> `rb_cli` path. `$PATH`'s `rb-cli` was `/home/dani/.local/bin/rb-cli`, built
> 2026-06-26 *before* the catalog fixes (0 `btree_try_rotate_leaf` symbols; HEAD
> has 4). So every "recipe A" run — and the `macatrium-755-full.hda` output cited
> below — came from the 06-26 binary, not 1b22518. The `RBCLI_ARGV_LOG` even
> recorded `self.bin` as the bare `"rb-cli"`, which confirms it.
>
> **Proof — same build, identical inputs, only the binary differs:**
>
> | rb-cli | result |
> |---|---|
> | `/home/dani/.local/bin/rb-cli` (06-26, pre-fix) | dies at 13,474 files — 415 `IndexSiblingLinkBroken` |
> | HEAD 1b22518 (forced first on `$PATH`) | **completes: 16,680 files, fsck 0 errors** — `/home/dani/repro-HEAD.hda` |
>
> **Resolution.** (1) `/home/dani/.local/bin/rb-cli` was replaced with the HEAD
> build (old one at `…/rb-cli.06-26.bak`). (2) MacAtrium was hardened so this can't
> recur: the per-build `cfg.rb_cli` (absolute) now wins over a bare
> `settings.rb_cli`, and every build logs the *resolved* rb-cli path + `--version`
> up front (both report `rb-cli 0.1.0`, so the **path** is the disambiguator).
>
> **One nit corrected below:** rb-cli writes each fork as a single contiguous
> extent or fails — it never produces multi-extent / extents-overflow records, so
> "12 MB forks → multi-extent" was never a factor.
>
> The reproduction recipe itself was sound — running the *real* build on the
> forced-HEAD binary is exactly what exposed the stale-`$PATH` issue. Everything
> below is the ORIGINAL (now-retracted) analysis, kept for history; read "GENERATED
> by HEAD" as "regenerated each run by the stale 06-26 binary."

---

## Original analysis (retracted headline — see RETRACTION above)

Reproduction recipe + all evidence for the `IndexSiblingLinkBroken` /
"no free B-tree nodes" corruption that kills MacAtrium `atrium image` builds.
Supersedes the diagnosis in `PROMPT-hfs-catalog-incremental-put-packing.md` with
the key new fact below.

## TL;DR — the headline finding

**The starting template is CLEAN; current rb-cli (HEAD 1b22518) GENERATES the
corruption during the build.** Verified just now:

| disk | `rb-cli fsck …@1` | what it is |
|---|---|---|
| `/home/dani/MacOS_SampleDisks/MacLC_7-5-5.hda` (the build's base template) | **265 files / 52 dirs, 0 errors — CLEAN** | pristine System 7.5.5 OS install |
| `/home/dani/macatrium-755-full.hda` (the build output) | 13,474 / 2,231, **415 IndexSiblingLinkBroken** | the corrupt build output |

So it is **not** inherited from a poisoned disk. A clean 265-record base becomes a
17,934-record, 415-error catalog purely through the build's per-file injection.

**Why your `put-macbinary`/`new`-disk replays stayed clean — the untested variable:**
the build does **not** inject onto the small template, nor onto a fresh `new` disk.
It does `cp template → out`, then **`rb-cli expand` the 73 MB disk to 2000 MB (a
catalog *clone*)**, and injects onto **that expand-clone**. Inject via
**`put-binhex --clear-inited`** (NOT `put-macbinary --force`). The combination
*(expand-clone base + `put-binhex --clear-inited` + real resource forks + ~18k
records + skip-on-collision)* is what corrupts; each factor alone is clean.

## The corruption (your earlier analysis, confirmed)

`fsck` on the output: leaves are densely/perfectly packed (~3.95 records/leaf), but
the **index** is bloated — ~15,930 index nodes for ~4,544 leaves, ~32–64 duplicate
separators per leaf, `IndexSiblingLinkBroken` at the **lowest index nodes (8, 19,
20, 32…) with zeroed fLink/bLink**. 17,934 records use ~20,326/20,480 catalog nodes
(1.14 nodes/rec) while **776 MB of data space is still free** — pure catalog
exhaustion from orphaned/duplicate index separators ("freed leaf nodes whose
separators were never removed, index reused → stale separators pile up").

## Exact build flow + the rb-cli commands it issues

From `tools/atrium-tool/src/{image.rs,rbcli.rs,harvest.rs}`:

1. `cp --sparse=always <template> <out>` (host cp; not rb-cli).
2. **`rb-cli expand <out> --size 2000M --output <tmp> -q`** ← apply_disk_size; the
   catalog is **cloned** into the 2 GB volume. (Disk: APM, one Apple_HFS partition.)
3. Harvest, per app file (×~13k):
   - `rb-cli get-binhex -q <donor-img> <src> <hqx>` (read from a *donor* disk → host `.hqx`)
   - `rb-cli mkdir <out> <component> -q` for each path component (mkdir_p; **errors
     ignored** — `let _ = run(...)`), creating `/MacAtrium/Apps/<App Name>/…`
   - **`rb-cli put-binhex <out> <hqx> --dst-dir "<.../App Name>" --clear-inited -q`**
     — both forks + Finder info, then clears `hasBeenInited`. **No `--force`.**
   - On a `put-binhex` error (e.g. two files in one app dir decode to the same
     31-char leaf name — real: `THE ANCIENT ART OF WAR/F` ×2), **harvest SKIPS that
     file and continues** (the `warning: … skipped` lines). So the stream contains
     create-attempts that fail-and-skip.
4. Art bake, per installed title (×few hundred):
   - `rb-cli put <out> <hostfile> /MacAtrium/images/<id>.<variant> --type PICT
     --creator ttxt --force -q` (data-fork PICTs/raws)
   - the build **dies** on one of these (`battletanks-2-12.shot.24.pict`) with
     `create_file: disk full: no free B-tree nodes` — but the catalog is **already**
     corrupt by then (from step 3).

Command-log audit of all 40,331 calls: **0 `rm`/`delete`, 0 `--force` repeats to the
same dest, 0 duplicate dest paths.** Verb counts: 13,193 `get-binhex`, **13,147
`put-binhex --clear-inited`**, 7,997 `mkdir`, 5,875 `ls`, 118 `put --force`, 1 `expand`.

## Data shape (matters)

- **Resource forks: real and large** — staged `.hqx` up to **12 MB**, many > 200 KB.
  *(My synthetic repros used data-only files.)* **CORRECTION:** these do **not**
  become multi-extent / extents-overflow records — rb-cli writes each fork as a
  single contiguous extent or fails, so fork size was never the trigger.
- Structure: **bushy, ~6 files per app dir**, ~2,230 dirs, ~13,474 files, ~18k
  records, on a **2000 MB** (32 KB-block) volume.
- Names: app file names are real Mac names (≤31 bytes; some carry `0x7F` DEL bytes);
  **intra-app duplicate leaf names occur and are skipped** (see step 3).
- (Aside, not the bug: 283 art `<id>.shot.24.pict` names exceed 31 chars → rb-cli
  correctly rejects them, art silently skipped.)

## Reproduction

### A. Confirmed, deterministic — run the build (uses existing binaries; do NOT rebuild)

```sh
cat > /tmp/755-full.json <<'JSON'
{ "base_os": "7.5.5", "out": "/home/dani/macatrium-755-full.hda",
  "selection": { "mode": "all" }, "art_depths": ["1","8","24"],
  "app_mem_kb": [3584,3072], "max_art_size": "448x448", "disk_size_mb": 2000,
  "mg_archive": "/home/dani/macgarden-archive",
  "rb_cli": "/home/dani/repos/rusty-backup/target/release/rb-cli" }
JSON
# existing atrium binary (Jun 28); rb_cli points at your HEAD build:
/home/dani/repos/MacAtrium/tools/atrium-tool/target/release/atrium image --config /tmp/755-full.json
# -> fails at ~17,934 records: "no free B-tree nodes"
/home/dani/repos/rusty-backup/target/release/rb-cli fsck /home/dani/macatrium-755-full.hda@1   # -> 415 IndexSiblingLinkBroken
```
To capture the exact command stream, set `RBCLI_ARGV_LOG=/path/log` in the env —
the build's `RbCli::run` appends every argv (a debug hook already in
MacAtrium's `tools/atrium-tool/src/rbcli.rs`). The `.hqx` it references live under
`/tmp/atrium-image-stage/**` during the run.

### B. Isolated rb-cli (to confirm it's rb-cli, not atrium) — inject onto an EXPAND-CLONE

This is the key recipe — replay onto an **expanded clone of a clean base**, not a
fresh `new` disk:

```sh
RB=/home/dani/repos/rusty-backup/target/release/rb-cli
# clean 73 MB base (verified fsck-clean) — already on disk:
$RB expand /home/dani/atrium-clean-template.hda --size 2000M --output /home/dani/work.hda -q
$RB fsck /home/dani/work.hda@1     # expect CLEAN (confirms expand of a clean disk is clean)
# then inject ~13k files with put-binhex --clear-inited into bushy per-app dirs,
# files having REAL resource forks (mix sizes incl. >200 KB), SKIPPING put errors,
# fsck every ~2000 files -> watch IndexSiblingLinkBroken appear.
```

To synthesize `.hqx` with resource forks (since the staged ones are session-local):
`$RB new --fs hfs --size 80M src.hfv`; for each file: `put` a data fork + `setrsrc`
a resource fork of varied size (some > 200 KB) + `chmeta`, then `get-binhex` it →
`.hqx`. Group ~6 per app dir, include a few intra-dir name dups (skip on error).
*(Note: my synthetic attempts up to 2,500 files stayed clean — the trigger needs
the full combo: expand-clone base + put-binhex + large real forks + ~18k scale +
skip-on-collision. Start from the expand-clone and push to ~18k.)*

## What's RULED OUT (all clean on HEAD 1b22518)

- plain `put` — flat ascending, shuffled, multi-parent round-robin (≤20k): clean
- `put` + `setrsrc` (resource forks): clean
- `put` into an `expand`ed base (plain put): clean
- `put-binhex --clear-inited`, single-dir AND bushy, ≤2,500 files: clean
- `put-macbinary --force` (your earlier repro): clean — **wrong verb/flag**

So no single factor reproduces; the **build** (full combo) does, deterministically.

## Preserved artifacts (on `/home/dani`, off the space-constrained `/tmp`)

- `/home/dani/atrium-clean-template.hda` — fsck-CLEAN 73 MB System 7.5.5 base (APM,
  `@1` = Apple_HFS 70 MiB). Replay onto an `expand` of this.
- `/home/dani/macatrium-755-full.hda` — the corrupt output (415 errors) to dissect.
- `/home/dani/macatrium-build-rbcli-argv.log` — the full 40,331-command sequence
  (mind: Mac dst-dirs contain spaces; the `.hqx` paths it references were under
  `/tmp/atrium-image-stage/` and have since been cleaned — re-run recipe A to
  regenerate them, or synthesize per recipe B).

## rb-cli under test
`/home/dani/repos/rusty-backup/target/release/rb-cli` — `rb-cli 0.1.0`, built from
HEAD **1b22518**. **CORRECTION (see RETRACTION at top):** the corrupt output and the
deterministic build failure were produced by the **stale 06-26 binary on `$PATH`**
(`/home/dani/.local/bin/rb-cli`), which silently shadowed this configured path — NOT
by 1b22518. Forcing HEAD first on `$PATH` completes fsck-clean (16,680 files, 0
errors). The bug is **fixed** in 1b22518.
