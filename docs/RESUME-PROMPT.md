# Resume Prompt

Hand this to a fresh Claude session to pick up the open-work implementation
loop where the previous session left off. It encodes the project conventions,
the open-work doc layout, the prior session's progress, and the working
preferences so the new session doesn't need to re-derive them.

---

## Prompt to paste

```
You are picking up implementation work on rusty-backup at branch
`mister-parity` (the working tree should be clean — confirm with
`git status` first).

Read these two files before doing anything:
  - docs/OPEN-WORK.md — the master plan. Every open item lives here
    with enough design detail to be actionable; the only other living
    plan is docs/mister_filesystem_implementation_plan.md (treated as
    a single line item from OPEN-WORK).
  - docs/need_fixtures.md — the index of real-image fixtures we need
    but don't have locally. Any item flagged "Parked — need fixture"
    in OPEN-WORK has its requirements listed here.

Rules I want you to follow as you implement:

1. Work one item at a time. Commit between items; for multi-phase
   items (like the ReiserFS R.3a/b/c/d split), commit between phases
   too. Use the existing commit-message style — area prefix + short
   subject, then a body that explains the why and what changed. Tag
   the OPEN-WORK section you closed (e.g. "§3.1 ticked off"). Include
   the `Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>`
   trailer.

2. Park anything that needs a fixture we don't have. Update its
   OPEN-WORK row to "Parked — need fixture" and add an entry to
   docs/need_fixtures.md with:
     - the target filename(s) under tests/fixtures/
     - the minimum content the fixture must carry
     - the tool / OS needed to produce it

3. Prefer non-Linux items right now. The Linux Unix-FS track
   (ReiserFS R.3b/c/d, UFS, JFS) is fixture-blocked anyway. Pick
   from the "still open and not fixture-blocked" list below.

4. Never delegate understanding. When you stage a commit, look at
   the diff first. Don't write commit messages like "based on the
   findings, fix the bug" — include the actual file paths and what
   specifically changed.

5. Synth-only validation is fine where the math is independently
   checkable (CRCs, bitfield layouts, round-trip through our own
   parser). When the eventual correctness check needs a real image,
   ship the synth tests, list the fixture in docs/need_fixtures.md,
   and park the remaining sub-phase.

6. Pre-commit hook gotcha: `.git/hooks/pre-commit` runs
   `cargo fmt --all` followed by `git add -u`, which re-stages every
   tracked file with working-tree changes. If you have unrelated
   modifications staged-but-not-yet-committed (e.g. files you
   intended for the next commit), stash them first or they'll get
   pulled into the current commit and contaminate it. Pattern:
     git stash push -m "next phase" -- <paths>
     git commit ...
     git stash pop

7. Codebase conventions in CLAUDE.md, code-placement rules in
   CONTRIBUTING.md. Two rules that bite the most:
     - No Unicode glyphs in user-visible strings (egui's default
       font renders them as boxes). Use ASCII: `->`, `-`, `X`, `OK`,
       `Warning`, etc. Doc comments are exempt.
     - GUI/CLI feature parity: when a user-facing operation lands,
       check that both surfaces have it. The shared logic goes in a
       core module (src/fs, src/backup, src/model), not duplicated.

Status when this session paused — see the "Recently shipped" and
"Still open" sections of docs/RESUME-PROMPT.md for the per-item
breakdown.

Start by checking `git log --oneline -20` to see what landed in the
previous session, then pick the next item from the "Still open"
list (in priority order: §2.3 HFS+ journal Step 27 if you want the
biggest piece, §6.2 `rb-cli get` globbing for medium scope, or §6.3
GUI .hqx import for a design checkpoint first). Park items the user
hasn't given a fixture for and move on.
```

---

## Recently shipped (last session, 2026-06-02)

9 commits on `mister-parity` ahead of `5082f17`:

| Commit | Item | Tests added |
|---|---|---|
| `151908e` | gui: ElevationGate dead-code cleanup (prep for clippy `-D warnings`) | — |
| `b149898` | **ReiserFS R.1** detect + superblock + sizing (v3.5 / v3.6 / reject reiser4) | 9 |
| `4079385` | **ReiserFS R.2** bitmap walk + `last_data_byte` + `CompactReiserFsReader` | 6 |
| `36c4440` | **ReiserFS R.3a** on-disk S+tree node parsers (BlockHead / Key / ItemHead / DiskChild) | 13 |
| `de3f1e3` | `docs/need_fixtures.md` introduced; R.3b/c/d, UFS, JFS parked as "need fixture" | — |
| `3af6ca4` | **ProDOS §3.1** `set_prodos_access` + GUI Lock/Unlock buttons | 4 |
| `2ae1072` | **CLI §6.1** `fsck --format text|json|yaml` envelope output | 5 |
| `5b6441d` | **SIT §6.3** writer emits nested folders (`StuffItInputNode` tree API) | 5 |
| `ea73d90` | **CLAUDE.md cleanup** ASCII tree connectors + `->` symlink arrows | — |

42 new unit tests, full lib suite green except the pre-existing
Windows-only `os::windows::tests::test_enumerate_devices_nonempty`
which requires a real physical disk.

---

## Still open — pick one

### Large
- **§2.3 HFS+ journal Step 27** — route every `do_sync_metadata`
  block write through `TransactionBuilder.record_*`. Deep refactor
  across every HFS+ write site (~500+ LOC + journal-replay-on-open
  plumbing). The recorder + replay infra (`src/fs/hfsplus_journal.rs`)
  is already in place from steps 25/26; this is the consumer wiring.

### Medium
- **§6.2 `rb-cli get` globbing** — extend the `get` verb with glob
  expansion + recursive extract. Plan calls for an "implicit batch"
  shape: multi-match `get` builds an in-memory batch, runs one
  preflight pass, applies as one operation. The glob infrastructure
  in `src/cli/glob.rs` already powers `ls` / `rm` / `put`.

### Design-checkpoint
- **§6.3 GUI `.hqx` import + auto-unwrap hook** — pair work. Import
  lands a decoded `.hqx` into edit mode (Add File → decode → create
  on the open filesystem). Auto-unwrap routes a `.hqx` whose payload
  sniffs as DiskCopy 4.2 or raw HFS into the disk-image pipeline
  instead of treating it as a loose file. Decide UX first (does the
  picker auto-detect? do we always prompt? what about ambiguous
  payloads?), then code.

---

## Parked — need fixtures

All these are in `docs/need_fixtures.md` with target filenames and
producer instructions. Drop the fixture, remove the "Parked" tag in
OPEN-WORK, resume.

- **ReiserFS R.3b/c/d** — S+tree walker, `list_directory`, `read_file`
  with tail packing. Needs `tests/fixtures/test_reiserfs_v3_5.img.zst`
  + `test_reiserfs_v3_6.img.zst`. Grab `reiserfsprogs` while it's still
  packaged — the Linux kernel is on the removal track.
- **UFS U.1/U.2/U.3** — UFS1 (FreeBSD ≤4, Solaris, NetBSD, OpenBSD) +
  UFS2 (FreeBSD 5+). Needs `tests/fixtures/test_ufs1.img.zst` +
  `test_ufs2.img.zst`.
- **JFS J.1/J.2/J.3** — IBM JFS2. Needs
  `tests/fixtures/test_jfs2.img.zst`.
- **§3.2 NTFS file-aware GHO compressed path** — needs a real
  Norton-Ghost-produced compressed file-aware NTFS backup.
- **§3.3 HFS raw partition extend** — needs an APM-less raw HFS
  partition image.
- **§4.1 Clonezilla LVM** + **§4.2 Clonezilla RAID** — need real
  Clonezilla sidecar dumps.

User-side verification (not coding):
- **§7 HFV in BasiliskII / MAME** — boot/mount our blank + cloned HFVs.
- **§7 HFV restore filename default** — optional nicety.
- **§7 CD CHD ISO9660 browse** — manual SHA256 cross-check.

---

## Where the canonical state lives

- `docs/OPEN-WORK.md` — every open item with design detail.
- `docs/need_fixtures.md` — fixtures blocking parked items.
- `docs/mister_filesystem_implementation_plan.md` — the one other
  living plan (treated as one line item in OPEN-WORK).
- `CLAUDE.md` — codebase conventions.
- `CONTRIBUTING.md` — architecture + code-placement rules.

Every other per-area plan was consolidated into OPEN-WORK and
deleted on 2026-06-02 (commit `5082f17`). If you find a reference
to a deleted doc (`docs/codecleanup.md`, `docs/cli-todo.md`,
`docs/windows_self_update.md`, etc.) in a doc comment, that's a
stale pointer — leave the comment, the surrounding code still works.
