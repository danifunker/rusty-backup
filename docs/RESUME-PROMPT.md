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
list. Recommended order:

  1. **§1.2 UFS** (Tier A: U.1 + U.2) — fixture generation is now
     unblocked (`linux-modules-extra-$(uname -r)` is installed in
     WSL so libguestfs can mount UFS2 in its appliance). Reuses the
     EFS scaffolding (`src/fs/efs.rs` + `src/fs/unix_common/`) which
     already implements the cylinder-group + classic-Unix-inode
     shape. Should be a smaller lift per phase than ReiserFS was.
  2. **§1.3 JFS** (Tier A: J.1 + J.2) — same fixture story; JFS2
     has a basic B+tree walker even at Tier A because BMAP itself
     is a B+tree of allocation control pages.
  3. **§2.3 HFS+ journal Step 27** if you want the biggest piece
     (~500 LOC across every HFS+ write site).
  4. **§6.2 `rb-cli get` globbing** for medium scope.
  5. **§6.3 GUI `.hqx` import** for a design checkpoint first.

To regenerate or extend a fixture, model your script on
`scripts/generate-reiserfs-fixtures.sh`. The WSL environment is
already configured (libguestfs + linux-image-virtual + modules-extra
+ dpkg-statoverride on the vmlinuz).

Park items the user hasn't given a fixture for and move on.
```

---

## Recently shipped (last session, 2026-06-02 evening)

4 commits on `mister-parity` ahead of `9759a97`:

| Commit | Item | Tests added |
|---|---|---|
| `7e58ffc` | **ReiserFS fixture** — `test_reiserfs_v3_6.img.zst` (3.7 KB compressed, 64 MiB raw) + `scripts/generate-reiserfs-fixtures.sh`. v3.5 fixture parked: modern kernel won't mount it. | — |
| `e2b53b1` | **ReiserFS R.3b** S+tree walker (`collect_leaf_block_numbers`, `collect_items_for_object`). Fixed two R.3a bugs the real image exposed: `LEAF_LEVEL` was 0 (should be 1; kernel `DISK_LEAF_NODE_LEVEL = 1`); `KeyFormat::from_version` mapped `2 → V2` (should be `1 → V2`; kernel `ITEM_VERSION_2 = 1`). | 10 |
| `9f53b83` | **ReiserFS R.3c** `list_directory` — DIR_ENTRY decoder (16-byte `reiserfs_de_head` + name slots), StatData parser (new 44 B / old 32 B), `pack_loc/unpack_loc`, `.reiserfs_priv` filter, recursive subdir descent. | 14 |
| `2be0e0b` | **ReiserFS R.3d** `read_file` — SD + IND (sparse-zero handling) + DRCT (tail-padding truncation), `max_bytes` honoured eagerly. Closes the entire §1.1 read track. | 5 |

29 new ReiserFS unit tests (57 total in the module). Full lib suite
green except the pre-existing Windows-only
`os::windows::tests::test_enumerate_devices_nonempty` which requires
a real physical disk.

### WSL fixture-generation environment is now set up

`scripts/generate-reiserfs-fixtures.sh` documents the host-side
setup as comments at the top, but the one-time work is already done
on the user's WSL Ubuntu 24.04:

  * `reiserfsprogs`, `libguestfs-tools`, `linux-image-virtual`,
    `linux-modules-extra-$(uname -r)` are all installed
  * `dpkg-statoverride` makes `/boot/vmlinuz-*` readable to the
    user (libguestfs needs it; persists across kernel upgrades)

So UFS / JFS fixture generation works the same way: format with the
host's `mkfs.*`, populate via libguestfs's QEMU appliance kernel.
Just write a `scripts/generate-{ufs,jfs}-fixtures.sh` modeled on
the reiserfs one.

---

## Still open — pick one

### Filesystem read-track (fixture work unblocked)
- **§1.2 UFS** (Recommended next) — Tier A: U.1 superblock (UFS1 at
  byte 8192 magic `0x011954`, UFS2 at byte 65536 magic `0x19540119`),
  U.2 cylinder-group walk → compact + `last_data_byte`. Tier B: U.3
  inode + dir + file browse. Reuses `src/fs/efs.rs` +
  `src/fs/unix_common/` patterns (cylinder-group + classic-Unix-inode
  shape). Refuse softupdate-journaled + SU+J dirty volumes with a
  clear message rather than risk corrupting recovery state.
- **§1.3 JFS** (Tier A: J.1 + J.2) — JFS2 only; reject AIX JFS1 with
  a clear error. Aggregate Superblock at byte 32768. BMAP is itself
  a B+tree of allocation control pages so even Tier A needs a basic
  B+tree walker.

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

- **ReiserFS v3.5 real-image validation** — parked because modern
  Linux kernels (6.x) refuse to mount v3.5 volumes (`EUCLEAN
  "Structure needs cleaning"`; `-o conv` no longer auto-upgrades).
  R.1/R.2/R.3a synth tests cover v3.5 superblock + V1 keys
  end-to-end. Would need an older kernel (~5.x) to mount-and-
  populate; not worth chasing now that the read path is shipped.
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
