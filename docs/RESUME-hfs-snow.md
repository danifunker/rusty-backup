# Resume prompt: HFS follow-ups, judged by Disk First Aid under Snow

Run after `docs/RESUME-audit-3-macos.md` (DONE 2026-09-05). Pull
`bugfixer1` first. Work on the Mac: Snow, the ROMs and the classic-Mac
disks live here.

## Prompt

You are continuing the HFS / HFS+ work that the 2026-09-01 audit's macOS
leg left open, on branch `bugfixer1`. That leg let `fsck_hfs -n` and the
kernel HFS+ driver judge our writers and fixed R-054..R-059 on the way
(`docs/Regression_Bugs.md`, "leg 3"). This leg adds the second judge Apple
shipped with the machines these volumes are for: **Disk First Aid running
on Mac OS 7 inside the Snow emulator**, and closes the HFS items that leg
left behind. One finding per commit; the pre-commit hook (fmt + clippy
`-D warnings`) is on; run `cargo test --lib` before each commit.
CONTRIBUTING.md rules apply: two-line comments, ASCII-only UI strings, no
threads spawned from `gui/`, engine code stays on Rust 1.73.

The HFS items come first, in the order below; the non-HFS remainder at
the end needs the user's hardware and can only be prepared.

### 0. Snow, and what it can and cannot judge

- Snow is `/Applications/Snow.app` (`Snow [--floppy IMG]... [ROM|workspace]`);
  sources are `~/repos/snow` (upstream) and `~/repos/snow-danifunker` (the
  fork with the CD-ROM patches). It emulates the Plus, SE, Classic, II /
  IIx / IIcx / SE/30 with up to seven SCSI disks and floppies (`.img`,
  `.dsk`, MOOF).
- The proven headless driver is `~/repos/MacAtrium/tools/snow-harness/`
  (README there): `macatrium_harness.rs` builds as a bin inside
  `~/repos/snow/testrunner`, boots a Mac II from a SCSI image, attaches a
  second disk (`--disk2`), taps keys at CPU-cycle marks (`--keys`) and
  dumps the framebuffer to PNG (`--snap-every`, `final.png`). It needs the
  Mac II FDHD ROM (`~/repos/lbmactwo_MiSTer/releases/MacIIFDHD.rom`, present)
  and the Macintosh Display Card 8-24 ROM `3410868.bin` from MAME's
  `nb_mdc824.zip`, which is at
  `~/Library/Application Support/Ample/roms/nb_mdc824.zip` (unzip it; the
  `/tmp/mdc/` copy the README used is gone). The System 7 boot disks the
  README names moved: they are in `~/Documents/MacOS_SampleDisks/`
  (`MacLC_7-1.hda`, `MacLC_7-5-5_OG.hda`, ...). Check that one carries
  Disk First Aid (`rb-cli ls` under its Utilities or Apple Extras folder)
  and put it there with `put-binhex` from the Disk Tools floppy if not.
  The harness has no mouse and no floppy option; Disk First Aid is driven
  by clicks, so add `--click X,Y@CYCLE` and `--floppy IMG` to the harness
  (snow_core has both inputs) before scripting it.
- **Disk First Aid 7.x judges classic HFS and reads MFS. It does not know
  HFS+**; HFS+ needs Mac OS 8.1+, which needs a 68040 and is outside Snow.
  HFS+ therefore keeps `fsck_hfs -n` (`scripts/verify-fs-macos.sh`) as its
  judge, with Basilisk II + `fs.hfv.populated-macos81.hd` (Mac OS 8.1's Disk
  First Aid) as the second opinion if one is wanted.
- Boot media: the System 7.1 800K set is in `~/Downloads/Apple Mac OS 7.1
  (3.5-800k)/` (`Disk Tools.img` boots and carries Disk First Aid); the
  1440K `~/Downloads/Disk Tools.img` is the 7.5 one (needs the FDHD ROM).
  A SCSI test volume must be a disk the ROM registers: wrap a bare volume
  with `rb-cli new hd apm --partition <size>:Apple_HFS --fill 1=VOL`, then
  `rb-cli mac-scsi-bless IMG --driver-from ~/Downloads/EmptyHFS.hda` (or
  any BlueSCSI `.hda` in `~/Downloads`). The headless boot disk is one of
  the `MacLC_*.hda` images above (copy it first, they are the only ones);
  only if none has Disk First Aid, copy it over from the Disk Tools floppy
  with `rb-cli get-binhex` / `put-binhex` (forks intact, the harness
  README shows the pattern).
- First pass by hand is fine: open Snow, pick the Mac II ROM, add the
  SCSI image, insert the floppy, run Disk First Aid, read the dialog. Save
  a workspace so `Snow that.snoww` reopens the same machine. Then script it
  so the table below can be re-run; a PNG of the verdict dialog is the
  evidence, read it with the image reader and keep it under
  `docs/evidence/` only if it is small.

### 1. Disk First Aid on the leg-3 volumes (the point of this leg)

`scripts/verify-fs-macos.sh -w DIR` builds every volume and leaves them in
`DIR`. Take each classic-HFS one to Snow and let Disk First Aid verify it;
record the verdict in the verification table of `docs/Regression_Bugs.md`
(add a "Disk First Aid" column beside the `fsck_hfs` result):

- H1-hfs (`h1-hfs.img`): 1500 files, 200 deleted from the middle, re-added.
  Also open a few files from the Finder.
- H7-hfs (`h7-hfs.img@1`, the APM disk): the grown volume; the fill grew it
  to the partition. This one is already an APM disk; only the driver is
  missing.
- H6 (`h6-mfs.img`): insert as a floppy (`--floppy`). If Disk First Aid
  declines MFS, the check is: the Finder mounts it, every file opens, and a
  Finder copy of each file compares byte-equal with `rb-cli get`.
- H5's macOS-made HFS+ volume is out of scope here (HFS+).

Anything Disk First Aid reports that `fsck_hfs` did not is a new R-0xx:
reproduce it with rb-cli alone, fix the writer, add the check to
`src/fs/hfs_fsck/`, then re-run both judges.

### 2. F-011: the classic HFS writer allocates contiguously

`create_file` on classic HFS asks `allocate_blocks` for one run of the
whole fork and reports disk full on a fragmented volume with room to
spare (`docs/missing_features_from_regression.md`, F-011). Allocate a
fork as a list of runs (first fit over `bitmap_collect_clear_runs_be`),
fill the three inline extents, insert the rest as overflow records through
`btree_insert_full` with `BTreeKeyFormat::CLASSIC_EXTENTS` (the delete and
read sides already handle them). Then H3 on classic HFS can finally be
built: `scripts/verify-fs-macos.sh -o H3-hfs` must pass `fsck_hfs -n`
before and after the delete, and Disk First Aid must agree.

### 3. H3 against a fork a real Mac fragmented

The strongest version of H3: a file whose extents a real Mac spilled into
the overflow file, deleted by rb-cli, judged by both tools. Either fragment
a volume inside Snow (fill it with copies from the Finder, delete every
other one, copy a large file) or use the regression corpus's real Mac II
disk `fs.hfs.populated-macii.hd` (`regression-tests/FIXTURES.md`; count
its overflow records with the `xt_leaf_records` helper in
`scripts/verify-fs-macos.sh`, classic layout). Delete a spilled file with
`rb-cli rm`, then `fsck_hfs -n` and Disk First Aid.

### 4. Classic HFS must fill its partition (R-058, the rest of it)

- Add the check `fsck_hfs` applies (`E_ABlkSt`) to `src/fs/hfs_fsck/`: with
  the partition length known, `drAlBlSt + drNmAlBlks * (drAlBlkSiz / 512)`
  must equal the partition's next-to-last sector; report the shortfall and
  say the volume needs to fill its partition.
- `rb-cli resize IMG@N --size` exited 1 on the classic-HFS H7 volume even
  though `resize_hfs_in_place` exists; find where the verb routes HFS and
  make it reach the in-place grow (it refuses cleanly when the fixed volume
  bitmap has no room, which is the right answer, but it must say so).
- Say the same at open time: an editable classic-HFS open whose volume is
  smaller than its partition logs one warning naming `resize`.

### 5. B-tree header attributes

Our HFS+ trees carry `attributes = 0` in the BTHeaderRec; Apple writes
`kBTBigKeysMask | kBTVariableIndexKeysMask` (6) on the catalog and
attributes trees and `kBTBigKeysMask` (2) on the extents tree (see the
`hdiutil`-made H5 volume). Neither the kernel nor `fsck_hfs` complained,
but match Apple in `write_blank_btree_header_node` and wherever the
resize / clone paths build a header, then re-run H1-hfsplus through
`scripts/verify-fs-macos.sh` and mount it. Low risk, one commit.

### 6. The HFS+ shrink half of R-056

R-056 rewrote `resize_hfsplus_in_place` for both directions but only the
grow was judged. Back up an HFS+ partition with files near its tail,
restore it with Minimum sizing (the trim point from `last_data_byte`),
attach the result and run `fsck_hfs -n`; then grow it back and check
again. Record the outcome next to R-056.

### 7. A real-Mac-formatted volume edited by rb-cli

Format a small volume inside Snow (Disk First Aid's neighbour on the Disk
Tools floppy can initialize a SCSI disk; or use `~/Downloads/EmptyHFS.hda`),
then `put`, `mkdir`, `mv`, `rm` and `setrsrc` with rb-cli, and verify in
Snow. Everything above starts from rb-cli's own formatter; this catches
what the formatter's layout hides.

### 8. The non-HFS remainder (needs the user's hardware)

These are pending from leg 3 and cannot be done unattended; prepare the
exact commands and leave them in the status line for the user:

- R6 (`5f1fd54`): an SD card with its lock switch on, opened in Inspect and
  by `rb-cli backup`; expect "is write-protected ... opened read-only" and
  no prompt; a restore to it must refuse before unmounting anything.
- R11 (`f2edc77`): cancel the authorization dialog once; expect
  "administrator authorization was cancelled" and no second prompt.
- R19 (`0093c49`): `rb-cli inspect /dev/rdiskN` on the USB floppy drive;
  expect the one-time "continuing one sector at a time" warning if the
  drive refuses large reads, and a correct partition listing.
- Section 3 of leg 3 (`63e8d3f`): a card opened in Inspect while mounted;
  expect "read-write escalation ... retrying read-only".
- After those pass, remove the `TEMP-DIAG` logging (`grep -rn TEMP-DIAG src/`,
  eight sites) that was kept for exactly this confirmation.

### Close-out

Update the status line below, add the Disk First Aid column to the
verification table in `docs/Regression_Bugs.md`, run
`bash scripts/preflight.sh`, commit, push.

Status: sections 0-2 done 2026-09-05. Snow is scripted:
`scripts/verify-hfs-snow.sh leg3 -w DIR` (harness source in
`scripts/snow/macatrium_harness.rs`, now with `--floppy`, digits and
`type@`, 1-bit PNG + PBM frames; the verdict is read off the frame against
`scripts/snow/dfa-*.pbm`). Disk First Aid passes H1-hfs, H7-hfs and, after
F-011 shipped the run-list allocator, H3-hfs before and after the delete;
it declines MFS, so H6 is judged by the Finder copy + TeachText. Evidence in
`docs/evidence/`. Section 3 done too: `verify-hfs-snow.sh h3-real` lets the
System 7.1 Finder copy a 1 MB file onto the rb-cli-fragmented H3 volume
(Mac OS writes 5 overflow records), `rb-cli rm` deletes it, both judges
pass before and after. Next: section 4 (classic HFS must fill its partition).
