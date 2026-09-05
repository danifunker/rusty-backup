# Resume prompt: audit sweep, leg 3 of 3 (macOS)

Run after `docs/RESUME-audit-2-windows.md`. Pull `bugfixer1` first.

## Prompt

You are continuing the 2026-09-01 codebase audit on branch `bugfixer1` on a
Mac. Legs 1 and 2 covered Linux and Windows; this leg does the macOS-only
findings, an open macOS bug from an earlier session, and verifies the HFS /
HFS+ fixes against Mac OS itself. One finding per commit; the pre-commit
hook (fmt + clippy -D warnings) is on; run `cargo test --lib` before each
commit. CONTRIBUTING.md rules apply: two-line comments, ASCII-only UI
strings, no threads spawned from `gui/`, engine code stays on Rust 1.73.

### 1. macOS-only findings (R6, R11)

- R6: `os/macos.rs` (search `EACCES`, around the authopen retry) treats a
  write-protected card's EACCES as "needs privilege", so the app asks for
  authorization it cannot use. Distinguish read-only media (report it) from
  a real permission failure (escalate). Test with an SD card's lock switch.
- R11: the cancel string compared against authopen errors never matches, so
  a user cancelling the authorization dialog is reported as a failure.
  Capture the actual authopen text on this machine and match on that.

### 2. Open bug: floppy inspect fails with EIO on sector 0 (R19)

Carried over from an earlier session. Facts established then:

- `rb-cli inspect /dev/rdiskN` on a USB floppy drive fails with EIO reading
  sector 0; `dd bs=512` from the same device works (about nine times slower
  than normal, which is expected for a floppy).
- Prime suspect: `F_NOCACHE` set on the handle by `open_source_for_reading`
  before `clear_nocache` runs; the other suspect is the large `pread` in
  `SectorAlignedReader` (`os/mod.rs`, around `read_at`).
- `open_for_inspect` in `os/macos.rs` is dead code and can go.

Reproduce with the floppy drive, then try: (a) skip `F_NOCACHE` for
removable media, (b) cap the first read at 512 bytes. Fix whichever
reproduces, remove the dead code, and record the cause in
`docs/Regression_Bugs.md` under R19.

### 3. Verify the authopen read-only fallback (63e8d3f)

An earlier session shipped this and was waiting on CI for the arm64 DMG to
test it. Insert a card, open it in Inspect while it is mounted, and look for
the log line

```
read-write escalation of /dev/rdiskN failed (...); retrying read-only
```

Inspect should succeed read-only; browse works; edit mode says why it is
unavailable. Note the result in `docs/Regression_Bugs.md`.

### 4. Verify the HFS / HFS+ fixes against Mac OS

Disk Utility's First Aid (or `fsck_hfs -n` in Terminal) is the judge:

- H1 / H2 / H4 / H9 / H11 (ee07cf4): fill an HFS+ image with rb-cli until
  the catalog splits several times, delete from the middle, add again;
  `fsck_hfs -n` must be clean and every file readable in Finder.
- H3 (67ab9f2): delete a file with a resource fork that spilled into the
  extents overflow file; `fsck_hfs -n` reports no orphaned extents.
- H5 (3a53254): run `rb-cli fsck` on the images above and on a known-good
  volume; no false "keys out of order" reports.
- H6 (a1f7558): an MFS image edited by rb-cli opens in Mini vMac or
  Basilisk II and Disk First Aid there is clean.
- H7 (shipped in leg 1, 9cb5383): the alternate MDB / volume header is now written 1024 bytes before the partition end when the length is known; confirm Disk First Aid accepts an edited volume.

Record results next to each ID in `docs/Regression_Bugs.md`.

### 5. Build sanity on macOS

`cargo test --no-run` from a clean `target/debug`; note the peak memory in
Activity Monitor in `docs/build-memory-crashes.md` if it is worth recording.
The vintage 10.7 build is CI's job; nothing to do locally.

### Close-out

Update the status line below, run `bash scripts/preflight.sh`, commit, push.
This is the last leg; afterwards the audit's Low items live in the findings
list only, and `docs/Regression_Bugs.md` is the record.

Status: DONE 2026-09-05 (5f1fd54..HEAD, one finding per commit), with
the hardware halves still open because no removable media was attached.
Step 1: R6 (5f1fd54) and R11 (f2edc77) shipped; R6 verified against an
`hdiutil -readonly` image (opened read-only, no prompt) and R11 against
authopen's two-byte reply protocol read from its disassembly (module
header of `os/macos.rs`), so a card's lock switch and a live cancel are
what the user still has to try. Step 2: R19 (0093c49) — the CLI's device
path was a plain `BufReader` (unaligned 8 KiB reads, every device shown as
0 B), never `F_NOCACHE`, which the CLI never set; devices now read through
`SectorAlignedReader`, which drops to one sector per read when a larger
read is refused. The floppy drive itself is pending. Step 3 needs a card.
Step 4 was the productive part: `scripts/verify-fs-macos.sh` (fsck_hfs -n
plus the kernel HFS+ driver) passed H1, H3 (HFS+), H5 and H7, and on the
way found six defects our own fsck never saw, all fixed with the matching
fsck checks added: R-054 stale index separators after a delete, R-055 an
empty root leaf and unerased free nodes, R-056 the HFS+ grow's bitmap and
allocation file, R-057 the alternate header on the put and fill paths,
R-058 classic HFS having to fill its partition, R-059 the classic grow
overlapping the alternate MDB (`docs/Regression_Bugs.md`). H3 on classic
HFS cannot be built by rb-cli (F-011, contiguous allocator); H6 needs Disk
First Aid in Mini vMac, the image is built. Step 5: 152 s, 2.35 GB peak
(`docs/build-memory-crashes.md`). This was the last leg; the audit's Low
items live in the findings list only.
