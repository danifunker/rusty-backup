# RESUME: close the four macOS hardware checks

Everything in the 2026-09-01 audit is shipped and merged. Four verification
checks remain, and every one of them needs removable media and a human at the
keyboard — they cannot be run unattended, which is why they have survived two
sessions. This prompt exists to close them.

Prior context: `docs/RESUME-audit-3-macos.md` (the audit leg itself),
`docs/Regression_Bugs.md` (the record; the verification table near the bottom
holds the four rows to update).

## Read this before you start

**The blocker was fixed on 2026-09-08.** R-068: `open_source_for_reading` —
the function that escalates through `authopen`, recognises write-protected
media (R-051) and decodes a cancelled dialog (R-052) — had four callers, all
in the GUI. The CLI's two device sites in `src/model/source_reader.rs` reached
the device with a plain `File::open`, so on macOS, where privilege is escalated
per operation and never inherited by the process, `rb-cli` could only touch a
raw device under `sudo`. Both sites now go through `open_source_for_reading`.

Two consequences for this work:

1. The R19 reproduction command in `docs/RESUME-audit-3-macos.md` **could not
   have worked** unprivileged before that fix. That is why the check never ran
   on 2026-09-05, not any property of the drive.
2. The user recalls rusty-backup reading this floppy successfully in the past.
   That was almost certainly the **GUI**, which has always had the authopen
   path. Treat "it worked before" as evidence about the GUI, not the CLI, and
   not as evidence that the drive is currently healthy.

## Setup

The release binary is stale as of this writing — rebuild first:

```bash
cd /Users/dani/repos/rusty-backup && cargo build --release --bin rb-cli
```

Do not run two cargo builds at once, and do not raise the job count or
debuginfo (`.cargo/config.toml` caps both; see `docs/build-memory-crashes.md`).

Confirm the media is actually attached before anything else:

```bash
diskutil list external && diskutil info diskN | grep -iE 'read-only|removable|media name|disk size'
```

`Media Read-Only: Yes` means the write-protect tab is open — that is the R6
condition, so note it now.

### Hardware gotchas that cost the last session

- **The drive vanishes.** On 2026-09-08 `/dev/disk5` and the whole USB floppy
  device disappeared from the bus mid-session; `diskutil list external`
  returned nothing at all. A read then fails with **ENXIO (os error 6),
  "Device not configured"** — which reads like a driver refusing the transfer
  but is really "there is no device". Re-seat the drive and re-check
  `diskutil list external` before believing any read failure.
- **ENXIO is not EIO.** R-053's original note recorded EIO at sector 0. If you
  get ENXIO instead, suspect the bus, not the read path.
- **800K Mac floppies cannot be read at all** by a PC USB floppy drive: they
  are GCR-encoded with variable speed. Only 1.44MB MFM ("SuperDrive"-format)
  Mac floppies work. The drive advertises 2880 x 512 for any disk, so the
  reported size tells you nothing. If this disk is an 800K, R19 cannot be
  closed on this hardware and the row should say so rather than staying open
  forever.

## The four checks

Run each, then update its row in the verification table in
`docs/Regression_Bugs.md` (the rows currently reading "pending hardware") and
strike the matching summary row if it is now fully verified.

### 1. R19 / R-053 — the USB floppy raw read

```bash
./target/release/rb-cli --log-level debug inspect /dev/rdiskN
```

Expect: the real size (a 1.44MB floppy is 1474560 bytes / 2880 sectors), not
`0 B`. If the drive refuses a multi-sector read, expect exactly one warning:

```
a <n>-byte read failed (...) but one sector reads fine; continuing one sector at a time
```

That line is `SectorAlignedReader::read_after_refusal` in `src/os/mod.rs`. Its
absence is fine — it only fires on a drive that refuses large reads. What
matters is that the size is right and the partition table or HFS volume is
detected.

If it fails, get a baseline before blaming our code:

```bash
sudo dd if=/dev/rdiskN bs=512 count=1 of=/dev/null && sudo dd if=/dev/rdiskN bs=8192 count=1 of=/dev/null
```

512 works and 8192 fails is the R-053 shape. Both failing is the drive or the
media (see the gotchas above).

Since this disk is HFS, also confirm the volume actually opens:

```bash
./target/release/rb-cli ls /dev/rdiskN
```

### 2. R6 / R-051 — write-protected media

Needs media whose lock is engaged: an SD card with the lock switch down, or
this floppy with its write-protect tab open (`Media Read-Only: Yes`).

```bash
./target/release/rb-cli --log-level debug backup /dev/rdiskN /tmp/r6-backup
```

Expect the read path to log, verbatim:

```
<path> is write-protected (lock switch or read-only image); opened read-only, a restore to it cannot work
```

with **no second authorization prompt**, and the backup completing read-only.
Then confirm a restore refuses *before* it unmounts anything — it must not
touch the device first. The write path has its own, different message:

```
<path> is write-protected (media lock switch or read-only image); it cannot be written to
```

The code is `da_media_writable` / `DKIOCISWRITABLE` in `src/os/macos.rs`
(the ioctl at ~127, the read-path log at ~925, the write-path bail at ~1468).

### 3. R11 / R-052 — a cancelled authorization dialog

This is now reachable from the CLI (it was GUI-only before R-068). With no
cached descriptor for the device — a fresh process, since `ELEVATED_DEVICES`
caches per device per session — run the inspect and **click Cancel**:

```bash
./target/release/rb-cli --log-level debug inspect /dev/rdiskN
```

Expect: the log says the administrator authorization was **cancelled**, and
**no second prompt** — the read-only retry must be skipped, because a cancel is
the user's answer. Getting a second dialog, or an error mentioning "no
ancillary control message", is the R-052 regression.

### 4. Section 3 (63e8d3f) — read-only escalation fallback

Needs media that is **mounted** while it is opened, so an SD card or USB stick
formatted FAT/exFAT — the HFS floppy will not mount on modern macOS and cannot
exercise this path.

Insert it, let macOS mount it, then open it in the GUI's Inspect tab. Expect:

```
read-write escalation of /dev/rdiskN failed (...); retrying read-only
```

Inspect should then succeed read-only, browse should work, and edit mode should
say why it is unavailable rather than failing obscurely.

## Then: remove TEMP-DIAG

`describe_device_access` and its call sites were added to diagnose a macOS
restore failing with `Permission denied` at the very end of an otherwise
successful run. That was diagnosed and fixed on 2026-08-05. The instrumentation
was kept only until the fix was confirmed on hardware — which is check 2 above.

**Once R6 passes on real write-protected media, delete all eight sites:**

```bash
grep -rn 'TEMP-DIAG' src/
```

- `src/os/mod.rs:195` — `describe_device_access` itself (and its doc comment at
  200)
- `src/os/macos.rs:936` — `probe_device_access`
- `src/os/macos_stub.rs:346` — the stub counterpart
- `src/gui/backup_tab.rs:2591`, `src/gui/inspect_tab.rs:2885`,
  `src/gui/restore_tab.rs:1850` — the three call sites

Removing the function means the `#[allow(unused_variables)]` on it goes too.
Check `cargo clippy --all-targets -- -D warnings` after.

## Close-out

- Update the four rows in the verification table in `docs/Regression_Bugs.md`,
  and strike R-051 / R-053 in the summary table if their hardware halves now
  pass. Record a genuine "cannot be tested on this hardware" verdict if the
  floppy turns out to be 800K — an untestable check should be closed with a
  reason, not left pending a third time.
- Update the status block at the bottom of `docs/RESUME-audit-3-macos.md`.
- Engine code must still compile on Rust 1.73 — see CONTRIBUTING.md. The modern
  build will not catch a violation, and clippy's autofixes postdate 1.73.
- `bash scripts/preflight.sh`, then commit. Integration is by **pull request**
  against `main`; never merge locally.
