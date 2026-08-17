# Prompt: two cue-sheet fixes for `opticaldiscs-rs`

> **SUPERSEDED 2026-08-10 — both defects are fixed upstream and our pin is
> now `opticaldiscs = "0.15.0"` (`Cargo.toml`). R-012 and R-015 are closed.
> The version numbers below (0.13.0 / 0.14.0) are left as written: this is
> the report that produced the fix, not a description of today.**


Paste into a session working on the `opticaldiscs-rs` checkout. Both defects
were found by rusty-backup's regression suite against real-world media and are
tracked there as R-015 and R-012.

Both reproduce on **0.13.0** and on **0.14.0** — I bumped and re-ran to check,
so neither is already fixed on the latest release.

---

## Context: who consumes this

rusty-backup uses the crate at `version = "0.14.0", features = ["drives"]`,
through this surface:

```
opticaldiscs::detect::DiscImageInfo::{open, open_physical}
opticaldiscs::browse::{open_disc_filesystem, open_hybrid_filesystem, open_physical_filesystem}
opticaldiscs::browse::entry::{EntryType, FileEntry}
opticaldiscs::browse::filesystem::{Filesystem, FilesystemError}
opticaldiscs::{BinCueSectorReader, DiscFormat, FilesystemType, OpticaldiscsError,
               ElTorito, GameDiscInfo, Console, BootMediaType, JolietVolumeDescriptor}
```

It reads these `DiscImageInfo` fields: `path`, `format`, `filesystem`,
`hybrid_filesystems`, `volume_label`, `pvd`, `hfs_mdb`, `hfsplus_header`,
`el_torito`, `game`.

**Please keep the change additive** — new fields and variants rather than
changed signatures — so consumers upgrade without edits.

---

## Issue 1 — cue sheets with unpadded track numbers are rejected

The CUE spec's examples pad to two digits, but plenty of real tools emit
`TRACK 1`. One such disc is a retail CD-ROM (Microsoft Bookshelf), so this is
not a hand-written edge case.

**Reproduce:**

```
FILE "BOOKSHELF.img" BINARY
   TRACK 1 MODE1/2352
   INDEX 1 00:00:00
```

```rust
DiscImageInfo::open("BOOKSHELF.cue")
```

```
CUE error: Error(Msg("Expeceted number but found String(\"1\") instead"), ...)
```

**Expected:** parses identically to `TRACK 01` / `INDEX 01`. The same disc
parses fine when both numbers are zero-padded — padding is the only
difference, verified by editing one byte.

**Also worth fixing while you are in there:** `Expeceted` is misspelled. It is
load-bearing right now only because it makes the message easy to grep for.

**Suggested scope:** accept 1-or-2-digit numbers wherever the cue grammar
takes a track or index number. Worth auditing the same parser for other
tokens it requires to be padded.

---

## Issue 2 — audio-only discs are rejected outright

A pure CD-DA disc has **no data track by definition**. Today that is treated
as a failure, so an audio CD image is indistinguishable from a corrupt one.

**Reproduce:**

```
FILE "cdda-noaudiodata.bin" BINARY
  TRACK 01 AUDIO
    INDEX 01 00:00:00
  TRACK 02 AUDIO
    INDEX 01 00:05:25
```

```rust
DiscImageInfo::open("cdda-noaudiodata.cue")   // -> Err
```

```
No data track found
```

(Note this cue *is* correctly padded, so it is a genuinely separate defect
from Issue 1 — fixing the parser will not fix this one.)

**Expected:** `open` succeeds and describes the disc. There is nothing wrong
with it; there is simply no filesystem on it. Something like:

- `filesystem: None` (or a `FilesystemType::None` / `AudioOnly` variant)
- a track list the caller can render: index, `TRACK` type (AUDIO / MODE1 /
  MODE2), start LBA, length in sectors, and MSF or duration
- ideally `is_audio_only()` or equivalent, so a caller need not infer it

Mixed-mode discs (a data track followed by audio tracks) already open — the
audio tracks just are not surfaced. Exposing the track list fixes both cases
with one addition, and lets a caller report "1 data + 12 audio" instead of
silently describing only the data track.

**Why it matters to the caller:** rusty-backup's `optical info` is
specifically the verb meant to survive discs that strict parsers reject. It
currently has to report `Container: unknown, Filesystems: (none recognized)`
for a perfectly good audio CD.

---

## Acceptance

Both of these should hold afterwards:

```rust
// Issue 1
assert!(DiscImageInfo::open("unpadded.cue").is_ok());

// Issue 2
let info = DiscImageInfo::open("audio_only.cue").expect("audio-only is a valid disc");
assert!(info.tracks.iter().all(|t| t.is_audio()));
assert!(info.filesystem.is_none());
```

Please add both cue sheets above as test fixtures — they are small, and each
pins a real-world shape that a stricter parser would otherwise regress.

Once released, bump the version and I will re-run rusty-backup's cases
`optical.cue.unpadded-track-number` and `optical.cdda.no-data-track-opens`,
which assert exactly this behaviour and are currently red on purpose.
