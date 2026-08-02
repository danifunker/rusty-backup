# Fixture Gaps

State of the corpus as of 2026-08-02. Regenerate whenever fixtures are added
or a new `rb-cli new` builder lands.

## Where things stand

| | Count | Size |
|---|---:|---:|
| Corpus on the NAS (`rb-fixtures/fixtures/`) | **59** | **40.3 MB** |
| — mirrored from `tests/fixtures/` | 45 | 3.3 MB |
| — harvested by extension | 8 | 6.4 MB |
| — harvested by folder name (optical) | 6 | 30.6 MB |
| Budget ceiling | — | 250 MB |

Every one of the 53 has been checked to actually open (`rb-cli inspect`, or
`ls --fs-type` for the signature-less ones). Nothing was admitted on the
strength of its file extension alone — see § False positives.

Also worth restating: **~26 of 42 filesystems and ~18 of 47 containers need
no fixture at all**, because `new volume` / `new floppy` / `new hd` /
`squashfs create` / `convert` build them. The gaps below are only what
neither synthesis nor the corpus covers.

## Gaps

### Filesystems — 6

| Missing | Note |
|---------|------|
| Apple Lisa FS | No builder, no fixture, none found in any source. |
| PFS3 | **Cheapest fix is not a fixture.** `create_blank_pfs3` already exists in the engine for tests; exposing it on `new volume` deletes this row. |
| SFS | Same — `create_blank_sfs` exists internally. |
| Alto BFS / TFS | Candidate material in NAS `Software/PARC-Stuff.zip`, unexamined. |
| Pilot / Cedar | Same archive. |
| QDOS QXL.WIN | We hold a `.mdv` microdrive anchor, but not the QXL.WIN hard-disk form. |

Promoting the two Amiga builders is the single highest-value action here: it
removes two rows for a small change and gains tier-1 coverage that needs no
stored bytes at all.

### Containers — ~13

| Missing | Note |
|---------|------|
| DART | Apple disk archive. |
| NDIF / self-mounting `.smi` | Candidates likely in `Old Mac Stuff`. |
| Apple sparseimage | Not seen in any scan. |
| IMZ (encrypted) | Needs both a correct- and wrong-password case. |
| WinImage `.ima` | 12 candidates in `C:\Temp`, unverified. |
| ZIP-disk (raw image in a `.zip`) | Trivially constructible by hand. |
| cb-dos / `.cbk` | Constructible via `rb-cli cbk pack`. |
| AppImage | Constructible — any AppImage on disk works. |
| Xerox family: Alto pack, Salto, ContrAlto2, Diablo Trident, Pilot/Cedar volume, Dwarf 6085 | Six formats, one likely source (`PARC-Stuff.zip`). Worth one focused session. |

Several of these are *constructible* rather than needing a harvest — ZIP-disk,
cbk and AppImage should become tier-1 cases, not fixtures.

### Optical — 4 remaining (was 8)

Closed this pass by searching **by folder name rather than extension** —
see § Searching for container-payload formats. Six admitted, all verified
with `rb-cli optical info`:

| Fixture | Size | Detected as |
|---------|-----:|-------------|
| `optical.gamecube.druaga.cd` | 0.4 MB | `gamecube` |
| `optical.iso9660.joliet.cd` | 1.0 MB | `iso9660, joliet` |
| `optical.cdi.hieroglyph.cd` | 3.6 MB | `cdi` |
| `optical.hfs.opentransport.cd` | 6.8 MB | `iso9660, joliet, hfs` |
| `optical.efs.irix-scsitb.cd` | 8.0 MB | `efs` (real IRIX disc) |
| `optical.3do-opera.puttputt.cd` | 10.8 MB | `opera` |

The GameCube disc at 0.4 MB and the CD-i at 3.6 MB are far below what a
"real disc" usually costs — worth knowing that small discs exist rather than
assuming every optical fixture is a 700 MB problem.

**Still missing: High Sierra, UDF, optical UFS, VMS ODS-2.** Wii is
technically open (smallest local candidate is 308 MB, over budget) but the
GameCube disc exercises the same driver, so it is low priority.

Not a fixture gap, worth noting: `TGCD/Addams Family (USA).chd` opens as a
CHD but reports `(none recognized)` for filesystems, which is expected —
PC-Engine CD carries no standard filesystem.

### Partition schemes — needs verification

MBR, SGI and X68000 are synthesizable; AHDI and superfloppy have fixtures.
**GPT, APM and Sun disk label are not explicitly covered** by either a
verified fixture or a confirmed builder. I have not verified these
individually — treat as unknown rather than missing until checked.

## Searching for container-payload formats

**Extension matching has a blind spot, and it is not a small one.** CD-i,
3DO, GameCube, PC-Engine CD and Mac optical discs carry no distinctive
extension — they are `.chd`, `.iso`, `.cue`/`.bin`, indistinguishable from
thousands of console dumps with no fixture value. The first scan therefore
reported "no CD-i, no 3DO, no GameCube" while all three sat in
`ConsoleGames/ROMs/` in folders named exactly `CDi`, `Philips CD-i`, `3DO`
and `GCN`.

Worse, the extension scan actively misled: it counted 6,214 `.chd` totalling
2,525 GiB, which I dismissed wholesale as console media. The CD-i and 3DO
fixtures were inside that pile the whole time.

**Rule: for any format whose payload is a generic container, search by folder
and filename, not by extension.** `inventory-fixtures.ps1 -ByFolder`
implements this against a maintained list of platform folder patterns, and
tags each row with `why = ext | folder` so the two discovery paths stay
distinguishable. Extend the folder list rather than widening the extension
list — a generic container in the right folder is a fixture; the same
container anywhere else is noise.

### Blind spot 2 — bin/cue is structurally invisible to a size-filtered scan

`.bin` is in the noisy-extension exclusion (off by default, because it is
mostly ROMs and CD data tracks), and `.cue` files are ~100 bytes, far under
the 64 KB `-MinBytes` floor. So **the canonical CD-DA distribution format
could not appear in a scan at all**, and the first pass reported CD audio as
an unfilled gap while `USBODE-backup` held purpose-built mixed-mode and
audio-sampler test discs at the share root.

Fix when scanning for optical work: `-IncludeNoisy -MinBytes 0`, or pair
every `.bin` with a `.cue` lookup. Treat a `.cue` as the unit, not the `.bin`.

### Blind spot 3 — archives are never opened

The scanner walks files, not archive members. `Software/BMOW.zip` contains
`BMOW/Lisa stuff/` with **twenty-plus Lisa disk images**, which is the entire
Apple Lisa FS gap sitting inside one zip that the scan reported as a single
uninteresting file. Same pattern in `winworld-pc/extracted`, where the useful
ISOs live inside `.7z` directory names.

No tooling change made yet. For now, hand-check the archives at the root of
each share before declaring a format unavailable — `PARC-Stuff.zip` is the
remaining unexamined one, and it likely holds the entire Xerox set.

## False positives found while harvesting

The reverse failure mode. Four extensions collide badly with unrelated file
types, so nothing is admitted without opening it first:

| Extension | What it actually was |
|-----------|----------------------|
| `.scp` | Xbox game saves, not SuperCard Pro flux images |
| `.mdx` | PS Vita 3D models, not the disk format |
| `.st` (87 KB) | A ScummVM data file, not an Atari ST disk |
| `.cdi` | Dreamcast DiscJuggler images, **not** CD-i — the actual CD-i discs are `.chd` under `ROMs/CDi/` |

## Rejected candidates, and what they revealed

Three verified candidates failed to open. Two are expected; one is a finding.

- **`.imd` (ImageDisk)** — not a supported format. Correct behaviour; ImageDisk
  is not in the README format table. Not a gap, just not a thing we do.
- **Acorn ADFS `.adf`** (`C:\Temp\adfs_arc01_orig.adf`, 640 KB) — fails
  detection. `.adf` routes as Amiga ADF, so an Acorn ADFS image sharing the
  extension is not reached. `new floppy adfs` covers Acorn ADFS for tier 1, so
  this is a read-path limitation rather than a coverage hole. Worth a look.
- **G64 is partial** — of three real G64 files tested, one opened and two
  failed: `Protector II (must be write protected).g64` and an
  `American Express ... [patched].g64`. Both are copy-protected or patched
  dumps with non-standard GCR. `C'est la Vie.g64` opens fine and is now the
  corpus fixture. **Filed as R-011** — worth confirming whether protected GCR
  is out of scope by design or an actual decoder limitation, since preserving
  protected disks is exactly what G64 exists for.

## Shopping list

What to actually go and get. Ordered by value per unit of effort. **Smallest
example that works is always better than a representative one** — a 5 MB
disc proves the same code path as a 700 MB one.

### Closed since the list was written — all found locally

| Was | Found in | Fixture |
|-----|----------|---------|
| Mixed-mode CD (data + audio) | `USBODE-backup/mixedmode-both.bin/.cue` | `optical.cdda.mixedmode.cd/` (39.7 MB, MODE1 + AUDIO) |
| CHD CD-profile with audio | `USBODE-backup/usbode-audio-test.chd` | `optical.chdcd.audio-test.cd` (18.5 MB) |
| Apple Lisa FS | **`Software/BMOW.zip` → `BMOW/Lisa stuff/`** | `fs.lisa.los31-blank.floppy` + `fs.lisa.office31-d1.floppy` (409 KB each) |
| Pure HFS optical | `winworld-pc/extracted` StuffIt Expander 5.5 | `optical.hfs.stuffit.cd` (3 MB, `hfs` only, no ISO 9660) |

### Tier A — still genuinely needed

| # | Want | Acceptable form | Notes |
|--:|------|-----------------|-------|
| 1 | **UDF disc, small** | `.iso` | Only local candidate is `USBODE-backup/DVDs/FANTASIA_RESTORED.ISO` at **4.3 GB** — verified `Filesystems: udf`, but 17x over the whole corpus budget. Need a small UDF disc, or accept a one-off oversized fixture kept outside the corpus. |
| 2 | **High Sierra CD** | `.iso` or `.bin`/`.cue` | Pre-ISO 9660, roughly 1986-1988 pressings. Nothing in USBODE or winworld matched — every ISO probed came back `iso9660`. Grolier / Compton encyclopaedias, early Mac CD-ROM titles. |
| 3 | **Solaris / SunOS install CD** | `.iso` | Closes optical UFS **and** likely the Sun disk label row at once. |
| 4 | **OpenVMS install CD** (VAX or Alpha) | `.iso` | Only realistic source of ODS-2 / Files-11. |
| 5 | **Sinclair QL QXL.WIN** | the `QXL.WIN` file itself | We have a `.mdv` microdrive but not the hard-disk container. |

### Not a fixture gap — blocked on code (R-012)

**Pure audio CDs are already on hand and cannot be used.** `USBODE-backup`
holds `Audio-only.bin/.cue` (35 MB, 1 track) and `Audio CDs/FF_CD.bin/.cue`
(130 MB, 5 tracks), but `optical info` rejects any disc with no data track:
`unrecognized disc image: No data track found`.

Nothing to source here. Decide R-012 first — a fixture no verb can open
proves nothing. Mixed-mode is unaffected and is already admitted.

### Tier B — Mac container formats

Likely already on the NAS in `Old Mac Stuff`; I have not dug through it.

| # | Want | Form |
|--:|------|------|
| 9 | **DART archive** | `.dart` |
| 10 | **NDIF / self-mounting image** | `.smi` |
| 11 | **Apple sparseimage** | `.sparseimage` |

### Tier C — Xerox family, one archive may cover all of it

`Software/PARC-Stuff.zip` is unexamined and may contain the lot: **Alto disk
pack, Salto, ContrAlto2, Diablo Trident, Xerox Pilot/Cedar volume, Dwarf 6085**
— six container rows plus the Alto BFS/TFS and Pilot/Cedar filesystem rows.
Worth one focused session before sourcing anything externally.

### Tier D — PC floppy containers

| # | Want | Form |
|--:|------|------|
| 12 | **WinImage image** | `.ima` — 12 candidates already in `C:\Temp`, unverified |
| 13 | **IMZ, ideally encrypted** | `.imz` — needs a correct-password *and* a wrong-password case |

### Do NOT source — these are constructible

Turning these into tier-1 constructed cases is cheaper than hosting fixtures:

- **ZIP-disk** — a raw image inside a `.zip`, trivially made by hand
- **cbk** — `rb-cli cbk pack`
- **AppImage** — any AppImage on disk
- **ADZ / HDZ** — gzip-wrapped ADF/HDF, which we already hold
- **PFS3 / SFS** — `create_blank_pfs3` / `create_blank_sfs` exist in the
  engine already; expose them on `new volume` instead

### Already have, no action needed

Wii is technically uncovered (smallest local candidate is 308 MB, over
budget) but the 0.4 MB GameCube disc drives the same driver. Not worth the
size.

## Next actions, cheapest first

1. Expose `create_blank_pfs3` / `create_blank_sfs` on `new volume` — removes
   two filesystem gaps with no stored bytes.
2. Turn ZIP-disk, cbk and AppImage into tier-1 constructed cases.
3. Verify GPT / APM / Sun coverage and close whichever are genuinely open.
4. One focused session on `PARC-Stuff.zip` — potentially closes six container
   rows and two filesystem rows at once.
5. Optical, last and most carefully, with aggressive minimisation.
