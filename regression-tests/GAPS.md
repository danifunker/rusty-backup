# Fixture Gaps

Narrative only. **The counts are computed — do not maintain them here:**

```
rb-regress fixtures            # catalogued, verified, runnable, and what is unused
rb-regress query verbs         # rb-cli verbs no case invokes
rb-regress query unfixtured-reads   # formats we read with no real-media fixture
rb-regress produce             # builders with no recipe
rb-regress validate            # manifests, registry, bug list
```

The table that used to sit here said 80 fixtures and 3251 MB. It was wrong
within a week, and a wrong count reads as authority. Ask the tools.

Every fixture has been opened with `rb-cli` before admission — nothing is
admitted on the strength of its extension. See § False positives for why that
rule exists.

## CloneCD set — recovered, and the format was never missing

A `BOOKSHELF.ccd`/`.img`/`.sub` set was deleted during triage on 2026-08-07
and has since been repopulated. Now held as
`optical.clonecd.bookshelf.cd/` in the annex.

Worth recording why it was deleted, because the reasoning was wrong twice
over: it was discarded as "an unsupported format", and CloneCD turned out to
be **supported** — `optical info` on the `.ccd` reports
`Container: clonecd`, `Filesystems: high_sierra`, and walks 1,056 files. The
earlier test had been run against the `.img`, which correctly reports
`unknown` because raw 2352-byte sectors carry no header.

Two lessons, both cheap:

- **Test the descriptor, not the payload.** `.ccd` for CloneCD, `.cue` for
  bin/cue. Pointing a detector at the raw data file proves nothing.
- **Never discard an unrecognised specimen.** Even if the format genuinely
  were unsupported, that is the one case where a sample has the most value.

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

## Open, 2026-08-08

Not fixture gaps in the corpus sense — things the suite cannot yet assert.
Listed because each one was discovered by trying, and would otherwise be
rediscovered.

**Harness**

- **Multi-file artifacts.** `fmt.vmdk-flat` (descriptor + `-flat.vmdk` extent)
  and `fmt.bincue` (66-byte `.cue` + sibling `.bin`) are the only two builders
  `produce` still has no recipe for, both for this reason. Keeping half of
  either would read as coverage of a format only half looked at.
- **Emulator oracles are mostly, no longer entirely, uninvokable.** Two now
  run from a script: `iris` (IRIX 6.5, which produced R-039) and FS-UAE via
  `oracles/fsuae/affs_mount.py`, which closed R-020 on 2026-08-14. Both use
  the same trick — a host directory the guest writes its verdict into, so
  nothing is screen-scraped. The remaining ~60 emulator and MiSTer-core
  oracles are still `skip-manual`.

  The AFFS one has a `check` line and `verify` can invoke it, but fs-uae's
  availability stays `manual`: it wants a GUI session and about a minute per
  run, so it is on-demand rather than part of an unattended sweep. Making it
  a first-class row means deciding whether the runner may open a window —
  a policy call, not a missing feature.
- **27 package oracles have no runnable check command**, so `verify` skips
  them with `skip-no-check`.

**Assertions we cannot write yet**

- **Documented-lossy conversions.** Every conversion case asserts identity.
  Where a conversion is *meant* to lose something, nothing asserts what it
  loses, so a conversion that starts losing more would pass. Needs the losses
  enumerated per format pair first — the format matrix does not record them.
- **Amiga file comments.** `FileEntry` carries `amiga_comment` and the GUI
  browse view shows it, but no CLI verb reads or writes one — `chmeta` covers
  protection bits only. A CLI/GUI parity gap, not a defect; the case cannot be
  written until the surface exists. Protection bits *are* covered
  (`edit.affs.protection-bits-persist`).
- **HFS type/creator on a directory**, and resource-fork content beyond
  `setrsrc` writing one.

**Fixtures still wanted**

- **A pure CD-DA disc inside a CHD.** `optical.chdcd.audio-test.cd` was
  assumed to be one on the strength of its AUDIOTST volume label and is not —
  it carries an ISO 9660 data track. R-012's CHD half is therefore uncovered;
  only the cue/bin half is pinned.
- **A donor Mac volume with a blessed System Folder**, so
  `make-bootable` has a positive case rather than only the diagnostic one.

**Verbs**

`rb-regress query verbs` is the live answer. Of what it lists, only `write`
(physical media, needs a `--device-allowlist` that does not exist) and `serve`
(a daemon lifecycle) are worth closing; the rest are interactive or reach the
network.
