# Fixture Gaps

State of the corpus as of 2026-08-02. Regenerate whenever fixtures are added
or a new `rb-cli new` builder lands.

## Where things stand

Regenerate whenever fixtures are added or a builder lands.

| | Count | Size |
|---|---:|---:|
| Core corpus (`rb-fixtures/fixtures/`) | **75** | see catalogue |
| Large-fixture annex (`fixtures-large/`) | **5** | — |
| Catalogued total | **80** | **3251 MB** |
| Soft budget for the core corpus | — | 250 MB |

Every fixture has been opened with `rb-cli` before admission — nothing is
admitted on the strength of its extension. See § False positives for why that
rule exists.

## Re-source: CloneCD set (deleted in error)

A `BOOKSHELF.ccd` / `.img` / `.sub` CloneCD rip was dropped into the inbox on
2026-08-07 and deleted during triage, on the reasoning that CloneCD is not a
supported container. That was backwards — an unsupported format is exactly
what you keep a specimen of, and the `.sub` subchannel data is the part no
`.iso` can carry. Not recoverable from the share.

The same disc survives as `optical.high-sierra.bookshelf.cd` in ISO form, but
that cannot stand in: no subchannel, no `.ccd`. See F-002 in
`docs/missing_features_from_regression.md`.

**Wanted:** any CloneCD set, not necessarily this disc.

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
