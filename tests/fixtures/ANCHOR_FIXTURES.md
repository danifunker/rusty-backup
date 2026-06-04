# Anchor fixtures — committed but not yet consumed

Files in `tests/fixtures/` with the `anchor_mister_` prefix are
**real-world fixtures pulled from a live MiSTer board** that don't
have a consuming test today. They're committed so the next slice of
engine work has a known-good oracle ready, and so we don't lose
them between sessions.

Each anchor is paired with a follow-up item in `docs/OPEN-WORK.md`
§7 user-side / §1 engine that names the work blocking its
consumption.

## Current anchors

### `anchor_mister_BLANK_disk_X68000.D88.zst` (348 KB compressed, 1.28 MB raw)

A freshly-formatted blank Human68k floppy from the MiSTer X68000
core distribution. Sharp `.d88` container wrapping a FAT12-derived
Human68k volume.

**Blocked by:** `.d88` container decoder (parked under
`docs/mister_filesystem_implementation_plan.md` §3.2). Once that
ships, `src/fs/human68k.rs` (already implemented + write-path-ready)
can validate against this disk byte-for-byte.

### `anchor_atom_FROGGER.atm.zst` (2.5 KB) + `anchor_atom_INVADER.atm.zst` (2.9 KB)

Two sample Acorn Atom files in the canonical `.atm` cassette/tape
format from `hoglet67/AtomSoftwareArchive` V13.00 (2024-05-05).
Each `.atm` file has a 22-byte header (16-byte name + 2-byte load
+ 2-byte exec + 2-byte length) followed by raw program bytes.

**Status:** the MiSTer AcornAtom core uses a FAT-formatted .vhd as
its SD-card backing (per the MiSTer-devel/AcornAtom_MiSTer README),
so the FAT side is already covered by our existing FAT engine. These
`.atm` anchors are for **any future Atom file-format-aware tooling**
(metadata extraction, type/load/exec surfacing, AGD/.atm conversion).
A real-world FAT VHD with the full 5311-file archive is on the user's
MiSTer at `/media/fat/games/AcornAtom/AtomSoftwareArchive_V13.vhd`
(built by `scripts/build_atom_vhd.sh` and uploaded via scp). Our
FAT engine extracts files from that VHD **byte-for-byte identical**
to the original `.atm` sources (verified by host-side cmp against
the two committed anchors).

**Blocked by:** nothing required for MiSTer use today (FAT is
shipped). Optional follow-up: `src/fs/atom_file.rs` for `.atm`
metadata decoding, which would surface load/exec addresses in
inspect rows for tools that scan .atm collections.

### `anchor_mister_GamesCart.mdv.zst` (36 KB) + `anchor_mister_crazy.mdv.zst` (54 KB)

Two QL Microdrive cartridges shipping with the MiSTer QL core
distribution. **Different filesystem layout from QXL.WIN** —
microdrive is sector-streamed with per-sector record headers (the
"MD" / "Test" name bytes visible in the first sector are part of
the QDOS microdrive header struct).

**Blocked by:** a `.mdv` reader (`src/fs/qdos.rs` covers QXL.WIN
HDD volumes only; microdrive is a separate format documented under
`docs/mister_filesystem_implementation_plan.md` §5 QL row). Our
QDOS engine field interpretations are based on Sinclair's hard-disk
spec; the microdrive on-disk format is described in
*QDOS Reference Manual* ch. 12.

## When to consume

- An anchor stays anchored until a test under
  `tests/<core>_e2e.rs` references it and asserts byte-level
  expectations against our decoder.
- When that lands, remove the `anchor_` prefix and update this file.
- If an anchor becomes obsolete (the format gets de-scoped, the
  fixture turns out to be the wrong sample), delete it from
  `tests/fixtures/` and remove the entry here in the same commit.

## Provenance

- Pulled 2026-06-04 from `192.168.99.143` (MiSTer ARM build at
  `/media/fat/games/{X68000,QL}/`) via `scp` using the `~/.ssh/
  mister_only` key the user provided.
- Files have been part of the public MiSTer-Distribution build for
  > 12 months at pull time; the BLANK_disk_X68000.D88 ships with
  the X68000 core, GamesCart.mdv + crazy.mdv ship with the QL core.
  Treated as redistributable per the MiSTer project's bundled-asset
  conventions; revisit if any rights-holder disputes that.
