# NDIF non-ADC chunk codecs — sample request + support notes

**Status:** NOT on the active roadmap — gated on someone contributing real
sample images. Sourcing NDIF images that use these codecs is hard, and modern
`hdiutil` cannot create them, so we are **not** hunting for samples ourselves.
If you have a `.smi` / `.img` (with its resource fork preserved — see below)
that uses one of these codecs, please open an issue / PR with it attached and
point at this doc; that's the trigger to implement it.

**Scope:** decode the NDIF chunk codecs `rusty-backup` does not yet handle —
Apple RLE (0x81), LZH (0x82), KenCode (0x80), and StuffIt (0xF0). Zero / raw /
ADC chunks and `.smi` browsing already work.

---

## Background — what already works

NDIF ("New Disk Image Format") is the pre-UDIF classic-Mac disk image format
(Disk Copy 6, Mac OS 8/9). It also underlies **self-mounting images (`.smi`)**.
The disk blocks live in the file's **data fork** as a series of chunks; a `bcem`
resource in the **resource fork** is the block map describing them.

Support already shipped (see [`src/rbformats/ndif.rs`](../src/rbformats/ndif.rs)
and [`src/rbformats/README.md`](../src/rbformats/README.md)):

- Fork-carrier delivery: MacBinary (`.smi.bin`), AppleDouble (`._name`), and
  native resource forks are decoded in
  [`src/model/source_reader.rs`](../src/model/source_reader.rs)
  (`try_open_ndif_carrier`), reconstructing the flat image so `.smi` files open
  like any disk. Gated on the presence of a `bcem` resource, so a plain `.bin`
  raw image is never affected.
- Chunk codecs **decoded today**: `0x00` zero-fill, `0x02` raw/copy, `0x83`
  ADC. Verified against a real *Disk First Aid 8.6.1* `.smi.bin`.
- Chunk codecs **recognized but not decoded**: `0x81` RLE, `0x82` LZH, `0x80`
  KenCode, `0xF0` StuffIt. As of the codec-dispatch refactor these produce a
  **clear per-codec error** (e.g. "NDIF chunk N uses LZH compression (type
  0x82), which is not yet supported") instead of silently misrouting the bytes
  to the ADC decoder and yielding garbage.

## The blocker

We have **no sample image that uses any of the four missing codecs**, and modern
`hdiutil` cannot create NDIF at all — so there is no oracle to implement or
verify against. Disk Copy 6.3.x defaults to ADC, which is why every easily
obtained sample (including the Disk First Aid one) is already covered.

Additionally, **KenCode (0x80) is undocumented** — there is no published
algorithm. It can only be reverse-engineered *from* a real sample, if at all.
RLE (0x81) and LZH (0x82) are more tractable (LZH is the classic LHA/LZH
algorithm; there are Rust decoders such as `delharc`), but the exact NDIF
framing still has to be pinned against a real chunk.

## Which tool produces which codec (approximate)

The chunk codec is chosen by the creating tool, not by NDIF itself. Boundaries
are fuzzy — verify empirically (see the scanner below) rather than trusting the
tool name.

| Codec | Chunk type | Typical creator |
|-------|-----------|-----------------|
| ADC | 0x83 | Disk Copy 6.3.x default *(already supported)* |
| LZH | 0x82 | ShrinkWrap (Aladdin) — the canonical LZH-NDIF creator |
| RLE | 0x81 | older Disk Copy 6.0–6.1, DiskDup+ |
| KenCode | 0x80 | very early Disk Copy 6.0-era — rare, undocumented |
| StuffIt | 0xF0 | tools that ran chunks through the StuffIt engine — uncommon |

## Getting samples

**Create (most reliable).** In a classic-Mac environment (real 68k/PPC Mac or
Basilisk II / SheepShaver / Mini vMac), make a small (400K/800K floppy) image
and save it once with each tool: **ShrinkWrap** → LZH, older **Disk Copy** /
**DiskDup+** → RLE, etc. **Preserve the resource fork on the way out** — the
`bcem` block map lives there, and a plain copy over a modern share strips it.
Encode as MacBinary (`.bin`) or BinHex (`.hqx`) first, exactly like the working
Disk First Aid sample.

**Find.** Macintosh Garden / Macintosh Repository host many old `.img`/`.smi`
distributions; late-90s Apple Software Update / CD-ROM archives on archive.org
are another source. Most are ADC, so scan candidates to find the exceptions.

## Proposed first step — an NDIF codec scanner (cheap, unblocks the hunt)

Before implementing any codec, add a read-only diagnostic that reports the
`bcem` chunk-type histogram of a file (or a folder of files), flagging any that
use 0x80/0x81/0x82/0xF0. This turns "hunt blindly" into one command and is the
fastest way to know when a real RLE/LZH/KenCode/StuffIt sample is in hand.

Suggested surface (pick one):
- `rb-cli inspect --ndif-codecs <file-or-dir>`, or
- a standalone `examples/ndif_codecs.rs`.

Logic: reuse `ndif::extract_bcem` on the carrier's resource fork (MacBinary /
AppleDouble / native), parse the chunk table, and tally the low-byte type of
each entry. ~30 lines; no new decode logic.

## Implementation notes (once a sample exists)

- Codecs live in [`src/rbformats/ndif.rs`](../src/rbformats/ndif.rs). The
  dispatch `match ty { … }` in `reconstruct()` already has the arms stubbed with
  clear errors — replace the relevant `bail!` with a decode call.
- Follow the existing ADC pattern: `decode(src, span) -> Result<Vec<u8>>`, then
  copy into `out[o..o+span]`.
- **RLE (0x81):** implement/verify the Apple RLE byte format against a real
  chunk; small and self-contained.
- **LZH (0x82):** evaluate the `delharc` crate (or port the LHA decoder); the
  chunk is a raw LZH stream — confirm the framing (header vs. bare) against a
  sample.
- **StuffIt (0xF0):** the tree already has a full StuffIt decoder in
  [`src/macarchive/`](../src/macarchive/) — but NDIF 0xF0 is raw StuffIt-*method*
  data on a chunk, not a `.sit` archive, so the framing differs. Determine
  whether the macarchive engine's inner-method entry points are reusable.
- **KenCode (0x80):** undocumented. Leave as a clear error unless a sample makes
  reverse-engineering worthwhile.
- Add a unit test per codec that reconstructs a known chunk (build the `bcem` +
  data fork in-test, as `reconstruct_raw_and_zero_chunks` does), and — when a
  real sample exists — a fixture-gated end-to-end test that browses it and
  compares an extracted file against a known hash.

## Acceptance criteria

- The scanner reports chunk-type histograms and flags the unimplemented codecs.
- For each codec a real sample surfaces: decode it, browse the reconstructed
  image, and verify an extracted file byte-for-byte against an oracle
  (mounted image or a known checksum).
- The per-codec `bail!` in `reconstruct()` is replaced with a real decode only
  once that codec is verified against a real image; codecs still lacking a
  sample keep their clear error.
