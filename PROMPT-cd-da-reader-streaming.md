# Adopt cd-da-reader's streaming + `TrackBounds` in the Optical CD-DA path

> **Task prompt** for a Claude Code session working in `rusty-backup`.
> Self-contained: follow top to bottom, run the verification steps, then report
> results. Line numbers are approximate — `rg` for symbols.

## Goal

Our `cd-da-reader` fork (`danifunker/rust-cd-da-reader`, branch
`file-backend-on-1.0`, PR #58 upstream) gained a streaming reader and an explicit
track-bounds geometry. Adopt them in `src/optical/cd_audio.rs` so CHD audio
playback **streams** instead of reading a whole track into a `Vec`, **without
changing the gapless bounds behaviour we already depend on**.

What changed in the crate:

1. **`TrackBounds { SessionGap, Gapless }`** — how a track's sector range is
   resolved from a `Toc`. `SessionGap` (the default used by `read_track`)
   subtracts the ~11,400-sector CD-Extra inter-session gap; `Gapless` does not.
2. **Streaming** over `AudioSectorReader`: `AudioTrackStream` with
   `open_track_stream` (TOC, `SessionGap`), `open_track_stream_with_bounds`
   (TOC + explicit `TrackBounds`), and `open_track_stream_at(src, start_lba,
   sectors)` (explicit absolute range, no TOC). Methods: `next_chunk`,
   `with_sectors_per_chunk`, `seek_to_sector`/`seek_to_seconds`,
   `current/total_sectors`, `current/total_seconds`.
3. **Unified error**: the file-backing `read_track` now returns `CdReaderError`
   (new `Backend(Box<dyn Error + Send + Sync>)` variant). The old generic
   `TrackReadError` is gone — we never referenced it, so no fallout.

## Non-negotiable correctness constraint (read this first)

Our CHD extracts are **gapless**: `extract_to_cue` lays tracks back-to-back and
`ChdDisc::open` builds the TOC from cumulative frame counts. So we MUST resolve
bounds **without** the CD-Extra subtraction — i.e. use `open_track_stream_at`
(we hand it our own bounds) or `TrackBounds::Gapless`. We must NOT use plain
`read_track` / `open_track_stream`, which default to `SessionGap` and would chop
~2.5 min off the last audio track before a data track.

This is the exact reason `extract_audio_pcm_chd` currently hand-computes bounds
and avoids `read_track` (see the comment at `src/optical/cd_audio.rs:167`). That
reasoning is unchanged; we're only expressing it through the new streaming API.

## Step 1 — pull the new crate commits

The changes are on `danifunker/rust-cd-da-reader` branch `file-backend-on-1.0`
(commits through `70f3756`: unify errors, add streaming + `TrackBounds`, reframe
`TrackBounds` naming). Make sure that branch is pushed, then in `rusty-backup`:

```sh
cargo update -p cd-da-reader
```

The dep line already points at the branch (no edit needed):

```toml
cd-da-reader = { git = "https://github.com/danifunker/rust-cd-da-reader", branch = "file-backend-on-1.0", optional = true }
```

Sanity-check the new symbols resolve: `rg 'AudioTrackStream|open_track_stream_at|TrackBounds' ~/.cargo/git/checkouts/rust-cd-da-reader-*/*/src/` (or just let Step 2's build fail loudly if the update didn't land).

## Step 2 — stream the CHD audio path

`src/optical/cd_audio.rs` → `extract_audio_pcm_chd` (~line 139) currently:
computes `start_lba`/`end_lba` inline, calls
`disc.read_audio_sectors(start_lba, end_lba - start_lba)` to buffer the **whole
track**, then re-chunks it into ~1 s callbacks via `BYTES_PER_BATCH`.

Keep the `is_audio` guard and the existing `start_lba`/`end_lba` computation
(including its comment), and replace the whole-track read + `for chunk in
pcm.chunks(..)` batching with a stream. Add `open_track_stream_at` to the
`cd_da_reader` import at the top of the file:

```rust
use cd_da_reader::{lba_to_msf, open_track_stream_at, AudioSectorReader, Toc, Track};
```

```rust
    // ...unchanged: idx lookup, is_audio guard, start_lba / end_lba, bounds check...
    let sectors = end_lba - start_lba;

    // Stream the gapless [start_lba, start_lba + sectors) we just computed.
    // open_track_stream_at bypasses TOC bounds entirely, so the CD-Extra rule
    // never runs — the reason we avoid read_track. 75 sectors = 1 s of CD audio,
    // matching the previous ~1 s callback cadence.
    let mut stream = open_track_stream_at(&disc, start_lba, sectors).with_sectors_per_chunk(75);

    let mut total = 0u64;
    while let Some(chunk) = stream
        .next_chunk()
        .map_err(|e| format!("read track {track_number}: {e}"))?
    {
        let samples = pcm_le_i16(&chunk);
        total += samples.len() as u64;
        on_samples(&samples);
    }
    Ok(total)
```

Then delete the now-unused `BYTES_PER_BATCH` constant.

Notes / gotchas:
- `open_track_stream_at` is the idiomatic choice for a backing that computes its
  own layout (which we do) — it takes explicit absolute bounds, no TOC, no
  policy, no CD-Extra rule.
- `ChdDisc`'s `AudioSectorReader` impl is unchanged (`&self`, re-opens the temp
  BIN per read, `type Error = std::io::Error`). `next_chunk` requires
  `R::Error: Error + Send + Sync + 'static`; `io::Error` satisfies it.
- Borrow check: compute `start_lba`/`end_lba` (both `u32`) before opening the
  stream so the `&disc.toc` borrows are released; the stream then holds only the
  `&disc` shared borrow, and `disc` outlives it.
- `CdReaderError` is `Display`, so the existing `.map_err(|e| format!("…: {e}"))`
  pattern is unchanged.

**Alternative — let the crate compute the bounds.** If you'd rather drop the
inline `start_lba`/`end_lba` block, use the TOC form with the gapless geometry:

```rust
let mut stream = cd_da_reader::open_track_stream_with_bounds(
    &disc, &disc.toc, track_number, cd_da_reader::TrackBounds::Gapless,
)
.map_err(|e| format!("open track {track_number}: {e}"))?
.with_sectors_per_chunk(75);
```

`TrackBounds::Gapless` resolves the same `[start_lba(n) .. start_lba(n+1)|leadout)`
we compute by hand. Pick ONE approach — `open_track_stream_at` (keep local bounds
+ comment) OR `Gapless` (delete them). Do **not** use the default `SessionGap`.

## Step 3 — BIN/CUE path: leave as-is (optional)

`extract_audio_pcm_bincue` reads straight from the `.bin` and already streams a
75-sector loop; it does not go through `AudioSectorReader`. Routing it through the
crate would require a bincue-backed `AudioSectorReader` impl — not worth it. Leave
it unless you specifically want the symmetry.

## Verification

The CD-DA player is behind the `audio` feature (→ forces `optical` + `chd`).
A lighter, module-focused build:

```sh
cargo build  --no-default-features --features optical,chd
cargo test   --no-default-features --features optical,chd
cargo clippy --no-default-features --features optical,chd -- -D warnings
cargo fmt --check
```

(`cargo build` with default features also covers it, but pulls the GUI stack.)

Then the check that actually matters — play a **CD-Extra CHD** (audio tracks
followed by a data track) and confirm the **last audio track before the data
track plays its full length**. If it's truncated by ~2.5 min, bounds are being
resolved as `SessionGap` — re-check you used `open_track_stream_at` (own bounds)
or `TrackBounds::Gapless`, not `read_track` / `open_track_stream`.

Also confirm a normal multi-track audio CHD plays every track start-to-finish,
and that memory stays flat during playback (streaming, not whole-track buffering).

## Done criteria

- CHD playback streams via `AudioTrackStream` (no whole-track `Vec`), gapless
  bounds preserved, `BYTES_PER_BATCH` removed.
- Build / test / clippy / fmt clean with `--features optical,chd`.
- Last-audio-before-data track on a CD-Extra CHD plays full length; normal audio
  CHDs play every track in full.
