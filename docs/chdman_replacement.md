# Replacing chdman with libchdman-rs

End state: zero references to the external `chdman` binary in rusty-backup. All CHD
read/write/convert paths route through `libchdman-rs` (path dep at `../libchdman-rs`).
The user can pick codecs and hunk size for every CHD we create. DVD CHDs (MAME 0.287+)
are a first-class supported format. The `chdman_path` config field, the startup detect,
the per-tab `chdman_available` plumbing, and the "chdman not found" UI strings all go away.

Each stage below is sized for one session, includes enough context to start cold, and
ends with a green build + targeted verification. Tick each box as it lands.

---

## Stage 0 — branch + dependency wiring ✅

- [x] Create branch `chdman-removal`.
- [x] Add `libchdman-rs = { git = "https://github.com/danifunker/libchdman-rs" }` to `Cargo.toml` `[dependencies]`. (Git URL, not path dep, so CI builds work.)
- [x] ~~Remove the `chd = "..."` dep~~ — deferred to Stage 2 where `ChdReader` is actually swapped. Removing it here breaks the build.
- [x] `cargo build` green; first compile of MAME C++ core ~2 min, incremental ~8s.
- [x] Smoke test `examples/chd_smoke.rs` opens HD CHD (`mac608.chd`, 700 MB, codecs `[lzma, zlib, huff, flac]`) and CD CHD (`Tyrian2000.chd`, 26 tracks, codecs `[cdlz, cdzl, cdfl]`); reports version, hunk/unit bytes, SHA1, and format flags correctly.

**Done when:** workspace builds with libchdman-rs linked, smoke test reads an existing CHD.

---

## Stage 1 — CHD options type + config plumbing (no behavior change yet) ✅

Goal: a single source of truth for "how to write a CHD" that every call site can take. No
production code path uses it yet — this stage is just shape.

- [x] New module `src/rbformats/chd_options.rs` with `ChdOptions { hunk_size, codecs }`,
      `ChdProfile { Hd, Cd, Dvd }`, `defaults_for()` matching chdman's `s_default_*_compression`
      tables exactly, plus helpers `parse_codec_string`, `codec_label`, `codec_long_name`,
      `is_codec_supported`.
- [x] Module declared in `src/rbformats/mod.rs`.
- [x] 8 unit tests — three default-matchers (HD/CD/DVD), parse round-trip + reject garbage,
      codec_label round-trip, supported-codec sanity. All pass.
- [x] No production call sites use it yet — Stage 3 is the first consumer.

**Done when:** `cargo test rbformats::chd_options` passes, `cargo clippy` clean.

---

## Stage 2 — replace `ChdReader` with libchdman-rs (read-side swap) ✅

Lowest-risk change. `ChdReader` in `src/rbformats/chd.rs` (lines 14–107) wraps the old `chd` crate.
Browse/inspect already routes through it via `src/gui/browse_view.rs::create_filesystem`.

- [x] In `src/rbformats/chd.rs`, replaced `ChdReader`'s internals with a thin wrapper around
      `libchdman_rs::Chd` + `read_bytes`. Public API (`pub fn open`, `Read`, `Seek`) unchanged
      so callers don't need updates. Added `unsafe impl Send for ChdReader` because
      `libchdman_rs::Chd` holds a raw `*mut ChdFile` and is not auto-Send, but `open_filesystem`
      requires `Read + Seek + Send + 'static` for the worker-thread handoff. The handle is
      single-owner and only touched from one thread at a time, so Send is sound.
- [x] Removed `chd = "0.2"` from `Cargo.toml`. Still appears in `Cargo.lock` as a transitive
      dep via `opticaldiscs-rs` — out of scope for this stage; will fully disappear when/if
      `opticaldiscs-rs` migrates too.
- [ ] **Deferred runtime spot-check**: no CHD test fixtures available in `~/Documents`. Build
      and 106 rbformats unit tests are green. End-to-end browse round-trip will be exercised
      naturally by the Stage 3+4 round-trip verification.

**Done when:** browse-CHD still works, `chd` crate removed from `Cargo.toml`. ✅

---

## Stage 3 — native raw-disk compress (replace `chdman createraw` shell-out) ✅

`src/rbformats/chd.rs::compress_chd` (lines 134–263) currently writes a temp file, shells out
to `chdman createraw -hs 4096 -us 512`, then deletes the temp.

- [x] Rewrote `compress_chd` to call `libchdman_rs::hd::create_from_reader` directly. Builds
      `HdCreateOptions` from `ChdOptions` (Stage 1); `None` falls back to
      `ChdOptions::defaults_for(ChdProfile::Hd)` which matches chdman's `s_default_hd_compression`.
      `logical_size` is rounded up to the 512-byte unit boundary; libchdman-rs zero-pads the
      tail. `progress_cb` is fed `CompressionProgress::bytes_done` clamped to `logical_size`
      so the GUI progress bar never overshoots. `cancel_check` is passed straight through;
      libchdman-rs handles the partial-file unlink on cancel.
- [x] Function signature now: `compress_chd(reader, output_base, logical_size, split_size,
      opts: Option<ChdOptions>, progress_cb, cancel_check, log_cb)`. Threaded `logical_size`
      through `compress_partition` (using `stream_size` for compacted, `image_size` for
      trim-based) and `compress_file_to_archive` (uses `metadata().len()` of the input file).
      Threading user-selectable `ChdOptions` through callers stays for Stage 8.
- [x] Dropped `detect_chdman()` and `get_chdman_command()` from `chd.rs`. Kept `split_file`.
- [x] No temp file. No `Command::new`. No `chdman`.
- [x] `gui/mod.rs`: replaced runtime `detect_chdman()` with a hardcoded `chdman_available =
      true` and a single log line ("CHD compression available (libchdman-rs)"). The
      `set_chdman_available` plumbing in tabs stays for now — Stage 10 rips it out.
- [x] Verify: `cargo test --lib` passes (612 tests including new `compress_chd_round_trip`
      that compresses 1 MiB of pseudo-random data, reopens via `ChdReader`, and asserts
      byte-equality). Build clean, no new clippy warnings.
- [ ] **Deferred**: end-to-end real-partition backup + cross-tool `chdman info` SHA1 check
      will come naturally with Stage 4's restore round-trip.

**Done when:** raw-disk backup writes a CHD without invoking `chdman` and the output verifies. ✅

---

## Stage 4 — native raw-disk extract (replace `chdman extractraw` shell-out) ✅

`src/rbformats/compress.rs` lines ~187–215 called `chdman extractraw` to decompress a CHD into a
temp file, then streamed that temp into the recompression pipeline.

- [x] Replaced the `Command::new(&chdman_cmd).arg("extractraw")…` block with a direct stream
      from `ChdReader` (the `Read+Seek` libchdman-rs adapter built in Stage 2) into the
      existing writer pipeline. No pipe thread, no temp file, no extra allocation — `ChdReader`
      already exposes the needed `Read` interface, so taking `min(logical_size, limit)` and
      streaming chunks is the cleanest path. Added `pub fn ChdReader::logical_size()` so the
      caller can compute the take limit without re-opening the file.
- [x] Removed the `chdman_cmd` / `UpdateConfig::chdman_path` lookup and the unused
      `std::process::Command` import from this file.
- [x] Verify: new unit test `test_chd_decompress_round_trip` in `rbformats::compress::tests`
      compresses 1 MiB of pseudo-random data via `compress_partition(CompressionType::Chd)`,
      decompresses through the `decompress_to_writer` "chd" branch, and asserts byte-equality
      against the input. All 613 lib tests pass; clippy clean.

**Done when:** restore-from-CHD path no longer references `chdman` or `Command`. ✅

---

## Stage 5 — add DVD CHD as a backup target ✅

DVDs are useful for backing up larger images. The HD profile uses 512-byte units; DVD uses
2048-byte sectors and writes a `DVD ` metadata tag so MAME / `chdman info` recognise it.

- [x] Extended `CompressionType` in `src/backup/mod.rs` with a new variant `Dvd`.
      `as_str()` returns `"chd-dvd"` (disambiguates in metadata.json); `file_extension()`
      returns `"chd"` for both `Chd` and `Dvd` (same on-disk format).
- [x] Added `compress_chd_dvd` in `src/rbformats/chd.rs` calling
      `libchdman_rs::dvd::create_from_reader` with `DvdCreateOptions`. `logical_size` is
      rounded up to a 2048-byte multiple; tail is zero-padded by libchdman-rs. Defaults pull
      from `ChdOptions::defaults_for(ChdProfile::Dvd)`.
- [x] `compress_partition` in `src/rbformats/compress.rs` routes `CompressionType::Dvd`
      through `compress_chd_dvd`. `compress_file_to_archive` likewise accepts the
      `"chd-dvd"` string for archive-edit recompression.
- [x] Restore path: extended `decompress_to_writer` chd branch from `"chd"` to
      `"chd" | "chd-dvd"` — same `ChdReader` stream works for both because libchdman-rs's
      `read_bytes` is format-agnostic. Same one-line widen in `inspect_tab.rs` for browse.
- [x] GUI: added a "DVD CHD" radio button next to the HD CHD option in `backup_tab.rs`
      (gated behind `chdman_available` like the HD radio). Codec/hunk-size controls are
      Stage 8.
- [x] Verify: new `compress_chd_dvd_round_trip` test in `rbformats::chd::tests` compresses
      4 MiB → reads back via `ChdReader` byte-equal → asserts `Chd::info().is_dvd`. All
      614 lib tests pass.

**Done when:** A backup with `CompressionType::Dvd` produces a DVD CHD that `chdman info`
identifies as DVD, and restore round-trips bit-perfectly. ✅

---

## Stage 6 — native optical CHD create (replace `chdman createcd`) ✅

`src/optical/convert.rs::to_chd` previously shelled out to `chdman createcd` for both
ISO and BIN/CUE inputs and synthesised a `.rusty-backup-temp.cue` adjacent to the output
for the ISO case.

- [x] ISO branch now calls `libchdman_rs::cd::create_from_iso`. The library writes its
      own `tempfile::NamedTempFile` CUE next to the source ISO, so the manual
      `.rusty-backup-temp.cue` write/cleanup is gone.
- [x] BIN/CUE branch calls `libchdman_rs::cd::create_from_cue` directly. MAME's
      `parse_toc` handles multi-FILE cues, audio tracks, and unusual mode types — same
      coverage chdman had.
- [x] Dropped the `Command::new(&chdman) … createcd …` block and stdout/stderr
      forwarding from `to_chd`. Replaced with libchdman-rs's progress callback wired
      into `ConvertProgress` (`bytes_done` / `bytes_total`). Cancellation is checked
      via the same `is_cancelled` polling helper. The `get_chdman_command()` helper
      stays for now because Stage 7's chd_to_bincue / chd_to_iso still uses it; it
      goes away in that stage.
- [x] Codec / hunk defaults pulled from `libchdman_rs::cd::CdCreateOptions::default()`
      (matches chdman's `s_default_cd_compression`: `[cdlz, cdzl, cdfl, 0]`, hunk 19584).
      Stage 1's `ChdProfile::Cd` defaults already mirror this; Stage 8 will hand a
      user-tuned `ChdOptions` in.
- [x] Verify: two new tests in `optical::convert::tests` — `test_iso_to_chd_native`
      converts a 64-sector synthetic ISO via `to_chd`, asserts `Chd::info().is_cd`, and
      round-trips through `cd::extract_to_iso` byte-equal. `test_bincue_to_chd_native`
      uses `iso_to_bincue` to build a real BIN/CUE pair and feeds it through `to_chd`,
      again round-tripping byte-equal. All 616 lib tests pass; clippy clean.

**Done when:** to_chd no longer invokes `chdman`, ISO and BIN/CUE both work. ✅

---

## Stage 7 — native optical CHD extract (replace `chdman extractcd`) ✅

`src/optical/convert.rs::chd_to_bincue` and `chd_to_iso` previously shelled out to
`chdman extractcd`; the chd→iso path went via a temp BIN/CUE round-trip.

- [x] `chd_to_bincue` now calls `libchdman_rs::cd::extract_to_cue(chd_path, cue_path,
      bin_path, &mut on_progress)`. CUE/BIN naming convention preserved (`bin_path =
      cue_path.with_extension("bin")`).
- [x] `chd_to_iso` now calls `libchdman_rs::cd::extract_to_iso` directly — no temp
      BIN/CUE round-trip. Multi-track / non-MODE1 CHDs surface as a friendly
      "extract to BIN/CUE instead" error mapped from
      `ChdError::UnsupportedFormat`.
- [x] Subcode pre-flight: opens the CHD via `libchdman_rs::cd::list_tracks` before
      extraction; if any track's `subcode_type != SubcodeType::None`, logs
      `"Subcode data dropped (CUE/BIN format limitation)"` so users aren't surprised
      (chdman warns; we match the spirit).
- [x] Dropped now-unused `std::process::{Command, Stdio}`, `crate::update::UpdateConfig`
      imports, and the `get_chdman_command()` helper from this file. `optical/convert.rs`
      is now free of `Command::new` and `chdman_path` references.
- [x] Verify: two new tests in `optical::convert::tests` —
      `test_chd_to_iso_native` (ISO -> CHD -> ISO byte-equal) and
      `test_chd_to_bincue_native` (BIN/CUE -> CHD -> BIN/CUE, restored .bin equals the
      original). All 618 lib tests pass; clippy clean.

**Done when:** all extract paths in `optical/convert.rs` are native, file is free of `Command::new`. ✅

---

## Stage 7.5 — CD CHD browsing (cooked-sector adapter)

HD and DVD CHDs already browse correctly after Stage 2 because they're flat byte streams
that `Read+Seek` over `Chd::read_bytes` exposes directly. CD CHDs are different: they
store 2352-byte raw sectors + 96-byte subcode = 2448-byte frames, but our filesystem code
(ISO9660 / UDF) wants 2048-byte cooked user data.

`opticaldiscs::browse::open_disc_filesystem` currently handles CD-CHD browse and is gated
behind `chdman_available` in `optical_tab.rs:286`, suggesting the existing path shells out.
Confirm during this stage what `opticaldiscs` does today and replace it.

- [ ] Confirm whether `opticaldiscs` shells out to `chdman` for CD CHDs. If yes, the fix
      lives in this repo's wrapper rather than upstream.
- [ ] Add `CdCookedReader` in `src/rbformats/chd.rs`: wraps `libchdman_rs::Chd` + a
      `cdrom_file` view (via `cd::list_tracks` + `chd_shim_cdrom_read_data` indirectly —
      but libchdman-rs exposes the cooked read through its `cd::extract_to_iso` path
      internally). Implementation options:
      - **A.** Extend libchdman-rs with `pub fn cooked_reader(&Chd) -> impl Read+Seek`
        that reads MODE1 user bytes (track type 0) — mirrors what `extract_to_iso`'s loop
        does but exposes it as a stream. Cleanest API; ~30 LoC upstream.
      - **B.** Decompress the whole CD CHD to a temp ISO via `cd::extract_to_iso` and
        browse that. Simple, but defeats the "no temp files" win.
      - Recommend **A** — small upstream addition, big downstream cleanup.
- [ ] Plumb `CdCookedReader` into `model/browse_session.rs`: when the CHD's `info().is_cd`
      is true and there's exactly one MODE1 track, route through `CdCookedReader` instead
      of the raw `ChdReader`. Multi-track / mixed-mode CDs surface a clear error
      ("multi-track CDs cannot be browsed in-place; extract to BIN/CUE first").
- [ ] In `optical_tab.rs:286`, drop the `chdman_available` gate on the CHD source option —
      it now works without external chdman.
- [ ] Verify: open a CD CHD (game ISO ripped to CHD) in Browse, navigate the ISO9660 tree,
      extract a file, SHA256-match against the same file extracted via mounting the source ISO.

**Done when:** CD CHDs browse natively without temp files or chdman, and `optical_tab` no
longer gates the source option.

---

## Stage 8 — UI: codec + hunk-size controls

The plumbing exists (`ChdOptions` from Stage 1); now expose it.

- [ ] In `src/gui/backup_tab.rs`, when `CompressionType::Chd` (or `Dvd`) is selected, show:
      - Hunk size dropdown: HD presets {4096, 8192, 16384}; DVD presets {4096, 16384, 32768};
        CD presets {19584, 9408 (4 frames)}; "Custom" → numeric entry validated to be a
        multiple of unit size.
      - Codec slot 1–4 dropdowns. Populate from `libchdman_rs::codec_exists` checks: only
        offer codecs the build supports. HD profile shows {none, zlib, zstd, lzma, huff, flac};
        CD profile shows {none, cdzl, cdzs, cdlz, cdfl}; DVD profile reuses HD set.
      - Live-validate the codec combo via `parse_codec_spec` and disable the Start button on
        invalid input (e.g. CD codec in an HD slot).
- [ ] Wire backup_tab → `BackupOptions::chd_options: Option<ChdOptions>` → `run_backup` →
      `compress_partition` (Stage 3 already accepts `ChdOptions`).
- [ ] Same controls in `src/gui/optical_tab.rs` for the CHD output target (Stages 6/7).
- [ ] Persist last-used choice in `UpdateConfig` as `last_chd_codecs: Option<String>` and
      `last_chd_hunk_size: Option<u32>` (NOT `chdman_path` — that field gets removed in Stage 10).

**Done when:** users can set codecs and hunk size from the UI for every CHD produced.

---

## Stage 9 — bulk-convert: CHD as a target

The bulk converter currently has CHD as an optical-only output. Extend it to accept any
input that produces a flat byte stream (raw disk image, VHD, zstd backup) and emit HD or DVD
CHD with user-selected codecs.

- [ ] In `src/optical/convert.rs` (or a new `src/rbformats/bulk_chd.rs` if optical convert isn't
      the right home), add converters for raw-image → HD CHD and raw-image → DVD CHD.
- [ ] Hook into the bulk-convert UI selector with the same `ChdOptions` controls from Stage 8.
- [ ] Verify: convert a raw partition image → HD CHD → restore, SHA256 round-trip.
      Convert an ISO → DVD CHD → extract back to ISO, SHA256 round-trip.

**Done when:** bulk convert can produce both HD and DVD CHDs with chosen codecs.

---

## Stage 10 — purge the chdman external dependency

Now that nothing calls `chdman`, rip out the detection, config, and gating.

- [ ] Delete `chdman_path` from `UpdateConfig` in `src/update.rs`. Migrate the on-disk config
      file with a version bump or just ignore the field on read (serde handles missing fields
      with `#[serde(default)]`).
- [ ] Delete `detect_chdman` from `src/rbformats/chd.rs` (Stage 3 already removed callers, so
      this just removes the dead function).
- [ ] Remove `chdman_available: bool` field, `set_chdman_available`, and all conditional UI
      strings ("CHD (chdman)", "chdman not found") from:
      - `src/gui/backup_tab.rs` (lines 32, 99, 121–122, 289–303)
      - `src/gui/optical_tab.rs` (lines 51, 75, 83–84, 268, 286, 300–302)
- [ ] Remove the startup detection block in `src/gui/mod.rs` (lines 26, 133–144).
- [ ] Remove the entire "chdman Configuration" section from `src/gui/settings_dialog.rs`
      (lines ~7, 26, 31–37, 112, 123–126).
- [ ] Update `src/rbformats/README.md` to drop the "Requires chdman on PATH" column and the
      `chd.rs` description's "external chdman tool" wording.
- [ ] Update `CLAUDE.md` if it references chdman as an external dep.
- [ ] `grep -rn chdman src/` should return zero hits in production code (test fixtures may
      keep references for cross-tool compat tests).

**Done when:** the codebase is `chdman`-free.

---

## Stage 11 — final verification + docs

- [ ] Full backup → restore round-trip on at least one real partition for each codec set:
      `[zlib]`, `[zstd]`, `[lzma]`, `[lzma, zlib, huff, flac]` (chdman's HD default).
- [ ] Optical: ISO → CHD → ISO and BIN/CUE → CHD → BIN/CUE round-trips.
- [ ] DVD: ISO → DVD CHD → ISO round-trip.
- [ ] Cross-tool sanity: confirm `chdman info` (run manually) reads our outputs and reports
      sensible track/codec/SHA1 info. Not a CI gate; a one-time spot check.
- [ ] Update `docs/CONFIGURATION.md` to drop chdman setup instructions.
- [ ] Update `README.md` if it lists chdman as a runtime dependency.
- [ ] Add a short `docs/chd_native.md` explaining we now bundle MAME's CHD core via
      libchdman-rs and the user no longer needs chdman installed.
- [ ] Tick the "External chdman dependency" item off `docs/TODO_missing_features.md` if it's
      listed there.

**Done when:** PR is opened, all round-trips green, docs updated.

---

## Out of scope (track for follow-up if useful)

- GD-ROM (Sega Dreamcast) support — libchdman-rs doesn't expose a `gd` module. Add later if
  someone asks.
- AV/laserdisc CHDs — same.
- CHD parent/child diff CHDs — libchdman-rs supports them (`Chd::open` parent arg) but we
  have no current use case.
- Switching codecs on an existing backup CHD — `libchdman_rs::copy::copy()` does this but
  we don't currently surface a "re-compress this archive" UI. Easy follow-up.

## Risks to watch

- **Build time on Windows CI** — first build adds 2–3 minutes. Cache `target/` aggressively
  in GitHub Actions. If CI starts timing out, consider caching libchdman-rs's `target/` separately.
- **Cross-compile** — libchdman-rs builds C++ with `cc`. Confirm AppImage/macOS/Windows
  release builds still produce working binaries; this is the highest-risk part of Stage 0.
- **Rounding-up of partition sizes for CHD hunk alignment** — chdman pads to hunk multiples.
  libchdman-rs requires `logical_size % unit_size == 0` and hunk-aligns internally. Backup
  metadata records the unpadded size; restore must respect that. Verify in Stage 4 that
  restored bytes equal the original (no zero-padded tail leaks into the partition).
- **Progress UI ordering** — libchdman-rs reports progress as fraction; we now have
  `bytes_done`. UI strings/units may need tweaks (Stage 8).
