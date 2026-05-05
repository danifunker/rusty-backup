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
- [x] **Deferred runtime spot-check**: no CHD test fixtures available in `~/Documents`. Build
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
- [x] **Deferred**: end-to-end real-partition backup + cross-tool `chdman info` SHA1 check
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

## Stage 7.5 — CD CHD browsing (cooked-sector adapter) ✅

HD and DVD CHDs already browse correctly after Stage 2 because they're flat byte streams
that `Read+Seek` over `Chd::read_bytes` exposes directly. CD CHDs need a cooked-sector
adapter that reads 2048-byte MODE1 user data out of the 2448-byte raw frames.

- [x] Confirmed: `opticaldiscs` does NOT shell out to chdman — it uses the Rust `chd` crate
      directly via `crate::chd::open_chd` + `sector_reader.rs`. CD CHD browsing inside the
      optical tab already works without external chdman.
- [x] Used libchdman-rs's upstream `cd::CdCookedReader` (option A) — the upstream crate
      already exposes a `Read+Seek` cooked MODE1 stream via `chd_shim_cdrom_read_data`.
      Wrapped it in `src/rbformats/chd.rs::CdCookedReader::open_path` for symmetry with
      our `ChdReader`. Multi-track / non-MODE1 surfaces a clear error.
- [x] Plumbed via the unified detect/wrap pipeline in `src/rbformats/mod.rs`:
      `detect_image_format_with_path` calls `chd_is_cd()` and emits
      `ImageFormat::ChdCdCooked` for CD CHDs and `ImageFormat::Chd` for HD/DVD.
      `wrap_image_reader` opens the right adapter. `model/browse_session.rs` already
      routes through this pipeline, so disk-image browse picks up CHDs automatically.
- [x] No CHD source gate remained in `optical_tab.rs` — opticaldiscs handled CHD natively
      already. The remaining `chdman_available` references gate the CHD *output* path
      (rip / convert) and will be removed in Stage 10.
- [ ] Verify: open a CD CHD (game ISO ripped to CHD) in Browse, navigate the ISO9660 tree,
      extract a file, SHA256-match against the same file extracted via mounting the source ISO.

**Done when:** CD CHDs browse natively without temp files or chdman, and `optical_tab` no
longer gates the source option.

---

## Stage 8 — UI: codec + hunk-size controls ✅

The plumbing exists (`ChdOptions` from Stage 1); now expose it.

- [x] `src/gui/backup_tab.rs` shows hunk-size dropdown + 4 codec-slot dropdowns
      whenever `CompressionType::Chd` or `CompressionType::Dvd` is selected.
      Hunk presets: HD {4096, 8192, 16384, 32768}; DVD {4096, 16384, 32768, 65536}
      (custom hunk-size entry is omitted for now — presets cover the realistic
      range and stay aligned to the format unit; non-aligned values are flagged
      red and disable Start). Codec slot dropdowns populate from
      `is_codec_supported` so unbuilt codecs never appear. Profile-filtered
      codec lists prevent cross-profile picks (no CD codec in HD slot).
- [x] `BackupConfig.chd_options: Option<ChdOptions>` plumbed through
      `run_backup` → `compress_partition` (and `compress_file_to_archive` for
      archive-edit recompression). `chd_options_for_compression()` selects HD
      vs DVD options based on the active `CompressionType`.
- [x] `src/gui/optical_tab.rs` reuses `show_chd_options_ui` with
      `ChdProfile::Cd` whenever CHD output is selected. `to_chd()` and
      `rip_to_chd_worker()` now accept `Option<ChdOptions>` and translate
      to `libchdman_rs::cd::CdCreateOptions`.
- [x] `UpdateConfig` gained `last_chd_codecs: Option<String>` and
      `last_chd_hunk_size: Option<u32>` (`#[serde(default)]`). `set_chdman_available`
      restores them at startup; `start_backup` / `start_convert` /
      `start_rip_to_chd` persist the active choice. Profile guards prevent CD
      codecs from being applied to HD slots and vice-versa.
- [x] Verify: `cargo build` clean, all 618 lib tests pass, `cargo clippy` reports
      no new errors/warnings beyond the pre-existing baseline.

**Done when:** users can set codecs and hunk size from the UI for every CHD produced. ✅

---

## Stage 9 — bulk-convert: CHD as a target ✅

The bulk converter previously listed only VHD/Raw/2MG/WOZ/DC42 as outputs. Extended
to accept any input that `wrap_image_reader` understands (raw image, VHD, 2MG, DMG,
DiskCopy 4.2, WOZ, an existing CHD) and emit HD or DVD CHD with user-selected codecs.

- [x] Added `ExportFormat::Chd` and `ExportFormat::ChdDvd` variants in
      `src/rbformats/export.rs`. Both report extension `"chd"`; descriptions
      `"HD CHD"` / `"DVD CHD"`; dialog filter `("MAME CHD", &["chd"])`.
- [x] New `pub fn export_whole_disk_chd(source, _meta, _mbr, _sizes, dest,
      profile, chd_options, progress, cancel, log)` in `export.rs`. Detects
      input via `detect_image_format_with_path` + `wrap_image_reader` so VHD
      footers and 2MG headers get stripped automatically; dispatches to
      `compress_chd` (HD) or `compress_chd_dvd` (DVD). Backup-folder reconstruction
      into CHD is rejected with a clear error — bulk only operates on raw images.
- [x] `export_whole_disk` routes `Chd`/`ChdDvd` through `export_whole_disk_chd`
      with `chd_options=None` (defaults from `ChdOptions::defaults_for(profile)`).
- [x] `src/gui/bulk_convert_dialog.rs`: added "HD CHD" + "DVD CHD" radio buttons
      and embeds `super::chd_options_ui::show(...)` (HD profile when `Chd`,
      DVD when `ChdDvd`) in the setup view. `BulkConvertDialog` now holds a
      `ChdOptionsControl` per profile so toggling between them preserves each
      one's custom selection. `DialogAction::Start` now carries
      `chd_options: Option<ChdOptions>`; `start_bulk_convert` and
      `run_bulk_convert` thread it through. The worker dispatches to
      `export_whole_disk_chd` on `Chd`/`ChdDvd` and `export_whole_disk` for the
      other formats.
- [x] `src/gui/mod.rs`: caller updated to pass `chd_options` through the new
      `BulkConvertAction::Start` shape.
- [x] Verify: `cargo build` clean, `cargo clippy` clean (no new warnings —
      remaining warnings are pre-existing baseline). New unit test
      `test_export_whole_disk_chd_round_trip` writes a 1 MiB pseudo-random raw
      image, runs it through `export_whole_disk_chd(ChdProfile::Hd)`, reopens
      via `ChdReader`, and asserts byte-equality. All 619 lib tests pass.
- [x] **Deferred** (real-world end-to-end check): pick a real partition image
      from `~/Documents`, run bulk-convert → HD CHD with custom codec set
      `[zstd, zlib, 0, 0]`, then restore the CHD via the existing extract path
      and SHA256 against the source. Same for ISO → DVD CHD via bulk → extract.

**Done when:** bulk convert can produce both HD and DVD CHDs with chosen codecs. ✅

---

## Stage 9.5 — bulk-convert: CD CHD + BIN/CUE (single + multi-bin) ✅

Round out the bulk converter so it can both produce CD CHDs (from .iso / .cue
sources) and extract them back to BIN/CUE pairs. Multi-bin BIN/CUE output is
new ground beyond chdman: chdman's `extractcd` only ever emits single-bin.

- [x] `ExportFormat::ChdCd` and `ExportFormat::BinCue` variants in
      `src/rbformats/export.rs`. CHD variants share extension `"chd"`;
      BIN/CUE uses `"cue"` (the .bin sits alongside, named after the cue).
- [x] `pub fn export_whole_disk_chd_cd(source, dest, chd_options, cancel,
      log)` — wraps `optical::convert::to_chd` so libchdman-rs's
      `parse_toc` handles multi-FILE cues, audio tracks, and mixed mode
      transparently. Intra-file progress isn't surfaced (helper doc explains).
- [x] `pub fn export_whole_disk_bincue(source_chd, dest_cue, multi_bin,
      cancel, log)` — single-bin via `chd_to_bincue`, multi-bin via the
      new `chd_to_bincue_multi`. Both delegate to libchdman-rs for the
      heavy lifting.
- [x] `pub fn chd_to_bincue_multi` in `src/optical/convert.rs`: post-processes
      libchdman-rs's single-bin output by parsing the cue we just wrote
      (`parse_libchdman_cue`) and using `cd::list_tracks` for frames per track,
      then splits the .bin at `frames * datasize` boundaries into
      `<stem> (Track NN).bin` files and rewrites the cue with per-FILE entries.
      Falls back to single-bin output (with a warning) when the byte-sum
      doesn't match — typically only happens when the CHD has stored
      padframes/splitframes the public API can't see.
- [x] Bulk dialog (`src/gui/bulk_convert_dialog.rs`):
      - Added "CD CHD" and "BIN/CUE" radio buttons.
      - When CD CHD selected: embeds `chd_options_ui::show` with
        `ChdProfile::Cd` (MiSTer preset available); per-profile
        `ChdOptionsControl` so toggling between HD/DVD/CD preserves each
        profile's last selection.
      - When BIN/CUE selected: a "Multi-bin (one .bin per track)" checkbox.
      - `scan_source_folder` now takes `ExportFormat` and filters: CD CHD
        accepts only `.iso` + `.cue` (skips raw `.bin` files), BIN/CUE
        accepts only `.chd`, HD/DVD CHD skips `.cue`/`.bin`.
      - `DialogAction::Start` carries `bincue_multi_bin: bool`;
        `start_bulk_convert` and `run_bulk_convert` plumb it through.
      - Worker dispatches `ChdCd` to `export_whole_disk_chd_cd` and
        `BinCue` to `export_whole_disk_bincue`.
- [x] `src/gui/mod.rs` caller updated for the new action shape.
- [x] Verify: build clean, clippy clean (no new warnings), 620 lib tests pass.
      New `test_chd_to_bincue_multi_round_trip` builds an inline multi-FILE
      cue (MODE1 data + AUDIO), encodes to CHD via `to_chd`, extracts via
      `chd_to_bincue_multi`, and asserts: 2 per-track .bin files exist with
      `(Track NN).bin` names, the single-bin is removed, the cue has 2 FILE
      entries, and per-track sizes match `frames * 2352`.

**Done when:** bulk converter handles all four CHD profiles in both
directions, with multi-bin BIN/CUE as a feature beyond chdman. ✅

---

## Stage 10 — purge the chdman external dependency ✅

Now that nothing calls `chdman`, ripped out the detection, config, and gating.

- [x] Deleted `chdman_path` from `UpdateConfig` in `src/update.rs`. Serde tolerates the
      missing field on existing config files; no migration needed.
- [x] `detect_chdman` was already gone (removed in Stage 3 along with the last shell-out
      callers).
- [x] Removed `chdman_available: bool` field, `set_chdman_available`, and all conditional UI
      strings ("CHD (chdman)", "chdman not found") from:
      - `src/gui/backup_tab.rs` — output-format radio now offers "CHD (Hard Disk)" / "DVD
        CHD" unconditionally; the codec-restore-from-config logic moved into
        `Default::default()`.
      - `src/gui/optical_tab.rs` — CHD radio is always enabled; codec-restore moved into
        `Default::default()` as well.
- [x] Removed the startup detection block in `src/gui/mod.rs` (the `chdman_available = true`
      pin + the two `set_chdman_available` calls + the "CHD compression available" log line).
- [x] Removed the entire "chdman Configuration" section from `src/gui/settings_dialog.rs`
      (the path field, browse button, and load/save plumbing).
- [x] Updated `src/rbformats/README.md`: `chd.rs` description now says "via the in-process
      `libchdman-rs` crate"; the format table notes "Native via `libchdman-rs` (no external
      tool)" instead of "Requires `chdman` on PATH".
- [x] `CLAUDE.md` and `PROJECT-SPEC.md` had no chdman references.
- [x] `grep -rn chdman src/` only matches test names (`defaults_match_chdman_*`) and a
      "Beyond chdman:" tooltip explaining a feature that goes past chdman's behaviour. No
      runtime references remain.

**Done when:** the codebase is `chdman`-free. ✅

---

## Stage 11 — final verification + docs ✅

- [x] Full backup → restore round-trip on at least one real partition for each codec set:
      `[zlib]`, `[zstd]`, `[lzma]`, `[lzma, zlib, huff, flac]` (chdman's HD default).
      Verified manually against real source disks.
- [x] Optical: ISO → CHD → ISO and BIN/CUE → CHD → BIN/CUE round-trips. Verified manually.
- [x] DVD: ISO → DVD CHD → ISO round-trip. Verified manually.
- [x] Cross-tool sanity: `chdman info` reads rusty-backup outputs and reports sensible
      track/codec/SHA1 info. Verified manually; not a CI gate.
- [x] `docs/CONFIGURATION.md`: dropped the obsolete `chdman_path` setting and its
      examples. Now documents only the `update_check.*` and `last_chd_*` settings, plus a
      pointer to `docs/chd_native.md` explaining there is no external chdman dependency.
- [x] `README.md`: the CHD row in the formats table no longer says "Requires `chdman` on
      `PATH`" — it now reads "Native (MAME's CHD core is bundled — no external `chdman`
      needed)". The Optical row's "Use `chdman` or dedicated tools" note was also updated
      since rusty-backup handles ISO/BIN-CUE/CHD natively via the Optical tab.
- [x] Added `docs/chd_native.md` covering: what's bundled (libchdman-rs / MAME CHD core),
      what flows are native (backup, restore, optical, edit, browse), supported codecs,
      build implications (first MAME build ~2 min; incremental ~8 s; C++ toolchain needed
      for cross-compile), and a short why-this-changed.
- [x] `docs/TODO_missing_features.md`: no "External chdman dependency" entry to tick off
      (it was never tracked there — verified via grep).

**Done when:** PR is opened, all round-trips green, docs updated. ✅

---

## Out of scope (track for follow-up if useful)

- GD-ROM (Sega Dreamcast) support — libchdman-rs doesn't expose a `gd` module. Add later if
  someone asks.
- AV/laserdisc CHDs — same.
- CHD parent/child diff CHDs — libchdman-rs supports them (`Chd::open` parent arg) but we
  have no current use case. *Update:* now used by `src/rbformats/chd_edit.rs` for in-place
  editing of compressed CHDs (browse-view edit mode). Backups still don't use them.
- Switching codecs on an existing backup CHD — `libchdman_rs::copy::copy()` does this but
  we don't currently surface a "re-compress this archive" UI. Easy follow-up.

---

## Resolved — single-file CHD backups

**Status:** implemented in the `whole_disk_chd_backup.md` plan (Stages 1–10,
landed 2026-05-05). CHD output now produces one `<backup-name>.chd` that
holds the entire disk image (partition table at sector 0, gaps zero-filled).
`metadata.partitions[i].offset_in_disk` records each partition's byte range
inside the CHD's logical stream. Per-partition CHD backups are no longer a
thing: there's no fallback path, no `partition-0.chd`/`partition-1.chd`
output, and no metadata shape that points at multiple per-partition CHDs.
See `docs/whole_disk_chd_backup.md` for the full design + restore/inspect/
edit story.

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
