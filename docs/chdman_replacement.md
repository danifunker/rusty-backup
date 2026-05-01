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

## Stage 2 — replace `ChdReader` with libchdman-rs (read-side swap)

Lowest-risk change. `ChdReader` in `src/rbformats/chd.rs` (lines 14–107) wraps the old `chd` crate.
Browse/inspect already routes through it via `src/gui/browse_view.rs::create_filesystem`.

- [ ] In `src/rbformats/chd.rs`, replace `ChdReader`'s internals with a thin wrapper around
      `libchdman_rs::Chd` + `read_bytes`. Keep the public API (`pub fn open`, `Read`, `Seek`)
      identical so callers don't change.
      - libchdman-rs has its own `ChdReader` in `enhancements.rs`; consider re-exporting it
        directly and dropping the wrapper, **but** the rusty-backup type is constructed from
        `&Path` while libchdman-rs's takes `&Chd`, so a small adapter is cleaner.
- [ ] Delete the now-unused `chd` crate references and any imports.
- [ ] Verify: open a known-good CHD (`~/Documents/HD10_512 Quadra_System7OldNew.hda` if it's
      a CHD, otherwise any test fixture), browse it via `BrowseView`, confirm directory listing
      and file reads match pre-change output byte-for-byte (use a SHA256 spot check on a known file).

**Done when:** browse-CHD still works, `chd` crate removed from `Cargo.toml`.

---

## Stage 3 — native raw-disk compress (replace `chdman createraw` shell-out)

`src/rbformats/chd.rs::compress_chd` (lines 134–263) currently writes a temp file, shells out
to `chdman createraw -hs 4096 -us 512`, then deletes the temp.

- [ ] Rewrite `compress_chd` to:
  1. Wrap the `&mut impl Read` source in `libchdman_rs::hd::create_from_reader`.
  2. Build `HdCreateOptions` from the new `ChdOptions` (Stage 1) — hunk size, codecs.
       Default to `[CHD_CODEC_LZMA, CHD_CODEC_ZLIB, CHD_CODEC_HUFF, CHD_CODEC_FLAC]` to match
       chdman's `s_default_hd_compression`.
  3. `logical_size` = caller-provided partition size. The reader will be zero-padded by libchdman-rs.
  4. Hook `progress_cb` into the `&mut dyn FnMut(CompressionProgress)` callback —
       use `bytes_done` (not the ratio).
  5. Hook `cancel_check` into the `&dyn Fn() -> bool` callback. libchdman-rs deletes the partial
       file on cancel; we no longer need the manual `fs::remove_file(&temp_path)` cleanup.
- [ ] Update the function signature: take `ChdOptions` (or accept `Option<ChdOptions>` and use
      defaults). Threading through callers will happen in Stage 8.
- [ ] Drop `detect_chdman()` and `get_chdman_command()` from `chd.rs` — both unused after this stage.
      Keep the splitting helper (`split_file`) — still needed if `split_size` is set.
- [ ] No temp file. No `Command`. No `chdman`.
- [ ] Verify: backup a small partition (~50 MB), confirm output CHD opens with `chdman info`
      AND with libchdman-rs (`Chd::open` + `verify()`), confirm SHA1 matches. Then restore
      it (Stage 4 not done yet, so use chdman temporarily for the readback only — or stage this
      verification with Stage 4 below).

**Done when:** raw-disk backup writes a CHD without invoking `chdman` and the output verifies.

---

## Stage 4 — native raw-disk extract (replace `chdman extractraw` shell-out)

`src/rbformats/compress.rs` lines ~187–215 call `chdman extractraw` to decompress a CHD into a
temp file, then stream that temp into the recompression pipeline.

- [ ] Replace the `Command::new(&chdman_cmd).arg("extractraw")…` block with
      `libchdman_rs::hd::extract_to_writer` writing into the streaming pipeline directly
      (no temp file). The current code reads the temp file as a `BufReader<File>`; the
      replacement should expose a `Read` adapter over libchdman-rs's `extract_to_writer` —
      easiest implementation: spawn a thread with `pipe::pipe()`, write into the writer end
      from libchdman-rs, hand the reader end to the existing pipeline.
      - Alternative if pipe-thread feels heavy: keep a temp file but produce it via libchdman-rs
        (still no chdman). Trade: same disk usage as today, simpler code. Pick one — pipe-thread
        is the cleaner end state.
- [ ] Remove the local `chdman_cmd` lookup and the `chdman_path` reference in this file.
- [ ] Verify: round-trip — backup a 50 MB partition (Stage 3), then restore through this
      path, SHA256 the restored bytes, confirm match against original.

**Done when:** restore-from-CHD path no longer references `chdman` or `Command`.

---

## Stage 5 — add DVD CHD as a backup target

DVDs are useful for backing up larger images (CHD's HD profile maxes the same way but DVD
hunk size + codecs are tuned for 2048-byte sectors). Adding now lets the GUI reuse it later.

- [ ] Extend `CompressionType` in `src/backup/mod.rs` with a new variant `Dvd` (file extension
      `.chd`, `as_str()` returning `"chd-dvd"` to disambiguate in metadata).
      - **Decision point:** Do we want DVD as a separate `CompressionType`, or as a flag on
        `CompressionType::Chd`? Separate variant is clearer in metadata; flag avoids enum
        sprawl. Recommend: separate variant. Document in metadata.json schema notes.
- [ ] In `src/rbformats/chd.rs`, add `compress_chd_dvd(...)` calling
      `libchdman_rs::dvd::create_from_reader` with `DvdCreateOptions`.
- [ ] In `src/rbformats/compress.rs`, route `CompressionType::Dvd` through the new function.
- [ ] In the restore path (Stage 4 covers HD; DVD uses the same `Chd::read_bytes` route since
      both are flat sector streams), confirm the same extract works for DVD CHDs without
      branching.

**Done when:** A backup with `CompressionType::Dvd` produces a DVD CHD that `chdman info`
identifies as DVD, and restore round-trips bit-perfectly.

---

## Stage 6 — native optical CHD create (replace `chdman createcd`)

`src/optical/convert.rs::to_chd` (lines 273–367) handles ISO and BIN/CUE → CHD. Currently shells
out for both, with a temp CUE for the ISO case.

- [ ] Replace ISO branch with `libchdman_rs::cd::create_from_iso(iso_path, output, opts, …)`.
      libchdman-rs writes its temp CUE adjacent to the ISO via `tempfile::NamedTempFile` so the
      manual `.rusty-backup-temp.cue` synthesis goes away.
- [ ] Replace BIN/CUE branch with `libchdman_rs::cd::create_from_cue(cue_path, output, opts, …)`.
      MAME's `parse_toc` (which libchdman-rs delegates to) handles multi-FILE cues, audio
      tracks, and unusual mode types — same support matrix as chdman.
- [ ] Drop the `Command::new(&chdman)` block, the `get_chdman_command()` helper, and the
      stdout/stderr forwarding (libchdman-rs provides progress directly).
- [ ] Codec defaults: `[CHD_CODEC_CD_LZMA, CHD_CODEC_CD_ZLIB, 0, 0]` for backward parity.
      Hunk size defaults to 19584. Both come from `ChdOptions::defaults_for(ChdProfile::Cd)`.
- [ ] Pipe progress callbacks into `ConvertProgress`. The existing UI displays a fraction;
      use `bytes_done / bytes_total`.
- [ ] Verify: convert a known ISO and a known BIN/CUE to CHD. Open both with `chdman info`
      AND libchdman-rs, confirm track count and SHA1 match what chdman would have produced
      (this last bit is the byte-for-byte parity check — easy to cross-check by also running
      chdman against the same inputs once and diffing).

**Done when:** to_chd no longer invokes `chdman`, ISO and BIN/CUE both work.

---

## Stage 7 — native optical CHD extract (replace `chdman extractcd`)

`src/optical/convert.rs::chd_to_bincue` (lines 370–430) and `chd_to_iso` (lines 433+) shell out.

- [ ] Replace `chd_to_bincue` with `libchdman_rs::cd::extract_to_cue(chd_path, cue_path, bin_path, &mut |bytes| ...)`.
- [ ] Replace `chd_to_iso` with `libchdman_rs::cd::extract_to_iso(chd_path, iso_path, ...)`.
      The library returns an error on multi-track / non-MODE1 CHDs — propagate that as the
      existing user-facing error suggesting CUE extraction instead.
- [ ] Drop the temp BIN/CUE round-trip in the chd→iso path (libchdman-rs's `extract_to_iso`
      goes direct from CHD to ISO without an intermediate file).
- [ ] If a stored CHD has subcode, libchdman-rs silently drops it (CUE format can't carry it).
      Add a one-line log: `"Subcode data dropped (CUE/BIN format limitation)"` so users aren't
      surprised — match behavior with chdman, which warns.
- [ ] Verify: extract one of the CHDs from Stage 6 back to BIN/CUE and to ISO; SHA256 the
      bytes against the original input. Round-trip must be bit-perfect.

**Done when:** all extract paths in `optical/convert.rs` are native, file is free of `Command::new`.

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
