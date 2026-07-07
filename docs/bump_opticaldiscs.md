# Bump `opticaldiscs` to 0.5.0 (breaking change + latent-bug fix)

> **Task prompt** for a Claude Code session working in this repo
> (`rusty-backup`). Self-contained: follow it top to bottom, then run the
> verification steps before reporting done. All line numbers below are
> approximate — `rg` for the symbols rather than trusting them.

## Goal

Bump the `opticaldiscs` dependency from `0.4.2` to **`0.5`** and update the
optical-disc browse/extract code for the one breaking API change. This is
gated behind the `optical` feature, so build and test **with `--features optical`**
or the breakage won't surface.

> Note: rusty-backup pins `opticaldiscs = "0.4.2"`, i.e. `^0.4.2` (`<0.5.0`), so
> nothing is broken today — this is a deliberate forward bump.

## Why bump (two reasons, both real)

1. **Fixes a latent extraction bug in *this* repo** (see "The bug" below): the
   optical AppleDouble / MacBinary export currently corrupts the Finder
   type/creator for any non-printable 4-byte code, because it rebuilds those
   bytes from opticaldiscs' *display string*. 0.5.0 exposes the raw bytes, which
   fixes it and lets us delete the `fourcc` reparse helpers.
2. **Decoding correctness for optical HFS browsing**: 0.5.0 fixes a shifted Mac OS
   Roman table (bytes ≥ 0x9B mis-decoded: `ü`→`†`, `©`→`™`, etc.) and makes HFS+
   UTF-16 name decoding lenient (a single bad code unit no longer drops the whole
   entry from the listing). This only affects the *opticaldiscs-backed* optical
   path — rusty-backup's own `src/fs/hfs*` code already has the correct table.

## The breaking API change

`FileEntry::type_code` / `FileEntry::creator_code` changed type:

```text
        before (0.4.x):  Option<String>          // display string, lossy for high-bit codes
        after  (0.5.0):  Option<[u8; 4]>          // raw Finder bytes, verbatim
```

New in 0.5.0:

- `FileEntry::type_code_string()` / `creator_code_string()` → `Option<String>` —
  the *exact* old display rendering (`"TEXT"`, or `"0x12345678"` for
  non-printable codes). Use these wherever you only display the code.
- `FileEntry::finder_flags: Option<u16>` — `FInfo.fdFlags` (`isAlias 0x8000`,
  `isInvisible 0x4000`, `hasBundle 0x2000`, `hasCustomIcon 0x0400`). Optional to use.

Nothing else rusty-backup imports from opticaldiscs changed
(`DiscFormat`, `DiscImageInfo`, `SectorReader`, `BinCueSectorReader`,
`bincue::parse_cue_tracks`, `browse::*`, `drives::list_drives`, the `drives`
feature) — so the break is localized to the `type_code`/`creator_code` reads.

## The bug (why the extraction edits matter)

`src/cli/verbs/optical.rs:fourcc(Option<&str>)` and
`src/optical/browse_view.rs:fourcc_bytes(&str)` both just copy the first 4 bytes
of opticaldiscs' display string into a `[u8; 4]`. For a high-bit creator code
like Prince of Persia's `50 6F C4 50`, opticaldiscs 0.4.x returns the hex
fallback string `"0x506FC450"`, so `fourcc` produces `b"0x50"` —
**a wrong type/creator written into the AppleDouble/MacBinary.** Printable codes
(`"TEXT"`) happen to round-trip; non-printable ones are silently corrupted.

With 0.5.0 the raw bytes are available directly, so the `fourcc`/`fourcc_bytes`
reparse is both unnecessary and the source of the bug — delete it and use
`entry.type_code` straight.

## Required edits

### 1. `Cargo.toml`

```toml
opticaldiscs = { version = "0.5", features = ["drives"], optional = true }
```

> If `opticaldiscs` 0.5.0 isn't on crates.io yet when you do this, use the local
> checkout for testing and flip back to the crates.io version once published:
>
> ```toml
> opticaldiscs = { path = "../opticaldiscs-rs", features = ["drives"], optional = true }
> ```

Then run `cargo tree -i libchdman-rs` and confirm opticaldiscs 0.5.0 and
rusty-backup's direct `libchdman-rs` dependency still resolve to a **single**
copy; reconcile the pin if they diverge.

### 2. Display sites → use `*_string()`

These format the code with `{tc}` / `format!("Type: {tc}")`, which no longer
works on `[u8; 4]`.

- `src/cli/verbs/optical.rs` (~396–401), tree listing:

  ```rust
  if let Some(tc) = child.type_code_string() {
      out.push_str(&format!("  {tc}"));
      if let Some(cc) = child.creator_code_string() {
          out.push_str(&format!("/{cc}"));
      }
  }
  ```

- `src/optical/browse_view.rs` (~366–371), file-info header:

  ```rust
  if let Some(tc) = entry.type_code_string() {
      ui.label(format!("Type: {tc}"));
  }
  if let Some(cc) = entry.creator_code_string() {
      ui.label(format!("Creator: {cc}"));
  }
  ```

  Apply the same change to any other display spot (there's another around
  ~687–689 in `browse_view.rs`).

### 3. Extraction sites → use raw bytes, drop `fourcc`

Everywhere a `[u8; 4]` is currently built via `fourcc(entry.type_code.as_deref())`
or `entry.type_code.as_ref().map(|s| fourcc_bytes(s)).unwrap_or([0; 4])`, replace
with the raw field directly (it's already `[u8; 4]`):

```rust
let type_code = entry.type_code.unwrap_or([0; 4]);
let creator_code = entry.creator_code.unwrap_or([0; 4]);
```

then pass `&type_code` / `&creator_code` to `resource_fork::build_appledouble`
and `resource_fork::build_macbinary` exactly as before (both already take
`&[u8; 4]`). Sites to fix:

- `src/cli/verbs/optical.rs` (~531–532 MacBinary, ~553–554 AppleDouble/etc.)
- `src/optical/browse_view.rs` (~771–784 MacBinary, ~812–825 AppleDouble)

### 4. Delete the now-dead helpers

Once the extraction sites use raw bytes, these become unused — remove them (and
fix any `dead_code` warning):

- `fn fourcc(s: Option<&str>) -> [u8; 4]` in `src/cli/verbs/optical.rs` (~600)
- `fn fourcc_bytes(s: &str) -> [u8; 4]` in `src/optical/browse_view.rs` (~877)

Grep first to be sure nothing else calls them: `rg -n 'fourcc(_bytes)?\(' src`.
(There are unrelated `fourcc_to_string` in `rbformats/chd.rs` and
`fourcc_plausible` in `macarchive/macbinary.rs` — leave those alone.)

### 5. Sweep for stragglers

```sh
rg -n 'type_code|creator_code|finder_flags' src
```

Fix any remaining `.as_deref()` / `{..}`-format / `&String` assumptions the same
way, and update tests asserting the old `Option<String>` shape.

## Optional follow-ups (only if clearly worthwhile)

- Use `entry.finder_flags` to hide `isInvisible (0x4000)` files in the browse
  view, or annotate aliases (`isAlias 0x8000`).

## Verification (run all; report output)

```sh
cargo build --features optical
cargo test  --features optical
cargo clippy --features optical -- -D warnings
cargo fmt --check
cargo tree -i libchdman-rs        # expect a single version
```

If you have (or can make) a Mac HFS optical image with a file whose creator has a
high-bit byte, extract it as AppleDouble and confirm the `._name` file now
carries the correct 4 type/creator bytes (previously it'd be `b"0x.."`).

## Done criteria

- `Cargo.toml` → `opticaldiscs = "0.5"` (crates.io), or the path dep if 0.5.0
  isn't published yet, with a note to flip it back.
- All `type_code`/`creator_code` reads use raw bytes (extraction) or
  `*_string()` (display); `fourcc` / `fourcc_bytes` deleted.
- All five verification commands clean; the optical test suite passes.
