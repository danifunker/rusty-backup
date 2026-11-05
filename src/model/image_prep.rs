//! Turn a picked image into a flat, seekable file the backup and restore paths can address by offset.
//!
//! Gzip-wrapped Amiga images and the floppy containers that store sectors
//! out of order are decoded to a tempfile; everything else passes through.
//! Decoding a `.hdz` can mean a whole hard disk, so the GUI runs it as a
//! [`crate::model::job::Job`] when [`needs_decoding`] says so.

/// Whether [`prepare_disk_image_path`] would decode `path` (a cheap extension check).
pub fn needs_decoding(path: &std::path::Path, decode_floppy_containers: bool) -> bool {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_ascii_lowercase());
    decode_floppy_containers
        && matches!(
            ext.as_deref(),
            Some("adz" | "hdz" | "msa" | "d88" | "xdf" | "hdm" | "dim")
        )
}

/// Prepare a disk image path for file-based access by downstream GUI
/// worker code. Returns a path to a raw, seekable image — possibly the
/// original (no work needed) or a tempfile.
///
/// Handles three wrapper formats whose raw stream isn't seekable / flat:
/// - `.adz` / `.hdz` — gzip-decompressed to a tempfile (gzip isn't
///   seekable).
/// - `.msa` — Atari ST Magic Shadow Archiver, decoded to a flat `.st`
///   tempfile.
/// - `.d88` — Sharp Japanese-emulator floppy container (X68000 / PC-88 /
///   PC-98 / MSX / FM-7), decoded to a flat raw-sector tempfile.
///
/// Everything else — including `.gho`/`.ghs`, `.imz`, `.chd` — passes
/// through unchanged, since the inspect worker opens those via
/// `gui::open_disk_image_for_reading` (streaming `Read + Seek` readers,
/// no tempfile).
///
/// The output file lives inside a fresh `tempfile::tempdir`; callers
/// must hold the returned `TempDir` (second tuple element) for the
/// duration they need the materialized file.
pub fn prepare_disk_image_path(
    path: &std::path::Path,
    decode_floppy_containers: bool,
) -> std::io::Result<(std::path::PathBuf, Option<tempfile::TempDir>)> {
    prepare_disk_image_path_reporting(path, decode_floppy_containers, &|_, _| {})
}

/// [`prepare_disk_image_path`], reporting `(compressed bytes read, compressed total)` while it gunzips.
pub fn prepare_disk_image_path_reporting(
    path: &std::path::Path,
    decode_floppy_containers: bool,
    progress: &dyn Fn(u64, u64),
) -> std::io::Result<(std::path::PathBuf, Option<tempfile::TempDir>)> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_ascii_lowercase());

    // GHO/GHS and IMZ have streaming Read+Seek readers (GhoReader,
    // ImzReader) — the inspect worker opens them via open_read, so
    // the path passes through unchanged. No tempfile needed.

    // `decode_floppy_containers` controls the floppy-wrapper formats
    // (.msa/.d88/.xdf/.hdm/.dim). The Backup / Restore tabs pass `true`: they
    // need a flat sector stream to compress / write. The Inspect tab (and the
    // drag-drop / file-association open path) pass `false` so the original
    // container is opened directly — the read paths are container-aware via
    // source_reader::open_read, and editing persists back into the container
    // through BrowseSession::open_editable's ContainerEditSession. Pre-decoding
    // to a throwaway tempfile would orphan edits and hide the real file name.
    // The gzip-wrapped Amiga images (.adz/.hdz) decode to flat sectors only for
    // the Backup/Restore path (`decode_floppy_containers`); the browse/inspect/
    // commander path leaves them as containers so `source_reader::open_read`
    // peels the gzip and edits re-gzip back in place (BrowseSession's gzip
    // ContainerEditSession), rather than orphaning edits in a throwaway .adf.
    let target_ext = match ext.as_deref() {
        Some("adz") if decode_floppy_containers => "adf",
        Some("hdz") if decode_floppy_containers => "hdf",
        Some("msa") if decode_floppy_containers => "st",
        Some("d88") if decode_floppy_containers => "img",
        Some("xdf") | Some("hdm") | Some("dim") if decode_floppy_containers => "img",
        _ => return Ok((path.to_path_buf(), None)),
    };
    let tmp = tempfile::tempdir()?;
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("image");
    let out_path = tmp.path().join(format!("{}.{}", stem, target_ext));

    if target_ext == "st" {
        // MSA → flat raw sectors, written to a tempfile so the rest of the
        // GUI pipeline (which addresses partitions by byte offset on a
        // path) sees a plain `.st` image.
        let bytes = std::fs::read(path)?;
        let flat = dsk_floppy::msa::decode_msa_bytes(&bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e:#}")))?;
        std::fs::write(&out_path, &flat)?;
        log::info!(
            "Materialized {} -> {} ({} bytes, MSA decoded)",
            path.display(),
            out_path.display(),
            flat.len()
        );
        return Ok((out_path, Some(tmp)));
    }
    if target_ext == "img" && ext.as_deref() == Some("d88") {
        // D88 → flat raw sectors. Same shape as MSA: small floppy
        // (~1.5 MB max for 2HD), in-memory decode, tempfile drop-in.
        let bytes = std::fs::read(path)?;
        let flat = dsk_floppy::d88::decode_d88_bytes(&bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e:#}")))?;
        std::fs::write(&out_path, &flat)?;
        log::info!(
            "Materialized {} -> {} ({} bytes, D88 decoded)",
            path.display(),
            out_path.display(),
            flat.len()
        );
        return Ok((out_path, Some(tmp)));
    }
    if target_ext == "img" && matches!(ext.as_deref(), Some("xdf") | Some("hdm") | Some("dim")) {
        let bytes = std::fs::read(path)?;
        let (flat, label) = match ext.as_deref() {
            Some("xdf") => (
                dsk_floppy::xdf::decode_xdf_bytes(&bytes)
                    .map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e:#}"))
                    })?
                    .0,
                "XDF",
            ),
            Some("hdm") => (
                dsk_floppy::hdm::decode_hdm_bytes(&bytes)
                    .map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e:#}"))
                    })?
                    .0,
                "HDM",
            ),
            Some("dim") => (
                dsk_floppy::dim::decode_dim_bytes(&bytes)
                    .map_err(|e| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e:#}"))
                    })?
                    .0,
                "DIM",
            ),
            _ => unreachable!("guarded by outer match arm"),
        };
        std::fs::write(&out_path, &flat)?;
        log::info!(
            "Materialized {} -> {} ({} bytes, {} decoded)",
            path.display(),
            out_path.display(),
            flat.len(),
            label
        );
        return Ok((out_path, Some(tmp)));
    }

    // Gzip-wrapped Amiga images (.adz / .hdz).
    let mut input = std::fs::File::open(path)?;
    // Spot-check the gzip magic so we surface a clear error if the
    // user renamed something to .adz that isn't actually gzipped.
    let mut magic = [0u8; 2];
    use std::io::Read;
    input.read_exact(&mut magic)?;
    if magic != [0x1F, 0x8B] {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{} does not start with gzip magic 1f 8b", path.display()),
        ));
    }
    use std::io::Seek;
    input.seek(std::io::SeekFrom::Start(0))?;
    let total = input.metadata().map(|m| m.len()).unwrap_or(0);
    let counted = CountingReader {
        inner: input,
        read: 0,
        total,
        progress,
    };
    let mut decoder = flate2::read::GzDecoder::new(counted);
    let mut out = std::fs::File::create(&out_path)?;
    std::io::copy(&mut decoder, &mut out)?;
    out.sync_all()?;
    log::info!(
        "Materialized {} -> {} ({} bytes)",
        path.display(),
        out_path.display(),
        out.metadata().map(|m| m.len()).unwrap_or(0)
    );
    Ok((out_path, Some(tmp)))
}

/// Reports how much of the compressed input has been read.
struct CountingReader<'a, R> {
    inner: R,
    read: u64,
    total: u64,
    progress: &'a dyn Fn(u64, u64),
}

impl<R: std::io::Read> std::io::Read for CountingReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.read += n as u64;
        (self.progress)(self.read, self.total);
        Ok(n)
    }
}

#[cfg(test)]
mod materialize_tests {
    use super::*;
    use std::io::Write as _;

    #[test]
    fn materialize_passes_raw_adf_through() {
        let tmp = tempfile::tempdir().unwrap();
        let raw = tmp.path().join("disk.adf");
        std::fs::write(&raw, b"raw bytes").unwrap();
        let (out, guard) = prepare_disk_image_path(&raw, true).unwrap();
        assert_eq!(out, raw);
        assert!(guard.is_none());
    }

    #[test]
    fn materialize_decompresses_adz() {
        let payload: &[u8] = b"PAYLOAD bytes for an adz round-trip";
        let tmp = tempfile::tempdir().unwrap();
        let adz_path = tmp.path().join("disk.adz");
        // Write a tiny gzipped file.
        let f = std::fs::File::create(&adz_path).unwrap();
        let mut enc = flate2::write::GzEncoder::new(f, flate2::Compression::default());
        enc.write_all(payload).unwrap();
        enc.finish().unwrap();
        let (out, guard) = prepare_disk_image_path(&adz_path, true).unwrap();
        assert!(guard.is_some(), "decompressed path needs a tempdir guard");
        assert_eq!(out.extension().and_then(|s| s.to_str()), Some("adf"));
        let actual = std::fs::read(&out).unwrap();
        assert_eq!(actual, payload);
    }

    #[test]
    fn materialize_rejects_non_gzip_adz() {
        let tmp = tempfile::tempdir().unwrap();
        let adz = tmp.path().join("bogus.adz");
        std::fs::write(&adz, b"not gzip data").unwrap();
        let err = prepare_disk_image_path(&adz, true).unwrap_err();
        assert!(err.to_string().contains("gzip magic"));
    }

    #[test]
    fn imz_passes_through_unchanged() {
        // IMZ has a streaming reader (ImzReader); prepare_disk_image_path
        // passes it through unchanged — the worker opens it via
        // open_disk_image_for_reading.
        let tmp = tempfile::tempdir().unwrap();
        let imz_path = tmp.path().join("floppy.imz");
        std::fs::write(&imz_path, b"PK\x03\x04anything").unwrap();
        let (out, guard) = prepare_disk_image_path(&imz_path, true).unwrap();
        assert_eq!(out, imz_path);
        assert!(guard.is_none());
    }

    #[test]
    fn gho_passes_through_unchanged() {
        let tmp = tempfile::tempdir().unwrap();
        let gho_path = tmp.path().join("test.gho");
        std::fs::write(&gho_path, [0xFE, 0xEF, 0x01, 0x00]).unwrap();
        let (out, guard) = prepare_disk_image_path(&gho_path, true).unwrap();
        assert_eq!(out, gho_path);
        assert!(guard.is_none());
    }

    #[test]
    fn hfv_passes_through_unchanged() {
        // HFV is a flat raw HFS volume opened directly at offset 0 by the
        // superfloppy path; prepare_disk_image_path must not materialize or
        // rewrite it. Covers both .hfv and .HFV.
        let tmp = tempfile::tempdir().unwrap();
        for name in ["disk.hfv", "DISK.HFV"] {
            let hfv_path = tmp.path().join(name);
            std::fs::write(&hfv_path, b"\x00\x00\x00\x00").unwrap();
            let (out, guard) = prepare_disk_image_path(&hfv_path, true).unwrap();
            assert_eq!(out, hfv_path);
            assert!(guard.is_none());
        }
    }
    #[test]
    fn needs_decoding_matches_what_prepare_decodes() {
        use std::path::Path;
        for (name, decoded) in [
            ("a.hdz", true),
            ("a.ADZ", true),
            ("a.msa", true),
            ("a.img", false),
            ("a.chd", false),
        ] {
            assert_eq!(needs_decoding(Path::new(name), true), decoded, "{name}");
            assert!(!needs_decoding(Path::new(name), false), "{name}");
        }
    }
}
