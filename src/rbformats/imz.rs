//! WinImage IMZ (`.imz`) reader.
//!
//! IMZ is a tiny ZIP archive containing a single floppy image (`.ima` /
//! `.img`). The reader walks the archive, picks the first non-directory
//! entry, and extracts it to a fresh tempdir. Callers hold the returned
//! [`tempfile::TempDir`] guard for the lifetime of the open image so the
//! decoded file lives long enough to be inspected/browsed.
//!
//! Password-protected archives (legacy PKZIP / ZipCrypto) are detected and
//! rejected with a precise error — UI-driven password prompting is a
//! follow-up.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};

/// IMZ magic = ZIP local file header signature.
pub const IMZ_MAGIC: &[u8; 4] = b"PK\x03\x04";

/// Sniff whether `file` *might* be an IMZ. Cheap check used by the detection
/// ladder before paying for `ZipArchive::new`.
///
/// Leaves the cursor undefined (caller should reset).
pub fn looks_like_imz(file: &mut File) -> bool {
    if file.seek(SeekFrom::Start(0)).is_err() {
        return false;
    }
    let mut magic = [0u8; 4];
    matches!(file.read_exact(&mut magic), Ok(())) && &magic == IMZ_MAGIC
}

/// Result of materializing an IMZ.
#[derive(Debug)]
pub struct ImzMaterialized {
    /// Path to the extracted floppy image inside the tempdir.
    pub temp_path: PathBuf,
    /// Logical size of the decoded image (== file size at `temp_path`).
    pub logical_size: u64,
    /// Tempdir guard. When dropped, the materialized file is removed.
    /// Callers (GUI tab / inspect flow) must keep this alive for as long
    /// as they need the materialized file.
    pub guard: tempfile::TempDir,
    /// Name of the entry inside the archive (for logging).
    pub entry_name: String,
}

fn is_image_entry(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.ends_with(".ima")
        || lower.ends_with(".img")
        || lower.ends_with(".dsk")
        || lower.ends_with(".flp")
        || lower.ends_with(".vfd")
}

/// Open `path` as an IMZ archive and extract its floppy image to a fresh
/// tempdir. The returned [`ImzMaterialized::guard`] must be kept alive for
/// the lifetime of the materialized file.
pub fn materialize_imz_to_temp(path: &Path) -> Result<ImzMaterialized> {
    let file = File::open(path).with_context(|| format!("opening IMZ {}", path.display()))?;
    let mut archive = zip::ZipArchive::new(file)
        .with_context(|| format!("parsing IMZ {} as ZIP", path.display()))?;

    if archive.is_empty() {
        return Err(anyhow!("IMZ {} contains no entries", path.display()));
    }

    // Pick the first entry whose name looks like a floppy image; fall back
    // to the first non-directory entry. WinImage always emits exactly one
    // entry whose name matches the host filename, so either rule picks it.
    let mut chosen: Option<usize> = None;
    let mut fallback: Option<usize> = None;
    for i in 0..archive.len() {
        let entry = archive.by_index(i)?;
        if entry.is_dir() {
            continue;
        }
        if is_image_entry(entry.name()) {
            chosen = Some(i);
            break;
        }
        if fallback.is_none() {
            fallback = Some(i);
        }
    }
    let idx = chosen
        .or(fallback)
        .ok_or_else(|| anyhow!("IMZ {} has no extractable file entry", path.display()))?;

    let mut entry = archive.by_index(idx)?;
    if entry.encrypted() {
        return Err(anyhow!(
            "IMZ {} is password-protected (entry \"{}\"); password-protected IMZ \
             archives are not yet supported",
            path.display(),
            entry.name()
        ));
    }
    let entry_name = entry.name().to_string();
    let logical_size = entry.size();

    let guard = tempfile::tempdir().with_context(|| "creating tempdir for IMZ materialization")?;
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("imz");
    // Always write with a `.ima` extension so downstream detection treats
    // the materialized file as a raw floppy image.
    let out_path = guard.path().join(format!("{}.ima", stem));
    let mut out =
        File::create(&out_path).with_context(|| format!("creating {}", out_path.display()))?;
    let copied = io::copy(&mut entry, &mut out)
        .with_context(|| format!("decompressing IMZ entry \"{}\"", entry_name))?;
    out.sync_all().ok();

    log::info!(
        "Materialized IMZ {} -> {} ({} bytes, entry \"{}\")",
        path.display(),
        out_path.display(),
        copied,
        entry_name
    );

    Ok(ImzMaterialized {
        temp_path: out_path,
        logical_size,
        guard,
        entry_name,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use zip::write::SimpleFileOptions;

    fn build_imz(entry_name: &str, payload: &[u8]) -> PathBuf {
        let dir = tempfile::tempdir().unwrap().keep();
        let path = dir.join("test.imz");
        let f = File::create(&path).unwrap();
        let mut zw = zip::ZipWriter::new(f);
        let opts =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);
        zw.start_file(entry_name, opts).unwrap();
        zw.write_all(payload).unwrap();
        zw.finish().unwrap();
        path
    }

    #[test]
    fn round_trip_imz_with_ima_entry() {
        let payload: Vec<u8> = (0..4096).map(|i| (i * 37) as u8).collect();
        let imz = build_imz("disk.ima", &payload);
        let out = materialize_imz_to_temp(&imz).unwrap();
        assert_eq!(out.logical_size, payload.len() as u64);
        assert!(out.temp_path.exists());
        let got = std::fs::read(&out.temp_path).unwrap();
        assert_eq!(got, payload);
        // Guard drop removes the tempdir.
        let p = out.temp_path.clone();
        drop(out);
        assert!(!p.exists(), "tempdir should be cleaned up when guard drops");
    }

    #[test]
    fn picks_image_entry_over_other_files() {
        // IMZ-in-the-wild has exactly one entry, but if a future archive
        // ever carried a sidecar txt we should still pick the .ima.
        let payload = b"floppy bytes";
        let dir = tempfile::tempdir().unwrap().keep();
        let path = dir.join("test.imz");
        let f = File::create(&path).unwrap();
        let mut zw = zip::ZipWriter::new(f);
        let opts = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
        zw.start_file("README.txt", opts).unwrap();
        zw.write_all(b"a sidecar").unwrap();
        zw.start_file("floppy.IMA", opts).unwrap();
        zw.write_all(payload).unwrap();
        zw.finish().unwrap();
        let out = materialize_imz_to_temp(&path).unwrap();
        assert_eq!(out.entry_name, "floppy.IMA");
        assert_eq!(std::fs::read(&out.temp_path).unwrap(), payload);
    }

    /// Gated on the real WinImage password fixture being present. Confirms
    /// we surface a clean "password-protected" error instead of panicking
    /// or producing garbage. The `zip` 2.x writer API doesn't expose the
    /// legacy ZipCrypto path publicly, so we lean on a real fixture rather
    /// than hand-rolling an encrypted local header.
    #[test]
    fn rejects_password_protected_real_winimage_fixture() {
        let home = match std::env::var("HOME") {
            Ok(h) => h,
            Err(_) => return,
        };
        let fixture = PathBuf::from(home).join("new-fixtures/imz/w99_boot_withpassword.imz");
        if !fixture.exists() {
            eprintln!(
                "skipping: {} not present (fixture-gated test)",
                fixture.display()
            );
            return;
        }
        let err = materialize_imz_to_temp(&fixture).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.to_lowercase().contains("password"),
            "error should mention password, got: {msg}"
        );
    }

    /// Gated on the real WinImage no-password fixture being present.
    /// Materializes the IMZ and confirms the decoded bytes match the
    /// reference `.ima` we have alongside it.
    #[test]
    fn matches_reference_ima_for_real_winimage_fixture() {
        let home = match std::env::var("HOME") {
            Ok(h) => h,
            Err(_) => return,
        };
        let base = PathBuf::from(home).join("new-fixtures/imz");
        let imz = base.join("w98_boot_nopassword.imz");
        let reference = base.join("w98_boot.ima");
        if !imz.exists() || !reference.exists() {
            eprintln!(
                "skipping: {} or {} missing",
                imz.display(),
                reference.display()
            );
            return;
        }
        let out = materialize_imz_to_temp(&imz).unwrap();
        let got = std::fs::read(&out.temp_path).unwrap();
        let want = std::fs::read(&reference).unwrap();
        assert_eq!(
            got.len(),
            want.len(),
            "decoded size {} != reference {}",
            got.len(),
            want.len()
        );
        assert_eq!(got, want, "decoded IMZ bytes differ from reference IMA");
    }

    #[test]
    fn empty_archive_errors() {
        let dir = tempfile::tempdir().unwrap().keep();
        let path = dir.join("empty.imz");
        let f = File::create(&path).unwrap();
        let zw = zip::ZipWriter::new(f);
        zw.finish().unwrap();
        let err = materialize_imz_to_temp(&path).unwrap_err();
        assert!(format!("{err}").contains("no entries"));
    }

    #[test]
    fn looks_like_imz_matches_pk_magic() {
        let dir = tempfile::tempdir().unwrap().keep();
        let path = dir.join("d.imz");
        std::fs::write(&path, b"PK\x03\x04rest").unwrap();
        let mut f = File::open(&path).unwrap();
        assert!(looks_like_imz(&mut f));

        let path2 = dir.join("not.imz");
        std::fs::write(&path2, b"\x00\x00not zip").unwrap();
        let mut f2 = File::open(&path2).unwrap();
        assert!(!looks_like_imz(&mut f2));
    }
}
