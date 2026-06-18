//! In-place editing of standalone container files: the floppy wrappers
//! (`.d88` / `.xdf` / `.hdm` / `.dim` / `.atr`), the gzip-wrapped Amiga
//! images (`.adz` / `.hdz`), and WOZ floppies (`.woz`).
//!
//! These wrappers can't be edited byte-in-place — D88 has a sparse track
//! table, DIM a 256-byte header, gzip is compressed, WOZ stores GCR
//! bitstreams. A [`ContainerEditSession`]
//! decodes the container into a temporary flat image, hands that path to the
//! caller to mutate via the normal [`crate::fs::EditableFilesystem`] flow,
//! then re-encodes the (mutated) flat back into the original container format
//! and atomically replaces the file on [`ContainerEditSession::commit`].
//!
//! This is the standalone-file parallel of [`crate::model::archive_edit`]
//! (which handles compressed *backup* partitions, with metadata.json
//! checksum updates). Both follow the same decode -> edit -> re-encode
//! pattern. Dropping a session without calling `commit` discards the edits
//! (the tempfile is cleaned up), so a failed mutation never corrupts the
//! original container.

use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::rbformats::containers::{
    decode_floppy_container_file, encode_floppy_container, ContainerKind,
};

/// What kind of wrapper a [`ContainerEditSession`] is re-encoding on commit.
enum EditFormat {
    /// A fixed-geometry floppy container (XDF / HDM / DIM / D88 / ATR). The
    /// edited flat must keep the same length.
    Floppy(ContainerKind),
    /// A gzip-wrapped image (`.adz` / `.hdz`). Re-gzipped on commit; the inner
    /// image may be any size, so no size check.
    Gzip,
    /// A WOZ 1/2 floppy (`.woz`). Decoded from its GCR bitstream to a flat
    /// sector buffer; re-encoded via `sectors_to_woz` on commit. Fixed
    /// geometry, so the edited flat must keep the same length.
    Woz,
}

/// A container opened for editing: decoded to a temp flat image that the caller
/// mutates in place, then re-encoded back to the original container format on
/// [`commit`](Self::commit).
pub struct ContainerEditSession {
    original_path: PathBuf,
    format: EditFormat,
    temp: tempfile::NamedTempFile,
    /// Flat-image length at open time (used only to reject a size change on a
    /// fixed-geometry floppy container).
    original_flat_len: u64,
}

impl ContainerEditSession {
    /// Decode `path` into a temp flat image for editing. Errors if `path` is
    /// not an editable container (the floppy wrappers or a gzip `.adz`/`.hdz`).
    pub fn open(path: &Path) -> Result<Self> {
        let temp = tempfile::Builder::new()
            .prefix(".rb-container-edit-")
            .tempfile()
            .context("create container edit tempfile")?;

        // Gzip-wrapped Amiga image: stream-decompress into the temp flat so a
        // large .hdz doesn't live entirely in RAM.
        if crate::model::source_reader::is_gzip_image_path(path) {
            let input =
                std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
            let mut decoder = flate2::read::GzDecoder::new(BufReader::new(input));
            let mut out = std::fs::File::create(temp.path())
                .with_context(|| format!("write decoded flat to {}", temp.path().display()))?;
            let n = std::io::copy(&mut decoder, &mut out)
                .with_context(|| format!("decompress gzip image {}", path.display()))?;
            out.sync_all().ok();
            return Ok(Self {
                original_path: path.to_path_buf(),
                format: EditFormat::Gzip,
                original_flat_len: n,
                temp,
            });
        }

        // WOZ floppy: decode the GCR bitstream into a flat sector buffer.
        if crate::model::source_reader::is_woz_path(path) {
            let mut reader = crate::rbformats::woz::WozReader::open(path)
                .with_context(|| format!("decode WOZ {}", path.display()))?;
            let flat_len = reader.len();
            let mut out = std::fs::File::create(temp.path())
                .with_context(|| format!("write decoded flat to {}", temp.path().display()))?;
            std::io::copy(&mut reader, &mut out)
                .with_context(|| format!("decode WOZ image {}", path.display()))?;
            out.sync_all().ok();
            return Ok(Self {
                original_path: path.to_path_buf(),
                format: EditFormat::Woz,
                original_flat_len: flat_len,
                temp,
            });
        }

        let (kind, flat) = decode_floppy_container_file(path)?;
        std::fs::write(temp.path(), &flat)
            .with_context(|| format!("write decoded flat to {}", temp.path().display()))?;
        Ok(Self {
            original_path: path.to_path_buf(),
            format: EditFormat::Floppy(kind),
            original_flat_len: flat.len() as u64,
            temp,
        })
    }

    /// Path to the decoded flat image. Open an editable filesystem on THIS
    /// path (read+write), mutate, and `sync_metadata`; then call
    /// [`commit`](Self::commit) to fold the changes back into the container.
    pub fn flat_path(&self) -> &Path {
        self.temp.path()
    }

    /// A short display name for the wrapper format (for logging / errors).
    pub fn format_name(&self) -> &'static str {
        match self.format {
            EditFormat::Floppy(kind) => kind.display_name(),
            EditFormat::Gzip => "gzip",
            EditFormat::Woz => "woz",
        }
    }

    /// Re-encode the (edited) flat image back into the container format and
    /// atomically replace the original file. Consumes the session. The temp
    /// flat is removed automatically (whether this succeeds or fails).
    pub fn commit(self) -> Result<()> {
        let encoded: Vec<u8> = match self.format {
            EditFormat::Floppy(kind) => {
                let flat = std::fs::read(self.temp.path())
                    .with_context(|| format!("read edited flat {}", self.temp.path().display()))?;
                // FAT edits don't change the partition size, so the flat length
                // must still match the geometry it had on open. A mismatch means
                // something grew the image, which floppy containers can't hold.
                if flat.len() as u64 != self.original_flat_len {
                    anyhow::bail!(
                        "edited image changed size ({} -> {} bytes); floppy containers are fixed-geometry",
                        self.original_flat_len,
                        flat.len()
                    );
                }
                encode_floppy_container(&flat, kind)
                    .with_context(|| format!("re-encode {} container", kind.display_name()))?
            }
            EditFormat::Gzip => {
                // Re-gzip the edited flat (any size is fine for a gzip wrapper).
                let mut input =
                    BufReader::new(std::fs::File::open(self.temp.path()).with_context(|| {
                        format!("read edited flat {}", self.temp.path().display())
                    })?);
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                std::io::copy(&mut input, &mut encoder).context("re-gzip edited image")?;
                encoder.finish().context("finish gzip stream")?
            }
            EditFormat::Woz => {
                let flat = std::fs::read(self.temp.path())
                    .with_context(|| format!("read edited flat {}", self.temp.path().display()))?;
                // WOZ is a fixed-geometry floppy; FS edits must not change the
                // size, and sectors_to_woz only accepts the exact 140K / 400K /
                // 800K disk sizes.
                if flat.len() as u64 != self.original_flat_len {
                    anyhow::bail!(
                        "edited image changed size ({} -> {} bytes); WOZ floppies are fixed-geometry",
                        self.original_flat_len,
                        flat.len()
                    );
                }
                crate::rbformats::woz_write::sectors_to_woz(&flat)
                    .context("re-encode WOZ container")?
            }
        };

        // Atomic replace: write a sibling temp in the same directory then
        // rename over the original, so a crash mid-write can't truncate the
        // user's only copy. tempfile_in keeps it on the same filesystem so the
        // persist() rename is atomic.
        let dir = self
            .original_path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let mut tmp = tempfile::Builder::new()
            .prefix(".rb-container-")
            .tempfile_in(dir)
            .context("create sibling temp for atomic container write")?;
        tmp.write_all(&encoded)
            .context("write re-encoded container")?;
        tmp.flush()?;
        tmp.persist(&self.original_path).map_err(|e| {
            anyhow::anyhow!(
                "persist re-encoded container to {}: {e}",
                self.original_path.display()
            )
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rbformats::containers::{convert_floppy_container, floppy_geom::FloppyMedia};

    /// Build a FAT12-bearing flat image, wrap it as `kind`, edit a byte in the
    /// decoded flat via the session, commit, and confirm the change survives a
    /// re-decode AND that the file is still a valid container of `kind`.
    fn round_trip_edit(kind_ext: &str) {
        let dir = tempfile::tempdir().unwrap();
        // Start from a raw 720K flat image and convert it to the target
        // container via the public conversion engine (covers header/track
        // encoding for DIM/D88, passthrough for XDF/HDM).
        let geom = FloppyMedia::Dd720.geometry();
        let total = geom.flat_size();
        let mut flat = vec![0u8; total];
        // Minimal FAT12 BPB so it's a plausible volume (not required for the
        // container round-trip, but keeps the bytes realistic).
        flat[0] = 0xEB;
        flat[1] = 0x3C;
        flat[2] = 0x90;
        // Byte 0x1B is the D88 media-type byte; set it to a non-D88 value
        // (0xFE, a typical FAT media descriptor) so an all-zero header doesn't
        // make the seed sniff as an "empty D88" before its .xdf extension is
        // consulted. (Same guard the containers::tests XDF cases use.)
        flat[0x1B] = 0xFE;
        flat[510] = 0x55;
        flat[511] = 0xAA;
        // A sentinel byte we'll later flip via the edit session.
        flat[1024] = 0x11;

        let raw_path = dir.path().join("seed.xdf"); // headerless == raw flat
        std::fs::write(&raw_path, &flat).unwrap();
        let container_path = dir.path().join(format!("disk.{kind_ext}"));
        let target =
            crate::rbformats::containers::floppy_kind_from_extension(&container_path).unwrap();
        convert_floppy_container(&raw_path, &container_path, target).unwrap();

        // Open an edit session, flip the sentinel byte in the decoded flat,
        // and commit.
        let session = ContainerEditSession::open(&container_path).unwrap();
        let flat_path = session.flat_path().to_path_buf();
        let mut edited = std::fs::read(&flat_path).unwrap();
        assert_eq!(
            edited[1024], 0x11,
            "sentinel survived decode for {kind_ext}"
        );
        edited[1024] = 0x22;
        std::fs::write(&flat_path, &edited).unwrap();
        session.commit().unwrap();

        // Re-decode the persisted container; the edit must be there.
        let (_kind, redecoded) = decode_floppy_container_file(&container_path).unwrap();
        assert_eq!(redecoded.len(), total, "{kind_ext} geometry preserved");
        assert_eq!(redecoded[1024], 0x22, "edit persisted into {kind_ext}");
    }

    #[test]
    fn round_trip_edit_xdf() {
        round_trip_edit("xdf");
    }

    #[test]
    fn round_trip_edit_hdm() {
        round_trip_edit("hdm");
    }

    #[test]
    fn round_trip_edit_dim() {
        round_trip_edit("dim");
    }

    #[test]
    fn round_trip_edit_d88() {
        round_trip_edit("d88");
    }

    /// A gzip-wrapped `.adz` decodes to a flat image, an edit to the flat
    /// re-gzips back into the file, and the result is still a valid gzip stream
    /// carrying the edit.
    #[test]
    fn round_trip_edit_gzip_adz() {
        use flate2::read::GzDecoder;
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Read;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("disk.adz");
        let mut flat = vec![0u8; 4096];
        flat[100] = 0x11; // sentinel we'll flip via the edit session
        {
            let f = std::fs::File::create(&path).unwrap();
            let mut enc = GzEncoder::new(f, Compression::default());
            enc.write_all(&flat).unwrap();
            enc.finish().unwrap();
        }

        let session = ContainerEditSession::open(&path).unwrap();
        assert_eq!(session.format_name(), "gzip");
        let flat_path = session.flat_path().to_path_buf();
        let mut edited = std::fs::read(&flat_path).unwrap();
        assert_eq!(edited.len(), 4096, "gzip decoded to the original flat");
        assert_eq!(edited[100], 0x11, "sentinel survived decode");
        edited[100] = 0x22;
        std::fs::write(&flat_path, &edited).unwrap();
        session.commit().unwrap();

        // Re-gunzip the persisted .adz: the edit must be there, still valid gzip.
        let f = std::fs::File::open(&path).unwrap();
        let mut dec = GzDecoder::new(f);
        let mut got = Vec::new();
        dec.read_to_end(&mut got).unwrap();
        assert_eq!(got.len(), 4096, "geometry preserved");
        assert_eq!(got[100], 0x22, "edit persisted into the .adz");
    }

    /// A `.woz` decodes from its GCR bitstream to a flat sector buffer; an edit
    /// re-encodes via `sectors_to_woz` and the result is still a valid WOZ
    /// carrying the edit (same fixed geometry).
    #[test]
    fn round_trip_edit_woz() {
        use crate::rbformats::woz::WozReader;
        use crate::rbformats::woz_write::write_woz;
        use std::io::Read;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("disk.woz");
        // 5.25" 140K flat; sentinel byte we'll flip via the session.
        let mut flat = vec![0u8; 143_360];
        flat[1024] = 0x11;
        write_woz(&path, &flat).unwrap();

        let session = ContainerEditSession::open(&path).unwrap();
        assert_eq!(session.format_name(), "woz");
        let flat_path = session.flat_path().to_path_buf();
        let mut edited = std::fs::read(&flat_path).unwrap();
        assert_eq!(edited.len(), 143_360, "WOZ decoded to the flat 140K image");
        assert_eq!(edited[1024], 0x11, "sentinel survived GCR decode");
        edited[1024] = 0x22;
        std::fs::write(&flat_path, &edited).unwrap();
        session.commit().unwrap();

        // Re-decode the persisted .woz: the edit must be there, still valid WOZ.
        let mut reader = WozReader::open(&path).unwrap();
        let mut got = Vec::new();
        reader.read_to_end(&mut got).unwrap();
        assert_eq!(got.len(), 143_360, "geometry preserved");
        assert_eq!(got[1024], 0x22, "edit persisted into the .woz");
    }

    #[test]
    fn open_rejects_non_container() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("plain.bin");
        std::fs::write(&p, vec![0u8; 4096]).unwrap();
        assert!(ContainerEditSession::open(&p).is_err());
    }
}
