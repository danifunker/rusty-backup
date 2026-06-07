//! In-place editing of standalone floppy-container files (`.d88` / `.xdf` /
//! `.hdm` / `.dim`).
//!
//! These wrappers can't be edited byte-in-place — D88 has a sparse track
//! table, DIM a 256-byte header; XDF/HDM are headerless but route through the
//! same path so all four behave identically. A [`ContainerEditSession`]
//! decodes the container into a temporary flat image, hands that path to the
//! caller to mutate via the normal [`crate::fs::EditableFilesystem`] flow,
//! then re-encodes the (mutated) flat back into the original container format
//! and atomically replaces the file on [`ContainerEditSession::commit`].
//!
//! This is the standalone-file parallel of [`crate::model::archive_edit`]
//! (which handles compressed *backup* partitions, with metadata.json
//! checksum updates). Both follow the same WOZ decode -> edit -> re-encode
//! pattern. Dropping a session without calling `commit` discards the edits
//! (the tempfile is cleaned up), so a failed mutation never corrupts the
//! original container.

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::rbformats::containers::{
    decode_floppy_container_file, encode_floppy_container, ContainerKind,
};

/// A floppy container opened for editing: decoded to a temp flat image that
/// the caller mutates in place, then re-encoded back to the original
/// container format on [`commit`](Self::commit).
pub struct ContainerEditSession {
    original_path: PathBuf,
    kind: ContainerKind,
    temp: tempfile::NamedTempFile,
    /// Flat-image length at open time. Floppy containers are fixed-geometry,
    /// so a commit that changed the size is a bug we refuse rather than write
    /// an unencodable image.
    original_flat_len: usize,
}

impl ContainerEditSession {
    /// Decode `path` into a temp flat image for editing. Errors if `path` is
    /// not one of the four editable floppy containers (XDF / HDM / DIM / D88).
    pub fn open(path: &Path) -> Result<Self> {
        let (kind, flat) = decode_floppy_container_file(path)?;
        let temp = tempfile::Builder::new()
            .prefix(".rb-container-edit-")
            .tempfile()
            .context("create container edit tempfile")?;
        std::fs::write(temp.path(), &flat)
            .with_context(|| format!("write decoded flat to {}", temp.path().display()))?;
        Ok(Self {
            original_path: path.to_path_buf(),
            kind,
            original_flat_len: flat.len(),
            temp,
        })
    }

    /// Path to the decoded flat image. Open an editable filesystem on THIS
    /// path (read+write), mutate, and `sync_metadata`; then call
    /// [`commit`](Self::commit) to fold the changes back into the container.
    pub fn flat_path(&self) -> &Path {
        self.temp.path()
    }

    /// The detected container kind (for logging / display).
    pub fn kind(&self) -> ContainerKind {
        self.kind
    }

    /// Re-encode the (edited) flat image back into the container format and
    /// atomically replace the original file. Consumes the session. The temp
    /// flat is removed automatically (whether this succeeds or fails).
    pub fn commit(self) -> Result<()> {
        let flat = std::fs::read(self.temp.path())
            .with_context(|| format!("read edited flat {}", self.temp.path().display()))?;
        // FAT edits don't change the partition size, so the flat length must
        // still match the geometry it had on open. A mismatch means something
        // upstream grew the image, which floppy containers can't represent.
        if flat.len() != self.original_flat_len {
            anyhow::bail!(
                "edited image changed size ({} -> {} bytes); floppy containers are fixed-geometry",
                self.original_flat_len,
                flat.len()
            );
        }
        let encoded = encode_floppy_container(&flat, self.kind)
            .with_context(|| format!("re-encode {} container", self.kind.display_name()))?;

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

    #[test]
    fn open_rejects_non_container() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("plain.bin");
        std::fs::write(&p, vec![0u8; 4096]).unwrap();
        assert!(ContainerEditSession::open(&p).is_err());
    }
}
