//! TR-DOS (ZX Spectrum Beta Disk) filesystem — read driver.
//!
//! The native disk filesystem of the ZX Spectrum's Beta Disk interface, stored
//! in the ubiquitous `.trd` raw image. Like UCSD p-System it is deliberately
//! tiny: a single **flat** directory (no subdirectories) of at most 128 files,
//! each stored as a **contiguous** run of 256-byte sectors. There is no
//! allocation bitmap — files are packed from the first data sector in catalogue
//! order and a first-free "high-water mark" advances on save and never retreats
//! on erase (space is reclaimed only by TR-DOS's MOVE/compress command).
//!
//! Layout:
//!
//! ```text
//!   track 0, sectors 0..7   catalogue (8 sectors = 128 x 16-byte entries)
//!   track 0, sector 8        disk-info sector (offset 0x800)
//!   track 1, sector 0 ..     file data
//! ```
//!
//! Geometry comes from the disk-type byte (offset 0xE3 of the disk-info sector):
//! 0x16 = 80-track double-sided (640 KiB), 0x17 = 40-track DS, 0x18 = 80-track
//! single-sided, 0x19 = 40-track SS — all 16 sectors/track, 256 bytes/sector.
//!
//! **Addressing.** File entries carry a *logical* track number, and a `.trd`
//! stores its sectors in logical order, so `byte_offset = (track*16 + sector) *
//! 256` — a flat sector array with no head/cylinder interleaving to reason about
//! (the single easiest place to introduce a bug on a double-sided disk).
//!
//! All offsets below were validated byte-for-byte against a clean-room oracle
//! (`scripts/trdos-oracle.py`) built from the same spec.
//!
//! This is the read half of the TR-DOS quartet (Browse); edit / create / fsck
//! build on it (`docs/filesystem_completion_plan.md` Part 2).

use std::io::{Read, Seek, SeekFrom};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

/// Bytes per sector.
pub(crate) const SECTOR: u64 = 256;
/// Sectors per (logical) track.
pub(crate) const SPT: u64 = 16;
/// The catalogue occupies track 0, sectors 0..7.
const CATALOG_SECTORS: u64 = 8;
/// The disk-info sector is track 0, sector 8 (byte offset 0x800).
pub(crate) const INFO_SECTOR: u64 = 8;
/// First data sector: track 1, sector 0 = linear sector 16.
pub(crate) const DATA_START: u64 = SPT;
/// Directory-entry stride.
const ENTRY: usize = 16;
/// Catalogue capacity.
pub(crate) const MAX_FILES: usize = 128;
/// Byte offset of the disk-info sector.
pub(crate) const INFO_OFF: u64 = INFO_SECTOR * SECTOR;
/// TR-DOS identifier byte (disk-info offset 0xE7).
pub(crate) const TRDOS_ID: u8 = 0x10;

/// A parsed disk geometry, derived from the disk-type byte.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrdosGeometry {
    pub disk_type: u8,
    pub logical_tracks: u16,
}

impl TrdosGeometry {
    /// Map a disk-type byte to its logical-track count, or `None` if invalid.
    pub fn from_type(disk_type: u8) -> Option<Self> {
        let logical_tracks = match disk_type {
            0x16 => 160, // 80 track, double sided
            0x17 => 80,  // 40 track, double sided
            0x18 => 80,  // 80 track, single sided
            0x19 => 40,  // 40 track, single sided
            _ => return None,
        };
        Some(TrdosGeometry {
            disk_type,
            logical_tracks,
        })
    }

    pub fn total_sectors(&self) -> u64 {
        self.logical_tracks as u64 * SPT
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_sectors() * SECTOR
    }

    /// Human-readable geometry (for logs / fsck stats).
    pub fn describe(&self) -> &'static str {
        match self.disk_type {
            0x16 => "80 track, double sided",
            0x17 => "40 track, double sided",
            0x18 => "80 track, single sided",
            0x19 => "40 track, single sided",
            _ => "unknown",
        }
    }
}

/// A parsed catalogue entry (16 bytes on disk).
#[derive(Clone, Debug)]
pub struct TrdosEntry {
    /// Slot index in the catalogue (0..127).
    pub slot: usize,
    /// The raw 8-byte name (space-padded), byte 0 holding the original name
    /// character (a live entry — deleted tombstones are filtered out).
    pub name: [u8; 8],
    /// Single-character file type (`B` BASIC, `C` CODE, `D` data array,
    /// `#` sequential, etc.).
    pub type_char: u8,
    /// Type-dependent parameter (CODE load address / BASIC autostart, LE).
    pub start_param: u16,
    /// Logical length in bytes.
    pub length_bytes: u16,
    /// Allocated length in sectors.
    pub length_sectors: u8,
    /// Starting sector within the starting track (0..15).
    pub start_sector: u8,
    /// Starting logical track.
    pub start_track: u8,
}

impl TrdosEntry {
    /// Linear sector index of the file's first sector.
    pub fn start_linear(&self) -> u64 {
        self.start_track as u64 * SPT + self.start_sector as u64
    }

    /// Byte size for display / extraction: the logical byte length when it fits
    /// the allocation, else the full sector allocation (robust on a foreign or
    /// slightly-inconsistent disk).
    pub fn byte_size(&self) -> u64 {
        let alloc = self.length_sectors as u64 * SECTOR;
        let lb = self.length_bytes as u64;
        if (1..=alloc).contains(&lb) {
            lb
        } else {
            alloc
        }
    }

    /// Display name: the trimmed 8-byte name plus a `.T` type suffix, so two
    /// entries sharing a name but differing in type (`boot.B` vs `boot.C`)
    /// stay distinct.
    pub fn display_name(&self) -> String {
        format!("{}.{}", trim_name(&self.name), printable(self.type_char))
    }
}

/// Trim trailing spaces from an 8-byte name and render printable ASCII,
/// replacing control bytes with `?` for display.
fn trim_name(name: &[u8]) -> String {
    let end = name.iter().rposition(|&b| b != b' ').map_or(0, |i| i + 1);
    name[..end].iter().map(|&b| printable(b)).collect()
}

fn printable(b: u8) -> char {
    if (0x20..0x7F).contains(&b) {
        b as char
    } else {
        '?'
    }
}

pub struct TrdosFilesystem<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    geometry: TrdosGeometry,
    volume_label: String,
    /// Free sectors from the disk-info sector.
    free_sectors: u16,
    /// Number of catalogue slots in use (live + tombstoned), from disk-info.
    num_files: u16,
    /// Live (non-deleted) entries, in catalogue order.
    entries: Vec<TrdosEntry>,
    /// Highest linear sector consumed by any allocation (defensive high-water
    /// mark for backup trimming: the max of the disk-info pointer and every
    /// entry's end, so a understated free-count never trims a real file).
    high_water: u64,
}

/// Parse a 16-byte catalogue entry at `off` into a live entry, or `None` when
/// the slot is empty (byte 0 == 0x00, end of catalogue) or a deleted tombstone
/// (byte 0 == 0x01).
fn parse_entry(cat: &[u8], slot: usize) -> Option<TrdosEntry> {
    let off = slot * ENTRY;
    let raw = &cat[off..off + ENTRY];
    if raw[0] == 0x00 || raw[0] == 0x01 {
        return None;
    }
    let mut name = [0u8; 8];
    name.copy_from_slice(&raw[0..8]);
    Some(TrdosEntry {
        slot,
        name,
        type_char: raw[8],
        start_param: u16::from_le_bytes([raw[9], raw[10]]),
        length_bytes: u16::from_le_bytes([raw[11], raw[12]]),
        length_sectors: raw[13],
        start_sector: raw[14],
        start_track: raw[15],
    })
}

/// True when the byte at `off` is 0x00 (end of catalogue).
fn is_end_of_catalog(cat: &[u8], slot: usize) -> bool {
    cat[slot * ENTRY] == 0x00
}

/// Structural detector: read the disk-info sector and confirm the TR-DOS
/// signature (id byte 0x10, a valid disk-type byte) plus a size/geometry sanity
/// check. Returns the parsed geometry on a match. Gated tightly so a same-sized
/// blob from another format doesn't false-positive.
pub fn looks_like_trdos<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
) -> Option<TrdosGeometry> {
    let len = reader
        .seek(SeekFrom::End(0))
        .ok()?
        .checked_sub(partition_offset)?;
    // Must be sector-aligned and at least large enough to hold catalogue+info.
    if len < INFO_OFF + SECTOR || len % SECTOR != 0 {
        return None;
    }
    reader
        .seek(SeekFrom::Start(partition_offset + INFO_OFF))
        .ok()?;
    let mut info = [0u8; SECTOR as usize];
    reader.read_exact(&mut info).ok()?;

    // The TR-DOS id byte and a valid disk-type byte are the signature.
    if info[0xE7] != TRDOS_ID {
        return None;
    }
    let geom = TrdosGeometry::from_type(info[0xE3])?;
    // The image must fit within the declared geometry (a truncated `.trd` that
    // drops trailing free sectors is fine; an oversized blob is not us).
    if len > geom.total_bytes() {
        return None;
    }
    // Counters must be in range (file count is a single byte at 0xE4).
    if info[0xE4] as usize > MAX_FILES {
        return None;
    }
    let free = u16::from_le_bytes([info[0xE5], info[0xE6]]);
    if free as u64 > geom.total_sectors() {
        return None;
    }
    // First-free pointer must land inside the volume.
    let ff_linear = info[0xE2] as u64 * SPT + info[0xE1] as u64;
    if ff_linear > geom.total_sectors() {
        return None;
    }
    Some(geom)
}

impl<R: Read + Seek> TrdosFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let geometry = looks_like_trdos(&mut reader, partition_offset)
            .ok_or_else(|| FilesystemError::InvalidData("not a TR-DOS volume".into()))?;

        // Read the catalogue (8 sectors) and the disk-info sector.
        let mut cat = vec![0u8; (CATALOG_SECTORS * SECTOR) as usize];
        reader.seek(SeekFrom::Start(partition_offset))?;
        reader.read_exact(&mut cat)?;
        let mut info = [0u8; SECTOR as usize];
        reader.seek(SeekFrom::Start(partition_offset + INFO_OFF))?;
        reader.read_exact(&mut info)?;

        let free_sectors = u16::from_le_bytes([info[0xE5], info[0xE6]]);
        let num_files = info[0xE4] as u16;
        let volume_label = trim_name(&info[0xF5..0xFD]);

        let mut entries = Vec::new();
        // Track the packed high-water mark across all used slots (live and
        // tombstoned) so a foreign / understated free-count can't hide a file.
        let mut packed_end = DATA_START;
        for slot in 0..MAX_FILES {
            if is_end_of_catalog(&cat, slot) {
                break;
            }
            let off = slot * ENTRY;
            let length_sectors = cat[off + 13] as u64;
            let start_linear = cat[off + 15] as u64 * SPT + cat[off + 14] as u64;
            packed_end = packed_end.max(start_linear + length_sectors);
            if let Some(e) = parse_entry(&cat, slot) {
                entries.push(e);
            }
        }
        let info_high_water = geometry.total_sectors().saturating_sub(free_sectors as u64);
        let high_water = packed_end
            .max(info_high_water)
            .min(geometry.total_sectors());

        Ok(TrdosFilesystem {
            reader,
            partition_offset,
            geometry,
            volume_label,
            free_sectors,
            num_files,
            entries,
            high_water,
        })
    }

    fn read_at(&mut self, off: u64, buf: &mut [u8]) -> Result<(), FilesystemError> {
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + off))?;
        self.reader.read_exact(buf)?;
        Ok(())
    }

    fn entry_to_file(&self, e: &TrdosEntry) -> FileEntry {
        let name = e.display_name();
        FileEntry::new_file(
            name.clone(),
            format!("/{name}"),
            e.byte_size(),
            e.start_linear(),
        )
    }

    /// Number of catalogue slots in use per the disk-info sector.
    pub fn declared_file_count(&self) -> u16 {
        self.num_files
    }
}

impl<R: Read + Seek + Send> Filesystem for TrdosFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if !entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        // Flat filesystem: only the root has entries.
        if entry.path != "/" {
            return Ok(Vec::new());
        }
        Ok(self
            .entries
            .clone()
            .iter()
            .map(|e| self.entry_to_file(e))
            .collect())
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::InvalidData(format!(
                "TR-DOS read_file on directory: {}",
                entry.path
            )));
        }
        let want = (entry.size as usize).min(max_bytes);
        if want == 0 {
            return Ok(Vec::new());
        }
        let mut buf = vec![0u8; want];
        self.read_at(entry.location * SECTOR, &mut buf)?;
        Ok(buf)
    }

    fn volume_label(&self) -> Option<&str> {
        if self.volume_label.is_empty() {
            None
        } else {
            Some(&self.volume_label)
        }
    }

    fn fs_type(&self) -> &str {
        "TR-DOS"
    }

    fn total_size(&self) -> u64 {
        self.geometry.total_bytes()
    }

    fn used_size(&self) -> u64 {
        let total = self.geometry.total_sectors();
        total.saturating_sub(self.free_sectors as u64) * SECTOR
    }

    fn allocation_unit(&self) -> Option<u64> {
        Some(SECTOR)
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.high_water * SECTOR)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::process::Command;

    /// Hand-build a minimal 640K TR-DOS image with a couple of files placed at
    /// known offsets — pins the reader to the on-disk spec independently of the
    /// oracle.
    fn forge_image() -> Vec<u8> {
        let geom = TrdosGeometry::from_type(0x16).unwrap();
        let mut img = vec![0u8; geom.total_bytes() as usize];

        // Two contiguous files starting at the first data sector (linear 16).
        // File A: "HELLO" type C, 300 bytes -> 2 sectors, at track 1 sector 0.
        // File B: "WORLD" type B, 256 bytes -> 1 sector, at track 1 sector 2.
        let mut put_entry =
            |slot: usize, name: &str, tc: u8, len: u16, secs: u8, st: u8, ss: u8| {
                let off = slot * ENTRY;
                let nm = format!("{name:<8}");
                img[off..off + 8].copy_from_slice(&nm.as_bytes()[..8]);
                img[off + 8] = tc;
                img[off + 9..off + 11].copy_from_slice(&0u16.to_le_bytes());
                img[off + 11..off + 13].copy_from_slice(&len.to_le_bytes());
                img[off + 13] = secs;
                img[off + 14] = ss;
                img[off + 15] = st;
            };
        put_entry(0, "HELLO", b'C', 300, 2, 1, 0);
        put_entry(1, "WORLD", b'B', 256, 1, 1, 2);

        // Payloads (track 1 sector 0 = linear 16 = SPT; track 1 sector 2 = 18).
        let a_off = (SPT * SECTOR) as usize;
        img[a_off..a_off + 300].fill(0xAA);
        let b_off = ((SPT + 2) * SECTOR) as usize;
        img[b_off..b_off + 256].fill(0xBB);

        // Disk-info sector.
        let base = INFO_OFF as usize;
        img[base + 0xE1] = 3; // first free sector (linear 19 = track 1 sector 3)
        img[base + 0xE2] = 1; // first free track
        img[base + 0xE3] = 0x16; // disk type
        img[base + 0xE4] = 2; // file count
        let ff_linear = SPT + 3; // track 1 sector 3 = linear 19
        let free = (geom.total_sectors() - ff_linear) as u16;
        img[base + 0xE5..base + 0xE7].copy_from_slice(&free.to_le_bytes());
        img[base + 0xE7] = TRDOS_ID;
        let label = b"TESTDISK";
        img[base + 0xF5..base + 0xFD].copy_from_slice(label);
        img
    }

    #[test]
    fn detects_and_reads_forged_image() {
        let img = forge_image();
        let mut fs = TrdosFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.fs_type(), "TR-DOS");
        assert_eq!(fs.volume_label(), Some("TESTDISK"));
        assert_eq!(fs.total_size(), 655_360);

        let root = fs.root().unwrap();
        let files = fs.list_directory(&root).unwrap();
        let names: Vec<&str> = files.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"HELLO.C"), "{names:?}");
        assert!(names.contains(&"WORLD.B"), "{names:?}");

        let hello = files.iter().find(|e| e.name == "HELLO.C").unwrap();
        assert_eq!(hello.size, 300);
        let data = fs.read_file(hello, usize::MAX).unwrap();
        assert_eq!(data.len(), 300);
        assert!(data.iter().all(|&b| b == 0xAA));

        let world = files.iter().find(|e| e.name == "WORLD.B").unwrap();
        assert_eq!(world.size, 256);
        let data = fs.read_file(world, usize::MAX).unwrap();
        assert!(data.iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn rejects_non_trdos() {
        // All zeros: no id byte.
        let img = vec![0u8; 655_360];
        assert!(looks_like_trdos(&mut Cursor::new(img), 0).is_none());
        // Random-ish with a bad type byte but a good id byte.
        let mut img = vec![0u8; 655_360];
        img[INFO_OFF as usize + 0xE7] = TRDOS_ID;
        img[INFO_OFF as usize + 0xE3] = 0x99; // invalid type
        assert!(looks_like_trdos(&mut Cursor::new(img), 0).is_none());
    }

    #[test]
    fn geometry_sizes() {
        assert_eq!(
            TrdosGeometry::from_type(0x16).unwrap().total_bytes(),
            640 * 1024
        );
        assert_eq!(
            TrdosGeometry::from_type(0x17).unwrap().total_bytes(),
            320 * 1024
        );
        assert_eq!(
            TrdosGeometry::from_type(0x18).unwrap().total_bytes(),
            320 * 1024
        );
        assert_eq!(
            TrdosGeometry::from_type(0x19).unwrap().total_bytes(),
            160 * 1024
        );
        assert!(TrdosGeometry::from_type(0x00).is_none());
    }

    // ---- Oracle cross-validation ----

    fn oracle() -> Option<PathBuf> {
        let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/trdos-oracle.py");
        p.exists().then_some(p)
    }

    fn have_python() -> bool {
        Command::new("python3").arg("--version").output().is_ok()
    }

    /// Build a real TR-DOS volume with the clean-room oracle (mkfs + a couple of
    /// files) and return the raw image, or None if python3/oracle unavailable.
    fn oracle_volume() -> Option<Vec<u8>> {
        let script = oracle()?;
        if !have_python() {
            return None;
        }
        let dir = std::env::temp_dir();
        let vol = dir.join(format!("rb_trdos_{}.trd", std::process::id()));
        let volp = vol.to_str()?;
        let run = |args: &[&str]| {
            Command::new("python3")
                .arg(&script)
                .args(args)
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false)
        };
        if !run(&["mkfs", volp, "80DS", "ORACLE"]) {
            return None;
        }
        let hi = dir.join(format!("rb_trdos_hi_{}.bin", std::process::id()));
        std::fs::write(&hi, vec![0x41u8; 700]).ok()?; // 700 bytes -> 3 sectors
        let big = dir.join(format!("rb_trdos_big_{}.bin", std::process::id()));
        std::fs::write(&big, vec![0x42u8; 5000]).ok()?; // 5000 bytes -> 20 sectors
        run(&["addfile", volp, "HELLO", "C", hi.to_str()?]);
        run(&["addfile", volp, "BIGFILE", "C", big.to_str()?]);
        let bytes = std::fs::read(&vol).ok();
        let _ = std::fs::remove_file(&vol);
        let _ = std::fs::remove_file(&hi);
        let _ = std::fs::remove_file(&big);
        bytes
    }

    #[test]
    fn reads_oracle_volume() {
        let Some(img) = oracle_volume() else {
            eprintln!("skipping reads_oracle_volume: python3/oracle unavailable");
            return;
        };
        let mut fs = TrdosFilesystem::open(Cursor::new(img), 0).expect("open oracle volume");
        assert_eq!(fs.volume_label(), Some("ORACLE"));
        assert_eq!(fs.total_size(), 655_360);
        let root = fs.root().unwrap();
        let files = fs.list_directory(&root).unwrap();
        let names: Vec<&str> = files.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"HELLO.C"), "{names:?}");
        assert!(names.contains(&"BIGFILE.C"), "{names:?}");

        let big = files.iter().find(|e| e.name == "BIGFILE.C").unwrap();
        assert_eq!(big.size, 5000);
        let data = fs.read_file(big, usize::MAX).unwrap();
        assert_eq!(data.len(), 5000);
        assert!(data.iter().all(|&b| b == 0x42));
    }
}
