//! UCSD p-System filesystem — read driver.
//!
//! The filesystem of UCSD Pascal (Apple II/III, PC, and many 1970s-80s
//! machines). It is deliberately tiny: a single **flat** directory (no
//! subdirectories) of at most 77 files, each stored as a **contiguous** run of
//! 512-byte blocks. There is no allocation bitmap — the sorted directory *is*
//! the allocation map, and free space is the gaps between files.
//!
//! Layout (from `ucsdpsys_fs(5)`):
//!
//! ```text
//!   block 0,1     bootstrap
//!   block 2..5    directory (4 blocks = 78 x 26-byte entries)
//!   block 6..     file data
//! ```
//!
//! Directory entry 0 is the **volume label**; entries 1..77 are files. Every
//! multi-byte word takes the host CPU's byte order; it is recovered from the
//! volume label's `DLASTBLK` field, which is always 6 or 10 (so its high byte
//! is zero). All offsets below were verified byte-for-byte against a clean-room
//! oracle (`scripts/ucsd-oracle.py`) built from the same spec.
//!
//! This is the read half of the UCSD quartet (Browse); edit / create / fsck
//! build on it (`docs/filesystem_completion_plan.md` Part 2).

use std::io::{Read, Seek, SeekFrom};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

const BLOCK: u64 = 512;
/// Directory starts at block 2 and spans 4 blocks.
const DIR_BLOCK: u64 = 2;
const DIR_BLOCKS: u64 = 4;
/// Directory-entry stride.
const ENTRY: usize = 26;
/// First data block after a single (non-duplicated) directory.
const DIR_END: u16 = 6;
/// First data block after a duplicated (backed-up) directory.
const DIR_END_DUP: u16 = 10;

const KIND_NAMES: [&str; 9] = [
    "untyped",
    "xdsk",
    "code",
    "text",
    "info",
    "data",
    "graf",
    "foto",
    "securedir",
];

/// A parsed file directory entry.
#[derive(Clone, Debug)]
pub struct UcsdEntry {
    pub first_block: u16,
    pub last_block: u16, // first block past the file
    pub kind: u8,
    pub name: String,
    pub last_byte: u16, // bytes used in the final block (1..=512)
    pub date: u16,
}

impl UcsdEntry {
    fn byte_size(&self) -> u64 {
        if self.last_block <= self.first_block {
            return 0;
        }
        (self.last_block - self.first_block - 1) as u64 * BLOCK + self.last_byte as u64
    }
}

pub struct UcsdFilesystem<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    volume_name: String,
    /// Blocks on the volume, from the label (`DEOVBLK`).
    eov_blocks: u16,
    entries: Vec<UcsdEntry>,
}

fn read_u16(buf: &[u8], off: usize, little: bool) -> u16 {
    if little {
        u16::from_le_bytes([buf[off], buf[off + 1]])
    } else {
        u16::from_be_bytes([buf[off], buf[off + 1]])
    }
}

/// A UCSD name field: byte `off` is the length, then 7-bit ASCII. `max` is 7
/// for the volume label, 15 for files. Returns None if the length is invalid.
fn read_name(buf: &[u8], off: usize, max: usize) -> Option<String> {
    let n = buf[off] as usize;
    if n == 0 || n > max {
        return None;
    }
    let bytes = &buf[off + 1..off + 1 + n];
    if bytes.iter().any(|&b| !(0x20..=0x7E).contains(&b)) {
        return None;
    }
    Some(String::from_utf8_lossy(bytes).into_owned())
}

/// Recover byte order from the volume-label DLASTBLK (bytes 2..3 of block 2),
/// which is 6 or 10. Returns `Some(little_endian)` or None if it looks wrong.
fn detect_byte_sex(dir: &[u8]) -> Option<bool> {
    let (b2, b3) = (dir[2], dir[3]);
    if (b2 == 6 || b2 == 10) && b3 == 0 {
        Some(true)
    } else if (b3 == 6 || b3 == 10) && b2 == 0 {
        Some(false)
    } else {
        None
    }
}

/// True if the block-2 buffer looks like a UCSD volume label. Used for content
/// detection. Requires ≥ 32 bytes (one directory entry).
pub fn looks_like_ucsd(dir: &[u8]) -> bool {
    if dir.len() < ENTRY + 6 {
        return false;
    }
    let Some(little) = detect_byte_sex(dir) else {
        return false;
    };
    // FIRSTBLK == 0, kind == 0, DLASTBLK in {6,10}, a valid 1..7 volume name,
    // and a plausible non-zero volume block count.
    if read_u16(dir, 0, little) != 0 {
        return false;
    }
    let dlast = read_u16(dir, 2, little);
    if dlast != DIR_END && dlast != DIR_END_DUP {
        return false;
    }
    if read_u16(dir, 4, little) != 0 {
        return false;
    }
    if read_name(dir, 6, 7).is_none() {
        return false;
    }
    let eov = read_u16(dir, 14, little);
    // At least the directory + one block, and not absurdly small.
    eov > DIR_END
}

impl<R: Read + Seek> UcsdFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let mut dir = vec![0u8; (DIR_BLOCKS * BLOCK) as usize];
        reader.seek(SeekFrom::Start(partition_offset + DIR_BLOCK * BLOCK))?;
        reader.read_exact(&mut dir)?;
        if !looks_like_ucsd(&dir) {
            return Err(FilesystemError::InvalidData(
                "not a UCSD p-System volume label".into(),
            ));
        }
        let little = detect_byte_sex(&dir).unwrap();
        let volume_name = read_name(&dir, 6, 7).unwrap_or_default();
        let eov_blocks = read_u16(&dir, 14, little);
        let num_files = read_u16(&dir, 16, little) as usize;

        let mut entries = Vec::new();
        // Entries 1..=num_files (bounded by the 77-file / directory capacity).
        let max_entries = ((DIR_BLOCKS * BLOCK) as usize / ENTRY).saturating_sub(1);
        for i in 1..=num_files.min(max_entries) {
            let off = i * ENTRY;
            let first_block = read_u16(&dir, off, little);
            let last_block = read_u16(&dir, off + 2, little);
            let kind = (read_u16(&dir, off + 4, little) & 0xF) as u8;
            let Some(name) = read_name(&dir, off + 6, 15) else {
                continue;
            };
            let last_byte = read_u16(&dir, off + 22, little);
            let date = read_u16(&dir, off + 24, little);
            entries.push(UcsdEntry {
                first_block,
                last_block,
                kind,
                name,
                last_byte,
                date,
            });
        }

        Ok(UcsdFilesystem {
            reader,
            partition_offset,
            volume_name,
            eov_blocks,
            entries,
        })
    }

    fn read_at(&mut self, off: u64, buf: &mut [u8]) -> Result<(), FilesystemError> {
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + off))?;
        self.reader.read_exact(buf)?;
        Ok(())
    }

    fn entry_to_file(&self, e: &UcsdEntry) -> FileEntry {
        let mut fe = FileEntry::new_file(
            e.name.clone(),
            format!("/{}", e.name),
            e.byte_size(),
            e.first_block as u64,
        );
        fe.modified = decode_date(e.date);
        fe
    }
}

/// UCSD date word: bits 0-4 day (1..31), 5-8 month (1..12), 9-15 year (0..99).
fn decode_date(word: u16) -> Option<String> {
    if word == 0 {
        return None;
    }
    let day = word & 0x1F;
    let month = (word >> 5) & 0x0F;
    let year = (word >> 9) & 0x7F;
    if day == 0 || month == 0 || month > 12 {
        return None;
    }
    // Two-digit year: the p-System treats 0..=79 as 20xx conventionally, but we
    // just render the raw fields (the era is ambiguous on disk).
    Some(format!("{:04}-{:02}-{:02}", 1900 + year as u32, month, day))
}

impl<R: Read + Seek + Send> Filesystem for UcsdFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if !entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        // The filesystem is flat: only the root directory has entries.
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
                "UCSD read_file on directory: {}",
                entry.path
            )));
        }
        let want = (entry.size as usize).min(max_bytes);
        if want == 0 {
            return Ok(Vec::new());
        }
        let mut buf = vec![0u8; want];
        self.read_at(entry.location * BLOCK, &mut buf)?;
        Ok(buf)
    }

    fn volume_label(&self) -> Option<&str> {
        if self.volume_name.is_empty() {
            None
        } else {
            Some(&self.volume_name)
        }
    }

    fn fs_type(&self) -> &str {
        "UCSD p-System"
    }

    fn total_size(&self) -> u64 {
        self.eov_blocks as u64 * BLOCK
    }

    fn used_size(&self) -> u64 {
        // Boot + directory, plus every file's block span.
        let meta = DIR_END as u64 * BLOCK;
        let files: u64 = self
            .entries
            .iter()
            .map(|e| (e.last_block.saturating_sub(e.first_block)) as u64 * BLOCK)
            .sum();
        meta + files
    }

    fn allocation_unit(&self) -> Option<u64> {
        Some(BLOCK)
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        let last = self
            .entries
            .iter()
            .map(|e| e.last_block)
            .max()
            .unwrap_or(DIR_END);
        Ok(last as u64 * BLOCK)
    }
}

/// Human-readable file-kind name (for callers that want the type).
pub fn kind_name(kind: u8) -> &'static str {
    KIND_NAMES.get(kind as usize).copied().unwrap_or("unknown")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::process::Command;

    fn oracle() -> Option<PathBuf> {
        let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/ucsd-oracle.py");
        p.exists().then_some(p)
    }

    /// Build a real UCSD volume with the clean-room oracle: mkfs + a couple of
    /// files. Returns the raw image bytes, or None if python3 is unavailable.
    fn oracle_volume() -> Option<Vec<u8>> {
        let script = oracle()?;
        if Command::new("python3").arg("--version").output().is_err() {
            return None;
        }
        let dir = std::env::temp_dir();
        let vol = dir.join(format!("rb_ucsd_{}.vol", std::process::id()));
        let run = |args: &[&str]| {
            Command::new("python3")
                .arg(&script)
                .args(args)
                .output()
                .ok()
                .map(|o| o.status.success())
                .unwrap_or(false)
        };
        let volp = vol.to_str()?;
        if !run(&["mkfs", volp, "280", "MYVOL"]) {
            return None;
        }
        let hi = dir.join(format!("rb_ucsd_hi_{}.txt", std::process::id()));
        std::fs::write(&hi, b"HELLO UCSD\n").ok()?;
        let big = dir.join(format!("rb_ucsd_big_{}.dat", std::process::id()));
        std::fs::write(&big, vec![0x41u8; 3000]).ok()?;
        run(&["addfile", volp, "HELLO.TEXT", "3", hi.to_str()?]);
        run(&["addfile", volp, "BIG.DATA", "5", big.to_str()?]);
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
        let mut fs = UcsdFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.volume_label(), Some("MYVOL"));
        assert_eq!(fs.total_size(), 280 * BLOCK);
        let root = fs.root().unwrap();
        let files = fs.list_directory(&root).unwrap();
        let names: Vec<&str> = files.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"HELLO.TEXT"), "{names:?}");
        assert!(names.contains(&"BIG.DATA"), "{names:?}");

        let big = files.iter().find(|e| e.name == "BIG.DATA").unwrap();
        assert_eq!(big.size, 3000);
        let data = fs.read_file(big, usize::MAX).unwrap();
        assert_eq!(data.len(), 3000);
        assert!(data.iter().all(|&b| b == 0x41));
    }

    #[test]
    fn detection_and_byte_sex() {
        // Minimal little-endian volume label.
        let mut dir = [0u8; ENTRY * 2];
        dir[0..2].copy_from_slice(&0u16.to_le_bytes()); // FIRSTBLK
        dir[2..4].copy_from_slice(&6u16.to_le_bytes()); // DLASTBLK
        dir[4..6].copy_from_slice(&0u16.to_le_bytes()); // kind
        dir[6] = 5;
        dir[7..12].copy_from_slice(b"MYVOL");
        dir[14..16].copy_from_slice(&280u16.to_le_bytes()); // DEOVBLK
        assert!(looks_like_ucsd(&dir));
        assert_eq!(detect_byte_sex(&dir), Some(true));

        // Big-endian: DLASTBLK high byte holds the 6.
        let mut be = [0u8; ENTRY * 2];
        be[2..4].copy_from_slice(&6u16.to_be_bytes());
        be[6] = 3;
        be[7..10].copy_from_slice(b"VOL");
        be[14..16].copy_from_slice(&280u16.to_be_bytes());
        assert_eq!(detect_byte_sex(&be), Some(false));
        assert!(looks_like_ucsd(&be));

        // Random data is rejected.
        assert!(!looks_like_ucsd(&[0xABu8; ENTRY * 2]));
    }

    #[test]
    fn date_decode() {
        // year 84, month 6, day 15 -> (84<<9)|(6<<5)|15
        let word = (84u16 << 9) | (6 << 5) | 15;
        assert_eq!(decode_date(word).as_deref(), Some("1984-06-15"));
        assert_eq!(decode_date(0), None);
    }
}
