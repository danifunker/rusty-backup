//! Sun disk label (SMI VTOC) — the partition scheme SPARC Solaris / SunOS
//! disks use instead of MBR/GPT.
//!
//! A single 512-byte label at sector 0 describes disk geometry and up to **8
//! slices** (`{start_cylinder, num_sectors}`). All multi-byte fields are
//! big-endian. This unlocks reading real Sun SPARC disk images — the UFS
//! driver (`src/fs/ufs.rs`, big-endian SPARC variant included) is already in
//! place; this is the missing piece that tells it where the slices live.
//!
//! Layout (from the Linux kernel `block/partitions/sun.c`, the reference this
//! is modeled on and — via `fdisk`/`sfdisk` — validated against):
//! - `info[128]` @0, VTOC block @128 (`version` be32, `volume[8]`, `nparts`
//!   be16, then 8 × `{id, flags}` be16 pairs, `sanity` be32 = `0x600DDEEE`),
//! - geometry: `ntrks` (heads) be16 @436, `nsect` (sectors/track) be16 @438;
//!   sectors-per-cylinder = `ntrks * nsect`,
//! - `partitions[8]` @444: each `{start_cylinder be32, num_sectors be32}`; a
//!   slice's start sector = `start_cylinder * spc`,
//! - `magic` be16 @508 = `0xDABE`, `csum` be16 @510 (XOR of all 256 words = 0).
//!
//! Slice **tag 5** (`SUN_WHOLE_DISK`, conventionally slice 2 "backup") spans
//! the entire disk and overlaps the real slices, so it is skipped from the
//! browse list — exactly like the SGI VOLHDR/VOLUME disk-wide wrappers.

use byteorder::{BigEndian, ByteOrder};
use serde::{Deserialize, Serialize};

use crate::error::RustyBackupError;

/// Magic at byte 508 of the 512-byte label (big-endian).
pub const SUN_LABEL_MAGIC: u16 = 0xDABE;
/// VTOC sanity value at offset 188 (big-endian) when the tag table is valid.
pub const SUN_VTOC_SANITY: u32 = 0x600D_DEEE;
/// Partition tag for the whole-disk "backup" slice (overlaps everything).
pub const SUN_TAG_WHOLE_DISK: u16 = 5;

const LABEL_SIZE: usize = 512;
const N_SLICES: usize = 8;

/// One Sun slice, resolved to absolute sectors.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SunSlice {
    /// Cylinder-relative start (as stored on disk).
    pub start_cylinder: u32,
    /// Absolute start sector = `start_cylinder * sectors_per_cylinder`.
    pub start_sector: u64,
    /// Length in 512-byte sectors.
    pub num_sectors: u32,
    /// VTOC partition tag (`id`), when the VTOC is valid; else 0.
    pub tag: u16,
    /// VTOC flags (bit 0 = unmountable, bit 1 = read-only), when valid.
    pub flags: u16,
}

impl SunSlice {
    pub fn is_empty(&self) -> bool {
        self.num_sectors == 0
    }
    pub fn is_whole_disk(&self) -> bool {
        self.tag == SUN_TAG_WHOLE_DISK
    }
    pub fn size_bytes(&self) -> u64 {
        self.num_sectors as u64 * 512
    }
    pub fn start_offset(&self) -> u64 {
        self.start_sector * 512
    }
    /// Plain-ASCII name for the VTOC tag (no Unicode glyphs).
    pub fn tag_name(&self) -> &'static str {
        match self.tag {
            0 => "unassigned",
            1 => "boot",
            2 => "root",
            3 => "swap",
            4 => "usr",
            5 => "backup",
            6 => "stand",
            7 => "var",
            8 => "home",
            9 => "altsctr",
            10 => "cache",
            11 => "reserved",
            _ => "slice",
        }
    }
}

/// Resolve a slice-tag spelling — a name as [`SunSlice::tag_name`] prints it,
/// or a bare decimal number — to its VTOC tag.
pub fn tag_from_text(text: &str) -> Option<u16> {
    let t = text.trim();
    if let Ok(n) = t.parse::<u16>() {
        return Some(n);
    }
    let lower = t.to_ascii_lowercase();
    (0..=11u16).find(|&tag| {
        SunSlice {
            start_cylinder: 0,
            start_sector: 0,
            num_sectors: 0,
            tag,
            flags: 0,
        }
        .tag_name()
            == lower
    })
}

/// A parsed Sun disk label.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SunDiskLabel {
    pub volume: String,
    /// Data cylinder count.
    pub ncyl: u16,
    /// Tracks (heads) per cylinder.
    pub ntrks: u16,
    /// Sectors per track.
    pub nsect: u16,
    /// Sectors per cylinder = `ntrks * nsect`.
    pub sectors_per_cylinder: u64,
    /// True when the VTOC tag table validated (sanity + version + nparts).
    pub vtoc_valid: bool,
    pub slices: [SunSlice; N_SLICES],
}

impl SunDiskLabel {
    /// Structural detector: magic `0xDABE` at offset 508 plus a valid label
    /// checksum (XOR of all 256 big-endian words is zero).
    pub fn detect(buf: &[u8]) -> bool {
        if buf.len() < LABEL_SIZE {
            return false;
        }
        if BigEndian::read_u16(&buf[508..510]) != SUN_LABEL_MAGIC {
            return false;
        }
        checksum_ok(&buf[..LABEL_SIZE])
    }

    pub fn parse(buf: &[u8]) -> Result<Self, RustyBackupError> {
        if buf.len() < LABEL_SIZE {
            return Err(RustyBackupError::InvalidMbr(
                "Sun label: buffer smaller than 512 bytes".into(),
            ));
        }
        if BigEndian::read_u16(&buf[508..510]) != SUN_LABEL_MAGIC {
            return Err(RustyBackupError::InvalidMbr(
                "Sun label: bad magic (expected 0xDABE)".into(),
            ));
        }
        if !checksum_ok(&buf[..LABEL_SIZE]) {
            return Err(RustyBackupError::InvalidMbr(
                "Sun label: checksum mismatch".into(),
            ));
        }

        let version = BigEndian::read_u32(&buf[128..132]);
        let volume = {
            let raw = &buf[132..140];
            let end = raw.iter().position(|&c| c == 0).unwrap_or(raw.len());
            String::from_utf8_lossy(&raw[..end]).trim().to_string()
        };
        let nparts = BigEndian::read_u16(&buf[140..142]);
        let sanity = BigEndian::read_u32(&buf[188..192]);

        // Mirror the kernel's `use_vtoc` gate: either a fully sane VTOC, or the
        // old-Linux-Sun convention where sanity/version/nparts are all zero.
        let vtoc_valid = (sanity == SUN_VTOC_SANITY && version == 1 && nparts <= 8)
            || (sanity == 0 && version == 0 && nparts == 0);

        let ntrks = BigEndian::read_u16(&buf[436..438]);
        let nsect = BigEndian::read_u16(&buf[438..440]);
        let ncyl = BigEndian::read_u16(&buf[432..434]);
        let spc = ntrks as u64 * nsect as u64;

        let mut slices = [SunSlice {
            start_cylinder: 0,
            start_sector: 0,
            num_sectors: 0,
            tag: 0,
            flags: 0,
        }; N_SLICES];
        for (i, slice) in slices.iter_mut().enumerate() {
            let base = 444 + i * 8;
            let start_cylinder = BigEndian::read_u32(&buf[base..base + 4]);
            let num_sectors = BigEndian::read_u32(&buf[base + 4..base + 8]);
            let (tag, flags) = if vtoc_valid {
                let ib = 142 + i * 4;
                (
                    BigEndian::read_u16(&buf[ib..ib + 2]),
                    BigEndian::read_u16(&buf[ib + 2..ib + 4]),
                )
            } else {
                (0, 0)
            };
            *slice = SunSlice {
                start_cylinder,
                start_sector: start_cylinder as u64 * spc,
                num_sectors,
                tag,
                flags,
            };
        }

        Ok(SunDiskLabel {
            volume,
            ncyl,
            ntrks,
            nsect,
            sectors_per_cylinder: spc,
            vtoc_valid,
            slices,
        })
    }

    /// Non-empty slices in slice order, skipping the whole-disk "backup"
    /// alias (tag 5), which overlaps the real slices.
    pub fn browsable_slices(&self) -> impl Iterator<Item = (usize, &SunSlice)> {
        self.slices
            .iter()
            .enumerate()
            .filter(|(_, s)| !s.is_empty() && !s.is_whole_disk())
    }
}

/// The label checksum is a 16-bit XOR of all 256 big-endian words in the
/// 512-byte label; a correct label (with its stored `csum`) XORs to zero.
fn checksum_ok(label: &[u8]) -> bool {
    let mut csum: u16 = 0;
    for w in label.chunks_exact(2) {
        csum ^= BigEndian::read_u16(w);
    }
    csum == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn have(tool: &str) -> bool {
        Command::new(tool).arg("--help").output().is_ok()
    }

    /// Image bytes + `fdisk -l`'s `(start_sector, num_sectors)` per slice.
    type SunFixture = (Vec<u8>, Vec<(u64, u64)>);

    /// Build a Sun-labeled disk image with `sfdisk` (non-sudo, on a file) with
    /// a known slice layout, and return its bytes + the parsed `fdisk -l`
    /// (start_sector, num_sectors) per line for cross-checking.
    fn sfdisk_sun_image() -> Option<SunFixture> {
        if !have("sfdisk") || !have("fdisk") {
            return None;
        }
        let dir = std::env::temp_dir();
        let img = dir.join(format!("rb_sun_{}.img", std::process::id()));
        // 100 MiB.
        std::fs::write(&img, vec![0u8; 100 * 1024 * 1024]).ok()?;
        // slice 1 (root), slice 2 (whole-disk backup), slice 3 (usr).
        let script = "label: sun\n\
             1 : start=0, size=40000, type=2\n\
             2 : start=0, size=204800, type=5\n\
             3 : start=40000, size=100000, type=4\n";
        let ok = Command::new("sfdisk")
            .arg(&img)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .and_then(|mut c| {
                use std::io::Write;
                c.stdin.take().unwrap().write_all(script.as_bytes())?;
                c.wait()
            })
            .map(|s| s.success())
            .unwrap_or(false);
        if !ok {
            let _ = std::fs::remove_file(&img);
            return None;
        }
        // Parse `fdisk -l` for the authoritative Start/Sectors of each slice.
        let out = Command::new("fdisk").arg("-l").arg(&img).output().ok()?;
        let text = String::from_utf8_lossy(&out.stdout);
        let mut rows = Vec::new();
        for line in text.lines() {
            if line.contains(&*img.to_string_lossy()) {
                let cols: Vec<&str> = line.split_whitespace().collect();
                // Device Start End Sectors Size Id Type...
                if cols.len() >= 4 {
                    if let (Ok(start), Ok(sectors)) =
                        (cols[1].parse::<u64>(), cols[3].parse::<u64>())
                    {
                        rows.push((start, sectors));
                    }
                }
            }
        }
        let bytes = std::fs::read(&img).ok();
        let _ = std::fs::remove_file(&img);
        bytes.map(|b| (b, rows))
    }

    #[test]
    fn parses_sfdisk_sun_label_matching_fdisk() {
        let Some((img, fdisk_rows)) = sfdisk_sun_image() else {
            eprintln!("skipping: sfdisk/fdisk unavailable");
            return;
        };
        assert!(SunDiskLabel::detect(&img[..512]), "detect failed");
        let label = SunDiskLabel::parse(&img[..512]).expect("parse");

        // Our non-empty slices, in order, must match fdisk's Start/Sectors.
        let ours: Vec<(u64, u64)> = label
            .slices
            .iter()
            .filter(|s| !s.is_empty())
            .map(|s| (s.start_sector, s.num_sectors as u64))
            .collect();
        assert_eq!(
            ours, fdisk_rows,
            "slice geometry disagrees with fdisk -l (ours vs fdisk)"
        );

        // The whole-disk backup slice (tag 5) is present but excluded from browse.
        assert!(label.slices.iter().any(|s| s.is_whole_disk()));
        let browsable: Vec<u16> = label.browsable_slices().map(|(_, s)| s.tag).collect();
        assert_eq!(browsable, vec![2, 4], "root + usr, backup excluded");
    }

    /// Wrap the real `test_ufs1.img` fixture in a Sun label (slice 0 at
    /// cylinder 0, exactly how a Sun UFS root disk is laid out) and confirm the
    /// parsed slice 0 lands the UFS super block where the driver expects it —
    /// i.e. the label + slice offset correctly locate the filesystem.
    #[test]
    fn locates_ufs_filesystem_in_slice() {
        if !have("sfdisk") || !have("zstd") {
            eprintln!("skipping: sfdisk/zstd unavailable");
            return;
        }
        let dir = std::env::temp_dir();
        let ufs = dir.join(format!("rb_sunufs_ufs_{}.img", std::process::id()));
        let disk = dir.join(format!("rb_sunufs_{}.img", std::process::id()));
        let fixture = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/test_ufs1.img.zst"
        );
        let ok = Command::new("zstd")
            .args(["-dfq", fixture, "-o"])
            .arg(&ufs)
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if !ok {
            eprintln!("skipping: could not decompress fixture");
            return;
        }
        let ufs_bytes = std::fs::read(&ufs).unwrap();
        let spc: u64 = 16065; // sfdisk's default Sun geometry (255 * 63)
        let cyls = ufs_bytes.len() as u64 / 512 / spc + 2;
        std::fs::write(&disk, vec![0u8; (cyls * spc * 512) as usize]).unwrap();
        // Place the UFS filesystem at offset 0.
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().write(true).open(&disk).unwrap();
            f.write_all(&ufs_bytes).unwrap();
        }
        let slice_secs = ufs_bytes.len() as u64 / 512;
        let slice_cyls = slice_secs.div_ceil(spc);
        let script = format!(
            "label: sun\n1 : start=0, size={}, type=2\n",
            slice_cyls * spc
        );
        let applied = Command::new("sfdisk")
            .arg(&disk)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .and_then(|mut c| {
                use std::io::Write;
                c.stdin.take().unwrap().write_all(script.as_bytes())?;
                c.wait()
            })
            .map(|s| s.success())
            .unwrap_or(false);

        let result = (|| {
            if !applied {
                return None;
            }
            let img = std::fs::read(&disk).ok()?;
            assert!(SunDiskLabel::detect(&img[..512]), "not detected as Sun");
            let label = SunDiskLabel::parse(&img[..512]).ok()?;
            let s0 = label.slices.iter().find(|s| !s.is_empty())?;
            assert_eq!(s0.start_sector, 0, "slice 0 should start at cylinder 0");
            // UFS1 super block is at slice_offset + 8192; its magic (0x00011954)
            // sits at +1372 within the super block.
            let magic_off = (s0.start_offset() + 8192 + 1372) as usize;
            let magic = u32::from_le_bytes([
                img[magic_off],
                img[magic_off + 1],
                img[magic_off + 2],
                img[magic_off + 3],
            ]);
            assert_eq!(
                magic, 0x0001_1954,
                "UFS1 super block not where the slice points"
            );
            Some(())
        })();

        let _ = std::fs::remove_file(&ufs);
        let _ = std::fs::remove_file(&disk);
        if result.is_none() {
            eprintln!("skipping: sfdisk could not apply the Sun label");
        }
    }

    #[test]
    fn rejects_non_sun() {
        assert!(!SunDiskLabel::detect(&[0u8; 512]));
        // Right magic, bad checksum.
        let mut buf = [0u8; 512];
        buf[508] = 0xDA;
        buf[509] = 0xBE;
        buf[0] = 0x01; // perturb so XOR != 0
        assert!(!SunDiskLabel::detect(&buf));
    }
}
