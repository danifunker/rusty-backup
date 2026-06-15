//! Xerox Alto-family filesystems (BFS / TFS / Alto OS) and their disk-image
//! containers.
//!
//! These vintage disks store each sector as three independently addressed
//! fields — a 2-word **header** (disk address), an 8- or 10-word **label**
//! (file identity + page-chain links + free/used state), and the 256- or
//! 1024-word **data**. The filesystem's whole structure lives in the labels,
//! not the data area, so any faithful image must preserve them. See
//! `~/docs/PARC_FS.md` and `~/docs/PARC_DISK_IMAGE_SPEC.md` for the full
//! analysis and the container spec.
//!
//! This module is read-first scaffolding for that work:
//! - [`copydisk`] — import the period CopyDisk stream packs (`.bfs` /
//!   `.copydisk` / `.altodisk`) into an in-memory [`Disk`].
//! - [`pdi`] — read/write the flat, self-describing **PARC Disk Image** (PDI)
//!   container.
//! - [`bfs`] — read-only walk of the Alto Basic File System over a [`Disk`].
//!
//! On-disk structure references: `AltoFileSys.d` (file pointers, leader pages,
//! directory entries), `BFS.d` (Diablo geometry, disk-address + label layout),
//! `BFSBase.bcpl` (the real-DA <-> virtual-DA mapping reproduced in
//! [`Geometry::vda_from_da`]). All multi-byte fields are big-endian 16-bit
//! words (the Alto is a 16-bit big-endian machine).

pub mod bfs;
pub mod copydisk;
pub mod pdi;
pub mod write;

use super::filesystem::FilesystemError;

/// Open a disk pack from raw bytes, auto-detecting the container: a PDI image
/// (magic `PARCDISK`) or a period CopyDisk stream (`.bfs` / `.copydisk` /
/// `.altodisk`).
pub fn open_pack(bytes: &[u8]) -> Result<Disk, FilesystemError> {
    if bytes.len() >= pdi::MAGIC.len() && &bytes[..pdi::MAGIC.len()] == pdi::MAGIC {
        pdi::read(bytes)
    } else {
        copydisk::read(bytes)
    }
}

/// Read a big-endian 16-bit word at byte offset `i` in `b`.
#[inline]
pub(crate) fn be16(b: &[u8], i: usize) -> u16 {
    ((b[i] as u16) << 8) | (b[i + 1] as u16)
}

/// Write a big-endian 16-bit word at byte offset `i` in `b`.
#[inline]
pub(crate) fn put_be16(b: &mut [u8], i: usize, v: u16) {
    b[i] = (v >> 8) as u8;
    b[i + 1] = (v & 0xff) as u8;
}

/// Filesystem / disk family. Selects label and data sizes and the
/// disk-address encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsFamily {
    /// Alto on Diablo Model 31/44 (BFS, Alto OS): 512-byte data, 8-word label.
    Diablo,
    /// Alto on Trident T-80/T-300 (TFS): 2048-byte data, 10-word label.
    Trident,
    /// Pilot / Cedar nucleus: 512-byte data, 10-word label.
    Pilot,
}

impl FsFamily {
    /// PDI `fsFamily` header code.
    pub fn code(self) -> u16 {
        match self {
            FsFamily::Diablo => 0,
            FsFamily::Trident => 1,
            FsFamily::Pilot => 2,
        }
    }

    pub fn from_code(c: u16) -> Option<Self> {
        match c {
            0 => Some(FsFamily::Diablo),
            1 => Some(FsFamily::Trident),
            2 => Some(FsFamily::Pilot),
            _ => None,
        }
    }
}

/// Geometry of a whole disk pack plus its per-sector field sizes.
#[derive(Debug, Clone)]
pub struct Geometry {
    pub family: FsFamily,
    /// Advisory model number (31, 44, 80, 300; 0 = unspecified).
    pub disk_model: u16,
    pub n_disks: u16,
    pub n_cylinders: u16,
    pub n_heads: u16,
    pub n_sectors: u16,
    pub label_bytes: u16,
    pub data_bytes: u16,
}

impl Geometry {
    /// Total sectors in the pack (= the VDA count).
    pub fn total_sectors(&self) -> usize {
        self.n_disks as usize
            * self.n_cylinders as usize
            * self.n_heads as usize
            * self.n_sectors as usize
    }

    /// Convert a Diablo real disk-address word to a linear virtual disk
    /// address (VDA), matching `BFSVirtualDA` in `BFSBase.bcpl`:
    ///
    /// ```text
    /// VDA = (((disk*nTracks + track)*nHeads + head)*nSectors + sector)
    /// ```
    ///
    /// This is the *physical placement* mapping and does NOT special-case 0
    /// (unlike chain-following, where a `next`/`previous` of 0 or `eofDA`
    /// terminates the chain — see [`bfs`]). The Diablo `DA` bit layout
    /// (`BFS.d`, MSB-first): `sector` bits 0-3, `track` bits 4-12, `head` bit
    /// 13, `disk` bit 14, `restore` bit 15.
    pub fn vda_from_da(&self, da: u16) -> usize {
        let sector = ((da >> 12) & 0xf) as usize;
        let track = ((da >> 3) & 0x1ff) as usize;
        let head = ((da >> 2) & 0x1) as usize;
        // With a single spindle the disk bit cannot denote a second drive;
        // some single-partition captures (a drive-1 image) set it anyway, so
        // ignore it rather than addressing past the end of the pack.
        let disk = if self.n_disks <= 1 {
            0
        } else {
            ((da >> 1) & 0x1) as usize
        };
        (((disk * self.n_cylinders as usize + track) * self.n_heads as usize + head)
            * self.n_sectors as usize)
            + sector
    }

    /// Inverse of [`Geometry::vda_from_da`] for Diablo: pack a virtual disk
    /// address back into a real disk-address word. Used when writing chain
    /// links into labels. (`restore` bit left 0.)
    pub fn da_from_vda(&self, vda: usize) -> u16 {
        let n_sectors = self.n_sectors as usize;
        let n_heads = self.n_heads as usize;
        let n_cylinders = self.n_cylinders as usize;
        let sector = vda % n_sectors;
        let t = vda / n_sectors;
        let head = t % n_heads;
        let t = t / n_heads;
        let track = t % n_cylinders;
        let disk = t / n_cylinders;
        ((sector as u16) << 12)
            | ((track as u16) << 3)
            | ((head as u16) << 2)
            | ((disk as u16) << 1)
    }
}

/// One logical sector: its metadata label plus its data. No hardware
/// ECC/sync/gap bytes are stored (those are a controller concern).
#[derive(Debug, Clone)]
pub struct Sector {
    pub label: Vec<u8>,
    pub data: Vec<u8>,
}

impl Sector {
    /// A zeroed sector with the given field sizes (used for free/unwritten
    /// pages whose data the CopyDisk stream omitted).
    pub fn zeroed(label_bytes: usize, data_bytes: usize) -> Self {
        Sector {
            label: vec![0u8; label_bytes],
            data: vec![0u8; data_bytes],
        }
    }
}

/// A whole disk pack held in memory, sectors indexed by virtual disk address.
#[derive(Debug, Clone)]
pub struct Disk {
    pub geometry: Geometry,
    pub sectors: Vec<Sector>,
}

impl Disk {
    /// Borrow the sector at virtual disk address `vda`, if in range.
    pub fn sector(&self, vda: usize) -> Option<&Sector> {
        self.sectors.get(vda)
    }
}
