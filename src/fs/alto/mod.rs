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
pub mod pilot;
pub mod salto;
pub mod trident;
pub mod write;
pub mod zdisk;

use super::filesystem::FilesystemError;

/// Open a disk pack from raw bytes, auto-detecting the container: a PDI image
/// (magic `PARCDISK`), a Dwarf Draco 6085 `.zdisk` (a zlib stream inflating to
/// the `0xDAAD` signature; a Pilot pack), a Salto emulator cooked `.dsk` image
/// (recognized by its exact Diablo-31 size), or a period CopyDisk stream
/// (`.bfs` / `.copydisk` / `.altodisk`).
pub fn open_pack(bytes: &[u8]) -> Result<Disk, FilesystemError> {
    if bytes.len() >= pdi::MAGIC.len() && &bytes[..pdi::MAGIC.len()] == pdi::MAGIC {
        pdi::read(bytes)
    } else if zdisk::is_zdisk(bytes) {
        zdisk::read(bytes)
    } else if salto::is_salto_image(bytes) {
        // A Salto `.dsk` and a ContrAlto2 Diablo `.dsk` are the same size; try
        // Salto first (its leading word is the page number), then fall back to
        // the ContrAlto Diablo framing (leading dummy + disk-address header).
        salto::read(bytes).or_else(|_| trident::read_diablo(bytes))
    } else if trident::is_trident_image(bytes) {
        trident::read(bytes)
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

    /// Trident 2-word disk header (`DH`) from a VDA: word 0 = cylinder, word 1 =
    /// `(head << 8) | sector` (`Tfs.d` `DA` / `TfsA.asm:TFSRealDA`). Single file
    /// system (`firstVTrack = 0`); the T-300 multi-FS split is not modeled here.
    pub fn trident_dh_from_vda(&self, vda: usize) -> [u16; 2] {
        let n_sectors = self.n_sectors as usize;
        let n_heads = self.n_heads as usize;
        let sector = vda % n_sectors;
        let t = vda / n_sectors;
        let head = t % n_heads;
        let cyl = t / n_heads;
        [
            cyl as u16,
            (((head as u16) & 0xff) << 8) | ((sector as u16) & 0xff),
        ]
    }

    /// Inverse of [`Geometry::trident_dh_from_vda`].
    pub fn trident_vda_from_dh(&self, w0: u16, w1: u16) -> usize {
        let cyl = w0 as usize;
        let head = (w1 >> 8) as usize & 0xff;
        let sector = (w1 & 0xff) as usize;
        (cyl * self.n_heads as usize + head) * self.n_sectors as usize + sector
    }
}

/// `eofDA`: a chain link of this value (or 0) terminates a page chain.
pub(crate) const EOF_DA: u16 = 0xffff;

/// Encodes/decodes the per-sector **label** for the Alto file system, which is
/// the *same logical filesystem* on Diablo (BFS) and Trident (TFS) but with a
/// different on-disk label shape. Both carry the same fields — chain links
/// (`next`/`prev`), `numChars`, `pageNumber`, and the file id (`version` +
/// 2-word serial) — so the directory, leader pages, file pointers, and
/// free-page bitmap above this layer are identical; only the label byte layout
/// and the disk-address width differ.
///
/// | | Diablo (8 words) | Trident (10 words) |
/// |---|---|---|
/// | next | word 0 (1-word DA) | words 8-9 (2-word DH) |
/// | prev | word 1 (1-word DA) | words 6-7 (2-word DH) |
/// | numChars | word 3 | word 4 |
/// | pageNumber | word 4 | word 5 |
/// | fileId (version, sn1, sn2) | words 5,6,7 | words 0,1,2 |
/// | packID | — | word 3 |
///
/// Sources: `BFS.d` (Diablo `DL`), `Tfs.d` (Trident `DL` + `DA`/`DH`),
/// `BFSBase.bcpl` / `TfsA.asm` (the DA↔VDA mappings).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelCodec {
    Diablo,
    Trident,
}

/// A decoded chain link: a VDA, or `None` at end-of-file.
pub(crate) type Link = Option<usize>;

impl LabelCodec {
    pub fn for_family(family: FsFamily) -> Self {
        match family {
            FsFamily::Trident => LabelCodec::Trident,
            _ => LabelCodec::Diablo,
        }
    }

    /// Label size in bytes.
    pub fn label_bytes(&self) -> usize {
        match self {
            LabelCodec::Diablo => 16,  // 8 words
            LabelCodec::Trident => 20, // 10 words
        }
    }

    fn decode_link(&self, g: &Geometry, label: &[u8], word: usize) -> Link {
        match self {
            LabelCodec::Diablo => {
                let da = be16(label, word * 2);
                if da == EOF_DA || da == 0 {
                    None
                } else {
                    Some(g.vda_from_da(da))
                }
            }
            LabelCodec::Trident => {
                let w0 = be16(label, word * 2);
                let w1 = be16(label, word * 2 + 2);
                if (w0 == EOF_DA && w1 == EOF_DA) || (w0 == 0 && w1 == 0) {
                    None
                } else {
                    Some(g.trident_vda_from_dh(w0, w1))
                }
            }
        }
    }

    /// The `next` chain link (to the following page), or `None` at EOF.
    pub fn next(&self, g: &Geometry, label: &[u8]) -> Link {
        match self {
            LabelCodec::Diablo => self.decode_link(g, label, 0),
            LabelCodec::Trident => self.decode_link(g, label, 8),
        }
    }

    /// The `prev` chain link (to the preceding page), or `None` at the start.
    pub fn prev(&self, g: &Geometry, label: &[u8]) -> Link {
        match self {
            LabelCodec::Diablo => self.decode_link(g, label, 1),
            LabelCodec::Trident => self.decode_link(g, label, 6),
        }
    }

    pub fn num_chars(&self, label: &[u8]) -> u16 {
        match self {
            LabelCodec::Diablo => be16(label, 3 * 2),
            LabelCodec::Trident => be16(label, 4 * 2),
        }
    }

    pub fn page_number(&self, label: &[u8]) -> u16 {
        match self {
            LabelCodec::Diablo => be16(label, 4 * 2),
            LabelCodec::Trident => be16(label, 5 * 2),
        }
    }

    /// `(version, serial_word1, serial_word2)`.
    pub fn file_id(&self, label: &[u8]) -> (u16, u16, u16) {
        match self {
            LabelCodec::Diablo => (be16(label, 5 * 2), be16(label, 6 * 2), be16(label, 7 * 2)),
            // Trident: serialNumber (words 0,1) then version (word 2).
            LabelCodec::Trident => (be16(label, 4), be16(label, 0), be16(label, 2)),
        }
    }

    fn encode_link(&self, g: &Geometry, label: &mut [u8], word: usize, link: Link) {
        match self {
            LabelCodec::Diablo => {
                let da = link.map(|v| g.da_from_vda(v)).unwrap_or(EOF_DA);
                put_be16(label, word * 2, da);
            }
            LabelCodec::Trident => {
                let [w0, w1] = link
                    .map(|v| g.trident_dh_from_vda(v))
                    .unwrap_or([EOF_DA, EOF_DA]);
                put_be16(label, word * 2, w0);
                put_be16(label, word * 2 + 2, w1);
            }
        }
    }

    /// Build a label from decoded fields. `next`/`prev` are VDAs (`None` = EOF).
    #[allow(clippy::too_many_arguments)]
    pub fn make_label(
        &self,
        g: &Geometry,
        next: Link,
        prev: Link,
        num_chars: u16,
        page: u16,
        version: u16,
        sn_w1: u16,
        sn_w2: u16,
    ) -> Vec<u8> {
        let mut l = vec![0u8; self.label_bytes()];
        match self {
            LabelCodec::Diablo => {
                self.encode_link(g, &mut l, 0, next);
                self.encode_link(g, &mut l, 1, prev);
                put_be16(&mut l, 3 * 2, num_chars);
                put_be16(&mut l, 4 * 2, page);
                put_be16(&mut l, 5 * 2, version);
                put_be16(&mut l, 6 * 2, sn_w1);
                put_be16(&mut l, 7 * 2, sn_w2);
            }
            LabelCodec::Trident => {
                // fileId is serialNumber (2 words) then version — confirmed
                // against the real Spruce T-300 pack (SysDir = 8000 0064 0001 =
                // dir-bit/100/v1), the opposite order from Diablo.
                put_be16(&mut l, 0, sn_w1);
                put_be16(&mut l, 2, sn_w2);
                put_be16(&mut l, 4, version);
                // word 3 = packID (left 0)
                put_be16(&mut l, 4 * 2, num_chars);
                put_be16(&mut l, 5 * 2, page);
                self.encode_link(g, &mut l, 6, prev);
                self.encode_link(g, &mut l, 8, next);
            }
        }
        l
    }

    /// A free page's label: chain links zeroed (the writer's free-page marker —
    /// distinct from the `EOF_DA` used at file-chain ends) and the file id set to
    /// the all-ones free marker. Matches the original BFS writer byte-for-byte.
    pub fn free_label(&self, _g: &Geometry) -> Vec<u8> {
        let mut l = vec![0u8; self.label_bytes()];
        // version + serial = 0xFFFF,0xFFFF,0xFFFF; links left zero.
        match self {
            LabelCodec::Diablo => {
                put_be16(&mut l, 5 * 2, 0xffff);
                put_be16(&mut l, 6 * 2, 0xffff);
                put_be16(&mut l, 7 * 2, 0xffff);
            }
            LabelCodec::Trident => {
                put_be16(&mut l, 0, 0xffff);
                put_be16(&mut l, 2, 0xffff);
                put_be16(&mut l, 4, 0xffff);
            }
        }
        l
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
