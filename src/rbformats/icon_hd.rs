//! ICON / LEXICON hard-disk images: undo head padding and apply the bad-block table.
//!
//! An ICON hard disk starts with a header block (block 1): a geometry record at +0x10
//! (`02`, heads at +0x13, u32 total blocks at +0x14, 17 sectors per track, `03` at
//! +0x2F) and a bad-block-table record at +0x30 (spares +0x33, u16 BBT start block
//! +0x35, u16 BBT length +0x39). The BBT holds 8-byte entries `{u32 bad, u32
//! replacement}` (1-based blocks); a bad block of 0 marks an unused entry, so on a
//! healthy drive nothing is remapped.
//!
//! The MFM reader that images these drives can write 8 heads per cylinder whatever
//! the drive has; a 6-head ST-251 then carries two zero heads in every cylinder. Such
//! a file is exactly `cylinders x 8 x 17` blocks for the header's `T / (h x 17)`
//! cylinders. [`IconHd`] presents the drive's own geometry: the first `h x 17` blocks
//! of every `8 x 17`, with bad blocks read from (and written to) their replacements.

use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

const SECTOR: u64 = 512;
const SPT: u64 = 17;
const PADDED_HEADS: u64 = 8;

/// The geometry and remap table read from an ICON header.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IconLayout {
    /// Heads of the real drive; `Some` only when the file pads every cylinder to 8 heads.
    pub padded_heads: Option<u64>,
    /// Logical blocks of the drive (`T` from the header).
    pub total_blocks: u64,
    /// Bad block -> replacement block, both 0-based sectors.
    pub remap: HashMap<u64, u64>,
}

impl IconLayout {
    /// Whether presenting the drive needs anything beyond the plain file.
    pub fn needs_mapping(&self) -> bool {
        self.padded_heads.is_some() || !self.remap.is_empty()
    }
}

/// Whether a partition's first two blocks show it was never formatted: all zero apart from the
/// 16-byte stamp the ICON's `fdisk` leaves at the end of block 1 (seen on a real ST-251).
pub fn looks_unformatted(head: &[u8; 1024]) -> bool {
    head[..496].iter().all(|&b| b == 0) && head[512..].iter().all(|&b| b == 0)
}

/// Whether `sector0` is an ICON hard-disk header.
pub fn is_icon_header(sector0: &[u8]) -> bool {
    sector0.len() >= 512
        && sector0[0x10] == 0x02
        && sector0[0x2F] == 0x03
        && sector0[0x1FE..0x200] == [0x55, 0xAA]
}

/// Read the layout of an ICON hard-disk image of `file_len` bytes; `None` when it isn't one.
pub fn read_layout<R: Read + Seek>(r: &mut R, file_len: u64) -> Option<IconLayout> {
    let mut s0 = [0u8; 512];
    r.seek(SeekFrom::Start(0)).ok()?;
    r.read_exact(&mut s0).ok()?;
    if !is_icon_header(&s0) {
        return None;
    }
    let heads = u64::from(s0[0x13]);
    let total = u64::from(u32::from_le_bytes([s0[0x14], s0[0x15], s0[0x16], s0[0x17]]));
    if heads == 0 || total == 0 {
        return None;
    }
    let padded = (heads < PADDED_HEADS
        && total % (heads * SPT) == 0
        && file_len == total / (heads * SPT) * PADDED_HEADS * SPT * SECTOR)
        .then_some(heads);
    let mut layout = IconLayout {
        padded_heads: padded,
        total_blocks: total,
        remap: HashMap::new(),
    };
    // The BBT sits in the first cylinder, which head padding does not move.
    let start = u64::from(u16::from_le_bytes([s0[0x35], s0[0x36]]));
    let len = u64::from(u16::from_le_bytes([s0[0x39], s0[0x3A]]));
    if start >= 1 && len > 0 && start + len <= heads * SPT {
        let mut bbt = vec![0u8; (len * SECTOR) as usize];
        r.seek(SeekFrom::Start((start - 1) * SECTOR)).ok()?;
        r.read_exact(&mut bbt).ok()?;
        for e in bbt.chunks_exact(8) {
            let bad = u64::from(u32::from_le_bytes([e[0], e[1], e[2], e[3]]));
            let good = u64::from(u32::from_le_bytes([e[4], e[5], e[6], e[7]]));
            if bad != 0 && good != 0 && bad <= total && good <= total {
                layout.remap.insert(bad - 1, good - 1);
            }
        }
    }
    let _ = r.seek(SeekFrom::Start(0));
    Some(layout)
}

/// The layout of the ICON hard-disk image at `path`, when it needs mapping.
pub fn layout_for_path(path: &Path) -> Option<IconLayout> {
    let mut f = std::fs::File::open(path).ok()?;
    let len = f.metadata().ok()?.len();
    read_layout(&mut f, len).filter(IconLayout::needs_mapping)
}

/// An ICON hard-disk image seen with the drive's own geometry and its bad blocks remapped.
pub struct IconHd<T> {
    inner: T,
    layout: IconLayout,
    pos: u64,
}

impl<T: Seek> IconHd<T> {
    pub fn new(inner: T, layout: IconLayout) -> Self {
        Self {
            inner,
            layout,
            pos: 0,
        }
    }

    fn len(&self) -> u64 {
        self.layout.total_blocks * SECTOR
    }

    /// File offset of logical sector `s`.
    fn physical(&self, s: u64) -> u64 {
        let s = *self.layout.remap.get(&s).unwrap_or(&s);
        let sector = match self.layout.padded_heads {
            Some(h) => {
                let per = h * SPT;
                s / per * PADDED_HEADS * SPT + s % per
            }
            None => s,
        };
        sector * SECTOR
    }

    /// The contiguous file run for the logical position: (file offset, bytes until a sector end).
    fn run(&self) -> (u64, u64) {
        let s = self.pos / SECTOR;
        let within = self.pos % SECTOR;
        (self.physical(s) + within, SECTOR - within)
    }
}

impl<T: Read + Seek> Read for IconHd<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.len() || buf.is_empty() {
            return Ok(0);
        }
        let (at, room) = self.run();
        let n = (buf.len() as u64).min(room).min(self.len() - self.pos) as usize;
        self.inner.seek(SeekFrom::Start(at))?;
        let got = self.inner.read(&mut buf[..n])?;
        self.pos += got as u64;
        Ok(got)
    }
}

impl<T: Write + Seek> Write for IconHd<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.pos >= self.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "write past the end of the ICON drive",
            ));
        }
        let (at, room) = self.run();
        let n = (buf.len() as u64).min(room).min(self.len() - self.pos) as usize;
        self.inner.seek(SeekFrom::Start(at))?;
        let put = self.inner.write(&buf[..n])?;
        self.pos += put as u64;
        Ok(put)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<T: Seek> Seek for IconHd<T> {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        let next = match to {
            SeekFrom::Start(n) => n as i128,
            SeekFrom::End(d) => self.len() as i128 + d as i128,
            SeekFrom::Current(d) => self.pos as i128 + d as i128,
        };
        if next < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before start",
            ));
        }
        self.pos = next as u64;
        Ok(self.pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// A 6-head, 2-cylinder drive stored with 8 heads per cylinder, each sector filled with its LBA.
    fn padded(remap: &[(u32, u32)]) -> (Vec<u8>, u64) {
        let total = 2 * 6 * SPT;
        let mut img = vec![0u8; (2 * 8 * SPT * SECTOR) as usize];
        for s in 0..total {
            let phys = s / (6 * SPT) * 8 * SPT + s % (6 * SPT);
            let at = (phys * SECTOR) as usize;
            img[at..at + SECTOR as usize].fill(s as u8);
        }
        let h = &mut img[..512];
        h[0x10] = 0x02;
        h[0x13] = 6;
        h[0x14..0x18].copy_from_slice(&(total as u32).to_le_bytes());
        h[0x2F] = 0x03;
        h[0x35] = 2;
        h[0x39] = 1;
        h[0x1FE] = 0x55;
        h[0x1FF] = 0xAA;
        for (k, &(bad, good)) in remap.iter().enumerate() {
            let at = 512 + k * 8;
            img[at..at + 4].copy_from_slice(&bad.to_le_bytes());
            img[at + 4..at + 8].copy_from_slice(&good.to_le_bytes());
        }
        (img, total)
    }

    #[test]
    fn padding_heads_are_skipped() {
        let (img, total) = padded(&[]);
        let layout = read_layout(&mut Cursor::new(&img), img.len() as u64).unwrap();
        assert_eq!(layout.padded_heads, Some(6));
        let mut hd = IconHd::new(Cursor::new(img), layout);
        let mut all = Vec::new();
        hd.read_to_end(&mut all).unwrap();
        assert_eq!(all.len() as u64, total * SECTOR);
        // Logical sector 102 (cylinder 1, head 0) comes from file sector 136, past the padding.
        assert_eq!(all[(102 * SECTOR + 7) as usize], 102);
        assert_eq!(all[(203 * SECTOR) as usize], 203);
    }

    #[test]
    fn bad_blocks_read_and_write_their_replacements() {
        // Block 50 (sector 49) is bad, replaced by block 200 (sector 199); an unused entry has bad 0.
        let (img, _) = padded(&[(0, 21), (50, 200)]);
        let layout = read_layout(&mut Cursor::new(&img), img.len() as u64).unwrap();
        assert_eq!(layout.remap.len(), 1);
        let mut hd = IconHd::new(Cursor::new(img), layout);
        let mut b = [0u8; 4];
        hd.seek(SeekFrom::Start(49 * SECTOR)).unwrap();
        hd.read_exact(&mut b).unwrap();
        assert_eq!(b, [199; 4]);
        hd.seek(SeekFrom::Start(49 * SECTOR)).unwrap();
        hd.write_all(b"BAD!").unwrap();
        let img = hd.inner.into_inner();
        let phys = (199 / (6 * SPT) * 8 * SPT + 199 % (6 * SPT)) * SECTOR;
        assert_eq!(&img[phys as usize..phys as usize + 4], b"BAD!");
    }

    #[test]
    fn an_unpadded_healthy_disk_needs_no_mapping() {
        let mut img = vec![0u8; 102 * 512];
        img[0x10] = 0x02;
        img[0x13] = 6;
        img[0x14..0x18].copy_from_slice(&102u32.to_le_bytes());
        img[0x2F] = 0x03;
        img[0x1FE] = 0x55;
        img[0x1FF] = 0xAA;
        let layout = read_layout(&mut Cursor::new(&img), img.len() as u64).unwrap();
        assert!(!layout.needs_mapping());
    }
}
