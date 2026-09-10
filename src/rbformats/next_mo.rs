//! NeXT magneto-optical disk image (`.od`) — the 256 MB Canon MO cartridge the
//! NeXT Computer shipped with, as Previous and other emulators store it.
//!
//! Unlike every other image this crate reads, an `.od` is the **raw media**,
//! not the drive's user data: each physical sector is 1296 bytes holding 1024
//! bytes of data plus a cross-interleaved Reed-Solomon(36,32) code. The sector
//! is a 36 x 36 byte array — rows 0..31 carry 32 data bytes each in columns
//! 0..31, columns 32..35 hold each row's ECC, and rows 32..35 hold each
//! column's. So the user data is `raw[i*36 .. i*36+32]` for `i` in 0..32, and
//! writing one back means recomputing both ECC passes.
//!
//! Two more things stand between the file and a NeXT disk label:
//!
//! - **Origin.** The image starts at the drive's track 4096, but the kernel's
//!   logical block 0 is at track 4149 — 848 sectors in. Everything before that
//!   is erased media (`0xFF`).
//! - **Alternate groups.** The `od` driver, not the drive, does the bad-block
//!   sparing, using the label's own `d_ag_size` / `d_ag_alts` / `d_ag_off`:
//!   the data area (past the front porch) is cut into `d_ag_size`-sector
//!   groups, and the `d_ag_alts` sectors at `d_ag_off` into each group are
//!   spares that logical addressing skips. Read the image linearly and every
//!   cylinder group past the first drifts 8 sectors — the directory tree still
//!   lists, which is what makes this worth spelling out.
//!
//! [`NextMoReader`] presents the decoded, de-spared logical disk, so the NeXT
//! label parser and the big-endian UFS reader see an ordinary image.

use std::io::{self, Read, Seek, SeekFrom, Write};

use crate::partition::next;

/// Bytes one sector occupies on the media: 1024 data + 272 ECC.
pub const RAW_SECTOR: usize = 1296;
/// Bytes of user data one sector carries.
pub const DATA_SECTOR: usize = 1024;
/// Side of the square the ECC is computed over.
const ROW: usize = 36;
/// Rows that carry data; the last four are the column ECC.
const DATA_ROWS: usize = 32;

/// Sectors between the start of the image and the kernel's logical block 0.
/// Previous stores the media from track 4096; the driver's first sector is at
/// track 4149, 16 sectors per track.
pub const DEFAULT_ORIGIN_SECTOR: u64 = (4149 - 4096) * 16;

/// How far into the image [`detect`] looks for the block-0 label copy.
const ORIGIN_SCAN_SECTORS: u64 = 4096;

/// Physical sectors read in one go when serving a logical run.
const READ_RUN_SECTORS: usize = 256;

// --- Reed-Solomon(36,32) over GF(2**8), generator (x-1)(x-2)(x-4)(x-8) ------

/// GF(2**8) modulus NeXT's code uses.
const GF_POLY: u16 = 0x11d;

/// Coefficients of x^3..x^0 in the generator polynomial, so `x^4 == GEN(x)`.
const GEN: [u8; 4] = [0x0f, 0x36, 0x78, 0x40];

const fn gf_mul(a: u8, b: u8) -> u8 {
    let mut acc: u16 = 0;
    let mut x = a as u16;
    let mut y = b as u16;
    while y != 0 {
        if y & 1 != 0 {
            acc ^= x;
        }
        y >>= 1;
        x <<= 1;
        if x & 0x100 != 0 {
            x ^= GF_POLY;
        }
    }
    acc as u8
}

/// `T_REM[b]` is `b * x^4 mod g(x)`, its four GF coefficients packed big-endian.
const fn build_t_rem() -> [u32; 256] {
    let mut t = [0u32; 256];
    let mut b = 0usize;
    while b < 256 {
        t[b] = ((gf_mul(b as u8, GEN[0]) as u32) << 24)
            | ((gf_mul(b as u8, GEN[1]) as u32) << 16)
            | ((gf_mul(b as u8, GEN[2]) as u32) << 8)
            | (gf_mul(b as u8, GEN[3]) as u32);
        b += 1;
    }
    t
}

const T_REM: [u32; 256] = build_t_rem();

/// Remainder of the 32 code bytes at `off`, `step` apart — the four ECC bytes.
fn ecc_block(sector: &[u8], off: usize, step: usize) -> u32 {
    let mut r = ((sector[off] as u32) << 24)
        | ((sector[off + step] as u32) << 16)
        | ((sector[off + 2 * step] as u32) << 8)
        | (sector[off + 3 * step] as u32);
    let mut p = off + 4 * step;
    for i in 4..ROW {
        r = T_REM[(r >> 24) as usize] ^ (r << 8);
        if i < DATA_ROWS {
            r ^= sector[p] as u32;
            p += step;
        }
    }
    r
}

fn encode_string(sector: &mut [u8], off: usize, step: usize) {
    let ecc = ecc_block(sector, off, step);
    sector[off + 32 * step] = (ecc >> 24) as u8;
    sector[off + 33 * step] = (ecc >> 16) as u8;
    sector[off + 34 * step] = (ecc >> 8) as u8;
    sector[off + 35 * step] = ecc as u8;
}

/// Lift the 1024 user bytes out of one raw sector.
pub fn decode_sector(raw: &[u8], out: &mut [u8]) {
    for i in 0..DATA_ROWS {
        out[i * DATA_SECTOR / DATA_ROWS..(i + 1) * DATA_SECTOR / DATA_ROWS]
            .copy_from_slice(&raw[i * ROW..i * ROW + DATA_ROWS]);
    }
}

/// Lay 1024 user bytes into a raw sector and stamp both ECC passes.
pub fn encode_sector(data: &[u8], raw: &mut [u8]) {
    for b in raw.iter_mut() {
        *b = 0;
    }
    for i in 0..DATA_ROWS {
        raw[i * ROW..i * ROW + DATA_ROWS]
            .copy_from_slice(&data[i * DATA_ROWS..(i + 1) * DATA_ROWS]);
    }
    // Columns first: the row pass then covers the column ECC rows too.
    for i in 0..DATA_ROWS {
        encode_string(raw, i, ROW);
    }
    for i in 0..ROW {
        encode_string(raw, i * ROW, 1);
    }
}

// --- Logical geometry -------------------------------------------------------

/// Where the logical disk lives inside the physical media.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MoGeometry {
    /// Physical sector holding logical block 0.
    pub origin: u64,
    /// `d_front` — logical sectors before the data area, mapped one to one.
    pub front: u64,
    /// `d_ag_size`; zero disables sparing entirely.
    pub group_size: u64,
    /// `d_ag_alts` — spare sectors per group.
    pub group_alts: u64,
    /// `d_ag_off` — where in a group the spares sit.
    pub group_off: u64,
    /// Logical sectors the disk presents.
    pub logical_sectors: u64,
}

impl MoGeometry {
    /// Physical sector backing a logical one, skipping every spare.
    pub fn physical_sector(&self, logical: u64) -> u64 {
        let usable = self.group_size.saturating_sub(self.group_alts);
        if self.group_alts == 0 || usable == 0 || logical < self.front {
            return self.origin + logical;
        }
        let d = logical - self.front;
        let group = d / usable;
        let within = d % usable;
        let placed = if within < self.group_off {
            within
        } else {
            within + self.group_alts
        };
        self.origin + self.front + group * self.group_size + placed
    }

    /// Logical sectors from `logical` that map to consecutive physical ones.
    fn contiguous_run(&self, logical: u64) -> u64 {
        let usable = self.group_size.saturating_sub(self.group_alts);
        if self.group_alts == 0 || usable == 0 {
            return self.logical_sectors.saturating_sub(logical);
        }
        if logical < self.front {
            return self.front - logical;
        }
        let within = (logical - self.front) % usable;
        let next_break = if within < self.group_off {
            self.group_off - within
        } else {
            usable - within
        };
        next_break.max(1)
    }

    fn from_label(label: &next::NextDiskLabel, origin: u64, phys_sectors: u64) -> Self {
        let front = label.front_porch as u64;
        let group_size = label.group_size as u64;
        let group_alts = label.group_alts as u64;
        let usable = group_size.saturating_sub(group_alts);
        let by_label = if label.group_count == 0 || usable == 0 {
            u64::MAX
        } else {
            front + label.group_count as u64 * usable
        };
        let mut geo = MoGeometry {
            origin,
            front,
            group_size,
            group_alts,
            group_off: label.group_off as u64,
            logical_sectors: 0,
        };
        // The media is the other bound: a label may describe more groups than
        // the file holds, and the last physical sector must stay addressable.
        let mut by_media = phys_sectors.saturating_sub(origin);
        while by_media > 0 && geo.physical_sector(by_media - 1) >= phys_sectors {
            by_media -= 1;
        }
        geo.logical_sectors = by_label.min(by_media);
        geo
    }
}

// --- Reader -----------------------------------------------------------------

/// Read/write view of the decoded logical disk inside a raw `.od` image.
pub struct NextMoReader<R> {
    inner: R,
    geo: MoGeometry,
    pos: u64,
    raw: Vec<u8>,
    data: Vec<u8>,
}

impl<R: Read + Seek> NextMoReader<R> {
    /// Wrap `inner` with an already-resolved geometry.
    pub fn new(inner: R, geo: MoGeometry) -> Self {
        Self {
            inner,
            geo,
            pos: 0,
            raw: vec![0u8; READ_RUN_SECTORS * RAW_SECTOR],
            data: vec![0u8; READ_RUN_SECTORS * DATA_SECTOR],
        }
    }

    /// Logical bytes the decoded disk presents.
    pub fn len(&self) -> u64 {
        self.geo.logical_sectors * DATA_SECTOR as u64
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn geometry(&self) -> MoGeometry {
        self.geo
    }

    /// Decode `count` logical sectors from `logical` into `self.data`.
    fn fill(&mut self, logical: u64, count: usize) -> io::Result<()> {
        let first = self.geo.physical_sector(logical);
        self.inner
            .seek(SeekFrom::Start(first * RAW_SECTOR as u64))?;
        let want = count * RAW_SECTOR;
        self.inner.read_exact(&mut self.raw[..want])?;
        for i in 0..count {
            let (r, d) = (i * RAW_SECTOR, i * DATA_SECTOR);
            decode_sector(
                &self.raw[r..r + RAW_SECTOR],
                &mut self.data[d..d + DATA_SECTOR],
            );
        }
        Ok(())
    }

    /// How many sectors from `logical` can be served in one physical read.
    fn run_len(&self, logical: u64, wanted_bytes: usize) -> usize {
        let by_map = self.geo.contiguous_run(logical);
        let by_end = self.geo.logical_sectors.saturating_sub(logical);
        let by_buf = READ_RUN_SECTORS as u64;
        let by_req = (wanted_bytes / DATA_SECTOR + 1) as u64;
        by_map.min(by_end).min(by_buf).min(by_req).max(1) as usize
    }
}

impl<R: Read + Seek> Read for NextMoReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let end = self.len();
        if self.pos >= end || buf.is_empty() {
            return Ok(0);
        }
        let logical = self.pos / DATA_SECTOR as u64;
        let intra = (self.pos % DATA_SECTOR as u64) as usize;
        let count = self.run_len(logical, intra + buf.len());
        self.fill(logical, count)?;
        let avail = (count * DATA_SECTOR - intra).min((end - self.pos) as usize);
        let n = avail.min(buf.len());
        buf[..n].copy_from_slice(&self.data[intra..intra + n]);
        self.pos += n as u64;
        Ok(n)
    }
}

impl<R: Read + Seek> Seek for NextMoReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let end = self.len() as i64;
        let target = match pos {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::End(n) => end + n,
            SeekFrom::Current(n) => self.pos as i64 + n,
        };
        if target < 0 {
            return Err(crate::compat::io_other("seek before start of MO image"));
        }
        self.pos = target as u64;
        Ok(self.pos)
    }
}

impl<R: Read + Write + Seek> Write for NextMoReader<R> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let end = self.len();
        if self.pos >= end || buf.is_empty() {
            return Ok(0);
        }
        let logical = self.pos / DATA_SECTOR as u64;
        let intra = (self.pos % DATA_SECTOR as u64) as usize;
        let n = buf.len().min(DATA_SECTOR - intra);
        // Read-modify-write: the ECC covers the whole sector, so a partial
        // write still has to re-encode all 1024 bytes.
        self.fill(logical, 1)?;
        self.data[intra..intra + n].copy_from_slice(&buf[..n]);
        let mut raw = vec![0u8; RAW_SECTOR];
        encode_sector(&self.data[..DATA_SECTOR], &mut raw);
        let phys = self.geo.physical_sector(logical);
        self.inner.seek(SeekFrom::Start(phys * RAW_SECTOR as u64))?;
        self.inner.write_all(&raw)?;
        self.pos += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

// --- Detection --------------------------------------------------------------

/// Resolve the geometry of a raw NeXT MO image, or `None` when `inner` is not
/// one. A validating NeXT label in the decoded stream is the only signature
/// this format has, so detection insists on finding one.
pub fn detect<R: Read + Seek>(inner: &mut R) -> Option<MoGeometry> {
    let size = inner.seek(SeekFrom::End(0)).ok()?;
    if size == 0 || size % RAW_SECTOR as u64 != 0 {
        return None;
    }
    let phys_sectors = size / RAW_SECTOR as u64;
    let span = next::LABEL_SPAN.div_ceil(DATA_SECTOR) as u64;
    if phys_sectors < span {
        return None;
    }
    let window = ORIGIN_SCAN_SECTORS.min(phys_sectors - span) + span;
    inner.seek(SeekFrom::Start(0)).ok()?;
    let mut raw = vec![0u8; window as usize * RAW_SECTOR];
    inner.read_exact(&mut raw).ok()?;
    let mut decoded = vec![0u8; window as usize * DATA_SECTOR];
    for i in 0..window as usize {
        let (r, d) = (i * RAW_SECTOR, i * DATA_SECTOR);
        decode_sector(&raw[r..r + RAW_SECTOR], &mut decoded[d..d + DATA_SECTOR]);
    }
    // The block-0 copy starts a sector, so scanning sector starts finds it.
    // Only the leading erased region stands before it (848 sectors in Previous).
    for sector in 0..(window - span + 1) {
        let at = sector as usize * DATA_SECTOR;
        let copy = &decoded[at..at + next::LABEL_SPAN];
        if !next::validates(copy) {
            continue;
        }
        let label = next::NextDiskLabel::parse(copy, 0).ok()?;
        return Some(MoGeometry::from_label(&label, sector, phys_sectors));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::{BigEndian, ByteOrder};

    /// The four generator coefficients are what Previous's `t_rem` table holds;
    /// three entries pin the whole table without transcribing 256 constants.
    #[test]
    fn t_rem_matches_the_reference_table() {
        assert_eq!(T_REM[1], 0x0f36_7840);
        assert_eq!(T_REM[2], 0x1e6c_f080);
        assert_eq!(T_REM[4], 0x3cd8_fd1d);
        assert_eq!(T_REM[0], 0);
    }

    #[test]
    fn encode_then_decode_round_trips() {
        let data: Vec<u8> = (0..DATA_SECTOR).map(|i| (i * 7 + 3) as u8).collect();
        let mut raw = vec![0u8; RAW_SECTOR];
        encode_sector(&data, &mut raw);
        let mut back = vec![0u8; DATA_SECTOR];
        decode_sector(&raw, &mut back);
        assert_eq!(back, data);
    }

    /// Re-encoding an unchanged sector must reproduce the ECC bytes byte for
    /// byte, or a written-back sector would look corrupt to the drive.
    #[test]
    fn ecc_bytes_are_reproducible() {
        let data: Vec<u8> = (0..DATA_SECTOR).map(|i| (i % 251) as u8).collect();
        let mut a = vec![0u8; RAW_SECTOR];
        let mut b = vec![0u8; RAW_SECTOR];
        encode_sector(&data, &mut a);
        let mut mid = vec![0u8; DATA_SECTOR];
        decode_sector(&a, &mut mid);
        encode_sector(&mid, &mut b);
        assert_eq!(a, b);
    }

    fn geo(front: u64, size: u64, alts: u64, off: u64, sectors: u64) -> MoGeometry {
        MoGeometry {
            origin: 848,
            front,
            group_size: size,
            group_alts: alts,
            group_off: off,
            logical_sectors: sectors,
        }
    }

    /// The numbers are the `ns0.8.od` fixture's: spares land at image sectors
    /// 1888, 3488, 5088 — 1600 apart, 784 into each group.
    #[test]
    fn sparing_skips_the_alternate_sectors() {
        let g = geo(256, 1600, 8, 784, 100_000);
        assert_eq!(g.physical_sector(0), 848, "label at the origin");
        assert_eq!(g.physical_sector(256), 1104, "data area follows the porch");
        assert_eq!(
            g.physical_sector(1039),
            1887,
            "last sector before the spares"
        );
        assert_eq!(g.physical_sector(1040), 1896, "eight spares skipped");
        assert_eq!(g.physical_sector(1848), 2704, "second group starts");
        assert_eq!(g.physical_sector(2632), 3496, "second spare run skipped");
    }

    #[test]
    fn a_label_with_no_groups_maps_straight_through() {
        let g = geo(160, 0, 0, 0, 1000);
        assert_eq!(g.physical_sector(0), 848);
        assert_eq!(g.physical_sector(999), 1847);
    }

    #[test]
    fn contiguous_runs_stop_at_the_spares() {
        let g = geo(256, 1600, 8, 784, 100_000);
        assert_eq!(g.contiguous_run(0), 256);
        assert_eq!(g.contiguous_run(256), 784);
        assert_eq!(g.contiguous_run(1039), 1);
        assert_eq!(g.contiguous_run(1040), 808);
    }

    /// A round trip through the reader proves decode, sparing and the run
    /// batching agree with the writer.
    #[test]
    fn reader_round_trips_through_the_sparing_map() {
        let g = geo(4, 16, 2, 8, 40);
        let phys = 848 + 64;
        let mut media = vec![0u8; phys as usize * RAW_SECTOR];
        for logical in 0..g.logical_sectors {
            let data: Vec<u8> = (0..DATA_SECTOR)
                .map(|i| (logical as u8) ^ (i as u8))
                .collect();
            let p = g.physical_sector(logical) as usize;
            encode_sector(&data, &mut media[p * RAW_SECTOR..(p + 1) * RAW_SECTOR]);
        }
        let mut r = NextMoReader::new(std::io::Cursor::new(media), g);
        let mut got = Vec::new();
        r.read_to_end(&mut got).unwrap();
        assert_eq!(got.len(), g.logical_sectors as usize * DATA_SECTOR);
        for logical in 0..g.logical_sectors as usize {
            let want: Vec<u8> = (0..DATA_SECTOR)
                .map(|i| (logical as u8) ^ (i as u8))
                .collect();
            assert_eq!(
                &got[logical * DATA_SECTOR..(logical + 1) * DATA_SECTOR],
                &want[..]
            );
        }
    }

    /// Build a synthetic MO image — erased lead-in, then an ECC-coded NeXT
    /// label at the origin — and check `detect` reads the geometry back off it.
    fn synthetic_mo(origin: u64, front: u16, groups: (u16, u16, u16)) -> Vec<u8> {
        use crate::partition::next::{build_label, NextLabelSpec, NextPartitionSpec};
        let (size, alts, off) = groups;
        let mut spec = NextLabelSpec {
            front_porch: front,
            ..Default::default()
        };
        spec.partitions = vec![
            Some(NextPartitionSpec {
                base: 0,
                size: 512,
                ..Default::default()
            }),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ];
        let mut label = build_label(&spec);
        BigEndian::write_u16(&mut label[0x74..0x76], 1);
        BigEndian::write_u16(&mut label[0x76..0x78], size);
        BigEndian::write_u16(&mut label[0x78..0x7A], alts);
        BigEndian::write_u16(&mut label[0x7A..0x7C], off);
        crate::partition::next::NextDiskLabel::stamp_checksum(
            &mut label,
            crate::partition::next::NEXT_LABEL_V3,
        );

        // One copy at block 0, which is the one a sector-aligned scan sees.
        let span = next::LABEL_SPAN.div_ceil(DATA_SECTOR);
        let mut logical = vec![0u8; span * DATA_SECTOR];
        logical[..label.len()].copy_from_slice(&label);

        let phys = origin + 2048;
        let mut media = vec![0xFFu8; phys as usize * RAW_SECTOR];
        for i in 0..span {
            let p = (origin as usize + i) * RAW_SECTOR;
            encode_sector(
                &logical[i * DATA_SECTOR..(i + 1) * DATA_SECTOR],
                &mut media[p..p + RAW_SECTOR],
            );
        }
        media
    }

    #[test]
    fn detect_reads_the_geometry_off_the_label() {
        let media = synthetic_mo(848, 256, (1600, 8, 784));
        let geo = detect(&mut std::io::Cursor::new(media)).expect("a NeXT MO image");
        assert_eq!(geo.origin, DEFAULT_ORIGIN_SECTOR);
        assert_eq!(geo.front, 256);
        assert_eq!(geo.group_size, 1600);
        assert_eq!(geo.group_alts, 8);
        assert_eq!(geo.group_off, 784);
        // One group of 1600 with 8 spares, plus the porch, bounded by the media.
        assert_eq!(geo.logical_sectors, 256 + 1592);
    }

    /// The size test is the cheap gate; 1296 = 16 * 81, so an ordinary image
    /// never reaches the label scan at all.
    #[test]
    fn detect_declines_anything_without_a_label() {
        let mut plain = std::io::Cursor::new(vec![0u8; 4096 * RAW_SECTOR]);
        assert!(detect(&mut plain).is_none());
        let mut odd = std::io::Cursor::new(vec![0u8; 1024 * 512]);
        assert!(detect(&mut odd).is_none());
    }

    #[test]
    fn writing_a_sector_survives_a_reread() {
        let g = geo(4, 16, 2, 8, 40);
        let phys = 848 + 64;
        let media = vec![0u8; phys as usize * RAW_SECTOR];
        let mut rw = NextMoReader::new(std::io::Cursor::new(media), g);
        rw.seek(SeekFrom::Start(20 * DATA_SECTOR as u64 + 5))
            .unwrap();
        rw.write_all(b"NeXTSTEP").unwrap();
        rw.seek(SeekFrom::Start(20 * DATA_SECTOR as u64 + 5))
            .unwrap();
        let mut back = [0u8; 8];
        rw.read_exact(&mut back).unwrap();
        assert_eq!(&back, b"NeXTSTEP");
    }
}
