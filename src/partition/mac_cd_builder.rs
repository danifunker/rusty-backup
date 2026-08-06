//! Synthesize a classic-Mac CD-ROM image from scratch: an Apple Partition Map
//! at block 0 wrapping one `Apple_HFS` partition that holds a freshly formatted
//! HFS or HFS+ volume. `rb-cli optical new mac-hfs` / `mac-hfsplus`.
//!
//! This is the Mac counterpart of [`crate::partition::sgi_hdd_builder`]'s
//! CD-ROM mode: same shape, different platform conventions. A pure-HFS disc is
//! what a classic Mac CD-ROM actually is — the Apple CD-ROM driver hands the
//! ROM 512-byte logical blocks, so the APM is written at 512-byte blocks even
//! though the medium's sectors are 2048 bytes. No ISO 9660 volume descriptor is
//! written; a hybrid HFS/ISO 9660 disc needs an ISO 9660 mastering engine,
//! which this crate doesn't have.
//!
//! Layout (matches the disc images Apple's own tools produce):
//! - block 0: Driver Descriptor Record, 512-byte blocks, no SCSI driver.
//! - blocks 1..3: partition map — the self-referencing `Apple_partition_map`
//!   entry, then the `Apple_HFS` entry.
//! - block 64 onward: the HFS / HFS+ volume, covering the rest of the disc.
//!
//! HFS+ discs carry a bare HFS+ volume with no HFS wrapper — Mac OS 8.1 and
//! later mount that directly; only *booting* an older ROM needs the wrapper.

use anyhow::{bail, ensure, Result};
use std::io::{Cursor, Seek, SeekFrom, Write};

use crate::partition::apm::build_minimal_apm;

const SECTOR: u64 = 512;
/// CD-ROM logical sector. Disc images are sized in whole 2048-byte sectors so
/// they can be burned or mounted without a short final sector.
const CD_SECTOR: u64 = 2048;
/// First block of the `Apple_HFS` partition. Block 64 is where Apple's
/// formatters put it, leaving room for the driver partitions a bootable
/// hard disk would carry.
const HFS_START_BLOCK: u64 = 64;
/// Smallest volume worth building. Well under any real disc, but it keeps the
/// filesystem formatters clear of their own minimums.
const MIN_VOLUME_BYTES: u64 = 1024 * 1024;

/// Which Mac filesystem the disc carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MacCdFs {
    /// Classic HFS (Mac OS Standard) — readable by every Mac since System 3.
    Hfs,
    /// HFS+ (Mac OS Extended) — Mac OS 8.1 and later.
    HfsPlus,
}

impl MacCdFs {
    /// Name for log lines and `--help` text.
    pub fn label(self) -> &'static str {
        match self {
            MacCdFs::Hfs => "HFS",
            MacCdFs::HfsPlus => "HFS+",
        }
    }
}

/// Inputs for [`write_mac_hfs_cd`].
#[derive(Debug, Clone)]
pub struct MacCdOptions {
    /// Requested disc size in bytes. The volume is floored to a whole number of
    /// allocation blocks, so the result may come out slightly smaller.
    pub size_bytes: u64,
    /// Volume name, as it appears on the Mac desktop.
    pub name: String,
    pub fs: MacCdFs,
    /// Allocation block size. `None` picks the smallest that fits the volume
    /// (HFS) or Apple's 4 KiB default (HFS+).
    pub block_size: Option<u32>,
}

impl MacCdOptions {
    pub fn new(size_bytes: u64, name: impl Into<String>, fs: MacCdFs) -> Self {
        MacCdOptions {
            size_bytes,
            name: name.into(),
            fs,
            block_size: None,
        }
    }
}

/// The computed disc layout, for the CLI to print and tests to assert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MacCdLayout {
    pub disk_sectors: u64,
    pub disk_bytes: u64,
    /// First sector of the `Apple_HFS` partition.
    pub hfs_first_sector: u64,
    /// `Apple_HFS` partition length in 512-byte sectors — exactly the volume.
    pub hfs_sectors: u64,
    /// Allocation block size the volume was formatted with.
    pub block_size: u32,
    pub fs: MacCdFs,
}

impl MacCdLayout {
    pub fn hfs_bytes(&self) -> u64 {
        self.hfs_sectors * SECTOR
    }
}

/// Resolve the allocation block size and the volume budget, validating both
/// against the chosen filesystem's limits before any formatter can panic on
/// them. Returns `(block_size, volume_bytes)`.
fn plan_volume(opts: &MacCdOptions) -> Result<(u32, u64)> {
    let requested_sectors = opts.size_bytes.div_ceil(SECTOR);
    let avail_sectors = requested_sectors.saturating_sub(HFS_START_BLOCK);
    let avail_bytes = (avail_sectors * SECTOR).max(MIN_VOLUME_BYTES);

    let block_size = match opts.block_size {
        Some(bs) => bs,
        None => match opts.fs {
            MacCdFs::Hfs => crate::fs::hfs::pick_block_size(avail_bytes),
            // Apple's default, and the largest our HFS+ formatter supports.
            MacCdFs::HfsPlus => 4096,
        },
    };
    match opts.fs {
        MacCdFs::Hfs => ensure!(
            block_size != 0 && block_size % 512 == 0,
            "HFS block size must be a non-zero multiple of 512 (got {block_size})"
        ),
        MacCdFs::HfsPlus => ensure!(
            block_size.is_power_of_two() && (512..=4096).contains(&block_size),
            "HFS+ block size must be a power of two in [512, 4096] (got {block_size})"
        ),
    }

    // Floor the volume to a whole number of allocation blocks *and* whole CD
    // sectors, so the volume exactly fills its partition (the alternate MDB /
    // volume header lands on the partition's last sectors, where the Mac's
    // driver looks for it) and the disc still ends on a 2048-byte boundary.
    let unit = lcm(block_size as u64 / SECTOR, CD_SECTOR / SECTOR);
    let volume_sectors = (avail_bytes / SECTOR / unit) * unit;
    let volume_bytes = volume_sectors * SECTOR;
    ensure!(
        volume_bytes >= MIN_VOLUME_BYTES,
        "a {block_size}-byte allocation block leaves no room for a {} volume on a {}-byte disc",
        opts.fs.label(),
        opts.size_bytes
    );
    if opts.fs == MacCdFs::Hfs && volume_bytes / block_size as u64 > 65535 {
        bail!(
            "a {} MiB HFS volume needs more than 65535 allocation blocks at {} bytes each; \
             pass a larger --block-size (HFS's block count is 16-bit)",
            volume_bytes / (1024 * 1024),
            block_size
        );
    }
    Ok((block_size, volume_bytes))
}

/// Smallest common multiple of two sector counts (zero reads as one).
fn lcm(a: u64, b: u64) -> u64 {
    let (a, b) = (a.max(1), b.max(1));
    let (mut x, mut y) = (a, b);
    while y != 0 {
        let t = x % y;
        x = y;
        y = t;
    }
    a / x * b
}

/// Stream an APM-wrapped Mac CD-ROM image into `sink`: the partition map at
/// block 0 and a freshly formatted HFS / HFS+ volume at block 64, writing only
/// the non-zero regions so the free space stays a sparse hole. Returns the
/// [`MacCdLayout`]; the caller sizes the file to `layout.disk_bytes`.
pub fn write_mac_hfs_cd<W: Write + Seek>(sink: &mut W, opts: &MacCdOptions) -> Result<MacCdLayout> {
    let (block_size, volume_bytes) = plan_volume(opts)?;
    let volume_offset = HFS_START_BLOCK * SECTOR;

    // Format first: classic HFS floors its volume to whole allocation blocks
    // *after* reserving its bitmap, so only the formatter knows the exact
    // image size the partition entry has to describe.
    let image_bytes = match opts.fs {
        MacCdFs::Hfs => crate::fs::hfs::write_blank_hfs_into(
            sink,
            volume_offset,
            volume_bytes,
            block_size,
            &opts.name,
        )
        .map_err(|e| anyhow::anyhow!("formatting the HFS volume: {e}"))?,
        MacCdFs::HfsPlus => {
            crate::fs::hfsplus::write_blank_hfsplus_into(
                sink,
                volume_offset,
                volume_bytes,
                block_size,
                &opts.name,
                false,
            )
            .map_err(|e| anyhow::anyhow!("formatting the HFS+ volume: {e}"))?;
            volume_bytes
        }
    };

    let hfs_sectors = image_bytes / SECTOR;
    let disk_sectors =
        (HFS_START_BLOCK + hfs_sectors).div_ceil(CD_SECTOR / SECTOR) * (CD_SECTOR / SECTOR);
    ensure!(
        disk_sectors <= u32::MAX as u64,
        "disc too large for an Apple Partition Map: {disk_sectors} blocks exceeds the 32-bit \
         block range"
    );

    let mut apm = build_minimal_apm(
        &[(
            "Apple_HFS".to_string(),
            HFS_START_BLOCK as u32,
            hfs_sectors as u32,
        )],
        SECTOR as u32,
        disk_sectors as u32,
    );
    // "MacOS" is the entry name Apple's formatters write for a Mac OS volume;
    // Disk First Aid and pdisk both show it.
    apm.entries[1].name = "MacOS".to_string();
    let apm_blocks = apm.build_apm_blocks(Some(disk_sectors as u32));
    sink.seek(SeekFrom::Start(0))?;
    sink.write_all(&apm_blocks)?;

    Ok(MacCdLayout {
        disk_sectors,
        disk_bytes: disk_sectors * SECTOR,
        hfs_first_sector: HFS_START_BLOCK,
        hfs_sectors,
        block_size,
        fs: opts.fs,
    })
}

/// Build an APM-wrapped Mac CD-ROM image **in memory**. Returns the full disc
/// bytes (exactly `layout.disk_bytes`) plus the [`MacCdLayout`]. For real discs
/// prefer [`write_mac_hfs_cd`], which streams to a file without allocating the
/// whole image.
pub fn build_mac_hfs_cd(opts: &MacCdOptions) -> Result<(Vec<u8>, MacCdLayout)> {
    let mut cursor = Cursor::new(Vec::new());
    let layout = write_mac_hfs_cd(&mut cursor, opts)?;
    let mut image = cursor.into_inner();
    image.resize(layout.disk_bytes as usize, 0);
    Ok((image, layout))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::filesystem::Filesystem;
    use crate::partition::PartitionTable;

    fn open_volume(img: &[u8], layout: &MacCdLayout) -> Box<dyn Filesystem> {
        let table = PartitionTable::detect(&mut Cursor::new(img)).expect("APM detected");
        assert_eq!(table.type_name(), "APM");
        let parts = table.partitions();
        assert_eq!(parts.len(), 1, "only the Apple_HFS partition is listed");
        assert_eq!(parts[0].start_lba, layout.hfs_first_sector);
        assert_eq!(parts[0].size_bytes, layout.hfs_bytes());
        crate::fs::open_filesystem(
            Cursor::new(img.to_vec()),
            parts[0].start_lba * SECTOR,
            parts[0].partition_type_byte,
            parts[0].partition_type_string.as_deref(),
        )
        .expect("open the volume at the partition offset")
    }

    /// An HFS disc detects as APM, exposes exactly one `Apple_HFS` partition
    /// that opens as classic HFS with an empty, correctly-named root.
    #[test]
    fn hfs_disc_detects_and_opens() {
        let (img, layout) =
            build_mac_hfs_cd(&MacCdOptions::new(64 * 1024 * 1024, "MacCD", MacCdFs::Hfs)).unwrap();
        assert_eq!(img.len() as u64, layout.disk_bytes);
        assert_eq!(layout.disk_sectors % 4, 0, "whole 2048-byte CD sectors");
        assert_eq!(layout.hfs_first_sector, 64);

        let mut fs = open_volume(&img, &layout);
        assert_eq!(fs.fs_type(), "HFS");
        assert_eq!(fs.volume_label().unwrap_or_default(), "MacCD");
        let root = fs.root().expect("root");
        assert!(fs.list_directory(&root).expect("list root").is_empty());
    }

    /// The same for HFS+, which the `Apple_HFS` type string also covers — the
    /// factory probes the volume to tell the two apart.
    #[test]
    fn hfsplus_disc_detects_and_opens() {
        let (img, layout) = build_mac_hfs_cd(&MacCdOptions::new(
            64 * 1024 * 1024,
            "PlusCD",
            MacCdFs::HfsPlus,
        ))
        .unwrap();
        assert_eq!(layout.block_size, 4096);
        let mut fs = open_volume(&img, &layout);
        assert_eq!(fs.fs_type(), "HFS+");
        assert_eq!(fs.volume_label().unwrap_or_default(), "PlusCD");
        let root = fs.root().expect("root");
        assert!(fs.list_directory(&root).expect("list root").is_empty());
    }

    /// The partition covers exactly the volume image and nothing past it, so
    /// the alternate MDB / volume header sits on the partition's last sectors
    /// where the Mac's driver looks for it.
    #[test]
    fn partition_exactly_covers_the_volume() {
        for fs in [MacCdFs::Hfs, MacCdFs::HfsPlus] {
            let (_img, layout) =
                build_mac_hfs_cd(&MacCdOptions::new(32 * 1024 * 1024, "Exact", fs)).unwrap();
            assert_eq!(
                layout.hfs_bytes() % layout.block_size as u64,
                0,
                "{} volume is a whole number of allocation blocks",
                fs.label()
            );
            assert!(
                layout.hfs_first_sector + layout.hfs_sectors <= layout.disk_sectors,
                "{} partition fits on the disc",
                fs.label()
            );
        }
    }

    /// Streaming to a (sparse) file produces bytes identical to the in-memory
    /// build — untouched regions must read back as zero.
    #[test]
    fn streamed_file_matches_in_memory_image() {
        use std::io::Read;
        let opts = MacCdOptions::new(8 * 1024 * 1024, "Stream", MacCdFs::Hfs);
        let (mem, layout) = build_mac_hfs_cd(&opts).unwrap();

        let mut file = tempfile::tempfile().expect("tempfile");
        let streamed = write_mac_hfs_cd(&mut file, &opts).expect("stream");
        file.set_len(streamed.disk_bytes).unwrap();
        assert_eq!(streamed, layout);

        let mut on_disk = Vec::new();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.read_to_end(&mut on_disk).unwrap();
        assert_eq!(on_disk, mem, "streamed bytes match the in-memory image");
    }

    /// A 650 MiB HFS disc needs a block size above 512 to stay inside HFS's
    /// 16-bit allocation-block count; the auto pick handles that, and an
    /// explicit too-small block size is refused rather than mis-formatted.
    #[test]
    fn hfs_block_size_is_picked_and_validated() {
        let big = 650 * 1024 * 1024;
        let (_img, layout) =
            build_mac_hfs_cd(&MacCdOptions::new(big, "Big", MacCdFs::Hfs)).unwrap();
        assert!(layout.block_size >= 16384, "auto block size scales up");
        assert!(layout.hfs_bytes() / layout.block_size as u64 <= 65535);

        let mut too_small = MacCdOptions::new(big, "Big", MacCdFs::Hfs);
        too_small.block_size = Some(512);
        assert!(build_mac_hfs_cd(&too_small).is_err(), "65535-block ceiling");
    }

    /// An HFS+ block size the formatter would assert on is rejected with a
    /// message instead of panicking; a tiny request is floored to a valid
    /// disc rather than refused, matching the SGI CD builder.
    #[test]
    fn bad_block_size_refused_and_tiny_request_floored() {
        let mut bad_bs = MacCdOptions::new(16 * 1024 * 1024, "Bad", MacCdFs::HfsPlus);
        bad_bs.block_size = Some(8192);
        assert!(build_mac_hfs_cd(&bad_bs).is_err(), "HFS+ caps at 4096");

        let (img, layout) =
            build_mac_hfs_cd(&MacCdOptions::new(4096, "Tiny", MacCdFs::Hfs)).unwrap();
        assert!(layout.hfs_bytes() >= MIN_VOLUME_BYTES);
        let mut fs = open_volume(&img, &layout);
        assert!(fs.root().and_then(|r| fs.list_directory(&r)).is_ok());
    }
}
