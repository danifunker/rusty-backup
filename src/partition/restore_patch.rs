//! Restore-time rewrite of a disk label the single-file CHD carries verbatim.
//!
//! Every scheme here is backed up whole: the head region (label copies, the
//! RDSK/PART chain, the IPL, boot blocks) rides byte for byte, and a resize is
//! only ever applied on restore. The patchers rewrite the start/size fields
//! and checksums *in the verbatim bytes*, so everything else survives.
//!
//! Each scheme counts partitions in its own unit (cylinders on Sun and RDB,
//! 1024-byte sectors past a porch on NeXT, 256/512/1024 on X68k), so a patcher
//! may round a partition up and shift the ones after it. It returns the
//! overrides it actually applied, and the caller copies the bodies from those.

#[cfg(feature = "rust173-polyfill")]
use crate::rust173_compat::IntIsMultipleOf as _;
use std::io::Cursor;

use anyhow::{bail, Context, Result};
use byteorder::{BigEndian, ByteOrder};

use super::PartitionSizeOverride;

/// A head region with its table rewritten, plus the layout it now describes.
#[derive(Debug)]
pub struct PatchedHead {
    pub head: Vec<u8>,
    pub overrides: Vec<PartitionSizeOverride>,
}

/// Table types (as `metadata.partition_table_type` spells them) with a patcher.
pub fn can_patch(table_type: &str) -> bool {
    matches!(
        table_type,
        "Sun" | "NeXT" | "SGI" | "SGI-DkLabel" | "RDB" | "AHDI" | "X68k"
    )
}

/// Rewrite the label in `head` for `overrides`; the disk will be `target_size`.
pub fn patch_head_for_restore(
    table_type: &str,
    head: &[u8],
    overrides: &[PartitionSizeOverride],
    target_size: u64,
    log_cb: &mut dyn FnMut(&str),
) -> Result<PatchedHead> {
    match table_type {
        "Sun" => patch_sun(head, overrides, target_size, log_cb),
        "NeXT" => patch_next(head, overrides, target_size, log_cb),
        "SGI" => patch_sgi(head, overrides, target_size, log_cb),
        "SGI-DkLabel" => patch_sgi_dklabel(head, overrides, target_size, log_cb),
        "RDB" => patch_rdb(head, overrides, target_size, log_cb),
        "AHDI" => patch_ahdi(head, overrides, target_size, log_cb),
        "X68k" => patch_x68k(head, overrides, target_size, log_cb),
        other => bail!("no restore-time label patcher for {other} tables"),
    }
}

/// Lay `overrides` out in start order on a grid of `unit_bytes`, never below
/// `floor_byte`, rounding each size up; the shifted list is what gets written.
fn repack(
    overrides: &[PartitionSizeOverride],
    unit_bytes: u64,
    floor_byte: u64,
) -> Result<Vec<PartitionSizeOverride>> {
    if unit_bytes == 0 || !unit_bytes.is_multiple_of(512) {
        bail!("partition unit of {unit_bytes} bytes is not a multiple of 512");
    }
    let mut order: Vec<usize> = (0..overrides.len()).collect();
    order.sort_by_key(|&i| overrides[i].effective_start_lba());
    let mut out = overrides.to_vec();
    let mut cursor = floor_byte.div_ceil(unit_bytes) * unit_bytes;
    for i in order {
        let o = &overrides[i];
        let wanted = o.effective_start_lba() * 512;
        let start = wanted.max(cursor).div_ceil(unit_bytes) * unit_bytes;
        let size = o.export_size.div_ceil(unit_bytes) * unit_bytes;
        out[i].export_size = size;
        out[i].new_start_lba = if start / 512 != o.start_lba {
            Some(start / 512)
        } else {
            None
        };
        cursor = start + size;
    }
    Ok(out)
}

fn last_end(overrides: &[PartitionSizeOverride]) -> u64 {
    overrides
        .iter()
        .map(|o| o.effective_start_lba() * 512 + o.export_size)
        .max()
        .unwrap_or(0)
}

fn check_fits(overrides: &[PartitionSizeOverride], limit: u64, what: &str) -> Result<()> {
    let end = last_end(overrides);
    if end > limit {
        bail!(
            "the new layout ends at byte {end}, past {what} ({limit} bytes); \
             pick smaller sizes or a larger target"
        );
    }
    Ok(())
}

fn need(head: &[u8], len: usize, what: &str) -> Result<()> {
    if head.len() < len {
        bail!(
            "the head region is {} bytes; {what} needs {len}",
            head.len()
        );
    }
    Ok(())
}

// --- Sun ------------------------------------------------------------------------

fn patch_sun(
    head: &[u8],
    overrides: &[PartitionSizeOverride],
    target_size: u64,
    log_cb: &mut dyn FnMut(&str),
) -> Result<PatchedHead> {
    use super::sun::SunDiskLabel;
    need(head, 512, "a Sun label")?;
    let label = SunDiskLabel::parse(&head[..512]).context("Sun label in the backup")?;
    let spc = label.sectors_per_cylinder;
    if spc == 0 {
        bail!("Sun label has zero sectors per cylinder");
    }
    let unit = spc * 512;
    let floor = overrides
        .iter()
        .map(|o| o.start_lba * 512)
        .min()
        .unwrap_or(0);
    let adjusted = repack(overrides, unit, floor)?;
    let data_bytes = label.ncyl as u64 * unit;
    check_fits(
        &adjusted,
        target_size.min(data_bytes),
        "the label's data cylinders",
    )?;

    let mut out = head.to_vec();
    for o in &adjusted {
        let slice = label
            .slices
            .get(o.index)
            .ok_or_else(|| anyhow::anyhow!("Sun slice {} does not exist", o.index))?;
        if slice.start_sector != o.start_lba {
            bail!(
                "Sun slice {} starts at sector {} in the label but {} in the backup",
                o.index,
                slice.start_sector,
                o.start_lba
            );
        }
        let base = 444 + o.index * 8;
        let start_cyl = (o.effective_start_lba() * 512 / unit) as u32;
        let nblocks = (o.export_size / 512) as u32;
        BigEndian::write_u32(&mut out[base..base + 4], start_cyl);
        BigEndian::write_u32(&mut out[base + 4..base + 8], nblocks);
    }
    let mut csum: u16 = 0;
    for w in out[..510].chunks_exact(2) {
        csum ^= u16::from_be_bytes([w[0], w[1]]);
    }
    BigEndian::write_u16(&mut out[510..512], csum);
    log_cb("  table: Sun label rewritten (slices moved to cylinder boundaries)");
    Ok(PatchedHead {
        head: out,
        overrides: adjusted,
    })
}

// --- NeXT -----------------------------------------------------------------------

fn patch_next(
    head: &[u8],
    overrides: &[PartitionSizeOverride],
    target_size: u64,
    log_cb: &mut dyn FnMut(&str),
) -> Result<PatchedHead> {
    use super::next::{self, NextDiskLabel, LABEL_SPAN};
    need(head, LABEL_SPAN, "a NeXT label")?;
    let label = NextDiskLabel::parse(&head[..LABEL_SPAN], 0).context("NeXT label in the backup")?;
    let ss = label.sector_size as u64;
    if ss == 0 || !ss.is_multiple_of(512) {
        bail!("NeXT label sector size {ss} is not a multiple of 512");
    }
    let floor = label.front_porch as u64 * ss;
    let adjusted = repack(overrides, ss, floor)?;
    let usable = target_size.saturating_sub(label.back_porch as u64 * ss);
    check_fits(&adjusted, usable, "the back porch")?;

    let mut copy = head[..LABEL_SPAN].to_vec();
    for o in &adjusted {
        let p = label
            .partitions
            .get(o.index)
            .ok_or_else(|| anyhow::anyhow!("NeXT partition {} does not exist", o.index))?;
        if p.start_byte != o.start_lba * 512 {
            bail!(
                "NeXT partition {} starts at byte {} in the label but {} in the backup",
                o.index,
                p.start_byte,
                o.start_lba * 512
            );
        }
        let base = (o.effective_start_lba() * 512 / ss) as i64 - label.front_porch as i64;
        let size = (o.export_size / ss) as i64;
        if base < 0 || base > i32::MAX as i64 || size > i32::MAX as i64 {
            bail!(
                "NeXT partition {} would not fit the label's 32-bit fields",
                o.index
            );
        }
        next::set_partition_extent(&mut copy, o.index, base as i32, size as i32);
    }
    NextDiskLabel::stamp_checksum(&mut copy, label.version);

    let blocks = next::present_copies(&mut Cursor::new(head.to_vec()));
    if blocks.is_empty() {
        bail!("no valid NeXT label copy in the head region");
    }
    let mut out = Cursor::new(head.to_vec());
    next::write_copies(&mut out, &copy, &blocks).context("rewrite the NeXT label copies")?;
    log_cb(&format!(
        "  table: NeXT label rewritten in {} copies",
        blocks.len()
    ));
    Ok(PatchedHead {
        head: out.into_inner(),
        overrides: adjusted,
    })
}

// --- SGI volume header ----------------------------------------------------------

fn patch_sgi(
    head: &[u8],
    overrides: &[PartitionSizeOverride],
    target_size: u64,
    log_cb: &mut dyn FnMut(&str),
) -> Result<PatchedHead> {
    use super::sgi::{SgiPartitionEntry, SgiVolumeHeader, SGI_NUM_PARTITIONS};
    need(head, 512, "an SGI volume header")?;
    let mut vh = SgiVolumeHeader::parse(&head[..512]).context("SGI volume header in the backup")?;
    let adjusted = repack(overrides, 512, 0)?;
    check_fits(&adjusted, target_size, "the target")?;
    while vh.partitions.len() < SGI_NUM_PARTITIONS {
        vh.partitions.push(SgiPartitionEntry {
            blocks: 0,
            first: 0,
            partition_type_raw: 0,
        });
    }
    for o in &adjusted {
        let e = vh
            .partitions
            .get_mut(o.index)
            .ok_or_else(|| anyhow::anyhow!("SGI slot {} does not exist", o.index))?;
        if e.first as u64 != o.start_lba {
            bail!(
                "SGI slot {} starts at block {} in the header but {} in the backup",
                o.index,
                e.first,
                o.start_lba
            );
        }
        e.first = o.effective_start_lba() as u32;
        e.blocks = (o.export_size / 512) as u32;
    }
    let mut out = head.to_vec();
    out[..512].copy_from_slice(&vh.to_bytes());
    log_cb("  table: SGI volume header rewritten");
    Ok(PatchedHead {
        head: out,
        overrides: adjusted,
    })
}

// --- SGI disk label (IRIS 2000/3000) ----------------------------------------------

fn patch_sgi_dklabel(
    head: &[u8],
    overrides: &[PartitionSizeOverride],
    target_size: u64,
    log_cb: &mut dyn FnMut(&str),
) -> Result<PatchedHead> {
    use super::sgi_dklabel::{apply_byte_order, SgiDiskLabel, OFF_MAP};
    need(head, 512, "an SGI disk label")?;
    let label = SgiDiskLabel::parse(&head[..512]).context("SGI disk label in the backup")?;
    let adjusted = repack(overrides, 512, 0)?;
    let usable = if label.altstart > 0 {
        target_size.min(label.altstart as u64 * 512)
    } else {
        target_size
    };
    check_fits(&adjusted, usable, "the alternates region")?;
    let mut out = head.to_vec();
    for o in &adjusted {
        let m = label
            .map
            .get(o.index)
            .ok_or_else(|| anyhow::anyhow!("SGI disk label slot {} does not exist", o.index))?;
        if m.base as u64 != o.start_lba {
            bail!(
                "SGI disk label slot {} starts at block {} in the label but {} in the backup",
                o.index,
                m.base,
                o.start_lba
            );
        }
        let off = OFF_MAP + o.index * 8;
        BigEndian::write_u32(&mut out[off..off + 4], o.effective_start_lba() as u32);
        BigEndian::write_u32(&mut out[off + 4..off + 8], (o.export_size / 512) as u32);
        // The label's own word order: a swabbed image wants these words swapped too.
        apply_byte_order(label.byte_order, &mut out[off..off + 8]);
    }
    log_cb(&format!(
        "  table: SGI disk label rewritten ({} word order)",
        label.byte_order.display_name()
    ));
    Ok(PatchedHead {
        head: out,
        overrides: adjusted,
    })
}

// --- Amiga RDB ------------------------------------------------------------------

fn patch_rdb(
    head: &[u8],
    overrides: &[PartitionSizeOverride],
    target_size: u64,
    log_cb: &mut dyn FnMut(&str),
) -> Result<PatchedHead> {
    use super::rdb::Rdb;
    let rdb = Rdb::parse(&mut Cursor::new(head.to_vec())).context("RDB in the backup")?;
    // PART blocks count in cylinders, and the patcher below refuses a start off
    // one; lay the disk out on the cylinder grid first and hand it explicit starts.
    let cyl_units: Vec<u64> = rdb
        .partitions
        .iter()
        .map(|p| p.surfaces as u64 * p.blk_per_trk as u64 * p.fs_block_size() / 512)
        .collect();
    let unit_lbas = cyl_units.iter().copied().max().unwrap_or(0);
    if unit_lbas == 0 || cyl_units.iter().any(|&c| c == 0 || unit_lbas % c != 0) {
        bail!("the RDB's partitions do not share a cylinder size, so they cannot be repacked");
    }
    let floor = overrides
        .iter()
        .map(|o| o.start_lba * 512)
        .min()
        .unwrap_or(0);
    let explicit: Vec<PartitionSizeOverride> = repack(overrides, unit_lbas * 512, floor)?
        .into_iter()
        .map(|mut o| {
            o.new_start_lba = Some(o.effective_start_lba());
            o
        })
        .collect();
    let plan = rdb
        .patch_for_restore(&explicit, &mut Cursor::new(head.to_vec()))
        .context("patch the RDSK/PART blocks")?;
    if plan.new_disk_size_bytes > target_size {
        bail!(
            "the new Amiga layout needs {} bytes but the target is {} bytes",
            plan.new_disk_size_bytes,
            target_size
        );
    }
    let mut out = head.to_vec();
    let mut place = |block: u64, buf: &[u8; 512]| -> Result<()> {
        let off = (block * 512) as usize;
        if off + 512 > out.len() {
            bail!("RDB block {block} lies past the head region");
        }
        out[off..off + 512].copy_from_slice(buf);
        Ok(())
    };
    place(plan.rdsk_block.0, &plan.rdsk_block.1)?;
    for (block, buf) in &plan.part_blocks {
        place(*block, buf)?;
    }
    // The plan is in PART-chain order, which is how `partitions()` numbers them.
    let mut adjusted = overrides.to_vec();
    for o in adjusted.iter_mut() {
        let p = plan
            .partition_plans
            .get(o.index)
            .ok_or_else(|| anyhow::anyhow!("RDB partition {} does not exist", o.index))?;
        if p.source_lba != o.start_lba {
            bail!(
                "RDB partition {} starts at LBA {} in the chain but {} in the backup",
                o.index,
                p.source_lba,
                o.start_lba
            );
        }
        o.export_size = p.export_size;
        o.new_start_lba = if p.dest_lba != o.start_lba {
            Some(p.dest_lba)
        } else {
            None
        };
    }
    log_cb(&format!(
        "  table: RDSK and {} PART block(s) rewritten on cylinder boundaries",
        plan.part_blocks.len()
    ));
    Ok(PatchedHead {
        head: out,
        overrides: adjusted,
    })
}

// --- Atari AHDI -----------------------------------------------------------------

fn patch_ahdi(
    head: &[u8],
    overrides: &[PartitionSizeOverride],
    target_size: u64,
    log_cb: &mut dyn FnMut(&str),
) -> Result<PatchedHead> {
    use super::atari::{AhdiTable, AHDI_NUM_SLOTS};
    need(head, 512, "an AHDI root sector")?;
    let table = AhdiTable::parse_root(&head[..512]).context("AHDI root sector in the backup")?;
    let adjusted = repack(overrides, 512, 0)?;
    check_fits(&adjusted, target_size, "the target")?;
    let mut out = head.to_vec();
    for o in &adjusted {
        if o.index >= AHDI_NUM_SLOTS {
            bail!(
                "AHDI logical partition {} lives in an XGM chain no writer rebuilds",
                o.index
            );
        }
        let e = &table.primary[o.index];
        if e.start_sector as u64 != o.start_lba {
            bail!(
                "AHDI slot {} starts at sector {} in the root sector but {} in the backup",
                o.index,
                e.start_sector,
                o.start_lba
            );
        }
        let off = 0x1C6 + o.index * 12;
        BigEndian::write_u32(&mut out[off + 4..off + 8], o.effective_start_lba() as u32);
        BigEndian::write_u32(&mut out[off + 8..off + 12], (o.export_size / 512) as u32);
    }
    if table.disk_size_sectors != 0 {
        BigEndian::write_u32(&mut out[0x1F6..0x1FA], (target_size / 512) as u32);
    }
    // Only a bootable root sector carries the 0x1234 word-sum; keep a plain one plain.
    if table.checksum_valid {
        let mut sum: u32 = 0;
        for w in out[..0x1FE].chunks_exact(2) {
            sum = sum.wrapping_add(u16::from_be_bytes([w[0], w[1]]) as u32);
        }
        let cksum = (0x1234u32.wrapping_sub(sum) & 0xFFFF) as u16;
        BigEndian::write_u16(&mut out[0x1FE..0x200], cksum);
    }
    log_cb("  table: AHDI root sector rewritten");
    Ok(PatchedHead {
        head: out,
        overrides: adjusted,
    })
}

// --- Sharp X68000 ---------------------------------------------------------------

fn patch_x68k(
    head: &[u8],
    overrides: &[PartitionSizeOverride],
    target_size: u64,
    log_cb: &mut dyn FnMut(&str),
) -> Result<PatchedHead> {
    use super::x68k::{
        X68kPartitionTable, X68K_ENTRY_SIZE, X68K_MAX_PARTITIONS, X68K_TABLE_HEADER_SIZE,
    };
    // The same probe `PartitionTable::detect` runs: the table's offset decides
    // the sector size when the boot signature does not.
    let (_, table_off, ss) =
        X68kPartitionTable::detect_with_geometry(&mut Cursor::new(head.to_vec()))
            .context("probe the X68k table in the head region")?
            .ok_or_else(|| anyhow::anyhow!("no X68k table in the head region"))?;
    let table_off = table_off as usize;
    let table_len = X68K_TABLE_HEADER_SIZE + X68K_MAX_PARTITIONS * X68K_ENTRY_SIZE;
    need(head, table_off + table_len, "the X68k table")?;
    let floor = overrides
        .iter()
        .map(|o| o.start_lba * 512)
        .min()
        .unwrap_or(0);
    // Bodies sit on 512-byte LBAs, so a 256-byte SASI disk rounds to two sectors.
    let adjusted = repack(overrides, ss.max(512), floor)?;
    check_fits(&adjusted, target_size, "the target")?;
    let mut out = head.to_vec();
    let block = &mut out[table_off..table_off + table_len];
    for o in &adjusted {
        // Slots are matched by where the partition starts; the high byte of the
        // start word carries flags and is kept.
        let slot = (0..X68K_MAX_PARTITIONS).find(|&i| {
            let e = X68K_TABLE_HEADER_SIZE + i * X68K_ENTRY_SIZE;
            let start = (BigEndian::read_u32(&block[e + 8..e + 12]) & 0x00FF_FFFF) as u64;
            let len = BigEndian::read_u32(&block[e + 12..e + 16]);
            len != 0 && start * ss == o.start_lba * 512
        });
        let Some(slot) = slot else {
            bail!(
                "no X68k slot starts at byte {} for partition {}",
                o.start_lba * 512,
                o.index
            );
        };
        let e = X68K_TABLE_HEADER_SIZE + slot * X68K_ENTRY_SIZE;
        let flags = BigEndian::read_u32(&block[e + 8..e + 12]) & 0xFF00_0000;
        let new_start = (o.effective_start_lba() * 512 / ss) as u32 & 0x00FF_FFFF;
        BigEndian::write_u32(&mut block[e + 8..e + 12], flags | new_start);
        BigEndian::write_u32(&mut block[e + 12..e + 16], (o.export_size / ss) as u32);
    }
    if BigEndian::read_u32(&block[4..8]) != 0 {
        let sectors = (target_size / ss) as u32;
        BigEndian::write_u32(&mut block[4..8], sectors);
        BigEndian::write_u32(&mut block[8..12], sectors);
    }
    log_cb(&format!(
        "  table: X68k table rewritten ({ss}-byte sectors)"
    ));
    Ok(PatchedHead {
        head: out,
        overrides: adjusted,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::provision::{self, Geometry, PartSpec};
    use crate::partition::type_catalog::TableKind;
    use crate::partition::PartitionTable;
    use std::io::{Read, Seek, SeekFrom};

    const DISK: u64 = 64 * 1024 * 1024;

    /// A two-partition disk of `kind` from the provisioner, as raw bytes.
    fn disk_of(kind: TableKind) -> Vec<u8> {
        let geometry = Geometry::default();
        let align = provision::default_align(kind, geometry);
        let specs = vec![
            PartSpec {
                size: Some(16 * 1024 * 1024),
                ..Default::default()
            },
            PartSpec {
                size: None,
                ..Default::default()
            },
        ];
        let placed = provision::place(&specs, kind, DISK, align, geometry)
            .unwrap_or_else(|e| panic!("{} place: {e:#}", kind.label()));
        let mut file = tempfile::tempfile().unwrap();
        file.set_len(DISK).unwrap();
        provision::write_table(&mut file, kind, &placed, DISK, geometry)
            .unwrap_or_else(|e| panic!("{} write: {e:#}", kind.label()));
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        bytes
    }

    /// Shrink the first partition to 4 MiB, slide the second down after it.
    fn shrink_first(parts: &[crate::partition::PartitionInfo]) -> Vec<PartitionSizeOverride> {
        let a = &parts[0];
        let b = &parts[1];
        let new_a = 4 * 1024 * 1024;
        vec![
            PartitionSizeOverride {
                index: a.index,
                start_lba: a.start_lba,
                original_size: a.size_bytes,
                export_size: new_a,
                new_start_lba: None,
                heads: 0,
                sectors_per_track: 0,
            },
            PartitionSizeOverride {
                index: b.index,
                start_lba: b.start_lba,
                original_size: b.size_bytes,
                export_size: b.size_bytes,
                new_start_lba: Some(a.start_lba + new_a / 512),
                heads: 0,
                sectors_per_track: 0,
            },
        ]
    }

    fn head_len(parts: &[crate::partition::PartitionInfo]) -> usize {
        parts
            .iter()
            .map(|p| p.byte_offset())
            .min()
            .unwrap_or(512)
            .max(512) as usize
    }

    /// Every scheme: patch, re-detect, and the label describes the new layout.
    #[test]
    fn every_patchable_scheme_reparses_to_the_new_layout() {
        for kind in [
            TableKind::Sun,
            TableKind::Next,
            TableKind::Sgi,
            TableKind::SgiDkLabel,
            TableKind::Rdb,
            TableKind::Atari,
            TableKind::X68k,
        ] {
            let bytes = disk_of(kind);
            let table = PartitionTable::detect(&mut Cursor::new(bytes.clone())).unwrap();
            let name = table.type_name();
            assert!(can_patch(name), "{name}");
            let parts = table.partitions();
            assert_eq!(parts.len(), 2, "{name}");
            let overrides = shrink_first(&parts);
            let head = &bytes[..head_len(&parts)];
            let mut log = Vec::new();
            let patched = patch_head_for_restore(name, head, &overrides, DISK, &mut |m| {
                log.push(m.to_string())
            })
            .unwrap_or_else(|e| panic!("{name}: {e:#}"));
            assert_eq!(patched.head.len(), head.len(), "{name}: head length");

            let mut disk = bytes.clone();
            disk[..patched.head.len()].copy_from_slice(&patched.head);
            let again = PartitionTable::detect(&mut Cursor::new(disk)).unwrap();
            let after = again.partitions();
            assert_eq!(after.len(), 2, "{name}: partition count after patch");
            for (o, p) in patched.overrides.iter().zip(after.iter()) {
                assert_eq!(p.index, o.index, "{name}: slot");
                assert_eq!(p.start_lba, o.effective_start_lba(), "{name}: start");
                assert_eq!(p.size_bytes, o.export_size, "{name}: size");
            }
            assert!(
                after[0].size_bytes < parts[0].size_bytes,
                "{name}: the first partition must have shrunk"
            );
            assert!(
                after[1].start_lba < parts[1].start_lba,
                "{name}: the second must have moved down"
            );
            assert!(!log.is_empty(), "{name}: the patcher must say what it did");
        }
    }

    /// Bytes outside the entries are untouched: boot code, driver chains, the IPL.
    #[test]
    fn a_patch_leaves_the_rest_of_the_head_verbatim() {
        for kind in [
            TableKind::Sgi,
            TableKind::Atari,
            TableKind::X68k,
            TableKind::Rdb,
        ] {
            let bytes = disk_of(kind);
            let table = PartitionTable::detect(&mut Cursor::new(bytes.clone())).unwrap();
            let name = table.type_name();
            let parts = table.partitions();
            let head = &bytes[..head_len(&parts)];
            let patched =
                patch_head_for_restore(name, head, &shrink_first(&parts), DISK, &mut |_| {})
                    .unwrap();
            let changed = head
                .iter()
                .zip(patched.head.iter())
                .filter(|(a, b)| a != b)
                .count();
            assert!(
                changed > 0 && changed <= 4 * 512,
                "{name}: {changed} bytes changed"
            );
        }
    }

    #[test]
    fn a_layout_past_the_target_is_refused() {
        let bytes = disk_of(TableKind::Sgi);
        let table = PartitionTable::detect(&mut Cursor::new(bytes.clone())).unwrap();
        let parts = table.partitions();
        let mut overrides = shrink_first(&parts);
        overrides[1].export_size = DISK;
        let err = patch_head_for_restore("SGI", &bytes[..512], &overrides, DISK, &mut |_| {})
            .unwrap_err();
        assert!(err.to_string().contains("past"), "{err:#}");
    }

    #[test]
    fn a_slot_that_moved_since_backup_is_refused() {
        let bytes = disk_of(TableKind::Atari);
        let table = PartitionTable::detect(&mut Cursor::new(bytes.clone())).unwrap();
        let parts = table.partitions();
        let mut overrides = shrink_first(&parts);
        overrides[0].start_lba += 1;
        let err = patch_head_for_restore("AHDI", &bytes[..512], &overrides, DISK, &mut |_| {})
            .unwrap_err();
        assert!(err.to_string().contains("starts at"), "{err:#}");
    }

    #[test]
    fn repack_rounds_up_and_never_overlaps() {
        let overrides = vec![
            PartitionSizeOverride::size_only(0, 100, 1000 * 512, 1001 * 512),
            PartitionSizeOverride::size_only(1, 2000, 500 * 512, 500 * 512),
        ];
        let out = repack(&overrides, 100 * 512, 0).unwrap();
        assert_eq!(out[0].export_size, 1100 * 512);
        assert_eq!(out[0].new_start_lba, None);
        assert_eq!(out[1].effective_start_lba(), 2000);
        let out = repack(&overrides, 100 * 512, 150 * 512).unwrap();
        assert_eq!(out[0].effective_start_lba(), 200);
    }
}
