//! Parsing an image's record stream and splitting it into partitions.

use super::*;

/// Sector size used by the container header.
pub const GHO_SECTOR_SIZE: u64 = 512;

/// One inner record located by [`parse_gho_image`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GhoInnerRecord {
    pub offset: u64,
    pub type_code: u16,
    pub body_len: u16,
}

impl GhoInnerRecord {
    pub fn body_start(&self) -> u64 {
        self.offset + GHO_RECORD_HEADER_LEN as u64
    }
    pub fn body_end(&self) -> u64 {
        self.body_start() + self.body_len as u64
    }
}

/// Result of walking the inner record stream.
#[derive(Debug, Clone)]
pub struct GhoImage {
    pub records: Vec<GhoInnerRecord>,
    /// Pre-read record bodies, keyed by record offset. Populated
    /// during `parse_gho_image` for dir entries, boot sectors, and
    /// checksums so that `walk_file_aware_tree` and `split_partitions`
    /// can avoid re-seeking into the file.
    pub cached_bodies: std::collections::HashMap<u64, Vec<u8>>,
    /// Count of `0x0017` boot-sector records — proxy for partition count.
    pub partition_count: usize,
}

impl GhoImage {
    /// All data-block records (type `0x0002`) in stream order.
    pub fn data_blocks(&self) -> impl Iterator<Item = &GhoInnerRecord> {
        self.records
            .iter()
            .filter(|r| r.type_code == GHO_REC_DATA_BLOCK)
    }
}

/// Walk the inner record stream. Starts scanning past the container
/// header (sector 0); uses [`find_inner_stream_start`] to locate the
/// first record so 7.5 vs 11.5 padding differences are absorbed.
///
/// Each record advances the cursor by `HEADER + body_len`, since records
/// in this dialect are back-to-back (no scan-forward needed between them).
/// If a record's `body_len` walks us into garbage, we scan forward for
/// the next valid magic — defensive against minor corruption.
pub fn parse_gho_image<R: Read + Seek>(
    reader: &mut R,
    file_size: u64,
    header: &GhoContainerHeader,
) -> Result<GhoImage> {
    let header_end = if header.password_protected {
        (GHO_HEADER_PREFIX_LEN + GHO_PASSWORD_VERIFIER_LEN) as u64
    } else {
        GHO_HEADER_PREFIX_LEN as u64
    };
    let mut offset = match find_inner_stream_start(reader, header_end) {
        Ok(o) => o,
        Err(_) => {
            return Ok(GhoImage {
                records: Vec::new(),
                cached_bodies: std::collections::HashMap::new(),
                partition_count: 0,
            })
        }
    };

    let mut records = Vec::new();
    let mut cached_bodies = std::collections::HashMap::new();
    let mut partition_count = 0;
    while offset + GHO_RECORD_HEADER_LEN as u64 <= file_size {
        reader.seek(SeekFrom::Start(offset))?;
        let rec = match GhoRecordHeader::read_from(reader) {
            Ok(Some(r)) => r,
            Ok(None) => break,
            Err(_) => {
                // Bad magic — try to recover by scanning forward to next
                // record magic. If none, we're done.
                match find_inner_stream_start(reader, offset + 1) {
                    Ok(o) if o > offset && o < file_size => {
                        offset = o;
                        continue;
                    }
                    _ => break,
                }
            }
        };
        let inner = GhoInnerRecord {
            offset,
            type_code: rec.type_code,
            body_len: rec.body_len,
        };
        if is_boot_sector_record(inner.type_code) && inner.body_len == 512 {
            partition_count += 1;
        }

        // Eagerly read small record bodies that walk_file_aware_tree
        // and split_partitions will need later. The reader is already
        // positioned at body_start (right after the 10-byte header),
        // so this is free sequential I/O — no extra seeks.
        let should_cache = is_boot_sector_record(inner.type_code)
            || is_dir_entry_record(inner.type_code)
            || is_checksum_record(inner.type_code);
        if should_cache && inner.body_len > 0 {
            let mut body = vec![0u8; inner.body_len as usize];
            if reader.read_exact(&mut body).is_ok() {
                cached_bodies.insert(inner.offset, body);
            }
        }

        records.push(inner);
        offset = inner.body_end();
    }
    Ok(GhoImage {
        records,
        cached_bodies,
        partition_count,
    })
}

/// One partition's slice of the inner record stream. Produced by
/// [`split_partitions`] for multi-partition file-aware GHOs.
#[derive(Debug, Clone)]
pub struct GhoPartitionSlice {
    /// Index into `GhoImage.records` of this partition's boot-sector record.
    pub boot_record_index: usize,
    /// Inclusive start and exclusive end into `GhoImage.records`.
    pub record_range: std::ops::Range<usize>,
    /// Partition size in bytes (from VBR `total_sectors × bytes_per_sector`).
    pub partition_size: u64,
    /// VBR `hidden_sectors` — the partition's LBA offset on the source disk.
    pub hidden_sectors: u64,
    /// FAT type string: `"FAT32"`, `"FAT16"`, `"FAT12"`, or `"Unknown"`.
    pub fs_type: String,
    /// Index of the `0x0118` reserved-sectors record, if present.
    pub reserved_record_index: Option<usize>,
}

/// Read a VBR from a boot-sector record body and extract geometry.
pub(super) fn parse_vbr_geometry<R: Read + Seek>(
    reader: &mut R,
    rec: &GhoInnerRecord,
    cache: &std::collections::HashMap<u64, Vec<u8>>,
) -> Result<(u64, u64, String)> {
    if (rec.body_len as usize) < 64 {
        bail!("boot-sector body too short: {} bytes", rec.body_len);
    }
    let bpb_vec = read_record_body(reader, rec, cache)?;
    let byts_per_sec = u16::from_le_bytes([bpb_vec[11], bpb_vec[12]]) as u64;
    if byts_per_sec == 0 || byts_per_sec > 4096 {
        bail!("invalid bytes_per_sector: {}", byts_per_sec);
    }
    Ok(vbr_geometry(&bpb_vec))
}

/// (hidden sectors, partition bytes, fs type) from a FAT or NTFS boot sector of at least 64 bytes.
pub(super) fn vbr_geometry(bpb: &[u8]) -> (u64, u64, String) {
    let n = bpb.len();
    let bps = (u16::from_le_bytes([bpb[11], bpb[12]]) as u64).max(512);
    let hidden = u32::from_le_bytes([bpb[28], bpb[29], bpb[30], bpb[31]]) as u64;
    if n >= 0x30 && &bpb[3..11] == b"NTFS    " {
        // NTFS counts its sectors at 0x28; the backup boot sector sits one past them.
        let mut t = [0u8; 8];
        t.copy_from_slice(&bpb[0x28..0x30]);
        let total = u64::from_le_bytes(t).saturating_add(1);
        return (hidden, total.saturating_mul(bps), "NTFS".to_string());
    }
    let tot16 = u16::from_le_bytes([bpb[19], bpb[20]]) as u64;
    let tot32 = u32::from_le_bytes([bpb[32], bpb[33], bpb[34], bpb[35]]) as u64;
    let total_sectors = if tot16 != 0 { tot16 } else { tot32 };
    let fs_type = if n >= 90 && &bpb[82..87] == b"FAT32" {
        "FAT32"
    } else if n >= 62 && &bpb[54..59] == b"FAT16" {
        "FAT16"
    } else if n >= 62 && &bpb[54..59] == b"FAT12" {
        "FAT12"
    } else {
        "Unknown"
    };
    (hidden, total_sectors * bps, fs_type.to_string())
}

/// Split a multi-partition file-aware `GhoImage` into per-partition
/// slices. Each slice owns a range of records and carries the VBR
/// geometry needed for disk reconstruction.
///
/// The partition boundary pattern is a pair of `0x0117` records: the
/// first repeats the previous partition's VBR, the second carries the
/// new partition's VBR. We skip the "repeat" and use the second as the
/// new partition's boot record.
pub fn split_partitions<R: Read + Seek>(
    reader: &mut R,
    image: &GhoImage,
) -> Result<Vec<GhoPartitionSlice>> {
    let mut slices = Vec::new();
    let records = &image.records;
    if records.is_empty() {
        return Ok(slices);
    }

    // Identify boot-sector record indices (each marks a potential
    // partition start). We skip "repeat" VBRs by deduplicating
    // consecutive 0x0117 pairs: when two 0x0117 records are adjacent,
    // the first is a repeat and the second is the real new partition.
    let mut boot_indices: Vec<usize> = Vec::new();
    let mut i = 0;
    while i < records.len() {
        let rec = &records[i];
        if is_boot_sector_record(rec.type_code) && rec.body_len == 512 {
            // Check for 0x0117-pair: if next record is also a boot
            // sector, this one is a repeat — skip it.
            if i + 1 < records.len()
                && is_boot_sector_record(records[i + 1].type_code)
                && records[i + 1].body_len == 512
            {
                i += 1; // skip the repeat, loop will process the real one
                continue;
            }
            // Footer detection: a lone 0x0117 (not the first boot
            // record) that ISN'T followed by partition data (dir
            // entries, data blocks) is the "last VBR copy" footer.
            if rec.type_code == GHO_REC_BOOT_SECTOR_PARTITION {
                if i + 1 >= records.len() {
                    i += 1;
                    continue;
                }
                let next = records[i + 1].type_code;
                if !is_dir_entry_record(next) && !is_data_block_record(next) {
                    i += 1;
                    continue;
                }
            }
            boot_indices.push(i);
        }
        i += 1;
    }

    if boot_indices.is_empty() {
        return Ok(slices);
    }

    for (pi, &boot_idx) in boot_indices.iter().enumerate() {
        let rec = &records[boot_idx];
        let (hidden, size, fs_type) = match parse_vbr_geometry(reader, rec, &image.cached_bodies) {
            Ok(g) => g,
            Err(_) => continue,
        };

        // Record range: from boot_idx to the next partition's boot_idx
        // (exclusive), or to the first footer record (0x0005/0x0023),
        // or to end of records.
        let range_end = if pi + 1 < boot_indices.len() {
            // End just before the 0x0117 PAIR that starts the next
            // partition. The pair starts one record before the next
            // boot_idx (since we skipped the repeat).
            let next_boot = boot_indices[pi + 1];
            if next_boot > 0
                && is_boot_sector_record(records[next_boot - 1].type_code)
                && records[next_boot - 1].body_len == 512
            {
                next_boot - 1
            } else {
                next_boot
            }
        } else {
            // Last partition: end before footer records.
            let mut end = records.len();
            for j in boot_idx..records.len() {
                let t = records[j].type_code;
                if t == GHO_REC_FILE_CATALOG || t == GHO_REC_END {
                    end = j;
                    break;
                }
                // A lone 0x0117 after the last partition's data is
                // the "last VBR copy" footer — stop before it.
                if j > boot_idx
                    && records[j].type_code == GHO_REC_BOOT_SECTOR_PARTITION
                    && records[j].body_len == 512
                {
                    let after = if j + 1 < records.len() {
                        records[j + 1].type_code
                    } else {
                        0
                    };
                    if after == GHO_REC_FILE_CATALOG
                        || after == GHO_REC_END
                        || after == GHO_REC_BOOT_SECTOR_PARTITION
                    {
                        end = j;
                        break;
                    }
                }
            }
            end
        };

        // Look for a 0x0118 reserved-sectors record in this slice.
        let reserved_record_index =
            (boot_idx..range_end).find(|&j| records[j].type_code == GHO_REC_RESERVED_SECTORS);

        slices.push(GhoPartitionSlice {
            boot_record_index: boot_idx,
            record_range: boot_idx..range_end,
            partition_size: size,
            hidden_sectors: hidden,
            fs_type,
            reserved_record_index,
        });
    }

    Ok(slices)
}

/// Resolve absolute disk LBA offsets for a set of partition slices.
/// The first partition's `hidden_sectors` is treated as absolute.
/// Subsequent partitions that overlap with the first partition's
/// range are treated as relative (logical partitions inside an
/// extended container) and their absolute offset is computed as
/// `prev_partition_end + hidden_sectors`.
pub(super) fn resolve_absolute_offsets(slices: &mut [GhoPartitionSlice]) {
    if slices.is_empty() {
        return;
    }
    let p0_end = slices[0].hidden_sectors + slices[0].partition_size / 512;
    let mut prev_end = p0_end;
    for slice in &mut slices[1..] {
        let hidden = slice.hidden_sectors;
        let sectors = slice.partition_size / 512;
        if hidden < p0_end && hidden + sectors <= p0_end {
            let abs = prev_end + hidden;
            slice.hidden_sectors = abs;
            prev_end = abs + sectors;
        } else {
            prev_end = hidden + sectors;
        }
    }
}

/// MBR partition type byte for FAT32 (LBA).
pub(super) const MBR_TYPE_FAT32_LBA: u8 = 0x0C;
/// MBR partition type byte for FAT16 (LBA, > 32 MB).
pub(super) const MBR_TYPE_FAT16_LBA: u8 = 0x0E;
/// MBR partition type byte for extended (LBA).
pub(super) const MBR_TYPE_EXTENDED_LBA: u8 = 0x0F;

/// MBR type byte for a partition Ghost recorded as `fs`.
pub(super) fn mbr_type_for(fs: &str) -> u8 {
    match fs {
        "FAT32" => MBR_TYPE_FAT32_LBA,
        "FAT16" => MBR_TYPE_FAT16_LBA,
        "FAT12" => 0x01,
        "NTFS" => 0x07,
        _ => 0xDA, // non-FS data: nothing guesses a filesystem from it
    }
}

/// The MBR (at LBA 0) and any EBRs for a multi-partition image, keyed by LBA.
/// The second partition on are logical when each has 63 free sectors before it for its EBR;
/// otherwise up to four are primaries, so no EBR is ever served over partition data.
pub(super) fn synthesize_partition_table(
    slices: &[GhoPartitionSlice],
) -> Result<std::collections::HashMap<u64, [u8; 512]>> {
    let mut out = std::collections::HashMap::new();
    let mut mbr = [0u8; 512];
    mbr[510] = 0x55;
    mbr[511] = 0xAA;
    let mut parts: Vec<(u64, u64, u8)> = slices
        .iter()
        .map(|s| {
            (
                s.hidden_sectors,
                s.partition_size / 512,
                mbr_type_for(&s.fs_type),
            )
        })
        .collect();
    parts.sort_by_key(|p| p.0);
    let mut prev_end = 1u64;
    for &(start, len, _) in &parts {
        if start < prev_end {
            bail!("GHO partitions overlap or start in the MBR (LBA {start})");
        }
        if start.saturating_add(len) > u32::MAX as u64 {
            bail!("GHO partition at LBA {start} is past what an MBR can address");
        }
        prev_end = start + len;
    }
    // Logical partitions as Ghost's source had them, when each has a free gap for its EBR.
    let logicals = &parts[1..];
    let mut prev_end = parts[0].0 + parts[0].1;
    let mut room = true;
    for &(start, len, _) in logicals {
        room &= start >= prev_end + 63;
        prev_end = start + len;
    }
    if !room || parts.len() == 1 {
        if parts.len() > 4 {
            bail!(
                "GHO has {} partitions and no room for the EBRs of logical ones",
                parts.len()
            );
        }
        for (i, &(start, len, t)) in parts.iter().enumerate() {
            write_mbr_entry(&mut mbr, i, start, len, t, i == 0);
        }
        out.insert(0, mbr);
        return Ok(out);
    }
    write_mbr_entry(&mut mbr, 0, parts[0].0, parts[0].1, parts[0].2, true);
    let ext_start = logicals[0].0 - 63;
    write_mbr_entry(
        &mut mbr,
        1,
        ext_start,
        prev_end - ext_start,
        MBR_TYPE_EXTENDED_LBA,
        false,
    );
    out.insert(0, mbr);
    for (i, &(start, len, t)) in logicals.iter().enumerate() {
        let mut ebr = [0u8; 512];
        ebr[510] = 0x55;
        ebr[511] = 0xAA;
        let ebr_lba = start - 63;
        write_mbr_entry(&mut ebr, 0, 63, len, t, false);
        if let Some(&(nstart, nlen, _)) = logicals.get(i + 1) {
            let next_ebr = nstart - 63;
            write_mbr_entry(
                &mut ebr,
                1,
                next_ebr - ext_start,
                nlen + 63,
                MBR_TYPE_EXTENDED_LBA,
                false,
            );
        }
        out.insert(ebr_lba, ebr);
    }
    Ok(out)
}

/// Write one 16-byte MBR partition entry at slot `idx` (0..3).
pub(super) fn write_mbr_entry(
    mbr: &mut [u8; 512],
    idx: usize,
    start_lba: u64,
    sectors: u64,
    ptype: u8,
    active: bool,
) {
    let off = 446 + idx * 16;
    mbr[off] = if active { 0x80 } else { 0x00 };
    // CHS fields: use LBA-only mode (FE FF FF).
    mbr[off + 1] = 0xFE;
    mbr[off + 2] = 0xFF;
    mbr[off + 3] = 0xFF;
    mbr[off + 4] = ptype;
    mbr[off + 5] = 0xFE;
    mbr[off + 6] = 0xFF;
    mbr[off + 7] = 0xFF;
    mbr[off + 8..off + 12].copy_from_slice(&(start_lba as u32).to_le_bytes());
    mbr[off + 12..off + 16].copy_from_slice(&(sectors as u32).to_le_bytes());
}

/// Decode every `0x0002` data-block record in `image` into `writer`,
/// applying the container's compression. Returns total bytes written.
pub fn decode_data_blocks_to<R: Read + Seek, W: Write>(
    reader: &mut R,
    image: &GhoImage,
    compression: GhoCompression,
    writer: &mut W,
) -> Result<u64> {
    let mut total: u64 = 0;
    let mut decoded = vec![0u8; FAST_LZ_BLOCK_SIZE + 1024];
    let mut body_buf = vec![0u8; u16::MAX as usize];

    for rec in image.data_blocks() {
        let body_len = rec.body_len as usize;
        if body_len == 0 {
            continue;
        }
        reader.seek(SeekFrom::Start(rec.body_start()))?;
        let body = &mut body_buf[..body_len];
        reader
            .read_exact(body)
            .with_context(|| format!("reading 0x0002 body at offset {:#x}", rec.offset))?;

        let n = match compression {
            GhoCompression::None => {
                // Uncompressed: body IS the raw 32 KiB block. Write verbatim.
                writer.write_all(body)?;
                total += body_len as u64;
                continue;
            }
            GhoCompression::Fast => fast_lz_decompress(body, body_len, &mut decoded)
                .with_context(|| format!("Fast-LZ decode at offset {:#x}", rec.offset))?,
            GhoCompression::High => zlib_decode_block(body, &mut decoded)
                .with_context(|| format!("zlib decode at offset {:#x}", rec.offset))?,
            GhoCompression::Other(b) => bail!("unsupported GHO compression byte {:#04x}", b),
        };
        writer
            .write_all(&decoded[..n])
            .with_context(|| format!("writing decoded block at output offset {}", total))?;
        total += n as u64;
    }
    Ok(total)
}

pub(super) fn zlib_decode_block(block: &[u8], dst: &mut [u8]) -> Result<usize> {
    use flate2::read::ZlibDecoder;
    // Unlike Fast-LZ (which has a 4-byte prefix), zlib/High data blocks
    // contain raw zlib directly — no prefix to skip.  Confirmed on both
    // Ghost 7.5 and 11.5 corpora (all 0x0002 bodies start with 0x78).
    if block.is_empty() {
        return Ok(0);
    }
    let mut dec = ZlibDecoder::new(block);
    let mut n = 0;
    loop {
        if n >= dst.len() {
            bail!("zlib block decoded > {} bytes", dst.len());
        }
        match dec.read(&mut dst[n..]) {
            Ok(0) => return Ok(n),
            Ok(k) => n += k,
            Err(e) => return Err(anyhow::Error::from(e)),
        }
    }
}
