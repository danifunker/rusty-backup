//! `GhoReader`: streaming `Read + Seek` over a sector-mode or file-aware image.

use super::*;

// ---------------------------------------------------------------------------
// GhoReader — streaming Read+Seek over a SECTOR-mode GHO (or span set)
// ---------------------------------------------------------------------------
//
// Eliminates the temp-file decode for browse / inspect / metadata reads.
// Modelled after ChdReader: open once (cheap index pass, no
// decompression), then random-access reads of the logical disk image
// pull-decompress one block at a time, cached.
//
// Only SECTOR mode is supported here. File-aware mode keeps the legacy
// decode-to-temp path (it has no LBA-addressable block layout). For
// uncompressed SECTOR backups the reader is a thin wrapper over
// SpanReader; for Fast / High compression the open scan walks the
// `[u16 stored_len][block_body]` chunks recording each block's file
// offset and stored_len, decompresses only the final block to learn its
// exact size, and assumes the standard 32 KiB decompressed length for
// every other block (Ghost's writer emits fixed-size blocks
// throughout). A debug assertion fires inside `decode_block_into` if a
// non-final block ever decompresses to a different size.

pub(super) const GHO_BLOCK_DECOMPRESSED_SIZE: usize = FAST_LZ_BLOCK_SIZE; // 32 KiB
/// The most one zlib block may inflate to before the scanner treats it as a false start.
pub(super) const GHO_MAX_INFLATE: u64 = (FAST_LZ_BLOCK_SIZE + 1024) as u64;

#[derive(Debug, Clone)]
pub(super) struct GhoBlockEntry {
    /// Byte offset of the `[u16 stored_len]` prefix in the SpanReader.
    pub(super) file_offset: u64,
    /// Total length on disk (includes the 2-byte stored_len prefix).
    pub(super) stored_len: u16,
    /// Where this block's first decompressed byte sits in the logical image.
    pub(super) output_offset: u64,
    /// Decompressed length. 32 KiB for all blocks except possibly the last.
    pub(super) decompressed_len: u32,
}

/// Streaming `Read + Seek` over a Norton Ghost SECTOR-mode image. Opens
/// in milliseconds (header parse + index walk, no decompression) and
/// decompresses one block on each read miss, caching the last block.
///
/// Multi-file span sets are handled transparently via [`SpanReader`];
/// callers pass the primary `.gho` path and span discovery happens
/// inside [`GhoReader::open`].
///
/// Handles both SECTOR-mode (streaming the raw block stream) and
/// file-aware (in-memory virtual FAT image whose data clusters are
/// resolved on demand by decompressing `0x0002`/`0x0102` records).
/// Password-protected GHOs still return an error from `open`.
pub struct GhoReader {
    pub(super) inner: SpanReader,
    pub(super) logical_size: u64,
    pub(super) position: u64,
    pub(super) mode: GhoReaderMode,
}

/// One partition inside a multi-partition file-aware GHO.
pub(super) struct FileAwarePartition {
    pub(super) disk_offset: u64,
    pub(super) image: VirtualFatImage,
    pub(super) cluster_cache: ClusterCache,
}

// Exactly one GhoReaderMode exists per GhoReader, moved by value only once at
// open time. The size spread between variants (the NtfsFileAware index dominates)
// is therefore a single ~1 KB move, not a per-read cost — boxing the large
// variant would instead add heap indirection on the read hot path, so we keep
// the variants inline.
#[allow(clippy::large_enum_variant)]
pub(super) enum GhoReaderMode {
    Uncompressed {
        data_start: u64,
    },
    Blocked {
        compression: GhoCompression,
        blocks: Vec<GhoBlockEntry>,
        cache_block_index: Option<usize>,
        cache_buf: Vec<u8>,
        /// Synthesized MBR + EBRs for multi-partition sector mode.
        /// Empty for single-partition images.
        synth_sectors: std::collections::HashMap<u64, [u8; 512]>,
    },
    FileAware {
        partitions: Vec<FileAwarePartition>,
        compression: GhoCompression,
        mbr: [u8; 512],
        ebrs: std::collections::HashMap<u64, [u8; 512]>,
    },
    NtfsFileAware {
        index: NtfsGhoIndex,
        last_run_hint: usize,
        compressed: Option<NtfsCompressedState>,
    },
}

impl GhoReader {
    /// Open a GHO (single file or span set) for streaming reads, without a
    /// password. Equivalent to [`GhoReader::open_with_password`] with `None`.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_password(path, None)
    }

    /// Open a GHO (single file or span set) for streaming reads, optionally
    /// supplying a password for encrypted images.
    ///
    /// For password-protected images the seed is derived from `password`,
    /// validated against the container verifier, and transparent body
    /// decryption is enabled on the underlying reader (see `gho_crypto`).
    /// Returns an error if the password is missing or incorrect.
    pub fn open_with_password(path: &Path, password: Option<&[u8]>) -> Result<Self> {
        let span_set = discover_gho_span_set(path)
            .with_context(|| format!("discovering span set for {}", path.display()))?;

        let mut primary = File::open(&span_set[0])
            .with_context(|| format!("opening {}", span_set[0].display()))?;
        let header = GhoContainerHeader::parse(&mut primary)
            .with_context(|| format!("parsing GHO header from {}", span_set[0].display()))?;
        drop(primary);

        if let Some(desc) = &header.description {
            log::info!("Ghost description: {}", desc);
        }
        let comp_label = match header.compression {
            GhoCompression::None => "none",
            GhoCompression::Fast => "Fast (LZ)",
            GhoCompression::High => "High (zlib)",
            GhoCompression::Other(_) => "unknown",
        };
        let type_label = match header.image_type {
            GhoImageType::FileAware => "file-aware",
            GhoImageType::Sector => "sector-by-sector",
            GhoImageType::Other(_) => "unknown",
        };
        log::info!(
            "Ghost image: {} compression, {} mode",
            comp_label,
            type_label
        );

        if header.container_version != 0x01 {
            bail!(
                "GHO {} container version {:#04x} not supported by GhoReader",
                path.display(),
                header.container_version
            );
        }
        // Derive + verify the password seed up front so a wrong/missing
        // password fails before we touch the data stream.
        let password_seed: Option<u16> = if header.password_protected {
            let pw = password.ok_or_else(|| {
                anyhow!(
                    "GHO {} is password-protected; a password is required to open it",
                    path.display()
                )
            })?;
            let seed = gho_seed_from_password(pw);
            match &header.password_verifier {
                Some(v) if gho_verify_seed(v, seed) => Some(seed),
                Some(_) => bail!(
                    "GHO {}: incorrect password (verifier mismatch)",
                    path.display()
                ),
                None => bail!(
                    "GHO {} is flagged password-protected but carries no verifier",
                    path.display()
                ),
            }
        } else {
            None
        };

        let mut inner = SpanReader::open(&span_set)
            .with_context(|| format!("opening span set for {}", path.display()))?;
        let file_size = inner.total_len();

        // Enable transparent decryption before any decode path reads the
        // stream. The encryption shape depends on the image mode:
        //   * file-aware       -> per record body (headers stay plaintext)
        //   * SECTOR + None     -> contiguous image in 32 KiB chunks
        //   * SECTOR + compress -> NOT encrypted (Ghost 11.5 stores the block
        //                          stream plaintext even with a password set)
        //
        // For SECTOR mode we must locate `data_start` on the *plaintext* sub-
        // headers before switching the reader into decrypting mode, so the
        // FEEF scan can't trip over decrypted disk bytes. The value is cached
        // and reused by the SECTOR dispatch below.
        let mut sector_data_start: Option<u64> = None;
        if let Some(seed) = password_seed {
            match header.image_type {
                GhoImageType::FileAware => {
                    let header_end = (GHO_HEADER_PREFIX_LEN + GHO_PASSWORD_VERIFIER_LEN) as u64;
                    let bodies = collect_gho_body_ranges(&mut inner, header_end, file_size)
                        .with_context(|| {
                            format!("mapping encrypted record bodies for {}", path.display())
                        })?;
                    let body_count = bodies.len();
                    inner.enable_decryption(seed, bodies);
                    log::info!(
                        "GHO {}: password accepted, decrypting {} record bodies",
                        path.display(),
                        body_count
                    );
                }
                GhoImageType::Sector => {
                    let ds = find_sector_data_start(&mut inner, file_size)?;
                    sector_data_start = Some(ds);
                    match header.compression {
                        GhoCompression::None => {
                            inner.enable_chunked_decryption(
                                seed,
                                ds,
                                FAST_LZ_BLOCK_SIZE as u64,
                                file_size,
                            );
                            log::info!(
                                "GHO {}: password accepted, decrypting SECTOR data in {}-byte \
                                 chunks from {:#x}",
                                path.display(),
                                FAST_LZ_BLOCK_SIZE,
                                ds
                            );
                        }
                        _ => {
                            log::info!(
                                "GHO {}: compressed SECTOR image — Ghost 11.5 stores the block \
                                 stream unencrypted; password verified, no decryption applied",
                                path.display()
                            );
                        }
                    }
                }
                GhoImageType::Other(_) => {}
            }
        }

        // ---- File-aware dispatch ----
        if matches!(header.image_type, GhoImageType::FileAware) {
            // Try to find the FAT record stream. NTFS file-aware backups
            // don't use it — they have GHPR metadata + packed cluster runs
            // instead. We detect this by checking whether the record stream
            // magic appears in the header region.
            let header_end = if header.password_protected {
                GHO_HEADER_PREFIX_LEN as u64 + GHO_PASSWORD_VERIFIER_LEN as u64
            } else {
                GHO_HEADER_PREFIX_LEN as u64
            };
            let has_record_stream = find_inner_stream_start(&mut inner, header_end).is_ok();

            if !has_record_stream {
                // No FAT record stream — try NTFS file-aware path.
                log::info!("No FAT record stream found, trying NTFS file-aware path...");
                let cache_key = NtfsScanKey::from_span_set(&span_set);
                match try_open_ntfs_file_aware(&mut inner, file_size, header.compression, cache_key)
                {
                    Ok((mode, volume_size)) => {
                        return Ok(Self {
                            inner,
                            logical_size: volume_size,
                            position: 0,
                            mode,
                        });
                    }
                    Err(e) => {
                        bail!(
                            "GHO {} is file-aware but has no FAT record stream and \
                             NTFS detection also failed: {:#}",
                            path.display(),
                            e
                        );
                    }
                }
            }

            log::info!("Scanning Ghost record stream...");
            let image = parse_gho_image(&mut inner, file_size, &header)
                .with_context(|| format!("parsing inner record stream for {}", path.display()))?;
            log::info!(
                "Found {} records ({} cached bodies)",
                image.records.len(),
                image.cached_bodies.len()
            );

            let mut slices = split_partitions(&mut inner, &image)
                .with_context(|| format!("splitting partitions for {}", path.display()))?;

            if slices.len() <= 1 {
                // Single-partition: use the original path (whole tree).
                let tree = walk_file_aware_tree(&mut inner, &image)
                    .with_context(|| format!("walking file-aware tree for {}", path.display()))?;
                log::info!(
                    "Reconstructing FAT partition from {} files, {} directories...",
                    tree.file_count(),
                    tree.dir_count()
                );
                let virtual_image =
                    build_virtual_fat_image(&mut inner, &image, &tree, header.compression)
                        .with_context(|| {
                            format!("building virtual FAT image for {}", path.display())
                        })?;
                let logical_size = virtual_image.total_size;
                return Ok(Self {
                    inner,
                    logical_size,
                    position: 0,
                    mode: GhoReaderMode::FileAware {
                        partitions: vec![FileAwarePartition {
                            disk_offset: 0,
                            image: virtual_image,
                            cluster_cache: ClusterCache::default(),
                        }],
                        compression: header.compression,
                        mbr: [0u8; 512],
                        ebrs: std::collections::HashMap::new(),
                    },
                });
            }

            // Multi-partition: resolve absolute offsets, then build
            // one VirtualFatImage per partition.
            resolve_absolute_offsets(&mut slices);
            let mut ebrs = synthesize_partition_table(&slices)?;
            let mbr = ebrs.remove(&0).expect("the table has an MBR");

            log::info!(
                "Reconstructing {} partitions from sparsely populated Ghost image...",
                slices.len()
            );
            let mut fa_parts = Vec::with_capacity(slices.len());
            let mut max_end: u64 = 0;
            for (pi, slice) in slices.iter().enumerate() {
                let sub_records = image.records[slice.record_range.clone()].to_vec();
                let sub_bodies: std::collections::HashMap<u64, Vec<u8>> = sub_records
                    .iter()
                    .filter_map(|r| {
                        image
                            .cached_bodies
                            .get(&r.offset)
                            .map(|b| (r.offset, b.clone()))
                    })
                    .collect();
                let sub_image = GhoImage {
                    records: sub_records,
                    cached_bodies: sub_bodies,
                    partition_count: 1,
                };
                let tree = walk_file_aware_tree(&mut inner, &sub_image).with_context(|| {
                    format!(
                        "walking file-aware tree for partition at LBA {}",
                        slice.hidden_sectors
                    )
                })?;
                log::info!(
                    "  Partition {} of {}: {} ({} files, {} dirs)",
                    pi + 1,
                    slices.len(),
                    slice.fs_type,
                    tree.file_count(),
                    tree.dir_count()
                );
                let virtual_image =
                    build_virtual_fat_image(&mut inner, &sub_image, &tree, header.compression)
                        .with_context(|| {
                            format!(
                                "building virtual FAT image for partition at LBA {}",
                                slice.hidden_sectors
                            )
                        })?;
                let disk_offset = slice.hidden_sectors * 512;
                let part_end = disk_offset + virtual_image.total_size;
                if part_end > max_end {
                    max_end = part_end;
                }
                fa_parts.push(FileAwarePartition {
                    disk_offset,
                    image: virtual_image,
                    cluster_cache: ClusterCache::default(),
                });
            }

            log::info!(
                "Multi-partition file-aware GHO: {} partitions, logical disk size {}",
                fa_parts.len(),
                max_end
            );

            return Ok(Self {
                inner,
                logical_size: max_end,
                position: 0,
                mode: GhoReaderMode::FileAware {
                    partitions: fa_parts,
                    compression: header.compression,
                    mbr,
                    ebrs,
                },
            });
        }

        if !matches!(header.image_type, GhoImageType::Sector) {
            bail!(
                "GHO {} image_type {:?} not yet handled by GhoReader",
                path.display(),
                header.image_type
            );
        }

        // Reuse the `data_start` computed (on plaintext) when decryption was
        // enabled; otherwise scan for it now. Re-scanning after chunked
        // decryption is enabled would read decrypted disk bytes and could
        // mistake a stray `FE EF` for a sub-header.
        let data_start = match sector_data_start {
            Some(ds) => ds,
            None => find_sector_data_start(&mut inner, file_size)?,
        };

        match header.compression {
            GhoCompression::None => {
                // TODO: multi-partition uncompressed sector mode
                // (split at 0x0703 records) is not yet supported.
                let logical_size = file_size.saturating_sub(data_start);
                Ok(Self {
                    inner,
                    logical_size,
                    position: 0,
                    mode: GhoReaderMode::Uncompressed { data_start },
                })
            }
            GhoCompression::Fast | GhoCompression::High => {
                let index = index_sector_blocks(&mut inner, data_start, file_size)?;
                let mut blocks = index.blocks;
                let partition_boundaries = index.partition_boundaries;

                if partition_boundaries.is_empty() {
                    // Single partition — fix last block and return.
                    if let Some(last) = blocks.last_mut() {
                        let mut tmp = vec![0u8; GHO_BLOCK_DECOMPRESSED_SIZE + 1024];
                        let n = decode_block_into(&mut inner, last, header.compression, &mut tmp)
                            .with_context(|| {
                            format!("sizing final block of {}", path.display())
                        })?;
                        last.decompressed_len = n as u32;
                    }
                    let logical_size = blocks
                        .last()
                        .map(|b| b.output_offset + b.decompressed_len as u64)
                        .unwrap_or(0);
                    return Ok(Self {
                        inner,
                        logical_size,
                        position: 0,
                        mode: GhoReaderMode::Blocked {
                            compression: header.compression,
                            blocks,
                            cache_block_index: None,
                            cache_buf: Vec::with_capacity(GHO_BLOCK_DECOMPRESSED_SIZE + 1024),
                            synth_sectors: std::collections::HashMap::new(),
                        },
                    });
                }

                // Multi-partition sector mode. Each partition's blocks
                // are currently output_offset'd as if they're
                // contiguous. We need to:
                // 1. Fix the last block of each partition's range.
                // 2. Read VBR from each partition's first block to
                //    get hidden_sectors.
                // 3. Re-assign output_offsets to place each partition
                //    at hidden_sectors * 512.
                // 4. Synthesize MBR.

                // Partition ranges: [0..boundaries[0]),
                // [boundaries[0]..boundaries[1]), ...
                let mut ranges: Vec<std::ops::Range<usize>> = Vec::new();
                let mut prev = 0;
                for &b in &partition_boundaries {
                    ranges.push(prev..b);
                    prev = b;
                }
                ranges.push(prev..blocks.len());

                // Fix last block of each partition range. Trim trailing
                // blocks that fail to decompress (they're padding or
                // trailer data that the indexer picked up).
                let mut tmp = vec![0u8; GHO_BLOCK_DECOMPRESSED_SIZE + 1024];
                let mut adjusted_ranges = Vec::with_capacity(ranges.len());
                for range in &ranges {
                    if range.is_empty() {
                        adjusted_ranges.push(range.clone());
                        continue;
                    }
                    let mut end = range.end;
                    while end > range.start {
                        match decode_block_into(
                            &mut inner,
                            &blocks[end - 1],
                            header.compression,
                            &mut tmp,
                        ) {
                            Ok(n) => {
                                blocks[end - 1].decompressed_len = n as u32;
                                break;
                            }
                            // A read failing is the image, not trailer padding (which may run
                            // off the end of the file).
                            Err(e)
                                if e.chain().any(|c| {
                                    c.downcast_ref::<std::io::Error>().is_some_and(|io| {
                                        io.kind() != std::io::ErrorKind::UnexpectedEof
                                    })
                                }) =>
                            {
                                return Err(e);
                            }
                            Err(_) => {
                                end -= 1;
                            }
                        }
                    }
                    adjusted_ranges.push(range.start..end);
                }
                let ranges = adjusted_ranges;

                // Read each partition's VBR from its first block.
                let mut part_infos: Vec<(u64, u64, String)> = Vec::new();
                for range in &ranges {
                    if range.is_empty() {
                        part_infos.push((0, 0, "Unknown".to_string()));
                        continue;
                    }
                    let first = &blocks[range.start];
                    let n = decode_block_into(&mut inner, first, header.compression, &mut tmp)?;
                    if n >= 64 {
                        part_infos.push(vbr_geometry(&tmp[..n]));
                    } else {
                        part_infos.push((0, 0, "Unknown".to_string()));
                    }
                }

                // Resolve absolute disk offsets via the shared helper.
                let mut tmp_slices: Vec<GhoPartitionSlice> = part_infos
                    .iter()
                    .map(|(hidden, size, fs)| GhoPartitionSlice {
                        boot_record_index: 0,
                        record_range: 0..0,
                        partition_size: *size,
                        hidden_sectors: *hidden,
                        fs_type: fs.clone(),
                        reserved_record_index: None,
                    })
                    .collect();
                resolve_absolute_offsets(&mut tmp_slices);
                let abs_offsets: Vec<u64> = tmp_slices.iter().map(|s| s.hidden_sectors).collect();

                // Re-assign output_offsets: each partition at its
                // absolute disk offset.
                let mut max_end: u64 = 0;
                for (ri, range) in ranges.iter().enumerate() {
                    let disk_offset = abs_offsets.get(ri).copied().unwrap_or(0) * 512;
                    let mut part_output: u64 = 0;
                    for idx in range.clone() {
                        blocks[idx].output_offset = disk_offset + part_output;
                        part_output += blocks[idx].decompressed_len as u64;
                    }
                    let end = disk_offset + part_output;
                    if end > max_end {
                        max_end = end;
                    }
                }

                // Update part_infos with resolved absolute offsets.
                for (i, abs) in abs_offsets.iter().enumerate() {
                    part_infos[i].0 = *abs;
                }

                // Only blocks inside a partition range got their offsets placed; the trimmed
                // ones keep stale ones and would break the binary search (GHO-003).
                let mut kept: Vec<GhoBlockEntry> = ranges
                    .iter()
                    .flat_map(|r| r.clone())
                    .map(|i| blocks[i].clone())
                    .collect();
                kept.sort_by_key(|b| b.output_offset);
                let blocks = kept;

                // Synthesize MBR + EBRs. Filter out empty partitions.
                let slices: Vec<GhoPartitionSlice> = part_infos
                    .iter()
                    .filter(|(_, size, _)| *size > 0)
                    .map(|(hidden, size, fs)| GhoPartitionSlice {
                        boot_record_index: 0,
                        record_range: 0..0,
                        partition_size: *size,
                        hidden_sectors: *hidden,
                        fs_type: fs.clone(),
                        reserved_record_index: None,
                    })
                    .collect();
                let synth_sectors = synthesize_partition_table(&slices)?;

                let logical_size = max_end.max(512);

                log::info!(
                    "Multi-partition SECTOR-mode GHO: {} partitions, {} total blocks, logical size {}",
                    ranges.len(),
                    blocks.len(),
                    logical_size
                );

                Ok(Self {
                    inner,
                    logical_size,
                    position: 0,
                    mode: GhoReaderMode::Blocked {
                        compression: header.compression,
                        blocks,
                        cache_block_index: None,
                        cache_buf: Vec::with_capacity(GHO_BLOCK_DECOMPRESSED_SIZE + 1024),
                        synth_sectors,
                    },
                })
            }
            GhoCompression::Other(b) => bail!(
                "GHO {} has unknown compression byte {:#04x}",
                path.display(),
                b
            ),
        }
    }

    /// Logical (decompressed) byte length of the disk image.
    pub fn logical_size(&self) -> u64 {
        self.logical_size
    }

    /// Number of indexed compressed blocks. For uncompressed images this
    /// is 0 (the reader streams raw bytes). Exposed for diagnostics and
    /// tests.
    pub fn block_count(&self) -> usize {
        match &self.mode {
            GhoReaderMode::Uncompressed { .. } => 0,
            GhoReaderMode::Blocked { blocks, .. } => blocks.len(),
            GhoReaderMode::FileAware { partitions, .. } => partitions
                .iter()
                .map(|p| p.image.cluster_to_file.len())
                .sum(),
            GhoReaderMode::NtfsFileAware { index, .. } => index.runs.len(),
        }
    }

    pub(super) fn ensure_block_cached(&mut self, idx: usize) -> std::io::Result<()> {
        let GhoReaderMode::Blocked {
            compression,
            blocks,
            cache_block_index,
            cache_buf,
            ..
        } = &mut self.mode
        else {
            return Ok(());
        };
        if *cache_block_index == Some(idx) {
            return Ok(());
        }
        let block = &blocks[idx];
        cache_buf.clear();
        cache_buf.resize(GHO_BLOCK_DECOMPRESSED_SIZE + 1024, 0);
        let n = decode_block_into(&mut self.inner, block, *compression, cache_buf)
            .map_err(|e| disk_core::io_other(format!("GHO block {}: {:#}", idx, e)))?;
        // A block short of its indexed length would end the image early without a word.
        if n as u32 != block.decompressed_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "GHO block {idx} decoded to {n} bytes, the index says {}",
                    block.decompressed_len
                ),
            ));
        }
        cache_buf.truncate(n);
        *cache_block_index = Some(idx);
        Ok(())
    }
}

impl Read for GhoReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() || self.position >= self.logical_size {
            return Ok(0);
        }
        match &self.mode {
            GhoReaderMode::Uncompressed { data_start } => {
                let phys = data_start + self.position;
                let remaining = self.logical_size - self.position;
                let to_read = (buf.len() as u64).min(remaining) as usize;
                self.inner.seek(SeekFrom::Start(phys))?;
                let n = self.inner.read(&mut buf[..to_read])?;
                self.position += n as u64;
                Ok(n)
            }
            GhoReaderMode::Blocked {
                blocks,
                synth_sectors,
                ..
            } => {
                // Check synthesized sectors (MBR, EBRs) first.
                let sector_lba = self.position / 512;
                if let Some(sector_data) = synth_sectors.get(&sector_lba) {
                    let off = (self.position % 512) as usize;
                    let avail = 512 - off;
                    let remaining = (self.logical_size - self.position) as usize;
                    let n = buf.len().min(avail).min(remaining);
                    buf[..n].copy_from_slice(&sector_data[off..off + n]);
                    self.position += n as u64;
                    return Ok(n);
                }

                // Binary search for the block containing `self.position`.
                let idx = match blocks.binary_search_by(|b| b.output_offset.cmp(&self.position)) {
                    Ok(i) => i,
                    Err(0) => {
                        // Position is before the first block (gap area).
                        let first_off = blocks.first().map(|b| b.output_offset).unwrap_or(0);
                        if self.position < first_off {
                            let gap = (first_off - self.position) as usize;
                            let n = buf.len().min(gap).min(512);
                            for b in &mut buf[..n] {
                                *b = 0;
                            }
                            self.position += n as u64;
                            return Ok(n);
                        }
                        0
                    }
                    Err(i) => i - 1,
                };

                // Check if position is actually in a gap between
                // partitions (multi-partition: blocks may not be
                // contiguous in output space).
                let block = &blocks[idx];
                let block_end = block.output_offset + block.decompressed_len as u64;
                if self.position >= block_end {
                    // In a gap. Return zeros up to the next block.
                    let next_off = if idx + 1 < blocks.len() {
                        blocks[idx + 1].output_offset
                    } else {
                        self.logical_size
                    };
                    let gap = (next_off - self.position) as usize;
                    let n = buf.len().min(gap).min(512);
                    for b in &mut buf[..n] {
                        *b = 0;
                    }
                    self.position += n as u64;
                    return Ok(n);
                }

                self.ensure_block_cached(idx)?;
                let GhoReaderMode::Blocked {
                    blocks, cache_buf, ..
                } = &self.mode
                else {
                    unreachable!()
                };
                let block = &blocks[idx];
                let off_in_block = (self.position - block.output_offset) as usize;
                let avail = cache_buf.len().saturating_sub(off_in_block);
                let n = avail.min(buf.len());
                buf[..n].copy_from_slice(&cache_buf[off_in_block..off_in_block + n]);
                self.position += n as u64;
                Ok(n)
            }
            GhoReaderMode::FileAware { .. } => {
                let remaining = self.logical_size - self.position;
                let want = (buf.len() as u64).min(remaining) as usize;
                let position = self.position;
                let n = self.read_file_aware_into(position, &mut buf[..want])?;
                self.position += n as u64;
                Ok(n)
            }
            GhoReaderMode::NtfsFileAware { .. } => {
                let remaining = self.logical_size - self.position;
                let want = (buf.len() as u64).min(remaining) as usize;
                let position = self.position;
                let GhoReaderMode::NtfsFileAware {
                    index,
                    last_run_hint,
                    compressed,
                } = &mut self.mode
                else {
                    unreachable!()
                };
                let n = read_ntfs_file_aware_into(
                    &mut self.inner,
                    index,
                    last_run_hint,
                    compressed,
                    position,
                    &mut buf[..want],
                )?;
                self.position += n as u64;
                Ok(n)
            }
        }
    }
}

impl GhoReader {
    /// Prepare for a faithful whole-disk export. For NTFS file-aware GHOs this
    /// forces the lazy run scan to completion, then builds the synthesized
    /// `$Bitmap` and recovers `$Boot`'s boot code so the exported image is both
    /// mountable and bootable. No-op for other GHO shapes.
    ///
    /// Export paths call this (via `open_source_reader`); inspect/browse don't,
    /// so they keep fast lazy opens (and serve VBR + zeros for `$Boot`, which is
    /// fine since they never need the boot code).
    pub fn prepare_full_image(&mut self) -> std::io::Result<()> {
        let GhoReaderMode::NtfsFileAware {
            index, compressed, ..
        } = &mut self.mode
        else {
            return Ok(());
        };

        // Complete the run index so $Bitmap and $Boot recovery see every run.
        if let Some(cs) = compressed.as_mut() {
            if cs.lazy_scan.is_some() {
                extend_ntfs_compressed_index(&mut self.inner, index, cs, u64::MAX)?;
            }
        }

        // Post-scan content-aware fix-up: rewire runs the streaming matcher
        // misassigned among same-cc candidates by inspecting first-4-byte magic
        // (INDX / FILE / RSTR|RCRD). Resolves the cross-links that otherwise
        // cause chkdsk /f to orphan files. See fixup_ntfs_typed_misalignments.
        // Disable with RUSTYBACKUP_GHO_NTFS_NO_FIXUP=1 for diagnostic comparison.
        if std::env::var("RUSTYBACKUP_GHO_NTFS_NO_FIXUP")
            .map(|v| v == "1")
            .unwrap_or(false)
        {
            log::warn!("NTFS fixup disabled via RUSTYBACKUP_GHO_NTFS_NO_FIXUP");
        } else if let Ok(vbr_parsed) = fs_pc::ntfs::parse_vbr(&index.vbr) {
            fixup_ntfs_typed_misalignments(&mut self.inner, index, compressed, &vbr_parsed);
        }

        // Build $Bitmap (Ghost omits it).
        if index.synth_bitmap.is_empty() && !index.bitmap_runs.is_empty() {
            let total_clusters = index.volume_size / index.cluster_size;
            let mut allocation: Vec<(u64, u64)> = index
                .runs
                .iter()
                .map(|r| (r.lcn_start, r.cluster_count))
                .collect();
            allocation.extend_from_slice(&index.bitmap_runs);
            index.synth_bitmap = build_synth_ntfs_bitmap(&allocation, total_clusters);
            log::info!("Prepared $Bitmap: {} bytes", index.synth_bitmap.len());
        }

        // Recover $Boot's boot code (mislabeled by Ghost; matched via the VBR).
        if index.boot_code.is_empty() {
            match recover_ntfs_boot_code(&mut self.inner, index, compressed) {
                Some(bc) => {
                    log::info!("Recovered $Boot boot code: {} bytes", bc.len());
                    index.boot_code = bc;
                }
                None => log::warn!(
                    "Could not locate $Boot in the GHO stream; exported disk will \
                     mount but not boot"
                ),
            }
        }

        // Refresh the cache with the now-complete $Bitmap + $Boot so a later
        // export (or browse) of this archive reuses them too.
        if let Some(cs) = compressed.as_ref() {
            if let Some(key) = cs.cache_key.clone() {
                ntfs_scan_cache_store(&key, index, cs);
            }
        }
        Ok(())
    }

    /// Mapped NTFS cluster runs as `(lcn_start, cluster_count)`, sorted by LCN.
    /// Only meaningful after `prepare_full_image` (so the run set is complete);
    /// `None` for non-NTFS-file-aware GHOs. Used by diagnostics to report which
    /// files have declared clusters not covered by any mapped run.
    pub fn ntfs_mapped_ranges(&self) -> Option<Vec<(u64, u64)>> {
        let GhoReaderMode::NtfsFileAware { index, .. } = &self.mode else {
            return None;
        };
        let mut ranges: Vec<(u64, u64)> = index
            .runs
            .iter()
            .map(|r| (r.lcn_start, r.cluster_count))
            .collect();
        ranges.sort_by_key(|r| r.0);
        Some(ranges)
    }

    /// Diagnostic / recovery: every "sentinel" run (lcn_start == u64::MAX —
    /// recorded by the lazy scan but never assigned a real LCN), returning
    /// `(cluster_count, seq, full_decoded_bytes)`. The bytes ARE the dropped
    /// data — captured in the stream, just unplaced — so callers can splice
    /// them back into a file with a hole at the right offset. `max_bytes` caps
    /// the per-run read (pass a cluster-size multiple).
    pub fn debug_sentinel_runs_full(&mut self, max_bytes: usize) -> Vec<(u64, u64, Vec<u8>)> {
        let GhoReaderMode::NtfsFileAware {
            index, compressed, ..
        } = &mut self.mode
        else {
            return Vec::new();
        };
        let sentinels: Vec<(u64, u64, u64)> = index
            .runs
            .iter()
            .filter(|r| r.lcn_start == u64::MAX)
            .map(|r| (r.cluster_count, r.seq, r.file_offset))
            .collect();
        let mut out = Vec::with_capacity(sentinels.len());
        for (cc, seq, fo) in sentinels {
            let want = ((cc * index.cluster_size) as usize).min(max_bytes);
            let mut buf = vec![0u8; want];
            let mut got = 0usize;
            if let Some(cs) = compressed.as_mut() {
                let mut rdr = NtfsDecompressingReader {
                    inner: &mut self.inner,
                    state: cs,
                    position: fo,
                };
                while got < want {
                    match rdr.read(&mut buf[got..]) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => got += n,
                    }
                }
            } else if self.inner.seek(SeekFrom::Start(fo)).is_ok() {
                while got < want {
                    match self.inner.read(&mut buf[got..]) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => got += n,
                    }
                }
            }
            buf.truncate(got);
            out.push((cc, seq, buf));
        }
        out
    }

    /// Serve bytes for FileAware mode at `position` into `out`.
    pub(super) fn read_file_aware_into(
        &mut self,
        position: u64,
        out: &mut [u8],
    ) -> std::io::Result<usize> {
        let GhoReaderMode::FileAware {
            partitions,
            compression,
            mbr,
            ebrs,
        } = &mut self.mode
        else {
            unreachable!()
        };

        let sector_lba = position / 512;
        let off_in_sector = (position % 512) as usize;
        let avail_in_sector = 512 - off_in_sector;
        let to_read = out.len().min(avail_in_sector);
        if to_read == 0 {
            return Ok(0);
        }

        // 1) MBR at sector 0 (only for multi-partition images that
        //    have a real synthesized MBR, not the zero placeholder).
        if sector_lba == 0 && mbr[510] == 0x55 && mbr[511] == 0xAA {
            out[..to_read].copy_from_slice(&mbr[off_in_sector..off_in_sector + to_read]);
            return Ok(to_read);
        }

        // 2) EBR sectors.
        if let Some(ebr) = ebrs.get(&sector_lba) {
            out[..to_read].copy_from_slice(&ebr[off_in_sector..off_in_sector + to_read]);
            return Ok(to_read);
        }

        // 3) Partition data: find the partition containing this position.
        for part in partitions.iter_mut() {
            let part_end = part.disk_offset + part.image.total_size;
            if position >= part.disk_offset && position < part_end {
                let pos_in_part = position - part.disk_offset;
                return read_partition_data_at(
                    &mut self.inner,
                    &part.image,
                    *compression,
                    &mut part.cluster_cache,
                    pos_in_part,
                    out,
                    to_read,
                );
            }
        }

        // 4) Default: zero (inter-partition gap).
        for b in &mut out[..to_read] {
            *b = 0;
        }
        Ok(to_read)
    }
}

/// Serve bytes from a single `VirtualFatImage` at `pos_in_part` (byte
/// offset within the partition).
pub(super) fn read_partition_data_at(
    inner: &mut SpanReader,
    image: &VirtualFatImage,
    compression: GhoCompression,
    cluster_cache: &mut ClusterCache,
    pos_in_part: u64,
    out: &mut [u8],
    to_read: usize,
) -> std::io::Result<usize> {
    let bps = image.bytes_per_sector as u64;
    let sector_lba = pos_in_part / bps;
    let off_in_sector = (pos_in_part % bps) as usize;
    let avail_in_sector = (bps as usize) - off_in_sector;
    let to_read = to_read.min(avail_in_sector);

    // Sparse metadata hit (VBR, FAT tables, root dir, etc.)?
    if let Some(stored) = image.sparse.get(&sector_lba) {
        out[..to_read].copy_from_slice(&stored[off_in_sector..off_in_sector + to_read]);
        return Ok(to_read);
    }

    // File-owned cluster?
    let data_start_sector = image.data_start_sector as u64;
    if sector_lba >= data_start_sector {
        let spc = image.sectors_per_cluster as u64;
        let cluster_offset = sector_lba - data_start_sector;
        let cluster_id = (cluster_offset / spc + 2) as u32;
        if let Some(&(file_id, cluster_index_in_file)) = image.cluster_to_file.get(&cluster_id) {
            let cluster_bytes = ensure_cluster_decoded(
                inner,
                image,
                compression,
                file_id,
                cluster_index_in_file,
                cluster_id,
                cluster_cache,
            )?;
            let sector_in_cluster = (cluster_offset % spc) as usize;
            let byte_in_cluster = sector_in_cluster * bps as usize + off_in_sector;
            let end = (byte_in_cluster + to_read).min(cluster_bytes.len());
            if end > byte_in_cluster {
                let span = end - byte_in_cluster;
                out[..span].copy_from_slice(&cluster_bytes[byte_in_cluster..end]);
                for b in &mut out[span..to_read] {
                    *b = 0;
                }
            } else {
                for b in &mut out[..to_read] {
                    *b = 0;
                }
            }
            return Ok(to_read);
        }
    }

    for b in &mut out[..to_read] {
        *b = 0;
    }
    Ok(to_read)
}

/// Decode the requested cluster's bytes for a file-aware FAT cluster.
/// Caches the most recently decoded cluster to avoid re-decoding when
/// consecutive sectors target the same cluster.
pub(super) fn ensure_cluster_decoded(
    inner: &mut SpanReader,
    image: &VirtualFatImage,
    compression: GhoCompression,
    file_id: u32,
    cluster_index_in_file: u32,
    cluster_id: u32,
    cache: &mut ClusterCache,
) -> std::io::Result<Vec<u8>> {
    if let Some((cached_id, cached_bytes)) = &cache.last {
        if *cached_id == cluster_id {
            return Ok(cached_bytes.clone());
        }
    }

    let file = &image.files[file_id as usize];
    let cluster_size = image.cluster_size as usize;
    let byte_in_file = cluster_index_in_file as u64 * image.cluster_size;

    // Start at the last record known to begin at or before this cluster, not at the file's
    // start: decoding from byte 0 on every miss made a large file quadratic (GHO-005).
    let starts = cache
        .record_starts
        .entry(file_id)
        .or_insert_with(|| vec![0]);
    let k = match starts.binary_search(&byte_in_file) {
        Ok(i) => i,
        Err(i) => i - 1,
    };
    let first = starts[k];
    let mut content =
        GhoFileContentReader::new_at(inner, &file.records, compression, file.file_size, k, first);

    // Skip to the start of this cluster's bytes within the file.
    let mut skip = byte_in_file - first;
    let mut sink = [0u8; 8192];
    while skip > 0 {
        let want = (skip as usize).min(sink.len());
        let n = content.read(&mut sink[..want])?;
        if n == 0 {
            break;
        }
        skip -= n as u64;
    }

    // Read up to cluster_size bytes from this cluster.
    let mut out = vec![0u8; cluster_size];
    let mut filled = 0usize;
    while filled < cluster_size {
        let n = content.read(&mut out[filled..])?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    out.truncate(filled.max(cluster_size).min(cluster_size));
    out.resize(cluster_size, 0);

    // Remember where each record we just decoded starts, for the next miss.
    let starts = cache
        .record_starts
        .get_mut(&file_id)
        .expect("inserted above");
    let mut at = first;
    for (j, size) in content.record_sizes.iter().enumerate() {
        at += *size as u64;
        if k + j + 1 == starts.len() && k + j + 1 < file.records.len() {
            starts.push(at);
        }
    }
    cache.last = Some((cluster_id, out.clone()));
    Ok(out)
}

/// What file-aware reads remember between calls: the last cluster served, and per file the
/// decoded byte offset of each record start found so far.
#[derive(Default)]
pub(crate) struct ClusterCache {
    pub(super) last: Option<(u32, Vec<u8>)>,
    pub(super) record_starts: std::collections::HashMap<u32, Vec<u64>>,
}

impl Seek for GhoReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos: i128 = match pos {
            SeekFrom::Start(o) => o as i128,
            SeekFrom::Current(o) => self.position as i128 + o as i128,
            SeekFrom::End(o) => self.logical_size as i128 + o as i128,
        };
        if new_pos < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "GhoReader seek before start",
            ));
        }
        self.position = (new_pos as u64).min(self.logical_size);
        Ok(self.position)
    }
}

impl DataLen for GhoReader {
    fn total_len(&self) -> u64 {
        self.logical_size
    }
}

/// Walk the compressed block stream once without decompressing any
/// block except the last. Records each block's file offset, stored_len,
/// and computed logical output offset; decompresses only the final
/// block to learn its exact length so `logical_size` is correct down
/// to the byte.
///
/// Reads in 256 KiB chunks to avoid per-block seek+read syscalls —
/// highly-compressible data can produce millions of tiny blocks.
/// Result of indexing the sector-mode block stream, including
/// partition boundaries from `0x0703` continuation records.
pub(super) struct SectorBlockIndex {
    pub(super) blocks: Vec<GhoBlockEntry>,
    /// Block indices where new partitions start. The first partition
    /// implicitly starts at block 0; entries here mark subsequent ones.
    pub(super) partition_boundaries: Vec<usize>,
}

pub(super) fn index_sector_blocks<R: Read + Seek>(
    reader: &mut R,
    data_start: u64,
    data_end: u64,
) -> Result<SectorBlockIndex> {
    let mut blocks = Vec::new();
    let mut partition_boundaries = Vec::new();
    let mut off = data_start;
    let mut output_offset: u64 = 0;
    let record_magic = GHO_RECORD_MAGIC.to_le_bytes();

    const CHUNK: usize = 256 * 1024;
    let mut buf = vec![0u8; CHUNK];
    reader.seek(SeekFrom::Start(data_start))?;
    let mut buf_file_off: u64 = data_start;
    let mut buf_valid: usize = read_fully_or_eof(reader, &mut buf)?;

    while off + 2 <= data_end {
        let pos = (off - buf_file_off) as usize;
        if pos + GHO_RECORD_HEADER_LEN > buf_valid {
            reader.seek(SeekFrom::Start(off))?;
            buf_file_off = off;
            buf_valid = read_fully_or_eof(reader, &mut buf)?;
            if buf_valid < GHO_RECORD_HEADER_LEN {
                break;
            }
            continue;
        }

        let peek = &buf[pos..pos + GHO_RECORD_HEADER_LEN];
        if peek[4..8] == record_magic {
            // This is a record header. Check type to decide action.
            let type_code = u16::from_le_bytes([peek[0], peek[1]]);
            let body_len = u16::from_le_bytes([peek[8], peek[9]]);
            if type_code == GHO_REC_CONTINUATION {
                // Partition boundary — skip this record, mark boundary,
                // and continue indexing.
                partition_boundaries.push(blocks.len());
                off += GHO_RECORD_HEADER_LEN as u64 + body_len as u64;

                // After a 0x0703 continuation, Ghost inserts a 512-byte
                // FEEF sub-header before the next partition's block
                // stream. Skip it if present.
                if off + 2 <= data_end {
                    reader.seek(SeekFrom::Start(off))?;
                    let mut feef_check = [0u8; 2];
                    if reader.read_exact(&mut feef_check).is_ok() && feef_check == GHO_MAGIC {
                        off += GHO_SECTOR_SIZE;
                    }
                }

                // Re-fill buffer from new offset.
                reader.seek(SeekFrom::Start(off))?;
                buf_file_off = off;
                buf_valid = read_fully_or_eof(reader, &mut buf)?;
                continue;
            }
            // Any other record type (0x0023 end, etc.) = stop.
            break;
        }
        let stored_len = u16::from_le_bytes([peek[0], peek[1]]) as u64;
        if stored_len < 4 {
            break;
        }
        if off + stored_len > data_end {
            break;
        }
        blocks.push(GhoBlockEntry {
            file_offset: off,
            stored_len: stored_len as u16,
            output_offset,
            decompressed_len: GHO_BLOCK_DECOMPRESSED_SIZE as u32,
        });
        output_offset += GHO_BLOCK_DECOMPRESSED_SIZE as u64;
        off += stored_len;
    }
    // A `0x0703` that closes the stream (no blocks follow it) is a terminator,
    // not a partition boundary. Ghost emits one at the end of a single-volume
    // sector image; treating it as a boundary would synthesize a bogus empty
    // second partition (and mis-detect a superfloppy as MBR). Drop any boundary
    // at or past the final block. Real multi-partition boundaries sit *between*
    // blocks (boundary < blocks.len()) and are preserved.
    partition_boundaries.retain(|&b| b < blocks.len());
    Ok(SectorBlockIndex {
        blocks,
        partition_boundaries,
    })
}

/// Decompress one indexed block into `dst`. `dst` must be at least
/// `GHO_BLOCK_DECOMPRESSED_SIZE + 1024` bytes; the actual decompressed
/// length is returned.
pub(super) fn decode_block_into<R: Read + Seek>(
    reader: &mut R,
    block: &GhoBlockEntry,
    compression: GhoCompression,
    dst: &mut [u8],
) -> Result<usize> {
    let comp_len = (block.stored_len as usize).saturating_sub(2);
    let mut buf = vec![0u8; comp_len];
    reader.seek(SeekFrom::Start(block.file_offset + 2))?;
    reader
        .read_exact(&mut buf)
        .with_context(|| format!("reading block body at {:#x}", block.file_offset + 2))?;
    match compression {
        GhoCompression::Fast => fast_lz_decompress(&buf, comp_len, dst)
            .with_context(|| format!("Fast-LZ decode at {:#x}", block.file_offset)),
        GhoCompression::High => zlib_decode_block(&buf, dst)
            .with_context(|| format!("zlib decode at {:#x}", block.file_offset)),
        GhoCompression::None | GhoCompression::Other(_) => {
            bail!("decode_block_into called with non-block compression")
        }
    }
}

/// Result of materializing a GHO container to a temp file. Same shape as
/// rusty-backup's `rbformats::imz::ImzMaterialized` so the GUI's
/// `prepare_disk_image_path` can treat both formats identically.
#[derive(Debug)]
pub struct GhoMaterialized {
    /// Path to the raw decoded partition image inside the tempdir.
    pub temp_path: std::path::PathBuf,
    /// Bytes written to `temp_path` (== sum of decompressed block bytes).
    pub logical_size: u64,
    /// Tempdir guard — callers MUST hold this for the lifetime of the
    /// materialized file. Dropping it removes the file.
    pub guard: tempfile::TempDir,
    /// Number of partition records found in the container.
    pub partition_count: usize,
}
