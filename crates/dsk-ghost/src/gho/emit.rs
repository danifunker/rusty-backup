//! Rebuilding a mountable FAT volume from a file-aware image.

use super::*;

// ---------------------------------------------------------------------------
// Slice C — file-aware FAT image emitter
// ---------------------------------------------------------------------------
//
// Given a parsed `GhoImage` + walked `GhoFileAwareTree`, reconstruct a
// fresh, mountable FAT partition image. The output is byte-equivalent to
// the source partition up to cluster allocation order: the original FAT
// chain is NOT preserved (file-aware GHOs don't store the source FAT at
// all), but file names, sizes, content, directory structure, and 8.3 LFN
// pairings ARE.
//
// Sizing: read BPB_TotSec from the source boot sector and pass that
// to `create_blank_fat`. The output FAT type may differ from the
// source's (e.g. our blank formatter picks FAT16 for 1 GiB, even if
// source was FAT32), but the result is mountable everywhere a current
// OS / Ghost Explorer would mount it.
//
// File content: for each file we already have `content_record_offsets`
// from slice B (a list of `0x0002` / `0x0102` body offsets in stream
// order). `GhoFileContentReader` streams decompressed bytes from those
// records on demand, so we never materialise an entire file in RAM
// just to call `create_file`.

/// Per-file content streamer over a file-aware GHO's record body
/// offsets. Yields the file's decompressed content in stream order,
/// truncated to `total_left` bytes (so the final `0x0102` tail doesn't
/// over-spill if the source happened to pad).
pub struct GhoFileContentReader<'a, R: Read + Seek> {
    pub(super) reader: &'a mut R,
    pub(super) record_meta: std::borrow::Cow<'a, [(u64, u16)]>, // (body_start, body_len)
    /// Decoded size of each record read so far, from the first one this reader started at.
    pub(super) record_sizes: Vec<usize>,
    pub(super) compression: GhoCompression,
    pub(super) next_idx: usize,
    pub(super) decoded: Vec<u8>,
    pub(super) pos: usize,
    pub(super) end: usize,
    pub(super) total_left: u64,
    pub(super) body_buf: Vec<u8>,
}

impl<'a, R: Read + Seek> GhoFileContentReader<'a, R> {
    pub fn new(
        reader: &'a mut R,
        record_meta: Vec<(u64, u16)>,
        compression: GhoCompression,
        file_size: u64,
    ) -> Self {
        Self {
            reader,
            record_meta: std::borrow::Cow::Owned(record_meta),
            record_sizes: Vec::new(),
            compression,
            next_idx: 0,
            decoded: vec![0u8; FAST_LZ_BLOCK_SIZE + 1024],
            pos: 0,
            end: 0,
            total_left: file_size,
            body_buf: vec![0u8; u16::MAX as usize],
        }
    }

    /// A reader over `records` that starts at record `start_idx`, `bytes_before` bytes into the file.
    pub(super) fn new_at(
        reader: &'a mut R,
        records: &'a [(u64, u16)],
        compression: GhoCompression,
        file_size: u64,
        start_idx: usize,
        bytes_before: u64,
    ) -> Self {
        let mut r = Self::new(reader, Vec::new(), compression, file_size);
        r.record_meta = std::borrow::Cow::Borrowed(records);
        r.next_idx = start_idx;
        r.total_left = file_size.saturating_sub(bytes_before);
        r
    }

    pub(super) fn fill_next(&mut self) -> std::io::Result<bool> {
        if self.next_idx >= self.record_meta.len() {
            // Records ran out before the file did: the data is missing, not the file shorter.
            if self.total_left > 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("GHO file data ends {} bytes short", self.total_left),
                ));
            }
            return Ok(false);
        }
        let (body_start, body_len) = self.record_meta[self.next_idx];
        self.next_idx += 1;
        self.reader
            .seek(SeekFrom::Start(body_start))
            .map_err(disk_core::io_other)?;
        let body = &mut self.body_buf[..body_len as usize];
        self.reader.read_exact(body)?;
        let n = match self.compression {
            GhoCompression::None => {
                if self.decoded.len() < body.len() {
                    self.decoded.resize(body.len(), 0);
                }
                self.decoded[..body_len as usize].copy_from_slice(body);
                body_len as usize
            }
            GhoCompression::Fast => fast_lz_decompress(body, body_len as usize, &mut self.decoded)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?,
            GhoCompression::High => zlib_decode_block(body, &mut self.decoded)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?,
            GhoCompression::Other(b) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unsupported GHO compression byte {:#04x}", b),
                ))
            }
        };
        self.pos = 0;
        self.end = n;
        self.record_sizes.push(n);
        Ok(true)
    }
}

impl<'a, R: Read + Seek> Read for GhoFileContentReader<'a, R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.total_left == 0 || out.is_empty() {
            return Ok(0);
        }
        if self.pos >= self.end && !self.fill_next()? {
            return Ok(0);
        }
        let avail = (self.end - self.pos)
            .min(out.len())
            .min(self.total_left as usize);
        out[..avail].copy_from_slice(&self.decoded[self.pos..self.pos + avail]);
        self.pos += avail;
        self.total_left -= avail as u64;
        Ok(avail)
    }
}

/// Outcome of emitting a file-aware GHO to a fresh FAT image. The
/// `image` field is populated only by the Vec-returning variant
/// [`emit_file_aware_fat_image`]; the streaming variant
/// [`emit_file_aware_fat_image_to_sink`] leaves it empty since the
/// image is written directly to the caller's sink.
#[derive(Debug, Clone, Default)]
pub struct EmitFileAwareResult {
    pub image: Vec<u8>,
    pub bytes_written: u64,
    pub files_emitted: usize,
    pub dirs_emitted: usize,
    /// Entries we couldn't translate (e.g. name failed FAT validation).
    /// Includes the source short name + reason. Reconstruction continues
    /// past these; the resulting image is mountable but missing those
    /// entries.
    pub skipped: Vec<(String, String)>,
}

/// Read the source partition size in bytes from the boot sector record
/// (`0x0017` / `0x0717`). Returns `None` if the record is missing or the
/// BPB looks bogus.
pub(super) fn source_partition_size_from_boot<R: Read + Seek>(
    reader: &mut R,
    image: &GhoImage,
) -> Result<Option<u64>> {
    let Some(bs) = image
        .records
        .iter()
        .find(|r| is_boot_sector_record(r.type_code))
    else {
        return Ok(None);
    };
    if (bs.body_len as usize) < 36 {
        return Ok(None);
    }
    let bpb = read_record_body(reader, bs, &image.cached_bodies)?;
    let n = bpb.len();
    if n < 36 {
        return Ok(None);
    }
    let byts_per_sec = u16::from_le_bytes([bpb[11], bpb[12]]) as u64;
    if !(byts_per_sec == 512
        || byts_per_sec == 1024
        || byts_per_sec == 2048
        || byts_per_sec == 4096)
    {
        return Ok(None);
    }
    let tot_sec_16 = u16::from_le_bytes([bpb[19], bpb[20]]) as u64;
    let tot_sec_32 = u32::from_le_bytes([bpb[32], bpb[33], bpb[34], bpb[35]]) as u64;
    let tot_sec = if tot_sec_16 != 0 {
        tot_sec_16
    } else {
        tot_sec_32
    };
    if tot_sec == 0 {
        return Ok(None);
    }
    Ok(Some(tot_sec * byts_per_sec))
}

/// Pick a name to feed to the FAT writer. Prefer the long filename, but
/// fall back to the 8.3 short name if the LFN fails validation (control
/// chars, illegal char, trailing space/dot, etc.).
pub(super) fn choose_emit_name(entry: &GhoFileAwareEntry) -> Option<String> {
    use disk_core::filesystem::FilesystemError;
    fn try_validate(s: &str) -> Result<(), FilesystemError> {
        // Cheap mirror of validate_fat_name. Kept here to avoid making
        // that function public from src/fs/fat.rs.
        if s.is_empty() {
            return Err(FilesystemError::InvalidData("empty".into()));
        }
        for c in s.chars() {
            if matches!(c, '"' | '*' | '/' | ':' | '<' | '>' | '?' | '\\' | '|') {
                return Err(FilesystemError::InvalidData(format!("bad char {c}")));
            }
            if (c as u32) < 0x20 {
                return Err(FilesystemError::InvalidData("control".into()));
            }
        }
        if s.ends_with(' ') || s.ends_with('.') {
            return Err(FilesystemError::InvalidData("trailing".into()));
        }
        Ok(())
    }
    if let Some(lfn) = entry.long_name.as_deref() {
        if try_validate(lfn).is_ok() {
            return Some(lfn.to_string());
        }
    }
    let short = &entry.short_name;
    if try_validate(short).is_ok() {
        return Some(short.clone());
    }
    None
}

// ---------------------------------------------------------------------------
// Streaming file-aware reader support (unifies with GhoReader::FileAware)
// ---------------------------------------------------------------------------

/// In-RAM representation of a reconstructed FAT image. Metadata
/// (BPB, FAT tables, directory clusters, FSInfo) is stored sector-
/// granularly in `sparse`; data-region clusters are resolved on demand
/// by looking up `cluster_to_file` and decompressing the appropriate
/// `0x0002` / `0x0102` records.
///
/// Peak memory for an 8 GB FAT32 reconstruction: ≈ tens of MB
/// (reserved + 2 × FAT + dir clusters + per-file metadata).
pub struct VirtualFatImage {
    pub bytes_per_sector: u32,
    pub sectors_per_cluster: u32,
    pub data_start_sector: u32,
    pub total_size: u64,
    pub cluster_size: u64,
    pub sparse: std::collections::HashMap<u64, Vec<u8>>,
    pub cluster_to_file: std::collections::HashMap<u32, (u32, u32)>,
    pub files: Vec<FileContentRef>,
}

#[derive(Debug, Clone)]
pub struct FileContentRef {
    pub records: Vec<(u64, u16)>,
    pub file_size: u64,
}

/// Sparse `Read + Write + Seek` over a virtual byte range. Storage is
/// sector-granular (one `Vec<u8>` per touched sector). Reads of
/// unwritten sectors return zero.
pub(super) struct SparseSink {
    pub(super) sectors: std::collections::HashMap<u64, Vec<u8>>,
    pub(super) sector_size: u64,
    pub(super) total_size: u64,
    pub(super) pos: u64,
}

impl SparseSink {
    pub(super) fn new(total_size: u64, sector_size: u64) -> Self {
        Self {
            sectors: std::collections::HashMap::new(),
            sector_size,
            total_size,
            pos: 0,
        }
    }
}

impl Read for SparseSink {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() || self.pos >= self.total_size {
            return Ok(0);
        }
        let sector_lba = self.pos / self.sector_size;
        let off_in_sector = (self.pos % self.sector_size) as usize;
        let avail = (self.sector_size as usize) - off_in_sector;
        let remaining = (self.total_size - self.pos) as usize;
        let n = buf.len().min(avail).min(remaining);
        match self.sectors.get(&sector_lba) {
            Some(stored) => {
                buf[..n].copy_from_slice(&stored[off_in_sector..off_in_sector + n]);
            }
            None => {
                for b in &mut buf[..n] {
                    *b = 0;
                }
            }
        }
        self.pos += n as u64;
        Ok(n)
    }
}

impl Write for SparseSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let sector_lba = self.pos / self.sector_size;
        let off_in_sector = (self.pos % self.sector_size) as usize;
        let avail = (self.sector_size as usize) - off_in_sector;
        let n = buf.len().min(avail);
        let sector_size = self.sector_size as usize;
        let sector = self
            .sectors
            .entry(sector_lba)
            .or_insert_with(|| vec![0u8; sector_size]);
        sector[off_in_sector..off_in_sector + n].copy_from_slice(&buf[..n]);
        self.pos += n as u64;
        if self.pos > self.total_size {
            self.total_size = self.pos;
        }
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Seek for SparseSink {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos: i128 = match pos {
            SeekFrom::Start(o) => o as i128,
            SeekFrom::Current(o) => self.pos as i128 + o as i128,
            SeekFrom::End(o) => self.total_size as i128 + o as i128,
        };
        if new_pos < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "SparseSink seek before start",
            ));
        }
        self.pos = new_pos as u64;
        Ok(self.pos)
    }
}

/// Build the in-memory virtual FAT image used by
/// `GhoReaderMode::FileAware`.
pub(super) fn build_virtual_fat_image(
    inner: &mut SpanReader,
    image: &GhoImage,
    tree: &GhoFileAwareTree,
    compression: GhoCompression,
) -> Result<VirtualFatImage> {
    use disk_core::entry::FileEntry;
    use disk_core::filesystem::{
        CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem,
    };
    use fs_pc::fat::{compute_fat_blank_layout, write_blank_fat_metadata_to_sink, FatFilesystem};
    use std::collections::HashMap;

    let _ = compression;

    let size = match source_partition_size_from_boot(inner, image)? {
        Some(s) => s,
        None => bail!("file-aware GHO has no usable boot sector — can't size virtual image"),
    };
    if size < 64 * 1024 {
        bail!(
            "boot sector reports partition size {} bytes (< 64 KiB) — refusing to virtualise",
            size
        );
    }
    let layout = compute_fat_blank_layout(size)
        .with_context(|| format!("computing FAT layout for size {}", size))?;

    let bps = layout.bytes_per_sector as u64;
    let mut sink = SparseSink::new(size, bps);
    write_blank_fat_metadata_to_sink(&mut sink, &layout, tree.volume_label())
        .context("writing blank FAT metadata into sparse sink")?;

    let mut fs = FatFilesystem::open(&mut sink, 0).context("opening sparse FAT image for emit")?;

    let mut offset_to_len: HashMap<u64, u16> = HashMap::with_capacity(image.records.len());
    for rec in &image.records {
        if is_data_block_record(rec.type_code) {
            offset_to_len.insert(rec.body_start(), rec.body_len);
        }
    }

    let mut children_by_parent: HashMap<u32, Vec<usize>> = HashMap::new();
    for (idx, e) in tree.entries.iter().enumerate() {
        children_by_parent
            .entry(e.parent_cluster)
            .or_default()
            .push(idx);
    }

    let root = fs.root().map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut stack: Vec<(u32, FileEntry)> = vec![(tree.root_cluster, root)];
    // A directory that points back at an ancestor is emitted once, not nested until the volume fills.
    let mut seen_dirs = std::collections::HashSet::new();

    let mut files: Vec<FileContentRef> = Vec::new();
    let mut cluster_to_file: HashMap<u32, (u32, u32)> = HashMap::new();
    // Out of space means the derived layout is wrong, so the image can't be served whole.
    let skip = |name: &str, e: disk_core::filesystem::FilesystemError| -> Result<()> {
        if matches!(e, disk_core::filesystem::FilesystemError::DiskFull(_)) {
            bail!("virtual FAT image ran out of space at {name}: {e}");
        }
        log::warn!("GHO: skipping {name}: {e}");
        Ok(())
    };

    while let Some((src_cluster, parent_entry)) = stack.pop() {
        if !seen_dirs.insert(src_cluster) {
            log::warn!("GHO: directory cluster {src_cluster} is listed twice; skipping the repeat");
            continue;
        }
        let Some(child_idxs) = children_by_parent.get(&src_cluster) else {
            continue;
        };
        let child_idxs = child_idxs.clone();
        for idx in child_idxs {
            let entry = &tree.entries[idx];
            if entry.is_volume_label() {
                continue;
            }
            let Some(name) = choose_emit_name(entry) else {
                continue;
            };

            if entry.is_directory() {
                match fs.create_directory(&parent_entry, &name, &CreateDirectoryOptions::default())
                {
                    Ok(dir_entry) => stack.push((entry.source_cluster, dir_entry)),
                    Err(e) => skip(&name, e)?,
                }
            } else {
                let mut meta: Vec<(u64, u16)> =
                    Vec::with_capacity(entry.content_record_offsets.len());
                let mut missing = false;
                for &off in &entry.content_record_offsets {
                    match offset_to_len.get(&off) {
                        Some(&l) => meta.push((off, l)),
                        None => {
                            missing = true;
                            break;
                        }
                    }
                }
                if missing {
                    log::warn!("GHO: skipping {name}: its data records are not in the image");
                    continue;
                }
                let file_size = entry.file_size as u64;
                let mut empty = std::io::empty();
                let opts = CreateFileOptions {
                    skip_data_write: true,
                    skip_name_checks: true,
                    skip_fsinfo_update: true,
                    ..Default::default()
                };
                let f = match fs.create_file(&parent_entry, &name, &mut empty, file_size, &opts) {
                    Ok(f) => f,
                    Err(e) => {
                        skip(&name, e)?;
                        continue;
                    }
                };
                let file_id = files.len() as u32;
                files.push(FileContentRef {
                    records: meta,
                    file_size,
                });

                let mut cluster_idx: u32 = 0;
                let mut c = f.location as u32;
                while (2..0x0FFF_FFF8).contains(&c) {
                    cluster_to_file.insert(c, (file_id, cluster_idx));
                    cluster_idx += 1;
                    match fs.next_cluster(c) {
                        Ok(Some(next)) => c = next,
                        _ => break,
                    }
                }
            }
        }
    }

    fs.sync_metadata().map_err(|e| anyhow::anyhow!("{e}"))?;
    drop(fs);

    Ok(VirtualFatImage {
        bytes_per_sector: layout.bytes_per_sector,
        sectors_per_cluster: layout.sectors_per_cluster,
        data_start_sector: (layout.data_start_byte() / bps) as u32,
        total_size: size,
        cluster_size: layout.cluster_size(),
        sparse: sink.sectors,
        cluster_to_file,
        files,
    })
}

/// Vec-returning convenience wrapper: useful for small images and
/// tests. For large partitions, prefer
/// [`emit_file_aware_fat_image_to_sink`] which streams the image to any
/// `Read + Write + Seek` (e.g. a tempfile) without holding the whole
/// thing in RAM.
pub fn emit_file_aware_fat_image<R: Read + Seek>(
    reader: &mut R,
    image: &GhoImage,
    tree: &GhoFileAwareTree,
    compression: GhoCompression,
) -> Result<EmitFileAwareResult> {
    let mut cur = std::io::Cursor::new(Vec::<u8>::new());
    let mut result = emit_file_aware_fat_image_to_sink(reader, image, tree, compression, &mut cur)?;
    result.image = cur.into_inner();
    Ok(result)
}

/// Reconstruct a mountable FAT image from a file-aware GHO and write it
/// to `sink`. The sink is rewound and re-used as the FAT backing store
/// (it must be a true `Read + Write + Seek`). Errors are fatal only for
/// I/O / format problems on the source; per-entry name or disk-full
/// failures are recorded in `EmitFileAwareResult::skipped` and
/// reconstruction continues.
pub fn emit_file_aware_fat_image_to_sink<R: Read + Seek, S: Read + Write + Seek + Send>(
    reader: &mut R,
    image: &GhoImage,
    tree: &GhoFileAwareTree,
    compression: GhoCompression,
    sink: &mut S,
) -> Result<EmitFileAwareResult> {
    use disk_core::entry::FileEntry;
    use disk_core::filesystem::{
        CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem,
    };
    use fs_pc::fat::{compute_fat_blank_layout, write_blank_fat_metadata_to_sink, FatFilesystem};
    use std::collections::HashMap;

    // Decide output image size.
    let size = match source_partition_size_from_boot(reader, image)? {
        Some(s) => s,
        None => bail!("file-aware GHO has no usable boot sector — can't size output image"),
    };
    if size < 64 * 1024 {
        bail!(
            "boot sector reports partition size {} bytes (< 64 KiB) — refusing to format",
            size
        );
    }

    // Zero the partition, then stream the blank FAT's metadata over it: the image stays
    // sector-complete, which some FAT writers need when extending cluster chains.
    let layout = compute_fat_blank_layout(size).context("laying out blank FAT for emit")?;
    sink.seek(SeekFrom::Start(0))?;
    let chunk = vec![0u8; 1 << 20];
    let mut remaining = size;
    while remaining > 0 {
        let n = remaining.min(chunk.len() as u64) as usize;
        sink.write_all(&chunk[..n])?;
        remaining -= n as u64;
    }
    write_blank_fat_metadata_to_sink(sink, &layout, tree.volume_label())
        .context("formatting blank FAT for emit")?;
    sink.flush()?;
    sink.seek(SeekFrom::Start(0))?;

    let mut fs = FatFilesystem::open(sink, 0).context("opening fresh FAT image for emit")?;

    // Build record-offset → body_len lookup. The tree only stores
    // body_start offsets; we need body_len to decode.
    let mut offset_to_len: HashMap<u64, u16> = HashMap::with_capacity(image.records.len());
    for rec in &image.records {
        if is_data_block_record(rec.type_code) {
            offset_to_len.insert(rec.body_start(), rec.body_len);
        }
    }

    // Index children by parent_cluster for DFS pre-order emission.
    let mut children_by_parent: HashMap<u32, Vec<usize>> = HashMap::new();
    for (idx, e) in tree.entries.iter().enumerate() {
        children_by_parent
            .entry(e.parent_cluster)
            .or_default()
            .push(idx);
    }

    let root = fs.root().map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut stack: Vec<(u32, FileEntry)> = Vec::new();
    stack.push((tree.root_cluster, root));

    let mut result = EmitFileAwareResult {
        image: Vec::new(),
        bytes_written: size,
        files_emitted: 0,
        dirs_emitted: 0,
        skipped: Vec::new(),
    };

    while let Some((src_cluster, parent_entry)) = stack.pop() {
        let Some(child_idxs) = children_by_parent.get(&src_cluster) else {
            continue;
        };
        let child_idxs = child_idxs.clone();
        for idx in child_idxs {
            let entry = &tree.entries[idx];
            if entry.is_volume_label() {
                continue;
            }
            let Some(name) = choose_emit_name(entry) else {
                result
                    .skipped
                    .push((entry.short_name.clone(), "name failed validation".into()));
                continue;
            };

            if entry.is_directory() {
                match fs.create_directory(&parent_entry, &name, &CreateDirectoryOptions::default())
                {
                    Ok(dir_entry) => {
                        result.dirs_emitted += 1;
                        stack.push((entry.source_cluster, dir_entry));
                    }
                    Err(e) => {
                        result
                            .skipped
                            .push((entry.short_name.clone(), format!("mkdir: {e}")));
                    }
                }
            } else {
                let mut meta: Vec<(u64, u16)> =
                    Vec::with_capacity(entry.content_record_offsets.len());
                let mut missing = false;
                for &off in &entry.content_record_offsets {
                    match offset_to_len.get(&off) {
                        Some(&l) => meta.push((off, l)),
                        None => {
                            missing = true;
                            break;
                        }
                    }
                }
                if missing {
                    result.skipped.push((
                        entry.short_name.clone(),
                        "content record offset not in image".into(),
                    ));
                    continue;
                }
                let file_size = entry.file_size as u64;
                let mut content = GhoFileContentReader::new(reader, meta, compression, file_size);
                match fs.create_file(
                    &parent_entry,
                    &name,
                    &mut content,
                    file_size,
                    &CreateFileOptions::default(),
                ) {
                    Ok(_) => result.files_emitted += 1,
                    Err(e) => result
                        .skipped
                        .push((entry.short_name.clone(), format!("create_file: {e}"))),
                }
            }
        }
    }

    fs.sync_metadata().map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(result)
}
