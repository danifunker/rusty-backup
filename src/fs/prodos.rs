use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{Filesystem, FilesystemError};
use super::CompactResult;

const BLOCK_SIZE: u64 = 512;

// ─────────────────────────────── header ──────────────────────────────────────

struct ProDosVolumeHeader {
    name: String,
    total_blocks: u16,
    bitmap_pointer: u16,
    free_blocks: u16,
}

// ─────────────────────────────── filesystem ──────────────────────────────────

/// ProDOS filesystem reader for volume-directory browsing and compaction.
pub struct ProDosFilesystem<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    header: ProDosVolumeHeader,
}

impl<R: Read + Seek + Send> ProDosFilesystem<R> {
    /// Open a ProDOS filesystem at the given partition offset.
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let header = read_volume_header(&mut reader, partition_offset)?;
        Ok(Self {
            reader,
            partition_offset,
            header,
        })
    }
}

impl<R: Read + Seek + Send> Filesystem for ProDosFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry {
            name: self.header.name.clone(),
            path: "/".into(),
            entry_type: EntryType::Directory,
            size: 0,
            location: 2, // Volume Directory Key Block is always at block 2
            modified: None,
            type_code: None,
            creator_code: None,
            symlink_target: None,
            special_type: None,
            mode: None,
            uid: None,
            gid: None,
            resource_fork_size: None,
        })
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        list_prodos_directory(&mut self.reader, self.partition_offset, entry)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        if entry.size == 0 || max_bytes == 0 {
            return Ok(Vec::new());
        }
        let key_ptr = entry.location as u16;
        let eof = entry.size as usize;
        // storage_type is stored in entry.mode (set by list_directory).
        // Fall back to inference from eof if mode is absent.
        let storage_type = entry.mode.unwrap_or_else(|| infer_storage_type(eof)) as u8;

        let data = match storage_type {
            1 => read_seedling(&mut self.reader, self.partition_offset, key_ptr, eof)?,
            2 => read_sapling(&mut self.reader, self.partition_offset, key_ptr, eof)?,
            3 => read_tree(&mut self.reader, self.partition_offset, key_ptr, eof)?,
            _ => read_seedling(&mut self.reader, self.partition_offset, key_ptr, eof)?,
        };

        if data.len() > max_bytes {
            Ok(data[..max_bytes].to_vec())
        } else {
            Ok(data)
        }
    }

    fn volume_label(&self) -> Option<&str> {
        Some(&self.header.name)
    }

    fn fs_type(&self) -> &str {
        "ProDOS"
    }

    fn total_size(&self) -> u64 {
        self.header.total_blocks as u64 * BLOCK_SIZE
    }

    fn used_size(&self) -> u64 {
        (self.header.total_blocks as u64).saturating_sub(self.header.free_blocks as u64)
            * BLOCK_SIZE
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        let bitmap = read_bitmap(&mut self.reader, self.partition_offset, &self.header)?;
        let total = self.header.total_blocks as u32;
        let mut last_used = 0u32;
        for block in 0..total {
            if is_block_used(&bitmap, block) {
                last_used = block;
            }
        }
        Ok((last_used as u64 + 1) * BLOCK_SIZE)
    }
}

// ─────────────────────────────── compact reader ──────────────────────────────

/// Compact reader for ProDOS: emits all blocks 0..total_blocks, replacing
/// free blocks (bitmap bit=1) with zeros for better compression.
///
/// The original block layout is preserved (block N always at byte N×512),
/// so a restore from the compact image produces an immediately-usable ProDOS
/// volume with no further patching needed.
pub struct CompactProDosReader<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    total_blocks: u32,
    bitmap: Vec<u8>,
    current_block: u32,
    block_pos: u64,
}

impl<R: Read + Seek> CompactProDosReader<R> {
    pub fn new(
        mut reader: R,
        partition_offset: u64,
    ) -> Result<(Self, CompactResult), FilesystemError> {
        let header = read_volume_header(&mut reader, partition_offset)?;
        let bitmap = read_bitmap(&mut reader, partition_offset, &header)?;

        let total_blocks = header.total_blocks as u32;
        let used_blocks = total_blocks.saturating_sub(header.free_blocks as u32);
        let size = total_blocks as u64 * BLOCK_SIZE;

        let result = CompactResult {
            original_size: size,
            compacted_size: size,
            data_size: used_blocks as u64 * BLOCK_SIZE,
            clusters_used: used_blocks,
        };

        Ok((
            CompactProDosReader {
                reader,
                partition_offset,
                total_blocks,
                bitmap,
                current_block: 0,
                block_pos: 0,
            },
            result,
        ))
    }
}

impl<R: Read + Seek> Read for CompactProDosReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.current_block >= self.total_blocks {
            return Ok(0);
        }

        let remaining_in_block = BLOCK_SIZE - self.block_pos;
        let to_read = (remaining_in_block as usize).min(buf.len());

        let n = if is_block_used(&self.bitmap, self.current_block) {
            let offset =
                self.partition_offset + self.current_block as u64 * BLOCK_SIZE + self.block_pos;
            self.reader
                .seek(SeekFrom::Start(offset))
                .map_err(std::io::Error::other)?;
            self.reader.read(&mut buf[..to_read])?
        } else {
            // Free block — zero fill so free space compresses away.
            buf[..to_read].fill(0);
            to_read
        };

        self.block_pos += n as u64;
        if self.block_pos >= BLOCK_SIZE {
            self.block_pos = 0;
            self.current_block += 1;
        }

        Ok(n)
    }
}

// ─────────────────────────────── resize ──────────────────────────────────────

/// Resize a ProDOS filesystem in place.
///
/// Updates `total_blocks` in the volume directory key block and, when growing,
/// extends the volume bitmap so new blocks are marked free.
pub fn resize_prodos_in_place(
    device: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    new_total_bytes: u64,
    log: &mut impl FnMut(&str),
) -> anyhow::Result<()> {
    // Read the volume directory key block (always at partition_offset + 1024).
    device.seek(SeekFrom::Start(partition_offset + 1024))?;
    let mut sector = [0u8; 512];
    device.read_exact(&mut sector)?;

    if !validate_volume_key_block(&sector) {
        log("ProDOS: not a ProDOS volume, skipping resize");
        return Ok(());
    }

    let old_total = u16::from_le_bytes([sector[33], sector[34]]);
    let bitmap_pointer = u16::from_le_bytes([sector[31], sector[32]]);

    // Count used blocks to guard against shrinking into data.
    let bitmap_blocks_needed = ((old_total as u32 + 4095) / 4096) as usize;
    let mut bitmap: Vec<u8> = Vec::with_capacity(bitmap_blocks_needed * 512);
    {
        let mut bm_block = [0u8; 512];
        for i in 0..bitmap_blocks_needed {
            let offset = partition_offset + (bitmap_pointer as u64 + i as u64) * BLOCK_SIZE;
            device.seek(SeekFrom::Start(offset))?;
            // Ignore read errors; unreadable blocks are treated as fully-used.
            let _ = device.read_exact(&mut bm_block);
            bitmap.extend_from_slice(&bm_block);
        }
    }

    let mut used_blocks = 0u32;
    for block in 0..old_total as u32 {
        if is_block_used(&bitmap, block) {
            used_blocks += 1;
        }
    }

    let new_total = ((new_total_bytes / BLOCK_SIZE) as u32).min(65535) as u16;

    if new_total == old_total {
        log("ProDOS: no resize needed");
        return Ok(());
    }

    if (new_total as u32) < used_blocks {
        log(&format!(
            "ProDOS: cannot resize to {} blocks; {} blocks are in use — skipping",
            new_total, used_blocks
        ));
        return Ok(());
    }

    // Patch total_blocks in the key block and write it back.
    let tb = new_total.to_le_bytes();
    sector[33] = tb[0];
    sector[34] = tb[1];
    device.seek(SeekFrom::Start(partition_offset + 1024))?;
    device.write_all(&sector)?;

    // If growing, mark new blocks as free (bit=1) in the bitmap.
    if new_total > old_total {
        let new_bitmap_blocks = ((new_total as u32 + 4095) / 4096) as usize;
        bitmap.resize(new_bitmap_blocks * 512, 0);

        for block in old_total as u32..new_total as u32 {
            let byte_idx = (block / 8) as usize;
            let bit_idx = 7 - (block % 8);
            if byte_idx < bitmap.len() {
                bitmap[byte_idx] |= 1 << bit_idx;
            }
        }

        for i in 0..new_bitmap_blocks {
            let offset = partition_offset + (bitmap_pointer as u64 + i as u64) * BLOCK_SIZE;
            device.seek(SeekFrom::Start(offset))?;
            let start = i * 512;
            let end = (start + 512).min(bitmap.len());
            let mut bm_block = [0u8; 512];
            if start < bitmap.len() {
                bm_block[..end - start].copy_from_slice(&bitmap[start..end]);
            }
            device.write_all(&bm_block)?;
        }
    }

    log(&format!(
        "ProDOS: resized from {} to {} blocks",
        old_total, new_total
    ));
    Ok(())
}

/// Validate a ProDOS filesystem structure and return any warnings.
pub fn validate_prodos_integrity(
    device: &mut (impl Read + Seek),
    partition_offset: u64,
    log: &mut impl FnMut(&str),
) -> anyhow::Result<Vec<String>> {
    let mut warnings = Vec::new();

    device.seek(SeekFrom::Start(partition_offset + 1024))?;
    let mut sector = [0u8; 512];
    device.read_exact(&mut sector)?;

    if !validate_volume_key_block(&sector) {
        warnings.push("ProDOS: invalid volume directory key block signature".into());
        return Ok(warnings);
    }

    let entry_length = sector[27];
    let entries_per_block = sector[28];
    let total_blocks = u16::from_le_bytes([sector[33], sector[34]]);

    if entry_length != 39 {
        warnings.push(format!(
            "ProDOS: unexpected entry_length {entry_length} (expected 39)"
        ));
    }
    if entries_per_block != 13 {
        warnings.push(format!(
            "ProDOS: unexpected entries_per_block {entries_per_block} (expected 13)"
        ));
    }
    if total_blocks == 0 {
        warnings.push("ProDOS: total_blocks is zero".into());
    }

    if warnings.is_empty() {
        log("ProDOS: filesystem structure OK");
    }

    Ok(warnings)
}

// ─────────────────────────────── internal helpers ────────────────────────────

fn read_volume_header<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
) -> Result<ProDosVolumeHeader, FilesystemError> {
    reader
        .seek(SeekFrom::Start(partition_offset + 1024))
        .map_err(FilesystemError::Io)?;
    let mut buf = [0u8; 512];
    reader.read_exact(&mut buf).map_err(FilesystemError::Io)?;

    if !validate_volume_key_block(&buf) {
        return Err(FilesystemError::Parse(
            "not a ProDOS volume (invalid volume directory key block)".into(),
        ));
    }

    let name_len = (buf[4] & 0xF) as usize;
    let name: String = buf[5..5 + name_len.min(15)]
        .iter()
        .map(|&b| b as char)
        .collect();

    let bitmap_pointer = u16::from_le_bytes([buf[31], buf[32]]);
    let total_blocks = u16::from_le_bytes([buf[33], buf[34]]);

    // Build a temporary header so read_bitmap can iterate correctly.
    let tmp = ProDosVolumeHeader {
        name: name.clone(),
        total_blocks,
        bitmap_pointer,
        free_blocks: 0,
    };
    let bitmap = read_bitmap(reader, partition_offset, &tmp)?;

    let mut free_blocks = 0u32;
    for block in 0..total_blocks as u32 {
        if !is_block_used(&bitmap, block) {
            free_blocks += 1;
        }
    }

    Ok(ProDosVolumeHeader {
        name,
        total_blocks,
        bitmap_pointer,
        free_blocks: free_blocks.min(u16::MAX as u32) as u16,
    })
}

fn read_bitmap<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    header: &ProDosVolumeHeader,
) -> Result<Vec<u8>, FilesystemError> {
    let total_blocks = header.total_blocks as u32;
    let bitmap_blocks = ((total_blocks + 4095) / 4096) as usize;
    let mut bitmap = Vec::with_capacity(bitmap_blocks * 512);
    let mut bm_block = [0u8; 512];

    for i in 0..bitmap_blocks {
        let offset = partition_offset + (header.bitmap_pointer as u64 + i as u64) * BLOCK_SIZE;
        reader
            .seek(SeekFrom::Start(offset))
            .map_err(FilesystemError::Io)?;
        reader
            .read_exact(&mut bm_block)
            .map_err(FilesystemError::Io)?;
        bitmap.extend_from_slice(&bm_block);
    }

    Ok(bitmap)
}

/// Returns `true` if the block is used (ProDOS bitmap bit == 0 means used).
fn is_block_used(bitmap: &[u8], block: u32) -> bool {
    let byte_idx = (block / 8) as usize;
    let bit_idx = 7 - (block % 8); // MSB-first ordering
    if byte_idx >= bitmap.len() {
        return false;
    }
    (bitmap[byte_idx] >> bit_idx) & 1 == 0 // 0=used, 1=free
}

/// Infer storage type from EOF when the mode field is absent.
fn infer_storage_type(eof: usize) -> u32 {
    if eof <= 512 {
        1 // seedling
    } else if eof <= 256 * 512 {
        2 // sapling
    } else {
        3 // tree
    }
}

fn validate_volume_key_block(buf: &[u8]) -> bool {
    buf.len() >= 29
        && buf[0] == 0
        && buf[1] == 0 // prev_block == 0
        && (buf[4] >> 4) == 0xF // storage_type nibble = volume dir
        && (buf[4] & 0xF) >= 1 // name_length valid
        && buf[27] == 39 // entry_length
        && buf[28] == 13 // entries_per_block
}

fn list_prodos_directory<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    entry: &FileEntry,
) -> Result<Vec<FileEntry>, FilesystemError> {
    let mut result = Vec::new();
    let mut block_num = entry.location as u16;

    while block_num != 0 {
        let block_offset = partition_offset + block_num as u64 * BLOCK_SIZE;
        reader
            .seek(SeekFrom::Start(block_offset))
            .map_err(FilesystemError::Io)?;
        let mut block = [0u8; 512];
        reader.read_exact(&mut block).map_err(FilesystemError::Io)?;

        // next_block pointer at bytes 2–3 (LE u16)
        let next_block = u16::from_le_bytes([block[2], block[3]]);

        // 13 entries × 39 bytes starting at offset 4
        for i in 0..13usize {
            let eo = 4 + i * 39;
            if eo + 39 > 512 {
                break;
            }
            let e = &block[eo..eo + 39];
            let type_nibble = e[0] >> 4;
            let name_len = (e[0] & 0xF) as usize;

            // Skip deleted (0), volume directory header (0xF), subdir header (0xD).
            if type_nibble == 0 || type_nibble == 0xF || type_nibble == 0xD {
                continue;
            }
            if name_len == 0 || name_len > 15 {
                continue;
            }

            let name: String = e[1..1 + name_len].iter().map(|&b| b as char).collect();
            let path = if entry.path == "/" {
                format!("/{name}")
            } else {
                format!("{}/{name}", entry.path)
            };

            let key_pointer = u16::from_le_bytes([e[17], e[18]]);
            let eof = (e[21] as u32) | ((e[22] as u32) << 8) | ((e[23] as u32) << 16);
            let file_type = e[16];
            let creation_date = u16::from_le_bytes([e[24], e[25]]);
            let creation_time = u16::from_le_bytes([e[26], e[27]]);
            let modified_date = u16::from_le_bytes([e[33], e[34]]);
            let modified_time = u16::from_le_bytes([e[35], e[36]]);

            let modified = parse_prodos_datetime(modified_date, modified_time)
                .or_else(|| parse_prodos_datetime(creation_date, creation_time));

            if type_nibble == 0xE {
                // Subdirectory entry: key_pointer is the subdirectory key block.
                let mut fe = FileEntry::new_directory(name, path, key_pointer as u64);
                fe.modified = modified;
                result.push(fe);
            } else {
                // Regular file: seedling (1), sapling (2), or tree (3).
                let type_name = prodos_type_name(file_type);
                let mut fe = FileEntry::new_file(name, path, eof as u64, key_pointer as u64);
                fe.modified = modified;
                fe.type_code = Some(format!("${file_type:02X} {type_name}"));
                // Store storage type in `mode` so read_file can dispatch correctly.
                fe.mode = Some(type_nibble as u32);
                result.push(fe);
            }
        }

        block_num = next_block;
    }

    Ok(result)
}

/// Read the 256 block pointers from an index block.
///
/// Index block layout: low bytes at offsets 0–255, high bytes at offsets 256–511.
/// block_number[i] = buf[256 + i] as u16 * 256 + buf[i] as u16.
/// Block number 0 denotes a sparse (all-zeros) block.
fn read_index_block<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    block_num: u16,
) -> Result<[u16; 256], FilesystemError> {
    if block_num == 0 {
        return Ok([0u16; 256]);
    }
    let offset = partition_offset + block_num as u64 * BLOCK_SIZE;
    reader
        .seek(SeekFrom::Start(offset))
        .map_err(FilesystemError::Io)?;
    let mut buf = [0u8; 512];
    reader.read_exact(&mut buf).map_err(FilesystemError::Io)?;

    let mut ptrs = [0u16; 256];
    for i in 0..256 {
        ptrs[i] = buf[i] as u16 | ((buf[256 + i] as u16) << 8);
    }
    Ok(ptrs)
}

/// Read a seedling file (single data block, key_ptr → data).
fn read_seedling<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    key_ptr: u16,
    eof: usize,
) -> Result<Vec<u8>, FilesystemError> {
    if key_ptr == 0 {
        return Ok(vec![0u8; eof.min(512)]);
    }
    let offset = partition_offset + key_ptr as u64 * BLOCK_SIZE;
    reader
        .seek(SeekFrom::Start(offset))
        .map_err(FilesystemError::Io)?;
    let mut buf = [0u8; 512];
    reader.read_exact(&mut buf).map_err(FilesystemError::Io)?;
    Ok(buf[..eof.min(512)].to_vec())
}

/// Read a sapling file (index block → up to 256 data blocks).
fn read_sapling<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    key_ptr: u16,
    eof: usize,
) -> Result<Vec<u8>, FilesystemError> {
    let index = read_index_block(reader, partition_offset, key_ptr)?;
    let mut data = Vec::with_capacity(eof);

    for &data_block in index.iter() {
        if data.len() >= eof {
            break;
        }
        let remaining = eof - data.len();
        let to_take = remaining.min(512);

        if data_block == 0 {
            data.extend_from_slice(&vec![0u8; to_take]);
        } else {
            let offset = partition_offset + data_block as u64 * BLOCK_SIZE;
            reader
                .seek(SeekFrom::Start(offset))
                .map_err(FilesystemError::Io)?;
            let mut buf = [0u8; 512];
            reader.read_exact(&mut buf).map_err(FilesystemError::Io)?;
            data.extend_from_slice(&buf[..to_take]);
        }
    }

    Ok(data)
}

/// Read a tree file (master index block → up to 128 index blocks → data blocks).
fn read_tree<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    key_ptr: u16,
    eof: usize,
) -> Result<Vec<u8>, FilesystemError> {
    let master = read_index_block(reader, partition_offset, key_ptr)?;
    let mut data = Vec::with_capacity(eof);

    // ProDOS tree files use at most 128 index block slots in the master index.
    for &index_block_num in master[..128].iter() {
        if data.len() >= eof {
            break;
        }

        let index = if index_block_num == 0 {
            [0u16; 256] // Sparse index block
        } else {
            read_index_block(reader, partition_offset, index_block_num)?
        };

        for &data_block in index.iter() {
            if data.len() >= eof {
                break;
            }
            let remaining = eof - data.len();
            let to_take = remaining.min(512);

            if data_block == 0 {
                data.extend_from_slice(&vec![0u8; to_take]);
            } else {
                let offset = partition_offset + data_block as u64 * BLOCK_SIZE;
                reader
                    .seek(SeekFrom::Start(offset))
                    .map_err(FilesystemError::Io)?;
                let mut buf = [0u8; 512];
                reader.read_exact(&mut buf).map_err(FilesystemError::Io)?;
                data.extend_from_slice(&buf[..to_take]);
            }
        }
    }

    Ok(data)
}

// ─────────────────────────────── ProDOS type codes ───────────────────────────

fn prodos_type_name(file_type: u8) -> &'static str {
    match file_type {
        0x00 => "NON",
        0x01 => "BAD",
        0x04 => "TXT",
        0x06 => "BIN",
        0x0F => "DIR",
        0x19 => "ADB",
        0x1A => "AWP",
        0x1B => "ASP",
        0xB0 => "SRC",
        0xB3 => "S16",
        0xB4 => "RTL",
        0xB5 => "EXE",
        0xB6 => "PIF",
        0xB7 => "TIF",
        0xB8 => "NDA",
        0xB9 => "CDA",
        0xBA => "TOL",
        0xBB => "DVR",
        0xBC => "LDF",
        0xBD => "FST",
        0xBF => "DOC",
        0xC0 => "PNT",
        0xC1 => "PIC",
        0xC2 => "ANI",
        0xC3 => "PAL",
        0xC8 => "FON",
        0xC9 => "FND",
        0xCA => "ICN",
        0xD5 => "MUS",
        0xD6 => "INS",
        0xD7 => "MDI",
        0xD8 => "SND",
        0xDB => "DBM",
        0xE0 => "LBR",
        0xEF => "PAS",
        0xF0 => "CMD",
        0xF1 => "OVL",
        0xFC => "BAS",
        0xFD => "VAR",
        0xFE => "REL",
        0xFF => "SYS",
        _ => "???",
    }
}

/// Parse a ProDOS date+time pair into a human-readable string.
///
/// Date format (u16 LE): bits[15:9]=year (7-bit, +1900; <70 → 2000+),
///   bits[8:5]=month (4-bit, 1–12), bits[4:0]=day (5-bit, 1–31).
/// Time format (u16 LE): high byte = hour, low byte = minute.
fn parse_prodos_datetime(date: u16, time: u16) -> Option<String> {
    if date == 0 {
        return None;
    }

    let year_raw = (date >> 9) & 0x7F;
    let month = (date >> 5) & 0xF;
    let day = date & 0x1F;
    let hour = (time >> 8) as u8;
    let minute = (time & 0xFF) as u8;

    if month == 0 || month > 12 || day == 0 || day > 31 {
        return None;
    }

    let year = if year_raw < 70 {
        2000 + year_raw as u16
    } else {
        1900 + year_raw as u16
    };

    Some(format!(
        "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}"
    ))
}

// ─────────────────────────────── tests ───────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // Build a minimal 512-byte volume directory key block for testing.
    // Volume name "BLANK", total_blocks=280, bitmap at block 6.
    fn make_volume_key_block() -> [u8; 512] {
        let mut block = [0u8; 512];
        // prev_block = 0 (bytes 0-1), next_block = 0 (bytes 2-3)
        // storage_type=0xF, name_length=5
        block[4] = 0xF5;
        block[5] = b'B';
        block[6] = b'L';
        block[7] = b'A';
        block[8] = b'N';
        block[9] = b'K';
        block[27] = 39; // entry_length
        block[28] = 13; // entries_per_block
        block[31] = 6;
        block[32] = 0; // bitmap_pointer = 6
        let tb: u16 = 280;
        block[33] = tb as u8;
        block[34] = (tb >> 8) as u8; // total_blocks = 280
        block
    }

    // Build a 280-block disk image with the given key block at block 2
    // and a bitmap at block 6 that marks blocks 0–6 as used.
    fn make_disk_image(volume_key: [u8; 512]) -> Vec<u8> {
        let total = 280usize * 512;
        let mut disk = vec![0u8; total];
        disk[1024..1024 + 512].copy_from_slice(&volume_key);

        // Bitmap at block 6 (offset 3072):
        //   blocks 0-6 used (bit=0), block 7 free (bit=1), rest free (0xFF)
        // Byte 0 = 0b0000_0001: MSB=block0(used)…bit0=block7(free)
        disk[3072] = 0x01;
        for i in 1..512 {
            disk[3072 + i] = 0xFF;
        }
        disk
    }

    #[test]
    fn test_parse_volume_header() {
        let vk = make_volume_key_block();
        let disk = make_disk_image(vk);
        let mut cursor = Cursor::new(disk);
        let header = read_volume_header(&mut cursor, 0).unwrap();
        assert_eq!(header.name, "BLANK");
        assert_eq!(header.total_blocks, 280);
        assert_eq!(header.bitmap_pointer, 6);
        // 7 used blocks → free = 280 − 7 = 273
        assert_eq!(header.free_blocks, 273);
    }

    #[test]
    fn test_validate_volume_key_block() {
        let vk = make_volume_key_block();
        assert!(validate_volume_key_block(&vk));

        let mut bad = vk;
        bad[27] = 40; // wrong entry_length
        assert!(!validate_volume_key_block(&bad));

        let mut bad2 = vk;
        bad2[4] = 0xA5; // storage_type nibble != 0xF
        assert!(!validate_volume_key_block(&bad2));
    }

    #[test]
    fn test_is_block_used() {
        // byte 0 = 0x01 = 0b0000_0001
        // blocks 0–6: used (bit=0), block 7: free (bit=1)
        let bitmap = [0x01u8, 0xFF, 0xFF];
        assert!(is_block_used(&bitmap, 0));
        assert!(is_block_used(&bitmap, 1));
        assert!(is_block_used(&bitmap, 6));
        assert!(!is_block_used(&bitmap, 7));
        assert!(!is_block_used(&bitmap, 8));
    }

    #[test]
    fn test_parse_prodos_datetime() {
        // Year 91 (1991), month 10 (Oct), day 15.
        // Standard ProDOS format: bits[15:9]=year, bits[8:5]=month, bits[4:0]=day.
        let year: u16 = 91;
        let month: u16 = 10;
        let day: u16 = 15;
        let date = (year << 9) | (month << 5) | day;
        // Time: hour=14, minute=30
        let time: u16 = (14 << 8) | 30;
        let result = parse_prodos_datetime(date, time);
        assert_eq!(result, Some("1991-10-15 14:30".to_string()));

        // Y2K: year 3 → 2003
        let date2 = (3u16 << 9) | (month << 5) | day;
        let result2 = parse_prodos_datetime(date2, time);
        assert_eq!(result2, Some("2003-10-15 14:30".to_string()));

        // date == 0 → None
        assert_eq!(parse_prodos_datetime(0, 0), None);

        // month 0 → None
        let bad_date = (year << 9) | (0u16 << 5) | day;
        assert_eq!(parse_prodos_datetime(bad_date, time), None);
    }

    #[test]
    fn test_read_seedling_file() {
        let content = b"Hello, ProDOS!";
        let mut disk = vec![0u8; 512 * 10];
        // File data at block 5 (offset 2560)
        disk[5 * 512..5 * 512 + content.len()].copy_from_slice(content);
        let mut cursor = Cursor::new(disk);
        let data = read_seedling(&mut cursor, 0, 5, content.len()).unwrap();
        assert_eq!(&data, content);
    }

    #[test]
    fn test_read_sapling_file() {
        // Build a sapling: index block at block 3, data at blocks 4 and 5.
        // ProDOS sapling: block 4 holds bytes 0–511, block 5 holds bytes 512–1023.
        let mut disk = vec![0u8; 512 * 10];
        // Index block at block 3: entry[0] = block 4, entry[1] = block 5
        disk[3 * 512] = 4; // lo byte of pointer[0]
        disk[3 * 512 + 256] = 0; // hi byte of pointer[0]
        disk[3 * 512 + 1] = 5; // lo byte of pointer[1]
        disk[3 * 512 + 257] = 0; // hi byte of pointer[1]
                                 // Block 4: fill with 'A' (bytes 0–511 of file)
        for i in 0..512 {
            disk[4 * 512 + i] = b'A';
        }
        // Block 5: first 4 bytes = "EFGH" (bytes 512–515 of file)
        disk[5 * 512..5 * 512 + 4].copy_from_slice(b"EFGH");

        let mut cursor = Cursor::new(disk);
        // eof = 516: 512 bytes from block 4, 4 bytes from block 5
        let data = read_sapling(&mut cursor, 0, 3, 516).unwrap();
        assert_eq!(data.len(), 516);
        assert!(
            data[..512].iter().all(|&b| b == b'A'),
            "block 4 content mismatch"
        );
        assert_eq!(&data[512..516], b"EFGH");
    }

    #[test]
    fn test_compact_reader_size() {
        let vk = make_volume_key_block();
        let disk = make_disk_image(vk);
        let cursor = Cursor::new(disk);

        let (mut reader, info) = CompactProDosReader::new(cursor, 0).unwrap();
        assert_eq!(info.original_size, 280 * 512);
        assert_eq!(info.compacted_size, 280 * 512);
        assert_eq!(info.clusters_used, 7);
        assert_eq!(info.data_size, 7 * 512);

        // Read all output
        let mut output = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut output).unwrap();
        assert_eq!(output.len(), 280 * 512);
    }

    #[test]
    fn test_resize_grow() {
        let vk = make_volume_key_block();
        let mut disk = make_disk_image(vk);
        // Extend disk to hold more blocks.
        disk.resize(400 * 512, 0);

        let mut cursor = Cursor::new(disk);
        resize_prodos_in_place(&mut cursor, 0, 400 * 512, &mut |_| {}).unwrap();

        // Verify total_blocks was updated.
        cursor.seek(SeekFrom::Start(1024)).unwrap();
        let mut sector = [0u8; 512];
        cursor.read_exact(&mut sector).unwrap();
        let new_total = u16::from_le_bytes([sector[33], sector[34]]);
        assert_eq!(new_total, 400);
    }

    #[test]
    fn test_resize_no_op() {
        let vk = make_volume_key_block();
        let disk = make_disk_image(vk);
        let mut cursor = Cursor::new(disk);
        // Request same size — should be a no-op.
        resize_prodos_in_place(&mut cursor, 0, 280 * 512, &mut |_| {}).unwrap();
        cursor.seek(SeekFrom::Start(1024)).unwrap();
        let mut sector = [0u8; 512];
        cursor.read_exact(&mut sector).unwrap();
        let total = u16::from_le_bytes([sector[33], sector[34]]);
        assert_eq!(total, 280); // unchanged
    }

    #[test]
    fn test_validate_integrity() {
        let vk = make_volume_key_block();
        let disk = make_disk_image(vk);
        let mut cursor = Cursor::new(disk);
        let warnings = validate_prodos_integrity(&mut cursor, 0, &mut |_| {}).unwrap();
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_list_root_directory() {
        let vk = make_volume_key_block();
        let mut disk = make_disk_image(vk);

        // Write two file entries into the volume key block at offsets 43 and 82.
        // Entry 1: seedling (storage_type=1), name="HELLO", type=TXT ($04), eof=14
        {
            let eo = 1024 + 43; // block 2 + skip header entry (slot 0 = 39 bytes)
            disk[eo] = (1 << 4) | 5; // type=1 (seedling), name_len=5
            disk[eo + 1..eo + 6].copy_from_slice(b"HELLO");
            disk[eo + 16] = 0x04; // file_type = TXT
            disk[eo + 17] = 10; // key_pointer = block 10
            disk[eo + 18] = 0;
            disk[eo + 21] = 14; // eof lo byte
            disk[eo + 22] = 0;
            disk[eo + 23] = 0;
        }
        // Entry 2: subdirectory (storage_type=0xE), name="SUBDIR"
        {
            let eo = 1024 + 82; // slot 2
            disk[eo] = (0xE << 4) | 6; // type=0xE (subdir), name_len=6
            disk[eo + 1..eo + 7].copy_from_slice(b"SUBDIR");
            disk[eo + 17] = 20; // key_pointer = block 20
            disk[eo + 18] = 0;
        }

        let cursor = Cursor::new(disk);
        let mut fs = ProDosFilesystem::open(cursor, 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();

        assert_eq!(entries.len(), 2);

        let file_entry = entries.iter().find(|e| e.name == "HELLO").unwrap();
        assert!(file_entry.is_file());
        assert_eq!(file_entry.size, 14);
        assert_eq!(file_entry.location, 10);
        assert_eq!(file_entry.mode, Some(1)); // seedling

        let dir_entry = entries.iter().find(|e| e.name == "SUBDIR").unwrap();
        assert!(dir_entry.is_directory());
        assert_eq!(dir_entry.location, 20);
    }
}
