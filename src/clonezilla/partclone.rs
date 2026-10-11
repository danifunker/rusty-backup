#[cfg(feature = "rust173-polyfill")]
use crate::rust173_compat::IntIsMultipleOf as _;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt};

/// Partclone image header (v0001 and v0002).
#[derive(Debug, Clone)]
pub struct PartcloneHeader {
    /// Format version string ("0001" or "0002")
    pub version: String,
    /// Filesystem type string (e.g. "FAT16", "NTFS", "FAT32")
    pub fs_type: String,
    /// Block size in bytes
    pub block_size: u32,
    /// Total number of blocks on the device
    pub total_blocks: u64,
    /// Number of used (non-free) blocks
    pub used_blocks: u64,
    /// Checksum mode (0 = none, 1 = CRC32)
    pub checksum_mode: u8,
    /// Number of used blocks between checksums (v0002 only)
    pub blocks_per_checksum: u32,
}

impl PartcloneHeader {
    /// Compute the expanded partition size in bytes.
    pub fn partition_size(&self) -> u64 {
        self.total_blocks * self.block_size as u64
    }

    /// Compute the used data size in bytes (for minimum size estimates).
    pub fn used_size(&self) -> u64 {
        self.used_blocks * self.block_size as u64
    }
}

/// Bitmap tracking which blocks are used.
pub(crate) struct PartcloneBitmap {
    /// One bit per block (v0002) or one byte per block (v0001)
    pub(crate) data: Vec<u8>,
    pub(crate) total_blocks: u64,
    pub(crate) is_v2: bool,
}

impl PartcloneBitmap {
    pub(crate) fn is_used(&self, block_index: u64) -> bool {
        if block_index >= self.total_blocks {
            return false;
        }
        if self.is_v2 {
            // v0002: bit-packed, 1 bit per block
            let byte_idx = (block_index / 8) as usize;
            let bit_idx = (block_index % 8) as u32;
            if byte_idx >= self.data.len() {
                return false;
            }
            (self.data[byte_idx] >> bit_idx) & 1 == 1
        } else {
            // v0001: 1 byte per block
            let idx = block_index as usize;
            if idx >= self.data.len() {
                return false;
            }
            self.data[idx] != 0
        }
    }
}

/// Reads multiple split files (.aa, .ab, ...) as a single contiguous stream.
pub struct MultiPartReader {
    files: Vec<PathBuf>,
    current_index: usize,
    current_reader: Option<BufReader<File>>,
}

impl MultiPartReader {
    pub fn new(files: Vec<PathBuf>) -> Result<Self> {
        if files.is_empty() {
            bail!("no partclone files provided");
        }
        let first = File::open(&files[0])
            .with_context(|| format!("failed to open {}", files[0].display()))?;
        Ok(Self {
            files,
            current_index: 0,
            current_reader: Some(BufReader::new(first)),
        })
    }
}

impl Read for MultiPartReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if let Some(ref mut reader) = self.current_reader {
                let n = reader.read(buf)?;
                if n > 0 {
                    return Ok(n);
                }
                // Current file exhausted, try next
            } else {
                return Ok(0);
            }

            self.current_index += 1;
            if self.current_index >= self.files.len() {
                self.current_reader = None;
                return Ok(0);
            }

            let file = File::open(&self.files[self.current_index]).map_err(|e| {
                crate::compat::io_other(format!(
                    "failed to open {}: {e}",
                    self.files[self.current_index].display()
                ))
            })?;
            self.current_reader = Some(BufReader::new(file));
        }
    }
}

const PARTCLONE_MAGIC: &[u8; 16] = b"partclone-image\0";

/// Wrap the concatenated image stream in the decompressor its leading magic names.
/// Clonezilla's `-z` choices: zstd, gzip/pigz, bzip2, xz, lz4, or `.uncomp` (none).
fn open_decompressed(multi: MultiPartReader) -> Result<Box<dyn Read + Send>> {
    let mut buffered = BufReader::with_capacity(1 << 20, multi);
    let head = io::BufRead::fill_buf(&mut buffered)
        .context("failed to read partclone image")?
        .to_vec();
    let starts = |m: &[u8]| head.starts_with(m);
    Ok(if starts(b"partclone-image") {
        Box::new(buffered)
    } else if starts(&[0x28, 0xB5, 0x2F, 0xFD]) {
        crate::rbformats::zstd_compat::decoder(buffered)
            .context("failed to create zstd decoder for partclone")?
    } else if starts(&[0x1F, 0x8B]) {
        Box::new(flate2::read::MultiGzDecoder::new(buffered))
    } else if starts(b"BZh") {
        Box::new(bzip2::read::MultiBzDecoder::new(buffered))
    } else if starts(&[0x04, 0x22, 0x4D, 0x18]) {
        Box::new(lz4_flex::frame::FrameDecoder::new(buffered))
    } else if starts(&[0xFD, b'7', b'z', b'X', b'Z', 0x00]) {
        Box::new(XzPipeReader::spawn(buffered))
    } else if starts(&[0x89, b'L', b'Z', b'O']) {
        bail!("lzop-compressed Clonezilla images are not supported; re-save with -z1p (gzip) or -z9p (zstd)")
    } else {
        bail!(
            "unrecognized partclone image compression (leading bytes {:02X?})",
            &head[..head.len().min(8)]
        )
    })
}

/// Streams an xz image by running `lzma_rs`'s push-style decoder on a worker thread.
struct XzPipeReader {
    rx: std::sync::mpsc::Receiver<io::Result<Vec<u8>>>,
    chunk: Vec<u8>,
    pos: usize,
}

struct ChannelWriter(std::sync::mpsc::SyncSender<io::Result<Vec<u8>>>);

impl io::Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .send(Ok(buf.to_vec()))
            .map_err(|_| crate::compat::io_other("xz reader dropped"))?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl XzPipeReader {
    fn spawn<R: io::BufRead + Send + 'static>(mut src: R) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel(16);
        std::thread::spawn(move || {
            let mut out = io::BufWriter::with_capacity(1 << 20, ChannelWriter(tx.clone()));
            let res = lzma_rs::xz_decompress(&mut src, &mut out)
                .map_err(|e| crate::compat::io_other(format!("xz decode failed: {e}")))
                .and_then(|_| io::Write::flush(&mut out));
            if let Err(e) = res {
                let _ = tx.send(Err(e));
            }
        });
        Self {
            rx,
            chunk: Vec::new(),
            pos: 0,
        }
    }
}

impl Read for XzPipeReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while self.pos >= self.chunk.len() {
            match self.rx.recv() {
                Ok(Ok(c)) => {
                    self.chunk = c;
                    self.pos = 0;
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => return Ok(0),
            }
        }
        let n = buf.len().min(self.chunk.len() - self.pos);
        buf[..n].copy_from_slice(&self.chunk[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// Parse a partclone v0002 image descriptor from a reader.
///
/// The on-disk layout (110 bytes total) consists of three packed structs + CRC:
///   image_head_v2 (36 bytes):
///     magic[16] + ptc_version[14] + version[4] + endianess(u16)
///   file_system_info_v2 (52 bytes):
///     fs[16] + device_size(u64) + totalblock(u64) + superBlockUsedBlocks(u64)
///     + usedblocks(u64) + block_size(u32)
///   image_options_v2 (18 bytes):
///     feature_size(u32) + image_version(u16) + cpu_bits(u16) + checksum_mode(u16)
///     + checksum_size(u16) + blocks_per_checksum(u32) + reseed_checksum(u8)
///     + bitmap_mode(u8)
///   crc32(u32)
pub(crate) fn parse_header(reader: &mut impl Read) -> Result<PartcloneHeader> {
    // --- image_head_v2 (36 bytes) ---

    // Magic: 16 bytes (IMAGE_MAGIC_SIZE + 1)
    let mut magic = [0u8; 16];
    reader
        .read_exact(&mut magic)
        .context("failed to read partclone magic")?;
    if &magic != PARTCLONE_MAGIC {
        bail!(
            "not a partclone image (magic: {:?})",
            String::from_utf8_lossy(&magic)
        );
    }

    // Partclone version: 14 bytes (null-padded, e.g. "0.3.32\0...")
    let mut _ptc_version = [0u8; 14];
    reader.read_exact(&mut _ptc_version)?;

    // Format version: 4 bytes (e.g. "0002")
    let mut fmt_version = [0u8; 4];
    reader.read_exact(&mut fmt_version)?;
    let version = String::from_utf8_lossy(&fmt_version).to_string();

    // Endianess: 2 bytes (0xC0DE = little-endian, 0xDEC0 = big-endian)
    let mut _endian = [0u8; 2];
    reader.read_exact(&mut _endian)?;

    // --- file_system_info_v2 (52 bytes) ---

    // Filesystem type: 16 bytes (FS_MAGIC_SIZE + 1, null-padded)
    let mut fs_buf = [0u8; 16];
    reader.read_exact(&mut fs_buf)?;
    let fs_type = String::from_utf8_lossy(&fs_buf)
        .trim_end_matches('\0')
        .to_string();

    // Device size: 8 bytes LE (total device/partition size in bytes)
    let _device_size = reader.read_u64::<LittleEndian>()?;

    // Total blocks: 8 bytes LE
    let total_blocks = reader.read_u64::<LittleEndian>()?;

    // Super block used blocks: 8 bytes LE (from filesystem superblock metadata)
    let _sb_used_blocks = reader.read_u64::<LittleEndian>()?;

    // Used blocks: 8 bytes LE (actual used blocks per partclone bitmap analysis)
    let used_blocks = reader.read_u64::<LittleEndian>()?;

    // Block size: 4 bytes LE
    let block_size = reader.read_u32::<LittleEndian>()?;

    // --- image_options_v2 (18 bytes) ---

    // Feature size: 4 bytes LE (size of this options struct in bytes)
    let _feature_size = reader.read_u32::<LittleEndian>()?;

    // Image version: 2 bytes LE
    let _image_version = reader.read_u16::<LittleEndian>()?;

    // CPU bits: 2 bytes LE (32 or 64)
    let _cpu_bits = reader.read_u16::<LittleEndian>()?;

    // Checksum mode: 2 bytes LE
    let checksum_mode = reader.read_u16::<LittleEndian>()? as u8;

    // Checksum size: 2 bytes LE (0 = none, 4 = CRC32)
    let _checksum_size = reader.read_u16::<LittleEndian>()?;

    // Blocks per checksum: 4 bytes LE
    let blocks_per_checksum = reader.read_u32::<LittleEndian>()?;

    // Reseed checksum: 1 byte
    let mut _reseed = [0u8; 1];
    reader.read_exact(&mut _reseed)?;

    // Bitmap mode: 1 byte (0=none, 1=bit-packed, 2=byte-per-block)
    let mut bitmap_mode_buf = [0u8; 1];
    reader.read_exact(&mut bitmap_mode_buf)?;

    // --- CRC32 of the descriptor (4 bytes) ---
    let mut _crc = [0u8; 4];
    reader.read_exact(&mut _crc)?;

    Ok(PartcloneHeader {
        version,
        fs_type,
        block_size,
        total_blocks,
        used_blocks,
        checksum_mode,
        blocks_per_checksum,
    })
}

/// Parse the bitmap from a partclone stream (after the header).
///
/// For v0002 images: bit-packed bitmap (1 bit per block) + CRC32.
/// For v0001 images: byte-per-block bitmap + "BiTmAgIc" signature.
pub(crate) fn parse_bitmap(
    reader: &mut impl Read,
    header: &PartcloneHeader,
) -> Result<PartcloneBitmap> {
    if header.version.as_str() >= "0002" {
        // v0002: bit-packed bitmap, ceil(total_blocks / 8) bytes
        let bitmap_bytes = header.total_blocks.div_ceil(8) as usize;
        let mut data = vec![0u8; bitmap_bytes];
        reader.read_exact(&mut data).with_context(|| {
            format!(
                "failed to read partclone bitmap ({} bytes for {} blocks)",
                bitmap_bytes, header.total_blocks
            )
        })?;

        // CRC32 after bitmap
        let mut _crc = [0u8; 4];
        reader.read_exact(&mut _crc)?;

        Ok(PartcloneBitmap {
            data,
            total_blocks: header.total_blocks,
            is_v2: true,
        })
    } else {
        // v0001: 1 byte per block + "BiTmAgIc" signature
        let bitmap_bytes = header.total_blocks as usize;
        let mut data = vec![0u8; bitmap_bytes];
        reader
            .read_exact(&mut data)
            .context("failed to read partclone bitmap (v1)")?;

        // Read and verify "BiTmAgIc" signature
        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic)?;
        if &magic != b"BiTmAgIc" {
            bail!("invalid partclone v1 bitmap magic");
        }

        Ok(PartcloneBitmap {
            data,
            total_blocks: header.total_blocks,
            is_v2: false,
        })
    }
}

/// A streaming reader that expands a partclone image back to full partition data.
///
/// Used blocks are read from the data stream; unused blocks become zero-filled.
/// CRC32 checksums between blocks are skipped.
pub struct PartcloneExpandReader<R: Read> {
    reader: R,
    header: PartcloneHeader,
    bitmap: PartcloneBitmap,
    /// Current block index
    current_block: u64,
    /// Offset within current block
    offset_in_block: u32,
    /// Buffer for the current block
    block_buf: Vec<u8>,
    /// Whether current block is loaded
    block_loaded: bool,
    /// Count of used blocks read so far (for checksum skipping)
    used_blocks_read: u64,
    /// Whether we're done
    finished: bool,
}

impl<R: Read> PartcloneExpandReader<R> {
    fn new(reader: R, header: PartcloneHeader, bitmap: PartcloneBitmap) -> Self {
        let block_size = header.block_size as usize;
        Self {
            reader,
            block_buf: vec![0u8; block_size],
            header,
            bitmap,
            current_block: 0,
            offset_in_block: 0,
            block_loaded: false,
            used_blocks_read: 0,
            finished: false,
        }
    }

    fn load_current_block(&mut self) -> io::Result<()> {
        if self.block_loaded {
            return Ok(());
        }

        let block_size = self.header.block_size as usize;

        if self.bitmap.is_used(self.current_block) {
            // Read used block from data stream
            self.reader.read_exact(&mut self.block_buf[..block_size])?;
            self.used_blocks_read += 1;

            // Skip CRC32 checksum after every blocks_per_checksum used blocks
            if self.header.checksum_mode > 0
                && self.header.blocks_per_checksum > 0
                && self
                    .used_blocks_read
                    .is_multiple_of(self.header.blocks_per_checksum as u64)
            {
                let mut _crc = [0u8; 4];
                self.reader.read_exact(&mut _crc)?;
            }
        } else {
            // Unused block: zeros
            self.block_buf[..block_size].fill(0);
        }

        self.block_loaded = true;
        Ok(())
    }
}

impl<R: Read> Read for PartcloneExpandReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.finished {
            return Ok(0);
        }

        let mut total_read = 0;
        let block_size = self.header.block_size;

        while total_read < buf.len() {
            if self.current_block >= self.header.total_blocks {
                self.finished = true;
                break;
            }

            self.load_current_block()?;

            let available = block_size - self.offset_in_block;
            let to_copy = (buf.len() - total_read).min(available as usize);
            let start = self.offset_in_block as usize;
            buf[total_read..total_read + to_copy]
                .copy_from_slice(&self.block_buf[start..start + to_copy]);
            total_read += to_copy;
            self.offset_in_block += to_copy as u32;

            if self.offset_in_block >= block_size {
                self.current_block += 1;
                self.offset_in_block = 0;
                self.block_loaded = false;
            }
        }

        Ok(total_read)
    }
}

/// A streaming reader that outputs only used blocks (compacted, no expansion).
///
/// This is used for compacted writing where we don't need to fill unused blocks with zeros.
pub struct PartcloneCompactReader<R: Read> {
    reader: R,
    header: PartcloneHeader,
    bitmap: PartcloneBitmap,
    current_block: u64,
    offset_in_block: u32,
    block_buf: Vec<u8>,
    block_loaded: bool,
    used_blocks_read: u64,
    finished: bool,
}

impl<R: Read> PartcloneCompactReader<R> {
    fn new(reader: R, header: PartcloneHeader, bitmap: PartcloneBitmap) -> Self {
        let block_size = header.block_size as usize;
        Self {
            reader,
            block_buf: vec![0u8; block_size],
            header,
            bitmap,
            current_block: 0,
            offset_in_block: 0,
            block_loaded: false,
            used_blocks_read: 0,
            finished: false,
        }
    }

    fn load_next_used_block(&mut self) -> io::Result<bool> {
        let block_size = self.header.block_size as usize;

        // Skip unused blocks
        while self.current_block < self.header.total_blocks {
            if self.bitmap.is_used(self.current_block) {
                // Read used block
                self.reader.read_exact(&mut self.block_buf[..block_size])?;
                self.used_blocks_read += 1;

                // Skip CRC32 checksum
                if self.header.checksum_mode > 0
                    && self.header.blocks_per_checksum > 0
                    && self
                        .used_blocks_read
                        .is_multiple_of(self.header.blocks_per_checksum as u64)
                {
                    let mut _crc = [0u8; 4];
                    self.reader.read_exact(&mut _crc)?;
                }

                self.block_loaded = true;
                return Ok(true);
            }
            self.current_block += 1;
        }

        Ok(false)
    }
}

impl<R: Read> Read for PartcloneCompactReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.finished {
            return Ok(0);
        }

        let mut total_read = 0;
        let block_size = self.header.block_size;

        while total_read < buf.len() {
            if !self.block_loaded && !self.load_next_used_block()? {
                self.finished = true;
                break;
            }

            let available = block_size - self.offset_in_block;
            let to_copy = (buf.len() - total_read).min(available as usize);
            let start = self.offset_in_block as usize;
            buf[total_read..total_read + to_copy]
                .copy_from_slice(&self.block_buf[start..start + to_copy]);
            total_read += to_copy;
            self.offset_in_block += to_copy as u32;

            if self.offset_in_block >= block_size {
                self.current_block += 1;
                self.offset_in_block = 0;
                self.block_loaded = false;
            }
        }

        Ok(total_read)
    }
}

/// Open a partclone image (multi-part, any Clonezilla compression) and return a reader that
/// expands unused blocks to zeros, producing the full partition image.
///
/// Chain: MultiPartReader → decompressor → PartcloneExpandReader
pub fn open_partclone_reader(files: &[PathBuf]) -> Result<(PartcloneHeader, impl Read)> {
    let mut decoder = open_decompressed(MultiPartReader::new(files.to_vec())?)?;

    let header = parse_header(&mut decoder)?;
    let bitmap = parse_bitmap(&mut decoder, &header)?;

    let expand_reader = PartcloneExpandReader::new(decoder, header.clone(), bitmap);
    Ok((header, expand_reader))
}

/// Open a partclone image and return a reader that streams only used blocks
/// (compacted, without expanding unused blocks to zeros).
pub fn open_partclone_stream(files: &[PathBuf]) -> Result<(PartcloneHeader, impl Read)> {
    let mut decoder = open_decompressed(MultiPartReader::new(files.to_vec())?)?;

    let header = parse_header(&mut decoder)?;
    let bitmap = parse_bitmap(&mut decoder, &header)?;

    let compact_reader = PartcloneCompactReader::new(decoder, header.clone(), bitmap);
    Ok((header, compact_reader))
}

/// Open a partclone image and return the header, bitmap, and a reader positioned
/// at the start of the block data stream (after header+bitmap).
///
/// Chain: MultiPartReader → decompressor (positioned after header+bitmap)
pub(crate) fn open_partclone_raw(
    files: &[PathBuf],
) -> Result<(PartcloneHeader, PartcloneBitmap, impl Read + Send)> {
    let mut decoder = open_decompressed(MultiPartReader::new(files.to_vec())?)?;

    let header = parse_header(&mut decoder)?;
    let bitmap = parse_bitmap(&mut decoder, &header)?;

    Ok((header, bitmap, decoder))
}

/// Read only the partclone header from a set of split image files,
/// without reading the full bitmap or data. Used for quick metadata extraction.
pub fn read_partclone_header(files: &[PathBuf]) -> Result<PartcloneHeader> {
    let mut decoder = open_decompressed(MultiPartReader::new(files.to_vec())?)?;
    parse_header(&mut decoder)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_v2() {
        // 16 blocks, bitmap = 2 bytes
        let data = vec![0b10101010, 0b01010101]; // blocks 1,3,5,7 and 8,10,12,14 are used
        let bitmap = PartcloneBitmap {
            data,
            total_blocks: 16,
            is_v2: true,
        };
        assert!(!bitmap.is_used(0));
        assert!(bitmap.is_used(1));
        assert!(!bitmap.is_used(2));
        assert!(bitmap.is_used(3));
        assert!(bitmap.is_used(8));
        assert!(!bitmap.is_used(9));
        assert!(bitmap.is_used(10));
    }

    #[test]
    fn test_bitmap_v1() {
        let data = vec![0, 1, 0, 1, 0]; // blocks 1 and 3 are used
        let bitmap = PartcloneBitmap {
            data,
            total_blocks: 5,
            is_v2: false,
        };
        assert!(!bitmap.is_used(0));
        assert!(bitmap.is_used(1));
        assert!(!bitmap.is_used(2));
        assert!(bitmap.is_used(3));
        assert!(!bitmap.is_used(4));
    }

    #[test]
    fn test_bitmap_out_of_range() {
        let bitmap = PartcloneBitmap {
            data: vec![0xFF],
            total_blocks: 8,
            is_v2: true,
        };
        assert!(!bitmap.is_used(8));
        assert!(!bitmap.is_used(100));
    }

    #[test]
    fn decompressor_is_picked_by_magic() {
        use std::io::Write;
        let payload: Vec<u8> = b"partclone-image\0"
            .iter()
            .copied()
            .chain((0..100_000u32).map(|i| (i % 251) as u8))
            .collect();
        let dir = tempfile::tempdir().unwrap();
        let raw = dir.path().join("sda1.ext4-ptcl-img.uncomp");
        std::fs::write(&raw, &payload).unwrap();
        let gz = dir.path().join("sda1.ext4-ptcl-img.gz.aa");
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        enc.write_all(&payload).unwrap();
        std::fs::write(&gz, enc.finish().unwrap()).unwrap();
        for path in [raw, gz] {
            let mut out = Vec::new();
            open_decompressed(MultiPartReader::new(vec![path]).unwrap())
                .unwrap()
                .read_to_end(&mut out)
                .unwrap();
            assert_eq!(out, payload);
        }
    }
}
