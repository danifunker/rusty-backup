//! Generic compact reader framework for Unix-like filesystems.
//!
//! Provides a streaming `Read` implementation over a virtual compacted image
//! described by a sequence of [`CompactSection`]s. Each filesystem's compact
//! reader builds a [`CompactLayout`] describing pre-built metadata regions,
//! mapped data blocks, and zero-fill gaps, then wraps it in a
//! [`CompactStreamReader`] that handles all the streaming I/O.

use std::io::{self, Read, Seek, SeekFrom};

/// A section in the virtual compacted image.
pub enum CompactSection {
    /// Pre-built data served from memory (boot sectors, rebuilt metadata, etc.)
    PreBuilt(Vec<u8>),

    /// A contiguous run of mapped blocks from the source.
    ///
    /// Each entry in `old_blocks` is the source block number. The blocks are
    /// emitted sequentially in the virtual image — `old_blocks[0]` comes first,
    /// then `old_blocks[1]`, etc. The source byte offset for block `b` is:
    /// `source_partition_offset + source_data_start + b * block_size`.
    MappedBlocks {
        /// For each new block index in this section, the old block number in the source.
        old_blocks: Vec<u64>,
    },

    /// Zero-filled region of the given byte length.
    Zeros(u64),
}

impl CompactSection {
    /// Returns the byte size of this section.
    fn size(&self, block_size: usize) -> u64 {
        match self {
            CompactSection::PreBuilt(data) => data.len() as u64,
            CompactSection::MappedBlocks { old_blocks } => {
                old_blocks.len() as u64 * block_size as u64
            }
            CompactSection::Zeros(len) => *len,
        }
    }
}

/// Describes the layout of a virtual compacted image.
pub struct CompactLayout {
    /// Ordered sequence of sections that make up the virtual image.
    pub sections: Vec<CompactSection>,
    /// Block size in bytes (e.g. 1024, 2048, or 4096 for ext2/3/4).
    pub block_size: usize,
    /// Byte offset of the data block region relative to the partition start.
    ///
    /// For ext this would typically be 0 since block numbers are absolute
    /// within the partition. For FAT this would be the offset past reserved
    /// sectors and FAT tables. Set to 0 if block numbers are partition-relative.
    pub source_data_start: u64,
    /// Absolute byte offset of the partition within the source device.
    pub source_partition_offset: u64,
}

impl CompactLayout {
    /// Total byte size of the virtual compacted image.
    pub fn total_size(&self) -> u64 {
        self.sections.iter().map(|s| s.size(self.block_size)).sum()
    }
}

/// Cached information about each section's position in the virtual image.
struct SectionSpan {
    /// Starting virtual byte offset of this section.
    start: u64,
    /// Ending virtual byte offset (exclusive).
    end: u64,
}

/// Generic streaming reader over a [`CompactLayout`].
///
/// Implements `Read` by serving bytes from pre-built buffers, mapped source
/// blocks, or zero-fill regions as described by the layout. Reads that span
/// section or block boundaries are handled transparently.
pub struct CompactStreamReader<R: Read + Seek> {
    source: R,
    layout: CompactLayout,
    spans: Vec<SectionSpan>,
    position: u64,
    total_size: u64,
    block_buf: Vec<u8>,
    /// Which source block is currently in `block_buf`, or `None`.
    cached_block: Option<u64>,
}

impl<R: Read + Seek> CompactStreamReader<R> {
    /// Create a new compact stream reader.
    pub fn new(source: R, layout: CompactLayout) -> Self {
        let total_size = layout.total_size();
        let block_size = layout.block_size;

        // Pre-compute section spans for fast lookup
        let mut spans = Vec::with_capacity(layout.sections.len());
        let mut offset = 0u64;
        for section in &layout.sections {
            let size = section.size(block_size);
            spans.push(SectionSpan {
                start: offset,
                end: offset + size,
            });
            offset += size;
        }

        Self {
            source,
            layout,
            spans,
            position: 0,
            total_size,
            block_buf: vec![0u8; block_size],
            cached_block: None,
        }
    }

    /// Returns the total size of the virtual compacted image.
    pub fn total_size(&self) -> u64 {
        self.total_size
    }

    /// Read a source block into `self.block_buf`, using the cache.
    fn load_block(&mut self, block_num: u64) -> io::Result<()> {
        if self.cached_block == Some(block_num) {
            return Ok(());
        }
        let offset = self.layout.source_partition_offset
            + self.layout.source_data_start
            + block_num * self.layout.block_size as u64;
        self.source.seek(SeekFrom::Start(offset))?;
        self.source.read_exact(&mut self.block_buf)?;
        self.cached_block = Some(block_num);
        Ok(())
    }

    /// Find which section contains the given virtual position.
    /// Returns `(section_index, offset_within_section)`.
    fn find_section(&self, pos: u64) -> Option<(usize, u64)> {
        // Linear scan is fine for typical layouts with < 10 sections.
        // Could switch to binary search if layouts grow large.
        for (i, span) in self.spans.iter().enumerate() {
            if pos < span.end {
                return Some((i, pos - span.start));
            }
        }
        None
    }
}

impl<R: Read + Seek> Read for CompactStreamReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.total_size {
            return Ok(0);
        }

        let to_read = buf.len().min((self.total_size - self.position) as usize);
        if to_read == 0 {
            return Ok(0);
        }

        let mut filled = 0;

        while filled < to_read {
            let current_pos = self.position + filled as u64;
            if current_pos >= self.total_size {
                break;
            }

            let (section_idx, offset_in_section) = match self.find_section(current_pos) {
                Some(v) => v,
                None => break,
            };

            let section = &self.layout.sections[section_idx];
            let section_size = section.size(self.layout.block_size);
            let remaining_in_section = (section_size - offset_in_section) as usize;

            match section {
                CompactSection::PreBuilt(data) => {
                    let off = offset_in_section as usize;
                    let copy_len = (to_read - filled).min(remaining_in_section);
                    buf[filled..filled + copy_len].copy_from_slice(&data[off..off + copy_len]);
                    filled += copy_len;
                }

                CompactSection::MappedBlocks { old_blocks } => {
                    let block_size = self.layout.block_size;
                    let block_idx = offset_in_section as usize / block_size;
                    let offset_in_block = offset_in_section as usize % block_size;

                    if block_idx >= old_blocks.len() {
                        // Safety: emit zeros for out-of-range blocks
                        let copy_len = (to_read - filled).min(remaining_in_section);
                        buf[filled..filled + copy_len].fill(0);
                        filled += copy_len;
                        continue;
                    }

                    let source_block = old_blocks[block_idx];
                    self.load_block(source_block)?;

                    let remaining_in_block = block_size - offset_in_block;
                    let copy_len = (to_read - filled).min(remaining_in_block);
                    buf[filled..filled + copy_len].copy_from_slice(
                        &self.block_buf[offset_in_block..offset_in_block + copy_len],
                    );
                    filled += copy_len;
                }

                CompactSection::Zeros(_) => {
                    let copy_len = (to_read - filled).min(remaining_in_section);
                    buf[filled..filled + copy_len].fill(0);
                    filled += copy_len;
                }
            }
        }

        self.position += filled as u64;
        Ok(filled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_prebuilt_section() {
        let layout = CompactLayout {
            sections: vec![CompactSection::PreBuilt(vec![0xAA; 512])],
            block_size: 512,
            source_data_start: 0,
            source_partition_offset: 0,
        };
        let source = Cursor::new(vec![0u8; 0]);
        let mut reader = CompactStreamReader::new(source, layout);
        assert_eq!(reader.total_size(), 512);
        let mut buf = vec![0u8; 512];
        reader.read_exact(&mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0xAA));
    }

    #[test]
    fn test_zeros_section() {
        let layout = CompactLayout {
            sections: vec![CompactSection::Zeros(1024)],
            block_size: 512,
            source_data_start: 0,
            source_partition_offset: 0,
        };
        let source = Cursor::new(vec![0u8; 0]);
        let mut reader = CompactStreamReader::new(source, layout);
        assert_eq!(reader.total_size(), 1024);
        let mut buf = vec![0xFFu8; 1024];
        reader.read_exact(&mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0x00));
    }

    #[test]
    fn test_mapped_blocks() {
        // Source has 4 blocks of 512 bytes each. Map blocks [3, 1] (reverse order).
        let mut source_data = vec![0u8; 2048];
        source_data[512..1024].fill(0xBB); // block 1
        source_data[1536..2048].fill(0xDD); // block 3
        let layout = CompactLayout {
            sections: vec![CompactSection::MappedBlocks {
                old_blocks: vec![3, 1],
            }],
            block_size: 512,
            source_data_start: 0,
            source_partition_offset: 0,
        };
        let mut reader = CompactStreamReader::new(Cursor::new(source_data), layout);
        assert_eq!(reader.total_size(), 1024);
        let mut buf = vec![0u8; 1024];
        reader.read_exact(&mut buf).unwrap();
        assert!(buf[..512].iter().all(|&b| b == 0xDD)); // block 3 first
        assert!(buf[512..].iter().all(|&b| b == 0xBB)); // block 1 second
    }

    #[test]
    fn test_cross_section_read() {
        // PreBuilt(4 bytes) + Zeros(4 bytes), read 8 bytes at once
        let layout = CompactLayout {
            sections: vec![
                CompactSection::PreBuilt(vec![0xFF; 4]),
                CompactSection::Zeros(4),
            ],
            block_size: 512,
            source_data_start: 0,
            source_partition_offset: 0,
        };
        let mut reader = CompactStreamReader::new(Cursor::new(vec![]), layout);
        assert_eq!(reader.total_size(), 8);
        let mut buf = vec![0u8; 8];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, &[0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn test_multi_section_layout() {
        // PreBuilt(512) + MappedBlocks([0]) + Zeros(512)
        let mut source_data = vec![0u8; 512];
        source_data.fill(0xCC);
        let layout = CompactLayout {
            sections: vec![
                CompactSection::PreBuilt(vec![0xAA; 512]),
                CompactSection::MappedBlocks {
                    old_blocks: vec![0],
                },
                CompactSection::Zeros(512),
            ],
            block_size: 512,
            source_data_start: 0,
            source_partition_offset: 0,
        };
        let mut reader = CompactStreamReader::new(Cursor::new(source_data), layout);
        assert_eq!(reader.total_size(), 1536);

        let mut buf = vec![0u8; 1536];
        reader.read_exact(&mut buf).unwrap();
        assert!(buf[..512].iter().all(|&b| b == 0xAA)); // PreBuilt
        assert!(buf[512..1024].iter().all(|&b| b == 0xCC)); // MappedBlocks
        assert!(buf[1024..].iter().all(|&b| b == 0x00)); // Zeros
    }

    #[test]
    fn test_partial_read() {
        // Read less than a full section at a time
        let layout = CompactLayout {
            sections: vec![CompactSection::PreBuilt(vec![0xAA; 1024])],
            block_size: 512,
            source_data_start: 0,
            source_partition_offset: 0,
        };
        let mut reader = CompactStreamReader::new(Cursor::new(vec![]), layout);

        // Read 100 bytes at a time
        let mut all = Vec::new();
        let mut buf = [0u8; 100];
        loop {
            let n = reader.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            all.extend_from_slice(&buf[..n]);
        }
        assert_eq!(all.len(), 1024);
        assert!(all.iter().all(|&b| b == 0xAA));
    }

    #[test]
    fn test_read_past_end() {
        let layout = CompactLayout {
            sections: vec![CompactSection::PreBuilt(vec![0xAA; 10])],
            block_size: 512,
            source_data_start: 0,
            source_partition_offset: 0,
        };
        let mut reader = CompactStreamReader::new(Cursor::new(vec![]), layout);
        let mut buf = vec![0u8; 20];
        // read_exact should fail since only 10 bytes available
        assert!(reader.read_exact(&mut buf).is_err());
    }

    #[test]
    fn test_empty_layout() {
        let layout = CompactLayout {
            sections: vec![],
            block_size: 512,
            source_data_start: 0,
            source_partition_offset: 0,
        };
        let mut reader = CompactStreamReader::new(Cursor::new(vec![]), layout);
        assert_eq!(reader.total_size(), 0);
        let mut buf = [0u8; 1];
        assert_eq!(reader.read(&mut buf).unwrap(), 0);
    }

    #[test]
    fn test_source_data_start_offset() {
        // Source data starts at byte 1024 within the partition.
        // Block 0 is at partition_offset + 1024 + 0*512 = 1024.
        let mut source_data = vec![0u8; 2048];
        source_data[1024..1536].fill(0xEE); // block 0 at source offset 1024
        let layout = CompactLayout {
            sections: vec![CompactSection::MappedBlocks {
                old_blocks: vec![0],
            }],
            block_size: 512,
            source_data_start: 1024,
            source_partition_offset: 0,
        };
        let mut reader = CompactStreamReader::new(Cursor::new(source_data), layout);
        let mut buf = vec![0u8; 512];
        reader.read_exact(&mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0xEE));
    }

    #[test]
    fn test_partition_offset() {
        // Partition starts at byte 512 in the device.
        // Block 0 at 512 + 0 + 0*512 = 512.
        let mut source_data = vec![0u8; 1024];
        source_data[512..1024].fill(0xDD);
        let layout = CompactLayout {
            sections: vec![CompactSection::MappedBlocks {
                old_blocks: vec![0],
            }],
            block_size: 512,
            source_data_start: 0,
            source_partition_offset: 512,
        };
        let mut reader = CompactStreamReader::new(Cursor::new(source_data), layout);
        let mut buf = vec![0u8; 512];
        reader.read_exact(&mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0xDD));
    }

    #[test]
    fn test_cross_block_read_in_mapped_section() {
        // Two mapped blocks, read across the block boundary
        let mut source_data = vec![0u8; 1024];
        source_data[..512].fill(0xAA); // block 0
        source_data[512..1024].fill(0xBB); // block 1
        let layout = CompactLayout {
            sections: vec![CompactSection::MappedBlocks {
                old_blocks: vec![0, 1],
            }],
            block_size: 512,
            source_data_start: 0,
            source_partition_offset: 0,
        };
        let mut reader = CompactStreamReader::new(Cursor::new(source_data), layout);

        // Read 256 bytes from block 0
        let mut buf1 = vec![0u8; 256];
        reader.read_exact(&mut buf1).unwrap();
        assert!(buf1.iter().all(|&b| b == 0xAA));

        // Read 512 bytes across the block boundary (256 from block 0 + 256 from block 1)
        let mut buf2 = vec![0u8; 512];
        reader.read_exact(&mut buf2).unwrap();
        assert!(buf2[..256].iter().all(|&b| b == 0xAA));
        assert!(buf2[256..].iter().all(|&b| b == 0xBB));

        // Read remaining 256 bytes from block 1
        let mut buf3 = vec![0u8; 256];
        reader.read_exact(&mut buf3).unwrap();
        assert!(buf3.iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn test_total_size() {
        let layout = CompactLayout {
            sections: vec![
                CompactSection::PreBuilt(vec![0; 100]),
                CompactSection::MappedBlocks {
                    old_blocks: vec![0, 1, 2],
                },
                CompactSection::Zeros(200),
            ],
            block_size: 4096,
            source_data_start: 0,
            source_partition_offset: 0,
        };
        assert_eq!(layout.total_size(), 100 + 3 * 4096 + 200);
    }

    #[test]
    fn test_block_cache_reuse() {
        // Map the same block twice — should use cache on second access
        let mut source_data = vec![0u8; 512];
        source_data.fill(0xAB);
        let layout = CompactLayout {
            sections: vec![CompactSection::MappedBlocks {
                old_blocks: vec![0, 0],
            }],
            block_size: 512,
            source_data_start: 0,
            source_partition_offset: 0,
        };
        let mut reader = CompactStreamReader::new(Cursor::new(source_data), layout);
        let mut buf = vec![0u8; 1024];
        reader.read_exact(&mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0xAB));
    }
}
