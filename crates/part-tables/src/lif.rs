//! The LIF (HP Logical Interchange Format) volume header that opens HP disks, as a probe.

use byteorder::{BigEndian, ByteOrder};

/// Volume header magic at byte 0.
pub const LIF_MAGIC: u16 = 0x8000;
/// Directory sizes seen in the wild are a handful of sectors; anything past this is not LIF.
pub const MAX_DIR_SECTORS: u32 = 4096;

/// Whether the first sector of `head` is a LIF volume header.
pub fn looks_like_lif(head: &[u8]) -> bool {
    if head.len() < 22 || BigEndian::read_u16(&head[0..2]) != LIF_MAGIC {
        return false;
    }
    let dir_start = BigEndian::read_u32(&head[8..12]);
    let dir_len = BigEndian::read_u32(&head[16..20]);
    dir_start >= 1 && (1..=MAX_DIR_SECTORS).contains(&dir_len)
}
