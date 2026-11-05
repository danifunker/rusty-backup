//! 16-bit word order of SGI (MIPS / 68020) disk images, shared by the volume label and EFS.

/// Which way round the 16-bit words of an image sit. Byte-swapped images come from
/// controllers that wired the data bus the other way round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SgiLabelByteOrder {
    /// Big-endian as the 68020 saw it; no fixup needed.
    Native,
    /// Every 16-bit word has its two bytes reversed.
    Swabbed,
}

impl SgiLabelByteOrder {
    /// Plain-ASCII name for logs and the inspect view.
    pub fn display_name(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Swabbed => "byte-swapped",
        }
    }
}

/// Reverse the two bytes of every 16-bit word in `buf`. A trailing odd byte is
/// left alone, which only matters for callers that read unaligned tails.
pub fn swab16_in_place(buf: &mut [u8]) {
    for pair in buf.chunks_exact_mut(2) {
        pair.swap(0, 1);
    }
}

/// Apply `order` to a freshly-read buffer so its 16-bit words read as the
/// 68020 wrote them.
pub fn apply_byte_order(order: SgiLabelByteOrder, buf: &mut [u8]) {
    if order == SgiLabelByteOrder::Swabbed {
        swab16_in_place(buf);
    }
}
