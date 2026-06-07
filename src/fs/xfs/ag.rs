//! AGF / AGI parsers. Step 5 does not consume these for browse, but the plan
//! has us read them for completeness so later steps (allocation walks,
//! integrity check) can plug straight in.

use byteorder::{BigEndian, ByteOrder};

use super::types::{XFS_AGF_MAGIC, XFS_AGI_MAGIC};
use crate::fs::filesystem::FilesystemError;

#[derive(Debug, Clone)]
pub struct XfsAgf {
    pub seqno: u32,
    pub length: u32,
    pub bno_root: u32,
    pub cnt_root: u32,
    /// rmapbt root (v5 + `XFS_SB_FEAT_RO_RMAPBT`). Lives at AGF byte
    /// offset 24 = `agf_roots[2]`. Zero on v4 / when rmapbt disabled.
    pub rmap_root: u32,
    /// rmapbt height (matches `agf_levels[2]`).
    pub rmap_level: u32,
    /// refcountbt root (v5 + `XFS_SB_FEAT_RO_REFLINK`). Lives at AGF
    /// byte offset 88. Zero on v4 / when reflink disabled.
    pub refcount_root: u32,
    /// refcountbt height. Lives at AGF byte offset 92.
    pub refcount_level: u32,
    /// AGFL circular-buffer indices + count (`agf_flfirst`/`fllast`/`flcount`).
    /// The AGFL holds pre-reserved free-list blocks that are allocated
    /// metadata, not part of the free-space btrees.
    pub flfirst: u32,
    pub fllast: u32,
    pub flcount: u32,
    pub freeblks: u32,
}

impl XfsAgf {
    pub fn parse(buf: &[u8]) -> Result<Self, FilesystemError> {
        if buf.len() < 64 {
            return Err(FilesystemError::Parse(format!(
                "AGF buffer too small: {} bytes",
                buf.len()
            )));
        }
        let magic = BigEndian::read_u32(&buf[0..4]);
        if magic != XFS_AGF_MAGIC {
            return Err(FilesystemError::Parse(format!(
                "bad AGF magic: 0x{magic:08X}"
            )));
        }
        // rmap_root + rmap_level live at offsets 24 + 36 (= agf_roots[2]
        // + agf_levels[2]) in the same v4 64-byte region. v4 keeps these
        // zero; v5 + rmapbt populates them. Reading is unconditional —
        // callers gate use on the SB feature bit.
        let rmap_root = BigEndian::read_u32(&buf[24..28]);
        let rmap_level = BigEndian::read_u32(&buf[36..40]);
        // refcount_root + refcount_level only exist in the v5 extension
        // (byte 88 + 92). Read only if the buffer is large enough; the
        // sector size on a v5 image is at minimum 512 so 96 bytes is a
        // safe lower bound.
        let (refcount_root, refcount_level) = if buf.len() >= 96 {
            (
                BigEndian::read_u32(&buf[88..92]),
                BigEndian::read_u32(&buf[92..96]),
            )
        } else {
            (0, 0)
        };
        Ok(XfsAgf {
            seqno: BigEndian::read_u32(&buf[8..12]),
            length: BigEndian::read_u32(&buf[12..16]),
            bno_root: BigEndian::read_u32(&buf[16..20]),
            cnt_root: BigEndian::read_u32(&buf[20..24]),
            rmap_root,
            rmap_level,
            refcount_root,
            refcount_level,
            flfirst: BigEndian::read_u32(&buf[40..44]),
            fllast: BigEndian::read_u32(&buf[44..48]),
            flcount: BigEndian::read_u32(&buf[48..52]),
            freeblks: BigEndian::read_u32(&buf[52..56]),
        })
    }
}

#[derive(Debug, Clone)]
pub struct XfsAgi {
    pub seqno: u32,
    pub length: u32,
    pub count: u32,
    pub root: u32,
    pub level: u32,
    pub freecount: u32,
    /// finobt root (v5 + `XFS_SB_FEAT_RO_FINOBT`). Lives at AGI byte
    /// offset 328 = right after the v5 `lsn` field, past the 256-byte
    /// unlinked[] hash chains. Zero on v4 / when finobt disabled.
    pub free_root: u32,
    /// finobt height. Lives at AGI byte offset 332.
    pub free_level: u32,
}

impl XfsAgi {
    pub fn parse(buf: &[u8]) -> Result<Self, FilesystemError> {
        if buf.len() < 32 {
            return Err(FilesystemError::Parse(format!(
                "AGI buffer too small: {} bytes",
                buf.len()
            )));
        }
        let magic = BigEndian::read_u32(&buf[0..4]);
        if magic != XFS_AGI_MAGIC {
            return Err(FilesystemError::Parse(format!(
                "bad AGI magic: 0x{magic:08X}"
            )));
        }
        // free_root / free_level only exist in the v5 extension at
        // bytes 328-336. v4 buffers are typically 128 or 256 bytes —
        // smaller than that and the FINOBT side is simply absent.
        let (free_root, free_level) = if buf.len() >= 336 {
            (
                BigEndian::read_u32(&buf[328..332]),
                BigEndian::read_u32(&buf[332..336]),
            )
        } else {
            (0, 0)
        };
        Ok(XfsAgi {
            seqno: BigEndian::read_u32(&buf[8..12]),
            length: BigEndian::read_u32(&buf[12..16]),
            count: BigEndian::read_u32(&buf[16..20]),
            root: BigEndian::read_u32(&buf[20..24]),
            level: BigEndian::read_u32(&buf[24..28]),
            freecount: BigEndian::read_u32(&buf[28..32]),
            free_root,
            free_level,
        })
    }
}
