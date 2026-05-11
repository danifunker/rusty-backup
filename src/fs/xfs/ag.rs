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
        Ok(XfsAgf {
            seqno: BigEndian::read_u32(&buf[8..12]),
            length: BigEndian::read_u32(&buf[12..16]),
            bno_root: BigEndian::read_u32(&buf[16..20]),
            cnt_root: BigEndian::read_u32(&buf[20..24]),
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
        Ok(XfsAgi {
            seqno: BigEndian::read_u32(&buf[8..12]),
            length: BigEndian::read_u32(&buf[12..16]),
            count: BigEndian::read_u32(&buf[16..20]),
            root: BigEndian::read_u32(&buf[20..24]),
            level: BigEndian::read_u32(&buf[24..28]),
            freecount: BigEndian::read_u32(&buf[28..32]),
        })
    }
}
