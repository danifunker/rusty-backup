//! StuffIt 5 (`.sit` v5, magic "StuffIt (c)1997…") container reader.
//!
//! A different container from classic `SIT!`, but it reuses the same fork
//! compression methods (0 none, 13 LZ+Huffman, 15 Arsenic), so once parsed the
//! forks flow through [`super::stuffit::decompress_fork`]. Ported from XADMaster
//! `XADStuffIt5Parser.m`.
//!
//! Common in the wild (e.g. Apple's own Mac OS 7.1 800K disk set). Encrypted
//! archives are detected and rejected.

use anyhow::{bail, Result};
use std::collections::HashMap;

use super::stuffit::{ForkInfo, StuffItArchive, StuffItEntry};

const SIT5_MAGIC: &[u8] = b"StuffIt (c)1997";
const SIT5_ID: u32 = 0xA5A5_A5A5;

const FLAG_DIRECTORY: u8 = 0x40;
const FLAG_CRYPTED: u8 = 0x20;

/// True if `data` begins with the StuffIt 5 signature.
pub fn is_stuffit5(data: &[u8]) -> bool {
    data.len() >= SIT5_MAGIC.len() && &data[..SIT5_MAGIC.len()] == SIT5_MAGIC
}

/// A minimal big-endian cursor over the archive bytes.
struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn need(&self, n: usize) -> Result<()> {
        if self.pos + n > self.data.len() {
            bail!("StuffIt 5: unexpected end of archive at {}", self.pos);
        }
        Ok(())
    }
    fn u8(&mut self) -> Result<u8> {
        self.need(1)?;
        let v = self.data[self.pos];
        self.pos += 1;
        Ok(v)
    }
    fn u16(&mut self) -> Result<u16> {
        self.need(2)?;
        let v = u16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }
    fn u32(&mut self) -> Result<u32> {
        self.need(4)?;
        let v = u32::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }
    fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        self.need(n)?;
        let s = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn skip(&mut self, n: usize) -> Result<()> {
        self.need(n)?;
        self.pos += n;
        Ok(())
    }
    fn seek(&mut self, abs: usize) {
        self.pos = abs;
    }
}

/// Parse a StuffIt 5 archive into the shared [`StuffItArchive`] representation.
pub fn parse(data: &[u8]) -> Result<StuffItArchive> {
    if !is_stuffit5(data) {
        bail!("not a StuffIt 5 archive");
    }
    if data.len() < 100 {
        bail!("StuffIt 5: truncated header");
    }
    let version = data[82];
    if version != 5 {
        bail!("StuffIt 5: unexpected archive version {version}");
    }
    // numfiles @ 92 (u16 BE), firstoffs @ 94 (u32 BE). firstoffs is the
    // authoritative offset of the first top-level entry, so we don't need to
    // walk the optional comment/hash header sections.
    let numfiles = u16::from_be_bytes([data[92], data[93]]) as usize;
    let firstoffs = u32::from_be_bytes([data[94], data[95], data[96], data[97]]) as usize;

    let mut cur = Cursor {
        data,
        pos: firstoffs,
    };
    let mut entries = Vec::new();
    let mut dirs: HashMap<u32, Vec<String>> = HashMap::new();

    let mut total = numfiles;
    let mut done = 0usize;
    // Safety bound: never iterate more times than there are possible entries.
    let max_iters = data.len() / 48 + numfiles + 16;
    let mut iters = 0usize;

    while done < total {
        iters += 1;
        if iters > max_iters {
            bail!("StuffIt 5: entry walk did not terminate");
        }
        if cur.pos + 48 > data.len() {
            break;
        }
        let offs = cur.pos as u32;

        let headid = cur.u32()?;
        if headid != SIT5_ID {
            bail!("StuffIt 5: bad entry id {headid:#x} at {offs}");
        }
        let entry_version = cur.u8()?;
        cur.skip(1)?;
        let headersize = cur.u16()? as usize;
        let headerend = offs as usize + headersize;
        cur.skip(1)?;
        let flags = cur.u8()?;
        let creation_date = cur.u32()?;
        let modification_date = cur.u32()?;
        let _prevoffs = cur.u32()?;
        let _nextoffs = cur.u32()?;
        let diroffs = cur.u32()?;
        let namelength = cur.u16()? as usize;
        let _headercrc = cur.u16()?;
        let datalength = cur.u32()?;
        let datacomplen = cur.u32()?;
        let datacrc = cur.u16()?;
        cur.skip(2)?;

        let is_dir = flags & FLAG_DIRECTORY != 0;
        let crypted = flags & FLAG_CRYPTED != 0;

        let mut dir_numfiles = 0usize;
        let mut datamethod = 0u8;
        if is_dir {
            dir_numfiles = cur.u16()? as usize;
            if datalength == 0xffff_ffff {
                // Phantom marker entry that follows each directory; skip it.
                // (Header is exactly 48 bytes; cursor is already positioned.)
                total += 1;
                done += 1;
                continue;
            }
        } else {
            datamethod = cur.u8()?;
            let passlen = cur.u8()? as usize;
            if crypted && datalength != 0 {
                bail!("StuffIt 5: encrypted archives are not supported");
            } else if passlen != 0 {
                bail!("StuffIt 5: unexpected password data");
            }
        }

        let namedata = cur.bytes(namelength)?.to_vec();

        if cur.pos < headerend {
            let commentsize = cur.u16()? as usize;
            cur.skip(2)?;
            cur.skip(commentsize)?;
        }

        // Second block (present for both files and directories).
        let something = cur.u16()?;
        cur.skip(2)?;
        let filetype = cur.bytes(4)?.try_into().unwrap();
        let filecreator = cur.bytes(4)?.try_into().unwrap();
        let finder_flags = cur.u16()?;
        if entry_version == 1 {
            cur.skip(22)?;
        } else {
            cur.skip(18)?;
        }

        let has_resource = something & 0x01 != 0;
        let mut resourcecomplen = 0u32;
        let mut resourcelength = 0u32;
        let mut resourcecrc = 0u16;
        let mut resourcemethod = 0u8;
        if has_resource {
            resourcelength = cur.u32()?;
            resourcecomplen = cur.u32()?;
            resourcecrc = cur.u16()?;
            cur.skip(2)?;
            resourcemethod = cur.u8()?;
            let passlen = cur.u8()? as usize;
            if crypted && resourcelength != 0 {
                bail!("StuffIt 5: encrypted archives are not supported");
            } else if passlen != 0 {
                bail!("StuffIt 5: unexpected password data");
            }
        }

        let datastart = cur.pos as u64;

        let name = crate::fs::hfs::mac_roman_to_utf8(&namedata);
        let parent = dirs.get(&diroffs).cloned().unwrap_or_default();
        let mut path = parent.clone();
        path.push(name.clone());

        if is_dir {
            dirs.insert(offs, path.clone());
            entries.push(StuffItEntry {
                path,
                name,
                is_dir: true,
                type_code: [0; 4],
                creator_code: [0; 4],
                finder_flags,
                create_date: creation_date,
                mod_date: modification_date,
                data: None,
                rsrc: None,
            });
            cur.seek(datastart as usize);
            total += dir_numfiles;
        } else {
            let rsrc = if has_resource && (resourcelength > 0 || resourcecomplen > 0) {
                Some(ForkInfo {
                    method: resourcemethod,
                    encrypted: false,
                    uncompressed_len: resourcelength,
                    compressed_len: resourcecomplen,
                    crc: resourcecrc,
                    offset: datastart,
                })
            } else {
                None
            };
            let data_fork = Some(ForkInfo {
                method: datamethod,
                encrypted: false,
                uncompressed_len: datalength,
                compressed_len: datacomplen,
                crc: datacrc,
                offset: datastart + resourcecomplen as u64,
            });
            entries.push(StuffItEntry {
                path,
                name,
                is_dir: false,
                type_code: filetype,
                creator_code: filecreator,
                finder_flags,
                create_date: creation_date,
                mod_date: modification_date,
                data: data_fork,
                rsrc,
            });
            cur.seek(datastart as usize + resourcecomplen as usize + datacomplen as usize);
        }
        done += 1;
    }

    Ok(StuffItArchive { entries })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_sit5() {
        assert!(!is_stuffit5(b"SIT!......rLau"));
        assert!(parse(b"nope").is_err());
    }

    /// End-to-end against a real StuffIt 5 archive if present (decoded from its
    /// `.sit.hqx` on the fly). Validates the container parser + Arsenic codec +
    /// per-fork CRCs (decompress_fork verifies CRCs internally).
    #[test]
    fn extract_real_sit5_if_present() {
        let path = std::path::Path::new("/Users/dani/Downloads/Apple_Mac_OS_7.1_800k.sit__0.hqx");
        if !path.is_file() {
            return;
        }
        let raw = std::fs::read(path).unwrap();
        let bh = crate::fs::binhex::parse_binhex(&raw).expect("binhex decode");
        let archive = parse(&bh.data_fork).expect("sit5 parse");
        let files: Vec<_> = archive.entries.iter().filter(|e| !e.is_dir).collect();
        assert!(!files.is_empty());

        let mut decompressed = 0;
        for e in &files {
            if let Some(f) = e.data.as_ref() {
                if f.uncompressed_len == 0 {
                    continue;
                }
                let out = super::super::stuffit::decompress_fork(&bh.data_fork, f)
                    .unwrap_or_else(|err| panic!("{}: {err}", e.name));
                assert_eq!(out.len(), f.uncompressed_len as usize);
                decompressed += 1;
            }
        }
        eprintln!("SIT5: decompressed + CRC-verified {decompressed} data forks");
        assert!(decompressed > 0);
    }
}
