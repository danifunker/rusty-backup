//! MacZip — Info-ZIP's Macintosh port (D. Haase), the "new" `Mac3` format.
//!
//! MacZip writes a standard PKZIP archive where each Mac file becomes a normal
//! ZIP entry (the data fork) plus a second entry for the resource fork, stored
//! under a `XtraStuf.mac/` directory component (the `ResourceMark`,
//! `"XtraStuf.mac:"` on a real Mac). Per-file Finder metadata rides in a
//! `Mac3` (`0x334D`) extra field present on both the local and central header:
//!
//! ```text
//! Mac3 extra block (id 'M','3' = 0x334D, little-endian):
//!   +0  u16  TSize        block data size = 14 + compsize (local) / 14 (central)
//!   --- block data (TSize bytes) ---
//!   +0  u32  attrsize     uncompressed size of the attribute blob
//!   +4  u16  info flags   EB_M3_FL_* (bit0 DATFRK, bit2 UNCMPR, ...)
//!   +6  u32  fdType       4 chars, native (big-endian) order
//!   +10 u32  fdCreator    4 chars
//!   +14 ..   attribute blob (LOCAL header only; absent in central)
//! ```
//!
//! Type/creator live uncompressed in the fixed part. The attribute blob (only
//! in the local header) carries fdFlags + dates + folder settings + comment,
//! and is itself `memcompress`ed unless the `UNCMPR` (0x04) bit is set:
//! `[u16 method][u32 crc][raw-deflate stream]`. Within the decompressed blob,
//! `fdFlags` is the first `u16` and the create/modify dates are at +26/+30
//! (Mac 1904-epoch seconds), matching the StuffIt/MacBinary date convention.
//!
//! Detection is content-driven: a plain disk-image-in-a-zip carries no `Mac3`
//! field, so [`is_maczip`] returns false for it (see the `.zip` overload note
//! in `docs/maczip_plus_fixes.md`).
//!
//! Format reference: the MacZip 1.06 source (`zip/macos/source/extrafld.c`
//! writer + `unzip/macos/source/macos.c` reader).

use std::io::Read;

use anyhow::{bail, Context, Result};

use super::stuffit::{crc16_arc, ForkCodec, ForkInfo, StuffItArchive, StuffItEntry};

/// The path component MacZip prepends to a resource fork's entry name
/// (`ResourceMark` is `"XtraStuf.mac:"`; in ZIP paths the separator is `/`).
const RESOURCE_MARK: &str = "XtraStuf.mac";

/// Info-ZIP `Mac3` extra-field header id (`'M','3'`, little-endian).
const EB_ID_MAC3: u16 = 0x334D;
/// Johnny Lee's old Macintosh extra-field id (`0x07c8`).
const EB_ID_JLEE: u16 = 0x07C8;
/// Attribute blob is stored uncompressed (`EB_M3_FL_UNCMPR`).
const EB_M3_FL_UNCMPR: u16 = 0x04;
/// Fixed part of a `Mac3` block before the (local-only) attribute blob.
const MAC3_FIXED_LEN: usize = 14;

// --- little-endian readers (bounds-checked) --------------------------------

fn le16(b: &[u8], off: usize) -> Option<u16> {
    b.get(off..off + 2)
        .map(|s| u16::from_le_bytes([s[0], s[1]]))
}

fn le32(b: &[u8], off: usize) -> Option<u32> {
    b.get(off..off + 4)
        .map(|s| u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
}

/// One central-directory record (no file data, no inflation).
struct CentralEntry {
    name: String,
    method: u16,
    comp_size: u32,
    uncomp_size: u32,
    local_offset: u32,
    /// Central-header extra field (carries the `Mac3` fixed part; no blob).
    extra: Vec<u8>,
    is_dir: bool,
}

/// Find the End Of Central Directory record and return `(cd_offset, count)`.
fn find_eocd(bytes: &[u8]) -> Option<(usize, usize)> {
    const EOCD_SIG: &[u8; 4] = b"PK\x05\x06";
    if bytes.len() < 22 {
        return None;
    }
    // The EOCD is within the last 22 + 65535 bytes (max comment length).
    let scan_start = bytes.len().saturating_sub(22 + 0xFFFF);
    let mut i = bytes.len() - 22;
    loop {
        if &bytes[i..i + 4] == EOCD_SIG {
            let count = le16(bytes, i + 10)? as usize;
            let cd_offset = le32(bytes, i + 16)? as usize;
            if cd_offset <= bytes.len() {
                return Some((cd_offset, count));
            }
        }
        if i == scan_start {
            return None;
        }
        i -= 1;
    }
}

/// Walk the central directory into a list of records. Errors if `bytes` is not
/// a well-formed ZIP.
fn read_central_directory(bytes: &[u8]) -> Result<Vec<CentralEntry>> {
    let (cd_offset, count) = find_eocd(bytes).context("not a ZIP archive (no EOCD record)")?;
    const CEN_SIG: &[u8; 4] = b"PK\x01\x02";
    let mut entries = Vec::with_capacity(count.min(4096));
    let mut p = cd_offset;
    while p + 46 <= bytes.len() && &bytes[p..p + 4] == CEN_SIG {
        let method = le16(bytes, p + 10).context("central header truncated")?;
        let comp_size = le32(bytes, p + 20).context("central header truncated")?;
        let uncomp_size = le32(bytes, p + 24).context("central header truncated")?;
        let name_len = le16(bytes, p + 28).context("central header truncated")? as usize;
        let extra_len = le16(bytes, p + 30).context("central header truncated")? as usize;
        let comment_len = le16(bytes, p + 32).context("central header truncated")? as usize;
        let local_offset = le32(bytes, p + 42).context("central header truncated")?;
        let name_start = p + 46;
        let extra_start = name_start + name_len;
        let comment_start = extra_start + extra_len;
        let next = comment_start + comment_len;
        if next > bytes.len() {
            bail!("central directory entry extends past end of file");
        }
        let name_raw = &bytes[name_start..extra_start];
        let name = decode_zip_name(name_raw);
        let extra = bytes[extra_start..comment_start].to_vec();
        let is_dir = name.ends_with('/');
        entries.push(CentralEntry {
            name,
            method,
            comp_size,
            uncomp_size,
            local_offset,
            extra,
            is_dir,
        });
        p = next;
    }
    Ok(entries)
}

/// Decode a ZIP entry name. MacZip stores Mac Roman → ISO-8859-1 in the public
/// name; we accept UTF-8 when valid (covers ASCII and modern writers) and fall
/// back to Latin-1 (lossless byte→char) otherwise.
fn decode_zip_name(raw: &[u8]) -> String {
    match std::str::from_utf8(raw) {
        Ok(s) => s.to_string(),
        Err(_) => raw.iter().map(|&b| b as char).collect(),
    }
}

/// Parsed `Mac3` extra-field contents for one entry.
struct Mac3 {
    info_flags: u16,
    type_code: [u8; 4],
    creator_code: [u8; 4],
    /// Attribute blob (present in local headers, empty in central headers).
    blob: Vec<u8>,
}

/// Find and parse a `Mac3` (`0x334D`) extra block within an extra-field buffer.
fn find_mac3(extra: &[u8]) -> Option<Mac3> {
    let mut i = 0usize;
    while i + 4 <= extra.len() {
        let id = u16::from_le_bytes([extra[i], extra[i + 1]]);
        let sz = u16::from_le_bytes([extra[i + 2], extra[i + 3]]) as usize;
        let data_start = i + 4;
        let data_end = data_start.checked_add(sz)?;
        if data_end > extra.len() {
            break;
        }
        if id == EB_ID_MAC3 && sz >= MAC3_FIXED_LEN {
            let data = &extra[data_start..data_end];
            let info_flags = u16::from_le_bytes([data[4], data[5]]);
            let mut type_code = [0u8; 4];
            type_code.copy_from_slice(&data[6..10]);
            let mut creator_code = [0u8; 4];
            creator_code.copy_from_slice(&data[10..14]);
            return Some(Mac3 {
                info_flags,
                type_code,
                creator_code,
                blob: data[MAC3_FIXED_LEN..].to_vec(),
            });
        }
        i = data_end;
    }
    None
}

/// Johnny Lee's old Macintosh extra field (`0x07c8`): `'JLEE'` then
/// `fdType(4) fdCreator(4) fdFlags(2 BE)`. Best-effort fallback for archives
/// from Info-ZIP's first Mac port.
fn find_jlee(extra: &[u8]) -> Option<([u8; 4], [u8; 4], u16)> {
    let mut i = 0usize;
    while i + 4 <= extra.len() {
        let id = u16::from_le_bytes([extra[i], extra[i + 1]]);
        let sz = u16::from_le_bytes([extra[i + 2], extra[i + 3]]) as usize;
        let data_start = i + 4;
        let data_end = data_start.checked_add(sz)?;
        if data_end > extra.len() {
            break;
        }
        if id == EB_ID_JLEE && sz >= 14 && &extra[data_start..data_start + 4] == b"JLEE" {
            let data = &extra[data_start..data_end];
            let mut type_code = [0u8; 4];
            type_code.copy_from_slice(&data[4..8]);
            let mut creator_code = [0u8; 4];
            creator_code.copy_from_slice(&data[8..12]);
            let flags = u16::from_be_bytes([data[12], data[13]]);
            return Some((type_code, creator_code, flags));
        }
        i = data_end;
    }
    None
}

/// Decompress a `Mac3` attribute blob to its raw form. When the `UNCMPR` bit is
/// set the blob is already raw; otherwise it's Info-ZIP `memcompress` output:
/// `[u16 method][u32 crc][stream]` (method 8 = raw deflate, 0 = stored).
fn decode_mac3_blob(blob: &[u8], info_flags: u16) -> Option<Vec<u8>> {
    if info_flags & EB_M3_FL_UNCMPR != 0 {
        return Some(blob.to_vec());
    }
    if blob.len() < 6 {
        return None;
    }
    let method = u16::from_le_bytes([blob[0], blob[1]]);
    let stream = &blob[6..]; // skip method(2) + crc(4)
    match method {
        0 => Some(stream.to_vec()),
        8 => inflate(stream).ok(),
        _ => None,
    }
}

/// Raw-deflate inflate (ZIP method 8 — no zlib/gzip wrapper).
fn inflate(stream: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    flate2::read::DeflateDecoder::new(stream)
        .read_to_end(&mut out)
        .context("inflating deflate stream")?;
    Ok(out)
}

/// Finder metadata pulled from a `Mac3` blob: fdFlags + create/modify dates.
struct Mac3Meta {
    finder_flags: u16,
    create_date: u32,
    mod_date: u32,
}

/// Read the fixed fields we surface from a decompressed `Mac3` attribute blob:
/// `fdFlags` at +0, `ioFlCrDat` at +26, `ioFlMdDat` at +30 (all little-endian).
fn mac3_meta(blob: &[u8]) -> Mac3Meta {
    Mac3Meta {
        finder_flags: le16(blob, 0).unwrap_or(0),
        create_date: le32(blob, 26).unwrap_or(0),
        mod_date: le32(blob, 30).unwrap_or(0),
    }
}

/// Quick content check: is this a MacZip archive (vs. a plain disk-image zip)?
/// True iff it's a ZIP whose central directory carries at least one `Mac3` (or
/// old `JLEE`) extra field — the signal a plain zip never has.
pub fn is_maczip(bytes: &[u8]) -> bool {
    // Cheap gate: a ZIP starts with a local-file-header or (empty) EOCD magic.
    if !(bytes.starts_with(b"PK\x03\x04") || bytes.starts_with(b"PK\x05\x06")) {
        return false;
    }
    match read_central_directory(bytes) {
        Ok(entries) => entries
            .iter()
            .any(|e| find_mac3(&e.extra).is_some() || find_jlee(&e.extra).is_some()),
        Err(_) => false,
    }
}

/// Per-file accumulator while pairing data + resource forks.
struct Merged {
    components: Vec<String>,
    name: String,
    is_dir: bool,
    type_code: [u8; 4],
    creator_code: [u8; 4],
    finder_flags: u16,
    create_date: u32,
    mod_date: u32,
    data: Option<Vec<u8>>,
    rsrc: Option<Vec<u8>>,
    /// True once metadata has been taken from the data-fork entry (which wins
    /// over the resource-fork entry's copy if both carry a `Mac3`).
    meta_from_data: bool,
}

/// Read and inflate one entry's fork bytes using the central-directory sizes
/// (authoritative even when the local header zeroes them for a data
/// descriptor), plus the local-header extra field (which carries the `Mac3`
/// attribute blob).
fn read_entry_payload<'a>(bytes: &'a [u8], e: &CentralEntry) -> Result<(Vec<u8>, &'a [u8])> {
    let lo = e.local_offset as usize;
    if lo + 30 > bytes.len() || &bytes[lo..lo + 4] != b"PK\x03\x04" {
        bail!("bad local header for {}", e.name);
    }
    let name_len = le16(bytes, lo + 26).context("local header truncated")? as usize;
    let extra_len = le16(bytes, lo + 28).context("local header truncated")? as usize;
    let extra_start = lo + 30 + name_len;
    let data_start = extra_start + extra_len;
    let data_end = data_start
        .checked_add(e.comp_size as usize)
        .context("fork length overflow")?;
    if data_end > bytes.len() || extra_start > bytes.len() {
        bail!("fork data extends past end of file for {}", e.name);
    }
    let local_extra = &bytes[extra_start..data_start];
    let raw = &bytes[data_start..data_end];
    let data = match e.method {
        0 => raw.to_vec(),
        8 => inflate(raw).with_context(|| format!("inflating {}", e.name))?,
        m => bail!("unsupported ZIP compression method {m} for {}", e.name),
    };
    if data.len() as u32 != e.uncomp_size {
        bail!(
            "size mismatch for {} ({} != {})",
            e.name,
            data.len(),
            e.uncomp_size
        );
    }
    Ok((data, local_extra))
}

/// Parse a MacZip archive into the family's [`StuffItArchive`] model. Returns
/// the decompressed-fork buffer (entries reference it with method-0/store
/// `ForkInfo`) plus the parsed directory, so the whole extract/browse path is
/// reused unchanged.
pub fn parse(bytes: &[u8]) -> Result<(Vec<u8>, StuffItArchive)> {
    let entries = read_central_directory(bytes)?;

    let mut merged: Vec<Merged> = Vec::new();
    let mut index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

    for e in &entries {
        let comps: Vec<String> = e
            .name
            .split('/')
            .filter(|c| !c.is_empty())
            .map(|c| c.to_string())
            .collect();
        if comps.is_empty() {
            continue;
        }
        let is_rsrc = comps.iter().any(|c| c == RESOURCE_MARK);
        let logical: Vec<String> = comps.into_iter().filter(|c| c != RESOURCE_MARK).collect();
        if logical.is_empty() {
            continue; // the bare "XtraStuf.mac/" container directory
        }
        let key = logical.join("/");

        if e.is_dir {
            // Emit a folder marker only for real directories (skip the
            // resource-fork container dirs, already filtered above).
            if is_rsrc {
                continue;
            }
            index.entry(key.clone()).or_insert_with(|| {
                merged.push(Merged {
                    name: logical.last().cloned().unwrap_or_default(),
                    components: logical.clone(),
                    is_dir: true,
                    type_code: [0; 4],
                    creator_code: [0; 4],
                    finder_flags: 0,
                    create_date: 0,
                    mod_date: 0,
                    data: None,
                    rsrc: None,
                    meta_from_data: false,
                });
                merged.len() - 1
            });
            continue;
        }

        let (payload, local_extra) = match read_entry_payload(bytes, e) {
            Ok(v) => v,
            Err(_) => continue, // skip an unreadable fork; keep the rest
        };
        let mac3 = find_mac3(local_extra);
        let jlee = mac3.is_none().then(|| find_jlee(local_extra)).flatten();

        let idx = *index.entry(key.clone()).or_insert_with(|| {
            merged.push(Merged {
                name: logical.last().cloned().unwrap_or_default(),
                components: logical.clone(),
                is_dir: false,
                type_code: [0; 4],
                creator_code: [0; 4],
                finder_flags: 0,
                create_date: 0,
                mod_date: 0,
                data: None,
                rsrc: None,
                meta_from_data: false,
            });
            merged.len() - 1
        });
        let m = &mut merged[idx];

        // Apply Finder metadata. The data fork wins if both forks carry it.
        if !m.meta_from_data {
            if let Some(ref info) = mac3 {
                m.type_code = info.type_code;
                m.creator_code = info.creator_code;
                if let Some(blob) = decode_mac3_blob(&info.blob, info.info_flags) {
                    let meta = mac3_meta(&blob);
                    m.finder_flags = meta.finder_flags;
                    m.create_date = meta.create_date;
                    m.mod_date = meta.mod_date;
                }
                if !is_rsrc {
                    m.meta_from_data = true;
                }
            } else if let Some((tc, cc, flags)) = jlee {
                m.type_code = tc;
                m.creator_code = cc;
                m.finder_flags = flags;
                if !is_rsrc {
                    m.meta_from_data = true;
                }
            }
        }

        if is_rsrc {
            m.rsrc = Some(payload);
        } else {
            m.data = Some(payload);
        }
    }

    // Lay every fork into one buffer; entries reference it with method-0
    // (store) ForkInfo so stuffit::decompress_fork returns them verbatim.
    let mut buf: Vec<u8> = Vec::new();
    let mut out_entries: Vec<StuffItEntry> = Vec::with_capacity(merged.len());
    for m in merged {
        if m.is_dir {
            out_entries.push(StuffItEntry {
                path: m.components,
                name: m.name,
                is_dir: true,
                type_code: [0; 4],
                creator_code: [0; 4],
                finder_flags: 0,
                create_date: 0,
                mod_date: 0,
                data: None,
                rsrc: None,
            });
            continue;
        }
        let data = m.data.unwrap_or_default();
        let rsrc = m.rsrc.unwrap_or_default();
        let data_off = buf.len() as u64;
        buf.extend_from_slice(&data);
        let rsrc_off = buf.len() as u64;
        buf.extend_from_slice(&rsrc);
        out_entries.push(StuffItEntry {
            path: m.components,
            name: m.name,
            is_dir: false,
            type_code: m.type_code,
            creator_code: m.creator_code,
            finder_flags: m.finder_flags,
            create_date: m.create_date,
            mod_date: m.mod_date,
            data: Some(store_fork(&data, data_off, crc16_arc(&data))),
            rsrc: (!rsrc.is_empty()).then(|| store_fork(&rsrc, rsrc_off, crc16_arc(&rsrc))),
        });
    }

    if out_entries.iter().all(|e| e.is_dir) {
        bail!("ZIP carries no MacZip file entries");
    }

    Ok((
        buf,
        StuffItArchive {
            entries: out_entries,
        },
    ))
}

/// A method-0 (store) [`ForkInfo`] pointing at already-decompressed bytes.
fn store_fork(data: &[u8], offset: u64, crc: u16) -> ForkInfo {
    ForkInfo {
        method: 0,
        codec: ForkCodec::StuffIt,
        encrypted: false,
        uncompressed_len: data.len() as u32,
        compressed_len: data.len() as u32,
        crc,
        crc32: 0,
        offset,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::macarchive::stuffit::decompress_fork;

    /// Build a one-entry ZIP (stored) with an optional Mac3 extra field on both
    /// headers, used to exercise the parser without the `zip` crate's writer.
    struct ZipBuilder {
        local: Vec<u8>,
        central: Vec<u8>,
        count: u16,
    }
    impl ZipBuilder {
        fn new() -> Self {
            Self {
                local: Vec::new(),
                central: Vec::new(),
                count: 0,
            }
        }
        /// Add a stored entry. `extra_local` is the local-header extra field
        /// (carrying the Mac3 blob); `extra_central` the central one.
        fn add(&mut self, name: &str, data: &[u8], extra_local: &[u8], extra_central: &[u8]) {
            let crc = crc32(data);
            let off = self.local.len() as u32;
            // Local file header.
            self.local.extend_from_slice(b"PK\x03\x04");
            self.local.extend_from_slice(&[20, 0]); // version needed
            self.local.extend_from_slice(&[0, 0]); // flags
            self.local.extend_from_slice(&[0, 0]); // method 0 (store)
            self.local.extend_from_slice(&[0, 0, 0, 0]); // time/date
            self.local.extend_from_slice(&crc.to_le_bytes());
            self.local
                .extend_from_slice(&(data.len() as u32).to_le_bytes()); // comp
            self.local
                .extend_from_slice(&(data.len() as u32).to_le_bytes()); // uncomp
            self.local
                .extend_from_slice(&(name.len() as u16).to_le_bytes());
            self.local
                .extend_from_slice(&(extra_local.len() as u16).to_le_bytes());
            self.local.extend_from_slice(name.as_bytes());
            self.local.extend_from_slice(extra_local);
            self.local.extend_from_slice(data);
            // Central directory header.
            self.central.extend_from_slice(b"PK\x01\x02");
            self.central.extend_from_slice(&[20, 0, 20, 0]); // versions
            self.central.extend_from_slice(&[0, 0]); // flags
            self.central.extend_from_slice(&[0, 0]); // method
            self.central.extend_from_slice(&[0, 0, 0, 0]); // time/date
            self.central.extend_from_slice(&crc.to_le_bytes());
            self.central
                .extend_from_slice(&(data.len() as u32).to_le_bytes());
            self.central
                .extend_from_slice(&(data.len() as u32).to_le_bytes());
            self.central
                .extend_from_slice(&(name.len() as u16).to_le_bytes());
            self.central
                .extend_from_slice(&(extra_central.len() as u16).to_le_bytes());
            self.central.extend_from_slice(&[0, 0]); // comment len
            self.central.extend_from_slice(&[0, 0]); // disk start
            self.central.extend_from_slice(&[0, 0]); // internal attrs
            self.central.extend_from_slice(&[0, 0, 0, 0]); // external attrs
            self.central.extend_from_slice(&off.to_le_bytes());
            self.central.extend_from_slice(name.as_bytes());
            self.central.extend_from_slice(extra_central);
            self.count += 1;
        }
        fn finish(self) -> Vec<u8> {
            let mut out = self.local;
            let cd_off = out.len() as u32;
            out.extend_from_slice(&self.central);
            let cd_size = out.len() as u32 - cd_off;
            out.extend_from_slice(b"PK\x05\x06");
            out.extend_from_slice(&[0, 0, 0, 0]); // disk numbers
            out.extend_from_slice(&self.count.to_le_bytes());
            out.extend_from_slice(&self.count.to_le_bytes());
            out.extend_from_slice(&cd_size.to_le_bytes());
            out.extend_from_slice(&cd_off.to_le_bytes());
            out.extend_from_slice(&[0, 0]); // comment len
            out
        }
    }

    fn crc32(data: &[u8]) -> u32 {
        // Minimal CRC-32 (IEEE) for the test ZIP headers.
        let mut crc = 0xFFFF_FFFFu32;
        for &b in data {
            crc ^= b as u32;
            for _ in 0..8 {
                crc = if crc & 1 != 0 {
                    (crc >> 1) ^ 0xEDB8_8320
                } else {
                    crc >> 1
                };
            }
        }
        !crc
    }

    /// Build a Mac3 extra field. `blob` (uncompressed, UNCMPR set) is included
    /// only when `local` is true (central headers carry just the fixed part).
    fn mac3_field(type_code: &[u8; 4], creator: &[u8; 4], blob: &[u8], local: bool) -> Vec<u8> {
        let mut data = Vec::new();
        let attrsize = if local { blob.len() as u32 } else { 0 };
        data.extend_from_slice(&attrsize.to_le_bytes()); // attrsize
        data.extend_from_slice(&EB_M3_FL_UNCMPR.to_le_bytes()); // info flags: uncompressed
        data.extend_from_slice(type_code);
        data.extend_from_slice(creator);
        if local {
            data.extend_from_slice(blob);
        }
        let mut out = Vec::new();
        out.extend_from_slice(&EB_ID_MAC3.to_le_bytes());
        out.extend_from_slice(&(data.len() as u16).to_le_bytes());
        out.extend_from_slice(&data);
        out
    }

    /// A Mac3 attribute blob with fdFlags + create/modify dates at the fixed
    /// offsets the reader pulls (others zero).
    fn mac3_blob(fd_flags: u16, create: u32, modify: u32) -> Vec<u8> {
        let mut b = vec![0u8; 40];
        b[0..2].copy_from_slice(&fd_flags.to_le_bytes());
        b[26..30].copy_from_slice(&create.to_le_bytes());
        b[30..34].copy_from_slice(&modify.to_le_bytes());
        b
    }

    #[test]
    fn pairs_forks_and_reads_finder_metadata() {
        let blob = mac3_blob(0x2000, 0x1111_1111, 0x2222_2222);
        let mut zb = ZipBuilder::new();
        // Data fork.
        zb.add(
            "Apps/Game",
            b"DATA-FORK",
            &mac3_field(b"APPL", b"Po.P", &blob, true),
            &mac3_field(b"APPL", b"Po.P", &[], false),
        );
        // Resource fork under XtraStuf.mac/.
        zb.add(
            "Apps/XtraStuf.mac/Game",
            b"RSRC-FORK",
            &mac3_field(b"APPL", b"Po.P", &[], true),
            &mac3_field(b"APPL", b"Po.P", &[], false),
        );
        let zip = zb.finish();

        assert!(is_maczip(&zip), "should detect MacZip via Mac3 field");
        let (buf, archive) = parse(&zip).expect("parse MacZip");
        // One merged file entry (forks paired), no separate XtraStuf entry.
        let files: Vec<_> = archive.entries.iter().filter(|e| !e.is_dir).collect();
        assert_eq!(files.len(), 1, "data + rsrc forks must merge to one entry");
        let f = files[0];
        assert_eq!(f.name, "Game");
        assert_eq!(f.path, vec!["Apps".to_string(), "Game".to_string()]);
        assert_eq!(&f.type_code, b"APPL");
        assert_eq!(&f.creator_code, b"Po.P");
        assert_eq!(f.finder_flags, 0x2000, "hasBundle from Mac3 blob");
        assert_eq!(f.create_date, 0x1111_1111);
        assert_eq!(f.mod_date, 0x2222_2222);
        let data = decompress_fork(&buf, f.data.as_ref().unwrap()).unwrap();
        assert_eq!(data, b"DATA-FORK");
        let rsrc = decompress_fork(&buf, f.rsrc.as_ref().unwrap()).unwrap();
        assert_eq!(rsrc, b"RSRC-FORK");
    }

    #[test]
    fn plain_disk_image_zip_is_not_maczip() {
        // A normal zip (no Mac3 extra field) must NOT be classified as MacZip,
        // so a raw disk image inside a .zip stays a disk image.
        let mut zb = ZipBuilder::new();
        zb.add("backup.img", b"\x00\x01\x02 not a mac file", &[], &[]);
        let zip = zb.finish();
        assert!(!is_maczip(&zip), "plain disk-image zip must not be MacZip");
    }

    #[test]
    fn deflated_mac3_blob_round_trips() {
        // memcompress-style blob: [method=8][crc][raw deflate].
        use flate2::{write::DeflateEncoder, Compression};
        use std::io::Write;
        let raw_blob = mac3_blob(0x0400, 7, 9); // hasCustomIcon
        let mut enc = DeflateEncoder::new(Vec::new(), Compression::default());
        enc.write_all(&raw_blob).unwrap();
        let deflated = enc.finish().unwrap();
        let mut packed = Vec::new();
        packed.extend_from_slice(&8u16.to_le_bytes()); // method 8
        packed.extend_from_slice(&0u32.to_le_bytes()); // crc (unchecked)
        packed.extend_from_slice(&deflated);

        let decoded = decode_mac3_blob(&packed, 0).expect("inflate memcompress blob");
        let meta = mac3_meta(&decoded);
        assert_eq!(meta.finder_flags, 0x0400);
        assert_eq!(meta.create_date, 7);
        assert_eq!(meta.mod_date, 9);
    }
}
