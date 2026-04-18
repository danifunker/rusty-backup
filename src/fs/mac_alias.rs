//! Classic Mac alias resolution.
//!
//! Parses the `alis` resource record stored in a file's resource fork
//! (the marker for aliases is `kIsAlias`/`0x8000` in `FInfo.fdFlags`).
//!
//! We extract just enough to show the user where an alias points:
//!   - Target filename (from the fixed portion at offset 50)
//!   - Volume name   (from the fixed portion at offset 10)
//!   - Optional absolute path from tagged record 18 (UTF-8) or 2 (MacRoman)
//!
//! We do not attempt to follow the alias; the target may no longer exist.

use byteorder::{BigEndian, ByteOrder};

/// Finder flag bit indicating the file is an alias.
pub const IS_ALIAS_FLAG: u16 = 0x8000;

/// Type code for classic HFS+ UNIX symlinks.
pub const SLNK_TYPE: &str = "slnk";
/// Creator code for classic HFS+ UNIX symlinks.
pub const RHAP_CREATOR: &str = "rhap";

/// Read a Pascal string padded to `field_len` bytes. The first byte is the
/// length; `field_len - 1` bytes of data follow. Non-ASCII bytes are passed
/// through (MacRoman ≈ ASCII for the low 128), with high bytes mapped to `?`.
fn read_pascal_string(field: &[u8]) -> String {
    if field.is_empty() {
        return String::new();
    }
    let len = field[0] as usize;
    let max = field.len().saturating_sub(1).min(len);
    let mut out = String::with_capacity(max);
    for &b in &field[1..1 + max] {
        if b < 0x80 {
            out.push(b as char);
        } else {
            out.push('?');
        }
    }
    out
}

/// Parse an `alis` resource record and return a human-readable target string
/// like `"Volume:TargetName"`, or an absolute path when one is present.
///
/// Returns `None` if the record is too short or clearly malformed.
pub fn parse_alis_record(data: &[u8]) -> Option<String> {
    // Fixed portion is 150 bytes. Older records may be shorter but always ≥ 114
    // (up through the target filename field).
    if data.len() < 114 {
        return None;
    }

    // Volume name: Pascal string in bytes 10..38 (28-byte field).
    let volume = read_pascal_string(&data[10..38]);
    // Target file/folder name: Pascal string in bytes 50..114 (64-byte field).
    let target = read_pascal_string(&data[50..114]);

    // Scan tagged records for a UTF-8 (tag 18) or Pascal (tag 2) absolute path.
    let abs_path = if data.len() > 150 {
        scan_tagged_path(&data[150..])
    } else {
        None
    };

    match (abs_path, volume.is_empty(), target.is_empty()) {
        (Some(p), _, _) if !p.is_empty() => Some(p),
        (_, true, true) => None,
        (_, true, false) => Some(target),
        (_, false, true) => Some(format!("{volume}:")),
        (_, false, false) => Some(format!("{volume}:{target}")),
    }
}

/// Walk the tag-length-value records after the fixed portion looking for an
/// absolute path.  Each record is: u16 tag, u16 length, `length` data bytes,
/// padded to even length.  Tag `0xFFFF` terminates.
fn scan_tagged_path(tags: &[u8]) -> Option<String> {
    let mut i = 0;
    while i + 4 <= tags.len() {
        let tag = BigEndian::read_u16(&tags[i..i + 2]);
        let len = BigEndian::read_u16(&tags[i + 2..i + 4]) as usize;
        if tag == 0xFFFF {
            return None;
        }
        let data_start = i + 4;
        let data_end = data_start.checked_add(len)?;
        if data_end > tags.len() {
            return None;
        }
        match tag {
            // Tag 18 — absolute path as UTF-8.
            18 => {
                if let Ok(s) = std::str::from_utf8(&tags[data_start..data_end]) {
                    let trimmed = s.trim_end_matches('\0');
                    if !trimmed.is_empty() {
                        return Some(trimmed.to_string());
                    }
                }
            }
            // Tag 2 — absolute path (Pascal-ish, MacRoman). Not length-prefixed
            // here; the record length IS the string length.
            2 => {
                let mut out = String::with_capacity(len);
                for &b in &tags[data_start..data_end] {
                    if b == 0 {
                        break;
                    } else if b < 0x80 {
                        out.push(b as char);
                    } else {
                        out.push('?');
                    }
                }
                if !out.is_empty() {
                    return Some(out);
                }
            }
            _ => {}
        }
        // Advance past data, rounding length up to even.
        let padded = (len + 1) & !1;
        i = data_start + padded;
    }
    None
}

/// Parse a resource fork and return the data bytes of the first `alis`
/// resource, if any.
///
/// Classic Mac resource fork layout:
/// - 16-byte header: data_offset(u32), map_offset(u32), data_len(u32), map_len(u32)
/// - At `map_offset`: 24 reserved bytes, then fork attrs (u16), type-list offset
///   (u16, from start of map), name-list offset (u16).
/// - At (map_offset + type_list_offset): `num_types - 1` (u16), followed by
///   an array of type entries: four-cc (4), `count-1` (u16), ref-list offset
///   (u16, from start of type list).
/// - Ref list entry (12 bytes): id (i16), name_off (i16), attrs+data_off (4,
///   the top byte is attrs and bottom 3 bytes are offset from data_offset),
///   reserved (4).
/// - At data_offset + data_off: u32 length followed by `length` data bytes.
pub fn find_alis_in_resource_fork(rsrc: &[u8]) -> Option<Vec<u8>> {
    if rsrc.len() < 16 {
        return None;
    }
    let data_off = BigEndian::read_u32(&rsrc[0..4]) as usize;
    let map_off = BigEndian::read_u32(&rsrc[4..8]) as usize;
    let _data_len = BigEndian::read_u32(&rsrc[8..12]) as usize;
    let map_len = BigEndian::read_u32(&rsrc[12..16]) as usize;

    if map_off + map_len > rsrc.len() || map_off + 30 > rsrc.len() {
        return None;
    }
    let map = &rsrc[map_off..map_off + map_len.min(rsrc.len() - map_off)];
    if map.len() < 30 {
        return None;
    }

    // Map header (28 bytes): copy of rsrc header (16) + handle (4) + file-ref (2)
    // + fork attrs (2) + type-list off (2) + name-list off (2).
    let type_list_off = BigEndian::read_u16(&map[24..26]) as usize;
    if type_list_off + 2 > map.len() {
        return None;
    }

    let num_types = BigEndian::read_u16(&map[type_list_off..type_list_off + 2]).wrapping_add(1);
    let types_start = type_list_off + 2;

    for t in 0..num_types as usize {
        let te = types_start + t * 8;
        if te + 8 > map.len() {
            return None;
        }
        let four_cc = &map[te..te + 4];
        let count = BigEndian::read_u16(&map[te + 4..te + 6]).wrapping_add(1);
        let ref_list_off = BigEndian::read_u16(&map[te + 6..te + 8]) as usize;

        if four_cc == b"alis" && count > 0 {
            // Take the first resource in the ref list.
            let re = type_list_off + ref_list_off;
            if re + 12 > map.len() {
                return None;
            }
            // Data offset is a 24-bit value at bytes 5..8 (byte 4 is attrs).
            let data_rel = ((map[re + 5] as usize) << 16)
                | ((map[re + 6] as usize) << 8)
                | (map[re + 7] as usize);
            let abs = data_off + data_rel;
            if abs + 4 > rsrc.len() {
                return None;
            }
            let res_len = BigEndian::read_u32(&rsrc[abs..abs + 4]) as usize;
            let res_start = abs + 4;
            if res_start + res_len > rsrc.len() {
                return None;
            }
            return Some(rsrc[res_start..res_start + res_len].to_vec());
        }
    }

    None
}

/// One-shot helper: given raw resource-fork bytes, return a display string
/// for the alias target, or `None` if no `alis` resource is present / parseable.
pub fn resolve_alias_target(rsrc: &[u8]) -> Option<String> {
    let alis = find_alis_in_resource_fork(rsrc)?;
    parse_alis_record(&alis)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_pascal_field(s: &str, field_len: usize) -> Vec<u8> {
        let mut out = vec![0u8; field_len];
        let bytes = s.as_bytes();
        let n = bytes.len().min(field_len - 1).min(u8::MAX as usize);
        out[0] = n as u8;
        out[1..1 + n].copy_from_slice(&bytes[..n]);
        out
    }

    fn build_alis_record(volume: &str, target: &str, extra_tags: &[u8]) -> Vec<u8> {
        let mut rec = vec![0u8; 150];
        // volume field: 10..38 (28 bytes)
        rec[10..38].copy_from_slice(&build_pascal_field(volume, 28));
        // target field: 50..114 (64 bytes)
        rec[50..114].copy_from_slice(&build_pascal_field(target, 64));
        rec.extend_from_slice(extra_tags);
        rec
    }

    #[test]
    fn parse_alis_minimal_volume_and_target() {
        let rec = build_alis_record("MyDisk", "ReadMe", &[]);
        assert_eq!(parse_alis_record(&rec).as_deref(), Some("MyDisk:ReadMe"));
    }

    #[test]
    fn parse_alis_tag_18_utf8_path_wins() {
        let mut tags = Vec::new();
        tags.extend_from_slice(&18u16.to_be_bytes()); // tag
        let path = b"/Volumes/MyDisk/Apps/MyApp.app";
        tags.extend_from_slice(&(path.len() as u16).to_be_bytes());
        tags.extend_from_slice(path);
        if path.len() % 2 != 0 {
            tags.push(0);
        }
        tags.extend_from_slice(&0xFFFFu16.to_be_bytes());
        tags.extend_from_slice(&0u16.to_be_bytes());

        let rec = build_alis_record("MyDisk", "MyApp.app", &tags);
        assert_eq!(
            parse_alis_record(&rec).as_deref(),
            Some("/Volumes/MyDisk/Apps/MyApp.app")
        );
    }

    #[test]
    fn parse_alis_rejects_short() {
        assert!(parse_alis_record(&[0u8; 50]).is_none());
    }

    #[test]
    fn parse_alis_empty_volume_and_target() {
        let rec = build_alis_record("", "", &[]);
        assert!(parse_alis_record(&rec).is_none());
    }

    #[test]
    fn find_alis_in_minimal_rsrc() {
        // Build a tiny resource fork with one `alis` resource.
        // Layout:
        //   [0..16]   header: data_off, map_off, data_len, map_len
        //   [data_off..] length-prefixed resource data
        //   [map_off..] map (30 bytes reserved + our layout)

        let alis_body = b"hello-alias";
        let res_data_start = 16;
        let res_len_prefix = 4;
        let data_section_len = res_len_prefix + alis_body.len();

        let mut buf = vec![0u8; 16];
        // Placeholder for res length + data
        buf.extend_from_slice(&(alis_body.len() as u32).to_be_bytes());
        buf.extend_from_slice(alis_body);

        let map_off = buf.len();

        // Map layout (28 bytes fixed):
        //   [0..16]  copy of rsrc header
        //   [16..20] handle to next map (reserved)
        //   [20..22] file reference number (reserved)
        //   [22..24] fork attrs
        //   [24..26] offset from map beginning to type list
        //   [26..28] offset from map beginning to name list
        let map_header_size = 28;
        let type_list_off_in_map = map_header_size;
        // Ref list immediately after the single type entry.
        let ref_list_off_from_type_list = 2 + 8; // skip num-1 and one type entry

        let mut map = vec![0u8; map_header_size];
        map[24] = (type_list_off_in_map >> 8) as u8;
        map[25] = (type_list_off_in_map & 0xFF) as u8;
        // name list offset: point past everything (unused)
        map[26] = 0xFF;
        map[27] = 0xFF;
        // num_types - 1 = 0
        map.extend_from_slice(&[0, 0]);
        // Type entry: "alis", count-1=0, ref_list_off
        map.extend_from_slice(b"alis");
        map.extend_from_slice(&[0, 0]); // count-1
        map.extend_from_slice(&(ref_list_off_from_type_list as u16).to_be_bytes());
        // Ref list entry (12 bytes): id(2), name_off(2), attrs+data_off(4), reserved(4)
        map.extend_from_slice(&[0, 0]); // id 0
        map.extend_from_slice(&[0xFF, 0xFF]); // name off = none
        let data_rel: u32 = 0; // resource is first in data section
        map.push(0); // attrs byte
        map.push(((data_rel >> 16) & 0xFF) as u8);
        map.push(((data_rel >> 8) & 0xFF) as u8);
        map.push((data_rel & 0xFF) as u8);
        map.extend_from_slice(&[0, 0, 0, 0]); // reserved

        let map_len = map.len();

        // Now fix header
        BigEndian::write_u32(&mut buf[0..4], res_data_start as u32);
        BigEndian::write_u32(&mut buf[4..8], map_off as u32);
        BigEndian::write_u32(&mut buf[8..12], data_section_len as u32);
        BigEndian::write_u32(&mut buf[12..16], map_len as u32);

        buf.extend_from_slice(&map);

        let found = find_alis_in_resource_fork(&buf).expect("alis should be found");
        assert_eq!(found, alis_body);
    }
}
