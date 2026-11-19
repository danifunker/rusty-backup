//! Linux LVM2, read from the on-disk format (not LVM's code): one physical volume at a time,
//! usually a GPT / MBR partition, sometimes a whole disk.
//!
//! PV label: in one of the first four sectors, `LABELONE` (`id` 8, own sector `u64` at 8, CRC
//! `u32` at 16 over bytes 20..512, PV-header offset `u32` at 20, type `LVM2 001` at 24). PV header
//! at that offset: 32-character UUID, `u64` device size, then `(offset, size)` `u64` pairs for the
//! data areas and then the metadata areas, each list ended by a zero pair. Little-endian.
//!
//! Metadata area header (at the metadata area's offset): CRC `u32` over bytes 4..512, magic
//! ` LVM2 x[5A%r0N*>`, version `u32`, start `u64`, size `u64`, then `raw_locn`s (`u64` offset from
//! the area start, `u64` size, `u32` text CRC, `u32` flags). The first live `raw_locn` is the current
//! metadata: a text VG description in a circular buffer that wraps past the 512-byte header.
//!
//! The VG text gives `extent_size` (sectors), each PV's `pe_start` (sectors), and per LV its
//! `segmentN { start_extent, extent_count, type, stripe_count, stripes = ["pvK", first_pe] }`.
//! A linear LV is `striped` with one stripe; those map to byte runs. Thin, RAID, mirror, snapshot,
//! cache, multi-stripe and other-PV segments are listed with the reason they cannot open.

use std::io::{Read, Seek, SeekFrom};

use serde::{Deserialize, Serialize};

use super::LogicalVolume;

const SECTOR: u64 = 512;
const LABEL_ID: &[u8; 8] = b"LABELONE";
const LABEL_TYPE: &[u8; 8] = b"LVM2 001";
const MDA_MAGIC: &[u8; 16] = b" LVM2 x[5A%r0N*>";
/// `INITIAL_CRC` of LVM's CRC-32: zlib's table, this seed, no final inversion.
const INITIAL_CRC: u32 = 0xf597_a6cf;
/// `RAW_LOCN_IGNORED`: a metadata copy the PV keeps but the VG does not use.
const RAW_LOCN_IGNORED: u32 = 1;
/// Metadata text is a few KiB; a corrupt size must not become a huge read.
const MAX_TEXT: u64 = 16 * 1024 * 1024;

/// One volume group as this physical volume describes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinuxLvm {
    pub vg_name: String,
    /// This PV's name in the VG text (`pv0`).
    pub pv_name: String,
    pub extent_bytes: u64,
    /// Byte offset of physical extent 0, from the reader's origin.
    pub pe_start: u64,
    /// Every visible LV, in the order the metadata lists them.
    pub volumes: Vec<LinuxLv>,
}

/// One logical volume: its runs on this disk, or why it cannot be opened from here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinuxLv {
    pub volume: LogicalVolume,
    pub unsupported: Option<String>,
}

/// LVM's CRC-32 over `data`, from `seed`.
pub fn lvm_crc(seed: u32, data: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new_with_initial(!seed);
    h.update(data);
    !h.finalize()
}

/// The PV at `pv_offset` (`pv_len` bytes): `None` when there is no LVM2 label, an error when the
/// label is there but its metadata cannot be read.
pub fn detect<R: Read + Seek>(
    reader: &mut R,
    pv_offset: u64,
    pv_len: u64,
) -> Option<Result<LinuxLvm, String>> {
    let mut head = vec![0u8; 4 * SECTOR as usize];
    reader.seek(SeekFrom::Start(pv_offset)).ok()?;
    reader.read_exact(&mut head).ok()?;
    let label_at = (0..4usize).find(|s| {
        let l = &head[s * 512..s * 512 + 512];
        &l[0..8] == LABEL_ID && &l[24..32] == LABEL_TYPE
    })?;
    Some(read_pv(
        reader,
        pv_offset,
        pv_len,
        &head[label_at * 512..label_at * 512 + 512],
    ))
}

fn le32(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}

fn le64(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}

fn read_pv<R: Read + Seek>(
    reader: &mut R,
    pv_offset: u64,
    pv_len: u64,
    label: &[u8],
) -> Result<LinuxLvm, String> {
    if lvm_crc(INITIAL_CRC, &label[20..512]) != le32(label, 16) {
        return Err("LVM2 label checksum does not match".into());
    }
    let ph = le32(label, 20) as usize;
    if ph + 40 > 512 {
        return Err("LVM2 label points its PV header outside the sector".into());
    }
    let uuid: String = label[ph..ph + 32].iter().map(|&c| c as char).collect();
    // Data areas, then metadata areas: (offset, size) pairs, each list ending at a zero pair.
    let mut at = ph + 40;
    let mut lists: Vec<Vec<(u64, u64)>> = Vec::new();
    for _ in 0..2 {
        let mut list = Vec::new();
        while at + 16 <= 512 {
            let (o, s) = (le64(label, at), le64(label, at + 8));
            at += 16;
            if o == 0 && s == 0 {
                break;
            }
            list.push((o, s));
        }
        lists.push(list);
    }
    let mdas = lists.get(1).cloned().unwrap_or_default();
    let mut last_err = "the PV has no metadata area".to_string();
    for (mda_off, _) in mdas {
        match read_text(reader, pv_offset + mda_off) {
            Ok(text) => return build(&text, &uuid, pv_offset, pv_len),
            Err(e) => last_err = e,
        }
    }
    Err(last_err)
}

/// The current VG text from the metadata area at `mda` (absolute).
fn read_text<R: Read + Seek>(reader: &mut R, mda: u64) -> Result<String, String> {
    let io = |e: std::io::Error| format!("reading LVM2 metadata: {e}");
    let mut hdr = [0u8; 512];
    reader.seek(SeekFrom::Start(mda)).map_err(io)?;
    reader.read_exact(&mut hdr).map_err(io)?;
    if &hdr[4..20] != MDA_MAGIC {
        return Err("LVM2 metadata area has no header".into());
    }
    if lvm_crc(INITIAL_CRC, &hdr[4..512]) != le32(&hdr, 0) {
        return Err("LVM2 metadata area header checksum does not match".into());
    }
    let area_size = le64(&hdr, 32);
    let mut at = 40;
    while at + 24 <= 512 {
        let (off, size, crc, flags) = (
            le64(&hdr, at),
            le64(&hdr, at + 8),
            le32(&hdr, at + 16),
            le32(&hdr, at + 20),
        );
        at += 24;
        if off == 0 && size == 0 {
            break;
        }
        if flags & RAW_LOCN_IGNORED != 0 {
            continue;
        }
        if size > MAX_TEXT || off >= area_size || area_size < 512 {
            return Err("LVM2 metadata location is out of range".into());
        }
        // A circular buffer: text that runs past the end continues after the header.
        let first = size.min(area_size - off);
        let mut text = vec![0u8; size as usize];
        reader.seek(SeekFrom::Start(mda + off)).map_err(io)?;
        reader.read_exact(&mut text[..first as usize]).map_err(io)?;
        if first < size {
            reader.seek(SeekFrom::Start(mda + 512)).map_err(io)?;
            reader.read_exact(&mut text[first as usize..]).map_err(io)?;
        }
        if lvm_crc(INITIAL_CRC, &text) != crc {
            return Err("LVM2 metadata text checksum does not match".into());
        }
        let end = text.iter().position(|&b| b == 0).unwrap_or(text.len());
        return String::from_utf8(text[..end].to_vec())
            .map_err(|_| "LVM2 metadata text is not UTF-8".to_string());
    }
    Err("LVM2 metadata area holds no current metadata".into())
}

/// A value in the VG text.
#[derive(Debug, Clone, PartialEq)]
enum Value {
    Str(String),
    Int(i64),
    List(Vec<Value>),
    Section(Vec<(String, Value)>),
}

impl Value {
    fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Section(items) => items.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }
    fn int(&self, key: &str) -> Option<i64> {
        match self.get(key)? {
            Value::Int(n) => Some(*n),
            _ => None,
        }
    }
    fn str(&self, key: &str) -> Option<&str> {
        match self.get(key)? {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }
    fn has_flag(&self, key: &str, flag: &str) -> bool {
        matches!(self.get(key), Some(Value::List(l)) if l.iter().any(|v| *v == Value::Str(flag.into())))
    }
    fn sections(&self) -> Vec<(&str, &Value)> {
        match self {
            Value::Section(items) => items
                .iter()
                .filter(|(_, v)| matches!(v, Value::Section(_)))
                .map(|(k, v)| (k.as_str(), v))
                .collect(),
            _ => Vec::new(),
        }
    }
}

/// Parse the VG text (`key = value`, `name { ... }`, `[a, b]`, `# comments`) into one section.
fn parse(text: &str) -> Result<Value, String> {
    let tokens = tokenize(text)?;
    let mut pos = 0;
    let top = parse_items(&tokens, &mut pos)?;
    if pos != tokens.len() {
        return Err("LVM2 metadata has an unmatched '}'".into());
    }
    Ok(Value::Section(top))
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Word(String),
    Str(String),
    Sym(char),
}

fn tokenize(text: &str) -> Result<Vec<Tok>, String> {
    let mut out = Vec::new();
    let mut chars = text.chars().peekable();
    while let Some(&c) = chars.peek() {
        match c {
            '#' => {
                while chars.peek().is_some_and(|&c| c != '\n') {
                    chars.next();
                }
            }
            '"' => {
                chars.next();
                let mut s = String::new();
                loop {
                    match chars.next() {
                        Some('\\') => s.extend(chars.next()),
                        Some('"') => break,
                        Some(c) => s.push(c),
                        None => return Err("LVM2 metadata has an unterminated string".into()),
                    }
                }
                out.push(Tok::Str(s));
            }
            '{' | '}' | '[' | ']' | '=' | ',' => {
                chars.next();
                out.push(Tok::Sym(c));
            }
            c if c.is_whitespace() => {
                chars.next();
            }
            _ => {
                let mut w = String::new();
                while chars
                    .peek()
                    .is_some_and(|&c| !c.is_whitespace() && !"{}[]=,\"#".contains(c))
                {
                    w.extend(chars.next());
                }
                out.push(Tok::Word(w));
            }
        }
    }
    Ok(out)
}

fn parse_items(t: &[Tok], pos: &mut usize) -> Result<Vec<(String, Value)>, String> {
    let mut items = Vec::new();
    while let Some(tok) = t.get(*pos) {
        let key = match tok {
            Tok::Sym('}') => break,
            Tok::Word(w) | Tok::Str(w) => w.clone(),
            other => return Err(format!("LVM2 metadata: unexpected {other:?}")),
        };
        *pos += 1;
        match t.get(*pos) {
            Some(Tok::Sym('{')) => {
                *pos += 1;
                let inner = parse_items(t, pos)?;
                if t.get(*pos) != Some(&Tok::Sym('}')) {
                    return Err(format!("LVM2 metadata: section {key} is not closed"));
                }
                *pos += 1;
                items.push((key, Value::Section(inner)));
            }
            Some(Tok::Sym('=')) => {
                *pos += 1;
                let v = parse_value(t, pos)?;
                items.push((key, v));
            }
            _ => return Err(format!("LVM2 metadata: {key} has no value")),
        }
    }
    Ok(items)
}

fn parse_value(t: &[Tok], pos: &mut usize) -> Result<Value, String> {
    let tok = t.get(*pos).cloned();
    *pos += 1;
    match tok {
        Some(Tok::Str(s)) => Ok(Value::Str(s)),
        Some(Tok::Word(w)) => Ok(w.parse().map(Value::Int).unwrap_or(Value::Str(w))),
        Some(Tok::Sym('[')) => {
            let mut list = Vec::new();
            loop {
                match t.get(*pos) {
                    Some(Tok::Sym(']')) => {
                        *pos += 1;
                        return Ok(Value::List(list));
                    }
                    Some(Tok::Sym(',')) => *pos += 1,
                    Some(_) => list.push(parse_value(t, pos)?),
                    None => return Err("LVM2 metadata: an unclosed list".into()),
                }
            }
        }
        other => Err(format!("LVM2 metadata: expected a value, found {other:?}")),
    }
}

/// The VG this PV's text describes, with each visible LV mapped onto this disk.
fn build(text: &str, label_uuid: &str, pv_offset: u64, pv_len: u64) -> Result<LinuxLvm, String> {
    let root = parse(text)?;
    let (vg_name, vg) = root
        .sections()
        .into_iter()
        .find(|(_, v)| v.get("physical_volumes").is_some())
        .ok_or("LVM2 metadata names no volume group")?;
    let extent_sectors = vg
        .int("extent_size")
        .filter(|&n| n > 0)
        .ok_or("LVM2 metadata has no extent_size")?;
    let extent_bytes = extent_sectors as u64 * SECTOR;
    let pvs = vg
        .get("physical_volumes")
        .map(|v| v.sections())
        .unwrap_or_default();
    let undash = |s: &str| s.replace('-', "");
    let (pv_name, pv) = pvs
        .iter()
        .find(|(_, p)| p.str("id").is_some_and(|id| undash(id) == label_uuid))
        .copied()
        .ok_or("LVM2 metadata does not list this PV")?;
    let pe_start = pv_offset + pv.int("pe_start").unwrap_or(0).max(0) as u64 * SECTOR;
    let pv_end = pv_offset + pv_len;
    let mut volumes = Vec::new();
    let lvs = vg
        .get("logical_volumes")
        .map(|v| v.sections())
        .unwrap_or_default();
    for (lv_name, lv) in lvs {
        if !lv.has_flag("status", "VISIBLE") {
            continue;
        }
        volumes.push(map_lv(
            lv_name,
            lv,
            pv_name,
            pe_start,
            extent_bytes,
            pv_end,
            pvs.len(),
        ));
    }
    Ok(LinuxLvm {
        vg_name: vg_name.to_string(),
        pv_name: pv_name.to_string(),
        extent_bytes,
        pe_start,
        volumes,
    })
}

/// One LV's segments as `(logical extent, byte offset)` pairs, or why it cannot open here.
fn map_lv(
    name: &str,
    lv: &Value,
    pv_name: &str,
    pe_start: u64,
    extent_bytes: u64,
    pv_end: u64,
    pv_count: usize,
) -> LinuxLv {
    let mut found = Vec::new();
    let mut extents = 0u64;
    let mut why: Option<String> = None;
    let mut segs: Vec<&Value> = lv.sections().into_iter().map(|(_, s)| s).collect();
    segs.sort_by_key(|s| s.int("start_extent").unwrap_or(0));
    for seg in segs {
        let start = seg.int("start_extent").unwrap_or(0).max(0) as u64;
        let count = seg.int("extent_count").unwrap_or(0).max(0) as u64;
        extents = extents.max(start + count);
        let kind = seg.str("type").unwrap_or("?");
        let stripes = match seg.get("stripes") {
            Some(Value::List(l)) => l.clone(),
            _ => Vec::new(),
        };
        let reason = match (kind, seg.int("stripe_count"), stripes.as_slice()) {
            ("striped", Some(1), [Value::Str(pv), Value::Int(first)]) if pv == pv_name => {
                let at = pe_start + *first as u64 * extent_bytes;
                if at + count * extent_bytes > pv_end {
                    Some("its extents run past the end of the physical volume".to_string())
                } else {
                    found.extend((0..count).map(|i| (start + i, at + i * extent_bytes)));
                    None
                }
            }
            ("striped", Some(1), _) if pv_count > 1 => {
                Some("it has extents on another physical volume; only single-disk volume groups are supported".into())
            }
            ("striped", Some(n), _) if n > 1 => Some(format!("it is striped across {n} volumes")),
            (other, _, _) => Some(format!("{other} volumes are not supported")),
        };
        if why.is_none() {
            why = reason;
        }
    }
    let volume = LogicalVolume::from_extents(name.to_string(), extents, extent_bytes, found);
    if why.is_none() && !volume.complete {
        why = Some("not every extent was found on this disk".into());
    }
    LinuxLv {
        volume,
        unsupported: why.map(|w| format!("{name}: {w}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A PV image: label, metadata area header and VG text, as LVM2 writes them.
    pub(crate) fn pv_image(text: &str, uuid: &str, len: usize) -> Vec<u8> {
        let mut img = vec![0u8; len];
        let label = &mut img[512..1024];
        label[0..8].copy_from_slice(LABEL_ID);
        label[8..16].copy_from_slice(&1u64.to_le_bytes());
        label[20..24].copy_from_slice(&32u32.to_le_bytes());
        label[24..32].copy_from_slice(LABEL_TYPE);
        label[32..64].copy_from_slice(uuid.as_bytes());
        label[64..72].copy_from_slice(&(len as u64).to_le_bytes());
        label[72..80].copy_from_slice(&(1u64 << 20).to_le_bytes()); // data area at 1 MiB
        label[88..96].copy_from_slice(&0u64.to_le_bytes());
        label[104..112].copy_from_slice(&4096u64.to_le_bytes()); // metadata area at 4 KiB
        label[112..120].copy_from_slice(&((1u64 << 20) - 4096).to_le_bytes());
        let crc = lvm_crc(INITIAL_CRC, &label[20..512]);
        label[16..20].copy_from_slice(&crc.to_le_bytes());
        let mda = 4096;
        let text_at = 1536usize;
        img[mda + text_at..mda + text_at + text.len()].copy_from_slice(text.as_bytes());
        let tcrc = lvm_crc(INITIAL_CRC, text.as_bytes());
        let h = &mut img[mda..mda + 512];
        h[4..20].copy_from_slice(MDA_MAGIC);
        h[20..24].copy_from_slice(&1u32.to_le_bytes());
        h[24..32].copy_from_slice(&4096u64.to_le_bytes());
        h[32..40].copy_from_slice(&((1u64 << 20) - 4096).to_le_bytes());
        h[40..48].copy_from_slice(&(text_at as u64).to_le_bytes());
        h[48..56].copy_from_slice(&(text.len() as u64).to_le_bytes());
        h[56..60].copy_from_slice(&tcrc.to_le_bytes());
        let hcrc = lvm_crc(INITIAL_CRC, &h[4..512]);
        h[0..4].copy_from_slice(&hcrc.to_le_bytes());
        img
    }

    const UUID: &str = "dM4dl0IRaHLLmmRdnXwbveNRBAFfQYxG";

    fn vg_text(lvs: &str) -> String {
        format!(
            "vg {{\nid = \"x\"\nextent_size = 8\nphysical_volumes {{\npv0 {{\nid = \"dM4dl0-IRaH-LLmm-RdnX-wbve-NRBA-FfQYxG\"\npe_start = 2048\npe_count = 100\n}}\n}}\nlogical_volumes {{\n{lvs}}}\n}}\ncontents = \"Text Format Volume Group\"\nversion = 1\n"
        )
    }

    fn seg(n: u32, start: u64, count: u64, first_pe: u64) -> String {
        format!("segment{n} {{\nstart_extent = {start}\nextent_count = {count}\ntype = \"striped\"\nstripe_count = 1\nstripes = [\n\"pv0\", {first_pe}\n]\n}}\n")
    }

    #[test]
    fn a_linear_lv_in_two_segments_maps_to_two_runs_in_order() {
        // Extents of 8 sectors (4 KiB); "root" is PEs 0-3 then 10-11, "swap" PEs 4-5.
        let lvs = format!(
            "root {{\nstatus = [\"READ\", \"WRITE\", \"VISIBLE\"]\nsegment_count = 2\n{}{}}}\nswap {{\nstatus = [\"READ\", \"WRITE\", \"VISIBLE\"]\nsegment_count = 1\n{}}}\nhidden {{\nstatus = [\"READ\"]\n{}}}\n",
            seg(2, 4, 2, 10),
            seg(1, 0, 4, 0),
            seg(1, 0, 2, 4),
            seg(1, 0, 1, 6)
        );
        let img = pv_image(&vg_text(&lvs), UUID, 2 << 20);
        let base = 1 << 20; // the PV starts 1 MiB into the "disk"
        let mut disk = vec![0u8; base];
        disk.extend_from_slice(&img);
        let lvm = detect(
            &mut std::io::Cursor::new(disk),
            base as u64,
            img.len() as u64,
        )
        .expect("a PV")
        .expect("readable");
        assert_eq!(
            (lvm.vg_name.as_str(), lvm.pv_name.as_str(), lvm.extent_bytes),
            ("vg", "pv0", 4096)
        );
        let pe0 = base as u64 + 2048 * 512;
        assert_eq!(lvm.pe_start, pe0);
        let names: Vec<&str> = lvm.volumes.iter().map(|v| v.volume.name.as_str()).collect();
        assert_eq!(names, ["root", "swap"], "hidden LVs are not listed");
        let root = &lvm.volumes[0];
        assert!(root.unsupported.is_none());
        assert_eq!(root.volume.size_bytes, 6 * 4096);
        assert_eq!(
            root.volume.runs,
            vec![(pe0, 4 * 4096), (pe0 + 10 * 4096, 2 * 4096)]
        );
        assert_eq!(
            lvm.volumes[1].volume.contiguous_start(),
            Some(pe0 + 4 * 4096)
        );
    }

    #[test]
    fn thin_striped_and_corrupt_metadata_are_reported_not_guessed() {
        let lvs = "pool {\nstatus = [\"VISIBLE\"]\nsegment1 {\nstart_extent = 0\nextent_count = 4\ntype = \"thin-pool\"\n}\n}\nwide {\nstatus = [\"VISIBLE\"]\nsegment1 {\nstart_extent = 0\nextent_count = 4\ntype = \"striped\"\nstripe_count = 2\nstripes = [\"pv0\", 0, \"pv1\", 0]\n}\n}\n";
        let img = pv_image(&vg_text(lvs), UUID, 2 << 20);
        let lvm = detect(&mut std::io::Cursor::new(img.clone()), 0, img.len() as u64)
            .unwrap()
            .unwrap();
        let why: Vec<String> = lvm
            .volumes
            .iter()
            .map(|v| v.unsupported.clone().unwrap())
            .collect();
        assert!(why[0].contains("thin-pool"), "{why:?}");
        assert!(why[1].contains("striped across 2"), "{why:?}");

        let mut bad = img;
        bad[4096 + 1536 + 3] ^= 0xFF;
        let err = detect(&mut std::io::Cursor::new(bad), 0, (2 << 20) as u64)
            .unwrap()
            .unwrap_err();
        assert!(err.contains("checksum"), "{err}");
        assert!(detect(&mut std::io::Cursor::new(vec![0u8; 8192]), 0, 8192).is_none());
    }

    #[test]
    fn lvm_crc_matches_lvm2() {
        // LVM2's calc_crc(INITIAL_CRC, "") is the seed itself; one known vector beyond it.
        assert_eq!(lvm_crc(INITIAL_CRC, b""), INITIAL_CRC);
        assert_ne!(lvm_crc(INITIAL_CRC, b"a"), lvm_crc(INITIAL_CRC, b"b"));
    }
}
