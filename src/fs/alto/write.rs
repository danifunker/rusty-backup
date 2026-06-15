//! Write support for the Alto Basic File System: a blank-volume formatter,
//! file add/delete, and a clone-to-new-geometry path that powers resizing.
//!
//! The model is **decompose -> edit the file list -> rebuild**: every mutation
//! reads the source's user files into [`FileImage`]s, edits that list, and
//! rebuilds a fresh, defragmented volume via [`build_disk`]. Alto packs are
//! small (a few MB at most) and live in a whole-file container (PDI / CopyDisk
//! that is rewritten wholesale anyway), so rebuilding per edit is cheap and
//! sidesteps in-place free-slot / bitmap juggling. It also means every write
//! produces a cleanly laid-out, contiguous volume.
//!
//! On-disk specifics come from the BFS sources: `BFSNewDisk.bcpl` (formatter;
//! bitmap convention **bit set = page in use**; free-page marker fileId =
//! `-1,-1,-1`; SysDir leader at VDA 1 with SN 100 + directory bit;
//! DiskDescriptor SN 101), `BFSCreate.bcpl` (leader page + `DV` entry layout),
//! and `AltoFileSys.d` / `BFS.d` (structures). Empirically cross-checked
//! against every CopyDisk pack in the CHM Xerox PARC archive.

use super::super::filesystem::FilesystemError;
use super::bfs::{parse_dir_entries, Bfs, SYSDIR_LEADER_VDA};
use super::{be16, put_be16, Disk, Geometry, Sector};

const EOF_DA: u16 = 0xffff;
const DIR_BIT: u16 = 0x8000; // SN.word1 directory flag
const SYSDIR_SN: u16 = 100;
const DD_SN: u16 = 101;
const DV_TYPE_FREE: u16 = 0;
const DV_TYPE_FILE: u16 = 1;
const FPROP_DSHAPE: u16 = 1; // leader-page property type: disk shape

const WORDS_PER_PAGE: usize = 256;
const DATA_BYTES: usize = 512;
const LABEL_BYTES: usize = 16;
const LKDHEADER: usize = 16; // KDH words preceding the bit table
const MAX_NAME_CHARS: usize = 39; // maxLengthFn in AltoFileSys.d
const DV_HEADER_WORDS: usize = 6; // lDV: type/length word + 5-word file pointer

// Leader-page (LD) byte offsets. See AltoFileSys.d.
const LD_NAME: usize = 12; // word 6: name string (length byte + chars)
const LD_LEADER_PROPS: usize = 52; // word 26: leaderProps
const LD_PROP_BEGIN_BYTE: usize = 492; // word 246 byte 0: propertyBegin
const LD_PROP_LEN_BYTE: usize = 493; // word 246 byte 1: propertyLength
const LD_DIRFP: usize = 248 * 2; // word 248: dirFp (5 words)
const LD_HINT_FA: usize = 253 * 2; // word 253: hintLastPageFa (da, page, charPos)
const LD_LEADER_PROPS_WORD: u8 = 26;
const LD_LEADER_PROPS_LEN: u8 = 210;

fn div_ceil(a: usize, b: usize) -> usize {
    a.div_ceil(b)
}

/// One data page of a file: 512 bytes of data and the count of valid bytes.
#[derive(Clone)]
pub struct DataPage {
    pub data: Vec<u8>,
    pub num_chars: u16,
}

/// A user file lifted out of (or destined for) a BFS volume, independent of
/// where its pages physically sit.
#[derive(Clone)]
pub struct FileImage {
    /// Directory name, e.g. `"Bravo.Run."`.
    pub name: String,
    pub sn_w1: u16,
    pub sn_w2: u16,
    pub version: u16,
    /// 512-byte leader page (page 0) — name, dates, properties.
    pub leader: Vec<u8>,
    pub pages: Vec<DataPage>,
}

impl FileImage {
    fn serial(&self) -> u32 {
        (((self.sn_w1 & 0x1fff) as u32) << 16) | self.sn_w2 as u32
    }
}

/// Read every **user** file (everything except SysDir and DiskDescriptor) out
/// of a volume into [`FileImage`]s, preserving names, serial numbers, versions,
/// leader pages, and page contents.
pub fn extract_files(disk: &Disk) -> Result<Vec<FileImage>, FilesystemError> {
    let dir = Bfs::new(disk).read_file_bytes(SYSDIR_LEADER_VDA, 8 * 1024 * 1024)?;
    let geom = &disk.geometry;
    let total = geom.total_sectors();
    let mut out = Vec::new();
    for e in parse_dir_entries(&dir)? {
        if e.entry_type != DV_TYPE_FILE {
            continue;
        }
        let bare = e.name.trim_end_matches('.');
        if bare.eq_ignore_ascii_case("SysDir") || bare.eq_ignore_ascii_case("DiskDescriptor") {
            continue;
        }
        let leader_sec = disk.sector(e.leader_vda).ok_or_else(|| {
            FilesystemError::InvalidData(format!("leader VDA {} out of range", e.leader_vda))
        })?;
        let leader = leader_sec.data.clone();
        let mut pages = Vec::new();
        let mut link = be16(&leader_sec.label, 0);
        let mut guard = 0usize;
        while link != EOF_DA && link != 0 {
            let vda = geom.vda_from_da(link);
            let s = disk.sector(vda).ok_or_else(|| {
                FilesystemError::InvalidData(format!("page VDA {vda} out of range"))
            })?;
            let num_chars = be16(&s.label, 6).min(DATA_BYTES as u16);
            pages.push(DataPage {
                data: s.data.clone(),
                num_chars,
            });
            link = be16(&s.label, 0);
            guard += 1;
            if guard > total {
                return Err(FilesystemError::InvalidData(
                    "page-chain loop in extract".into(),
                ));
            }
        }
        out.push(FileImage {
            name: e.name,
            sn_w1: e.sn_w1,
            sn_w2: e.sn_w2,
            version: e.version,
            leader,
            pages,
        });
    }
    Ok(out)
}

/// Format a blank BFS volume of the given geometry.
pub fn create_blank(geometry: Geometry) -> Result<Disk, FilesystemError> {
    build_disk(geometry, &[])
}

/// Resize / re-geometry a volume: rebuild every user file into a fresh volume
/// of `target` geometry (grow, shrink, or reformat). Fails with `DiskFull` if
/// the data doesn't fit the target.
pub fn clone_to(source: &Disk, target: Geometry) -> Result<Disk, FilesystemError> {
    let files = extract_files(source)?;
    build_disk(target, &files)
}

/// Add a host file to a volume, returning the rebuilt volume.
pub fn add_file(source: &Disk, name: &str, data: &[u8]) -> Result<Disk, FilesystemError> {
    let name = normalize_name(name)?;
    let mut files = extract_files(source)?;
    let bare = name.trim_end_matches('.');
    if bare.eq_ignore_ascii_case("SysDir") || bare.eq_ignore_ascii_case("DiskDescriptor") {
        return Err(FilesystemError::InvalidData(
            "that name is reserved by the filesystem".into(),
        ));
    }
    if files
        .iter()
        .any(|f| f.name.trim_end_matches('.').eq_ignore_ascii_case(bare))
    {
        return Err(FilesystemError::AlreadyExists(name));
    }
    let next_serial = files
        .iter()
        .map(FileImage::serial)
        .max()
        .unwrap_or(DD_SN as u32)
        + 1;
    let pages = data
        .chunks(DATA_BYTES)
        .map(|c| {
            let mut d = vec![0u8; DATA_BYTES];
            d[..c.len()].copy_from_slice(c);
            DataPage {
                data: d,
                num_chars: c.len() as u16,
            }
        })
        .collect();
    let mut leader = vec![0u8; DATA_BYTES];
    write_leader_name(&mut leader, &name);
    files.push(FileImage {
        name,
        sn_w1: ((next_serial >> 16) & 0x1fff) as u16,
        sn_w2: (next_serial & 0xffff) as u16,
        version: 1,
        leader,
        pages,
    });
    build_disk(source.geometry.clone(), &files)
}

/// Delete a file from a volume, returning the rebuilt volume.
pub fn delete_file(source: &Disk, name: &str) -> Result<Disk, FilesystemError> {
    let mut files = extract_files(source)?;
    let bare = name.trim_end_matches('.');
    let before = files.len();
    files.retain(|f| !f.name.trim_end_matches('.').eq_ignore_ascii_case(bare));
    if files.len() == before {
        return Err(FilesystemError::NotFound(name.to_string()));
    }
    build_disk(source.geometry.clone(), &files)
}

/// Build a complete BFS volume from a set of user files. Lays out, in order:
/// the boot page (VDA 0, reserved free); SysDir (leader at the fixed VDA 1 plus
/// data); DiskDescriptor (leader plus KDH/bit-table data); then each file
/// (leader plus data), packed contiguously.
///
/// **This is a defragmenting rebuild and does NOT preserve bootability.** Per
/// `InOutLd.asm`, the Alto OS's `OutLd` writes the running OS *memory image*
/// into the boot file's own pages, and that image embeds absolute disk
/// addresses of the live filesystem state (e.g. the `LastLdCB` disk command
/// block plus in-core file addresses). `Sys.Boot` is therefore a snapshot tied
/// to the disk's exact physical layout. Verified in the Salto emulator: a
/// rebuilt disk's boot chain loads a byte-for-byte identical image and the
/// loader still `JMP @0`s into it, but the resumed OS reads a stale (moved)
/// address and hangs. Bootability is preserved only by a *verbatim* copy
/// (e.g. backup -> restore), which keeps every page at its original VDA.
pub fn build_disk(geometry: Geometry, files: &[FileImage]) -> Result<Disk, FilesystemError> {
    let total = geometry.total_sectors();

    // --- Pass 1: sizes + VDA layout ------------------------------------
    let mut dir_words = dv_words("SysDir.") + dv_words("DiskDescriptor.");
    for f in files {
        dir_words += dv_words(&f.name);
    }
    // page-align with at least one trailing free word
    let sysdir_content_words = round_up_page(dir_words + 1);
    let sysdir_data_pages = sysdir_content_words / WORDS_PER_PAGE;

    let disk_bt_size = ((total - 1) >> 4) + 1; // words in the bit table
    let dd_content_words = LKDHEADER + disk_bt_size;
    let dd_data_pages = div_ceil(dd_content_words, WORDS_PER_PAGE);

    let mut next = 0usize;
    let mut alloc = |n: usize| {
        let s = next;
        next += n;
        s
    };
    let boot = alloc(1);
    let sysdir_leader = alloc(1);
    let sysdir_data = alloc(sysdir_data_pages);
    let dd_leader = alloc(1);
    let dd_data = alloc(dd_data_pages);
    let file_layouts: Vec<FileLayout> = files
        .iter()
        .map(|f| FileLayout {
            leader: alloc(1),
            data: (0..f.pages.len()).map(|_| alloc(1)).collect(),
        })
        .collect();
    let needed = next;
    if needed > total {
        return Err(FilesystemError::DiskFull(format!(
            "volume needs {needed} pages but the target holds {total}"
        )));
    }
    debug_assert_eq!(boot, 0);
    debug_assert_eq!(sysdir_leader, SYSDIR_LEADER_VDA);

    // --- Pass 2: write ---------------------------------------------------
    let mut sectors: Vec<Sector> = (0..total)
        .map(|_| Sector {
            label: free_label(),
            data: vec![0u8; DATA_BYTES],
        })
        .collect();
    let da = |v: usize| geometry.da_from_vda(v);

    // (a) user files
    for (f, lay) in files.iter().zip(&file_layouts) {
        let first_data = lay.data.first().map(|&v| da(v)).unwrap_or(EOF_DA);
        let (last_da, last_page, last_chars) = match lay.data.last() {
            Some(&v) => (
                da(v),
                lay.data.len() as u16,
                f.pages.last().map(|p| p.num_chars).unwrap_or(0),
            ),
            None => (EOF_DA, 0, 0),
        };
        let mut leader_data = f.leader.clone();
        leader_data.resize(DATA_BYTES, 0);
        put_be16(&mut leader_data, LD_HINT_FA, last_da);
        put_be16(&mut leader_data, LD_HINT_FA + 2, last_page);
        put_be16(&mut leader_data, LD_HINT_FA + 4, last_chars);
        sectors[lay.leader] = Sector {
            label: make_label(first_data, EOF_DA, 0, 0, f.version, f.sn_w1, f.sn_w2),
            data: leader_data,
        };
        for (i, page) in f.pages.iter().enumerate() {
            let prev = if i == 0 {
                da(lay.leader)
            } else {
                da(lay.data[i - 1])
            };
            let next = if i + 1 < lay.data.len() {
                da(lay.data[i + 1])
            } else {
                EOF_DA
            };
            let mut d = page.data.clone();
            d.resize(DATA_BYTES, 0);
            sectors[lay.data[i]] = Sector {
                label: make_label(
                    next,
                    prev,
                    page.num_chars,
                    (i + 1) as u16,
                    f.version,
                    f.sn_w1,
                    f.sn_w2,
                ),
                data: d,
            };
        }
    }

    // (b) SysDir directory content
    let mut dir = Vec::with_capacity(sysdir_content_words * 2);
    write_dv(
        &mut dir,
        "SysDir.",
        DIR_BIT,
        SYSDIR_SN,
        1,
        sysdir_leader as u16,
    );
    write_dv(&mut dir, "DiskDescriptor.", 0, DD_SN, 1, dd_leader as u16);
    for (f, lay) in files.iter().zip(&file_layouts) {
        write_dv(
            &mut dir,
            &f.name,
            f.sn_w1,
            f.sn_w2,
            f.version,
            lay.leader as u16,
        );
    }
    let mut pad = sysdir_content_words - dir.len() / 2;
    while pad > 0 {
        let chunk = pad.min(100);
        push_be16(&mut dir, (DV_TYPE_FREE << 10) | (chunk as u16 & 0x3ff));
        dir.resize(dir.len() + (chunk - 1) * 2, 0);
        pad -= chunk;
    }

    let sysdir_last = sysdir_data + sysdir_data_pages - 1;
    let sysdir_leader_data = build_leader(
        "SysDir.",
        Some(&geometry),
        [DIR_BIT, SYSDIR_SN, 1, 0, sysdir_leader as u16],
        (da(sysdir_last), sysdir_data_pages as u16, DATA_BYTES as u16),
    );
    sectors[sysdir_leader] = Sector {
        label: make_label(da(sysdir_data), EOF_DA, 0, 0, 1, DIR_BIT, SYSDIR_SN),
        data: sysdir_leader_data,
    };
    write_data_pages(
        &mut sectors,
        &geometry,
        sysdir_data,
        sysdir_data_pages,
        sysdir_leader,
        &dir,
        1,
        DIR_BIT,
        SYSDIR_SN,
    );

    // (c) DiskDescriptor: KDH + bit table
    let used: Vec<usize> = std::iter::once(boot)
        .chain(std::iter::once(sysdir_leader))
        .chain(sysdir_data..sysdir_data + sysdir_data_pages)
        .chain(std::iter::once(dd_leader))
        .chain(dd_data..dd_data + dd_data_pages)
        .chain(
            file_layouts
                .iter()
                .flat_map(|l| std::iter::once(l.leader).chain(l.data.iter().copied())),
        )
        .collect();
    let free_pages = total - used.len();
    let max_serial = files
        .iter()
        .map(FileImage::serial)
        .max()
        .unwrap_or(0)
        .max(DD_SN as u32);

    let mut dd = vec![0u8; dd_content_words * 2];
    put_be16(&mut dd, 0, geometry.n_disks);
    put_be16(&mut dd, 2, geometry.n_cylinders); // nTracks
    put_be16(&mut dd, 4, geometry.n_heads);
    put_be16(&mut dd, 6, geometry.n_sectors);
    put_be16(&mut dd, 8, ((max_serial >> 16) & 0x1fff) as u16); // lastSn.word1
    put_be16(&mut dd, 10, (max_serial & 0xffff) as u16); // lastSn.word2
    put_be16(&mut dd, 14, disk_bt_size as u16); // diskBTsize
    put_be16(&mut dd, 18, free_pages.min(0xffff) as u16); // freePages
    let bt0 = LKDHEADER * 2; // byte offset of the bit table within DD content
    let mut set_used = |v: usize| {
        let off = bt0 + (v >> 4) * 2;
        let mask = 0x8000u16 >> (v & 15);
        let cur = be16(&dd, off);
        put_be16(&mut dd, off, cur | mask);
    };
    for &v in &used {
        set_used(v);
    }
    // pages addressed by the bit table but past the end of the disk don't exist
    for v in total..(disk_bt_size * 16) {
        set_used(v);
    }

    let dd_last = dd_data + dd_data_pages - 1;
    let dd_last_chars = (dd_content_words * 2 - (dd_data_pages - 1) * DATA_BYTES) as u16;
    let dd_leader_data = build_leader(
        "DiskDescriptor.",
        None,
        [0, DD_SN, 1, 0, dd_leader as u16],
        (da(dd_last), dd_data_pages as u16, dd_last_chars),
    );
    sectors[dd_leader] = Sector {
        label: make_label(da(dd_data), EOF_DA, 0, 0, 1, 0, DD_SN),
        data: dd_leader_data,
    };
    write_data_pages(
        &mut sectors,
        &geometry,
        dd_data,
        dd_data_pages,
        dd_leader,
        &dd,
        1,
        0,
        DD_SN,
    );

    Ok(Disk { geometry, sectors })
}

struct FileLayout {
    leader: usize,
    data: Vec<usize>,
}

fn round_up_page(words: usize) -> usize {
    div_ceil(words, WORDS_PER_PAGE) * WORDS_PER_PAGE
}

/// Words occupied by one `DV` entry for `name` (header + name string).
fn dv_words(name: &str) -> usize {
    DV_HEADER_WORDS + name_words(name)
}

fn name_words(name: &str) -> usize {
    (name.len() + 2) >> 1
}

fn push_be16(buf: &mut Vec<u8>, v: u16) {
    buf.push((v >> 8) as u8);
    buf.push(v as u8);
}

fn make_label(
    next: u16,
    prev: u16,
    num_chars: u16,
    page: u16,
    version: u16,
    sn_w1: u16,
    sn_w2: u16,
) -> Vec<u8> {
    let mut l = vec![0u8; LABEL_BYTES];
    put_be16(&mut l, 0, next);
    put_be16(&mut l, 2, prev);
    put_be16(&mut l, 6, num_chars);
    put_be16(&mut l, 8, page);
    put_be16(&mut l, 10, version);
    put_be16(&mut l, 12, sn_w1);
    put_be16(&mut l, 14, sn_w2);
    l
}

/// A free page's label: fileId = `-1,-1,-1` (the free-page marker).
fn free_label() -> Vec<u8> {
    make_label(0, 0, 0, 0, 0xffff, 0xffff, 0xffff)
}

/// Append a `DV` directory entry: type/length word, 5-word file pointer, then
/// the length-prefixed name padded to whole words.
fn write_dv(buf: &mut Vec<u8>, name: &str, sn_w1: u16, sn_w2: u16, version: u16, leader_vda: u16) {
    let nw = name_words(name);
    let length = (DV_HEADER_WORDS + nw) as u16;
    push_be16(buf, (DV_TYPE_FILE << 10) | (length & 0x3ff));
    push_be16(buf, sn_w1);
    push_be16(buf, sn_w2);
    push_be16(buf, version);
    push_be16(buf, 0); // FP.blank
    push_be16(buf, leader_vda);
    let start = buf.len();
    buf.resize(start + nw * 2, 0);
    buf[start] = name.len() as u8;
    buf[start + 1..start + 1 + name.len()].copy_from_slice(name.as_bytes());
}

/// Write the data pages of a file from a flat content buffer (used for SysDir
/// and DiskDescriptor, whose pages are full except possibly the last).
#[allow(clippy::too_many_arguments)]
fn write_data_pages(
    sectors: &mut [Sector],
    geom: &Geometry,
    start: usize,
    n_pages: usize,
    leader_vda: usize,
    content: &[u8],
    version: u16,
    sn_w1: u16,
    sn_w2: u16,
) {
    let da = |v: usize| geom.da_from_vda(v);
    for i in 0..n_pages {
        let vda = start + i;
        let off = i * DATA_BYTES;
        let end = (off + DATA_BYTES).min(content.len());
        let mut d = vec![0u8; DATA_BYTES];
        if off < end {
            d[..end - off].copy_from_slice(&content[off..end]);
        }
        let prev = if i == 0 {
            da(leader_vda)
        } else {
            da(start + i - 1)
        };
        let next = if i + 1 < n_pages {
            da(start + i + 1)
        } else {
            EOF_DA
        };
        sectors[vda] = Sector {
            label: make_label(
                next,
                prev,
                (end - off) as u16,
                (i + 1) as u16,
                version,
                sn_w1,
                sn_w2,
            ),
            data: d,
        };
    }
}

fn write_leader_name(leader: &mut [u8], name: &str) {
    leader[LD_NAME] = name.len() as u8;
    leader[LD_NAME + 1..LD_NAME + 1 + name.len()].copy_from_slice(name.as_bytes());
    leader[LD_PROP_BEGIN_BYTE] = LD_LEADER_PROPS_WORD;
    leader[LD_PROP_LEN_BYTE] = LD_LEADER_PROPS_LEN;
}

/// Build a leader page for SysDir / DiskDescriptor: name, property header, an
/// optional disk-shape property, the directory file-pointer hint, and the
/// end-of-file hint.
fn build_leader(
    name: &str,
    dshape: Option<&Geometry>,
    dir_fp: [u16; 5],
    hint: (u16, u16, u16),
) -> Vec<u8> {
    let mut d = vec![0u8; DATA_BYTES];
    write_leader_name(&mut d, name);
    if let Some(g) = dshape {
        // FPROP word: (type << 8) | length; DSHAPE = 4 words, length = 5.
        put_be16(&mut d, LD_LEADER_PROPS, (FPROP_DSHAPE << 8) | 5);
        put_be16(&mut d, LD_LEADER_PROPS + 2, g.n_disks);
        put_be16(&mut d, LD_LEADER_PROPS + 4, g.n_cylinders);
        put_be16(&mut d, LD_LEADER_PROPS + 6, g.n_heads);
        put_be16(&mut d, LD_LEADER_PROPS + 8, g.n_sectors);
    }
    for (i, &w) in dir_fp.iter().enumerate() {
        put_be16(&mut d, LD_DIRFP + i * 2, w);
    }
    put_be16(&mut d, LD_HINT_FA, hint.0);
    put_be16(&mut d, LD_HINT_FA + 2, hint.1);
    put_be16(&mut d, LD_HINT_FA + 4, hint.2);
    d
}

/// Validate / normalize a filename for `add_file`: printable ASCII, not too
/// long, with a trailing `.` per Alto convention.
fn normalize_name(name: &str) -> Result<String, FilesystemError> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(FilesystemError::InvalidData("name cannot be empty".into()));
    }
    if !trimmed.bytes().all(|b| (0x21..0x7f).contains(&b)) {
        return Err(FilesystemError::InvalidData(
            "name must be printable ASCII with no spaces".into(),
        ));
    }
    let with_dot = if trimmed.ends_with('.') {
        trimmed.to_string()
    } else {
        format!("{trimmed}.")
    };
    if with_dot.len() > MAX_NAME_CHARS {
        return Err(FilesystemError::InvalidData(format!(
            "name longer than {MAX_NAME_CHARS} characters"
        )));
    }
    Ok(with_dot)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::alto::bfs::BfsFilesystem;
    use crate::fs::alto::{pdi, FsFamily};
    use crate::fs::filesystem::Filesystem;

    fn diablo31() -> Geometry {
        Geometry {
            family: FsFamily::Diablo,
            disk_model: 31,
            n_disks: 1,
            n_cylinders: 203,
            n_heads: 2,
            n_sectors: 12,
            label_bytes: 16,
            data_bytes: 512,
        }
    }

    /// User-file names (the filesystem's own SysDir / DiskDescriptor entries
    /// are real and are listed, but filtered out here).
    fn names(disk: &Disk) -> Vec<String> {
        let mut n: Vec<String> = Bfs::new(disk)
            .list_files()
            .unwrap()
            .into_iter()
            .map(|f| f.name)
            .filter(|n| {
                let b = n.trim_end_matches('.');
                !b.eq_ignore_ascii_case("SysDir") && !b.eq_ignore_ascii_case("DiskDescriptor")
            })
            .collect();
        n.sort();
        n
    }

    #[test]
    fn blank_volume_is_valid() {
        let disk = create_blank(diablo31()).unwrap();
        // SysDir + DiskDescriptor exist; no user files.
        assert_eq!(names(&disk), Vec::<String>::new());
        let dd = Bfs::new(&disk).disk_descriptor().unwrap();
        assert_eq!(dd.n_disks, 1);
        assert_eq!(dd.n_tracks, 203);
        assert_eq!(dd.n_sectors, 12);
        // a fresh Diablo-31 has nearly all of its 4872 pages free
        assert!(dd.free_pages > 4800 && (dd.free_pages as usize) < disk.geometry.total_sectors());
        // and the whole thing survives a PDI round-trip
        let back = pdi::read(&pdi::write(&disk)).unwrap();
        assert_eq!(names(&back), Vec::<String>::new());
    }

    #[test]
    fn add_read_delete_roundtrip() {
        let blank = create_blank(diablo31()).unwrap();
        let payload = b"Hello from a Xerox Alto disk!".repeat(100); // ~2.9 KB, multi-page
        let disk = add_file(&blank, "GREETING.TXT", &payload).unwrap();

        // It lists and reads back byte-identical through the trait.
        let mut fs = BfsFilesystem::open(disk.clone());
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let greeting = entries
            .iter()
            .find(|e| e.name == "GREETING.TXT.")
            .expect("added file is listed");
        assert_eq!(fs.read_file(greeting, usize::MAX).unwrap(), payload);

        // Duplicate add is rejected.
        assert!(add_file(&disk, "greeting.txt", b"x").is_err());

        // Delete removes it and frees pages.
        let after = delete_file(&disk, "GREETING.TXT").unwrap();
        assert_eq!(names(&after), Vec::<String>::new());
        let before_free = Bfs::new(&blank).disk_descriptor().unwrap().free_pages;
        let after_free = Bfs::new(&after).disk_descriptor().unwrap().free_pages;
        assert_eq!(before_free, after_free);
        assert!(delete_file(&after, "GREETING.TXT").is_err());
    }

    #[test]
    fn many_files_survive_rebuild() {
        let mut disk = create_blank(diablo31()).unwrap();
        for i in 0..30 {
            let body = format!("file number {i}\n").repeat(i + 1);
            disk = add_file(&disk, &format!("FILE{i:02}.TXT"), body.as_bytes()).unwrap();
        }
        let listed = names(&disk);
        assert_eq!(listed.len(), 30);
        // spot-check contents of one
        let bytes = Bfs::new(&disk)
            .read_file_by_name("FILE05.TXT", usize::MAX)
            .unwrap();
        assert_eq!(bytes, "file number 5\n".repeat(6).into_bytes());
    }

    #[test]
    fn resize_preserves_files() {
        let mut src = create_blank(diablo31()).unwrap();
        src = add_file(&src, "A.TXT", b"alpha").unwrap();
        src = add_file(
            &src,
            "B.BIN",
            &(0u8..=255).cycle().take(5000).collect::<Vec<_>>(),
        )
        .unwrap();

        // Grow to a Diablo-44 (406x2x12).
        let mut g44 = diablo31();
        g44.disk_model = 44;
        g44.n_cylinders = 406;
        let big = clone_to(&src, g44).unwrap();
        assert_eq!(big.geometry.total_sectors(), 406 * 2 * 12);
        assert_eq!(names(&big), names(&src));
        assert_eq!(
            Bfs::new(&big)
                .read_file_by_name("B.BIN", usize::MAX)
                .unwrap(),
            (0u8..=255).cycle().take(5000).collect::<Vec<_>>()
        );
    }

    #[test]
    fn resize_too_small_fails_cleanly() {
        let mut src = create_blank(diablo31()).unwrap();
        src = add_file(&src, "BIG.BIN", &vec![0xab; 200_000]).unwrap();
        // A tiny 1x1x1x20 geometry can't hold it.
        let tiny = Geometry {
            n_cylinders: 1,
            n_heads: 1,
            n_sectors: 20,
            ..diablo31()
        };
        assert!(matches!(
            clone_to(&src, tiny),
            Err(FilesystemError::DiskFull(_))
        ));
    }
}
