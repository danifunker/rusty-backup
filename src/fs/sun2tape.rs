//! Sun QIC tape images (`.qic`, the `SUN2TAPE` container), browsed as one filesystem.
//!
//! A QIC cartridge is 512-byte blocks in files separated by file marks. The image
//! format (written by the Sun-2 / Sun-3 MiSTer cores' `tools/mktape`) keeps a table
//! in block 0, all big-endian `u32`: `"SUN2"`, `"TAPE"`, version `1`, the volume
//! count, then for each volume at word `4 + 32 * v` its file count N and N + 1 block
//! numbers (each file's start, then where the last one ends). Files follow, padded
//! to whole blocks.
//!
//! The volume becomes a read-only tree. With more than one volume the root holds
//! `vol1/`, `vol2/`...; otherwise the tape files sit at the root, numbered by
//! position (`01-...`). A tape file becomes a directory when it holds a tar archive,
//! a `compress(1)`-ed tar (SunOS 4.1 install tapes), or a UFS image (the miniroot);
//! anything else (boot block, table of contents, kernel, text) is a plain file. The
//! names come from the tape's own XDR table of contents when it has one (SunOS
//! install tapes do), else from the content type.
//!
//! Directory contents are decoded in memory on first access and cached; tape files
//! are tens of megabytes at most.

use std::collections::HashMap;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

const BLOCK: u64 = 512;
const MAGIC: &[u8; 8] = b"SUN2TAPE";
const MAX_VOLUMES: usize = 3;
const MAX_FILES: usize = 30;
const COMPRESS_MAGIC: [u8; 2] = [0x1F, 0x9D];

/// Whether `head` (the image's first block) is a `SUN2TAPE` table.
pub fn looks_like_tape(head: &[u8]) -> bool {
    head.len() >= 16
        && &head[0..8] == MAGIC
        && u32::from_be_bytes([head[8], head[9], head[10], head[11]]) == 1
        && (1..=MAX_VOLUMES as u32).contains(&u32::from_be_bytes([
            head[12], head[13], head[14], head[15],
        ]))
}

/// What a tape file holds, from its first bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TapeFileKind {
    Tar,
    CompressedTar,
    Compressed,
    Ufs,
    AOut,
    Text,
    Data,
}

/// One file on the tape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TapeFile {
    pub volume: usize,
    /// Position on its volume, from 0.
    pub index: usize,
    pub start: u64,
    pub len: u64,
    pub kind: TapeFileKind,
    /// Display name, e.g. `05-root.tar.Z`.
    pub name: String,
}

impl TapeFile {
    fn is_dir(&self) -> bool {
        matches!(
            self.kind,
            TapeFileKind::Tar | TapeFileKind::CompressedTar | TapeFileKind::Ufs
        )
    }
}

fn be32(b: &[u8], o: usize) -> u32 {
    u32::from_be_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}

fn is_tar_header(h: &[u8]) -> bool {
    if h.len() < 512 || h[0] == 0 {
        return false;
    }
    let field = String::from_utf8_lossy(&h[148..156]);
    let Ok(stored) = u32::from_str_radix(field.trim_matches(|c: char| c == '\0' || c == ' '), 8)
    else {
        return false;
    };
    let sum: u32 = h[..148]
        .iter()
        .chain(&[b' '; 8])
        .chain(&h[156..512])
        .map(|&b| b as u32)
        .sum();
    stored == sum
}

fn classify(head: &[u8]) -> TapeFileKind {
    if head.len() >= 2 && head[0..2] == COMPRESS_MAGIC {
        return TapeFileKind::Compressed;
    }
    if is_tar_header(head) {
        return TapeFileKind::Tar;
    }
    if head.len() >= 8192 + 1376 {
        let m = [
            head[8192 + 1372],
            head[8192 + 1373],
            head[8192 + 1374],
            head[8192 + 1375],
        ];
        if super::ufs::is_ufs_magic(m, true) {
            return TapeFileKind::Ufs;
        }
    }
    // Sun a.out: machine type byte, then magic 0407 / 0410 / 0413.
    if head.len() >= 4
        && matches!(
            u16::from_be_bytes([head[2], head[3]]),
            0o407 | 0o410 | 0o413
        )
    {
        return TapeFileKind::AOut;
    }
    let sample = &head[..head.len().min(512)];
    if !sample.is_empty()
        && sample
            .iter()
            .all(|&b| b == b'\n' || b == b'\t' || (32..127).contains(&b) || b == 0)
    {
        return TapeFileKind::Text;
    }
    TapeFileKind::Data
}

/// Entry names from a SunOS XDR table of contents for volume `volume` (1-based), in
/// tape order; empty when it isn't one. Entries are listed in tape order, each led by
/// a `VOLUME n` label, and its name follows a word with the high bit set (the boot
/// entry has none, so it falls back to the first string after the device name).
fn toc_names(toc: &[u8], volume: usize) -> Vec<String> {
    let mut strings: Vec<(usize, String)> = Vec::new();
    let mut o = 0;
    while o + 4 <= toc.len() {
        let n = be32(toc, o) as usize;
        if (1..=120).contains(&n) && o + 4 + n <= toc.len() {
            let s = &toc[o + 4..o + 4 + n];
            if s.iter().all(|&b| (32..127).contains(&b)) {
                strings.push((o, String::from_utf8_lossy(s).into_owned()));
                o += 4 + n.div_ceil(4) * 4;
                continue;
            }
        }
        o += 4;
    }
    if !strings.iter().any(|(_, s)| s == "XDRTOC") {
        return Vec::new();
    }
    let mut names = Vec::new();
    for (i, (_, label)) in strings.iter().enumerate() {
        let Some(digits) = label.strip_prefix("VOLUME") else {
            continue;
        };
        if digits.trim().parse::<usize>().ok() != Some(volume) {
            continue;
        }
        let entry: Vec<&(usize, String)> = strings[i + 1..]
            .iter()
            .take_while(|(_, n)| !n.starts_with("VOLUME"))
            .collect();
        let flagged = entry
            .iter()
            .find(|(at, _)| *at >= 4 && be32(toc, at - 4) & 0x8000_0000 != 0);
        let plain = entry
            .iter()
            .skip(1)
            .find(|(_, n)| !(n.starts_with("sun") && n.len() <= 5));
        names.push(
            flagged
                .or(plain)
                .map(|(_, n)| n.clone())
                .unwrap_or_default(),
        );
    }
    names
}

/// A `SUN2TAPE` image browsed as a read-only filesystem.
pub struct TapeFilesystem<R: Read + Seek + Send> {
    reader: R,
    offset: u64,
    files: Vec<TapeFile>,
    volumes: usize,
    opened: HashMap<usize, Box<dyn Filesystem>>,
}

impl<R: Read + Seek + Send> TapeFilesystem<R> {
    pub fn open(mut reader: R, offset: u64) -> Result<Self, FilesystemError> {
        let mut head = [0u8; 512];
        reader.seek(SeekFrom::Start(offset))?;
        reader.read_exact(&mut head)?;
        if !looks_like_tape(&head) {
            return Err(FilesystemError::Parse("not a SUN2TAPE image".into()));
        }
        let volumes = be32(&head, 12) as usize;
        let mut files = Vec::new();
        for v in 0..volumes {
            let w = 4 + 32 * v;
            let n = (be32(&head, w * 4) as usize).min(MAX_FILES);
            let blocks: Vec<u64> = (0..=n)
                .map(|k| be32(&head, (w + 1 + k) * 4) as u64)
                .collect();
            for k in 0..n {
                let (start, end) = (blocks[k] * BLOCK, blocks[k + 1] * BLOCK);
                if end <= start {
                    continue;
                }
                let mut probe = vec![0u8; ((end - start) as usize).min(16384)];
                reader.seek(SeekFrom::Start(offset + start))?;
                reader.read_exact(&mut probe)?;
                files.push(TapeFile {
                    volume: v,
                    index: k,
                    start,
                    len: end - start,
                    kind: classify(&probe),
                    name: String::new(),
                });
            }
        }
        let mut fs = Self {
            reader,
            offset,
            files,
            volumes,
            opened: HashMap::new(),
        };
        fs.name_files()?;
        Ok(fs)
    }

    /// The tape's files, in order.
    pub fn files(&self) -> &[TapeFile] {
        &self.files
    }

    fn name_files(&mut self) -> Result<(), FilesystemError> {
        let mut by_volume: HashMap<usize, Vec<String>> = HashMap::new();
        for v in 0..self.volumes {
            let toc = self
                .files
                .iter()
                .find(|f| f.volume == v && f.index == 1)
                .cloned();
            if let Some(toc) = toc.filter(|t| t.kind == TapeFileKind::Data && t.len <= 64 * 1024) {
                let bytes = self.read_range(toc.start, toc.len)?;
                by_volume.insert(v, toc_names(&bytes, v + 1));
            }
        }
        for i in 0..self.files.len() {
            if self.files[i].kind == TapeFileKind::Compressed {
                let f = self.files[i].clone();
                let raw = self.read_range(f.start, f.len)?;
                let tar = crate::macarchive::stuffit_lzw::decompress_unix_z(&raw)
                    .map(|d| is_tar_header(&d))
                    .unwrap_or(false);
                if tar {
                    self.files[i].kind = TapeFileKind::CompressedTar;
                }
            }
            let f = &self.files[i];
            let base = by_volume
                .get(&f.volume)
                .and_then(|m| m.get(f.index))
                .filter(|n| !n.is_empty())
                .map(|n| n.replace('/', "_"))
                .unwrap_or_else(|| match f.kind {
                    TapeFileKind::Tar | TapeFileKind::CompressedTar => "archive".into(),
                    TapeFileKind::Compressed => "file".into(),
                    TapeFileKind::Ufs => "filesystem".into(),
                    TapeFileKind::AOut => "program".into(),
                    TapeFileKind::Text => "text".into(),
                    TapeFileKind::Data => "data".into(),
                });
            let ext = match f.kind {
                TapeFileKind::Tar => ".tar",
                TapeFileKind::CompressedTar => ".tar.Z",
                TapeFileKind::Compressed => ".Z",
                TapeFileKind::Ufs => ".ufs",
                TapeFileKind::Text => ".txt",
                _ => "",
            };
            let name = format!("{:02}-{base}{ext}", f.index);
            self.files[i].name = name;
        }
        Ok(())
    }

    fn read_range(&mut self, start: u64, len: u64) -> Result<Vec<u8>, FilesystemError> {
        let mut buf = vec![0u8; len as usize];
        self.reader.seek(SeekFrom::Start(self.offset + start))?;
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Path of tape file `i`'s node: `/NN-name`, under `/volN` on a multi-volume image.
    fn node_path(&self, i: usize) -> String {
        let f = &self.files[i];
        if self.volumes > 1 {
            format!("/vol{}/{}", f.volume + 1, f.name)
        } else {
            format!("/{}", f.name)
        }
    }

    /// Which tape file a path lies under, and the path inside it.
    fn split_path(&self, path: &str) -> Option<(usize, String)> {
        (0..self.files.len()).find_map(|i| {
            let node = self.node_path(i);
            let rest = path.strip_prefix(&node)?;
            if rest.is_empty() {
                Some((i, "/".to_string()))
            } else {
                rest.starts_with('/').then(|| (i, rest.to_string()))
            }
        })
    }

    fn child(&mut self, i: usize) -> Result<&mut Box<dyn Filesystem>, FilesystemError> {
        if !self.opened.contains_key(&i) {
            let f = self.files[i].clone();
            let raw = self.read_range(f.start, f.len)?;
            let label = Some(f.name.clone());
            let fs: Box<dyn Filesystem> = match f.kind {
                TapeFileKind::Tar => Box::new(super::mem_archive::open_tar(raw, label)?),
                TapeFileKind::CompressedTar => {
                    let tar = crate::macarchive::stuffit_lzw::decompress_unix_z(&raw)
                        .map_err(|e| FilesystemError::Parse(format!("{}: {e}", f.name)))?;
                    Box::new(super::mem_archive::open_tar(tar, label)?)
                }
                TapeFileKind::Ufs => {
                    Box::new(super::ufs::UfsFilesystem::open(Cursor::new(raw), 0)?)
                }
                _ => return Err(FilesystemError::NotADirectory(f.name)),
            };
            self.opened.insert(i, fs);
        }
        Ok(self.opened.get_mut(&i).expect("just inserted"))
    }

    fn raw_entry(&self, i: usize) -> FileEntry {
        let f = &self.files[i];
        let path = self.node_path(i);
        if f.is_dir() {
            FileEntry::new_directory(f.name.clone(), path, i as u64)
        } else {
            FileEntry::new_file(f.name.clone(), path, f.len, i as u64)
        }
    }

    /// Re-root a child's entry under its tape-file node.
    fn lift(&self, i: usize, mut e: FileEntry) -> FileEntry {
        let node = self.node_path(i);
        e.path = if e.path == "/" {
            node
        } else {
            format!("{node}{}", e.path)
        };
        e
    }
}

impl<R: Read + Seek + Send> Filesystem for TapeFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::root())
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if entry.path == "/" {
            if self.volumes > 1 {
                return Ok((1..=self.volumes)
                    .map(|v| FileEntry::new_directory(format!("vol{v}"), format!("/vol{v}"), 0))
                    .collect());
            }
            return Ok((0..self.files.len()).map(|i| self.raw_entry(i)).collect());
        }
        if let Some(v) = entry
            .path
            .strip_prefix("/vol")
            .and_then(|n| n.parse::<usize>().ok())
        {
            return Ok((0..self.files.len())
                .filter(|&i| self.files[i].volume + 1 == v)
                .map(|i| self.raw_entry(i))
                .collect());
        }
        let (i, inner) = self
            .split_path(&entry.path)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        let child = self.child(i)?;
        let target = if inner == "/" {
            child.root()?
        } else {
            let mut e = entry.clone();
            e.path = inner;
            e
        };
        let kids = child.list_directory(&target)?;
        Ok(kids.into_iter().map(|k| self.lift(i, k)).collect())
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let (i, inner) = self
            .split_path(&entry.path)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        if inner == "/" {
            let f = self.files[i].clone();
            return self.read_range(f.start, f.len.min(max_bytes as u64));
        }
        let mut e = entry.clone();
        e.path = inner;
        self.child(i)?.read_file(&e, max_bytes)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let (i, inner) = self
            .split_path(&entry.path)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        if inner == "/" {
            let f = self.files[i].clone();
            let data = self.read_range(f.start, f.len)?;
            writer.write_all(&data)?;
            return Ok(f.len);
        }
        let mut e = entry.clone();
        e.path = inner;
        self.child(i)?.write_file_to(&e, writer)
    }

    fn volume_label(&self) -> Option<&str> {
        None
    }

    fn fs_type(&self) -> &str {
        "Sun QIC tape"
    }

    fn total_size(&self) -> u64 {
        self.files
            .iter()
            .map(|f| f.start + f.len)
            .max()
            .unwrap_or(0)
    }

    fn used_size(&self) -> u64 {
        self.files.iter().map(|f| f.len).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tar_member(name: &str, body: &[u8]) -> Vec<u8> {
        let mut b = tar::Builder::new(Vec::new());
        let mut h = tar::Header::new_old();
        h.set_size(body.len() as u64);
        h.set_mode(0o644);
        h.set_cksum();
        b.append_data(&mut h, name, body).unwrap();
        b.into_inner().unwrap()
    }

    /// One volume: a text file, then a V7 tar holding `etc/motd`.
    fn sample() -> Vec<u8> {
        let mut img = vec![0u8; 512];
        img[0..8].copy_from_slice(MAGIC);
        img[8..12].copy_from_slice(&1u32.to_be_bytes());
        img[12..16].copy_from_slice(&1u32.to_be_bytes());
        let text = b"SunOS 4.1.1 tape\n".to_vec();
        let tar = tar_member("etc/motd", b"hello\n");
        let mut blocks = vec![1u32];
        let mut body = Vec::new();
        for f in [&text, &tar] {
            let mut padded = f.clone();
            padded.resize(f.len().div_ceil(512) * 512, 0);
            body.extend_from_slice(&padded);
            blocks.push(1 + (body.len() / 512) as u32);
        }
        img[16..20].copy_from_slice(&2u32.to_be_bytes());
        for (k, b) in blocks.iter().enumerate() {
            img[20 + k * 4..24 + k * 4].copy_from_slice(&b.to_be_bytes());
        }
        img.extend_from_slice(&body);
        img
    }

    #[test]
    fn lists_tape_files_and_browses_into_a_tar() {
        let mut fs = TapeFilesystem::open(Cursor::new(sample()), 0).expect("open");
        let root = fs.root().unwrap();
        let top = fs.list_directory(&root).unwrap();
        let names: Vec<&str> = top.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["00-text.txt", "01-archive.tar"]);
        assert!(top[1].is_directory());
        let inner = fs.list_directory(&top[1]).unwrap();
        let etc = inner.iter().find(|e| e.name == "etc").expect("etc dir");
        assert_eq!(etc.path, "/01-archive.tar/etc");
        let motd = fs
            .list_directory(etc)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "motd")
            .unwrap();
        assert_eq!(fs.read_file(&motd, usize::MAX).unwrap(), b"hello\n");
        assert!(fs
            .read_file(&top[0], usize::MAX)
            .unwrap()
            .starts_with(b"SunOS"));
    }

    #[test]
    fn rejects_other_images() {
        assert!(!looks_like_tape(&[0u8; 512]));
        assert!(TapeFilesystem::open(Cursor::new(vec![0u8; 1024]), 0).is_err());
    }
}
