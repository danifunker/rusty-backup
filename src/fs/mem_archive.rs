//! Read-only [`Filesystem`] view over a host archive whose whole contents fit
//! in memory — ZIP and tar (plain / `.tgz` / `.tar.gz`).
//!
//! Commander Mode descends into these with the inline `+` tree exactly like a
//! Mac archive, so the user can browse and copy files out without extracting
//! first. Unlike the StuffIt path ([`super::archive_fs`]) these formats have no
//! resource forks; entries are plain files and directories.
//!
//! A small archive is fully decoded into memory (capped — see [`MAX_TOTAL_BYTES`]).
//! One opened by path above [`INLINE_MAX`] is indexed instead ([`open_path`]):
//! tar members are byte ranges in the file (a compressed tar is first inflated
//! to a temp file), ZIP members are inflated when read, so neither ever sits
//! in RAM whole. Built on the crates already in the tree: `zip`, `tar`, and
//! `flate2` for gzip.

use crate::compat::ZipEntryName as _;
use std::collections::HashMap;
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

/// Refuse to inline-browse an archive whose uncompressed contents exceed this
/// (2 GiB) — extract it instead. Guards against decompressing a huge tarball
/// into RAM.
const MAX_TOTAL_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Archives opened by path above this are indexed rather than read into memory.
pub const INLINE_MAX: u64 = 64 * 1024 * 1024;

/// Sentinel `location` for a synthesized intermediate directory (no archive
/// entry of its own); directories are never read so it needs no backing data.
const SYNTH_DIR: u64 = u64::MAX;

/// One decoded archive member handed to [`MemArchiveFilesystem::from_members`].
struct Member {
    /// Path components, root-relative (no leading/trailing separators).
    parts: Vec<String>,
    is_dir: bool,
    data: Body,
    modified: Option<String>,
}

/// Where a member's bytes are: decoded in memory, a range of the tar file, or a ZIP index.
enum Body {
    Mem(Vec<u8>),
    Range { at: u64, len: u64 },
    Zip { index: usize, len: u64 },
}

impl Body {
    fn len(&self) -> u64 {
        match self {
            Body::Mem(v) => v.len() as u64,
            Body::Range { len, .. } | Body::Zip { len, .. } => *len,
        }
    }
}

/// The open file an indexed archive's members are read from.
enum Backing {
    None,
    /// The tar file (or the temp file a compressed tar was inflated into, kept alive here).
    Tar(
        std::fs::File,
        #[allow(dead_code)] Option<tempfile::TempPath>,
    ),
    Zip(Box<zip::ZipArchive<BufReader<std::fs::File>>>),
}

/// A read-only filesystem backed by an in-memory archive's decoded members.
pub struct MemArchiveFilesystem {
    label: Option<String>,
    fs_type: &'static str,
    /// parent-directory path string -> child entries.
    children: HashMap<String, Vec<FileEntry>>,
    /// file path string -> contents.
    files: HashMap<String, Body>,
    backing: Backing,
    total: u64,
}

impl MemArchiveFilesystem {
    fn from_members(label: Option<String>, fs_type: &'static str, members: Vec<Member>) -> Self {
        let mut children: HashMap<String, Vec<FileEntry>> = HashMap::new();
        children.insert("/".to_string(), Vec::new());
        let mut files: HashMap<String, Body> = HashMap::new();
        let mut total: u64 = 0;

        for m in members {
            if m.parts.is_empty() {
                continue;
            }
            // Ensure every ancestor directory exists as a navigable node.
            for depth in 1..m.parts.len() {
                let dir_path = path_string(&m.parts[..depth]);
                if !children.contains_key(&dir_path) {
                    children.insert(dir_path.clone(), Vec::new());
                    let parent = parent_string(&m.parts[..depth]);
                    let name = m.parts[depth - 1].clone();
                    children
                        .entry(parent)
                        .or_default()
                        .push(FileEntry::new_directory(name, dir_path, SYNTH_DIR));
                }
            }

            let path = path_string(&m.parts);
            let name = m.parts.last().cloned().unwrap_or_default();
            let parent = parent_string(&m.parts);
            let mut fe = if m.is_dir {
                children.entry(path.clone()).or_default();
                FileEntry::new_directory(name, path.clone(), 0)
            } else {
                total += m.data.len();
                let fe = FileEntry::new_file(name, path.clone(), m.data.len(), 0);
                files.insert(path.clone(), m.data);
                fe
            };
            fe.modified = m.modified;

            let bucket = children.entry(parent).or_default();
            // Prefer an explicit entry over a previously synthesized stub.
            if fe.is_directory() {
                bucket.retain(|c| !(c.path == fe.path && c.location == SYNTH_DIR));
            }
            bucket.push(fe);
        }

        Self {
            label,
            fs_type,
            children,
            files,
            backing: Backing::None,
            total,
        }
    }

    /// Stream up to `max` bytes of the member at `path` into `w`.
    fn copy_member(
        &mut self,
        path: &str,
        w: &mut dyn Write,
        max: u64,
    ) -> Result<u64, FilesystemError> {
        let body = self
            .files
            .get(path)
            .ok_or_else(|| FilesystemError::NotFound(path.to_string()))?;
        match (body, &mut self.backing) {
            (Body::Mem(v), _) => {
                let n = (v.len() as u64).min(max) as usize;
                w.write_all(&v[..n])?;
                Ok(n as u64)
            }
            (Body::Range { at, len }, Backing::Tar(f, _)) => {
                f.seek(SeekFrom::Start(*at))?;
                Ok(std::io::copy(&mut (&*f).take((*len).min(max)), w)?)
            }
            (Body::Zip { index, .. }, Backing::Zip(z)) => {
                let zf = z
                    .by_index(*index)
                    .map_err(|e| FilesystemError::Parse(format!("zip entry {index}: {e}")))?;
                Ok(std::io::copy(&mut zf.take(max), w)?)
            }
            _ => Err(FilesystemError::Parse(format!(
                "{path}: archive backing missing"
            ))),
        }
    }
}

/// `"/"`-rooted path string for a sequence of name components.
fn path_string(parts: &[String]) -> String {
    if parts.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", parts.join("/"))
    }
}

/// Path string of the parent directory of `parts`.
fn parent_string(parts: &[String]) -> String {
    if parts.len() <= 1 {
        "/".to_string()
    } else {
        path_string(&parts[..parts.len() - 1])
    }
}

/// Split a `/`-separated archive name into clean path components.
fn split_name(name: &str) -> Vec<String> {
    name.split('/')
        .filter(|s| !s.is_empty() && *s != "." && *s != "..")
        .map(String::from)
        .collect()
}

/// Format a Unix timestamp (seconds) as `YYYY-MM-DD HH:MM:SS` local time,
/// matching the other Modified columns.
fn format_unix_mtime(secs: u64) -> Option<String> {
    let dt = chrono::DateTime::from_timestamp(secs as i64, 0)?;
    let local: chrono::DateTime<chrono::Local> = dt.into();
    Some(local.format("%Y-%m-%d %H:%M:%S").to_string())
}

impl Filesystem for MemArchiveFilesystem {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::root())
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        Ok(self.children.get(&entry.path).cloned().unwrap_or_default())
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let mut out = Vec::new();
        if self.files.contains_key(&entry.path) {
            self.copy_member(&entry.path, &mut out, max_bytes as u64)?;
        }
        Ok(out)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        self.copy_member(&entry.path, writer, u64::MAX)
    }

    fn volume_label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    fn fs_type(&self) -> &str {
        self.fs_type
    }

    fn total_size(&self) -> u64 {
        self.total
    }

    fn used_size(&self) -> u64 {
        self.total
    }
}

/// Open a ZIP archive's bytes as a browsable filesystem.
pub fn open_zip(
    bytes: &[u8],
    label: Option<String>,
) -> Result<MemArchiveFilesystem, FilesystemError> {
    let mut zip = zip::ZipArchive::new(Cursor::new(bytes))
        .map_err(|e| FilesystemError::Parse(format!("open zip: {e}")))?;
    let mut members = Vec::with_capacity(zip.len());
    let mut total: u64 = 0;
    for i in 0..zip.len() {
        let mut zf = zip
            .by_index(i)
            .map_err(|e| FilesystemError::Parse(format!("zip entry {i}: {e}")))?;
        let name = zf.entry_name();
        let parts = split_name(&name);
        if parts.is_empty() {
            continue;
        }
        let is_dir = zf.is_dir();
        let data = Body::Mem(if is_dir {
            Vec::new()
        } else {
            total = total.saturating_add(zf.size());
            if total > MAX_TOTAL_BYTES {
                return Err(FilesystemError::Unsupported(
                    "zip is too large to browse inline; extract it instead".into(),
                ));
            }
            let mut buf = Vec::with_capacity(zf.size() as usize);
            zf.read_to_end(&mut buf)
                .map_err(|e| FilesystemError::Parse(format!("zip read '{name}': {e}")))?;
            buf
        });
        let modified = zf.last_modified().map(|d| {
            format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                d.year(),
                d.month(),
                d.day(),
                d.hour(),
                d.minute(),
                d.second()
            )
        });
        members.push(Member {
            parts,
            is_dir,
            data,
            modified,
        });
    }
    Ok(MemArchiveFilesystem::from_members(label, "ZIP", members))
}

/// Whether `h` is a tar header whose checksum adds up.
fn valid_tar_header(h: &[u8]) -> bool {
    let field = String::from_utf8_lossy(&h[148..156]);
    let Ok(stored) = u32::from_str_radix(field.trim_matches(|c: char| c == '\0' || c == ' '), 8)
    else {
        return false;
    };
    h[0] != 0
        && stored
            == h[..148]
                .iter()
                .chain(&[b' '; 8])
                .chain(&h[156..512])
                .map(|&b| b as u32)
                .sum::<u32>()
}

/// SunOS tar records a hard link's target size but stores no data, which desyncs a
/// reader that skips `size` bytes. Zero such sizes (and restamp the checksum) when the
/// very next block is already a header.
fn zero_link_sizes(tar: &mut [u8]) {
    let mut o = 0usize;
    while o + 1024 <= tar.len() && valid_tar_header(&tar[o..o + 512]) {
        let field = String::from_utf8_lossy(&tar[o + 124..o + 136]).into_owned();
        let size = u64::from_str_radix(field.trim_matches(|c: char| c == '\0' || c == ' '), 8)
            .unwrap_or(0);
        let is_link = matches!(tar[o + 156], b'1' | b'2');
        if is_link && size > 0 && valid_tar_header(&tar[o + 512..o + 1024]) {
            tar[o + 124..o + 136].copy_from_slice(b"00000000000\0");
            let sum: u32 = tar[o..o + 148]
                .iter()
                .chain(&[b' '; 8])
                .chain(&tar[o + 156..o + 512])
                .map(|&b| b as u32)
                .sum();
            tar[o + 148..o + 156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
            o += 512;
            continue;
        }
        o += 512 + (size as usize).div_ceil(512) * 512;
    }
}

/// Open a tar archive's bytes as a browsable filesystem. Transparently
/// gzip-decompresses a `.tgz` / `.tar.gz` (and zstd `.tar.zst` when the feature
/// is built); plain `.tar` is used as-is.
pub fn open_tar(
    bytes: Vec<u8>,
    label: Option<String>,
) -> Result<MemArchiveFilesystem, FilesystemError> {
    let mut tar_bytes = decompress_tar(bytes)?;
    zero_link_sizes(&mut tar_bytes);
    let mut archive = tar::Archive::new(Cursor::new(&tar_bytes));
    let mut members = Vec::new();
    let mut total: u64 = 0;
    let entries = archive
        .entries()
        .map_err(|e| FilesystemError::Parse(format!("read tar: {e}")))?;
    for entry in entries {
        let mut entry = entry.map_err(|e| FilesystemError::Parse(format!("tar entry: {e}")))?;
        let kind = entry.header().entry_type();
        // Only plain files and directories are representable here; skip
        // symlinks / hardlinks / devices.
        if !kind.is_file() && !kind.is_dir() {
            continue;
        }
        // Pre-POSIX (V7) tar has no directory type; a trailing `/` on the name marks one.
        let slash_dir = entry.path_bytes().last() == Some(&b'/');
        let parts = match entry.path() {
            Ok(p) => p
                .components()
                .filter_map(|c| match c {
                    std::path::Component::Normal(s) => Some(s.to_string_lossy().into_owned()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            Err(_) => continue,
        };
        if parts.is_empty() {
            continue;
        }
        let modified = entry.header().mtime().ok().and_then(format_unix_mtime);
        let is_dir = kind.is_dir() || slash_dir;
        let data = Body::Mem(if is_dir {
            Vec::new()
        } else {
            total = total.saturating_add(entry.size());
            if total > MAX_TOTAL_BYTES {
                return Err(FilesystemError::Unsupported(
                    "tar is too large to browse inline; extract it instead".into(),
                ));
            }
            let mut buf = Vec::with_capacity(entry.size() as usize);
            entry
                .read_to_end(&mut buf)
                .map_err(|e| FilesystemError::Parse(format!("tar read: {e}")))?;
            buf
        });
        members.push(Member {
            parts,
            is_dir,
            data,
            modified,
        });
    }
    Ok(MemArchiveFilesystem::from_members(label, "tar", members))
}

/// Open a ZIP or tar archive by path: decoded in memory when small, indexed past [`INLINE_MAX`].
pub fn open_path(
    path: &std::path::Path,
    label: Option<String>,
) -> Result<MemArchiveFilesystem, FilesystemError> {
    let len = std::fs::metadata(path)?.len();
    let mut head = [0u8; 512];
    let n = std::fs::File::open(path)?.read(&mut head)?;
    let zip = head[..n].starts_with(b"PK\x03\x04") || head[..n].starts_with(b"PK\x05\x06");
    if len <= INLINE_MAX {
        let bytes = std::fs::read(path)?;
        return if zip {
            open_zip(&bytes, label)
        } else {
            open_tar(bytes, label)
        };
    }
    if zip {
        open_zip_indexed(path, label)
    } else {
        open_tar_indexed(path, &head[..n], label)
    }
}

fn open_zip_indexed(
    path: &std::path::Path,
    label: Option<String>,
) -> Result<MemArchiveFilesystem, FilesystemError> {
    let mut zip = zip::ZipArchive::new(BufReader::new(std::fs::File::open(path)?))
        .map_err(|e| FilesystemError::Parse(format!("open zip: {e}")))?;
    let mut members = Vec::with_capacity(zip.len());
    for i in 0..zip.len() {
        let zf = zip
            .by_index_raw(i)
            .map_err(|e| FilesystemError::Parse(format!("zip entry {i}: {e}")))?;
        let parts = split_name(&zf.entry_name());
        if parts.is_empty() {
            continue;
        }
        let is_dir = zf.is_dir();
        let modified = zf.last_modified().map(|d| {
            format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                d.year(),
                d.month(),
                d.day(),
                d.hour(),
                d.minute(),
                d.second()
            )
        });
        let data = if is_dir {
            Body::Mem(Vec::new())
        } else {
            Body::Zip {
                index: i,
                len: zf.size(),
            }
        };
        members.push(Member {
            parts,
            is_dir,
            data,
            modified,
        });
    }
    let mut fs = MemArchiveFilesystem::from_members(label, "ZIP", members);
    fs.backing = Backing::Zip(Box::new(zip));
    Ok(fs)
}

fn open_tar_indexed(
    path: &std::path::Path,
    head: &[u8],
    label: Option<String>,
) -> Result<MemArchiveFilesystem, FilesystemError> {
    // A compressed tar has no member offsets; inflate it to a temp file (disk, not RAM) first.
    let (file, temp) =
        if head.starts_with(&[0x1f, 0x8b]) || head.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
            let mut tmp = tempfile::NamedTempFile::new()?;
            let src = BufReader::new(std::fs::File::open(path)?);
            if head.starts_with(&[0x1f, 0x8b]) {
                std::io::copy(&mut flate2::read::MultiGzDecoder::new(src), &mut tmp)
                    .map_err(|e| FilesystemError::Parse(format!("gunzip tar: {e}")))?;
            } else {
                #[cfg(feature = "native-zstd")]
                {
                    zstd::stream::copy_decode(src, &mut tmp)
                        .map_err(|e| FilesystemError::Parse(format!("unzstd tar: {e}")))?;
                }
                #[cfg(not(feature = "native-zstd"))]
                {
                    let _ = src;
                    return Err(FilesystemError::Unsupported(
                        "this build can't decompress .tar.zst".into(),
                    ));
                }
            }
            let (f, p) = tmp.into_parts();
            (f, Some(p))
        } else {
            (std::fs::File::open(path)?, None)
        };
    let mut file = file;
    file.seek(SeekFrom::Start(0))?;
    let mut members = Vec::new();
    {
        let mut archive = tar::Archive::new(BufReader::new(&file));
        let entries = archive
            .entries()
            .map_err(|e| FilesystemError::Parse(format!("read tar: {e}")))?;
        for entry in entries {
            let entry = entry.map_err(|e| FilesystemError::Parse(format!("tar entry: {e}")))?;
            let kind = entry.header().entry_type();
            if !kind.is_file() && !kind.is_dir() {
                continue;
            }
            let slash_dir = entry.path_bytes().last() == Some(&b'/');
            let parts = match entry.path() {
                Ok(p) => p
                    .components()
                    .filter_map(|c| match c {
                        std::path::Component::Normal(s) => Some(s.to_string_lossy().into_owned()),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
                Err(_) => continue,
            };
            if parts.is_empty() {
                continue;
            }
            let modified = entry.header().mtime().ok().and_then(format_unix_mtime);
            let is_dir = kind.is_dir() || slash_dir;
            let data = if is_dir {
                Body::Mem(Vec::new())
            } else {
                Body::Range {
                    at: entry.raw_file_position(),
                    len: entry.size(),
                }
            };
            members.push(Member {
                parts,
                is_dir,
                data,
                modified,
            });
        }
    }
    let mut fs = MemArchiveFilesystem::from_members(label, "tar", members);
    fs.backing = Backing::Tar(file, temp);
    Ok(fs)
}

/// Decompress a possibly-compressed tar stream by sniffing its magic.
fn decompress_tar(bytes: Vec<u8>) -> Result<Vec<u8>, FilesystemError> {
    if bytes.starts_with(&[0x1f, 0x8b]) {
        // gzip (.tgz / .tar.gz)
        let mut out = Vec::new();
        flate2::read::GzDecoder::new(Cursor::new(&bytes))
            .read_to_end(&mut out)
            .map_err(|e| FilesystemError::Parse(format!("gunzip tar: {e}")))?;
        Ok(out)
    } else if bytes.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        // zstd (.tar.zst) — only when the optional codec is built.
        #[cfg(feature = "native-zstd")]
        {
            zstd::stream::decode_all(Cursor::new(&bytes))
                .map_err(|e| FilesystemError::Parse(format!("unzstd tar: {e}")))
        }
        #[cfg(not(feature = "native-zstd"))]
        {
            Err(FilesystemError::Unsupported(
                "this build can't decompress .tar.zst".into(),
            ))
        }
    } else {
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a small tar in memory, open it, and verify listing + reads.
    #[test]
    fn tar_round_trips() {
        let mut buf = Vec::new();
        {
            let mut b = tar::Builder::new(&mut buf);
            let mut add = |name: &str, data: &[u8]| {
                let mut h = tar::Header::new_gnu();
                h.set_size(data.len() as u64);
                h.set_mode(0o644);
                h.set_mtime(0);
                h.set_cksum();
                b.append_data(&mut h, name, data).unwrap();
            };
            add("readme.txt", b"hello");
            add("dir/inner.txt", b"deep");
            b.finish().unwrap();
        }
        let mut fs = open_tar(buf, Some("a.tar".into())).unwrap();
        let root = fs.root().unwrap();
        let mut names: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .iter()
            .map(|e| e.name.clone())
            .collect();
        names.sort();
        assert_eq!(names, vec!["dir".to_string(), "readme.txt".to_string()]);

        let dir = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "dir")
            .unwrap();
        let inner = fs.list_directory(&dir).unwrap().remove(0);
        assert_eq!(inner.name, "inner.txt");
        let mut out = Vec::new();
        fs.write_file_to(&inner, &mut out).unwrap();
        assert_eq!(out, b"deep");
    }

    /// Build a small ZIP in memory, open it, and verify listing + reads.
    #[test]
    fn zip_round_trips() {
        let mut buf = Vec::new();
        {
            let mut w = zip::ZipWriter::new(Cursor::new(&mut buf));
            let opts: zip::write::SimpleFileOptions = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored);
            w.start_file("top.txt", opts).unwrap();
            w.write_all(b"top").unwrap();
            w.start_file("sub/leaf.bin", opts).unwrap();
            w.write_all(b"leafdata").unwrap();
            w.finish().unwrap();
        }
        let mut fs = open_zip(&buf, Some("a.zip".into())).unwrap();
        let root = fs.root().unwrap();
        let names: std::collections::HashSet<String> = fs
            .list_directory(&root)
            .unwrap()
            .iter()
            .map(|e| e.name.clone())
            .collect();
        assert!(names.contains("top.txt"));
        assert!(names.contains("sub")); // intermediate dir synthesized

        let sub = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "sub")
            .unwrap();
        let leaf = fs.list_directory(&sub).unwrap().remove(0);
        assert_eq!(leaf.name, "leaf.bin");
        let mut out = Vec::new();
        fs.write_file_to(&leaf, &mut out).unwrap();
        assert_eq!(out, b"leafdata");
    }

    /// Reads `/dir/inner.txt` and `/readme.txt` back from an indexed archive.
    fn check_indexed(fs: &mut MemArchiveFilesystem) {
        let root = fs.root().unwrap();
        let top = fs.list_directory(&root).unwrap();
        let readme = top.iter().find(|e| e.name == "readme.txt").unwrap();
        assert_eq!(fs.read_file(readme, usize::MAX).unwrap(), b"hello");
        assert_eq!(fs.read_file(readme, 2).unwrap(), b"he");
        let dir = top.iter().find(|e| e.name == "dir").unwrap();
        let inner = fs.list_directory(dir).unwrap().remove(0);
        let mut out = Vec::new();
        fs.write_file_to(&inner, &mut out).unwrap();
        assert_eq!(out, b"deep");
    }

    fn sample_tar() -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut b = tar::Builder::new(&mut buf);
            for (name, data) in [("readme.txt", &b"hello"[..]), ("dir/inner.txt", b"deep")] {
                let mut h = tar::Header::new_gnu();
                h.set_size(data.len() as u64);
                h.set_mode(0o644);
                h.set_cksum();
                b.append_data(&mut h, name, data).unwrap();
            }
            b.finish().unwrap();
        }
        buf
    }

    #[test]
    fn indexed_tar_and_tgz_read_members_from_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let plain = dir.path().join("a.tar");
        std::fs::write(&plain, sample_tar()).unwrap();
        let head = std::fs::read(&plain).unwrap();
        check_indexed(&mut open_tar_indexed(&plain, &head[..512], None).unwrap());

        let gz = dir.path().join("a.tar.gz");
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        enc.write_all(&sample_tar()).unwrap();
        std::fs::write(&gz, enc.finish().unwrap()).unwrap();
        let head = std::fs::read(&gz).unwrap();
        check_indexed(&mut open_tar_indexed(&gz, &head, None).unwrap());
    }

    #[test]
    fn indexed_zip_inflates_members_on_demand() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.zip");
        let mut buf = Vec::new();
        {
            let mut w = zip::ZipWriter::new(Cursor::new(&mut buf));
            let opts: zip::write::SimpleFileOptions = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated);
            w.start_file("readme.txt", opts).unwrap();
            w.write_all(b"hello").unwrap();
            w.start_file("dir/inner.txt", opts).unwrap();
            w.write_all(b"deep").unwrap();
            w.finish().unwrap();
        }
        std::fs::write(&path, buf).unwrap();
        check_indexed(&mut open_zip_indexed(&path, None).unwrap());
    }
}
