//! Read-only [`Filesystem`] view over a host archive whose whole contents fit
//! in memory — ZIP and tar (plain / `.tgz` / `.tar.gz`).
//!
//! Commander Mode descends into these with the inline `+` tree exactly like a
//! Mac archive, so the user can browse and copy files out without extracting
//! first. Unlike the StuffIt path ([`super::archive_fs`]) these formats have no
//! resource forks; entries are plain files and directories.
//!
//! The archive is fully decoded into memory (capped — see [`MAX_TOTAL_BYTES`]),
//! which keeps the reader simple and is fine for the modest archives people
//! browse interactively. Built on the crates already in the tree: `zip`, `tar`,
//! and `flate2` for gzip.

use crate::compat::ZipEntryName as _;
use std::collections::HashMap;
use std::io::{Cursor, Read, Write};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

/// Refuse to inline-browse an archive whose uncompressed contents exceed this
/// (2 GiB) — extract it instead. Guards against decompressing a huge tarball
/// into RAM.
const MAX_TOTAL_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Sentinel `location` for a synthesized intermediate directory (no archive
/// entry of its own); directories are never read so it needs no backing data.
const SYNTH_DIR: u64 = u64::MAX;

/// One decoded archive member handed to [`MemArchiveFilesystem::from_members`].
struct Member {
    /// Path components, root-relative (no leading/trailing separators).
    parts: Vec<String>,
    is_dir: bool,
    data: Vec<u8>,
    modified: Option<String>,
}

/// A read-only filesystem backed by an in-memory archive's decoded members.
pub struct MemArchiveFilesystem {
    label: Option<String>,
    fs_type: &'static str,
    /// parent-directory path string -> child entries.
    children: HashMap<String, Vec<FileEntry>>,
    /// file path string -> contents.
    files: HashMap<String, Vec<u8>>,
    total: u64,
}

impl MemArchiveFilesystem {
    fn from_members(label: Option<String>, fs_type: &'static str, members: Vec<Member>) -> Self {
        let mut children: HashMap<String, Vec<FileEntry>> = HashMap::new();
        children.insert("/".to_string(), Vec::new());
        let mut files: HashMap<String, Vec<u8>> = HashMap::new();
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
                total += m.data.len() as u64;
                let fe = FileEntry::new_file(name, path.clone(), m.data.len() as u64, 0);
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
            total,
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
        let mut data = self.files.get(&entry.path).cloned().unwrap_or_default();
        data.truncate(max_bytes);
        Ok(data)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let data = self
            .files
            .get(&entry.path)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        writer.write_all(data)?;
        Ok(data.len() as u64)
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
        let data = if is_dir {
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
        };
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
        let data = if is_dir {
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
        };
        members.push(Member {
            parts,
            is_dir,
            data,
            modified,
        });
    }
    Ok(MemArchiveFilesystem::from_members(label, "tar", members))
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
}
