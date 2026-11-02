//! What File Info reads from a source beyond the listing's entry: a preview, xattrs, account
//! names and, at a SquashFS volume's root, its size budget. Run as a job; the source reopens fresh.

use disk_core::entry::FileEntry;
use disk_core::id_names::IdNameMap;
use disk_core::xattr::Xattr;
use fs_squashfs::squashfs_edit::SizePlan;

use crate::model::commander_ops::StageSource;

/// Where the entry lives.
#[derive(Clone)]
pub enum DetailSource {
    /// A host file, read by its absolute path.
    Host,
    Image(StageSource),
}

/// File Info's data read off the source.
#[derive(Debug, Clone, Default)]
pub struct DetailExtras {
    /// Up to the asked-for number of bytes of a file; `None` for a folder or a failed read.
    pub preview: Option<Vec<u8>>,
    /// Up to the same number of bytes of the resource fork, for a file that has one.
    pub rsrc_preview: Option<Vec<u8>>,
    pub xattrs: Vec<Xattr>,
    /// uid/gid -> name from the image's own account files, if any.
    pub id_names: IdNameMap,
    pub size_plan: Option<SizePlan>,
}

/// Read `entry`'s extras; `at_root` asks for the SquashFS budget (it walks the whole tree).
pub fn read_detail(
    source: DetailSource,
    entry: &FileEntry,
    max: usize,
    at_root: bool,
) -> anyhow::Result<DetailExtras> {
    let source = match source {
        DetailSource::Host => {
            let preview = entry
                .is_file()
                .then(|| read_host_file_capped(&entry.path, max))
                .flatten();
            return Ok(DetailExtras {
                preview,
                ..DetailExtras::default()
            });
        }
        DetailSource::Image(source) => source,
    };
    // A volume inside a partition may grow only to the partition's end; a bare file just grows.
    let capacity = match &source {
        StageSource::Session(s) if s.partition_offset != 0 => s.partition_size,
        _ => None,
    };
    let mut fs = source.open()?;
    let preview = if entry.is_file() {
        fs.read_file(entry, max).ok()
    } else {
        None
    };
    let rsrc_preview = if entry.is_file() && entry.resource_fork_size.unwrap_or(0) > 0 {
        let mut buf = Vec::new();
        fs.write_resource_fork_to(entry, &mut buf).ok().map(|_| {
            buf.truncate(max);
            buf
        })
    } else {
        None
    };
    let xattrs = if fs.supports_xattrs() {
        fs.list_xattrs(entry).unwrap_or_default()
    } else {
        Vec::new()
    };
    let size_plan = if at_root && fs.fs_type() == "SquashFS" {
        let image_len = fs_squashfs::squashfs_write::image_footprint(fs.total_size());
        fs_squashfs::squashfs_edit::plan_size(&mut *fs, image_len, capacity).ok()
    } else {
        None
    };
    let id_names = IdNameMap::from_filesystem(&mut *fs);
    Ok(DetailExtras {
        preview,
        rsrc_preview,
        xattrs,
        id_names,
        size_plan,
    })
}

fn read_host_file_capped(path: &str, max: usize) -> Option<Vec<u8>> {
    use std::io::Read;
    let file = std::fs::File::open(path).ok()?;
    let mut buf = Vec::new();
    file.take(max as u64).read_to_end(&mut buf).ok()?;
    Some(buf)
}

/// Decoded file content for the preview pane: either a text rendering or the
/// raw bytes for a hex dump.
#[derive(Debug, Clone)]
pub enum FileContent {
    Binary(Vec<u8>),
    Text(String),
}

/// HFS/HFS+ file type classification from `assets/hfs_file_types.json`,
/// embedded at compile time.
mod hfs_file_types {
    use std::collections::HashMap;
    use std::sync::OnceLock;

    #[derive(serde::Deserialize)]
    struct TypeInfo {
        category: String,
    }

    #[derive(serde::Deserialize)]
    struct HfsFileTypes {
        types: HashMap<String, TypeInfo>,
    }

    static HFS_TYPES: OnceLock<HashMap<String, bool>> = OnceLock::new();
    const HFS_FILE_TYPES_JSON: &str =
        include_str!("../../crates/fs-mac/assets/hfs_file_types.json");

    fn get() -> &'static HashMap<String, bool> {
        HFS_TYPES.get_or_init(|| {
            let ft: HfsFileTypes =
                serde_json::from_str(HFS_FILE_TYPES_JSON).unwrap_or(HfsFileTypes {
                    types: HashMap::new(),
                });
            ft.types
                .into_iter()
                .map(|(k, v)| (k, v.category == "text"))
                .collect()
        })
    }

    /// Returns Some(true) for known text type, Some(false) for known binary, None for unknown.
    pub fn classify(type_code: &str) -> Option<bool> {
        get().get(type_code).copied()
    }
}

/// Detect whether data is text or binary and return appropriate content.
pub fn detect_content_type(entry: &FileEntry, data: &[u8]) -> FileContent {
    if data.is_empty() {
        return FileContent::Text(String::new());
    }

    // Check HFS type code first (if present)
    if let Some(ref type_code) = entry.type_code {
        let type_str = fs_mac::hfs_common::decode_ostype(type_code);
        if let Some(is_text) = hfs_file_types::classify(&type_str) {
            if !is_text {
                return FileContent::Binary(data.to_vec());
            }
            // Known text type — decode as text (Mac files may not be UTF-8)
            if let Ok(text) = std::str::from_utf8(data) {
                return FileContent::Text(text.to_string());
            }
            // Not valid UTF-8 but known text type — show as lossy text
            let text = data
                .iter()
                .map(|&b| {
                    if b.is_ascii_graphic() || b.is_ascii_whitespace() {
                        b as char
                    } else {
                        '.'
                    }
                })
                .collect();
            return FileContent::Text(text);
        }
    }

    // Fall back to content heuristics
    // Try UTF-8 first
    if let Ok(text) = std::str::from_utf8(data) {
        let non_printable = text
            .chars()
            .filter(|c| !c.is_ascii_graphic() && !c.is_ascii_whitespace())
            .count();
        if non_printable * 10 < text.len() {
            return FileContent::Text(text.to_string());
        }
    }

    // Check if mostly printable bytes
    let printable = data
        .iter()
        .filter(|&&b| b.is_ascii_graphic() || b.is_ascii_whitespace())
        .count();
    if printable * 10 >= data.len() * 8 {
        // 80% printable
        let text = data
            .iter()
            .map(|&b| {
                if b.is_ascii_graphic() || b.is_ascii_whitespace() {
                    b as char
                } else {
                    '.'
                }
            })
            .collect();
        return FileContent::Text(text);
    }

    FileContent::Binary(data.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_host_file_previews_up_to_the_cap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.txt");
        std::fs::write(&path, b"hello world").unwrap();
        let entry = FileEntry::new_file("f.txt".into(), path.to_string_lossy().into_owned(), 11, 0);
        let extras = read_detail(DetailSource::Host, &entry, 5, false).unwrap();
        assert_eq!(extras.preview.as_deref(), Some(&b"hello"[..]));
        assert!(extras.xattrs.is_empty() && extras.size_plan.is_none());
    }
}
