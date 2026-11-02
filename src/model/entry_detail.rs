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
