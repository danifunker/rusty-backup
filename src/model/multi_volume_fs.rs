//! A virtual, read-only [`Filesystem`] that presents each volume of a
//! multi-volume container as a directory at its root — so Commander Mode's
//! inline `+`/`-` tree can show, and descend into, *every* filesystem on a disc
//! or every partition of a disk image instead of silently opening only the
//! first.
//!
//! Two containers routinely produce more than one browsable volume:
//!
//! * a **hybrid Mac/PC optical disc** — an ISO 9660 primary plus a co-resident
//!   HFS/HFS+ Apple side that the partition table hides (enumerated by
//!   [`commander_descend::optical_filesystems`]);
//! * a **partitioned disk image** with more than one browsable partition
//!   (enumerated by [`commander_descend::browsable_partitions`]).
//!
//! A single-volume container never needs this adapter — the caller opens that
//! one volume directly. When there are several, this wraps them: the adapter
//! root lists one directory per volume (named for the filesystem / partition),
//! and paths beneath it are `"/{index}/{native path}"`. Reads strip the leading
//! `"/{index}"` and delegate to that volume's real filesystem, which is opened
//! lazily on first access and cached.
//!
//! Path remapping only ever splits off the leading `/{digits}` segment, so it is
//! safe for volumes (HFS) whose filenames may themselves contain `/`. Every
//! delegated call preserves the entry's other fields (crucially `location`), so
//! filesystems that resolve reads by cluster/CNID rather than by path work
//! unchanged — exactly as they do on the single-volume path.

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::fs::entry::FileEntry;
use crate::fs::filesystem::{Filesystem, FilesystemError};
use crate::model::commander_descend;
use crate::partition::PartitionInfo;

/// How to open one volume of the container as a real filesystem. Held so a
/// volume's filesystem can be opened lazily (first access) and reopened fresh on
/// a worker thread via [`commander_descend::ReopenRecipe`].
enum VolumeOpener {
    #[cfg(feature = "optical")]
    OpticalPrimary,
    #[cfg(feature = "optical")]
    OpticalHybrid(usize),
    Partition(Box<PartitionInfo>),
    /// Test-only placeholder for volumes constructed already-open; opening it
    /// panics because a pre-opened volume is never reopened.
    #[cfg(test)]
    Preopened,
}

impl VolumeOpener {
    // `label` is consumed only by the optical arms; a build without the feature
    // has just the partition arm, which opens by partition info alone.
    #[cfg_attr(not(feature = "optical"), allow(unused_variables))]
    fn open(&self, path: &Path, label: Option<String>) -> Result<Box<dyn Filesystem>> {
        match self {
            #[cfg(feature = "optical")]
            VolumeOpener::OpticalPrimary => commander_descend::open_optical(path, label),
            #[cfg(feature = "optical")]
            VolumeOpener::OpticalHybrid(i) => {
                commander_descend::open_optical_hybrid(path, *i, label)
            }
            VolumeOpener::Partition(p) => commander_descend::open_image_partition(path, p),
            #[cfg(test)]
            VolumeOpener::Preopened => unreachable!("a pre-opened volume is never reopened"),
        }
    }
}

/// One volume slot: its display label, how to open it, and the opened filesystem
/// once first accessed.
struct Volume {
    label: String,
    opener: VolumeOpener,
    fs: Option<Box<dyn Filesystem>>,
}

/// A read-only filesystem presenting each volume of a multi-volume container as
/// a directory at its root.
pub struct MultiVolumeFilesystem {
    /// The on-disk container path (each volume's filesystem opens from it).
    path: PathBuf,
    /// One entry per browsable volume; its position is the `"/{index}"` path
    /// segment.
    volumes: Vec<Volume>,
    /// Display string for [`Filesystem::volume_label`].
    label: Option<String>,
    /// Display token for [`Filesystem::fs_type`].
    fs_type: String,
}

impl MultiVolumeFilesystem {
    /// Build the adapter for a hybrid optical disc from its already-enumerated
    /// filesystems (see [`commander_descend::optical_filesystems`]). Infallible:
    /// the sub-filesystems open lazily on first access.
    #[cfg(feature = "optical")]
    pub fn optical_from_choices(
        path: &Path,
        choices: Vec<commander_descend::OpticalFsChoice>,
        label: Option<String>,
    ) -> Self {
        let volumes = choices
            .into_iter()
            .map(|c| Volume {
                label: c.label,
                opener: match c.hybrid_index {
                    None => VolumeOpener::OpticalPrimary,
                    Some(i) => VolumeOpener::OpticalHybrid(i),
                },
                fs: None,
            })
            .collect();
        Self {
            path: path.to_path_buf(),
            volumes,
            label,
            fs_type: "Hybrid disc".to_string(),
        }
    }

    /// Build the adapter for a hybrid optical disc by enumerating it — used to
    /// rebuild a fresh instance on a worker thread.
    #[cfg(feature = "optical")]
    pub fn optical(path: &Path, label: Option<String>) -> Self {
        let choices = commander_descend::optical_filesystems(path);
        Self::optical_from_choices(path, choices, label)
    }

    /// Build the adapter for a partitioned disk image from its already-probed
    /// browsable partitions (see [`commander_descend::browsable_partitions`]).
    pub fn disk_image_from_parts(path: &Path, parts: Vec<(usize, PartitionInfo)>) -> Self {
        let label = path.file_name().map(|n| n.to_string_lossy().into_owned());
        let volumes = parts
            .into_iter()
            .map(|(_, p)| Volume {
                label: partition_node_label(&p),
                opener: VolumeOpener::Partition(Box::new(p)),
                fs: None,
            })
            .collect();
        Self {
            path: path.to_path_buf(),
            volumes,
            label,
            fs_type: "Partitioned disk".to_string(),
        }
    }

    /// Build the adapter for a partitioned disk image by probing it — used to
    /// rebuild a fresh instance on a worker thread.
    pub fn disk_image(path: &Path) -> Result<Self> {
        let parts = commander_descend::browsable_partitions(path)?;
        Ok(Self::disk_image_from_parts(path, parts))
    }

    /// Number of volumes the container exposes.
    pub fn volume_count(&self) -> usize {
        self.volumes.len()
    }

    /// Lazily open (and cache) volume `idx`'s real filesystem.
    fn volume_fs(
        &mut self,
        idx: usize,
    ) -> Result<&mut (dyn Filesystem + 'static), FilesystemError> {
        let path = self.path.clone();
        let vol = self
            .volumes
            .get_mut(idx)
            .ok_or_else(|| FilesystemError::NotFound(format!("volume {idx}")))?;
        if vol.fs.is_none() {
            let opened = vol
                .opener
                .open(&path, Some(vol.label.clone()))
                .map_err(|e| {
                    FilesystemError::Parse(format!("open volume '{}': {e:#}", vol.label))
                })?;
            vol.fs = Some(opened);
        }
        Ok(vol.fs.as_mut().expect("just opened").as_mut())
    }
}

/// Split a virtual path (`"/{idx}"` or `"/{idx}/{native}"`) into the volume
/// index and the native path within that volume (`"/"` for the volume root).
/// Returns `None` for the adapter root (`"/"`) or any malformed path. Only the
/// leading `/{digits}` segment is consumed, so a native segment containing `/`
/// (legal on HFS) is preserved intact.
fn split_volume(path: &str) -> Option<(usize, String)> {
    let rest = path.strip_prefix('/')?;
    if rest.is_empty() {
        return None; // the adapter root itself
    }
    let (idx_str, native) = match rest.split_once('/') {
        Some((i, r)) => (i, format!("/{r}")),
        None => (rest, "/".to_string()),
    };
    let idx: usize = idx_str.parse().ok()?;
    Some((idx, native))
}

/// Re-root a volume-native child under the `"/{idx}"` prefix so its path is
/// unique across the virtual tree.
fn prefix_child(idx: usize, mut child: FileEntry) -> FileEntry {
    child.path = format!("/{idx}{}", child.path);
    child
}

/// Clone `entry` with its path rewritten to the volume-native `native_path`
/// (all other fields — including `location` — preserved) for delegation.
fn native_entry(entry: &FileEntry, native_path: String) -> FileEntry {
    let mut e = entry.clone();
    e.path = native_path;
    e
}

/// A friendly root-node label for one partition of a disk image. Mirrors the
/// Commander partition dropdown (`"{n}: {type} ({size})"`), prepending the
/// AmigaDOS drive name (e.g. `DH0`) when the RDB records one.
fn partition_node_label(p: &PartitionInfo) -> String {
    let size = crate::partition::format_size(p.size_bytes);
    match p.drv_name.as_deref() {
        Some(d) if !d.trim().is_empty() => {
            format!("{}: {} {} ({})", p.index + 1, d, p.type_name, size)
        }
        _ => format!("{}: {} ({})", p.index + 1, p.type_name, size),
    }
}

impl Filesystem for MultiVolumeFilesystem {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory(
            "/".to_string(),
            "/".to_string(),
            0,
        ))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        match split_volume(&entry.path) {
            // The adapter root: one directory per volume, in enumeration order.
            None => Ok(self
                .volumes
                .iter()
                .enumerate()
                .map(|(i, v)| FileEntry::new_directory(v.label.clone(), format!("/{i}"), i as u64))
                .collect()),
            // Inside a volume: delegate to its real filesystem, re-rooting the
            // returned children under the volume's `"/{idx}"` prefix.
            Some((idx, native)) => {
                let is_vol_root = native == "/";
                let fs = self.volume_fs(idx)?;
                let dir = if is_vol_root {
                    fs.root()?
                } else {
                    native_entry(entry, native)
                };
                let kids = fs.list_directory(&dir)?;
                Ok(kids.into_iter().map(|k| prefix_child(idx, k)).collect())
            }
        }
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let (idx, native) = split_volume(&entry.path)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        let ne = native_entry(entry, native);
        self.volume_fs(idx)?.read_file(&ne, max_bytes)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let (idx, native) = split_volume(&entry.path)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        let ne = native_entry(entry, native);
        self.volume_fs(idx)?.write_file_to(&ne, writer)
    }

    fn write_resource_fork_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let (idx, native) = split_volume(&entry.path)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        let ne = native_entry(entry, native);
        self.volume_fs(idx)?.write_resource_fork_to(&ne, writer)
    }

    fn resource_fork_size(&mut self, entry: &FileEntry) -> u64 {
        let Some((idx, native)) = split_volume(&entry.path) else {
            return 0;
        };
        let ne = native_entry(entry, native);
        match self.volume_fs(idx) {
            Ok(fs) => fs.resource_fork_size(&ne),
            Err(_) => 0,
        }
    }

    fn volume_label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    fn fs_type(&self) -> &str {
        &self.fs_type
    }

    fn total_size(&self) -> u64 {
        0
    }

    fn used_size(&self) -> u64 {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::entry::EntryType;
    use std::collections::HashMap;

    #[test]
    fn split_volume_maps_paths_and_preserves_inner_slashes() {
        assert_eq!(split_volume("/"), None); // adapter root
        assert_eq!(split_volume("/0"), Some((0, "/".to_string())));
        assert_eq!(split_volume("/12"), Some((12, "/".to_string())));
        assert_eq!(split_volume("/0/FOO"), Some((0, "/FOO".to_string())));
        // An HFS filename may contain '/'; only the leading /{idx} is consumed.
        assert_eq!(
            split_volume("/1/My/Weird/Name"),
            Some((1, "/My/Weird/Name".to_string()))
        );
        // Malformed: non-numeric volume segment.
        assert_eq!(split_volume("/abc/x"), None);
        assert_eq!(split_volume("relative"), None);
    }

    #[test]
    fn prefix_child_round_trips_with_split_volume() {
        let child = FileEntry::new_file("Read Me".into(), "/System Folder/Read Me".into(), 3, 7);
        let virt = prefix_child(1, child);
        assert_eq!(virt.path, "/1/System Folder/Read Me");
        let (idx, native) = split_volume(&virt.path).unwrap();
        assert_eq!(idx, 1);
        assert_eq!(native, "/System Folder/Read Me");
    }

    /// A minimal path-resolving in-memory filesystem, standing in for a real
    /// per-volume driver. Resolves purely by `entry.path` (like the optical
    /// backend) so the adapter's path remapping is what's under test.
    struct MockFs {
        // path -> (is_dir, data, children-paths)
        nodes: HashMap<String, (bool, Vec<u8>, Vec<String>)>,
        vol: &'static str,
    }

    impl MockFs {
        fn new(vol: &'static str) -> Self {
            let mut nodes = HashMap::new();
            nodes.insert(
                "/".to_string(),
                (true, vec![], vec!["/hi.txt".into(), "/sub".into()]),
            );
            nodes.insert(
                "/hi.txt".to_string(),
                (false, format!("hello-{vol}").into_bytes(), vec![]),
            );
            nodes.insert(
                "/sub".to_string(),
                (true, vec![], vec!["/sub/deep.txt".into()]),
            );
            nodes.insert(
                "/sub/deep.txt".to_string(),
                (false, format!("deep-{vol}").into_bytes(), vec![]),
            );
            MockFs { nodes, vol }
        }
    }

    impl Filesystem for MockFs {
        fn root(&mut self) -> Result<FileEntry, FilesystemError> {
            Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
        }
        fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
            let (_, _, kids) = self
                .nodes
                .get(&entry.path)
                .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?
                .clone();
            Ok(kids
                .iter()
                .map(|p| {
                    let (is_dir, data, _) = &self.nodes[p];
                    let name = p.rsplit('/').next().unwrap().to_string();
                    if *is_dir {
                        FileEntry::new_directory(name, p.clone(), 0)
                    } else {
                        FileEntry::new_file(name, p.clone(), data.len() as u64, 0)
                    }
                })
                .collect())
        }
        fn read_file(
            &mut self,
            entry: &FileEntry,
            max_bytes: usize,
        ) -> Result<Vec<u8>, FilesystemError> {
            let (_, data, _) = self
                .nodes
                .get(&entry.path)
                .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
            let mut d = data.clone();
            d.truncate(max_bytes);
            Ok(d)
        }
        fn volume_label(&self) -> Option<&str> {
            Some(self.vol)
        }
        fn fs_type(&self) -> &str {
            "MockFs"
        }
        fn total_size(&self) -> u64 {
            0
        }
        fn used_size(&self) -> u64 {
            0
        }
    }

    /// Build an adapter over two already-open mock volumes.
    fn two_mock_volumes() -> MultiVolumeFilesystem {
        let volumes = vec![
            Volume {
                label: "PC_SIDE (ISO 9660)".into(),
                opener: VolumeOpener::Preopened,
                fs: Some(Box::new(MockFs::new("iso"))),
            },
            Volume {
                label: "Mac Side (HFS)".into(),
                opener: VolumeOpener::Preopened,
                fs: Some(Box::new(MockFs::new("hfs"))),
            },
        ];
        MultiVolumeFilesystem {
            path: PathBuf::from("/dev/null"),
            volumes,
            label: Some("disc.iso".into()),
            fs_type: "Hybrid disc".into(),
        }
    }

    #[test]
    fn root_lists_one_directory_per_volume() {
        let mut fs = two_mock_volumes();
        let root = fs.root().unwrap();
        let vols = fs.list_directory(&root).unwrap();
        assert_eq!(vols.len(), 2);
        assert!(vols.iter().all(|v| v.entry_type == EntryType::Directory));
        assert_eq!(vols[0].name, "PC_SIDE (ISO 9660)");
        assert_eq!(vols[0].path, "/0");
        assert_eq!(vols[1].name, "Mac Side (HFS)");
        assert_eq!(vols[1].path, "/1");
    }

    #[test]
    fn descends_each_volume_and_reads_the_right_side() {
        let mut fs = two_mock_volumes();

        // Volume 0 (path "/0") lists its own root, re-rooted under "/0".
        let vol0 = FileEntry::new_directory("PC_SIDE".into(), "/0".into(), 0);
        let k0 = fs.list_directory(&vol0).unwrap();
        let names0: Vec<_> = k0.iter().map(|e| e.name.as_str()).collect();
        assert!(names0.contains(&"hi.txt") && names0.contains(&"sub"));
        assert!(k0.iter().all(|e| e.path.starts_with("/0/")));

        // Read a file from each side: the data proves the right volume answered.
        let hi0 = k0.iter().find(|e| e.name == "hi.txt").unwrap();
        assert_eq!(fs.read_file(hi0, usize::MAX).unwrap(), b"hello-iso");

        // A deep, nested path on volume 1 resolves through the same machinery.
        let deep1 = FileEntry::new_file("deep.txt".into(), "/1/sub/deep.txt".into(), 8, 0);
        let mut out = Vec::new();
        let n = fs.write_file_to(&deep1, &mut out).unwrap();
        assert_eq!(out, b"deep-hfs");
        assert_eq!(n as usize, out.len());
    }

    #[test]
    fn missing_volume_and_root_read_error_cleanly() {
        let mut fs = two_mock_volumes();
        // Reading the adapter root (no volume selected) is NotFound, not a panic.
        let root = FileEntry::new_directory("/".into(), "/".into(), 0);
        assert!(matches!(
            fs.read_file(&root, usize::MAX),
            Err(FilesystemError::NotFound(_))
        ));
        // Out-of-range volume index.
        let ghost = FileEntry::new_file("x".into(), "/9/x".into(), 1, 0);
        assert!(matches!(
            fs.read_file(&ghost, usize::MAX),
            Err(FilesystemError::NotFound(_))
        ));
    }

    /// End-to-end over the committed hybrid optical fixture: the adapter built by
    /// `optical()` lists both filesystems as root nodes and reads a real file
    /// from the primary side, matching a direct primary open.
    #[cfg(feature = "optical")]
    #[test]
    fn optical_hybrid_lists_both_sides_as_root_nodes() {
        use std::io::{Cursor, Read};

        let compressed = std::fs::read("tests/fixtures/optical/hybrid_rsrc.iso.zst")
            .expect("read hybrid fixture");
        let mut dec = zstd::stream::read::Decoder::new(Cursor::new(compressed)).unwrap();
        let mut iso = Vec::new();
        dec.read_to_end(&mut iso).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("disc.iso");
        std::fs::write(&path, &iso).unwrap();

        let mut fs = MultiVolumeFilesystem::optical(&path, Some("disc.iso".into()));
        assert_eq!(fs.volume_count(), 2, "hybrid disc exposes two filesystems");

        let root = fs.root().unwrap();
        let vols = fs.list_directory(&root).unwrap();
        assert_eq!(vols.len(), 2);
        assert!(vols.iter().any(|v| v.name.contains("ISO 9660")));
        assert!(vols.iter().any(|v| v.name.contains("HFS")));

        // Each side lists a non-empty, correctly-prefixed root.
        for v in &vols {
            let kids = fs.list_directory(v).unwrap();
            assert!(!kids.is_empty(), "{} should list entries", v.name);
            let want_prefix = format!("{}/", v.path);
            assert!(kids.iter().all(|k| k.path.starts_with(&want_prefix)));
        }

        // Reading a file through the adapter matches reading it via a direct
        // primary open — proving the /{idx} remap delegates correctly.
        let iso_vol = vols.iter().find(|v| v.name.contains("ISO 9660")).unwrap();
        let iso_kids = fs.list_directory(iso_vol).unwrap();
        let file = iso_kids
            .iter()
            .find(|e| e.entry_type == EntryType::File)
            .expect("iso side has at least one file");
        let via_adapter = fs.read_file(file, usize::MAX).unwrap();

        let mut direct = commander_descend::open_optical(&path, None).unwrap();
        let (_, native) = split_volume(&file.path).unwrap();
        let dnative = native_entry(file, native);
        let via_direct = direct.read_file(&dnative, usize::MAX).unwrap();
        assert_eq!(via_adapter, via_direct);
    }
}
