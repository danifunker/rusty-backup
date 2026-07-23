//! Editable SquashFS — an in-memory tree, rebuilt on sync.
//!
//! `docs/squashfs_edit.md` phase 2c. SquashFS has no in-place write story, so
//! editing is: read the whole image into a [`BuildNode`] tree once, mutate the
//! tree for every `create_file` / `create_directory` / `delete_entry` /
//! `rename`, and rebuild the entire image on `sync_metadata`. That mirrors how
//! `mksquashfs` works and is the only shape the format allows.
//!
//! # Fidelity
//!
//! The tree comes from [`SquashfsFilesystem::read_build_tree`], so every
//! unchanged file keeps its exact bytes, mode, ownership, timestamps and
//! **extended attributes** — including the `security.capability` bits that make
//! an appliance binary work. The rebuild preserves the source's compressor and
//! block size (decision D3). New files get their POSIX attributes through the
//! shared [`crate::fs::attrs`] resolver (D6). The one gap in this slice: a file
//! that is *replaced* (deleted then recreated) loses its xattrs, because
//! `CreateFileOptions` has no channel to carry them — noted, and narrow (only
//! the handful of capability-bearing binaries, and only when overwritten).
//!
//! # Memory & safety
//!
//! Content is held in RAM (the tree's `FileContent::Bytes`), so peak use is the
//! decompressed image — fine for AppImages and appliance images, and the lazy
//! streaming / block reuse noted in the module docs of `squashfs_write` is the
//! follow-up. `sync_metadata` rebuilds into a buffer *first* and only overwrites
//! the backing store once the whole rebuild has succeeded, so a mid-rebuild
//! failure leaves the original image intact.
//!
//! # Where the image lives, and how big it may get
//!
//! A rebuild produces a whole new image, so the question "does it still fit?"
//! has to be asked explicitly — there is no allocator to ask. Two numbers
//! answer it (`docs/squashfs_edit.md` §2):
//!
//! - the **container capacity**: how many bytes the image may occupy where it
//!   sits. `None` for a bare `.squashfs` at offset 0 — the file *is* the
//!   filesystem, so it simply grows. `Some(len)` for a partition, where
//!   overrunning the end would scribble over the next one.
//! - the **[`SizeBudget`]**: an additional ceiling the user asked for.
//!
//! The lower of the two is enforced twice: once at open, so an impossible
//! request is named before any edit is made, and once after the rebuild but
//! **before anything is overwritten**, so a rebuild that turned out too large
//! leaves the original image intact.

use std::io::{Read, Seek, SeekFrom, Write};

use crate::partition::format_size;

use super::attrs::{resolve_attrs, resolve_dir_attrs, AttrOverrides};
use super::entry::{EntryType, FileEntry};
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
use super::squashfs::SquashfsFilesystem;
use super::squashfs_write::{
    compressor_is_writable, image_footprint, write_squashfs, BuildKind, BuildNode, BuildOptions,
    FileContent,
};

/// How large the rebuilt image is allowed to be (`docs/squashfs_edit.md` §2).
///
/// This is **not** slack inside the filesystem — padding a SquashFS with zeroes
/// buys nothing, since you cannot allocate into it later without another full
/// rebuild. It is the ceiling on the region the image occupies, and the slack
/// it implies lives *between the end of the image and the end of its
/// container*, so a future rebuild can grow without re-cutting the container.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeBudget {
    /// Accept whatever the rebuild produces. Still bounded by the container.
    Fit,
    /// The rebuilt image must be at most this many bytes.
    Limit(u64),
}

impl SizeBudget {
    /// `--grow N`: the current image size plus `headroom` bytes.
    pub fn headroom(current_len: u64, headroom: u64) -> Self {
        Self::Limit(current_len.saturating_add(headroom))
    }

    /// The ceiling this budget imposes, if any.
    pub fn ceiling(&self) -> Option<u64> {
        match *self {
            Self::Fit => None,
            Self::Limit(n) => Some(n),
        }
    }
}

/// A projection of what the rebuilt image will occupy
/// (`docs/squashfs_edit.md` §2.5).
///
/// SquashFS cannot answer "will this fit?" the way a bitmap-backed filesystem
/// can — the true answer only exists once the rebuild has run. This is the
/// informed estimate that turns "how much room do you want?" into a question
/// the user can actually answer, and it is deliberately a **range**: the
/// anchor (the source image's own size) is exact, the delta is not.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SizeProjection {
    /// Best estimate of the rebuilt image size, in bytes.
    pub estimate: u64,
    /// Lower bound — added content turns out to compress unusually well.
    pub low: u64,
    /// Upper bound — added content turns out to be incompressible.
    pub high: u64,
    /// The source image's observed ratio (image bytes / file content bytes).
    pub ratio: f64,
}

/// Ratio assumed for added content in the optimistic bound — roughly what a
/// well-compressing text tree achieves.
const BEST_CASE_RATIO: f64 = 0.02;
/// Ratio assumed for added content in the pessimistic bound: already-compressed
/// input stores verbatim, plus a little per-block bookkeeping.
const WORST_CASE_RATIO: f64 = 1.02;

/// An editable SquashFS image backed by an in-memory tree.
pub struct SquashfsEditor<RW: Read + Write + Seek> {
    rw: RW,
    /// Byte offset of the image within `rw` (0 for a bare superfloppy).
    offset: u64,
    /// Bytes the image may occupy in its container, or `None` when it can grow
    /// freely (a bare `.squashfs` file). Always enforced, budget or not.
    capacity: Option<u64>,
    /// The user's requested ceiling, on top of `capacity`.
    budget: SizeBudget,
    /// Footprint of the image we opened, for `--grow` and for the projection's
    /// anchor.
    source_len: u64,
    /// Sum of file content bytes in the source tree, for the observed ratio.
    source_content_len: u64,
    /// The editable tree; the root node's name is empty by convention.
    root: BuildNode,
    /// Compression settings carried from the source (D3).
    opts: BuildOptions,
}

impl<RW: Read + Write + Seek> SquashfsEditor<RW> {
    /// Open a bare `.squashfs` at `offset` for editing, free to grow.
    ///
    /// Equivalent to [`open_within`](Self::open_within) with no container
    /// capacity and no budget. Use that instead for a partition-hosted image.
    pub fn open(rw: RW, offset: u64) -> Result<Self, FilesystemError> {
        Self::open_within(rw, offset, None, SizeBudget::Fit)
    }

    /// Open `rw` (a whole SquashFS image at `offset`) for editing: read the tree
    /// into memory and reclaim the handle for the eventual rebuild.
    ///
    /// `capacity` is how many bytes the image may occupy where it lives — the
    /// partition length for a partition-hosted image, `None` for a bare file.
    /// `budget` is any further ceiling the user asked for; the two are enforced
    /// together, so `SizeBudget::Fit` never means "may overrun the partition".
    ///
    /// Refuses here (stage 1 of §2.4) when the requested budget is larger than
    /// the container can hold: that is a decision the user should revisit
    /// *before* making edits, not after a multi-minute rebuild.
    pub fn open_within(
        rw: RW,
        offset: u64,
        capacity: Option<u64>,
        budget: SizeBudget,
    ) -> Result<Self, FilesystemError> {
        let mut src = SquashfsFilesystem::open(rw, offset)?;
        let opts = src.source_build_options();
        if !compressor_is_writable(opts.compressor) {
            return Err(FilesystemError::Unsupported(format!(
                "squashfs: cannot edit a {}-compressed image — we read it but \
                 have no encoder to rebuild it (gzip / XZ / zstd only)",
                opts.compressor.name()
            )));
        }
        // What the image occupies is `bytes_used` padded out, and the padded
        // figure is what a ceiling has to be compared against.
        let source_len = image_footprint(src.bytes_used());
        let root = src.read_build_tree()?;
        let rw = src.into_inner();
        let source_content_len = Self::content_len(&root);

        // §2.3. At offset 0 the file is the filesystem and simply grows. Any
        // other offset means the image is hosted inside something; when the
        // caller did not say how much room that leaves, fall back to the image's
        // own size — a shrinking edit still works, and a growing one is refused
        // by name instead of overwriting whatever follows.
        let capacity = match (capacity, offset) {
            (Some(c), _) => Some(c),
            (None, 0) => None,
            (None, _) => Some(source_len),
        };

        if let (Some(cap), Some(want)) = (capacity, budget.ceiling()) {
            if want > cap {
                return Err(FilesystemError::Unsupported(format!(
                    "squashfs: a size budget of {} exceeds the {} this image has \
                     to live in — lower the budget, or make room after it first",
                    format_size(want),
                    format_size(cap)
                )));
            }
        }

        Ok(Self {
            rw,
            offset,
            capacity,
            budget,
            source_len,
            source_content_len,
            root,
            opts,
        })
    }

    /// The effective ceiling on a rebuild, and why it applies. `None` means the
    /// image may grow freely.
    fn ceiling(&self) -> Option<(u64, &'static str)> {
        match (self.capacity, self.budget.ceiling()) {
            (None, None) => None,
            (Some(c), None) => Some((c, "the room available where this image lives")),
            (None, Some(b)) => Some((b, "the requested size budget")),
            (Some(c), Some(b)) if b < c => Some((b, "the requested size budget")),
            (Some(c), Some(_)) => Some((c, "the room available where this image lives")),
        }
    }

    /// The size the image occupied when it was opened. `--grow N` is
    /// `SizeBudget::headroom(this, N)`.
    pub fn source_len(&self) -> u64 {
        self.source_len
    }

    /// Total file content bytes under `node`, following the tree.
    fn content_len(node: &BuildNode) -> u64 {
        match &node.kind {
            BuildKind::Dir(children) => children.iter().map(Self::content_len).sum(),
            BuildKind::File(c) => c.len(),
            _ => 0,
        }
    }

    /// Project what a rebuild would occupy right now (`§2.5`).
    ///
    /// Anchored on the source image's actual size rather than re-estimating the
    /// whole tree, because the common edit touches a sliver of a large image:
    /// only the content *delta* is guessed, at the ratio this image itself was
    /// built at. That leaves two sources of error, both small and both inside
    /// the reported range — how the added bytes actually compress, and the
    /// sub-percent difference between our encoder's output and the original's.
    pub fn project_size(&self) -> SizeProjection {
        let content = Self::content_len(&self.root);
        let ratio = if self.source_content_len == 0 {
            1.0
        } else {
            (self.source_len as f64 / self.source_content_len as f64).clamp(0.001, 1.5)
        };
        let added = content.saturating_sub(self.source_content_len) as f64;
        let removed = self.source_content_len.saturating_sub(content) as f64;
        let anchor = self.source_len as f64;

        let clamp = |v: f64| v.max(0.0).min(u64::MAX as f64) as u64;
        let estimate = clamp(anchor + (added - removed) * ratio);
        let low = clamp(anchor + added * BEST_CASE_RATIO - removed);
        let high = clamp(anchor + added * WORST_CASE_RATIO - removed * BEST_CASE_RATIO);
        SizeProjection {
            estimate,
            low: low.min(estimate),
            high: high.max(estimate),
            ratio,
        }
    }

    /// Split a filesystem path into its non-empty components.
    fn components(path: &str) -> Vec<&str> {
        path.split('/').filter(|c| !c.is_empty()).collect()
    }

    /// Immutable node lookup by absolute path (`"/"` is the root).
    fn node_at(&self, path: &str) -> Option<&BuildNode> {
        let mut node = &self.root;
        for comp in Self::components(path) {
            let BuildKind::Dir(children) = &node.kind else {
                return None;
            };
            node = children.iter().find(|c| c.name == comp)?;
        }
        Some(node)
    }

    /// The children Vec of the directory at `path`, for mutation. Errors if the
    /// path is missing or names a non-directory.
    fn children_at_mut(&mut self, path: &str) -> Result<&mut Vec<BuildNode>, FilesystemError> {
        let mut node = &mut self.root;
        for comp in Self::components(path) {
            let BuildKind::Dir(children) = &mut node.kind else {
                return Err(FilesystemError::NotADirectory(path.to_string()));
            };
            node = children
                .iter_mut()
                .find(|c| c.name == comp)
                .ok_or_else(|| FilesystemError::NotFound(path.to_string()))?;
        }
        match &mut node.kind {
            BuildKind::Dir(children) => Ok(children),
            _ => Err(FilesystemError::NotADirectory(path.to_string())),
        }
    }

    /// Mutable node lookup by absolute path (`"/"` is the root itself).
    fn node_at_mut(&mut self, path: &str) -> Result<&mut BuildNode, FilesystemError> {
        let mut node = &mut self.root;
        for comp in Self::components(path) {
            let BuildKind::Dir(children) = &mut node.kind else {
                return Err(FilesystemError::NotADirectory(path.to_string()));
            };
            node = children
                .iter_mut()
                .find(|c| c.name == comp)
                .ok_or_else(|| FilesystemError::NotFound(path.to_string()))?;
        }
        Ok(node)
    }

    /// Build a browse `FileEntry` for `node` at `path`.
    fn to_entry(node: &BuildNode, path: &str) -> FileEntry {
        let mut fe = match &node.kind {
            BuildKind::Dir(_) => FileEntry::new_directory(node.name.clone(), path.to_string(), 0),
            BuildKind::File(c) => {
                FileEntry::new_file(node.name.clone(), path.to_string(), c.len(), 0)
            }
            BuildKind::Symlink(target) => FileEntry::new_symlink(
                node.name.clone(),
                path.to_string(),
                target.len() as u64,
                0,
                target.clone(),
            ),
            _ => {
                let mut e = FileEntry::new_file(node.name.clone(), path.to_string(), 0, 0);
                e.entry_type = EntryType::Special;
                e
            }
        };
        fe.mode = Some(node.unix_mode());
        fe.uid = Some(node.uid);
        fe.gid = Some(node.gid);
        if node.mtime != 0 {
            fe.modified = Some(super::unix_common::inode::format_unix_timestamp(
                node.mtime as i64,
            ));
        }
        if let BuildKind::BlockDev { major, minor } | BuildKind::CharDev { major, minor } =
            &node.kind
        {
            let kind = if matches!(node.kind, BuildKind::BlockDev { .. }) {
                "block device"
            } else {
                "char device"
            };
            fe.special_type = Some(format!("{kind} ({major}, {minor})"));
        }
        fe
    }

    /// Join a parent path and a child name into a normalized absolute path.
    fn child_path(parent: &str, name: &str) -> String {
        if parent == "/" || parent.is_empty() {
            format!("/{name}")
        } else {
            format!("{}/{name}", parent.trim_end_matches('/'))
        }
    }

    /// Reject a name a SquashFS directory entry cannot hold.
    fn check_name(name: &str) -> Result<(), FilesystemError> {
        if name.is_empty() || name == "." || name == ".." {
            return Err(FilesystemError::InvalidData(format!(
                "invalid name {name:?}"
            )));
        }
        if name.contains('/') {
            return Err(FilesystemError::InvalidData(
                "a SquashFS name may not contain '/'".into(),
            ));
        }
        if name.len() > 256 {
            return Err(FilesystemError::InvalidData(
                "a SquashFS name may be at most 256 bytes".into(),
            ));
        }
        Ok(())
    }
}

impl<RW: Read + Write + Seek + Send> Filesystem for SquashfsEditor<RW> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(Self::to_entry(&self.root, "/"))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let node = self
            .node_at(&entry.path)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        let BuildKind::Dir(children) = &node.kind else {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        };
        Ok(children
            .iter()
            .map(|c| Self::to_entry(c, &Self::child_path(&entry.path, &c.name)))
            .collect())
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let node = self
            .node_at(&entry.path)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        match &node.kind {
            BuildKind::File(FileContent::Bytes(b)) => Ok(b[..b.len().min(max_bytes)].to_vec()),
            BuildKind::File(FileContent::HostFile { path, .. }) => {
                let f = std::fs::File::open(path).map_err(FilesystemError::Io)?;
                let mut out = Vec::new();
                f.take(max_bytes as u64)
                    .read_to_end(&mut out)
                    .map_err(FilesystemError::Io)?;
                Ok(out)
            }
            BuildKind::Symlink(target) => Ok(target.clone().into_bytes()),
            _ => Err(FilesystemError::InvalidData(format!(
                "squashfs: {} is not a regular file",
                entry.path
            ))),
        }
    }

    fn volume_label(&self) -> Option<&str> {
        None
    }

    fn supports_xattrs(&self) -> bool {
        true
    }

    fn list_xattrs(
        &mut self,
        entry: &FileEntry,
    ) -> Result<Vec<super::xattr::Xattr>, FilesystemError> {
        Ok(self
            .node_at(&entry.path)
            .map(|n| n.xattrs.clone())
            .unwrap_or_default())
    }

    fn fs_type(&self) -> &str {
        "SquashFS"
    }

    fn total_size(&self) -> u64 {
        // What the image is allowed to occupy. Absent any ceiling it can grow
        // freely, and the honest stand-in is what it occupies today.
        self.ceiling().map(|(c, _)| c).unwrap_or(self.source_len)
    }

    fn used_size(&self) -> u64 {
        // Everything in a SquashFS is live by construction, so "used" is the
        // whole image — projected, since the real figure only exists after a
        // rebuild.
        self.project_size().estimate
    }
}

impl<RW: Read + Write + Seek + Send> EditableFilesystem for SquashfsEditor<RW> {
    fn as_filesystem(&self) -> &dyn Filesystem {
        self
    }

    fn as_filesystem_mut(&mut self) -> &mut dyn Filesystem {
        self
    }

    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn std::io::Read,
        _data_len: u64,
        options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Self::check_name(name)?;
        // Resolve POSIX attributes through the shared policy. A create over an
        // existing name would have deleted the old entry first (the put flow
        // does), so `replacing` is None here; ownership falls back to the parent
        // directory. `options.mode` carries the full mode (type bits + perms)
        // the drivers expect; we keep only the permission bits for the node.
        let parent_entry = self
            .node_at(&parent.path)
            .map(|n| Self::to_entry(n, &parent.path));
        let attrs = resolve_attrs(
            &AttrOverrides {
                mode: options.mode.map(|m| m & 0o7777),
                uid: options.uid,
                gid: options.gid,
            },
            None,
            parent_entry.as_ref(),
            None,
            0o644,
        );

        let mut bytes = Vec::new();
        data.read_to_end(&mut bytes).map_err(FilesystemError::Io)?;

        let children = self.children_at_mut(&parent.path)?;
        if children.iter().any(|c| c.name == name) {
            return Err(FilesystemError::AlreadyExists(Self::child_path(
                &parent.path,
                name,
            )));
        }
        children.push(BuildNode {
            name: name.to_string(),
            mode: attrs.mode as u16,
            uid: attrs.uid,
            gid: attrs.gid,
            mtime: 0,
            xattrs: Vec::new(),
            kind: BuildKind::File(FileContent::Bytes(bytes)),
        });
        Ok(Self::to_entry(
            children.last().unwrap(),
            &Self::child_path(&parent.path, name),
        ))
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Self::check_name(name)?;
        let parent_entry = self
            .node_at(&parent.path)
            .map(|n| Self::to_entry(n, &parent.path));
        let attrs = resolve_dir_attrs(
            &AttrOverrides {
                mode: options.mode.map(|m| m & 0o7777),
                uid: options.uid,
                gid: options.gid,
            },
            None,
            parent_entry.as_ref(),
        );

        let children = self.children_at_mut(&parent.path)?;
        if children.iter().any(|c| c.name == name) {
            return Err(FilesystemError::AlreadyExists(Self::child_path(
                &parent.path,
                name,
            )));
        }
        children.push(BuildNode {
            name: name.to_string(),
            mode: attrs.mode as u16,
            uid: attrs.uid,
            gid: attrs.gid,
            mtime: 0,
            xattrs: Vec::new(),
            kind: BuildKind::Dir(Vec::new()),
        });
        Ok(Self::to_entry(
            children.last().unwrap(),
            &Self::child_path(&parent.path, name),
        ))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        let children = self.children_at_mut(&parent.path)?;
        let idx = children
            .iter()
            .position(|c| c.name == entry.name)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        if let BuildKind::Dir(inner) = &children[idx].kind {
            if !inner.is_empty() {
                return Err(FilesystemError::InvalidData(format!(
                    "squashfs delete_entry: directory '{}' not empty",
                    entry.path
                )));
            }
        }
        children.remove(idx);
        Ok(())
    }

    fn rename(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
        new_name: &str,
    ) -> Result<(), FilesystemError> {
        Self::check_name(new_name)?;
        let children = self.children_at_mut(&parent.path)?;
        if children.iter().any(|c| c.name == new_name) {
            return Err(FilesystemError::AlreadyExists(Self::child_path(
                &parent.path,
                new_name,
            )));
        }
        let node = children
            .iter_mut()
            .find(|c| c.name == entry.name)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;
        node.name = new_name.to_string();
        Ok(())
    }

    fn supports_symlinks(&self) -> bool {
        true
    }

    fn create_symlink(
        &mut self,
        parent: &FileEntry,
        name: &str,
        target: &str,
        options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Self::check_name(name)?;
        let uid = options.uid.unwrap_or(0);
        let gid = options.gid.unwrap_or(0);
        let children = self.children_at_mut(&parent.path)?;
        if children.iter().any(|c| c.name == name) {
            return Err(FilesystemError::AlreadyExists(Self::child_path(
                &parent.path,
                name,
            )));
        }
        children.push(BuildNode {
            name: name.to_string(),
            // Symlinks are conventionally 0777; the value is not used by
            // resolvers but keeps `ls -l` honest.
            mode: 0o777,
            uid,
            gid,
            mtime: 0,
            xattrs: Vec::new(),
            kind: BuildKind::Symlink(target.to_string()),
        });
        Ok(Self::to_entry(
            children.last().unwrap(),
            &Self::child_path(&parent.path, name),
        ))
    }

    fn set_permissions(&mut self, entry: &FileEntry, mode: u32) -> Result<(), FilesystemError> {
        self.node_at_mut(&entry.path)?.mode = (mode & 0o7777) as u16;
        Ok(())
    }

    fn set_owner(&mut self, entry: &FileEntry, uid: u32, gid: u32) -> Result<(), FilesystemError> {
        let node = self.node_at_mut(&entry.path)?;
        node.uid = uid;
        node.gid = gid;
        Ok(())
    }

    fn set_xattr(
        &mut self,
        entry: &FileEntry,
        name: &str,
        value: &[u8],
    ) -> Result<(), FilesystemError> {
        if !super::xattr::has_valid_namespace(name) {
            return Err(FilesystemError::InvalidData(format!(
                "xattr {name:?} has no representable namespace prefix \
                 (user. / trusted. / security. / system.)"
            )));
        }
        let node = self.node_at_mut(&entry.path)?;
        // Replace an existing attribute of the same name, else append — matches
        // setxattr(2) without XATTR_CREATE / XATTR_REPLACE flags.
        if let Some(x) = node.xattrs.iter_mut().find(|x| x.name == name) {
            x.value = value.to_vec();
        } else {
            node.xattrs.push(super::xattr::Xattr {
                name: name.to_string(),
                value: value.to_vec(),
            });
        }
        Ok(())
    }

    fn remove_xattr(&mut self, entry: &FileEntry, name: &str) -> Result<(), FilesystemError> {
        // Removing an absent attribute is a no-op, not an error.
        self.node_at_mut(&entry.path)?
            .xattrs
            .retain(|x| x.name != name);
        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        // Rebuild into a buffer first: only overwrite the backing store once the
        // whole image is known-good, so a failed rebuild can't corrupt the
        // original. (Whole image in RAM — see the module docs.)
        let mut buf = std::io::Cursor::new(Vec::new());
        write_squashfs(&mut buf, &self.root, &self.opts)?;
        let image = buf.into_inner();

        // Stage 2 of §2.4. The rebuild has succeeded but nothing has been
        // overwritten yet, so a too-large result costs the user their time and
        // nothing else. Report both numbers: "too big" without them leaves no
        // way to pick a budget that would have worked.
        if let Some((ceiling, why)) = self.ceiling() {
            if image.len() as u64 > ceiling {
                return Err(FilesystemError::DiskFull(format!(
                    "squashfs: the rebuilt image is {} but {why} is {} — the \
                     original image is unchanged. Delete something, or allow \
                     the image to grow.",
                    format_size(image.len() as u64),
                    format_size(ceiling)
                )));
            }
        }

        self.rw
            .seek(SeekFrom::Start(self.offset))
            .map_err(FilesystemError::Io)?;
        self.rw.write_all(&image).map_err(FilesystemError::Io)?;
        self.rw.flush().map_err(FilesystemError::Io)?;
        // Note: if the new image is shorter than the old backing file, trailing
        // bytes remain past `bytes_used`. SquashFS readers stop at the
        // superblock's `bytes_used`, so this is still a valid image; exact
        // truncation is a commit-side concern (temp + rename, phase 2d).
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        // An estimate, necessarily: the true figure does not exist until the
        // rebuild runs, and `sync_metadata` is what actually enforces the
        // ceiling. A bare file with no ceiling simply grows.
        Ok(match self.ceiling() {
            Some((ceiling, _)) => ceiling.saturating_sub(self.project_size().estimate),
            None => u64::MAX,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::squashfs::Xattr;
    use crate::fs::squashfs_write::BuildKind as BK;
    use std::io::Cursor;

    /// True when `unsquashfs` is on PATH (it exits 1 while printing its banner,
    /// so match text, not status).
    fn unsquashfs_available() -> bool {
        std::process::Command::new("unsquashfs")
            .arg("-version")
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).contains("unsquashfs version"))
            .unwrap_or(false)
    }

    /// Build a starter image with a couple of files, a directory, and — the
    /// point — a file carrying an xattr, so we can prove edits preserve it.
    fn starter_image() -> Vec<u8> {
        let tree = BuildNode {
            name: String::new(),
            mode: 0o755,
            uid: 0,
            gid: 0,
            mtime: 1_700_000_000,
            xattrs: Vec::new(),
            kind: BK::Dir(vec![
                BuildNode::file("readme", 0o644, b"original readme\n".to_vec()),
                BuildNode::dir(
                    "bin",
                    0o755,
                    vec![BuildNode {
                        name: "ping".into(),
                        mode: 0o4755, // setuid
                        uid: 0,
                        gid: 0,
                        mtime: 1_700_000_000,
                        xattrs: vec![Xattr {
                            name: "security.capability".into(),
                            value: vec![1, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0],
                        }],
                        kind: BK::File(FileContent::Bytes(b"ping binary\n".to_vec())),
                    }],
                ),
            ]),
        };
        let mut cur = Cursor::new(Vec::new());
        write_squashfs(&mut cur, &tree, &BuildOptions::default()).expect("build starter");
        cur.into_inner()
    }

    fn open_editor(bytes: Vec<u8>) -> SquashfsEditor<Cursor<Vec<u8>>> {
        SquashfsEditor::open(Cursor::new(bytes), 0).expect("open editor")
    }

    /// Incompressible bytes, so a size-budget test measures the budget rather
    /// than the compressor. A plain LCG: no rand dependency, and reproducible.
    fn incompressible(len: usize) -> Vec<u8> {
        let mut state: u64 = 0x2545_F491_4F6C_DD1D;
        (0..len)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                (state >> 33) as u8
            })
            .collect()
    }

    /// A starter image planted at `offset` inside a larger backing store, as a
    /// partition would be, with `slack` spare bytes after it.
    fn hosted_image(offset: u64, slack: u64) -> (Vec<u8>, u64) {
        let img = starter_image();
        let partition_len = img.len() as u64 + slack;
        let mut disk = vec![0u8; (offset + partition_len) as usize];
        disk[offset as usize..offset as usize + img.len()].copy_from_slice(&img);
        (disk, partition_len)
    }

    /// The point of the whole size budget: a rebuild that outgrows its
    /// partition is refused, and the original image survives intact.
    #[test]
    fn a_rebuild_that_overflows_its_partition_is_refused() {
        const OFFSET: u64 = 1 << 20;
        let (disk, partition_len) = hosted_image(OFFSET, 64 * 1024);
        let before = disk.clone();
        let mut ed = SquashfsEditor::open_within(
            Cursor::new(disk),
            OFFSET,
            Some(partition_len),
            SizeBudget::Fit,
        )
        .expect("open hosted");

        let root = ed.root().expect("root");
        ed.create_file(
            &root,
            "big.bin",
            &mut Cursor::new(incompressible(2 << 20)),
            2 << 20,
            &CreateFileOptions::default(),
        )
        .expect("create");

        let err = ed.sync_metadata().expect_err("must refuse to overflow");
        let msg = err.to_string();
        assert!(
            msg.contains("rebuilt image is") && msg.contains("unchanged"),
            "expected an actual-vs-budget refusal, got: {msg}"
        );

        // Nothing was written: the backing store still holds the original image,
        // and everything after the partition is still zero.
        let after = std::mem::replace(&mut ed.rw, Cursor::new(Vec::new())).into_inner();
        assert_eq!(after, before, "a refused rebuild still touched the disk");
    }

    /// The same edit against a partition with room to hold it goes through, and
    /// stays inside the partition.
    #[test]
    fn a_rebuild_that_fits_its_partition_is_written_at_the_offset() {
        const OFFSET: u64 = 1 << 20;
        let (disk, partition_len) = hosted_image(OFFSET, 4 << 20);
        let mut ed = SquashfsEditor::open_within(
            Cursor::new(disk),
            OFFSET,
            Some(partition_len),
            SizeBudget::Fit,
        )
        .expect("open hosted");
        let root = ed.root().expect("root");
        ed.create_file(
            &root,
            "added",
            &mut Cursor::new(b"added\n".to_vec()),
            6,
            &CreateFileOptions::default(),
        )
        .expect("create");
        ed.sync_metadata().expect("sync must fit");

        let disk = std::mem::replace(&mut ed.rw, Cursor::new(Vec::new())).into_inner();
        assert_eq!(
            disk.len() as u64,
            OFFSET + partition_len,
            "the rebuild grew the backing store past the partition"
        );
        let mut fs = SquashfsFilesystem::open(Cursor::new(disk), OFFSET).expect("reopen at offset");
        let root = fs.root().unwrap();
        let names: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(names.contains(&"added".to_string()), "got: {names:?}");
    }

    /// Deleting into a partition too small to hold a *grown* image still works
    /// — the ceiling constrains the result, not the operation.
    #[test]
    fn a_shrinking_edit_fits_a_tight_partition() {
        const OFFSET: u64 = 512 * 1024;
        // No slack at all: only an image at most as large as the original fits.
        let (disk, partition_len) = hosted_image(OFFSET, 0);
        let mut ed = SquashfsEditor::open_within(
            Cursor::new(disk),
            OFFSET,
            Some(partition_len),
            SizeBudget::Fit,
        )
        .expect("open hosted");
        let root = ed.root().expect("root");
        let readme = ed
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "readme")
            .unwrap();
        ed.delete_entry(&root, &readme).expect("delete");
        ed.sync_metadata().expect("a smaller image must still fit");
    }

    /// Stage 1 of the two-stage enforcement: a budget the container cannot hold
    /// is named at open, before the user makes any edits.
    #[test]
    fn a_budget_larger_than_the_container_is_refused_at_open() {
        const OFFSET: u64 = 1 << 20;
        let (disk, partition_len) = hosted_image(OFFSET, 64 * 1024);
        let err = SquashfsEditor::open_within(
            Cursor::new(disk),
            OFFSET,
            Some(partition_len),
            SizeBudget::Limit(partition_len * 4),
        )
        .err()
        .expect("must refuse a budget it cannot honour");
        assert!(
            err.to_string().contains("size budget"),
            "expected a budget-vs-container refusal, got: {err}"
        );
    }

    /// A budget *below* the container is the binding ceiling, and says so.
    #[test]
    fn an_explicit_budget_below_the_container_is_what_binds() {
        const OFFSET: u64 = 1 << 20;
        let (disk, partition_len) = hosted_image(OFFSET, 8 << 20);
        let img_len = partition_len - (8 << 20);
        let mut ed = SquashfsEditor::open_within(
            Cursor::new(disk),
            OFFSET,
            Some(partition_len),
            SizeBudget::headroom(img_len, 8 * 1024),
        )
        .expect("open hosted");
        let root = ed.root().expect("root");
        ed.create_file(
            &root,
            "big.bin",
            &mut Cursor::new(incompressible(1 << 20)),
            1 << 20,
            &CreateFileOptions::default(),
        )
        .expect("create");
        let err = ed.sync_metadata().expect_err("must refuse");
        assert!(
            err.to_string().contains("requested size budget"),
            "the budget, not the partition, should be named: {err}"
        );
    }

    /// An image at a non-zero offset whose container size nobody declared may
    /// not grow — we cannot know what follows it.
    #[test]
    fn an_undeclared_container_caps_growth_at_the_current_size() {
        const OFFSET: u64 = 1 << 20;
        let (disk, _) = hosted_image(OFFSET, 8 << 20);
        let mut ed = SquashfsEditor::open_within(Cursor::new(disk), OFFSET, None, SizeBudget::Fit)
            .expect("open hosted");
        let root = ed.root().expect("root");
        ed.create_file(
            &root,
            "big.bin",
            &mut Cursor::new(incompressible(1 << 20)),
            1 << 20,
            &CreateFileOptions::default(),
        )
        .expect("create");
        ed.sync_metadata()
            .expect_err("growth past an unknown boundary must be refused");
    }

    /// A bare `.squashfs` at offset 0 is the file itself, so it grows freely.
    #[test]
    fn a_bare_image_grows_freely() {
        let mut ed = open_editor(starter_image());
        let root = ed.root().expect("root");
        ed.create_file(
            &root,
            "big.bin",
            &mut Cursor::new(incompressible(1 << 20)),
            1 << 20,
            &CreateFileOptions::default(),
        )
        .expect("create");
        assert_eq!(ed.free_space().unwrap(), u64::MAX, "a bare file has no cap");
        ed.sync_metadata().expect("a bare file simply grows");
        let bytes = std::mem::replace(&mut ed.rw, Cursor::new(Vec::new())).into_inner();
        assert!(
            bytes.len() > 1 << 20,
            "the added megabyte did not land: {} bytes",
            bytes.len()
        );
    }

    /// The projection has to move with the tree, and bracket its own estimate.
    #[test]
    fn the_projection_tracks_added_content() {
        let mut ed = open_editor(starter_image());
        let before = ed.project_size();
        assert_eq!(
            before.estimate,
            ed.source_len(),
            "an untouched tree should project its own size"
        );

        let root = ed.root().expect("root");
        ed.create_file(
            &root,
            "big.bin",
            &mut Cursor::new(incompressible(1 << 20)),
            1 << 20,
            &CreateFileOptions::default(),
        )
        .expect("create");
        let after = ed.project_size();
        assert!(
            after.estimate > before.estimate,
            "adding a megabyte did not move the projection"
        );
        assert!(
            after.low <= after.estimate && after.estimate <= after.high,
            "estimate outside its own range: {after:?}"
        );

        // Incompressible content: the true size lands near the pessimistic end,
        // and the range must contain it.
        ed.sync_metadata().expect("sync");
        let actual = std::mem::replace(&mut ed.rw, Cursor::new(Vec::new()))
            .into_inner()
            .len() as u64;
        assert!(
            actual >= after.low && actual <= after.high,
            "actual {actual} outside the projected range {}..={}",
            after.low,
            after.high
        );
    }

    #[test]
    fn create_delete_mkdir_rename_then_rebuild() {
        let mut ed = open_editor(starter_image());
        let root = ed.root().expect("root");

        // Add a file, a directory, and a symlink; rename readme; delete nothing
        // yet.
        ed.create_file(
            &root,
            "hello.txt",
            &mut Cursor::new(b"hi\n".to_vec()),
            3,
            &CreateFileOptions {
                mode: Some(0o100_600),
                uid: Some(0),
                gid: Some(0),
                ..Default::default()
            },
        )
        .expect("create hello");
        ed.create_directory(&root, "etc", &CreateDirectoryOptions::default())
            .expect("mkdir etc");
        ed.create_symlink(&root, "link", "hello.txt", &CreateFileOptions::default())
            .expect("symlink");
        let readme = ed
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "readme")
            .unwrap();
        ed.rename(&root, &readme, "README").expect("rename");

        // Rebuild, reopen through the reader, and check the tree.
        ed.sync_metadata().expect("sync");
        let bytes = std::mem::replace(&mut ed.rw, Cursor::new(Vec::new())).into_inner();
        let mut fs = SquashfsFilesystem::open(Cursor::new(bytes), 0).expect("reopen");
        let root = fs.root().unwrap();
        let names: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(names.contains(&"hello.txt".to_string()));
        assert!(names.contains(&"etc".to_string()));
        assert!(names.contains(&"link".to_string()));
        assert!(
            names.contains(&"README".to_string()),
            "rename not applied: {names:?}"
        );
        assert!(
            !names.contains(&"readme".to_string()),
            "old name survived: {names:?}"
        );

        let hello = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "hello.txt")
            .unwrap();
        assert_eq!(fs.read_file(&hello, 100).unwrap(), b"hi\n");
        assert_eq!(
            hello.mode.map(|m| m & 0o7777),
            Some(0o600),
            "explicit mode not applied"
        );
    }

    /// The heart of D4: an edit that touches other files must not disturb an
    /// unchanged file's extended attributes.
    #[test]
    fn unchanged_files_keep_their_xattrs_across_an_edit() {
        let mut ed = open_editor(starter_image());
        let root = ed.root().expect("root");
        // Touch something unrelated.
        ed.create_file(
            &root,
            "new.txt",
            &mut Cursor::new(b"new\n".to_vec()),
            4,
            &CreateFileOptions::default(),
        )
        .expect("create");
        ed.sync_metadata().expect("sync");

        let bytes = std::mem::replace(&mut ed.rw, Cursor::new(Vec::new())).into_inner();
        let mut fs = SquashfsFilesystem::open(Cursor::new(bytes), 0).expect("reopen");
        // Re-read the tree and confirm /bin/ping still has its capability.
        let tree = fs.read_build_tree().expect("read tree");
        let BK::Dir(top) = &tree.kind else { panic!() };
        let bin = top.iter().find(|n| n.name == "bin").expect("bin");
        let BK::Dir(binc) = &bin.kind else { panic!() };
        let ping = binc.iter().find(|n| n.name == "ping").expect("ping");
        assert_eq!(
            ping.xattrs,
            vec![Xattr {
                name: "security.capability".into(),
                value: vec![1, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0],
            }],
            "an unrelated edit dropped ping's capability xattr"
        );
        assert_eq!(ping.mode & 0o7777, 0o4755, "setuid bit lost");
    }

    #[test]
    fn new_file_inherits_parent_ownership() {
        let mut ed = open_editor(starter_image());
        let root = ed.root().expect("root");
        // Make a dir owned by uid/gid 5, then create a file inside it with no
        // explicit ownership: it should inherit 5:5.
        let dir = ed
            .create_directory(
                &root,
                "owned",
                &CreateDirectoryOptions {
                    uid: Some(5),
                    gid: Some(5),
                    ..Default::default()
                },
            )
            .expect("mkdir");
        let f = ed
            .create_file(
                &dir,
                "child",
                &mut Cursor::new(b"x".to_vec()),
                1,
                &CreateFileOptions::default(),
            )
            .expect("create child");
        assert_eq!(f.uid, Some(5), "child did not inherit parent uid");
        assert_eq!(f.gid, Some(5), "child did not inherit parent gid");
    }

    #[test]
    fn delete_refuses_a_nonempty_directory() {
        let mut ed = open_editor(starter_image());
        let root = ed.root().expect("root");
        let bin = ed
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "bin")
            .unwrap();
        let err = ed.delete_entry(&root, &bin).expect_err("should refuse");
        assert!(err.to_string().contains("not empty"), "got: {err}");
        // Emptying it first makes the delete succeed.
        let ping = ed.list_directory(&bin).unwrap().into_iter().next().unwrap();
        ed.delete_entry(&bin, &ping).expect("delete ping");
        ed.delete_entry(&root, &bin).expect("delete now-empty bin");
    }

    /// set_permissions / set_owner / set_xattr / remove_xattr must all mutate
    /// the tree and survive a rebuild.
    #[test]
    fn metadata_edits_survive_rebuild() {
        let mut ed = open_editor(starter_image());
        let root = ed.root().expect("root");
        let readme = ed
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "readme")
            .unwrap();

        ed.set_permissions(&readme, 0o640).expect("chmod");
        ed.set_owner(&readme, 33, 44).expect("chown");
        ed.set_xattr(&readme, "user.mime_type", b"text/plain")
            .expect("setfattr");
        assert!(ed.supports_xattrs());

        // Also drop the capability xattr off /bin/ping via remove_xattr.
        let bin = ed
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "bin")
            .unwrap();
        let ping = ed.list_directory(&bin).unwrap().into_iter().next().unwrap();
        assert_eq!(ed.list_xattrs(&ping).unwrap().len(), 1);
        ed.remove_xattr(&ping, "security.capability")
            .expect("rm xattr");
        assert!(ed.list_xattrs(&ping).unwrap().is_empty());

        ed.sync_metadata().expect("sync");
        let bytes = std::mem::replace(&mut ed.rw, Cursor::new(Vec::new())).into_inner();
        let mut fs = SquashfsFilesystem::open(Cursor::new(bytes), 0).expect("reopen");
        let tree = fs.read_build_tree().expect("read tree");
        let BK::Dir(top) = &tree.kind else { panic!() };
        let readme = top.iter().find(|n| n.name == "readme").unwrap();
        assert_eq!(readme.mode & 0o7777, 0o640, "mode not persisted");
        assert_eq!((readme.uid, readme.gid), (33, 44), "owner not persisted");
        assert_eq!(
            readme.xattrs,
            vec![Xattr {
                name: "user.mime_type".into(),
                value: b"text/plain".to_vec(),
            }],
            "xattr not persisted"
        );
        let bin = top.iter().find(|n| n.name == "bin").unwrap();
        let BK::Dir(binc) = &bin.kind else { panic!() };
        let ping = binc.iter().find(|n| n.name == "ping").unwrap();
        assert!(ping.xattrs.is_empty(), "removed xattr came back");
    }

    /// An xattr with no valid namespace prefix is refused (it can't be
    /// represented on disk).
    #[test]
    fn set_xattr_rejects_bad_namespace() {
        let mut ed = open_editor(starter_image());
        let root = ed.root().expect("root");
        let readme = ed
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "readme")
            .unwrap();
        let err = ed
            .set_xattr(&readme, "bogus.attr", b"x")
            .expect_err("bad namespace must fail");
        assert!(err.to_string().contains("namespace"), "got: {err}");
    }

    #[test]
    fn refuses_to_edit_an_unwritable_compressor() {
        // Forge a superblock claiming LZO (id 3); the editor must refuse at open
        // rather than fail later during a rebuild.
        let mut bytes = starter_image();
        bytes[20..22].copy_from_slice(&3u16.to_le_bytes()); // compressor id
        let err = SquashfsEditor::open(Cursor::new(bytes), 0)
            .err()
            .expect("must refuse");
        assert!(
            err.to_string().contains("lzo") || err.to_string().contains("LZO"),
            "expected an LZO-specific refusal, got: {err}"
        );
    }

    /// The rebuilt image after an edit must be real SquashFS `unsquashfs` reads.
    #[test]
    fn unsquashfs_accepts_an_edited_image() {
        if !unsquashfs_available() {
            eprintln!("unsquashfs not on PATH — skipping");
            return;
        }
        let mut ed = open_editor(starter_image());
        let root = ed.root().expect("root");
        ed.create_file(
            &root,
            "added.txt",
            &mut Cursor::new(b"added by editor\n".to_vec()),
            16,
            &CreateFileOptions::default(),
        )
        .expect("create");
        ed.sync_metadata().expect("sync");
        let bytes = std::mem::replace(&mut ed.rw, Cursor::new(Vec::new())).into_inner();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("edited.squashfs");
        std::fs::write(&path, &bytes).unwrap();
        let out = std::process::Command::new("unsquashfs")
            .arg("-no-xattrs")
            .arg("-d")
            .arg(dir.path().join("x"))
            .arg(&path)
            .output()
            .expect("run unsquashfs");
        assert!(
            !String::from_utf8_lossy(&out.stderr).contains("Data queue"),
            "unsquashfs choked on our edited image:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            std::fs::read(dir.path().join("x/added.txt")).unwrap(),
            b"added by editor\n"
        );
        assert_eq!(
            std::fs::read(dir.path().join("x/readme")).unwrap(),
            b"original readme\n",
            "an unchanged file changed content across the edit"
        );
    }
}
