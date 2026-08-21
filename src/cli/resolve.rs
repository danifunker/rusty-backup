//! Resolve `IMG[@N]` references to a (file handle, partition context)
//! pair that the filesystem-dispatch helpers in `src/fs/mod.rs` can
//! consume directly.
//!
//! The CLI verbs use this to support every filesystem the engine layer
//! knows about — instead of each verb hand-rolling APM/MBR/GPT/RDB
//! parsing and HFS-specific dispatch, they go through
//! [`resolve_partition`] and get back the metadata `open_filesystem` /
//! `open_editable_filesystem` need.

use anyhow::{anyhow, bail, Context, Result};
use std::fs::File;
use std::io::{Read, Seek};

use crate::cli::backup_edit;
use crate::cli::img_at::PartSelector;
use crate::cli::io::{open_image_ro, open_image_rw};
use crate::model::source_reader;
use crate::partition::{PartitionInfo, PartitionTable};
use crate::rbformats::{BoxReadSeek, BoxRwSeek};
use std::path::PathBuf; // used by RwCommit::Cbk regardless of the chd feature

// Unconditional since the AppImage branch logs its payload offset; it used to
// be `chd`-only, which the vintage build (no `chd` feature) catches at once.
use crate::cli::logging::log_stderr;
#[cfg(feature = "chd")]
use crate::rbformats::chd_edit;
#[cfg(feature = "chd")]
use std::path::Path;

/// Resolved partition context — what to pass to
/// [`crate::fs::open_filesystem`] / [`crate::fs::open_editable_filesystem`].
#[derive(Debug, Clone)]
pub struct PartitionContext {
    /// Byte offset of the filesystem inside the image.
    pub offset: u64,
    /// Partition type byte (MBR-style). `0x00` for "auto-detect via
    /// superfloppy probing" — `open_filesystem` walks the magic
    /// signatures itself.
    pub type_byte: u8,
    /// APM / RDB type string (e.g. `"Apple_HFS"`, `"PFS\\3"`). `None`
    /// for MBR / GPT / raw superfloppy.
    pub type_string: Option<String>,
    /// How the read path names this filesystem, e.g. `"Alto BFS"`. Carried so a
    /// refusal can say what the volume is: content probing on the write path
    /// returns "unknown" for filesystems identified by their container, which
    /// told the user the disk was unreadable a moment after `ls` read it
    /// (R-034).
    pub type_name: String,
    /// Partition size in bytes. For raw superfloppies, the image's
    /// total length.
    pub size: u64,
    /// Human-readable label, intended for `eprintln!` so the user can
    /// confirm which partition the verb is operating on.
    pub label: String,
    /// Set only when the read-write handle is the image file itself *and* the
    /// filesystem occupies all of it — a plain image opened in place, not a
    /// decoded container temp, a CHD/QCOW2 session, or a partition inside a
    /// larger disk. Lets a driver that rewrites its whole image commit by
    /// atomic replacement. See [`crate::fs::EditContext::whole_file_path`].
    pub whole_file_path: Option<PathBuf>,
    /// A size ceiling the user asked for, for the drivers that rewrite their
    /// whole image. Set by the verbs that expose `--size` / `--grow`; `None`
    /// everywhere else means "no request", and the container still binds.
    pub rebuild_budget: Option<crate::fs::squashfs_edit::SizeBudget>,
}

impl PartitionContext {
    /// Open this partition's filesystem for editing.
    ///
    /// Every mutating verb goes through here rather than calling
    /// [`crate::fs::open_editable_filesystem`] itself, so they all pass the
    /// partition's **length** as well as its offset. Most drivers do not care
    /// — they write inside a structure they read from the partition. SquashFS
    /// does: it rebuilds the whole image, so without a declared length it has
    /// no way to know that growing would run into the next partition.
    /// Errors are returned raw so each verb keeps its own wording ("for write",
    /// "for repair", "destination filesystem …").
    /// Open the resolved partition read-only, telling the driver how long it
    /// is. The counterpart of [`Self::open_editable`], which has always passed
    /// `partition_len`; the read side had no way to and so mis-sized every
    /// AFFS partition that was not last on its disk (R-042).
    pub fn open_ro<R: Read + Seek + Send + 'static>(
        &self,
        handle: R,
        passphrase: Option<&str>,
    ) -> Result<Box<dyn crate::fs::filesystem::Filesystem>, crate::fs::filesystem::FilesystemError>
    {
        crate::fs::open_filesystem_full(
            handle,
            self.offset,
            // Zero means the resolver had nothing to report, not an empty
            // partition — the same reading `open_editable` takes.
            (self.size > 0).then_some(self.size),
            self.type_byte,
            self.type_string.as_deref(),
            passphrase,
        )
    }

    pub fn open_editable<R: Read + std::io::Write + Seek + Send + 'static>(
        &self,
        handle: R,
    ) -> std::result::Result<
        Box<dyn crate::fs::EditableFilesystem>,
        crate::fs::filesystem::FilesystemError,
    > {
        // Rewrite only the "we could not name it" case; every other
        // Unsupported message is specific and better than anything here.
        let name_it = |e: crate::fs::filesystem::FilesystemError| match &e {
            crate::fs::filesystem::FilesystemError::Unsupported(m) if m.contains("'unknown'") => {
                crate::fs::filesystem::FilesystemError::Unsupported(
                    m.replace("'unknown'", &format!("'{}'", self.type_name)),
                )
            }
            _ => e,
        };
        crate::fs::open_editable_filesystem_with(
            handle,
            self.offset,
            crate::fs::EditContext {
                // A zero size means the resolver had nothing to report, not a
                // zero-length partition; pass "unknown" rather than "no room".
                partition_len: (self.size > 0).then_some(self.size),
                whole_file_path: self.whole_file_path.as_deref(),
                rebuild_budget: self.rebuild_budget,
            },
            self.type_byte,
            self.type_string.as_deref(),
        )
        .map_err(name_it)
    }
}

/// Fail with `NOT_FOUND` when the source simply is not there.
///
/// `exit.rs` reserves 3 for "image file missing" and `inspect` has honoured it
/// since R-010 — with a per-verb `exists()` check that never spread. Every
/// other verb surfaced the raw `io::Error` as the catch-all 1, so a script
/// could not tell "no disk" from "bad disk", and the message was a
/// platform-specific syscall error (R-036). Living here means a verb added
/// later inherits it.
///
/// Three things are deliberately exempt, because none of them is a file whose
/// absence we can judge: a raw device (`\\.\PhysicalDrive0` does not
/// `exists()`), an `rb://` remote reference, and anything the caller has
/// already peeled to a temp.
pub fn require_source_exists(path: &std::path::Path) -> Result<()> {
    if path.exists() {
        return Ok(());
    }
    if crate::cli::device_safety::looks_like_device_path(path) {
        return Ok(());
    }
    if path.to_string_lossy().starts_with("rb://") {
        return Ok(());
    }
    Err(crate::cli::exit::not_found(format!(
        "{}: no such file",
        path.display()
    )))
}

/// Open `path` read-only and resolve which partition to use.
///
/// - When `selector` is `None` and the image has no partition table,
///   returns `offset=0, type_byte=0` (auto-detect at byte 0).
/// - When `selector` is `None` and there's a single FS-shaped partition,
///   that partition is selected.
/// - When `selector` is `None` and multiple FS partitions exist, errors
///   with a list of indices.
/// - When `selector` is `Some(n)`, the 1-based index is used; out-of-
///   range fires `NOT_FOUND`.
pub fn resolve_partition_ro(
    path: &std::path::Path,
    selector: Option<PartSelector>,
) -> Result<(File, PartitionContext)> {
    require_source_exists(path)?;
    let mut file = open_image_ro(path)?;
    let ctx = resolve(&mut file, selector)?;
    Ok((file, ctx))
}

/// Returned by [`resolve_partition_rw`] alongside the read+write handle.
/// Call [`RwCommit::commit`] once a mutation has succeeded (after the
/// `EditableFilesystem` has been synced) to persist it.
///
/// Three shapes, matching how the GUI persists each source kind:
/// - [`RwCommit::None`] — raw image / device: writes already landed; no-op.
/// - [`RwCommit::Container`] — floppy (.d88 / .xdf / .hdm / .dim / .atr) or
///   gzip (.adz / .hdz) container: the handle points at a decoded temp flat,
///   and `commit` re-encodes it back into the container, atomically replacing
///   the original (the same `ContainerEditSession` path the GUI uses).
/// - [`RwCommit::Chd`] — a CHD edited via `chd_edit`: an uncompressed CHD was
///   mutated in place (`diff: None`, no-op commit); a compressed CHD routed
///   writes into a diff (`diff: Some`) that `commit` merges + recompresses
///   back over the original, logging each step.
///
/// **Dropping without committing discards container / compressed-CHD edits**,
/// so a verb that errors out before `commit` leaves the original untouched.
#[must_use = "call commit() to persist edits made to a container"]
pub enum RwCommit {
    /// Raw image / device — writes already landed in place.
    None,
    /// Floppy or gzip container — re-encode the temp flat on commit.
    Container(crate::model::container_edit::ContainerEditSession),
    /// CHD edited through `chd_edit`. `diff: None` is an in-place
    /// uncompressed edit (nothing to do on commit); `diff: Some(path)` is a
    /// compressed-CHD diff that must be flattened back over `parent`.
    #[cfg(feature = "chd")]
    Chd {
        parent: PathBuf,
        diff: Option<PathBuf>,
    },
    /// A partition inside a backup folder, edited via its decompressed temp
    /// flat — recompressed back over the archive + `metadata.json` rewritten on
    /// commit. See [`crate::cli::backup_edit`].
    BackupArchive(crate::cli::backup_edit::BackupArchiveCommit),
    /// A partition inside a `.cbk` container: the container was materialized to
    /// `temp_folder`, the partition edited via the inner backup-folder commit,
    /// and on commit the inner commit runs (recompress + metadata) and the
    /// folder is repacked over the original `.cbk` (write-to-temp + rename).
    Cbk {
        inner: Box<RwCommit>,
        temp_folder: tempfile::TempDir,
        cbk_path: PathBuf,
    },
}

impl RwCommit {
    /// Persist the edit. No-op for raw images / in-place CHDs; re-encodes for
    /// containers; merges + recompresses for compressed CHDs.
    pub fn commit(self) -> Result<()> {
        match self {
            RwCommit::None => Ok(()),
            RwCommit::Container(session) => {
                let fmt = session.format_name();
                session
                    .commit()
                    .map_err(|e| anyhow!("re-encoding {fmt} container: {e:#}"))
            }
            #[cfg(feature = "chd")]
            RwCommit::Chd { diff: None, .. } => Ok(()),
            #[cfg(feature = "chd")]
            RwCommit::Chd {
                parent,
                diff: Some(diff),
            } => flatten_chd_with_progress(&parent, &diff),
            RwCommit::BackupArchive(commit) => commit.commit(),
            RwCommit::Cbk {
                inner,
                temp_folder,
                cbk_path,
            } => {
                // Persist the partition edit into the materialized folder
                // (recompress partition-N.gz + rewrite metadata.json), then
                // repack the folder over the original .cbk atomically.
                inner.commit()?;
                let tmp_out = cbk_path.with_extension("cbk.tmp");
                crate::rbformats::cbk::pack_folder_to_cbk(temp_folder.path(), &tmp_out)
                    .with_context(|| format!("repacking {}", cbk_path.display()))?;
                crate::os::replace_file(&tmp_out, &cbk_path).with_context(|| {
                    format!(
                        "replacing {} with the repacked container",
                        cbk_path.display()
                    )
                })?;
                Ok(())
            }
        }
    }
}

/// Merge a compressed CHD's edit diff back over the original via
/// [`chd_edit::flatten_to_parent`], surfacing each phase + recompression
/// progress to stderr so a multi-GB flatten doesn't look hung. Mirrors the
/// GUI's background flatten, minus the worker thread (the CLI is one-shot).
#[cfg(feature = "chd")]
fn flatten_chd_with_progress(parent: &Path, diff: &Path) -> Result<()> {
    log_stderr("Saving CHD edits: merging diff and recompressing the image...");
    let mut last_logged_mb: u64 = 0;
    let mut progress_cb = |bytes: u64| {
        let mb = bytes / (1024 * 1024);
        if mb >= last_logged_mb + 256 {
            last_logged_mb = mb;
            log_stderr(format!("  recompressed {mb} MB so far..."));
        }
    };
    let cancel = || false;
    let mut log_cb = |msg: &str| log_stderr(format!("  {msg}"));
    chd_edit::flatten_to_parent(parent, diff, None, &mut progress_cb, &cancel, &mut log_cb)
        .map_err(|e| anyhow!("recompressing edited CHD: {e:#}"))?;
    log_stderr("CHD edits saved.");
    Ok(())
}

/// Sibling diff path for a compressed CHD edit: `<stem>.edit-diff.chd`.
/// Matches the GUI's naming (`browse_view::diff_path_for`) so a diff left by
/// an interrupted edit is recognizable.
#[cfg(feature = "chd")]
fn chd_diff_path(parent: &Path) -> PathBuf {
    let mut name = parent
        .file_stem()
        .map(|s| s.to_os_string())
        .unwrap_or_default();
    name.push(".edit-diff.chd");
    parent.with_file_name(name)
}

/// Same as [`resolve_partition_ro`] but the handle is open read+write.
/// Caller is responsible for ensuring the path resolves to a regular file
/// (device-write safety lives in Phase C).
///
/// Returns a boxed `Read + Write + Seek` handle plus a [`RwCommit`] the
/// caller must `commit()` after a successful mutation. The handle is boxed
/// (rather than a bare [`File`]) so a CHD edit session — a `Read + Write +
/// Seek` adapter, not a `File` — can flow through the same path. For a
/// container or compressed CHD the handle is backed by a temp flat / diff and
/// `commit` re-encodes / flattens. See [`RwCommit`].
pub fn resolve_partition_rw(
    path: &std::path::Path,
    selector: Option<PartSelector>,
) -> Result<(BoxRwSeek, PartitionContext, RwCommit)> {
    resolve_partition_rw_forced(path, selector, None)
}

/// As [`resolve_partition_rw`], but a `--fs-type` override (`fs_override`)
/// makes a partition-table detection failure non-fatal — see
/// [`resolve_with_override`]. Used by `put` / `rm`, which then call
/// [`FsDispatchOverride::apply`] to install the override.
pub fn resolve_partition_rw_forced(
    path: &std::path::Path,
    selector: Option<PartSelector>,
    fs_override: Option<&str>,
) -> Result<(BoxRwSeek, PartitionContext, RwCommit)> {
    // A `.cbk` container: materialize it to a temp folder, edit the partition
    // there via the backup-folder path, and repack over the original `.cbk` on
    // commit (the "additional legwork" edit path — cb_dos_network_and_state.md
    // §2e). Read access to a `.cbk` is native (source_reader); editing repacks.
    require_source_exists(path)?;
    if crate::rbformats::cbk::is_cbk(path) {
        let temp = tempfile::Builder::new()
            .prefix(".rb-cbk-edit-")
            .tempdir()
            .context("creating temp folder for .cbk edit")?;
        crate::rbformats::cbk::materialize_cbk_to_folder(path, temp.path())
            .with_context(|| format!("materializing {} for edit", path.display()))?;
        let (handle, ctx, inner) = backup_edit::open_backup_partition_rw(temp.path(), selector)?;
        return Ok((
            handle,
            ctx,
            RwCommit::Cbk {
                inner: Box::new(inner),
                temp_folder: temp,
                cbk_path: path.to_path_buf(),
            },
        ));
    }

    // A backup folder stores each partition as a compressed file governed by
    // metadata.json, not as a partition inside one image — handle it before
    // the whole-image path (which would try to detect a partition table).
    if backup_edit::is_backup_folder(path) {
        return backup_edit::open_backup_partition_rw(path, selector);
    }

    // Open the whole image read-write (decoding CHD / container as needed),
    // then resolve which partition inside it the caller wants.
    let (mut reader, commit, shape) = resolve_image_rw(path)?;
    let mut ctx = resolve_with_override(&mut reader, selector, fs_override)?;
    // Only a handle that *is* the file, holding a filesystem that starts at its
    // first byte, may be committed by replacing the file — see [`HandleShape`].
    if shape == HandleShape::WholeFile && ctx.offset == 0 {
        ctx.whole_file_path = Some(path.to_path_buf());
    }
    Ok((reader, ctx, commit))
}

/// Open `path` read-write as a boxed whole-image `Read + Write + Seek` handle,
/// decoding a CHD or editable container to its editable backing (a `chd_edit`
/// diff / in-place session, or a `ContainerEditSession` temp flat) with a
/// [`RwCommit`] that flattens / re-encodes on commit. Unlike
/// [`resolve_partition_rw`] this does NOT resolve a partition — the caller
/// works in absolute image offsets. Used by whole-disk verbs (`partmap`,
/// `mac_scsi_bless`) so they edit a CHD / container the same way `put` does.
/// Whether a read-write handle covers its file byte-for-byte.
///
/// `WholeFile` means position 0 of the handle is byte 0 of the path *and* the
/// file holds nothing but the image — the only shape in which replacing the
/// file is the same act as replacing the image. Everything else is `Wrapped`: a
/// decoded temp flat, a CHD or QCOW2 session, or a window onto part of a larger
/// file such as an AppImage's appended payload.
///
/// Stated rather than inferred, because the inference is subtly wrong and the
/// consequence is silent. `RwCommit::None` looks like it means "the raw file",
/// and it doesn't — an AppImage's payload window also needs no re-encoding on
/// commit, so it is `RwCommit::None` too, and treating that as a whole-file
/// handle led SquashFS to atomically replace an entire AppImage with just its
/// payload, deleting the ELF stub that makes it runnable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleShape {
    /// The handle is the file, entirely.
    WholeFile,
    /// The handle is a decode, a session, or a window into something larger.
    Wrapped,
}

pub fn resolve_image_rw(path: &std::path::Path) -> Result<(BoxRwSeek, RwCommit, HandleShape)> {
    if crate::rbformats::appimage::is_squashfs_appimage(path) {
        // An AppImage's SquashFS is appended to an ELF stub and runs to the end
        // of the file, so editing it is a write through a window at the payload
        // offset — nothing is decoded, and nothing needs re-encoding on commit.
        // The payload being the *tail* is what lets a rebuild grow: the file
        // simply gets longer, and the stub in front finds its payload by the
        // same arithmetic either way.
        let file = open_image_rw(path)?;
        let mut probe = file
            .try_clone()
            .with_context(|| format!("opening {}", path.display()))?;
        let offset = crate::rbformats::appimage::squashfs_payload_offset(&mut probe)
            .ok_or_else(|| anyhow!("no SquashFS payload in AppImage {}", path.display()))?;
        log_stderr(format!(
            "AppImage: editing the SquashFS payload at offset {offset}"
        ));
        return Ok((
            Box::new(crate::rbformats::payload_slice::PayloadSlice::tail(
                file, offset,
            )),
            RwCommit::None,
            HandleShape::Wrapped,
        ));
    }
    if source_reader::is_squashfs_bearing_iso(path) {
        // A SquashFS inside an ISO 9660 is a contiguous file wedged between
        // other files on the disc, so it cannot grow — and a SquashFS edit is a
        // whole-image rebuild whose size can't be promised in advance. Rather
        // than offer an edit that would refuse the moment a rebuild came out
        // even slightly larger, decline it up front and point at the paths that
        // do work. (Browse and extract are unaffected — they go through the
        // read path.)
        bail!(
            "{} holds a SquashFS inside an ISO 9660, which can be browsed and \
             extracted but not edited in place: it cannot grow, since other \
             files sit after it on the disc. Extract it (`rb-cli tar` / `get`) \
             and rebuild the ISO, or edit the AppImage form, which can grow.",
            path.display()
        );
    }
    if let Some((handle, commit)) = try_open_chd_rw(path)? {
        return Ok((handle, commit, HandleShape::Wrapped));
    }
    if source_reader::is_qcow2_path(path) {
        // QCOW2 edits in place: `Qcow2Reader` is itself `Read + Write + Seek`,
        // allocating host clusters on demand, so there's nothing to re-encode
        // on commit. Without this branch the raw `File` below would be handed
        // to partition detection, which reads the QCOW2 header as sector 0 and
        // reports a bogus "Invalid MBR".
        let file = open_image_rw(path)?;
        let reader = crate::rbformats::qcow2::Qcow2Reader::open(file)
            .with_context(|| format!("opening QCOW2 {} for edit", path.display()))?;
        // Fail fast with the root cause: a snapshot-bearing image opens
        // read-only (shared clusters, no copy-on-write). The common case is a
        // UTM suspended-VM state, which also leaves the guest filesystem dirty
        // — so surfacing the snapshot here beats a downstream "dirty journal"
        // message that names the symptom instead.
        if reader.is_read_only() {
            anyhow::bail!(
                "QCOW2 {} has {} internal snapshot(s) and opens read-only \
                 (a UTM suspended-VM state is the usual one). Editing could \
                 corrupt them. Shut the VM down cleanly in UTM, or drop the \
                 snapshot (`qemu-img snapshot -d <name> {}`), then retry.",
                path.display(),
                reader.snapshot_count(),
                path.display(),
            );
        }
        return Ok((Box::new(reader), RwCommit::None, HandleShape::Wrapped));
    }
    // A container the read path decodes but the write path cannot re-encode:
    // G64/G71 raw GCR, MSA, EDSK, Apple-II .dsk. Refusing here says so; falling
    // through opened the undecoded container bytes and reported "Invalid MBR:
    // invalid boot signature", which reads as a corrupt disk a moment after
    // `ls` listed its files (R-011, same shape as R-034).
    // Named explicitly rather than derived as "flat-floppy minus editable":
    // that set also caught Apple-II `.dsk`, which is a plain sector image the
    // write path handles fine, and refusing it broke `new floppy apple-dos`
    // round-trips. These three re-encode their bytes on read — GCR bitstream,
    // MSA run-length, EDSK track records — so there is nowhere for a write to go.
    if (source_reader::is_g64_path(path)
        || source_reader::is_msa_path(path)
        || source_reader::is_edsk_path(path))
        && !source_reader::is_editable_container_path(path)
    {
        // Name a raw target, not a specific vintage extension: this branch
        // covers G64/G71, MSA, EDSK and Apple-II .dsk, and suggesting `.d64`
        // for an Apple II disk is worse than suggesting nothing.
        return Err(crate::cli::exit::permission_denied(format!(
            "{}: this container is read-only — it decodes for reading but cannot be \
             re-encoded, so edits would have nowhere to go. Convert it to a raw image \
             first: `rb-cli convert {} OUT.img --format raw`.",
            path.display(),
            path.display(),
        )));
    }
    if source_reader::is_editable_container_path(path) {
        // Floppy / gzip / WOZ container: decode to a temp flat, edit that,
        // re-encode on commit. open_image_rw on the temp gives the same File
        // the raw path would, so downstream dispatch is identical.
        let session = crate::model::container_edit::ContainerEditSession::open(path)
            .map_err(|e| anyhow!("opening container for edit: {e:#}"))?;
        let file = open_image_rw(session.flat_path())?;
        Ok((
            Box::new(file),
            RwCommit::Container(session),
            HandleShape::Wrapped,
        ))
    } else {
        let file = open_image_rw(path)?;
        // The only branch where the handle really is the file.
        Ok((Box::new(file), RwCommit::None, HandleShape::WholeFile))
    }
}

/// CHD read-write open: an uncompressed CHD is mutated in place; a compressed
/// CHD is opened read-only with writes routed into a fresh diff that is
/// flattened back over the original on commit. Mirrors the GUI
/// (`browse_view::enter_chd_edit_mode`). `Ok(None)` when `path` is not a CHD
/// (caller falls through to the container / raw branches).
#[cfg(feature = "chd")]
fn try_open_chd_rw(path: &Path) -> Result<Option<(BoxRwSeek, RwCommit)>> {
    if !source_reader::is_chd_path(path) {
        return Ok(None);
    }
    let compressed = chd_edit::is_compressed_chd(path)
        .map_err(|e| anyhow!("inspecting CHD {}: {e:#}", path.display()))?;
    let resolved: (BoxRwSeek, RwCommit) = if compressed {
        let backup = chd_edit::make_backup_copy(path)
            .map_err(|e| anyhow!("backing up CHD before edit: {e:#}"))?;
        log_stderr(format!(
            "Editing compressed CHD (original preserved at {})",
            backup.display()
        ));
        let diff = chd_diff_path(path);
        if diff.exists() {
            // Leftover diff from an interrupted edit — the parent was never
            // touched, so discard it and start clean.
            let _ = std::fs::remove_file(&diff);
        }
        let session = chd_edit::ChdEditSession::open_with_diff(path, &diff)
            .map_err(|e| anyhow!("opening CHD {} for edit: {e:#}", path.display()))?;
        (
            Box::new(session),
            RwCommit::Chd {
                parent: path.to_path_buf(),
                diff: Some(diff),
            },
        )
    } else {
        let session = chd_edit::ChdEditSession::open_uncompressed(path)
            .map_err(|e| anyhow!("opening CHD {} for edit: {e:#}", path.display()))?;
        (
            Box::new(session),
            RwCommit::Chd {
                parent: path.to_path_buf(),
                diff: None,
            },
        )
    };
    Ok(Some(resolved))
}

/// No-CHD-feature stub: a CHD path falls through to the raw branch (which then
/// fails at partition detection, same as before — a binary without the `chd`
/// feature can't read CHDs at all).
#[cfg(not(feature = "chd"))]
fn try_open_chd_rw(_path: &std::path::Path) -> Result<Option<(BoxRwSeek, RwCommit)>> {
    Ok(None)
}

/// Like [`resolve_partition_ro`] but returns a boxed `Read + Seek`,
/// transparently routing GHO / IMZ / CHD containers through their
/// streaming readers so the caller sees a raw disk image.
pub fn resolve_partition_streaming(
    path: &std::path::Path,
    selector: Option<PartSelector>,
) -> Result<(BoxReadSeek, PartitionContext)> {
    resolve_partition_streaming_with_password(path, selector, None)
}

/// Variant of [`resolve_partition_streaming`] that accepts an optional
/// password for encrypted IMZ files. Pass `None` to behave like
/// [`resolve_partition_streaming`].
pub fn resolve_partition_streaming_with_password(
    path: &std::path::Path,
    selector: Option<PartSelector>,
    password: Option<&[u8]>,
) -> Result<(BoxReadSeek, PartitionContext)> {
    resolve_partition_streaming_forced(path, selector, password, None)
}

/// As [`resolve_partition_streaming_with_password`], but a `--fs-type`
/// override (`fs_override`) makes a partition-table detection failure
/// non-fatal — see [`resolve_with_override`]. Used by `ls` / `get`, which
/// then call [`FsDispatchOverride::apply`] to install the override.
pub fn resolve_partition_streaming_forced(
    path: &std::path::Path,
    selector: Option<PartSelector>,
    password: Option<&[u8]>,
    fs_override: Option<&str>,
) -> Result<(BoxReadSeek, PartitionContext)> {
    resolve_partition_streaming_forced_inside(path, selector, password, fs_override, None)
}

/// As [`resolve_partition_streaming_forced`], but `inside` names a specific
/// entry to open when `path` is a `.zip` holding more than one disk image
/// (the CLI `--inside` flag). Ignored for every non-zip source.
pub fn resolve_partition_streaming_forced_inside(
    path: &std::path::Path,
    selector: Option<PartSelector>,
    password: Option<&[u8]>,
    fs_override: Option<&str>,
    inside: Option<&str>,
) -> Result<(BoxReadSeek, PartitionContext)> {
    // A backup folder stores each partition as a compressed file governed by
    // metadata.json; decompress the selected one to a temp flat (read-only) so
    // get / ls / inspect see it like any other raw partition.
    require_source_exists(path)?;
    if backup_edit::is_backup_folder(path) {
        return backup_edit::open_backup_partition_ro(path, selector);
    }

    // Peel any container *and* any image wrapper through the one shared
    // primitive so the CLI probes a source identically to the GUI: CHD / GHO /
    // IMZ / .zip-wrapped / flat-floppy containers decode to a flat stream, and
    // VHD / 2MG / DMG / DiskCopy 4.2 wrappers are unwrapped (previously the CLI
    // streaming path saw the wrapped bytes for those and mis-detected the
    // partition table). A raw image falls through to a buffered file.
    let mut reader = source_reader::open_peeled_read_with_entry(path, password, inside)?;
    let ctx = resolve_with_override(&mut reader, selector, fs_override)?;
    Ok((reader, ctx))
}

fn resolve<R: Read + Seek>(
    reader: &mut R,
    selector: Option<PartSelector>,
) -> Result<PartitionContext> {
    resolve_with_override(reader, selector, None)
}

/// Resolve which partition a `Read + Seek` source's `IMG[@N]` selector points at,
/// returning the offset / type / size context — the reader-based sibling of
/// [`resolve_partition_ro`]. Used for sources that aren't a local file handle
/// (e.g. a [`crate::remote::RemoteBlockReader`] over the block tier), so a remote
/// image gets the **exact same** `@N` semantics as a local one.
pub fn resolve_partition_in_reader<R: Read + Seek>(
    reader: &mut R,
    selector: Option<PartSelector>,
) -> Result<PartitionContext> {
    resolve(reader, selector)
}

/// As [`resolve`], but `fs_override` (the `--fs-type` value, if any) makes
/// a partition-table detection *failure* non-fatal: when the user has
/// explicitly declared a filesystem, an image with neither a partition
/// table nor an on-disk FS signature (the defining shape of a flat CP/M
/// floppy — the BIOS holds the DPB out-of-band) is treated as a raw FS at
/// byte 0. The override itself is applied by the caller afterwards via
/// [`FsDispatchOverride::apply`].
fn resolve_with_override<R: Read + Seek>(
    reader: &mut R,
    selector: Option<PartSelector>,
    fs_override: Option<&str>,
) -> Result<PartitionContext> {
    let total = reader_size(reader)?;
    let pt = match PartitionTable::detect(reader) {
        Ok(pt) => pt,
        Err(e) => {
            if fs_override.is_some() {
                if let Some(idx) = selector {
                    bail!(
                        "--partition / IMG@{idx} was given with --fs-type, but the image has no \
                         partition table; drop the suffix to operate on the raw filesystem at byte 0"
                    );
                }
                return Ok(PartitionContext {
                    offset: 0,
                    type_byte: 0x00,
                    type_string: None,
                    type_name: "raw".to_string(),
                    size: total,
                    label: "Partition: raw filesystem @ byte 0 (forced via --fs-type)".to_string(),
                    // `resolve` works from a reader and doesn't know whether it
                    // came from the image file or a decoded temp; the read-write
                    // resolver fills this in once it knows which branch it took.
                    whole_file_path: None,
                    rebuild_budget: None,
                });
            }
            return Err(anyhow!("detecting partition table: {e}"));
        }
    };
    let partitions = pt.partitions();

    // No partition table: treat as superfloppy / raw FS at byte 0.
    if partitions.is_empty() {
        if let Some(idx) = selector {
            bail!(
                "--partition / IMG@N specified ({idx}) but image has no partition table; \
                 drop the suffix to operate on the raw filesystem at byte 0"
            );
        }
        return Ok(PartitionContext {
            offset: 0,
            type_byte: 0x00,
            type_string: None,
            type_name: pt.type_name().to_string(),
            size: total,
            label: format!("Partition: raw filesystem @ byte 0 ({})", pt.type_name()),
            whole_file_path: None,
            rebuild_budget: None,
        });
    }

    let info = match selector {
        Some(sel) => select_partition(&pt, &partitions, &sel)?,
        None => pick_default_partition(&partitions)?,
    };

    Ok(PartitionContext {
        offset: info.byte_offset(),
        type_byte: info.partition_type_byte,
        type_string: info.partition_type_string.clone(),
        type_name: info.type_name.clone(),
        size: info.size_bytes,
        label: format_label(&pt, &info, &partitions),
        whole_file_path: None,
        rebuild_budget: None,
    })
}

/// Apply an `IMG@…` selector; each form gets its own failure message.
pub(crate) fn select_partition(
    pt: &PartitionTable,
    partitions: &[PartitionInfo],
    sel: &PartSelector,
) -> Result<PartitionInfo> {
    match sel {
        PartSelector::Position(idx) => {
            let i = *idx as usize;
            if i == 0 || i > partitions.len() {
                bail!(
                    "partition index {idx} out of range (image has {} partition(s))",
                    partitions.len()
                );
            }
            // `index` is the slot: 0- or 1-based per table, and it skips.
            Ok(partitions[i - 1].clone())
        }
        PartSelector::Slot(slot) => {
            if !pt.has_native_slots() {
                bail!(
                    "{} partitions have no slot number to select by — use @N \
                     (the position `inspect` prints) instead",
                    pt.type_name(),
                );
            }
            partitions
                .iter()
                .find(|p| pt.native_slot(p) == Some(*slot))
                .cloned()
                .ok_or_else(|| {
                    anyhow!(
                        "no browsable partition in slot {slot} (this image has {})",
                        list_slots(pt, partitions),
                    )
                })
        }
        PartSelector::Name(name) => {
            let hit = partitions.iter().find(|p| {
                p.drv_name
                    .as_deref()
                    .is_some_and(|d| d.eq_ignore_ascii_case(name))
            });
            match hit {
                Some(p) => Ok(p.clone()),
                None => {
                    let names: Vec<&str> = partitions
                        .iter()
                        .filter_map(|p| p.drv_name.as_deref())
                        .collect();
                    if names.is_empty() {
                        bail!(
                            "@{name} names a device, but {} partitions don't carry device \
                             names — use @N, or @sN for the table's own slot",
                            pt.type_name(),
                        );
                    }
                    bail!(
                        "no partition named {name:?} (this image has {})",
                        names.join(", ")
                    )
                }
            }
        }
    }
}

/// `slot 0, slot 1, slot 8` — for the not-found message.
fn list_slots(pt: &PartitionTable, partitions: &[PartitionInfo]) -> String {
    let slots: Vec<String> = partitions
        .iter()
        .filter_map(|p| pt.native_slot(p).map(|s| format!("slot {s}")))
        .collect();
    if slots.is_empty() {
        "no slots".to_string()
    } else {
        slots.join(", ")
    }
}

/// Default-partition picker.
///
/// - If exactly one partition has a filesystem-shaped type (i.e. not an
///   extended container, not bootable-only, not Apple_Driver*), pick it.
/// - Otherwise raise an error listing the candidates.
fn pick_default_partition(partitions: &[PartitionInfo]) -> Result<PartitionInfo> {
    // Each candidate keeps its *position* in `partitions`, because that - not
    // `PartitionInfo::index` - is what the selector means: the caller above
    // resolves `@N` as `partitions[N - 1]`, precisely because `index` is 0- or
    // 1-based depending on the table type. Listing `index` here would print a
    // number that selects a different partition on any table where the two
    // disagree.
    let candidates: Vec<(usize, &PartitionInfo)> = partitions
        .iter()
        .enumerate()
        .filter(|(_, p)| {
            !p.is_extended_container
                && !p
                    .partition_type_string
                    .as_deref()
                    .map(|s| s.starts_with("Apple_Driver") || s == "Apple_partition_map")
                    .unwrap_or(false)
        })
        .collect();
    match candidates.len() {
        0 => bail!("no usable partition found in image"),
        1 => Ok(candidates[0].1.clone()),
        _ => {
            // Size, not `partition_type_byte`: the byte is an MBR concept and
            // reads `(0)` for every row on APM/GPT/RDB, which tells the user
            // nothing. The size is what distinguishes two same-typed volumes.
            let summary: Vec<String> = candidates
                .iter()
                .map(|(pos, p)| {
                    format!(
                        "  {}  {:<28}  {}",
                        pos + 1,
                        p.type_name,
                        crate::partition::format_size(p.size_bytes)
                    )
                })
                .collect();
            let example = candidates[0].0 + 1;
            bail!(
                "image has multiple filesystem partitions; select one by appending \
                 `@N` to the image path (e.g. `IMAGE@{example}`) or with \
                 `--partition {example}`:\n{}",
                summary.join("\n")
            )
        }
    }
}

fn format_label(pt: &PartitionTable, info: &PartitionInfo, partitions: &[PartitionInfo]) -> String {
    let pt_name = match pt.byte_order_name() {
        Some(order) => format!("{}, {order}", pt.type_name()),
        None => pt.type_name().to_string(),
    };
    // Name it the two ways the user can select it, rather than echoing the raw
    // `index` — which matched neither `@N` nor `@sN` and read as a third answer.
    let pos = partitions
        .iter()
        .position(|p| p.start_lba == info.start_lba && p.size_bytes == info.size_bytes)
        .map(|i| format!("@{}", i + 1))
        .unwrap_or_default();
    let slot = pt
        .native_slot(info)
        .map(|s| format!(" / @s{s}"))
        .unwrap_or_default();
    match &info.partition_type_string {
        Some(s) => format!(
            "Partition {pos}{slot} ({pt_name}): {} {s} @ LBA {}, {} bytes",
            info.type_name, info.start_lba, info.size_bytes
        ),
        None => format!(
            "Partition {pos}{slot} ({pt_name}): {} 0x{:02x} @ LBA {}, {} bytes",
            info.type_name, info.partition_type_byte, info.start_lba, info.size_bytes
        ),
    }
}

fn reader_size<R: Seek>(reader: &mut R) -> Result<u64> {
    let cur = reader.stream_position()?;
    let end = reader.seek(std::io::SeekFrom::End(0))?;
    reader.seek(std::io::SeekFrom::Start(cur))?;
    Ok(end)
}

/// Flatten-able CLI flag group: an explicit filesystem-dispatch override.
///
/// CP/M floppies (Altair, Amstrad, PCW, Einstein, SVI328, MultiComp, ZX
/// +3) carry NO on-disk signature for their FS — the BIOS knows the
/// Disk Parameter Block (DPB) out-of-band. So `open_filesystem` can't
/// autodetect them, and every rb-cli verb operating on a CP/M image
/// needs the user to declare the DPB via `--fs-type cpm:<preset_name>`.
///
/// The same flag can in principle override dispatch for any other
/// `partition_type_string` the engine recognises (e.g. `human68k`,
/// `qdos`, `Apple_HFS`), but the headline use is the CP/M family.
///
/// Flatten into a verb's `Args` and call
/// [`FsDispatchOverride::apply`] right after `resolve_partition_*`.
#[derive(Debug, Clone, Default, clap::Args)]
pub struct FsDispatchOverride {
    /// Force a specific filesystem dispatch. The main use is `cpm:<preset>`
    /// for CP/M images (which have no on-disk signature). Valid CP/M
    /// presets: `amstrad_data`, `amstrad_sys`, `amstrad_pcw`, `einstein`,
    /// `svi328_cpm`, `altair_8in`, `altair_cf`, `multicomp`, `zxplus3`.
    /// Other strings (e.g. `human68k`, `qdos`) are also accepted and
    /// forwarded to the partition_type_string dispatch.
    #[arg(long = "fs-type", value_name = "TYPE")]
    pub fs_type: Option<String>,

    /// Scan the **entire** image for recoverable text in the synthetic carve
    /// view (used for disks with no recognized filesystem — e.g. custom
    /// bootblock Amiga "NDOS" disks). By default the carve view only scans
    /// the first 10 MB. No effect on disks with a real filesystem.
    #[arg(long = "carve-full")]
    pub carve_full: bool,
}

impl FsDispatchOverride {
    /// Apply the override to a resolved [`PartitionContext`] in place.
    /// No-op when `--fs-type` wasn't passed.
    ///
    /// The override replaces `ctx.type_string` and clears `ctx.type_byte`
    /// so the string-dispatch branch in `open_filesystem` wins. Updates
    /// `ctx.label` so the user can confirm the override is in effect.
    ///
    /// Also installs the process-wide carve scan policy from `--carve-full`
    /// so a subsequent carve open scans the whole image instead of the
    /// default first-10-MB window.
    pub fn apply(&self, ctx: &mut PartitionContext) {
        crate::fs::carve::set_full_scan(self.carve_full);
        if let Some(t) = &self.fs_type {
            ctx.type_string = Some(t.clone());
            ctx.type_byte = 0; // Force string-dispatch
            ctx.label = format!("{} [--fs-type {}]", ctx.label, t);
        }
    }
}

/// Classify a failure to open a filesystem for writing.
///
/// `Unsupported` from a write-open means the volume is readable and this build
/// will not write it — "a read-only filesystem on a write path", which is
/// exactly what `exit.rs` reserves PERMISSION_DENIED for. It used to exit 1,
/// indistinguishable from a genuine I/O failure (R-034). The caller's wording
/// is preserved so each verb keeps its own phrasing.
pub fn write_open_error(context: &str, e: crate::fs::filesystem::FilesystemError) -> anyhow::Error {
    let msg = format!("{context}: {e}");
    match e {
        crate::fs::filesystem::FilesystemError::Unsupported(_) => {
            crate::cli::exit::permission_denied(msg)
        }
        _ => anyhow!(msg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn part(index: usize, type_name: &str, apm_type: Option<&str>, size: u64) -> PartitionInfo {
        PartitionInfo {
            index,
            type_name: type_name.to_string(),
            partition_type_byte: 0,
            start_lba: 0,
            start_byte: None,
            size_bytes: size,
            bootable: false,
            is_logical: false,
            is_extended_container: false,
            partition_type_string: apm_type.map(|s| s.to_string()),
            hfs_block_size: None,
            rdb_part_block: None,
            drv_name: None,
        }
    }

    /// The ambiguity error has to name the number that actually selects the
    /// partition. `@N` resolves as `partitions[N - 1]`, so the listing must be
    /// positional - `PartitionInfo::index` is 0- or 1-based depending on the
    /// table type, and printing it would hand the user a number that picks a
    /// different volume. The `index` values here disagree with position on
    /// purpose.
    #[test]
    fn ambiguity_error_numbers_partitions_by_selector_position() {
        let parts = vec![
            part(
                70,
                "Apple_HFS (untitled)",
                Some("Apple_HFS"),
                256 * 1024 * 1024,
            ),
            part(
                80,
                "Apple_HFS (Untitled)",
                Some("Apple_HFS"),
                85 * 1024 * 1024 * 1024,
            ),
            part(
                90,
                "Apple_UNIX_SVR2 (untitled)",
                Some("Apple_UNIX_SVR2"),
                80 * 1024 * 1024 * 1024,
            ),
        ];
        let err = pick_default_partition(&parts).unwrap_err().to_string();

        for (pos, needle) in [
            (1, "Apple_HFS (untitled)"),
            (3, "Apple_UNIX_SVR2 (untitled)"),
        ] {
            let line = err
                .lines()
                .find(|l| l.contains(needle))
                .unwrap_or_else(|| panic!("no line for {needle} in:\n{err}"));
            assert_eq!(
                line.split_whitespace().next(),
                Some(pos.to_string().as_str()),
                "line should lead with selector position {pos}: {line}"
            );
        }
        assert!(
            !err.contains("70"),
            "must not print PartitionInfo::index:\n{err}"
        );
        // Size, so two same-typed volumes can be told apart - and not the MBR
        // type byte, which is `(0)` for every row on APM.
        assert!(
            err.contains(&crate::partition::format_size(80 * 1024 * 1024 * 1024)),
            "sizes should be listed:\n{err}"
        );
        assert!(!err.contains("(0)"), "type byte is noise on APM:\n{err}");
    }

    /// The APM partition map is a wrapper, not a volume: with it filtered out
    /// a single real filesystem must resolve without an ambiguity error.
    #[test]
    fn partition_map_entry_is_not_a_candidate() {
        let parts = vec![
            part(
                1,
                "Apple_partition_map",
                Some("Apple_partition_map"),
                32 * 1024,
            ),
            part(2, "Apple_HFS (Untitled)", Some("Apple_HFS"), 1024 * 1024),
        ];
        let picked = pick_default_partition(&parts).expect("one real filesystem");
        assert_eq!(picked.type_name, "Apple_HFS (Untitled)");
    }
}

#[cfg(test)]
mod source_exists_tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn a_missing_plain_file_is_not_found() {
        let e = require_source_exists(Path::new("definitely-not-here-9e3f.img"))
            .expect_err("a missing image must be refused");
        assert_eq!(crate::cli::exit::code_for(&e), crate::cli::exit::NOT_FOUND);
        assert!(format!("{e:#}").contains("no such file"));
    }

    /// The exemption that matters: backing up a raw disk is the app's job, and
    /// a device node does not `exists()` as a file. Getting this wrong would
    /// refuse every device before it was ever opened.
    #[test]
    fn device_paths_are_exempt() {
        for p in [
            r"\\.\PhysicalDrive0",
            r"\\?\PhysicalDrive1",
            "/dev/sda",
            "/dev/disk3",
        ] {
            assert!(
                require_source_exists(Path::new(p)).is_ok(),
                "{p} must pass the guard and fail (or not) at open time instead"
            );
        }
    }

    #[test]
    fn remote_refs_are_exempt() {
        assert!(require_source_exists(Path::new("rb://host:9000/disk.img")).is_ok());
    }

    #[test]
    fn an_existing_path_passes() {
        // A directory counts: a backup folder is a legitimate source.
        assert!(require_source_exists(Path::new(env!("CARGO_MANIFEST_DIR"))).is_ok());
    }
}
