//! Browsing a partition: the session the browse view opens, the zstd seekable cache, partclone scans.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use super::{BrowseEdit, BrowseOpen, HostRequest, InspectController};
use crate::model::browse_session::BrowseSession;
use crate::model::commander_source::{self, BackupEdit, ClonezillaOpen, PartcloneLookup};
use crate::model::job::{Job, JobCtx, Progress, Snapshot, Status, Unit};
use dsk_clonezilla::block_cache::PartcloneBlockCache;

type Cache = Arc<Mutex<PartcloneBlockCache>>;

#[derive(Default)]
pub(super) struct BrowseState {
    /// Clonezilla caches scanned this session, reused by a later open.
    caches: commander_source::PartcloneCacheStore,
    /// A partclone scan: (partition index, type byte, cache) and the job.
    scan: Option<(usize, u8, Cache, Job<()>)>,
    /// Seekable caches built this session, per backup partition.
    seekable: HashMap<usize, PathBuf>,
    /// A seekable-cache build: (partition index, cache path) and the job.
    build: Option<(usize, PathBuf, Job<()>)>,
}

impl BrowseState {
    pub fn reset(&mut self) {
        if let Some((_, _, _, job)) = &self.scan {
            job.cancel();
        }
        if let Some((_, _, job)) = &self.build {
            job.cancel();
        }
        *self = BrowseState::default();
    }
}

fn finished(job: &Job<()>) -> Option<Result<(), String>> {
    match job.snapshot().status {
        Status::Running => None,
        Status::Done => Some(Ok(())),
        Status::Failed(e) => Some(Err(e)),
        Status::Cancelled => Some(Err("cancelled".to_string())),
    }
}

impl InspectController {
    /// The partclone scan and seekable-cache build, for their progress bars.
    pub fn browse_jobs(&self) -> Vec<Snapshot> {
        let scan = self.browse.scan.as_ref().map(|s| s.3.snapshot());
        let build = self.browse.build.as_ref().map(|b| b.2.snapshot());
        [scan, build].into_iter().flatten().collect()
    }

    fn open_browse(
        &mut self,
        session: BrowseSession,
        edit: BrowseEdit,
        label_prefix: Option<String>,
    ) {
        let backup_folder = self.found.chd_backup.as_ref().map(|(f, _)| f.clone());
        self.requests
            .push(HostRequest::OpenBrowse(Box::new(BrowseOpen {
                session,
                edit,
                label_prefix,
                backup_folder,
            })));
    }

    pub(super) fn browse_partition(&mut self, index: usize) {
        let Some(part) = self.found.partitions.iter().find(|p| p.index == index) else {
            return;
        };
        let offset = part.byte_offset();
        let ptype = Self::effective_type(part);
        let type_string = part.partition_type_string.clone();
        // The Amiga drive name ("DH0") tells several RDB partitions apart in the header.
        let prefix = part.drv_name.clone().filter(|d| !d.is_empty());

        #[cfg(feature = "remote")]
        if let Some(conn) = self.remote() {
            self.info(format!(
                "Browsing remote partition {index} at offset {offset}"
            ));
            let session = BrowseSession {
                partition_offset: offset,
                partition_type: ptype,
                partition_type_string: type_string,
                remote: Some((Arc::clone(&conn.0), conn.1.clone())),
                ..Default::default()
            };
            // A remote image edits over a read-write block reader, like a local one.
            self.open_browse(session, BrowseEdit::InPlace, prefix);
            return;
        }

        if let Some(path) = self.disk_path() {
            self.info(format!(
                "Browsing partition {} from {} at offset {}",
                index,
                path.display(),
                offset,
            ));
            // macOS reuses the inspect's elevated descriptor so a raw device never re-prompts.
            let preopen =
                if cfg!(target_os = "macos") && path.to_string_lossy().starts_with("/dev/") {
                    self.found.device_file.clone()
                } else {
                    None
                };
            let session = BrowseSession {
                source_path: Some(path),
                partition_offset: offset,
                partition_type: ptype,
                partition_type_string: type_string,
                preopen_file: preopen,
                ..Default::default()
            };
            self.open_browse(session, BrowseEdit::InPlace, prefix);
            return;
        }

        if self.found.clonezilla.is_some() {
            self.browse_clonezilla(index, ptype);
            return;
        }

        let (Some(folder), Some(meta)) = (
            self.backup_folder().map(|f| f.to_path_buf()),
            self.found.backup.clone(),
        ) else {
            self.error(format!(
                "partition-{index}: no source available for browsing"
            ));
            return;
        };
        let edit = match commander_source::backup_edit_for(folder.as_path(), &meta, index) {
            Ok(BackupEdit::InPlace) => BrowseEdit::InPlace,
            Ok(BackupEdit::Archive(plan)) => {
                BrowseEdit::Archive(crate::model::archive_edit::ArchiveEditContext {
                    archive_path: plan.archive_path,
                    compression_type: plan.compression_type,
                    original_size: plan.original_size,
                    compacted: plan.compacted,
                    metadata_path: plan.metadata_path,
                    partition_index: plan.partition_index,
                    checksum_type: plan.checksum_type,
                })
            }
            Ok(BackupEdit::ReadOnly) => BrowseEdit::ReadOnly,
            Err(e) => {
                log::warn!("no edit context for partition {index}: {e:#}");
                BrowseEdit::ReadOnly
            }
        };

        // zstd opens streaming at once and is upgraded when a seekable cache is built.
        if meta.compression_type == "zstd" {
            let data = match commander_source::single_data_file(folder.as_path(), &meta, index) {
                Ok(p) => p,
                Err(e) => {
                    self.error(format!("partition-{index}: {e:#}"));
                    return;
                }
            };
            self.browse_zstd(index, ptype, type_string, data, folder, edit);
            return;
        }

        match commander_source::session_for_backup_partition(folder.as_path(), &meta, index) {
            Ok(session) => {
                self.info(format!(
                    "Browsing partition {index} from {} backup",
                    meta.compression_type
                ));
                self.open_browse(session, edit, None);
            }
            // These only fail on a real error (a missing or split data file).
            Err(e) => match meta.compression_type.as_str() {
                "none" | "chd" | "chd-dvd" | "woz" => {
                    self.error(format!("partition-{index}: {e:#}"))
                }
                other => self.warn(format!(
                    "partition-{index}: browsing {other} compressed backups is not yet supported"
                )),
            },
        }
    }

    fn browse_clonezilla(&mut self, index: usize, ptype: u8) {
        let (Some(cz), Some(folder)) = (
            self.found.clonezilla.as_ref(),
            self.backup_folder().map(|f| f.to_path_buf()),
        ) else {
            return;
        };
        let Some(part) = cz.partitions.iter().find(|p| p.index == index).cloned() else {
            self.error(format!("partition-{index}: not found in Clonezilla image"));
            return;
        };
        if part.partclone_files.is_empty() {
            self.error(format!("partition-{index}: no partclone data files"));
            return;
        }
        let open = ClonezillaOpen {
            partclone_files: part.partclone_files.clone(),
            partition_type: ptype,
            cache_path: folder.join(format!("_{}.metadata.cache", part.device_name)),
        };
        match self.browse.caches.resolve(index, &open) {
            PartcloneLookup::Ready(cache) => {
                self.info(format!("Browsing partition {index} from cached metadata"));
                self.open_browse(
                    BrowseSession::partclone(cache, ptype),
                    BrowseEdit::ReadOnly,
                    None,
                );
            }
            PartcloneLookup::NeedsScan { cache, cache_path } => {
                if self.browse.scan.is_some() {
                    self.warn("A metadata scan is already in progress. Please wait.");
                    return;
                }
                self.info(format!(
                    "Scanning metadata for partition {} ({})...",
                    index,
                    disk_core::display::format_size(part.size_bytes()),
                ));
                let scan_cache = Arc::clone(&cache);
                let label = format!("Scanning partition {index} metadata");
                let job = self.spawn(label.as_str(), move |job| {
                    scan_in_job(scan_cache, index, ptype, cache_path, job)
                });
                self.browse.scan = Some((index, ptype, cache, job));
            }
        }
    }

    fn browse_zstd(
        &mut self,
        index: usize,
        ptype: u8,
        type_string: Option<String>,
        data: PathBuf,
        folder: PathBuf,
        edit: BrowseEdit,
    ) {
        if let Some(cache) = self.browse.seekable.get(&index).cloned() {
            if cache.exists() {
                self.info(format!(
                    "Browsing partition {index} from cached seekable file"
                ));
                let session = BrowseSession {
                    source_path: Some(cache),
                    partition_type: ptype,
                    partition_type_string: type_string,
                    ..Default::default()
                };
                self.open_browse(session, edit, None);
                return;
            }
            // Deleted since: build it again.
            self.browse.seekable.remove(&index);
        }
        if self.browse.build.is_some() {
            self.warn("A seekable cache is already being created. Please wait.");
            return;
        }
        self.info(format!(
            "Opening partition {index} via streaming reader (seekable cache building in background)...",
        ));
        match BrowseSession::streaming(data.as_path(), ptype, type_string) {
            Ok(session) => self.open_browse(session, edit, None),
            Err(e) => {
                self.requests.push(HostRequest::CloseBrowse);
                self.error(e);
            }
        }
        let cache_path = folder.join(format!("_partition-{index}.seekable.zst"));
        self.info(format!(
            "Creating seekable cache for partition {index} in the background...",
        ));
        let target = cache_path.clone();
        let label = format!("Building seekable zstd index for partition {index}");
        let job = self.spawn(label.as_str(), move |job| {
            job.set_can_cancel(false);
            let _wake = crate::os::wakelock::acquire("Rusty Backup: build zstd seekable cache");
            crate::model::cache_runner::build_seekable_zstd(data.as_path(), target.as_path(), job)
        });
        self.browse.build = Some((index, cache_path, job));
    }

    pub(super) fn poll_browse(&mut self) {
        if let Some(done) = self.browse.scan.as_ref().and_then(|s| finished(&s.3)) {
            let Some((index, ptype, cache, _)) = self.browse.scan.take() else {
                return;
            };
            match done {
                Ok(()) => {
                    self.info(format!("Metadata scan complete for partition {index}."));
                    self.browse.caches.insert(index, Arc::clone(&cache));
                    self.open_browse(
                        BrowseSession::partclone(cache, ptype),
                        BrowseEdit::ReadOnly,
                        None,
                    );
                }
                Err(e) => self.error(format!("Metadata scan failed for partition {index}: {e}")),
            }
        }
        if let Some(done) = self.browse.build.as_ref().and_then(|b| finished(&b.2)) {
            let Some((index, path, _)) = self.browse.build.take() else {
                return;
            };
            match done {
                Ok(()) => {
                    self.info("Seekable cache ready - browser upgraded to full seek support.");
                    self.browse.seekable.insert(index, path.clone());
                    self.requests.push(HostRequest::UpgradeSeekable(path));
                }
                Err(e) => self.error(format!(
                    "Failed to create seekable cache for partition {index}: {e}"
                )),
            }
        }
    }
}

/// Scan a partclone partition's block metadata, reporting blocks scanned.
fn scan_in_job(
    cache: Cache,
    index: usize,
    ptype: u8,
    cache_path: PathBuf,
    job: &JobCtx,
) -> anyhow::Result<()> {
    job.set_can_cancel(false);
    let status = crate::model::cache_runner::spawn_partclone_scan(cache, index, ptype, cache_path);
    let mut err = None;
    crate::model::worker::wait_spawned(job, &status, |s, ctx| {
        if let Ok(c) = s.cache.lock() {
            if c.total_used_blocks > 0 {
                ctx.set_progress(Progress::Determinate {
                    done: c.scanned_used_blocks,
                    total: c.total_used_blocks,
                    unit: Unit::Items,
                });
            }
        }
        if s.finished {
            err = s.error.take();
        }
        s.finished
    });
    match err {
        Some(e) => Err(anyhow::anyhow!(e)),
        None => Ok(()),
    }
}
