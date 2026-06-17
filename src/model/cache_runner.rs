//! Background partclone metadata-scan runner.
//!
//! Browsing a Clonezilla partition needs a [`PartcloneBlockCache`] whose block
//! metadata has been scanned (the partclone bitmap walked + block offsets
//! indexed). That scan is slow, so it runs on a worker thread; this runner owns
//! the spawn and reports progress through a shared [`BlockCacheScan`] status the
//! caller polls each frame. Both the Inspect tab and Commander Mode panes use
//! it — the scan orchestration lives here in the model, not in either view (see
//! CONTRIBUTING, "Presentation layers: one model, many UIs").

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::clonezilla::block_cache::PartcloneBlockCache;
use crate::clonezilla::metadata_scan;
use crate::model::status::BlockCacheScan;

/// Spawn a background scan that fills `cache`'s block metadata and persists it to
/// `cache_path` (so the next open loads instantly). Returns a [`BlockCacheScan`]
/// status the caller polls each frame; when `finished` is set with no `error`,
/// the shared `cache` is ready to back a `BrowseSession` (build one with
/// [`session_for_partclone_cache`]).
///
/// [`session_for_partclone_cache`]: crate::model::commander_source::session_for_partclone_cache
pub fn spawn_partclone_scan(
    cache: Arc<Mutex<PartcloneBlockCache>>,
    partition_index: usize,
    partition_type: u8,
    cache_path: PathBuf,
) -> Arc<Mutex<BlockCacheScan>> {
    let status = Arc::new(Mutex::new(BlockCacheScan {
        finished: false,
        error: None,
        partition_index,
        partition_type,
        cache: Arc::clone(&cache),
    }));
    let status_thread = Arc::clone(&status);
    thread::spawn(move || {
        let _wake = crate::os::wakelock::acquire("Rusty Backup: Clonezilla metadata scan");
        let result = metadata_scan::scan_metadata(&cache, partition_type, Some(&cache_path));
        if let Ok(mut s) = status_thread.lock() {
            s.finished = true;
            if let Err(e) = result {
                s.error = Some(format!("{e:#}"));
            }
        }
    });
    status
}
