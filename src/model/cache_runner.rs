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

use crate::model::status::BlockCacheScan;
use dsk_clonezilla::block_cache::PartcloneBlockCache;
use dsk_clonezilla::metadata_scan;

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
    crate::model::worker::spawn_guarded(
        Arc::clone(&status),
        "Clonezilla metadata scan",
        move || {
            let _wake = crate::os::wakelock::acquire("Rusty Backup: Clonezilla metadata scan");
            let result = metadata_scan::scan_metadata(&cache, partition_type, Some(&cache_path));
            if let Ok(mut s) = status_thread.lock() {
                s.finished = true;
                if let Err(e) = result {
                    s.error = Some(format!("{e:#}"));
                }
            }
            Ok(())
        },
    );
    status
}

/// Run [`spawn_partclone_scan`] to the end inside a job; `Err` carries the scan's failure.
pub fn scan_in_job(
    ctx: &crate::model::job::JobCtx,
    cache: Arc<Mutex<PartcloneBlockCache>>,
    partition_index: usize,
    partition_type: u8,
    cache_path: PathBuf,
) -> anyhow::Result<()> {
    let status = spawn_partclone_scan(cache, partition_index, partition_type, cache_path);
    loop {
        {
            let mut g = crate::model::worker::lock_status(&status);
            if g.finished {
                return match g.error.take() {
                    Some(e) => Err(anyhow::anyhow!("Clonezilla metadata scan failed: {e}")),
                    None => Ok(()),
                };
            }
        }
        ctx.check_cancel()?;
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// Re-encode a native zstd partition image as a seekable zstd cache at `cache_path`.
/// Progress counts compressed input bytes, so it stays within the file's size.
#[cfg(feature = "seekable-zstd")]
pub fn build_seekable_zstd(
    data_path: &std::path::Path,
    cache_path: &std::path::Path,
    job: &crate::model::job::JobCtx,
) -> anyhow::Result<()> {
    use anyhow::Context;
    use std::io::{BufReader, BufWriter, Read};

    struct Counting<R> {
        inner: R,
        count: u64,
    }
    impl<R: Read> Read for Counting<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = self.inner.read(buf)?;
            self.count += n as u64;
            Ok(n)
        }
    }

    let input = std::fs::File::open(data_path)
        .with_context(|| format!("failed to open {}", data_path.display()))?;
    let total = input.metadata().map(|m| m.len()).unwrap_or(0);
    let counting = Counting {
        inner: BufReader::new(input),
        count: 0,
    };
    let mut decoder = zstd::Decoder::new(counting).context("failed to create zstd decoder")?;
    let output = std::fs::File::create(cache_path)
        .with_context(|| format!("failed to create cache file: {}", cache_path.display()))?;
    let opts = zeekstd::EncodeOptions::new()
        .frame_size_policy(zeekstd::FrameSizePolicy::Uncompressed(2 * 1024 * 1024));
    let mut encoder = opts
        .into_encoder(BufWriter::new(output))
        .map_err(|e| anyhow::anyhow!("failed to create zeekstd encoder: {e}"))?;
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        job.check_cancel()?;
        let n = decoder.read(&mut buf)?;
        if n == 0 {
            break;
        }
        encoder
            .compress(&buf[..n])
            .map_err(|e| anyhow::anyhow!("zeekstd compress error: {e}"))?;
        job.set_bytes(decoder.get_ref().get_ref().count, total);
    }
    encoder
        .finish()
        .map_err(|e| anyhow::anyhow!("zeekstd finish error: {e}"))?;
    Ok(())
}

#[cfg(not(feature = "seekable-zstd"))]
pub fn build_seekable_zstd(
    _: &std::path::Path,
    _: &std::path::Path,
    _: &crate::model::job::JobCtx,
) -> anyhow::Result<()> {
    anyhow::bail!("the seekable zstd cache is not built into this binary")
}
