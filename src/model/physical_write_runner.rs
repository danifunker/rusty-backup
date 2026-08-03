//! Background-thread runner for the Physical Disk Export action.
//!
//! Streams a source image directly to a `/dev/sdX` (or platform equivalent)
//! device handle. Optionally wraps a superfloppy source with a synthetic
//! MBR/GPT partition table on the way through, using
//! [`crate::restore::superfloppy_wrap`].
//!
//! The source is opened through the same peeling primitive as inspect and
//! browse (`source_reader::open_peeled_read_with_entry`), so anything the
//! engine can *read* can be written to a device: DMG, CHD, VHD, 2MG, GHO, IMZ,
//! and gzip- or zip-wrapped images all write their decoded disk bytes rather
//! than the container's. Sizes therefore come from the decoded stream, never
//! from file metadata.
//!
//! A [`WriteExtent`] addresses a partition as an offset into the *parent*
//! device rather than through a slice node (`/dev/disk6s3`, `/dev/sda2`).
//! Windows has no slice nodes, so offset-into-parent is the only shape that
//! works on all three platforms, and it reuses the unmount / claim / elevation
//! prep the whole-disk path already performs.
//!
//! Follows the canonical pattern: leaf I/O (`superfloppy_wrap::wrap_and_write`,
//! direct-copy loop) takes `progress_cb` / `cancel_check` / `log_cb` callbacks;
//! the runner owns `Arc<Mutex<PhysicalWriteStatus>>` and translates callback
//! fires into Status writes. The GUI polls the Status struct each frame.
//!
//! See `docs/progress_pattern.md` for the canonical shape.
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};

use crate::os::{open_target_for_writing, SectorAlignedWriter};
use crate::restore::superfloppy_wrap::{self, WrapParams};

const STREAM_CHUNK: usize = 1024 * 1024;

/// Shared status for a background Physical Disk Export operation.
pub struct PhysicalWriteStatus {
    pub finished: bool,
    pub error: Option<String>,
    pub log_messages: Vec<String>,
    pub current_bytes: u64,
    pub total_bytes: u64,
    pub cancel_requested: bool,
}

/// Source for a physical-disk write. Backup folders go through `restore`.
#[derive(Debug, Clone)]
pub enum PhysicalWriteSource {
    /// Any readable image; container/wrapper layers are peeled on the way out.
    Image(PathBuf),
}

/// The region of the target a write lands in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteExtent {
    /// Byte offset within the target device; 0 for a whole-disk write.
    pub offset: u64,
    /// Bytes available from `offset`; the write refuses a source larger.
    pub capacity: u64,
}

impl WriteExtent {
    /// The whole device, starting at LBA 0.
    pub fn whole_disk(size_bytes: u64) -> Self {
        Self {
            offset: 0,
            capacity: size_bytes,
        }
    }

    /// One partition, from a start LBA and byte length.
    pub fn partition(start_lba: u64, size_bytes: u64) -> Self {
        Self {
            offset: start_lba.saturating_mul(512),
            capacity: size_bytes,
        }
    }

    pub fn is_whole_disk(&self) -> bool {
        self.offset == 0
    }
}

/// Inputs for a Physical Disk Export job.
#[derive(Debug, Clone)]
pub struct PhysicalWriteRequest {
    pub source: PhysicalWriteSource,
    pub target_device_path: PathBuf,
    pub target_size_bytes: u64,
    /// Region to write; [`WriteExtent::whole_disk`] for the whole device.
    pub extent: WriteExtent,
    /// `Some` to synthesize a partition table around the source.
    pub wrap: Option<WrapParams>,
}

fn new_status(total_bytes: u64) -> Arc<Mutex<PhysicalWriteStatus>> {
    Arc::new(Mutex::new(PhysicalWriteStatus {
        finished: false,
        error: None,
        log_messages: Vec::new(),
        current_bytes: 0,
        total_bytes,
        cancel_requested: false,
    }))
}

/// Spawn a Physical Disk Export. Returns the Status handle for GUI polling.
pub fn start_physical_write(req: PhysicalWriteRequest) -> Arc<Mutex<PhysicalWriteStatus>> {
    let total = match &req.wrap {
        Some(p) => p.partition_size_bytes,
        None => source_size_for(&req.source).unwrap_or(0),
    };
    let status = new_status(total);
    let status_thread = Arc::clone(&status);

    std::thread::spawn(move || {
        let _wake = crate::os::wakelock::acquire("Rusty Backup: physical disk export");
        let result = run_worker(&req, Arc::clone(&status_thread));
        if let Ok(mut s) = status_thread.lock() {
            s.finished = true;
            if let Err(e) = result {
                s.error = Some(format!("{e:#}"));
            }
        }
    });

    status
}

/// Decoded length, for seeding the progress total before the worker starts.
fn source_size_for(source: &PhysicalWriteSource) -> Option<u64> {
    match source {
        PhysicalWriteSource::Image(p) => open_source_image(p).ok().map(|(_, len)| len),
    }
}

/// Worker function. Public for unit testing — call this inline with a
/// throwaway Status handle to skip the thread spawn.
pub fn run_worker(
    req: &PhysicalWriteRequest,
    status: Arc<Mutex<PhysicalWriteStatus>>,
) -> Result<()> {
    let status_progress = Arc::clone(&status);
    let status_cancel = Arc::clone(&status);
    let status_log = Arc::clone(&status);

    let mut progress_cb = move |bytes: u64| {
        if let Ok(mut s) = status_progress.lock() {
            s.current_bytes = bytes;
        }
    };
    let cancel_check = move || {
        status_cancel
            .lock()
            .map(|s| s.cancel_requested)
            .unwrap_or(false)
    };
    let mut log_cb = move |msg: &str| {
        if let Ok(mut s) = status_log.lock() {
            s.log_messages.push(msg.to_string());
        }
    };

    // A partition write must leave the rest of the target intact.
    let open_target = if req.extent.is_whole_disk() {
        open_target_for_writing
    } else {
        crate::os::open_target_preserving
    };
    let handle = open_target(&req.target_device_path).with_context(|| {
        format!(
            "opening target device {} for writing",
            req.target_device_path.display()
        )
    })?;
    // `handle` may carry platform-specific lock/claim fields that must remain
    // alive until the write finishes; bind it so it drops at end of scope.
    let mut target = SectorAlignedWriter::new(handle.file);

    match &req.source {
        PhysicalWriteSource::Image(path) => {
            let (mut src, source_size) = open_source_image(path)?;
            log_cb(&format!(
                "Source: {} ({} bytes decoded)",
                path.display(),
                source_size,
            ));

            if source_size > req.extent.capacity {
                anyhow::bail!(
                    "source is {} but the target region holds only {} -- \
                     pick a larger target",
                    crate::partition::format_size(source_size),
                    crate::partition::format_size(req.extent.capacity),
                );
            }

            if let Some(params) = &req.wrap {
                if !req.extent.is_whole_disk() {
                    anyhow::bail!(
                        "wrapping a source in a partition table only makes sense \
                         for a whole-disk write, not into a single partition",
                    );
                }
                superfloppy_wrap::wrap_and_write(
                    &mut src,
                    source_size,
                    &mut target,
                    params,
                    &mut progress_cb,
                    &cancel_check,
                    &mut log_cb,
                )?;
            } else {
                direct_copy(
                    &mut src,
                    &mut target,
                    source_size,
                    req.extent.offset,
                    &mut progress_cb,
                    &cancel_check,
                    &mut log_cb,
                )?;
            }
        }
    }

    target.flush().context("flushing target after write")?;
    log_cb("Physical disk export complete");
    Ok(())
}

/// Decoded reader + decoded length; a container's file size is not its disk size.
fn open_source_image(path: &std::path::Path) -> Result<(Box<dyn crate::rbformats::ReadSeek>, u64)> {
    let mut src = crate::model::source_reader::open_peeled_read_with_entry(path, None, None)
        .with_context(|| format!("opening source {}", path.display()))?;
    let size = src
        .seek(SeekFrom::End(0))
        .with_context(|| format!("measuring {}", path.display()))?;
    src.seek(SeekFrom::Start(0))
        .with_context(|| format!("rewinding {}", path.display()))?;
    if size == 0 {
        anyhow::bail!(
            "{} decoded to zero bytes; it may be an unsupported or truncated image",
            path.display(),
        );
    }
    Ok((src, size))
}

fn direct_copy<R, W>(
    source: &mut R,
    target: &mut W,
    source_size: u64,
    target_offset: u64,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
    log_cb: &mut impl FnMut(&str),
) -> Result<()>
where
    R: Read,
    W: Write + Seek,
{
    target
        .seek(SeekFrom::Start(target_offset))
        .with_context(|| format!("seek target to byte {target_offset}"))?;
    log_cb(&format!(
        "Direct write: streaming {} bytes to target at offset {}",
        source_size, target_offset,
    ));
    let mut buf = vec![0u8; STREAM_CHUNK];
    let mut written: u64 = 0;
    while written < source_size {
        if cancel_check() {
            anyhow::bail!("physical write cancelled");
        }
        let want = ((source_size - written).min(STREAM_CHUNK as u64)) as usize;
        let mut filled = 0;
        while filled < want {
            let n = source
                .read(&mut buf[filled..want])
                .context("reading source")?;
            if n == 0 {
                anyhow::bail!(
                    "source ended early: expected {} bytes, got {}",
                    source_size,
                    written + filled as u64
                );
            }
            filled += n;
        }
        target.write_all(&buf[..filled]).context("writing target")?;
        written += filled as u64;
        progress_cb(written);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::restore::superfloppy_wrap::{WrapAlignment, WrapParams, WrapTable};
    use std::io::Write;
    use tempfile::NamedTempFile;

    const ONE_MIB: u64 = 1024 * 1024;

    fn ntfs_vbr_sector() -> [u8; 512] {
        let mut s = [0u8; 512];
        s[0] = 0xEB;
        s[1] = 0x52;
        s[2] = 0x90;
        s[3..11].copy_from_slice(b"NTFS    ");
        s[11..13].copy_from_slice(&512u16.to_le_bytes());
        s[510] = 0x55;
        s[511] = 0xAA;
        s
    }

    fn write_source(size: u64) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        let mut buf = vec![0u8; size as usize];
        buf[..512].copy_from_slice(&ntfs_vbr_sector());
        f.write_all(&buf).unwrap();
        f.flush().unwrap();
        f
    }

    /// Gzip-wrap a source so the file on disk is nothing like the disk image.
    fn write_gzipped_source(size: u64) -> NamedTempFile {
        let mut raw = vec![0u8; size as usize];
        raw[..512].copy_from_slice(&ntfs_vbr_sector());
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(&raw).unwrap();
        let gz = enc.finish().unwrap();

        // `.img.gz` is what the peeling layer keys off for a gzip-wrapped image.
        let f = tempfile::Builder::new()
            .suffix(".img.gz")
            .tempfile()
            .unwrap();
        std::fs::write(f.path(), &gz).unwrap();
        f
    }

    #[test]
    fn writes_the_decoded_image_not_the_container_bytes() {
        // The whole point of peeling: a compressed source must land as the disk
        // image it decodes to, never as the container's literal bytes.
        let src = write_gzipped_source(ONE_MIB);
        let compressed_len = std::fs::metadata(src.path()).unwrap().len();
        assert!(compressed_len < ONE_MIB, "source should be compressed");

        let target_file = NamedTempFile::new().unwrap();
        let target_path = target_file.path().to_path_buf();
        drop(target_file);

        let req = PhysicalWriteRequest {
            source: PhysicalWriteSource::Image(src.path().to_path_buf()),
            target_device_path: target_path.clone(),
            target_size_bytes: 8 * ONE_MIB,
            extent: WriteExtent::whole_disk(8 * ONE_MIB),
            wrap: None,
        };
        run_worker(&req, new_status(0)).expect("worker");

        let bytes = std::fs::read(&target_path).unwrap();
        assert_eq!(
            bytes.len() as u64,
            ONE_MIB,
            "decoded length, not file length"
        );
        assert_eq!(&bytes[3..11], b"NTFS    ");
    }

    #[test]
    fn writes_into_a_partition_at_its_offset_leaving_the_rest_alone() {
        let src = write_source(ONE_MIB);
        let target_file = NamedTempFile::new().unwrap();
        let target_path = target_file.path().to_path_buf();
        // Pre-fill so we can prove the region before the offset is untouched.
        std::fs::write(&target_path, vec![0xAAu8; (8 * ONE_MIB) as usize]).unwrap();

        let start_lba = 2048;
        let req = PhysicalWriteRequest {
            source: PhysicalWriteSource::Image(src.path().to_path_buf()),
            target_device_path: target_path.clone(),
            target_size_bytes: 8 * ONE_MIB,
            extent: WriteExtent::partition(start_lba, 4 * ONE_MIB),
            wrap: None,
        };
        run_worker(&req, new_status(0)).expect("worker");

        let bytes = std::fs::read(&target_path).unwrap();
        let off = (start_lba * 512) as usize;
        assert_eq!(
            &bytes[off + 3..off + 11],
            b"NTFS    ",
            "image at the offset"
        );
        assert!(
            bytes[..off].iter().all(|b| *b == 0xAA),
            "bytes before the partition must be untouched",
        );
    }

    #[test]
    fn refuses_a_source_larger_than_the_target_region() {
        let src = write_source(4 * ONE_MIB);
        let target_file = NamedTempFile::new().unwrap();
        let target_path = target_file.path().to_path_buf();
        std::fs::write(&target_path, vec![0u8; (64 * ONE_MIB) as usize]).unwrap();

        let req = PhysicalWriteRequest {
            source: PhysicalWriteSource::Image(src.path().to_path_buf()),
            target_device_path: target_path,
            target_size_bytes: 64 * ONE_MIB,
            // Plenty of disk, but the chosen partition is far too small.
            extent: WriteExtent::partition(2048, ONE_MIB),
            wrap: None,
        };
        let err = run_worker(&req, new_status(0)).expect_err("must refuse");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("target region holds only"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn refuses_to_wrap_a_partition_target() {
        let src = write_source(ONE_MIB);
        let target_file = NamedTempFile::new().unwrap();
        let target_path = target_file.path().to_path_buf();
        std::fs::write(&target_path, vec![0u8; (8 * ONE_MIB) as usize]).unwrap();

        let req = PhysicalWriteRequest {
            source: PhysicalWriteSource::Image(src.path().to_path_buf()),
            target_device_path: target_path,
            target_size_bytes: 8 * ONE_MIB,
            extent: WriteExtent::partition(2048, 4 * ONE_MIB),
            wrap: Some(WrapParams {
                table: WrapTable::Mbr {
                    type_byte: 0x07,
                    bootable: false,
                    alignment: WrapAlignment::Modern1MB,
                },
                partition_size_bytes: ONE_MIB,
                target_size_bytes: 8 * ONE_MIB,
                source_fs_hint: "NTFS".into(),
            }),
        };
        let err = run_worker(&req, new_status(0)).expect_err("must refuse");
        assert!(format!("{err:#}").contains("whole-disk write"));
    }

    #[test]
    fn extent_helpers_convert_lba_and_flag_whole_disk() {
        assert!(WriteExtent::whole_disk(100).is_whole_disk());
        let p = WriteExtent::partition(2048, 4096);
        assert_eq!(p.offset, 2048 * 512);
        assert_eq!(p.capacity, 4096);
        assert!(!p.is_whole_disk());
    }

    #[test]
    fn test_run_worker_wrap_to_file_target() {
        // Use a regular file as the "device" target — open_target_for_writing
        // creates/truncates a file path that doesn't start with /dev/ or \\.\.
        let src_file = write_source(4 * ONE_MIB);
        let target_file = NamedTempFile::new().unwrap();
        let target_path = target_file.path().to_path_buf();
        drop(target_file); // we'll recreate via open_target_for_writing

        let params = WrapParams {
            table: WrapTable::Mbr {
                type_byte: 0x07,
                bootable: false,
                alignment: WrapAlignment::Modern1MB,
            },
            partition_size_bytes: 4 * ONE_MIB,
            target_size_bytes: 8 * ONE_MIB,
            source_fs_hint: "NTFS".into(),
        };
        let req = PhysicalWriteRequest {
            source: PhysicalWriteSource::Image(src_file.path().to_path_buf()),
            target_device_path: target_path.clone(),
            target_size_bytes: 8 * ONE_MIB,
            extent: WriteExtent::whole_disk(8 * ONE_MIB),
            wrap: Some(params),
        };
        let status = new_status(0);
        run_worker(&req, Arc::clone(&status)).expect("worker");
        let bytes = std::fs::read(&target_path).unwrap();
        // MBR signature.
        assert_eq!(bytes[510], 0x55);
        assert_eq!(bytes[511], 0xAA);
        // VBR at LBA 2048.
        let off = 2048 * 512;
        assert_eq!(&bytes[off + 3..off + 11], b"NTFS    ");
    }

    #[test]
    #[allow(clippy::identity_op)] // `1 * ONE_MIB` reads as "1 MiB"
    fn test_run_worker_direct_copy() {
        let src_file = write_source(1 * ONE_MIB);
        let target_file = NamedTempFile::new().unwrap();
        let target_path = target_file.path().to_path_buf();
        drop(target_file);

        let req = PhysicalWriteRequest {
            source: PhysicalWriteSource::Image(src_file.path().to_path_buf()),
            target_device_path: target_path.clone(),
            target_size_bytes: 1 * ONE_MIB,
            extent: WriteExtent::whole_disk(1 * ONE_MIB),
            wrap: None,
        };
        let status = new_status(0);
        run_worker(&req, Arc::clone(&status)).expect("worker");
        let bytes = std::fs::read(&target_path).unwrap();
        // First sector matches the NTFS VBR.
        assert_eq!(bytes[0], 0xEB);
        assert_eq!(&bytes[3..11], b"NTFS    ");
    }

    #[test]
    fn test_run_worker_cancel() {
        let src_file = write_source(4 * ONE_MIB);
        let target_file = NamedTempFile::new().unwrap();
        let target_path = target_file.path().to_path_buf();
        drop(target_file);

        let req = PhysicalWriteRequest {
            source: PhysicalWriteSource::Image(src_file.path().to_path_buf()),
            target_device_path: target_path,
            target_size_bytes: 8 * ONE_MIB,
            extent: WriteExtent::whole_disk(8 * ONE_MIB),
            wrap: None,
        };
        let status = new_status(0);
        // Pre-set cancel before starting.
        status.lock().unwrap().cancel_requested = true;
        let result = run_worker(&req, Arc::clone(&status));
        assert!(result.is_err());
    }
}
