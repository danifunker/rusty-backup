//! Background-thread runner for "build a disk": write a fresh partition table
//! onto a target, then pour a source image into each partition that has one.
//!
//! One job, not N, because the table and its partitions must land on the same
//! opened handle — reopening a raw device per partition costs an unmount /
//! claim / elevation round trip each time, and on macOS a second `authopen`
//! prompt.
//!
//! Layout maths and the table writers live in [`crate::partition::provision`];
//! the per-partition streaming reuses
//! [`crate::model::physical_write_runner::write_image_into`], so containers
//! (DMG, CHD, VHD, gzip, zip, ...) are peeled here exactly as they are for a
//! plain image write.
//!
//! Follows the canonical progress pattern: leaf I/O takes `progress_cb` /
//! `cancel_check` / `log_cb`; the runner owns the `Arc<Mutex<..>>` Status the
//! GUI polls. See `docs/progress_pattern.md`.

use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};

use crate::model::physical_write_runner::{self, PhysicalWriteStatus, WriteExtent};
use crate::os::{open_target_preserving, SectorAlignedWriter};
use crate::partition::provision::{self, Geometry, Placed};
use crate::partition::type_catalog::TableKind;

/// Inputs for a build-a-disk job.
#[derive(Debug, Clone)]
pub struct ProvisionRequest {
    /// Device node or image-file path to build on.
    pub target_path: PathBuf,
    /// Size of the disk the table describes. For an image-file target the file
    /// is created at exactly this length.
    pub target_size_bytes: u64,
    pub kind: TableKind,
    /// Only consulted for SGI, whose partitions are cylinder-aligned.
    pub geometry: Geometry,
    /// Partitions in disk order, already placed by [`provision::place`].
    pub partitions: Vec<Placed>,
    /// Source image per entry of `partitions`; `None` leaves it empty.
    pub sources: Vec<Option<PathBuf>>,
}

impl ProvisionRequest {
    /// Sum of the decoded sizes of every assigned source, for the progress
    /// total. Unreadable sources contribute nothing and fail later, in the
    /// worker, where the error can be reported properly.
    pub fn total_source_bytes(&self) -> u64 {
        self.sources
            .iter()
            .flatten()
            .filter_map(|p| physical_write_runner::decoded_source_size(p).ok())
            .sum()
    }
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

/// Spawn a build-a-disk job. Returns the Status handle for GUI polling.
pub fn start_provision(req: ProvisionRequest) -> Arc<Mutex<PhysicalWriteStatus>> {
    let status = new_status(req.total_source_bytes());
    let status_thread = Arc::clone(&status);

    std::thread::spawn(move || {
        let _wake = crate::os::wakelock::acquire("Rusty Backup: build disk");
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

/// Worker function. Public for unit testing — call this inline with a
/// throwaway Status handle to skip the thread spawn.
pub fn run_worker(req: &ProvisionRequest, status: Arc<Mutex<PhysicalWriteStatus>>) -> Result<()> {
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

    if req.partitions.is_empty() {
        anyhow::bail!("no partitions defined");
    }
    if req.sources.len() != req.partitions.len() {
        anyhow::bail!(
            "internal error: {} partitions but {} source slots",
            req.partitions.len(),
            req.sources.len(),
        );
    }
    if let Some(p) = req
        .partitions
        .iter()
        .find(|p| p.end_byte() > req.target_size_bytes)
    {
        anyhow::bail!(
            "partition at LBA {} runs past the end of the {} target",
            p.start_lba,
            crate::partition::format_size(req.target_size_bytes),
        );
    }

    // A regular-file target has to exist at full length before the table is
    // written, or GPT's backup header lands past EOF in a sparse hole the
    // reparse can't find.
    if !crate::os::is_device_path(&req.target_path) {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&req.target_path)
            .with_context(|| format!("creating {}", req.target_path.display()))?;
        file.set_len(req.target_size_bytes).with_context(|| {
            format!(
                "sizing {} to {} bytes",
                req.target_path.display(),
                req.target_size_bytes,
            )
        })?;
    }

    let handle = open_target_preserving(&req.target_path)
        .with_context(|| format!("opening target {} for writing", req.target_path.display()))?;
    // `handle` may carry platform-specific lock/claim fields that must remain
    // alive until the write finishes; bind it so it drops at end of scope.
    let mut target = SectorAlignedWriter::new(handle.file);

    provision::write_table(
        &mut target,
        req.kind,
        &req.partitions,
        req.target_size_bytes,
        req.geometry,
    )?;
    log_cb(&format!(
        "Wrote a {} table with {} partition(s) to {}",
        req.kind.label(),
        req.partitions.len(),
        req.target_path.display(),
    ));
    for (i, p) in req.partitions.iter().enumerate() {
        log_cb(&provision::describe_placed(req.kind, i, p));
    }

    let mut done: u64 = 0;
    for (i, (placed, source)) in req.partitions.iter().zip(req.sources.iter()).enumerate() {
        let Some(path) = source else { continue };
        if cancel_check() {
            anyhow::bail!("build cancelled");
        }
        log_cb(&format!(
            "Filling partition {} at LBA {} from {}",
            i + 1,
            placed.start_lba,
            path.display(),
        ));
        let extent = WriteExtent::partition(placed.start_lba, placed.size_bytes);
        done += physical_write_runner::write_image_into(
            &mut target,
            path,
            extent,
            done,
            &mut progress_cb,
            &cancel_check,
            &mut log_cb,
        )
        .with_context(|| format!("filling partition {}", i + 1))?;
        progress_cb(done);
    }

    target.flush().context("flushing target after build")?;
    log_cb("Disk build complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::provision::PartSpec;
    use crate::partition::PartitionTable;
    use std::io::{Cursor, Write};

    const MIB: u64 = 1024 * 1024;

    fn fat_ish_source(size: u64, marker: &[u8]) -> tempfile::NamedTempFile {
        let mut f = tempfile::Builder::new().suffix(".img").tempfile().unwrap();
        let mut buf = vec![0u8; size as usize];
        buf[3..3 + marker.len()].copy_from_slice(marker);
        buf[510] = 0x55;
        buf[511] = 0xAA;
        f.write_all(&buf).unwrap();
        f.flush().unwrap();
        f
    }

    fn spec(size: Option<u64>, type_text: &str) -> PartSpec {
        PartSpec {
            size,
            type_text: Some(type_text.to_string()),
            name: None,
        }
    }

    #[test]
    fn builds_a_two_partition_mbr_disk_and_fills_one() {
        let disk = 64 * MIB;
        let specs = vec![spec(Some(8 * MIB), "0C"), spec(None, "83")];
        let placed = provision::place(&specs, TableKind::Mbr, disk, MIB).unwrap();

        let src = fat_ish_source(MIB, b"RBTEST01");
        let target = tempfile::NamedTempFile::new().unwrap();
        let target_path = target.path().to_path_buf();
        drop(target);

        let req = ProvisionRequest {
            target_path: target_path.clone(),
            target_size_bytes: disk,
            kind: TableKind::Mbr,
            geometry: Geometry::default(),
            partitions: placed.clone(),
            sources: vec![Some(src.path().to_path_buf()), None],
        };
        run_worker(&req, new_status(0)).expect("worker");

        let bytes = std::fs::read(&target_path).unwrap();
        assert_eq!(bytes.len() as u64, disk, "target sized to the whole disk");

        let mut reader = Cursor::new(bytes.clone());
        let table = PartitionTable::detect(&mut reader).expect("reparse");
        let parts = table.partitions();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].start_lba, placed[0].start_lba);
        assert_eq!(parts[0].partition_type_byte, 0x0C);

        // The filled partition holds the source at its offset; the unfilled one
        // is untouched.
        let off = placed[0].start_byte() as usize;
        assert_eq!(&bytes[off + 3..off + 11], b"RBTEST01");
        let off2 = placed[1].start_byte() as usize;
        assert!(bytes[off2..off2 + 512].iter().all(|b| *b == 0));
    }

    #[test]
    fn refuses_a_source_too_big_for_its_partition() {
        let disk = 64 * MIB;
        let specs = vec![spec(Some(2 * MIB), "83")];
        let placed = provision::place(&specs, TableKind::Mbr, disk, MIB).unwrap();

        let src = fat_ish_source(8 * MIB, b"TOOBIG12");
        let target = tempfile::NamedTempFile::new().unwrap();
        let target_path = target.path().to_path_buf();
        drop(target);

        let req = ProvisionRequest {
            target_path,
            target_size_bytes: disk,
            kind: TableKind::Mbr,
            geometry: Geometry::default(),
            partitions: placed,
            sources: vec![Some(src.path().to_path_buf())],
        };
        let err = run_worker(&req, new_status(0)).expect_err("must refuse");
        assert!(
            format!("{err:#}").contains("target region holds only"),
            "{err:#}",
        );
    }

    #[test]
    fn refuses_partitions_past_the_end_of_the_target() {
        let placed = vec![Placed {
            start_lba: 2048,
            size_bytes: 900 * MIB,
            type_text: "83".into(),
            name: "P1".into(),
        }];
        let target = tempfile::NamedTempFile::new().unwrap();
        let req = ProvisionRequest {
            target_path: target.path().to_path_buf(),
            target_size_bytes: 64 * MIB,
            kind: TableKind::Mbr,
            geometry: Geometry::default(),
            partitions: placed,
            sources: vec![None],
        };
        let err = run_worker(&req, new_status(0)).expect_err("must refuse");
        assert!(format!("{err:#}").contains("past the end"), "{err:#}");
    }
}
