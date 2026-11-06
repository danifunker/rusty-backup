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

use crate::fs::mac_scsi_bless::{bless_apm_disk, BlessOptions, MacScsiDriver, BUILTIN_DRIVER_NAME};
use crate::model::disk_builder::RowFormat;
use crate::model::format::{self, Window};
use crate::model::physical_write_runner::{self, PhysicalWriteStatus, WriteExtent};
use crate::os::{open_target_preserving, SectorAlignedWriter};
use crate::partition::provision::{self, Geometry, Placed};
use crate::partition::type_catalog::TableKind;
use crate::rbformats::export::ExportFormat;

/// What goes in an APM disk's driver slot; a Mac won't mount a disk that has none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MacDriver {
    /// A donor's own driver if it has one, otherwise the bundled one.
    Bundled,
    Off,
    /// The driver of this Apple-formatted disk.
    FromDisk(PathBuf),
}

impl MacDriver {
    pub fn label(&self) -> String {
        match self {
            MacDriver::Bundled => format!("{} (bundled)", BUILTIN_DRIVER_NAME),
            MacDriver::Off => "None".to_string(),
            MacDriver::FromDisk(p) => format!("From {}", p.display()),
        }
    }
}

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
    /// Filesystem to format per entry of `partitions` (a source wins); empty means none.
    pub formats: Vec<Option<RowFormat>>,
    /// Write every byte of a formatted partition: the target may hold old data (a device).
    pub full_write: bool,
    /// Amiga filesystem handlers to embed in an RDB: `(DosType text, AmigaDOS load file)`.
    pub rdb_handlers: Vec<(String, PathBuf)>,
    /// A label donor's patched head, written in place of a fresh table (after the copies).
    pub head: Option<Vec<u8>>,
    /// The donor disk the copies read from.
    pub donor: Option<PathBuf>,
    /// `(partition, donor start byte, length)`: donor bytes copied to the partition's start.
    pub copies: Vec<(usize, u64, u64)>,
    /// The driver an APM disk gets; other tables ignore it.
    pub mac_driver: MacDriver,
}

/// The driver to install and where it came from, or `None`. An unusable chosen disk is an error.
fn load_mac_driver(req: &ProvisionRequest) -> Result<Option<(MacScsiDriver, String)>> {
    if req.kind != TableKind::Apm {
        return Ok(None);
    }
    let read = |path: &std::path::Path| -> Result<MacScsiDriver> {
        let mut disk = crate::model::source_reader::open_read(path)
            .with_context(|| format!("opening {}", path.display()))?;
        MacScsiDriver::from_donor(&mut disk)
            .map_err(|e| anyhow::anyhow!("reading the driver of {}: {e}", path.display()))
    };
    match &req.mac_driver {
        MacDriver::Off => Ok(None),
        MacDriver::FromDisk(path) => Ok(Some((read(path)?, path.display().to_string()))),
        MacDriver::Bundled => {
            let donor = req.donor.as_ref().and_then(|p| Some((read(p).ok()?, p)));
            Ok(Some(match donor {
                Some((driver, path)) => (driver, format!("the donor {}", path.display())),
                None => (MacScsiDriver::builtin(), BUILTIN_DRIVER_NAME.to_string()),
            }))
        }
    }
}

/// Read and check the handlers for `write_rdb_filesystems`: one per DosType, each an AmigaDOS
/// load file (a strap would hang on anything else rather than fail).
pub fn load_rdb_handlers(
    kind: TableKind,
    specs: &[(String, PathBuf)],
) -> Result<Vec<(u32, Vec<u8>)>> {
    if specs.is_empty() {
        return Ok(Vec::new());
    }
    if kind != TableKind::Rdb {
        anyhow::bail!(
            "filesystem handlers are an Amiga RDB feature; this table is {}",
            kind.label()
        );
    }
    let mut out = Vec::new();
    let mut seen = Vec::new();
    for (type_text, path) in specs {
        let dostype = part_tables::rdb::parse_dos_type(type_text.as_str()).ok_or_else(|| {
            anyhow::anyhow!("bad Amiga DosType '{type_text}' for a filesystem handler")
        })?;
        if seen.contains(&dostype) {
            anyhow::bail!("DosType '{type_text}' has more than one handler");
        }
        let image = std::fs::read(path)
            .with_context(|| format!("reading filesystem handler {}", path.display()))?;
        if image.get(..4) != Some(&[0x00, 0x00, 0x03, 0xF3]) {
            anyhow::bail!(
                "{} is not an AmigaDOS load file (no HUNK_HEADER); a handler is the \
                 binary itself, e.g. L:SmartFilesystem",
                path.display()
            );
        }
        seen.push(dostype);
        out.push((dostype, image));
    }
    Ok(out)
}

/// What a build makes: a partitioned disk, or one volume across the whole target.
#[derive(Debug, Clone)]
pub enum BuildRequest {
    Partitioned(ProvisionRequest),
    Volume {
        target_path: PathBuf,
        size: u64,
        format: RowFormat,
        /// As [`ProvisionRequest::full_write`].
        full_write: bool,
    },
    /// One of the fixed bootable builders, as `rb-cli new hd x68k / sgi-efs` runs it.
    Preset {
        target_path: PathBuf,
        size: u64,
        preset: Preset,
    },
}

/// The fixed-shape bootable disks Build Disk offers as presets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Preset {
    /// Sharp X68000: the IPL stub, the X68000 table and Human68k partitions.
    X68k {
        scsi: bool,
        partitions: usize,
        /// A Human68k system floppy cloned into partition 1.
        system_disk: Option<PathBuf>,
    },
    /// SGI IRIX: a volume header and an EFS root.
    SgiEfs { label: String },
}

impl Preset {
    pub fn name(&self) -> &'static str {
        match self {
            Preset::X68k { .. } => "Sharp X68000 bootable disk (Human68k)",
            Preset::SgiEfs { .. } => "SGI IRIX bootable disk (EFS)",
        }
    }
}

/// Build `preset` into a new file at `out` of `size` bytes.
fn run_preset(
    out: &std::path::Path,
    size: u64,
    preset: &Preset,
    log: &mut dyn FnMut(&str),
) -> Result<()> {
    match preset {
        Preset::X68k {
            scsi,
            partitions,
            system_disk,
        } => {
            use crate::partition::x68k_hdd_builder::{
                build_x68k_hdd, BootSectorSource, HddVariant,
            };
            let variant = if *scsi {
                HddVariant::Scsi
            } else {
                HddVariant::Sasi
            };
            let s = build_x68k_hdd(
                out,
                size / (1024 * 1024),
                variant,
                crate::partition::x68k_ipl::IplStub::Print,
                *partitions,
                system_disk.as_deref(),
                BootSectorSource::None,
            )?;
            log(&format!(
                "Wrote a {} X68000 disk: {} Human68k partition(s) of {}",
                s.variant.name(),
                s.partition_count,
                disk_core::display::format_size(s.partition_bytes),
            ));
        }
        Preset::SgiEfs { label } => {
            use crate::partition::sgi_hdd_builder::{write_sgi_efs_hdd, SgiHddOptions};
            let opts = SgiHddOptions::new(size, label.as_str());
            let mut file = std::fs::File::create(out)
                .with_context(|| format!("creating {}", out.display()))?;
            let layout = write_sgi_efs_hdd(&mut file, &opts)?;
            file.set_len(layout.disk_bytes)?;
            log(&format!(
                "Wrote an SGI volume header and an EFS root ({} cylinders)",
                layout.cylinders
            ));
        }
    }
    Ok(())
}

/// A target that may hold old data (a device) gets every byte of a formatted volume written,
/// so nothing stale shows through the holes a fresh-file format leaves.
fn writes_in_full(target: &std::path::Path, asked: bool) -> bool {
    asked || crate::os::is_device_path(target)
}

impl ProvisionRequest {
    /// Sum of the decoded sizes of every assigned source, for the progress
    /// total. Unreadable sources contribute nothing and fail later, in the
    /// worker, where the error can be reported properly.
    pub fn total_source_bytes(&self) -> u64 {
        let sources: u64 = self
            .sources
            .iter()
            .flatten()
            .filter_map(|p| physical_write_runner::decoded_source_size(p).ok())
            .sum();
        let formats: u64 = if writes_in_full(self.target_path.as_path(), self.full_write) {
            self.formatted().map(|(p, _)| p.size_bytes).sum()
        } else {
            0
        };
        sources + formats + self.copies.iter().map(|c| c.2).sum::<u64>()
    }

    /// The partitions to format, with no source image.
    fn formatted(&self) -> impl Iterator<Item = (&Placed, &RowFormat)> {
        self.partitions
            .iter()
            .zip(self.sources.iter())
            .zip(self.formats.iter())
            .filter_map(|((p, src), f)| match (src, f) {
                (None, Some(f)) => Some((p, f)),
                _ => None,
            })
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

    crate::model::worker::spawn_guarded(Arc::clone(&status), "provision", move || {
        let _wake = crate::os::wakelock::acquire("Rusty Backup: build disk");
        let result = run_worker(&req, Arc::clone(&status_thread));
        if let Ok(mut s) = status_thread.lock() {
            s.finished = true;
            if let Err(e) = result {
                s.error = Some(format!("{e:#}"));
            }
        }
        Ok(())
    });

    status
}

impl BuildRequest {
    pub fn target_path(&self) -> &std::path::Path {
        match self {
            BuildRequest::Partitioned(p) => p.target_path.as_path(),
            BuildRequest::Volume { target_path, .. } | BuildRequest::Preset { target_path, .. } => {
                target_path.as_path()
            }
        }
    }

    fn set_target(&mut self, path: PathBuf) {
        match self {
            BuildRequest::Partitioned(p) => p.target_path = path,
            BuildRequest::Volume { target_path, .. } | BuildRequest::Preset { target_path, .. } => {
                *target_path = path
            }
        }
    }

    fn total_bytes(&self) -> u64 {
        match self {
            BuildRequest::Partitioned(p) => p.total_source_bytes(),
            BuildRequest::Volume {
                target_path,
                size,
                full_write,
                ..
            } => {
                if writes_in_full(target_path.as_path(), *full_write) {
                    *size
                } else {
                    0
                }
            }
            BuildRequest::Preset {
                target_path, size, ..
            } => {
                if crate::os::is_device_path(target_path.as_path()) {
                    *size
                } else {
                    0
                }
            }
        }
    }
}

/// Spawn any build: a partitioned disk or a bare volume, encoded into `container` when given.
pub fn start_build(
    req: BuildRequest,
    container: Option<ExportFormat>,
) -> Arc<Mutex<PhysicalWriteStatus>> {
    let status = new_status(req.total_bytes());
    let status_thread = Arc::clone(&status);
    crate::model::worker::spawn_guarded(Arc::clone(&status), "build disk", move || {
        let _wake = crate::os::wakelock::acquire("Rusty Backup: build disk");
        let result = run_build(req, container, Arc::clone(&status_thread));
        if let Ok(mut s) = status_thread.lock() {
            s.finished = true;
            if let Err(e) = result {
                s.error = Some(format!("{e:#}"));
            }
        }
        Ok(())
    });
    status
}

/// Run a build inline: raw straight to the target, or raw to a temp beside it, then encoded.
pub fn run_build(
    mut req: BuildRequest,
    container: Option<ExportFormat>,
    status: Arc<Mutex<PhysicalWriteStatus>>,
) -> Result<()> {
    let raw = |req: &BuildRequest, status: Arc<Mutex<PhysicalWriteStatus>>| match req {
        BuildRequest::Partitioned(p) => run_worker(p, status),
        BuildRequest::Volume {
            target_path,
            size,
            format,
            full_write,
        } => run_volume(
            target_path.as_path(),
            *size,
            format,
            writes_in_full(target_path.as_path(), *full_write),
            status,
        ),
        BuildRequest::Preset {
            target_path,
            size,
            preset,
        } => run_preset_on(target_path.as_path(), *size, preset, status),
    };
    let Some(fmt) = container.filter(|c| *c != ExportFormat::Raw) else {
        return raw(&req, status);
    };
    let dest = req.target_path().to_path_buf();
    if crate::os::is_device_path(dest.as_path()) {
        anyhow::bail!(
            "a device is written raw; {} is an image format",
            fmt.description()
        );
    }
    let mut tmp = dest.clone().into_os_string();
    tmp.push(".rbtmp.img");
    let tmp = PathBuf::from(tmp);
    req.set_target(tmp.clone());
    let built = raw(&req, Arc::clone(&status));
    let result = built.and_then(|()| encode(fmt, tmp.as_path(), dest.as_path(), &status));
    let _ = std::fs::remove_file(&tmp);
    result
}

/// A preset onto a file directly, or onto a device through a temp image written in full.
fn run_preset_on(
    target: &std::path::Path,
    size: u64,
    preset: &Preset,
    status: Arc<Mutex<PhysicalWriteStatus>>,
) -> Result<()> {
    let push = |m: &str| {
        if let Ok(mut s) = status.lock() {
            s.log_messages.push(m.to_string());
        }
    };
    let mut log = |m: &str| push(m);
    if !crate::os::is_device_path(target) {
        run_preset(target, size, preset, &mut log)?;
        push("Disk build complete");
        return Ok(());
    }
    let dir = tempfile::tempdir().context("creating a temporary folder")?;
    let tmp = dir.path().join("preset.img");
    run_preset(tmp.as_path(), size, preset, &mut log)?;
    let handle = open_target_preserving(target)
        .with_context(|| format!("opening target {} for writing", target.display()))?;
    let mut dev = SectorAlignedWriter::new(handle.file);
    let mut progress = |n: u64| {
        if let Ok(mut s) = status.lock() {
            s.current_bytes = n;
        }
    };
    let cancel = || status.lock().map(|s| s.cancel_requested).unwrap_or(false);
    let mut wlog = |m: &str| push(m);
    physical_write_runner::write_image_into(
        &mut dev,
        tmp.as_path(),
        WriteExtent::whole_disk(size),
        0,
        &mut progress,
        &cancel,
        &mut wlog,
    )?;
    dev.flush().context("flushing target after build")?;
    dev.sync_all().context("syncing the target")?;
    push("Disk build complete");
    Ok(())
}

/// Copy `len` bytes at `from` in `src` to `to` in `dst`, a MiB at a time.
/// Fit a copied filesystem to its new place: grow it to the row and restamp its start LBA.
fn grow_copied_partition<T: std::io::Read + Write + std::io::Seek>(
    target: &mut T,
    p: &Placed,
    copied: u64,
    log_cb: &mut impl FnMut(&str),
) -> Result<()> {
    let offset = p.start_byte();
    if p.size_bytes > copied {
        use crate::fs::InPlaceResize;
        match crate::fs::in_place_resize_support(target, offset, Some(p.type_text.as_str())) {
            InPlaceResize::Supported(name) => {
                log_cb(&format!(
                    "Growing {name} toward {}",
                    disk_core::display::format_size(p.size_bytes)
                ));
                crate::fs::resize_filesystem_for(target, offset, p.size_bytes, log_cb)?;
            }
            InPlaceResize::Unsupported(name) => log_cb(&format!(
                "{name} can't be resized here: it keeps its size and the rest of the partition is empty"
            )),
            InPlaceResize::NoFilesystem => {}
        }
    }
    crate::fs::patch_hidden_sectors_for(target, offset, p.start_lba, log_cb)?;
    fs_pc::ntfs::ensure_backup_boot_sector(target, offset, log_cb)?;
    Ok(())
}

/// Clone a classic HFS volume into its row at a bigger block size, which no in-place grow can change.
fn rebuild_copied_hfs<R, T>(
    src: &mut R,
    from: u64,
    target: &mut T,
    p: &Placed,
    target_path: &std::path::Path,
    cancel: &impl Fn() -> bool,
    log_cb: &mut impl FnMut(&str),
) -> Result<()>
where
    R: std::io::Read + std::io::Seek + Send,
    T: Write + std::io::Seek,
{
    if p.size_bytes > fs_mac::hfs::HFS_MAX_VOLUME_BYTES {
        anyhow::bail!(
            "classic HFS stops at {}; this row is {}",
            disk_core::display::format_size(fs_mac::hfs::HFS_MAX_VOLUME_BYTES),
            disk_core::display::format_size(p.size_bytes)
        );
    }
    let block_size = fs_mac::hfs::pick_block_size(p.size_bytes);
    log_cb(&format!(
        "HFS can't grow to {} at its block size: rebuilding it at {} KiB blocks",
        disk_core::display::format_size(p.size_bytes),
        block_size / 1024
    ));
    if let Some(note) = fs_mac::hfs::mac_os_size_note(p.size_bytes) {
        log_cb(note);
    }
    // A device has no folder to hold the volume, and a device build must not format in place.
    let mut temp = match target_path.parent().filter(|d| d.is_dir()) {
        Some(dir) if !crate::os::is_device_path(target_path) => tempfile::tempfile_in(dir),
        _ => tempfile::tempfile(),
    }
    .context("creating a temporary file for the rebuilt volume")?;

    let mut source = fs_mac::hfs::HfsFilesystem::open(&mut *src, from)
        .map_err(|e| anyhow::anyhow!("opening the donor's HFS volume: {e}"))?;
    let inherited: std::collections::HashSet<String> = source
        .fsck()
        .map(|r| r.errors.into_iter().map(|e| e.code).collect())
        .unwrap_or_default();
    let name = source.volume_summary().volume_name;
    let (_, report) =
        fs_mac::hfs_clone::clone_hfs_at(&mut source, &mut temp, 0, p.size_bytes, block_size, &name)
            .map_err(|e| anyhow::anyhow!("cloning the HFS volume: {e}"))?;
    // A block size that doesn't divide the row leaves a remainder; the alternate MDB follows it.
    temp.set_len(p.size_bytes)?;
    fs_mac::hfs::fit_hfs_volume_to_partition(&mut temp, 0, p.size_bytes, log_cb)?;
    let result = fs_mac::hfs::HfsFilesystem::open(&mut temp, 0)
        .and_then(|mut fs| fs.fsck())
        .map_err(|e| anyhow::anyhow!("checking the rebuilt volume: {e}"))?;
    let fresh: Vec<&str> = result
        .errors
        .iter()
        .filter(|e| !inherited.contains(&e.code))
        .map(|e| e.code.as_str())
        .collect();
    if !fresh.is_empty() {
        anyhow::bail!("the rebuilt HFS volume fails its check: {fresh:?}");
    }
    log_cb(&format!(
        "Copied {} file(s) and {} folder(s) into the rebuilt volume",
        report.files_copied, report.dirs_copied
    ));
    copy_range(&mut temp, 0, target, p.start_byte(), p.size_bytes, cancel)
}

fn copy_range<R: std::io::Read + std::io::Seek, W: Write + std::io::Seek>(
    src: &mut R,
    from: u64,
    dst: &mut W,
    to: u64,
    len: u64,
    cancel: &impl Fn() -> bool,
) -> Result<()> {
    use std::io::SeekFrom;
    src.seek(SeekFrom::Start(from))?;
    dst.seek(SeekFrom::Start(to))?;
    let mut buf = vec![0u8; 1 << 20];
    let mut left = len;
    while left > 0 {
        if cancel() {
            anyhow::bail!("build cancelled");
        }
        let n = left.min(buf.len() as u64) as usize;
        src.read_exact(&mut buf[..n])?;
        dst.write_all(&buf[..n])?;
        left -= n as u64;
    }
    Ok(())
}

/// Encode the built raw image at `raw` into `fmt` at `dest`, reporting on `status`.
fn encode(
    fmt: ExportFormat,
    raw: &std::path::Path,
    dest: &std::path::Path,
    status: &Arc<Mutex<PhysicalWriteStatus>>,
) -> Result<()> {
    let size = std::fs::metadata(raw).map(|m| m.len()).unwrap_or(0);
    if let Ok(mut s) = status.lock() {
        s.log_messages.push(format!(
            "Encoding {} to {}",
            fmt.description(),
            dest.display()
        ));
        s.current_bytes = 0;
        s.total_bytes = size;
    }
    let progress = |n: u64| {
        if let Ok(mut s) = status.lock() {
            s.current_bytes = n;
        }
    };
    let cancel = || status.lock().map(|s| s.cancel_requested).unwrap_or(false);
    let log = |m: &str| {
        if let Ok(mut s) = status.lock() {
            s.log_messages.push(m.to_string());
        }
    };
    crate::model::bulk_convert_runner::convert_image(
        fmt, raw, dest, None, false, progress, cancel, log,
    )
    .with_context(|| format!("encoding {}", fmt.description()))
}

/// Format one volume across the whole target.
pub fn run_volume(
    target: &std::path::Path,
    size: u64,
    f: &RowFormat,
    full: bool,
    status: Arc<Mutex<PhysicalWriteStatus>>,
) -> Result<()> {
    let log = |msg: String| {
        if let Ok(mut s) = status.lock() {
            s.log_messages.push(msg);
        }
    };
    if !full {
        let done = format::format_to_file(target, size, f.kind, &f.opts)?;
        log(format!(
            "Wrote {} ({} bytes, {})",
            target.display(),
            done.len,
            done.summary
        ));
        return Ok(());
    }
    let handle = open_target_preserving(target)
        .with_context(|| format!("opening target {} for writing", target.display()))?;
    let mut dev = SectorAlignedWriter::new(handle.file);
    format_through_temp(&mut dev, 0, size, f, 0, &status)?;
    dev.flush().context("flushing target after build")?;
    dev.sync_all().context("syncing the target")?;
    log("Disk build complete".to_string());
    Ok(())
}

/// Format into a sparse temp image, then write every byte of it over `[offset, offset + size)`.
fn format_through_temp<W: Write + std::io::Seek>(
    target: &mut W,
    offset: u64,
    size: u64,
    f: &RowFormat,
    progress_base: u64,
    status: &Arc<Mutex<PhysicalWriteStatus>>,
) -> Result<u64> {
    let dir = tempfile::tempdir().context("creating a temporary folder")?;
    let tmp = dir.path().join("volume.img");
    let done = format::format_to_file(tmp.as_path(), size, f.kind, &f.opts)?;
    // The rest of the extent past the volume is zeroed too.
    std::fs::OpenOptions::new()
        .write(true)
        .open(&tmp)
        .and_then(|file| file.set_len(size))
        .context("sizing the temporary volume")?;
    let push = |msg: &str| {
        if let Ok(mut s) = status.lock() {
            s.log_messages.push(msg.to_string());
        }
    };
    push(&format!(
        "Formatted {} ({})",
        f.kind.display_name(),
        done.summary
    ));
    let mut progress = |n: u64| {
        if let Ok(mut s) = status.lock() {
            s.current_bytes = n;
        }
    };
    let cancel = || status.lock().map(|s| s.cancel_requested).unwrap_or(false);
    let mut log = |m: &str| push(m);
    physical_write_runner::write_image_into(
        target,
        tmp.as_path(),
        WriteExtent::partition_at(offset, size),
        progress_base,
        &mut progress,
        &cancel,
        &mut log,
    )
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
            disk_core::display::format_size(req.target_size_bytes),
        );
    }

    // Before the target is touched, so a bad driver disk costs nothing.
    let mac_driver = load_mac_driver(req)?;

    // A regular-file target has to exist at full length before the table is
    // written, or GPT's backup header lands past EOF in a sparse hole the
    // reparse can't find.
    if !crate::os::is_device_path(&req.target_path) {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(!req.full_write)
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

    if req.head.is_none() {
        provision::write_table(
            &mut target,
            req.kind,
            &req.partitions,
            req.target_size_bytes,
            req.geometry,
        )?;
    }
    // A donor's partitions, copied before its head: a volume header starts at byte 0.
    if let Some(donor) = &req.donor {
        let mut src = crate::model::source_reader::open_read(donor.as_path())
            .with_context(|| format!("opening the donor {}", donor.display()))?;
        for &(i, from, len) in &req.copies {
            let Some(p) = req.partitions.get(i) else {
                continue;
            };
            log_cb(&format!(
                "Copying partition {} ({}) from the donor",
                i + 1,
                disk_core::display::format_size(len)
            ));
            if p.size_bytes > len
                && fs_mac::hfs::hfs_outgrows_in_place(&mut src, from, p.size_bytes)
            {
                rebuild_copied_hfs(
                    &mut src,
                    from,
                    &mut target,
                    p,
                    req.target_path.as_path(),
                    &cancel_check,
                    &mut log_cb,
                )
                .with_context(|| format!("rebuilding partition {} from the donor", i + 1))?;
                continue;
            }
            copy_range(
                &mut src,
                from,
                &mut target,
                p.start_byte(),
                len,
                &cancel_check,
            )
            .with_context(|| format!("copying partition {} from the donor", i + 1))?;
            grow_copied_partition(&mut target, p, len, &mut log_cb)
                .with_context(|| format!("resizing partition {} after the copy", i + 1))?;
        }
    }
    if let Some(head) = &req.head {
        use std::io::Seek as _;
        target.seek(std::io::SeekFrom::Start(0))?;
        target
            .write_all(head)
            .context("writing the donor's patched label")?;
        log_cb("Wrote the donor's label, resized for this disk");
    }
    if let Some((driver, from)) = &mac_driver {
        let opts = BlessOptions::default();
        match bless_apm_disk(&mut target, req.target_size_bytes, driver, &opts) {
            Ok(_) => log_cb(&format!("Installed the Mac driver from {from}")),
            Err(e) if req.mac_driver == MacDriver::Bundled => log_cb(&format!(
                "Warning: no Mac driver installed, so a Mac won't mount this disk: {e}"
            )),
            Err(e) => anyhow::bail!("installing the Mac driver: {e}"),
        }
    }
    // The FSHD chain lands past the PART blocks, inside the RDSK's reserved area.
    let handlers = load_rdb_handlers(req.kind, req.rdb_handlers.as_slice())?;
    if !handlers.is_empty() {
        provision::write_rdb_filesystems(&mut target, handlers.as_slice())
            .context("embedding filesystem handlers in the RDB")?;
        for (dostype, image) in &handlers {
            log_cb(&format!(
                "Embedded the {} handler ({}) in the RDB",
                part_tables::rdb::format_dos_type(*dostype),
                disk_core::display::format_size(image.len() as u64),
            ));
        }
    }
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
    if !req.formats.is_empty() && req.formats.len() != req.partitions.len() {
        anyhow::bail!(
            "internal error: {} partitions but {} format slots",
            req.partitions.len(),
            req.formats.len(),
        );
    }
    let full = writes_in_full(req.target_path.as_path(), req.full_write);
    let jobs: Vec<(usize, Placed, RowFormat)> = req
        .partitions
        .iter()
        .enumerate()
        .zip(req.sources.iter())
        .zip(req.formats.iter().map(Some).chain(std::iter::repeat(None)))
        .filter_map(
            |(((i, p), src), f)| match (src, f.and_then(|f| f.as_ref())) {
                (None, Some(f)) => Some((i, p.clone(), f.clone())),
                _ => None,
            },
        )
        .collect();
    for (i, placed, f) in &jobs {
        if cancel_check() {
            anyhow::bail!("build cancelled");
        }
        log_cb(&format!(
            "Formatting partition {} as {}",
            i + 1,
            f.kind.display_name()
        ));
        if full {
            done += format_through_temp(
                &mut target,
                placed.start_byte(),
                placed.size_bytes,
                f,
                done,
                &status,
            )
            .with_context(|| format!("formatting partition {}", i + 1))?;
        } else {
            let file = target.inner_mut().context("flushing before the format")?;
            let mut w = Window::new(file, placed.start_byte(), placed.size_bytes);
            let r = format::format_into(&mut w, placed.size_bytes, f.kind, &f.opts)
                .with_context(|| format!("formatting partition {}", i + 1))?;
            log_cb(&format!("Partition {}: {}", i + 1, r.summary));
        }
        let file = target.inner_mut().context("flushing before the fit")?;
        crate::fs::fit_apple_volume_to_partition(
            file,
            placed.start_byte(),
            placed.size_bytes,
            &mut log_cb,
        )
        .with_context(|| format!("fitting the volume to partition {}", i + 1))?;
        progress_cb(done);
    }

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
        // A Mac volume smaller than its partition keeps its alternate header
        // where Mac OS will not look; fit it now rather than after a first edit.
        let file = target.inner_mut().context("flushing before the fit")?;
        crate::fs::fit_apple_volume_to_partition(
            file,
            placed.start_byte(),
            placed.size_bytes,
            &mut log_cb,
        )
        .with_context(|| format!("fitting the volume to partition {}", i + 1))?;
        progress_cb(done);
    }

    target.flush().context("flushing target after build")?;
    target.sync_all().context("syncing the target")?;
    log_cb("Disk build complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::provision::PartSpec;
    use crate::partition::PartitionTable;
    use byteorder::{BigEndian, ByteOrder};
    use std::io::{Read, Seek, SeekFrom, Write};

    const MIB: u64 = 1024 * 1024;

    /// Read `len` bytes at `offset` without pulling the whole target into
    /// memory — a 64 MiB `fs::read` is wasteful everywhere and fails on the
    /// 32-bit Windows CI leg.
    fn read_at(path: &std::path::Path, offset: u64, len: usize) -> Vec<u8> {
        let mut f = std::fs::File::open(path).expect("open target");
        f.seek(SeekFrom::Start(offset)).expect("seek");
        let mut buf = vec![0u8; len];
        f.read_exact(&mut buf).expect("read");
        buf
    }

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
        let placed =
            provision::place(&specs, TableKind::Mbr, disk, MIB, Geometry::default()).unwrap();

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
            formats: Vec::new(),
            full_write: false,
            rdb_handlers: Vec::new(),
            mac_driver: MacDriver::Off,
            head: None,
            donor: None,
            copies: Vec::new(),
        };
        run_worker(&req, new_status(0)).expect("worker");

        let mut reader = std::fs::File::open(&target_path).unwrap();
        assert_eq!(
            reader.seek(SeekFrom::End(0)).unwrap(),
            disk,
            "target sized to the whole disk",
        );
        reader.seek(SeekFrom::Start(0)).unwrap();
        let table = PartitionTable::detect(&mut reader).expect("reparse");
        let parts = table.partitions();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].start_lba, placed[0].start_lba);
        assert_eq!(parts[0].partition_type_byte, 0x0C);

        // The filled partition holds the source at its offset; the unfilled one
        // is untouched.
        let filled = read_at(&target_path, placed[0].start_byte(), 512);
        assert_eq!(&filled[3..11], b"RBTEST01");
        let empty = read_at(&target_path, placed[1].start_byte(), 512);
        assert!(empty.iter().all(|b| *b == 0));
    }

    /// Pour `image` into the first of two partitions and return the target path.
    fn build_with_source(image: Vec<u8>, part_mib: u64) -> (std::path::PathBuf, Vec<Placed>) {
        let disk = 64 * MIB;
        let specs = vec![spec(Some(part_mib * MIB), "AF"), spec(None, "83")];
        let placed =
            provision::place(&specs, TableKind::Mbr, disk, MIB, Geometry::default()).unwrap();
        let src = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(src.path(), image).unwrap();
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
            formats: Vec::new(),
            full_write: false,
            rdb_handlers: Vec::new(),
            mac_driver: MacDriver::Off,
            head: None,
            donor: None,
            copies: Vec::new(),
        };
        run_worker(&req, new_status(0)).expect("worker");
        (target_path, placed)
    }

    #[test]
    fn a_poured_hfsplus_volume_gets_its_alternate_header_at_the_partition_end() {
        // R-057: fsck_hfs read zeros where Mac OS keeps the alternate header.
        let image = fs_mac::hfsplus::create_blank_hfsplus(4 * MIB, 4096, "Fit", false);
        let (target, placed) = build_with_source(image, 8);
        let start = placed[0].start_byte();
        let primary = read_at(&target, start + 1024, 512);
        let alternate = read_at(&target, start + 8 * MIB - 1024, 512);
        assert_eq!(&primary[0..2], b"H+");
        assert_eq!(
            primary, alternate,
            "alternate header must mirror the primary"
        );
    }

    #[test]
    fn a_poured_hfs_volume_is_grown_to_its_partition() {
        // R-058: Mac OS expects the allocation area to end at the partition's
        // alternate MDB; a 3 MiB volume's bitmap can address 4 MiB.
        let image = fs_mac::hfs::create_blank_hfs(3 * MIB, 512, "Fit").unwrap();
        let before = BigEndian::read_u16(&image[1024 + 0x12..1024 + 0x14]);
        let (target, placed) = build_with_source(image, 4);
        let start = placed[0].start_byte();
        let mdb = read_at(&target, start + 1024, 512);
        let after = BigEndian::read_u16(&mdb[0x12..0x14]);
        assert!(after > before, "volume grew: {before} -> {after} blocks");
        let first_alloc = BigEndian::read_u16(&mdb[0x1C..0x1E]) as u64;
        assert_eq!(
            first_alloc * 512 + after as u64 * 512,
            4 * MIB - 1024,
            "allocation area ends at the alternate MDB"
        );
        let alternate = read_at(&target, start + 4 * MIB - 1024, 512);
        assert_eq!(&alternate[0..2], b"BD");
    }

    #[test]
    fn refuses_a_source_too_big_for_its_partition() {
        let disk = 64 * MIB;
        let specs = vec![spec(Some(2 * MIB), "83")];
        let placed =
            provision::place(&specs, TableKind::Mbr, disk, MIB, Geometry::default()).unwrap();

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
            formats: Vec::new(),
            full_write: false,
            rdb_handlers: Vec::new(),
            mac_driver: MacDriver::Off,
            head: None,
            donor: None,
            copies: Vec::new(),
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
            bootable: false,
        }];
        let target = tempfile::NamedTempFile::new().unwrap();
        let req = ProvisionRequest {
            target_path: target.path().to_path_buf(),
            target_size_bytes: 64 * MIB,
            kind: TableKind::Mbr,
            geometry: Geometry::default(),
            partitions: placed,
            sources: vec![None],
            formats: Vec::new(),
            full_write: false,
            rdb_handlers: Vec::new(),
            mac_driver: MacDriver::Off,
            head: None,
            donor: None,
            copies: Vec::new(),
        };
        let err = run_worker(&req, new_status(0)).expect_err("must refuse");
        assert!(format!("{err:#}").contains("past the end"), "{err:#}");
    }
}
