//! The optical-drive read abstraction.
//!
//! Ripping (`rip.rs`) touches the physical drive through only three operations
//! — read the TOC, read raw sectors, eject. Factoring those behind
//! [`OpticalSource`] lets the rip pipeline run unchanged against either a local
//! drive ([`LocalCdReader`], via the `cd-da-reader` crate) or — in a later phase
//! — a remote drive proxied over the rb-daemon. All output encoding (ISO /
//! BIN-CUE assembly, CHD compression) stays caller-side, so swapping the
//! *reader* moves the heavy work onto the desktop while the device only streams
//! raw sectors. See `docs/remote_ripping.md`.

use std::path::Path;

use anyhow::{bail, Context, Result};
use cd_da_reader::{CdReader, ReadOptions, RetryConfig, SectorReadFormat, Toc};

/// Exclusive-use guard on a locally-attached drive, held for as long as a source
/// is open. Dropping it releases the claim.
///
/// Only macOS arbitrates access to a drive across processes (DiskArbitration);
/// elsewhere this is inert, and a plain `Option<()>` keeps the field shape the
/// same on every platform.
#[cfg(target_os = "macos")]
pub(crate) type DriveClaim = Option<crate::os::macos::DiskClaim>;
#[cfg(not(target_os = "macos"))]
pub(crate) type DriveClaim = Option<()>;

/// Take exclusive use of `device_path` for the life of the returned guard, so
/// nothing else on the system competes for the drive head during a read.
///
/// Never fails the caller: a drive that cannot be claimed is still perfectly
/// readable, just shared. See [`crate::os::macos::claim_optical_disc`].
pub(crate) fn claim_drive(device_path: &str) -> DriveClaim {
    #[cfg(target_os = "macos")]
    {
        crate::os::macos::claim_optical_disc(device_path)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = device_path;
        None
    }
}

/// A source of optical-disc sectors. The three methods are the entire physical
/// surface the rip pipeline needs, so an implementor can be a locally-attached
/// drive or a network proxy without the rip/encode code knowing the difference.
pub trait OpticalSource {
    /// Read the disc's table of contents.
    fn read_toc(&self) -> Result<Toc>;

    /// Read `count` sectors starting at `lba` in the given format. Retry / backoff
    /// is configured when the source is opened (kept next to the drive for the
    /// remote case), so it is intentionally not a parameter here.
    fn read_data_sectors(&self, lba: u32, count: u32, format: SectorReadFormat) -> Result<Vec<u8>>;

    /// Eject the disc.
    fn eject(&self) -> Result<()>;
}

/// [`OpticalSource`] backed by a physically-attached drive via `cd-da-reader`.
pub struct LocalCdReader {
    inner: CdReader,
    device_path: String,
    retry: RetryConfig,
    /// Held for the life of the reader; released on drop.
    _claim: DriveClaim,
}

impl LocalCdReader {
    /// Open `device_path` (e.g. `/dev/sr0`, `disk6`, `\\.\E:`) with the default
    /// retry policy.
    pub fn open(device_path: &str) -> Result<Self> {
        Self::with_retry(device_path, RetryConfig::default())
    }

    /// Open `device_path` with an explicit retry policy.
    pub fn with_retry(device_path: &str, retry: RetryConfig) -> Result<Self> {
        // Claim before opening: unmounting the disc out from under an open
        // handle is a good way to get spurious I/O errors mid-read.
        let claim = claim_drive(device_path);
        let mut reader = Self::with_retry_unclaimed(device_path, retry)?;
        reader._claim = claim;
        Ok(reader)
    }

    /// Open without taking exclusive use, for a caller that already holds the
    /// claim (or is only probing whether this reader can drive the medium at
    /// all). Attach the claim afterwards with [`Self::set_claim`].
    pub fn with_retry_unclaimed(device_path: &str, retry: RetryConfig) -> Result<Self> {
        let inner = match CdReader::open_path(device_path) {
            Ok(inner) => inner,
            Err(e) => open_elevated_or_fail(device_path, e)?,
        };
        Ok(Self {
            inner,
            device_path: device_path.to_string(),
            retry,
            _claim: None,
        })
    }

    /// Take ownership of an exclusive-use claim already held by the caller.
    pub(crate) fn set_claim(&mut self, claim: DriveClaim) {
        self._claim = claim;
    }
}

/// Second chance at opening a drive the direct `open` was not allowed to touch.
///
/// On macOS this escalates through `authopen`, which shows the native
/// authorization dialog and hands back a descriptor. It matters most in the GUI:
/// a terminal usually already has Full Disk Access, so `rb-cli` opens the device
/// on the first try and never lands here, while the app bundle does not and used
/// to fail with no prompt at all. `cd-da-reader` opens `/dev/rdiskN` itself and
/// has no escalation of its own, so without this the denial was terminal.
///
/// Any error other than a permission denial is passed straight through — a
/// missing disc or a detached drive is not something a password can fix.
fn open_elevated_or_fail(device_path: &str, err: cd_da_reader::CdReaderError) -> Result<CdReader> {
    #[cfg(target_os = "macos")]
    {
        let denied = matches!(
            &err,
            cd_da_reader::CdReaderError::Io(io)
                if io.kind() == std::io::ErrorKind::PermissionDenied
        );
        if denied {
            let file =
                crate::os::macos::authopen_optical_device(device_path).with_context(|| {
                    format!(
                        "Failed to open drive {device_path}: permission denied, and \
                     requesting administrator access did not succeed. Granting \
                     Rusty Backup Full Disk Access in System Settings > Privacy \
                     & Security also fixes this."
                    )
                })?;
            return Ok(CdReader::from_file(file));
        }
    }
    Err(anyhow::Error::from(err)).with_context(|| format!("Failed to open drive: {device_path}"))
}

impl OpticalSource for LocalCdReader {
    fn read_toc(&self) -> Result<Toc> {
        self.inner.read_toc().map_err(anyhow::Error::from)
    }

    fn read_data_sectors(&self, lba: u32, count: u32, format: SectorReadFormat) -> Result<Vec<u8>> {
        let options = ReadOptions::default()
            .with_format(format)
            .with_retry(self.retry.clone());
        self.inner
            .read_sector_range(lba, count, &options)
            .map_err(anyhow::Error::from)
    }

    fn eject(&self) -> Result<()> {
        eject_disc(Path::new(&self.device_path))
    }
}

/// [`OpticalSource`] that proxies every drive op to a remote daemon over an open
/// [`crate::remote::connection::RemoteConnection`]. The daemon owns the physical
/// drive and runs the retry/backoff loop; this side only requests the TOC +
/// sector ranges and does all the encoding. See `docs/remote_ripping.md`.
#[cfg(feature = "remote")]
mod remote_source {
    use std::sync::{Arc, Mutex, MutexGuard};

    use anyhow::{anyhow, Context, Result};
    use cd_da_reader::{SectorReadFormat, Toc};

    use super::OpticalSource;
    use crate::remote::connection::RemoteConnection;
    use crate::remote::protocol::{WireRetryConfig, WireSectorMode};

    pub struct RemoteCdReader {
        conn: Arc<Mutex<RemoteConnection>>,
        handle: u64,
    }

    impl RemoteCdReader {
        /// Open `device_path` on the daemon behind `conn`. `retry` is sent to the
        /// daemon and applied there (next to the drive).
        pub fn open(
            conn: Arc<Mutex<RemoteConnection>>,
            device_path: &str,
            retry: WireRetryConfig,
        ) -> Result<Self> {
            let handle = conn
                .lock()
                .map_err(|_| anyhow!("remote connection lock poisoned"))?
                .open_optical(device_path, retry)
                .with_context(|| format!("opening remote optical drive {device_path}"))?;
            Ok(Self { conn, handle })
        }

        fn lock(&self) -> Result<MutexGuard<'_, RemoteConnection>> {
            self.conn
                .lock()
                .map_err(|_| anyhow!("remote connection lock poisoned"))
        }
    }

    impl OpticalSource for RemoteCdReader {
        fn read_toc(&self) -> Result<Toc> {
            let wire = self.lock()?.read_toc(self.handle)?;
            Ok(Toc::from(&wire))
        }

        fn read_data_sectors(
            &self,
            lba: u32,
            count: u32,
            format: SectorReadFormat,
        ) -> Result<Vec<u8>> {
            self.lock()?
                .read_optical_sectors(self.handle, lba, count, WireSectorMode::from(format))
        }

        fn eject(&self) -> Result<()> {
            self.lock()?.eject_optical(self.handle)
        }
    }

    impl Drop for RemoteCdReader {
        fn drop(&mut self) {
            // Best-effort: free the daemon's optical slot. Ignore errors — the
            // socket may be gone, and the daemon reaps the session on disconnect.
            if let Ok(mut conn) = self.conn.lock() {
                let _ = conn.close_optical(self.handle);
            }
        }
    }
}

#[cfg(feature = "remote")]
pub use remote_source::RemoteCdReader;

/// Eject the disc from the drive at `path` (OS-specific shell-out).
pub fn eject_disc(path: &Path) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        let status = std::process::Command::new("eject")
            .arg(path)
            .status()
            .context("Failed to run eject command")?;
        if !status.success() {
            bail!("eject command failed with status {status}");
        }
    }

    #[cfg(target_os = "macos")]
    {
        let path_str = path.to_string_lossy();
        let status = std::process::Command::new("diskutil")
            .args(["eject", &path_str])
            .status()
            .context("Failed to run diskutil eject")?;
        if !status.success() {
            bail!("diskutil eject failed with status {status}");
        }
    }

    #[cfg(target_os = "windows")]
    {
        let path_str = path.to_string_lossy();
        // Use PowerShell to eject. The path should be like "D:" or "E:"
        let drive_letter = path_str.trim_start_matches(r"\\.\").trim_end_matches(':');
        let ps_script = format!(
            "(New-Object -ComObject Shell.Application).NameSpace(17).ParseName('{drive_letter}:').InvokeVerb('Eject')"
        );
        let status = std::process::Command::new("powershell")
            .args(["-NoProfile", "-Command", &ps_script])
            .status()
            .context("Failed to run PowerShell eject")?;
        if !status.success() {
            bail!("PowerShell eject failed with status {status}");
        }
    }

    Ok(())
}

/// [`OpticalSource`] for a **DVD or Blu-ray** in a physical drive.
///
/// The default [`LocalCdReader`] path issues MMC/SCSI pass-through commands,
/// which are unavoidable for CD (a CD's sectors are 2352 bytes on the wire) but
/// are *CD-only*: on macOS `DKIOCCDREADTOC` / `DKIOCCDREAD` against DVD or
/// Blu-ray media fail with `ENOTTY`, so a DVD could not be ripped at all — the
/// rip died before it even read a table of contents.
///
/// A data DVD/BD needs none of that. The drive presents it as a flat run of
/// 2048-byte cooked sectors, so this source reads it through
/// [`opticaldiscs::physical::PhysicalDisc`] and synthesises the single-data-track
/// TOC the rip pipeline expects. Only `Mode1Cooked` reads are possible — the raw
/// 2352-byte formats have no meaning on DVD/BD media, which is also why this
/// source serves ISO output only and never BIN/CUE.
pub struct PhysicalDiscSource {
    /// `OpticalSource` takes `&self`, while `SectorReader` reads need `&mut`.
    disc: std::sync::Mutex<opticaldiscs::physical::PhysicalDisc>,
    device_path: String,
    /// Medium length in 2048-byte sectors — becomes the TOC's lead-out.
    sector_count: u32,
    /// Held for the life of the reader; released on drop.
    _claim: DriveClaim,
}

impl PhysicalDiscSource {
    /// Open the disc in `device_path` as a flat cooked-sector medium.
    ///
    /// Fails if the medium's capacity can't be determined, since the rip needs a
    /// sector count to know where the disc ends.
    pub fn open(device_path: &str) -> Result<Self> {
        // Claim before opening, so the unmount can't yank the disc out from
        // under a live handle.
        let claim = claim_drive(device_path);
        let mut src = Self::open_unclaimed(device_path)?;
        src._claim = claim;
        Ok(src)
    }

    /// Take ownership of an exclusive-use claim already held by the caller.
    pub(crate) fn set_claim(&mut self, claim: DriveClaim) {
        self._claim = claim;
    }

    /// Open without taking exclusive use, for a caller that already holds the
    /// claim. Attach it afterwards with [`Self::set_claim`].
    pub fn open_unclaimed(device_path: &str) -> Result<Self> {
        let disc = opticaldiscs::physical::PhysicalDisc::open(device_path)
            .with_context(|| format!("Failed to open disc in {device_path}"))?;

        let sectors = disc.sector_count().ok_or_else(|| {
            anyhow::anyhow!("Could not determine the capacity of the disc in {device_path}")
        })?;
        let sector_count = u32::try_from(sectors)
            .map_err(|_| anyhow::anyhow!("Disc in {device_path} is too large to address"))?;

        Ok(Self {
            disc: std::sync::Mutex::new(disc),
            device_path: device_path.to_string(),
            sector_count,
            _claim: None,
        })
    }
}

impl OpticalSource for PhysicalDiscSource {
    fn read_toc(&self) -> Result<Toc> {
        // A data DVD/BD has no CD-style TOC: the whole medium is one data track
        // starting at LBA 0, with the lead-out at the end of the medium.
        Ok(Toc {
            first_track: 1,
            last_track: 1,
            tracks: vec![cd_da_reader::Track {
                number: 1,
                start_lba: 0,
                start_msf: cd_da_reader::lba_to_msf(0),
                is_audio: false,
            }],
            leadout_lba: self.sector_count,
        })
    }

    fn read_data_sectors(&self, lba: u32, count: u32, format: SectorReadFormat) -> Result<Vec<u8>> {
        if format != SectorReadFormat::Mode1Cooked {
            bail!(
                "DVD/Blu-ray media can only be read as 2048-byte cooked sectors \
                 ({format:?} is a CD-only raw format); rip to ISO instead"
            );
        }

        let mut disc = self
            .disc
            .lock()
            .map_err(|_| anyhow::anyhow!("disc reader lock was poisoned"))?;

        // One seek + one read for the whole run. Reading sector-at-a-time here
        // measured ~10 KB/s against a DVD-R — an optical drive charges seek and
        // rotational latency per I/O, not per byte.
        disc.read_sectors(u64::from(lba), count)
            .with_context(|| format!("Failed to read {count} sectors at LBA {lba}"))
    }

    fn eject(&self) -> Result<()> {
        eject_disc(Path::new(&self.device_path))
    }
}
