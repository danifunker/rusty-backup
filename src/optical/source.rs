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
use cd_da_reader::{CdReader, RetryConfig, SectorReadMode, Toc};

/// A source of optical-disc sectors. The three methods are the entire physical
/// surface the rip pipeline needs, so an implementor can be a locally-attached
/// drive or a network proxy without the rip/encode code knowing the difference.
pub trait OpticalSource {
    /// Read the disc's table of contents.
    fn read_toc(&self) -> Result<Toc>;

    /// Read `count` sectors starting at `lba` in the given mode. Retry / backoff
    /// is configured when the source is opened (kept next to the drive for the
    /// remote case), so it is intentionally not a parameter here.
    fn read_data_sectors(&self, lba: u32, count: u32, mode: SectorReadMode) -> Result<Vec<u8>>;

    /// Eject the disc.
    fn eject(&self) -> Result<()>;
}

/// [`OpticalSource`] backed by a physically-attached drive via `cd-da-reader`.
pub struct LocalCdReader {
    inner: CdReader,
    device_path: String,
    retry: RetryConfig,
}

impl LocalCdReader {
    /// Open `device_path` (e.g. `/dev/sr0`, `disk6`, `\\.\E:`) with the default
    /// retry policy.
    pub fn open(device_path: &str) -> Result<Self> {
        Self::with_retry(device_path, RetryConfig::default())
    }

    /// Open `device_path` with an explicit retry policy.
    pub fn with_retry(device_path: &str, retry: RetryConfig) -> Result<Self> {
        let inner = CdReader::open(device_path)
            .with_context(|| format!("Failed to open drive: {device_path}"))?;
        Ok(Self {
            inner,
            device_path: device_path.to_string(),
            retry,
        })
    }
}

impl OpticalSource for LocalCdReader {
    fn read_toc(&self) -> Result<Toc> {
        self.inner.read_toc().map_err(anyhow::Error::from)
    }

    fn read_data_sectors(&self, lba: u32, count: u32, mode: SectorReadMode) -> Result<Vec<u8>> {
        self.inner
            .read_data_sectors(lba, count, mode, &self.retry)
            .map_err(anyhow::Error::from)
    }

    fn eject(&self) -> Result<()> {
        eject_disc(Path::new(&self.device_path))
    }
}

/// Eject the disc from the drive at `path` (OS-specific shell-out).
fn eject_disc(path: &Path) -> Result<()> {
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
