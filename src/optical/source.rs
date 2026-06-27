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

/// [`OpticalSource`] that proxies every drive op to a remote daemon over an open
/// [`crate::remote::connection::RemoteConnection`]. The daemon owns the physical
/// drive and runs the retry/backoff loop; this side only requests the TOC +
/// sector ranges and does all the encoding. See `docs/remote_ripping.md`.
#[cfg(feature = "remote")]
mod remote_source {
    use std::sync::{Arc, Mutex, MutexGuard};

    use anyhow::{anyhow, Context, Result};
    use cd_da_reader::{RetryConfig, SectorReadMode, Toc};

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
            retry: RetryConfig,
        ) -> Result<Self> {
            let handle = conn
                .lock()
                .map_err(|_| anyhow!("remote connection lock poisoned"))?
                .open_optical(device_path, WireRetryConfig::from(&retry))
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

        fn read_data_sectors(&self, lba: u32, count: u32, mode: SectorReadMode) -> Result<Vec<u8>> {
            self.lock()?
                .read_optical_sectors(self.handle, lba, count, WireSectorMode::from(mode))
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
