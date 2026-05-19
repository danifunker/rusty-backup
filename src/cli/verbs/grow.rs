//! `rb-cli grow IMG --add SIZE` — grow a disk image by `SIZE` bytes of
//! trailing zero-padding so subsequent `partmap` edits can place a new
//! partition in the freshly-available free space.
//!
//! For `.chd` images this re-encodes through
//! [`crate::model::chd_expand_runner`] (CHD's hunk layout is fixed at
//! creation, so growing is a re-encode). For everything else
//! (`.img`/`.hda`/`.vhd`/raw) we just append zero bytes to the file.

use anyhow::{bail, Context, Result};
use clap::Args;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;
use crate::model::chd_expand_runner;
use crate::model::status::ChdExpandStatus;
use crate::partition::format_size;

#[derive(Debug, Args)]
pub struct GrowArgs {
    /// Image to grow.
    pub image: PathBuf,
    /// Bytes of zero-padding to add at the end (e.g. `512M`, `2G`).
    #[arg(long)]
    pub add: String,
}

pub fn run(args: GrowArgs) -> Result<()> {
    let add = parse_size(&args.add).context("parsing --add")?;
    if add == 0 {
        bail!("--add must be > 0");
    }

    let is_chd = args
        .image
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.eq_ignore_ascii_case("chd"))
        .unwrap_or(false);

    if is_chd {
        log_stderr(format!(
            "rb-cli grow: re-encoding {} (+{})",
            args.image.display(),
            format_size(add)
        ));
        let status = chd_expand_runner::spawn(args.image.clone(), add);
        drain_chd(status)
    } else {
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&args.image)
            .with_context(|| format!("opening {}", args.image.display()))?;
        use std::io::{Seek, SeekFrom, Write};
        let cur = f.seek(SeekFrom::End(0))?;
        log_stderr(format!(
            "rb-cli grow: appending {} of zeros to {} (current size {})",
            format_size(add),
            args.image.display(),
            format_size(cur)
        ));
        // Write in 1-MiB chunks so very large grows don't allocate huge buffers.
        let chunk = vec![0u8; 1024 * 1024];
        let mut remaining = add;
        while remaining > 0 {
            let n = remaining.min(chunk.len() as u64) as usize;
            f.write_all(&chunk[..n])?;
            remaining -= n as u64;
        }
        f.flush()?;
        log_stderr(format!("grown: new size {}", format_size(cur + add)));
        Ok(())
    }
}

fn drain_chd(status: Arc<Mutex<ChdExpandStatus>>) -> Result<()> {
    let mut last_pct: i32 = -1;
    loop {
        std::thread::sleep(Duration::from_millis(250));
        let (logs, cur, total, finished, error) = match status.lock() {
            Ok(mut s) => (
                s.log_messages.drain(..).collect::<Vec<String>>(),
                s.current_bytes,
                s.total_bytes,
                s.finished,
                s.error.clone(),
            ),
            Err(_) => bail!("chd_expand worker poisoned its status mutex"),
        };
        for line in logs {
            log_stderr(format!("  {line}"));
        }
        if total > 0 {
            let pct = ((cur as f64 / total as f64) * 100.0) as i32;
            if pct / 5 != last_pct / 5 {
                log_stderr(format!(
                    "  progress: {pct:>3}% ({}/{})",
                    format_size(cur),
                    format_size(total)
                ));
                last_pct = pct;
            }
        }
        if finished {
            if let Some(e) = error {
                bail!("grow failed: {e}");
            }
            return Ok(());
        }
    }
}
