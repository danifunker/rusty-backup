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
#[cfg(feature = "chd")]
use std::sync::{Arc, Mutex};
#[cfg(feature = "chd")]
use std::time::Duration;

use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;
#[cfg(feature = "chd")]
use crate::model::chd_expand_runner;
#[cfg(feature = "chd")]
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
        #[cfg(feature = "chd")]
        {
            log_stderr(format!(
                "rb-cli grow: re-encoding {} (+{})",
                args.image.display(),
                format_size(add)
            ));
            let status = chd_expand_runner::spawn(args.image.clone(), add);
            drain_chd(status)
        }
        #[cfg(not(feature = "chd"))]
        {
            bail!(
                "this binary was built without the `chd` feature; \
                 growing a .chd image requires the full build"
            )
        }
    } else if crate::model::source_reader::is_editable_container_path(&args.image) {
        // Floppy / gzip / WOZ containers are fixed-geometry wrappers; appending
        // zero bytes to the container file would corrupt it (the bytes aren't
        // raw image data). Refuse rather than mangle the file.
        bail!(
            "cannot grow {}: floppy / gzip / WOZ containers are fixed-geometry. \
             Convert to a raw image first (`rb-cli convert ... --format raw`) if you need to resize.",
            args.image.display()
        );
    } else {
        // The GUI's Expand Image helper: a fixed VHD keeps its footer at the
        // end, and a container whose bytes are not the disk is refused.
        let new_total = crate::partition::resize::expand_image_file(&args.image, add, &mut |m| {
            log_stderr(format!("rb-cli grow: {m}"))
        })
        .with_context(|| format!("growing {}", args.image.display()))?;
        log_stderr(format!("grown: new size {}", format_size(new_total)));
        Ok(())
    }
}

#[cfg(feature = "chd")]
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
