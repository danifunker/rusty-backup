//! Background worker that swaps the two bytes of every 16-bit word in a disk
//! image, the GUI counterpart of `rb-cli swab16`.
//!
//! The transform itself lives in [`crate::rbformats::swab`]; this wraps it in
//! the runner pattern (callbacks at the leaf, `Status` at the runner) so the
//! view can poll progress each frame without owning a thread. See
//! [`super::status::SwabStatus`] and `docs/progress_pattern.md`.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;

use super::status::SwabStatus;
use crate::partition::{format_size, PartitionTable};
use crate::rbformats::swab;

/// Spawn a worker that writes a word-swapped copy of `input` to `output`, or
/// rewrites `input` itself when `output` is `None`. Returns a shared status
/// the caller polls each frame.
pub fn spawn(input: PathBuf, output: Option<PathBuf>) -> Arc<Mutex<SwabStatus>> {
    let status = Arc::new(Mutex::new(SwabStatus::default()));
    let status_clone = Arc::clone(&status);

    thread::spawn(move || {
        let target = output.clone().unwrap_or_else(|| input.clone());
        let before = describe(&input);
        log(
            &status_clone,
            format!(
                "Swapping 16-bit word order: {} (detected: {before})",
                input.display()
            ),
        );

        let status_progress = Arc::clone(&status_clone);
        let mut progress = move |done: u64, total: u64| {
            if let Ok(mut s) = status_progress.lock() {
                s.current_bytes = done;
                s.total_bytes = total;
            }
        };

        let result = match &output {
            Some(o) => swab::swab16_file(&input, o, &mut progress),
            None => swab::swab16_file_in_place(&input, &mut progress),
        };

        match result {
            Ok(written) => {
                let after = describe(&target);
                log(
                    &status_clone,
                    format!(
                        "Wrote {} to {} (detected after: {after})",
                        format_size(written),
                        target.display()
                    ),
                );
                if let Ok(mut s) = status_clone.lock() {
                    s.output_path = Some(target);
                }
            }
            Err(e) => {
                if let Ok(mut s) = status_clone.lock() {
                    s.error = Some(format!("{e}"));
                }
            }
        }
        if let Ok(mut s) = status_clone.lock() {
            s.finished = true;
        }
    });

    status
}

/// Best-effort probe so the log shows the orientation actually flipping.
fn describe(path: &std::path::Path) -> String {
    let mut f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return "unreadable".to_string(),
    };
    match PartitionTable::detect(&mut f) {
        Ok(t) => match t.byte_order_name() {
            Some(order) => format!("{} ({order})", t.type_name()),
            None => t.type_name().to_string(),
        },
        Err(_) => "unrecognized".to_string(),
    }
}

fn log(status: &Arc<Mutex<SwabStatus>>, msg: String) {
    if let Ok(mut s) = status.lock() {
        s.log_messages.push(msg);
    }
}
