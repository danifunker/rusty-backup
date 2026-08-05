//! Drive `restore::single::run_single_partition_restore` against a real device,
//! the way the GUI's Restore -> single-partition mode does. The CLI has no verb
//! for that path (`rb-cli restore` takes a backup folder), so this is how the
//! device-only steps get exercised — notably the final FAT-clean-flag step,
//! which used to reopen the target unelevated and fail with EACCES after every
//! byte had already been written.
//!
//!   cargo run --example apm_single_partition_restore -- \
//!       /dev/disk9 <offset_bytes> <size_bytes> <start_lba> source.dmg

use std::sync::{Arc, Mutex};

use rusty_backup::restore::single::{
    run_single_partition_restore, SinglePartitionRestoreConfig, SinglePartitionSource,
};
use rusty_backup::restore::RestoreProgress;

fn main() {
    let a: Vec<String> = std::env::args().skip(1).collect();
    if a.len() != 5 {
        eprintln!(
            "usage: apm_single_partition_restore DEVICE OFFSET_BYTES SIZE_BYTES START_LBA SOURCE"
        );
        std::process::exit(2);
    }
    let config = SinglePartitionRestoreConfig {
        source: SinglePartitionSource::ImageFile {
            path: a[4].clone().into(),
        },
        target_path: a[0].clone().into(),
        target_is_device: true,
        target_offset_bytes: a[1].parse().expect("offset"),
        target_size_bytes: Some(a[2].parse().expect("size")),
        target_start_lba: a[3].parse().expect("start lba"),
        source_start_lba: 0,
        new_disk: None,
    };

    let progress = Arc::new(Mutex::new(RestoreProgress::new()));
    let result = run_single_partition_restore(config, Arc::clone(&progress));

    if let Ok(p) = progress.lock() {
        for m in &p.log_messages {
            println!("  [{:?}] {}", m.level, m.message);
        }
    }
    match result {
        Ok(()) => println!("RESULT: ok"),
        Err(e) => {
            println!("RESULT: FAILED: {e:#}");
            std::process::exit(1);
        }
    }
}
