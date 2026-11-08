//! [`convert_image`]: one disk image to an [`ExportFormat`], for Build Disk's container output.
//! The Convert tab's engine is `model::convert`.

use std::path::Path;

use crate::rbformats::chd_options::ChdOptions;
#[cfg(feature = "chd")]
use crate::rbformats::chd_options::ChdProfile;
use crate::rbformats::containers::convert_floppy_container;
use crate::rbformats::export::{export_whole_disk, ExportFormat};
#[cfg(feature = "chd")]
use crate::rbformats::export::{
    export_whole_disk_bincue, export_whole_disk_chd, export_whole_disk_chd_cd,
};

/// Convert one disk image to `format` at `dest`: Build Disk's container output.
#[allow(clippy::too_many_arguments)]
pub fn convert_image(
    format: ExportFormat,
    file: &Path,
    dest: &Path,
    chd_options: Option<ChdOptions>,
    bincue_multi_bin: bool,
    progress_cb: impl FnMut(u64),
    cancel_cb: impl Fn() -> bool,
    mut log_cb: impl FnMut(&str),
) -> anyhow::Result<()> {
    match format {
        #[cfg(feature = "chd")]
        ExportFormat::Chd => export_whole_disk_chd(
            file,
            None,
            None,
            &[],
            dest,
            ChdProfile::Hd,
            chd_options.clone(),
            progress_cb,
            cancel_cb,
            log_cb,
        ),
        #[cfg(feature = "chd")]
        ExportFormat::ChdDvd => export_whole_disk_chd(
            file,
            None,
            None,
            &[],
            dest,
            ChdProfile::Dvd,
            chd_options.clone(),
            progress_cb,
            cancel_cb,
            log_cb,
        ),
        #[cfg(feature = "chd")]
        ExportFormat::ChdCd => {
            let _ = progress_cb;
            export_whole_disk_chd_cd(file, dest, chd_options.clone(), cancel_cb, log_cb)
        }
        #[cfg(feature = "chd")]
        ExportFormat::BinCue => {
            let _ = progress_cb;
            export_whole_disk_bincue(file, dest, bincue_multi_bin, cancel_cb, log_cb)
        }
        #[cfg(not(feature = "chd"))]
        ExportFormat::Chd | ExportFormat::ChdDvd | ExportFormat::ChdCd | ExportFormat::BinCue => {
            let _ = (progress_cb, cancel_cb, bincue_multi_bin, chd_options);
            Err(anyhow::anyhow!(
                "this binary was built without the `chd` feature; \
                 CHD / BIN/CUE output is unavailable"
            ))
        }
        f if f.is_x68k_floppy() => {
            // Floppy containers don't go through the streaming
            // `export_whole_disk` path — the engine reads the whole
            // source into RAM, decodes, encodes the target wrapper,
            // and writes it. Small (≤ 1.5 MB), so no progress reporting.
            let _ = (progress_cb, cancel_cb);
            match f.to_floppy_container_kind() {
                Some(kind) => convert_floppy_container(file, dest, kind)
                    .map(|r| {
                        log_cb(&format!(
                            "Converted floppy {} -> {} ({}, {} bytes)",
                            r.source.display_name(),
                            r.target.display_name(),
                            r.media.display_label(),
                            r.bytes_written,
                        ));
                    })
                    .map_err(|e| anyhow::anyhow!("floppy convert failed: {e:#}")),
                None => Err(anyhow::anyhow!(
                    "internal: is_x68k_floppy() returned true but \
                     to_floppy_container_kind() returned None for {f:?}"
                )),
            }
        }
        _ => export_whole_disk(
            format,
            file,
            None,
            None,
            &[],
            dest,
            progress_cb,
            cancel_cb,
            log_cb,
        ),
    }
}
