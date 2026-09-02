//! Fork-preserving single-file export, shared by the browse view's "Extract"
//! and Commander's copy/export-to-host.
//!
//! Reads a file's data fork (`read_file`) and resource fork
//! (`write_resource_fork_to`) off any [`Filesystem`] and writes it into a host
//! directory in the chosen [`ResourceForkMode`]:
//! - **MacBinary** — single `.bin` carrying both forks (only when a resource
//!   fork is present; otherwise falls through to a plain data-fork write).
//! - **BinHex** — single `.hqx` text file (always applicable; carries Finder
//!   info even with no resource fork).
//! - **AppleDouble** — data fork under the plain name + a `._name` sidecar
//!   (emitted whenever there's a resource fork *or* type/creator to preserve).
//! - **Native** — data fork + the macOS `..namedfork/rsrc` write (macOS only).
//! - **SeparateRsrc** — data fork + a `name.rsrc` sidecar.
//! - **DataForkOnly** — just the data fork.
//!
//! This mirrors the browse view's `extract_entry` per-file logic in one place
//! so Commander gets identical output, and it works over the remote wire too
//! (`RemoteFilesystem` implements `write_resource_fork_to` via the daemon's
//! resource-fork read op).

use std::path::Path;

use anyhow::{Context, Result};

use crate::fs::binhex;
use crate::fs::entry::FileEntry;
use crate::fs::filesystem::Filesystem;
use crate::fs::resource_fork::{self, MacFileDates, ResourceForkMode};

/// The default sanitized host filename for `entry` (no ProDOS type suffix).
/// Callers with no special naming needs pass this as `out_name`.
pub fn safe_name(entry: &FileEntry) -> String {
    resource_fork::sanitize_filename(&entry.name)
}

/// The host path [`export_file_with_fork`] writes `entry`'s data to under
/// `mode`, so a caller can see a collision before anything is overwritten.
pub fn primary_output_path(
    entry: &FileEntry,
    dest_dir: &Path,
    out_name: &str,
    mode: ResourceForkMode,
) -> std::path::PathBuf {
    let has_rsrc = entry.resource_fork_size.map(|s| s > 0).unwrap_or(false);
    match mode {
        ResourceForkMode::MacBinary if has_rsrc => dest_dir.join(format!("{out_name}.bin")),
        ResourceForkMode::BinHex => dest_dir.join(format!("{out_name}.hqx")),
        _ => dest_dir.join(out_name),
    }
}

/// Export one FILE `entry` into `dest_dir` under the base name `out_name`
/// (already sanitized / suffixed by the caller — e.g. the browse view appends a
/// ProDOS `#TTAAAA` suffix), using `mode`. Returns the number of bytes written
/// (data + resource) for progress accounting. The caller handles directories
/// (create + recurse). Use [`safe_name`] for the default sanitized name.
pub fn export_file_with_fork(
    fs: &mut dyn Filesystem,
    entry: &FileEntry,
    dest_dir: &Path,
    out_name: &str,
    mode: ResourceForkMode,
) -> Result<u64> {
    let safe = out_name;
    let has_rsrc = entry.resource_fork_size.map(|s| s > 0).unwrap_or(false);
    let type_code = entry.type_code.unwrap_or([0; 4]);
    let creator_code = entry.creator_code.unwrap_or([0; 4]);
    // HFS/HFS+ catalog dates are already raw Mac-1904 seconds — the exact
    // MacBinary / AppleDouble encoding — so carry them straight through.
    let dates = entry
        .mac_dates
        .map(|(created, modified, _backup)| MacFileDates { created, modified })
        .unwrap_or_default();

    let read_data = |fs: &mut dyn Filesystem| -> Result<Vec<u8>> {
        fs.read_file(entry, usize::MAX)
            .with_context(|| format!("reading '{}'", entry.name))
    };
    let read_rsrc = |fs: &mut dyn Filesystem| -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        fs.write_resource_fork_to(entry, &mut buf)
            .with_context(|| format!("reading resource fork of '{}'", entry.name))?;
        Ok(buf)
    };

    if mode == ResourceForkMode::MacBinary && has_rsrc {
        let data = read_data(fs)?;
        let rsrc = read_rsrc(fs)?;
        let mb =
            resource_fork::build_macbinary(safe, &type_code, &creator_code, dates, &data, &rsrc);
        let out = dest_dir.join(format!("{safe}.bin"));
        std::fs::write(&out, &mb).with_context(|| format!("writing {}", out.display()))?;
        return Ok(data.len() as u64 + rsrc.len() as u64);
    }

    if mode == ResourceForkMode::BinHex {
        let data = read_data(fs)?;
        let rsrc = if has_rsrc { read_rsrc(fs)? } else { Vec::new() };
        let bh = binhex::BinHexFile {
            name: entry.name.clone(),
            type_code,
            creator_code,
            flags: 0,
            data_fork: data,
            resource_fork: rsrc,
        };
        let text = binhex::build_binhex(&bh);
        let out = dest_dir.join(format!("{safe}.hqx"));
        std::fs::write(&out, text.as_bytes())
            .with_context(|| format!("writing {}", out.display()))?;
        return Ok(bh.data_fork.len() as u64 + bh.resource_fork.len() as u64);
    }

    // Data fork to the plain name...
    let out = dest_dir.join(safe);
    let mut f =
        std::fs::File::create(&out).with_context(|| format!("creating {}", out.display()))?;
    let written = fs
        .write_file_to(entry, &mut f)
        .with_context(|| format!("extracting '{}'", entry.name))?;
    drop(f);
    // Preserve the source's mtime on the extracted host file when the
    // source recorded one — a plain image->host extract now round-trips
    // the date the same way tar's `-p` flag does. Best-effort: a
    // filesystem that doesn't record dates leaves this None and the OS
    // stamps the current time as before. Errors are non-fatal (some
    // network filesystems / SMB shares reject setting mtime).
    apply_host_mtime(&out, entry);

    // ...plus a resource-fork sidecar for the fork-carrying "beside the data
    // fork" modes. MacBinary-without-rsrc and DataForkOnly land here as a plain
    // data-fork write with no sidecar.
    let mut extra = 0u64;
    let has_finfo = type_code != [0; 4] || creator_code != [0; 4];
    match mode {
        ResourceForkMode::Native if has_rsrc => {
            let rsrc = read_rsrc(fs)?;
            let p = out.join("..namedfork/rsrc");
            std::fs::write(&p, &rsrc)
                .with_context(|| format!("writing native resource fork of '{}'", entry.name))?;
            extra = rsrc.len() as u64;
        }
        ResourceForkMode::AppleDouble if has_rsrc || has_finfo => {
            let rsrc = if has_rsrc { read_rsrc(fs)? } else { Vec::new() };
            let ad = resource_fork::build_appledouble(&type_code, &creator_code, dates, &rsrc);
            let p = dest_dir.join(format!("._{safe}"));
            std::fs::write(&p, &ad)
                .with_context(|| format!("writing AppleDouble sidecar of '{}'", entry.name))?;
            extra = rsrc.len() as u64;
        }
        ResourceForkMode::SeparateRsrc if has_rsrc => {
            let rsrc = read_rsrc(fs)?;
            let p = dest_dir.join(format!("{safe}.rsrc"));
            std::fs::write(&p, &rsrc)
                .with_context(|| format!("writing .rsrc sidecar of '{}'", entry.name))?;
            extra = rsrc.len() as u64;
        }
        _ => {}
    }

    Ok(written + extra)
}

/// Stamp the extracted host file's mtime/atime from `entry.modified_unix`.
/// A best-effort call: filesystems that can't set times (network mounts,
/// some FUSE-backed FSes) just leave the OS default, they don't fail the
/// export.
fn apply_host_mtime(path: &Path, entry: &FileEntry) {
    let Some(secs) = entry.modified_unix else {
        return;
    };
    let ft = filetime::FileTime::from_unix_time(secs as i64, 0);
    let _ = filetime::set_file_times(path, ft, ft);
}
