use std::fs::File;
use std::io::{self, BufWriter, Read, Seek, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};

use super::compress::OutputHasherHandle;
use super::{file_name, is_all_zeros, output_path, CHUNK_SIZE};

/// Stream raw data with optional splitting and zero-skipping.
///
/// `output_hasher` is honoured only when there's a single output file
/// (no split). With split enabled, the produced files have separate
/// sidecar checksums and the caller falls back to a post-write hash
/// pass for each — handing them all through one shared hasher would
/// produce a digest that doesn't correspond to any single on-disk
/// file. When tee'd, the hasher sees every source byte (including
/// zero runs that get sparse-seeked instead of explicitly written),
/// so the digest matches what reading the output file back would
/// produce.
pub(crate) fn stream_with_split(
    reader: &mut impl Read,
    output_base: &Path,
    extension: &str,
    split_size: Option<u64>,
    skip_zeros: bool,
    output_hasher: Option<OutputHasherHandle>,
    progress_cb: &mut dyn FnMut(u64),
    cancel_check: &dyn Fn() -> bool,
) -> Result<Vec<String>> {
    // Tee is only sound when there's one output file — see the doc
    // comment. The compressor itself doesn't know up-front how many
    // files it'll produce, so we accept the hasher and only call into
    // it when split_size is None. Callers should pass None when
    // split_size is Some to avoid building a hasher that won't fire.
    let hasher = if split_size.is_none() {
        output_hasher
    } else {
        None
    };
    let update_hasher = |bytes: &[u8]| {
        if let Some(h) = hasher.as_ref() {
            if let Ok(mut guard) = h.lock() {
                if let Some(running) = guard.as_mut() {
                    running.update(bytes);
                }
            }
        }
    };
    let mut files = Vec::new();
    let mut total_read: u64 = 0;
    let mut part_index: u32 = 0;
    let mut current_file_bytes: u64 = 0;
    let split_bytes = split_size.unwrap_or(u64::MAX);
    let mut skipped_zeros = false;

    let first_path = output_path(output_base, extension, split_size.is_some(), part_index);
    let mut writer = BufWriter::new(
        File::create(&first_path)
            .with_context(|| format!("failed to create {}", first_path.display()))?,
    );
    files.push(file_name(&first_path));

    let mut buf = vec![0u8; CHUNK_SIZE];
    loop {
        if cancel_check() {
            bail!("backup cancelled");
        }

        let n = reader.read(&mut buf).context("failed to read source")?;
        if n == 0 {
            break;
        }

        // When skip_zeros is enabled and the entire chunk is zeros, seek forward
        // in the output instead of writing. This creates a sparse file on
        // supported filesystems and saves I/O time on large mostly-empty partitions.
        if skip_zeros && is_all_zeros(&buf[..n]) {
            // Even though we skip the write, the file's read-back content
            // includes these zeros (OS fills holes with zeros). Tee them
            // to the hasher so the digest matches a post-write hash pass.
            update_hasher(&buf[..n]);
            // We still need to account for split boundaries
            let mut remaining = n;
            while remaining > 0 {
                let space_in_split = split_bytes.saturating_sub(current_file_bytes) as usize;
                let skip_amount = remaining.min(space_in_split);
                current_file_bytes += skip_amount as u64;
                remaining -= skip_amount;

                if current_file_bytes >= split_bytes && remaining > 0 {
                    // Ensure correct file length before moving to next split
                    writer.flush()?;
                    writer.get_mut().set_len(current_file_bytes)?;
                    drop(writer);
                    part_index += 1;
                    current_file_bytes = 0;
                    let next_path = output_path(output_base, extension, true, part_index);
                    writer =
                        BufWriter::new(File::create(&next_path).with_context(|| {
                            format!("failed to create {}", next_path.display())
                        })?);
                    files.push(file_name(&next_path));
                }
            }
            skipped_zeros = true;
            total_read += n as u64;
            progress_cb(total_read);
            continue;
        }

        // If we previously skipped zeros, seek the writer to the correct position
        if skipped_zeros {
            writer.flush()?;
            writer.seek(io::SeekFrom::Start(current_file_bytes))?;
            skipped_zeros = false;
        }

        let mut written = 0;
        while written < n {
            let remaining_in_split = split_bytes.saturating_sub(current_file_bytes) as usize;
            let to_write = (n - written).min(remaining_in_split);
            writer
                .write_all(&buf[written..written + to_write])
                .context("failed to write output")?;
            update_hasher(&buf[written..written + to_write]);
            current_file_bytes += to_write as u64;
            written += to_write;

            if current_file_bytes >= split_bytes && written < n {
                writer.flush()?;
                drop(writer);
                part_index += 1;
                current_file_bytes = 0;
                skipped_zeros = false;
                let next_path = output_path(output_base, extension, true, part_index);
                writer = BufWriter::new(
                    File::create(&next_path)
                        .with_context(|| format!("failed to create {}", next_path.display()))?,
                );
                files.push(file_name(&next_path));
            }
        }

        total_read += n as u64;
        progress_cb(total_read);
    }

    // Ensure correct file length if the last chunk(s) were skipped zeros
    writer.flush()?;
    if skipped_zeros {
        writer.get_mut().set_len(current_file_bytes)?;
    }

    Ok(files)
}
