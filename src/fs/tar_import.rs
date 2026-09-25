//! Import a `.tar.gz` / `.tar.zst` / `.tar` archive's contents INTO a disk
//! image's filesystem — the inverse of [`crate::fs::tar_export`].
//!
//! Reads the archive (compression auto-detected from magic), recreates the
//! directory tree, streams each file into the target via the
//! [`EditableFilesystem`] API, and recreates symlinks where the target FS
//! supports them. Filesystems that can't store a symlink or a Unix mode are
//! handled gracefully — those entries are skipped/ignored and counted, so a
//! `tar -> FAT` import doesn't abort on the first symlink. (Symmetric with
//! the export, which reports dropped resource forks.)
//!
//! The caller owns the sync/commit lifecycle: like every other
//! `EditableFilesystem` mutation, callers MUST call `sync_metadata()` (and,
//! for a container, `commit`) after import returns.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use anyhow::{Context, Result};

use crate::fs::entry::FileEntry;
use crate::fs::filesystem::EditableFilesystem;
use crate::fs::import_sink::{
    is_appledouble, safe_components, ImportItem, ImportOptions, Importer,
};

// The conflict policy, the options bag, the stats tally and the preflight
// projection are all shared with `dir_import` — a tar stream and a host
// directory walk differ only in where entries come from. Re-exported here so
// the long-standing `tar_import::` paths keep working.
pub use crate::fs::import_sink::{
    ImportConflict, ImportPreflight as TarImportPreflight, ImportStats as TarImportStats,
};

/// Knobs for [`import_tar`]. Alias of the shared [`ImportOptions`].
pub type TarImportOptions = ImportOptions;

/// Cheap content sniff: does `path` look like a tar archive — plain, gzip-, or
/// zstd-compressed? Only a small prefix is (de)compressed to check for the tar
/// `ustar` magic at offset 257, so a gzip *disk image* (`.adz` / `.hdz`) is
/// **not** mistaken for a tarball just because it's gzip. Used by the GUI to
/// auto-route a dropped/added tar archive into the import flow.
pub fn looks_like_tar_archive(path: &Path) -> bool {
    let Ok(mut f) = File::open(path) else {
        return false;
    };
    let mut magic = [0u8; 4];
    let n = f.read(&mut magic).unwrap_or(0);
    if f.seek(SeekFrom::Start(0)).is_err() {
        return false;
    }
    // Need to see at least the first tar header (512 B) -> read ~600 of the
    // (decompressed) stream so offset 257..262 is covered.
    let prefix = if n >= 2 && magic[0] == 0x1f && magic[1] == 0x8b {
        read_prefix(flate2::read::GzDecoder::new(f), 600)
    } else if n >= 4 && magic == [0x28, 0xb5, 0x2f, 0xfd] {
        match crate::rbformats::zstd_compat::decoder(f) {
            Ok(d) => read_prefix(d, 600),
            Err(_) => return false,
        }
    } else {
        read_prefix(f, 600)
    };
    // ustar magic: "ustar\0" (POSIX) or "ustar  " (GNU) — both start "ustar".
    (prefix.len() >= 262 && &prefix[257..262] == b"ustar") || is_v7_tar_header(&prefix)
}

/// A pre-POSIX (v7) tar header, which has no magic: a name, octal numeric fields, and a
/// checksum that matches. NeXT's own patch tarballs are this shape.
fn is_v7_tar_header(h: &[u8]) -> bool {
    if h.len() < 512 || h[0] == 0 {
        return false;
    }
    // mode, uid, gid, size, mtime must all be octal before the checksum is worth checking.
    let numeric_ok = [(100, 108), (108, 116), (116, 124), (124, 136), (136, 148)]
        .iter()
        .all(|&(a, b)| tar_octal(&h[a..b]).is_some());
    let Some(stored) = tar_octal(&h[148..156]).filter(|_| numeric_ok) else {
        return false;
    };
    let sum: u64 = h[..512]
        .iter()
        .enumerate()
        .map(|(i, b)| {
            if (148..156).contains(&i) {
                32
            } else {
                u64::from(*b)
            }
        })
        .sum();
    sum == stored
}

/// A tar numeric field: optional leading spaces, octal digits, then only NULs or spaces.
/// An empty field reads as 0, as GNU tar reads it.
fn tar_octal(field: &[u8]) -> Option<u64> {
    let body = &field[field.iter().take_while(|b| **b == b' ').count()..];
    let n = body
        .iter()
        .take_while(|b| (b'0'..=b'7').contains(*b))
        .count();
    if !body[n..].iter().all(|b| *b == 0 || *b == b' ') {
        return None;
    }
    if n == 0 {
        return Some(0);
    }
    u64::from_str_radix(std::str::from_utf8(&body[..n]).ok()?, 8).ok()
}

/// An entry's type, reading a pre-POSIX tar's trailing `/` as the directory it marks.
/// v7 tar had no directory typeflag; GNU tar reads it the same way.
fn entry_kind<R: Read>(entry: &tar::Entry<'_, R>) -> tar::EntryType {
    let t = entry.header().entry_type();
    if t.is_file() && entry.path_bytes().ends_with(b"/") {
        tar::EntryType::Directory
    } else {
        t
    }
}

fn read_prefix(mut r: impl Read, n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    let mut filled = 0;
    while filled < n {
        match r.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(k) => filled += k,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
    }
    buf.truncate(filled);
    buf
}

/// Import from a host archive path, auto-detecting gzip / zstd / plain tar
/// from the leading magic bytes.
pub fn import_tar_from_path(
    efs: &mut dyn EditableFilesystem,
    dest: &FileEntry,
    path: &Path,
    opts: &TarImportOptions,
    progress: &dyn Fn(&TarImportStats),
) -> Result<TarImportStats> {
    let mut file =
        File::open(path).with_context(|| format!("opening archive {}", path.display()))?;
    let mut magic = [0u8; 4];
    let n = file.read(&mut magic).unwrap_or(0);
    file.seek(SeekFrom::Start(0)).context("rewind archive")?;

    if n >= 2 && magic[0] == 0x1f && magic[1] == 0x8b {
        let dec = flate2::read::GzDecoder::new(file);
        import_tar(efs, dest, dec, opts, progress)
    } else if n >= 4 && magic == [0x28, 0xb5, 0x2f, 0xfd] {
        let dec = crate::rbformats::zstd_compat::decoder(file).context("init zstd decoder")?;
        import_tar(efs, dest, dec, opts, progress)
    } else {
        import_tar(efs, dest, file, opts, progress)
    }
}

/// Import an (already-decompressed, or plain) tar stream into `dest`.
///
/// Brackets the work in [`EditableFilesystem::begin_bulk`] /
/// [`end_bulk`](EditableFilesystem::end_bulk). The importer aborts on the first
/// hard error (every create is `?`-propagated) and callers discard the volume
/// on `Err`, so per-operation rollback is redundant here; bulk mode lets HFS
/// skip cloning its whole catalog on every entry. `end_bulk` runs even on
/// error so the filesystem is never left stuck in bulk mode.
pub fn import_tar<R: Read>(
    efs: &mut dyn EditableFilesystem,
    dest: &FileEntry,
    archive: R,
    opts: &TarImportOptions,
    progress: &dyn Fn(&TarImportStats),
) -> Result<TarImportStats> {
    efs.begin_bulk();
    let result = import_tar_inner(efs, dest, archive, opts, progress);
    efs.end_bulk();
    result
}

/// [`import_tar`] without the bulk-mode bracketing, for callers already inside
/// one — `dir_import` expanding an archive it found mid-walk.
///
/// Bulk mode is a plain flag, not a counter (see `HfsFilesystem::begin_bulk`),
/// so a nested `import_tar` would clear it on the way out and leave the rest of
/// the enclosing import running unbracketed. The caller owns it instead.
pub fn import_tar_into<R: Read>(
    efs: &mut dyn EditableFilesystem,
    dest: &FileEntry,
    archive: R,
    opts: &TarImportOptions,
    progress: &dyn Fn(&TarImportStats),
) -> Result<TarImportStats> {
    import_tar_inner(efs, dest, archive, opts, progress)
}

/// [`import_tar_from_path`] without the bulk-mode bracketing. See
/// [`import_tar_into`].
pub fn import_tar_from_path_into(
    efs: &mut dyn EditableFilesystem,
    dest: &FileEntry,
    path: &Path,
    opts: &TarImportOptions,
    progress: &dyn Fn(&TarImportStats),
) -> Result<TarImportStats> {
    let mut file =
        File::open(path).with_context(|| format!("opening archive {}", path.display()))?;
    let mut magic = [0u8; 4];
    let n = file.read(&mut magic).unwrap_or(0);
    file.seek(SeekFrom::Start(0)).context("rewind archive")?;

    if n >= 2 && magic[0] == 0x1f && magic[1] == 0x8b {
        import_tar_into(
            efs,
            dest,
            flate2::read::GzDecoder::new(file),
            opts,
            progress,
        )
    } else if n >= 4 && magic == [0x28, 0xb5, 0x2f, 0xfd] {
        let dec = crate::rbformats::zstd_compat::decoder(file).context("init zstd decoder")?;
        import_tar_into(efs, dest, dec, opts, progress)
    } else {
        import_tar_into(efs, dest, file, opts, progress)
    }
}

/// Total (files, dirs, content bytes) an archive would expand to, read from
/// its headers without extracting. Feeds `--size auto`, where the archive's
/// own compressed size is a badly wrong estimate of what it costs in the image.
pub fn measure_tar_expanded(path: &Path) -> Result<(u64, u64, u64)> {
    fn tally<R: Read>(archive: R) -> Result<(u64, u64, u64)> {
        let mut files = 0u64;
        let mut dirs = 0u64;
        let mut bytes = 0u64;
        let mut ar = tar::Archive::new(archive);
        for entry in ar.entries().context("reading tar entries")? {
            let entry = entry.context("reading tar entry")?;
            let etype = entry_kind(&entry);
            if etype.is_dir() {
                dirs += 1;
            } else if etype.is_file() {
                files += 1;
                bytes += entry.size();
            }
        }
        Ok((files, dirs, bytes))
    }

    let mut file =
        File::open(path).with_context(|| format!("opening archive {}", path.display()))?;
    let mut magic = [0u8; 4];
    let n = file.read(&mut magic).unwrap_or(0);
    file.seek(SeekFrom::Start(0)).context("rewind archive")?;
    if n >= 2 && magic[0] == 0x1f && magic[1] == 0x8b {
        tally(flate2::read::GzDecoder::new(file))
    } else if n >= 4 && magic == [0x28, 0xb5, 0x2f, 0xfd] {
        tally(crate::rbformats::zstd_compat::decoder(file).context("init zstd decoder")?)
    } else {
        tally(file)
    }
}

fn import_tar_inner<R: Read>(
    efs: &mut dyn EditableFilesystem,
    dest: &FileEntry,
    archive: R,
    opts: &TarImportOptions,
    progress: &dyn Fn(&TarImportStats),
) -> Result<TarImportStats> {
    let mut sink = Importer::new(dest);
    // macOS tar writes `._name` right before `name`; the parsed sidecar waits
    // here so the primary file lands with its resource fork and type codes.
    let mut pending_forks: std::collections::HashMap<
        Vec<String>,
        crate::fs::resource_fork::ImportedResourceFork,
    > = std::collections::HashMap::new();
    const MAX_SIDECAR_BYTES: u64 = 16 * 1024 * 1024;
    // GNU tar 1.11 members parked under `@@MaNgLeD.N` until the `N` entry names them.
    let mut mangled: std::collections::HashMap<String, Mangled> = std::collections::HashMap::new();
    let mut mangled_bytes = 0u64;
    let mut ar = tar::Archive::new(archive);

    for entry in ar.entries().context("reading tar entries")? {
        let mut entry = entry.context("reading tar entry")?;
        let raw_path = entry.path().context("entry path")?.into_owned();
        let comps = match safe_components(&raw_path) {
            Some(c) if !c.is_empty() => c,
            // Skip empty paths and anything with `..` / absolute roots.
            _ => continue,
        };
        let display = raw_path.display().to_string();
        let etype = entry_kind(&entry);
        // What the archive says this entry's mode and ownership should be.
        // Empty when `apply_permissions` is off, in which case the shared
        // resolver falls back to the replaced entry / parent directory —
        // the same precedence `rb-cli put` uses.
        let overrides = archived_overrides(entry.header(), opts.apply_permissions);

        if entry.header().entry_type().as_byte() == b'N' {
            let mut list = Vec::new();
            entry
                .read_to_end(&mut list)
                .with_context(|| format!("reading {display}"))?;
            let list = String::from_utf8_lossy(&list).into_owned();
            restore_mangled(efs, &mut sink, &mut mangled, &list, opts)?;
            progress(&sink.stats);
            continue;
        }
        if comps.len() == 1
            && comps[0].starts_with(MANGLED_PREFIX)
            && (etype.is_file() || etype.is_dir())
            && mangled_bytes + entry.size() <= MAX_MANGLED_BYTES
        {
            let mut data = Vec::new();
            entry
                .read_to_end(&mut data)
                .with_context(|| format!("reading {display}"))?;
            mangled_bytes += data.len() as u64;
            let dir = etype.is_dir();
            mangled.insert(
                comps[0].clone(),
                Mangled {
                    dir,
                    data,
                    overrides,
                },
            );
            continue;
        }

        // Classify, then let the shared sink do the writing. Everything past
        // this point — traversal guarding, mkdir -p, conflict policy, attr
        // inheritance — is identical for a host-directory import, so it lives
        // in `import_sink` rather than here.
        if etype.is_file() && comps.last().map(|c| is_appledouble(c)).unwrap_or(false) {
            let mut bytes = Vec::new();
            if entry.size() <= MAX_SIDECAR_BYTES {
                entry
                    .read_to_end(&mut bytes)
                    .with_context(|| format!("reading sidecar {display}"))?;
            }
            match crate::fs::resource_fork::parse_appledouble(&bytes) {
                Some(fork) => {
                    let mut primary = comps.clone();
                    if let Some(last) = primary.last_mut() {
                        *last = last[2..].to_string();
                    }
                    pending_forks.insert(primary, fork);
                }
                None => sink.stats.appledouble_skipped += 1,
            }
            progress(&sink.stats);
            continue;
        }
        if etype.is_dir() {
            sink.push(efs, &comps, ImportItem::Dir, &overrides, opts, &display)?;
        } else if etype.is_symlink() {
            let target = entry
                .link_name()
                .ok()
                .flatten()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default();
            sink.push(
                efs,
                &comps,
                ImportItem::Symlink { target },
                &overrides,
                opts,
                &display,
            )?;
        } else if etype.is_file() {
            let size = entry.size();
            let fork = pending_forks.remove(&comps);
            if fork.is_some() {
                sink.stats.appledouble_paired += 1;
            }
            sink.push(
                efs,
                &comps,
                ImportItem::File {
                    size,
                    data: &mut entry,
                    mac_fork: fork.as_ref(),
                },
                &overrides,
                opts,
                &display,
            )?;
        } else if etype.is_hard_link() {
            // No import target can share an inode with an earlier member, so the link lands as a copy.
            let target = match entry.link_name().ok().flatten() {
                Some(t) => match safe_components(&t) {
                    Some(c) if !c.is_empty() => sink.lookup(efs, &c)?,
                    _ => None,
                },
                None => None,
            };
            match target.filter(|t| !t.is_directory()) {
                Some(t) => {
                    let bytes = efs
                        .read_file(&t, usize::MAX)
                        .map_err(|e| anyhow::anyhow!("reading link target {}: {e}", t.path))?;
                    let size = bytes.len() as u64;
                    sink.push(
                        efs,
                        &comps,
                        ImportItem::File {
                            size,
                            data: &mut std::io::Cursor::new(bytes),
                            mac_fork: None,
                        },
                        &overrides,
                        opts,
                        &display,
                    )?;
                    sink.stats.hardlinks_copied += 1;
                }
                None => sink.push(
                    efs,
                    &comps,
                    ImportItem::Unsupported,
                    &overrides,
                    opts,
                    &display,
                )?,
            }
        } else {
            // Char/block devices, fifos, sockets, and hard links whose target never landed.
            sink.push(
                efs,
                &comps,
                ImportItem::Unsupported,
                &overrides,
                opts,
                &display,
            )?;
        }
        progress(&sink.stats);
    }
    // A sidecar whose file never came, or came first, has nothing to join.
    sink.stats.appledouble_skipped += pending_forks.len() as u64;
    // Members the rename list never named keep the placeholder GNU tar gave them.
    let mut leftover: Vec<_> = mangled.into_iter().collect();
    leftover.sort_by(|a, b| a.0.cmp(&b.0));
    for (name, m) in leftover {
        push_mangled(efs, &mut sink, std::slice::from_ref(&name), m, opts, &name)?;
    }
    Ok(sink.stats)
}

/// Prefix GNU tar 1.11 stores over-long member names under.
const MANGLED_PREFIX: &str = "@@MaNgLeD.";
/// Most member data held in memory while waiting for the rename list; past it, members land as-is.
const MAX_MANGLED_BYTES: u64 = 256 * 1024 * 1024;

/// One `@@MaNgLeD.N` member waiting for its real name.
struct Mangled {
    dir: bool,
    data: Vec<u8>,
    overrides: crate::fs::attrs::AttrOverrides,
}

fn push_mangled(
    efs: &mut dyn EditableFilesystem,
    sink: &mut Importer,
    comps: &[String],
    m: Mangled,
    opts: &TarImportOptions,
    display: &str,
) -> Result<()> {
    if m.dir {
        return sink.push(efs, comps, ImportItem::Dir, &m.overrides, opts, display);
    }
    let size = m.data.len() as u64;
    sink.push(
        efs,
        comps,
        ImportItem::File {
            size,
            data: &mut std::io::Cursor::new(m.data),
            mac_fork: None,
        },
        &m.overrides,
        opts,
        display,
    )
}

/// Apply a GNU tar 1.11 `N` entry: `Rename @@MaNgLeD.N to <path>` moves a held member to its
/// real name, `Symlink <target> to <path>` creates a link whose name was too long.
fn restore_mangled(
    efs: &mut dyn EditableFilesystem,
    sink: &mut Importer,
    held: &mut std::collections::HashMap<String, Mangled>,
    list: &str,
    opts: &TarImportOptions,
) -> Result<()> {
    for line in list.lines() {
        let Some((kind, rest)) = line.split_once(' ') else {
            continue;
        };
        let Some((from, to)) = rest.split_once(" to ") else {
            continue;
        };
        let comps = match safe_components(Path::new(to)) {
            Some(c) if !c.is_empty() => c,
            _ => continue,
        };
        match kind {
            "Rename" => {
                if let Some(m) = held.remove(from) {
                    push_mangled(efs, sink, &comps, m, opts, to)?;
                    sink.stats.mangled_renamed += 1;
                }
            }
            "Symlink" => {
                let target = from.to_string();
                sink.push(
                    efs,
                    &comps,
                    ImportItem::Symlink { target },
                    &crate::fs::attrs::AttrOverrides::default(),
                    opts,
                    to,
                )?;
                sink.stats.mangled_renamed += 1;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Preflight an archive on the host against `efs`, auto-detecting compression.
pub fn preflight_tar_from_path(
    efs: &dyn EditableFilesystem,
    path: &Path,
    opts: &TarImportOptions,
) -> Result<TarImportPreflight> {
    let mut file =
        File::open(path).with_context(|| format!("opening archive {}", path.display()))?;
    let mut magic = [0u8; 4];
    let n = file.read(&mut magic).unwrap_or(0);
    file.seek(SeekFrom::Start(0)).context("rewind archive")?;

    if n >= 2 && magic[0] == 0x1f && magic[1] == 0x8b {
        preflight_tar(efs, flate2::read::GzDecoder::new(file), opts)
    } else if n >= 4 && magic == [0x28, 0xb5, 0x2f, 0xfd] {
        preflight_tar(
            efs,
            crate::rbformats::zstd_compat::decoder(file).context("init zstd decoder")?,
            opts,
        )
    } else {
        preflight_tar(efs, file, opts)
    }
}

/// Read-only preflight scan of a tar stream. Mirrors [`import_tar`]'s
/// classification but performs no mutations.
pub fn preflight_tar<R: Read>(
    efs: &dyn EditableFilesystem,
    archive: R,
    opts: &TarImportOptions,
) -> Result<TarImportPreflight> {
    let supports_symlinks = efs.supports_symlinks();
    let mut pf = TarImportPreflight::default();
    let mut ar = tar::Archive::new(archive);
    for entry in ar.entries().context("reading tar entries")? {
        let entry = entry.context("reading tar entry")?;
        let raw = entry.path().context("entry path")?.into_owned();
        let comps = match safe_components(&raw) {
            Some(c) if !c.is_empty() => c,
            _ => continue,
        };
        if opts.skip_appledouble && comps.last().map(|c| is_appledouble(c)).unwrap_or(false) {
            pf.appledouble += 1;
            continue;
        }
        let name_invalid = comps.iter().any(|c| efs.validate_name(c).is_err());
        let etype = entry_kind(&entry);
        if etype.is_dir() {
            pf.dirs += 1;
            if name_invalid {
                pf.invalid_names += 1;
            }
        } else if name_invalid {
            pf.invalid_names += 1;
        } else if etype.is_symlink() {
            pf.symlinks += 1;
            if !supports_symlinks {
                pf.symlinks_dropped += 1;
            }
        } else if etype.is_file() || etype.is_hard_link() {
            // A hard link lands as a copy of its target.
            pf.files += 1;
        } else {
            pf.other_unsupported += 1;
        }
    }
    Ok(pf)
}

/// The mode / ownership an archived entry asks for, or nothing when the
/// caller turned that off (`--no-permissions`), in which case the shared
/// resolver falls back to the replaced entry then the parent directory.
///
/// `tar_export` writes all three fields on every header it emits, so
/// reading only `mode` here — and only for files — is what made a
/// `tar` -> `untar` round-trip lose ownership outright and directory
/// modes with it.
fn archived_overrides(header: &tar::Header, apply: bool) -> crate::fs::attrs::AttrOverrides {
    // Header mtime is always harvested (independent of `apply_permissions`)
    // so a tar extract carries the source date end-to-end. atime/ctime are
    // not in the tar format, so the driver stamps them the same as mtime
    // via UnixTimes' `_or_now` accessors. `0` mtime is dropped — that's
    // the "no date" case tar emits when the source didn't record one.
    let unix_times = header
        .mtime()
        .ok()
        .filter(|&m| m > 0)
        .map(super::times::UnixTimes::mtime_only);
    if !apply {
        return crate::fs::attrs::AttrOverrides {
            unix_times,
            ..Default::default()
        };
    }
    crate::fs::attrs::AttrOverrides {
        mode: header.mode().ok().map(|m| m & 0o7777),
        uid: header.uid().ok().map(|v| v as u32),
        gid: header.gid().ok().map(|v| v as u32),
        unix_times,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::tar_export::{export_tar, TarCompression, TarExportOptions};

    fn put_file(efs: &mut dyn EditableFilesystem, parent: &FileEntry, name: &str, data: &[u8]) {
        let mut r: &[u8] = data;
        efs.create_file(parent, name, &mut r, data.len() as u64, &Default::default())
            .unwrap();
    }

    /// Round-trip: build a populated FAT volume, export it to a .tar.gz, then
    /// import that archive into a fresh blank FAT volume and confirm the tree
    /// + contents survive.
    fn sidecar_tar(sidecar_first: bool) -> Vec<u8> {
        let ad = crate::fs::resource_fork::build_appledouble(
            b"TEXT",
            b"ttxt",
            crate::fs::resource_fork::MacFileDates::default(),
            b"RSRC",
        );
        let mut b = tar::Builder::new(Vec::new());
        let mut add = |name: &str, bytes: &[u8]| {
            let mut h = tar::Header::new_gnu();
            h.set_entry_type(tar::EntryType::Regular);
            h.set_size(bytes.len() as u64);
            h.set_mode(0o644);
            h.set_cksum();
            b.append_data(&mut h, name, bytes).unwrap();
        };
        if sidecar_first {
            add("._HELLO.TXT", &ad);
            add("HELLO.TXT", b"hello");
        } else {
            add("HELLO.TXT", b"hello");
            add("._HELLO.TXT", &ad);
        }
        b.into_inner().unwrap()
    }

    fn blank_nextstep_ufs() -> Box<dyn EditableFilesystem> {
        let img =
            crate::fs::ufs_format::create_blank_ufs1(&crate::fs::ufs_format::Ufs1FormatParams {
                size_bytes: 8 * 1024 * 1024,
                cg_layout: crate::fs::ufs::CgLayout::Bsd43,
                endian: crate::fs::ufs::UfsEndian::Big,
                ..Default::default()
            })
            .unwrap();
        crate::fs::open_editable_filesystem(std::io::Cursor::new(img), 0, 0, None).unwrap()
    }

    fn read_at(efs: &mut dyn EditableFilesystem, path: &[&str]) -> Vec<u8> {
        let mut e = efs.root().unwrap();
        for name in path {
            e = efs
                .list_directory(&e)
                .unwrap()
                .into_iter()
                .find(|c| c.name == *name)
                .unwrap_or_else(|| panic!("{name} missing under {}", e.path));
        }
        efs.read_file(&e, usize::MAX).unwrap()
    }

    fn append(b: &mut tar::Builder<Vec<u8>>, name: &str, data: &[u8]) {
        let mut h = tar::Header::new_gnu();
        h.set_size(data.len() as u64);
        h.set_mode(0o755);
        h.set_cksum();
        b.append_data(&mut h, name, data).unwrap();
    }

    /// A hard link lands as a copy of its target: Opener.app's `gunzip` is `zcat`.
    #[test]
    fn a_hard_link_lands_as_a_copy_of_its_target() {
        let mut b = tar::Builder::new(Vec::new());
        append(&mut b, "Opener.app/zcat", b"zcat binary");
        let mut h = tar::Header::new_gnu();
        h.set_entry_type(tar::EntryType::Link);
        h.set_size(0);
        h.set_mode(0o755);
        b.append_link(&mut h, "Opener.app/gunzip", "Opener.app/zcat")
            .unwrap();
        let mut h = tar::Header::new_gnu();
        h.set_entry_type(tar::EntryType::Link);
        h.set_size(0);
        b.append_link(&mut h, "Opener.app/dangling", "Opener.app/missing")
            .unwrap();
        let bytes = b.into_inner().unwrap();

        let mut efs = blank_nextstep_ufs();
        let root = efs.root().unwrap();
        let stats = import_tar(
            &mut *efs,
            &root,
            &bytes[..],
            &TarImportOptions::default(),
            &|_| {},
        )
        .unwrap();
        assert_eq!((stats.hardlinks_copied, stats.other_skipped), (1, 1));
        assert_eq!(
            read_at(&mut *efs, &["Opener.app", "gunzip"]),
            b"zcat binary"
        );
    }

    /// GNU tar 1.11 parks long names under `@@MaNgLeD.N` and names them in a trailing `N` entry.
    #[test]
    fn gnu_mangled_names_are_restored() {
        let long = "Calendar/UsingCalendarPalette/UsingCalendarPalette.app/English.lproj/\
                    UsingCalendarPalette.nib/data.classes";
        let mut b = tar::Builder::new(Vec::new());
        append(&mut b, "Calendar/README", b"readme");
        append(&mut b, "@@MaNgLeD.0", b"FirstResponder = {};");
        append(&mut b, "@@MaNgLeD.1", b"never named");
        let list = format!("Rename @@MaNgLeD.0 to {long}\nSymlink data.classes to {long}.link\n");
        let mut h = tar::Header::new_gnu();
        h.set_entry_type(tar::EntryType::new(b'N'));
        h.set_size(list.len() as u64);
        h.set_cksum();
        b.append_data(&mut h, "././@MaNgLeD_NaMeS", list.as_bytes())
            .unwrap();
        let bytes = b.into_inner().unwrap();

        let mut efs = blank_nextstep_ufs();
        let root = efs.root().unwrap();
        let stats = import_tar(
            &mut *efs,
            &root,
            &bytes[..],
            &TarImportOptions::default(),
            &|_| {},
        )
        .unwrap();
        assert_eq!(stats.mangled_renamed, 2);
        let parts: Vec<&str> = long.split('/').collect();
        assert_eq!(read_at(&mut *efs, &parts), b"FirstResponder = {};");
        // A member the list never named keeps its placeholder rather than vanishing.
        assert_eq!(read_at(&mut *efs, &["@@MaNgLeD.1"]), b"never named");
        let names: Vec<String> = efs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(!names.iter().any(|n| n == "@@MaNgLeD.0"), "{names:?}");
    }

    /// X14: a `._name` member was dropped; macOS tar writes it right before
    /// `name`, and it carries the file's resource fork and type codes.
    #[test]
    fn an_appledouble_member_is_joined_to_its_file() {
        let img = crate::fs::hfs::create_blank_hfs(4 * 1024 * 1024, 512, "T").unwrap();
        let mut efs =
            crate::fs::open_editable_filesystem(std::io::Cursor::new(img), 0, 0, None).unwrap();
        let root = efs.root().unwrap();
        let stats = import_tar(
            &mut *efs,
            &root,
            &sidecar_tar(true)[..],
            &TarImportOptions::default(),
            &|_| {},
        )
        .unwrap();
        efs.sync_metadata().unwrap();
        assert_eq!(
            (
                stats.files,
                stats.appledouble_paired,
                stats.appledouble_skipped
            ),
            (1, 1, 0)
        );
        let entries = efs.as_filesystem_mut().list_directory(&root).unwrap();
        let hello = entries
            .iter()
            .find(|e| e.name == "HELLO.TXT")
            .expect("HELLO.TXT");
        assert_eq!(hello.resource_fork_size, Some(4), "{hello:?}");
        assert_eq!(hello.type_code, Some(*b"TEXT"));
        assert_eq!(hello.creator_code, Some(*b"ttxt"));

        // Sidecar after its file: nothing to join, and it is reported as skipped.
        let img = crate::fs::hfs::create_blank_hfs(4 * 1024 * 1024, 512, "T").unwrap();
        let mut efs =
            crate::fs::open_editable_filesystem(std::io::Cursor::new(img), 0, 0, None).unwrap();
        let root = efs.root().unwrap();
        let stats = import_tar(
            &mut *efs,
            &root,
            &sidecar_tar(false)[..],
            &TarImportOptions::default(),
            &|_| {},
        )
        .unwrap();
        assert_eq!(
            (
                stats.files,
                stats.appledouble_paired,
                stats.appledouble_skipped
            ),
            (1, 0, 1)
        );
    }

    #[test]
    fn round_trip_export_then_import() {
        let dir = tempfile::tempdir().unwrap();

        // Source volume.
        let src_img = dir.path().join("src.img");
        std::fs::write(
            &src_img,
            crate::fs::fat::create_blank_fat(4 * 1024 * 1024, Some("SRC")).unwrap(),
        )
        .unwrap();
        {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&src_img)
                .unwrap();
            let mut efs = crate::fs::open_editable_filesystem(f, 0, 0, None).unwrap();
            let root = efs.root().unwrap();
            put_file(&mut *efs, &root, "HELLO.TXT", b"hello world");
            let payload: Vec<u8> = (0u8..255).collect();
            put_file(&mut *efs, &root, "DATA.BIN", &payload);
            let sub = efs
                .create_directory(&root, "SUB", &Default::default())
                .unwrap();
            put_file(&mut *efs, &sub, "INNER.TXT", b"nested file");
            efs.sync_metadata().unwrap();
        }

        // Export to a .tar.gz.
        let tgz = dir.path().join("out.tar.gz");
        {
            let f = std::fs::File::open(&src_img).unwrap();
            let mut fs = crate::fs::open_filesystem(f, 0, 0, None).unwrap();
            let root = fs.root().unwrap();
            let out = std::fs::File::create(&tgz).unwrap();
            export_tar(
                &mut *fs,
                &root,
                "",
                out,
                TarCompression::Gzip,
                &TarExportOptions::default(),
                &|_| {},
            )
            .unwrap();
        }

        // Fresh blank target volume; import the archive into it.
        let dst_img = dir.path().join("dst.img");
        std::fs::write(
            &dst_img,
            crate::fs::fat::create_blank_fat(4 * 1024 * 1024, Some("DST")).unwrap(),
        )
        .unwrap();
        let stats = {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&dst_img)
                .unwrap();
            let mut efs = crate::fs::open_editable_filesystem(f, 0, 0, None).unwrap();
            let root = efs.root().unwrap();
            let stats = import_tar_from_path(
                &mut *efs,
                &root,
                &tgz,
                &TarImportOptions::default(),
                &|_| {},
            )
            .unwrap();
            efs.sync_metadata().unwrap();
            stats
        };
        assert_eq!(stats.files, 3, "stats: {stats:?}");
        assert_eq!(stats.dirs_created, 1);

        // Verify the target volume's contents.
        let f = std::fs::File::open(&dst_img).unwrap();
        let mut fs = crate::fs::open_filesystem(f, 0, 0, None).unwrap();
        let root = fs.root().unwrap();
        let names: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(names.iter().any(|n| n == "HELLO.TXT"), "names: {names:?}");
        assert!(names.iter().any(|n| n == "SUB"), "names: {names:?}");

        let hello = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "HELLO.TXT")
            .unwrap();
        let mut buf = Vec::new();
        fs.write_file_to(&hello, &mut buf).unwrap();
        assert_eq!(buf, b"hello world");

        let sub = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "SUB")
            .unwrap();
        let inner = fs
            .list_directory(&sub)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "INNER.TXT")
            .unwrap();
        let mut buf2 = Vec::new();
        fs.write_file_to(&inner, &mut buf2).unwrap();
        assert_eq!(buf2, b"nested file");
    }

    /// NeXT's patch tarballs predate POSIX: no `ustar` magic, only a checksum to go on.
    #[test]
    fn looks_like_tar_accepts_a_v7_archive_and_rejects_noise() {
        let dir = tempfile::tempdir().unwrap();
        let mut b = tar::Builder::new(Vec::new());
        let mut h = tar::Header::new_old();
        h.set_size(5);
        h.set_mode(0o644);
        h.set_cksum();
        let mut d = tar::Header::new_old();
        d.set_size(0);
        d.set_mode(0o755);
        d.set_path("Patch.pkg/").unwrap();
        d.set_cksum();
        b.append(&d, std::io::empty()).unwrap();
        b.append_data(&mut h, "Patch.pkg/Patch.info", &b"hello"[..])
            .unwrap();
        let bytes = b.into_inner().unwrap();
        assert_ne!(&bytes[257..262], b"ustar", "the fixture must be magic-less");
        let v7 = dir.path().join("Patch.tar");
        std::fs::write(&v7, &bytes).unwrap();
        assert!(looks_like_tar_archive(&v7));
        // The typeflag-less `Patch.pkg/` is a directory, not a file its children collide with.
        assert_eq!(measure_tar_expanded(&v7).unwrap(), (1, 1, 5));

        let mut noise = bytes.clone();
        noise[0x10] ^= 0x55;
        let bad = dir.path().join("noise.bin");
        std::fs::write(&bad, &noise).unwrap();
        assert!(
            !looks_like_tar_archive(&bad),
            "a broken checksum is not tar"
        );
        let zeros = dir.path().join("zeros.bin");
        std::fs::write(&zeros, vec![0u8; 1024]).unwrap();
        assert!(!looks_like_tar_archive(&zeros));
    }

    #[test]
    fn looks_like_tar_detects_tar_and_rejects_gzip_image() {
        use std::io::Write as _;
        let dir = tempfile::tempdir().unwrap();

        // A real .tar.gz built by exporting a tiny FAT volume.
        let img = dir.path().join("s.img");
        std::fs::write(
            &img,
            crate::fs::fat::create_blank_fat(2 * 1024 * 1024, Some("S")).unwrap(),
        )
        .unwrap();
        {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&img)
                .unwrap();
            let mut efs = crate::fs::open_editable_filesystem(f, 0, 0, None).unwrap();
            let root = efs.root().unwrap();
            put_file(&mut *efs, &root, "A.TXT", b"hi");
            efs.sync_metadata().unwrap();
        }
        let tgz = dir.path().join("a.tar.gz");
        {
            let f = std::fs::File::open(&img).unwrap();
            let mut fs = crate::fs::open_filesystem(f, 0, 0, None).unwrap();
            let root = fs.root().unwrap();
            let out = std::fs::File::create(&tgz).unwrap();
            export_tar(
                &mut *fs,
                &root,
                "",
                out,
                TarCompression::Gzip,
                &TarExportOptions::default(),
                &|_| {},
            )
            .unwrap();
        }
        assert!(
            looks_like_tar_archive(&tgz),
            "real .tar.gz should be detected"
        );

        // A plain (uncompressed) tar.
        let plain = dir.path().join("a.tar");
        {
            let f = std::fs::File::open(&img).unwrap();
            let mut fs = crate::fs::open_filesystem(f, 0, 0, None).unwrap();
            let root = fs.root().unwrap();
            let out = std::fs::File::create(&plain).unwrap();
            export_tar(
                &mut *fs,
                &root,
                "",
                out,
                TarCompression::None,
                &TarExportOptions::default(),
                &|_| {},
            )
            .unwrap();
        }
        assert!(
            looks_like_tar_archive(&plain),
            "plain .tar should be detected"
        );

        // A gzip stream that is NOT a tar (stand-in for a .adz disk image) —
        // must NOT be mistaken for a tarball.
        let adz = dir.path().join("disk.adz");
        {
            let f = std::fs::File::create(&adz).unwrap();
            let mut enc = flate2::write::GzEncoder::new(f, flate2::Compression::default());
            enc.write_all(&vec![0xE9u8; 4096]).unwrap(); // boot-sector-ish bytes
            enc.finish().unwrap();
        }
        assert!(
            !looks_like_tar_archive(&adz),
            "gzip disk image must not look like tar"
        );
    }

    #[test]
    fn skip_existing_does_not_error() {
        let dir = tempfile::tempdir().unwrap();
        // Build a one-file tar.gz in memory by exporting a tiny volume.
        let src = dir.path().join("s.img");
        std::fs::write(
            &src,
            crate::fs::fat::create_blank_fat(2 * 1024 * 1024, Some("S")).unwrap(),
        )
        .unwrap();
        {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&src)
                .unwrap();
            let mut efs = crate::fs::open_editable_filesystem(f, 0, 0, None).unwrap();
            let root = efs.root().unwrap();
            put_file(&mut *efs, &root, "A.TXT", b"one");
            efs.sync_metadata().unwrap();
        }
        let tgz = dir.path().join("a.tar.gz");
        {
            let f = std::fs::File::open(&src).unwrap();
            let mut fs = crate::fs::open_filesystem(f, 0, 0, None).unwrap();
            let root = fs.root().unwrap();
            let out = std::fs::File::create(&tgz).unwrap();
            export_tar(
                &mut *fs,
                &root,
                "",
                out,
                TarCompression::Gzip,
                &TarExportOptions::default(),
                &|_| {},
            )
            .unwrap();
        }
        // Target already containing A.TXT -> skip mode imports nothing new.
        let dst = dir.path().join("d.img");
        std::fs::write(
            &dst,
            crate::fs::fat::create_blank_fat(2 * 1024 * 1024, Some("D")).unwrap(),
        )
        .unwrap();
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&dst)
            .unwrap();
        let mut efs = crate::fs::open_editable_filesystem(f, 0, 0, None).unwrap();
        let root = efs.root().unwrap();
        put_file(&mut *efs, &root, "A.TXT", b"preexisting");
        let opts = TarImportOptions {
            conflict: ImportConflict::Skip,
            ..Default::default()
        };
        let stats = import_tar_from_path(&mut *efs, &root, &tgz, &opts, &|_| {}).unwrap();
        assert_eq!(stats.skipped_existing, 1, "stats: {stats:?}");
        assert_eq!(stats.files, 0);
    }

    /// Build a tar in memory carrying explicit modes and ownership on a
    /// file, an executable, and a directory.
    fn tar_with_permissions() -> Vec<u8> {
        let mut b = tar::Builder::new(Vec::new());

        let mut dh = tar::Header::new_gnu();
        dh.set_entry_type(tar::EntryType::Directory);
        dh.set_size(0);
        dh.set_mode(0o700);
        dh.set_uid(1000);
        dh.set_gid(100);
        dh.set_cksum();
        b.append_data(&mut dh, "private/", std::io::empty())
            .unwrap();

        let mut fh = tar::Header::new_gnu();
        fh.set_entry_type(tar::EntryType::Regular);
        fh.set_size(6);
        fh.set_mode(0o600);
        fh.set_uid(1000);
        fh.set_gid(100);
        fh.set_cksum();
        b.append_data(&mut fh, "private/key.txt", &b"secret"[..])
            .unwrap();

        let mut xh = tar::Header::new_gnu();
        xh.set_entry_type(tar::EntryType::Regular);
        xh.set_size(3);
        xh.set_mode(0o755);
        xh.set_uid(1000);
        xh.set_gid(100);
        xh.set_cksum();
        b.append_data(&mut xh, "run.sh", &b"#!\n"[..]).unwrap();

        b.into_inner().unwrap()
    }

    /// An import has to carry the archive's mode AND ownership onto files
    /// AND directories.
    ///
    /// It used to read only `mode`, apply it only to files, and only via
    /// `set_permissions` — a hook just two drivers implement — so on every
    /// other Unix filesystem an executable came back 0644, a 0600 secret
    /// came back world-readable, directory modes were dropped everywhere,
    /// and uid/gid were never read from the header at all. `tar_export`
    /// writes all three fields, so a `tar` -> `untar` round-trip lost them.
    #[test]
    fn import_applies_archived_mode_and_ownership() {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("ext.img");
        std::fs::write(
            &img,
            crate::fs::ext_format::create_blank_ext2(16 * 1024 * 1024, "PERMS").unwrap(),
        )
        .unwrap();
        let archive = dir.path().join("perms.tar");
        std::fs::write(&archive, tar_with_permissions()).unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&img)
            .unwrap();
        let mut efs = crate::fs::open_editable_filesystem(f, 0, 0, None).unwrap();
        let root = efs.root().unwrap();
        import_tar_from_path(
            &mut *efs,
            &root,
            &archive,
            &TarImportOptions::default(),
            &|_| {},
        )
        .unwrap();
        efs.sync_metadata().unwrap();

        let find =
            |efs: &mut dyn EditableFilesystem, parent: &FileEntry, name: &str| -> FileEntry {
                efs.list_directory(parent)
                    .unwrap()
                    .into_iter()
                    .find(|e| e.name == name)
                    .unwrap_or_else(|| panic!("{name} missing after import"))
            };

        let root = efs.root().unwrap();
        let private = find(&mut *efs, &root, "private");
        assert_eq!(
            private.mode.map(|m| m & 0o7777),
            Some(0o700),
            "directory mode came from the driver default, not the archive"
        );
        assert_eq!((private.uid, private.gid), (Some(1000), Some(100)));

        let run = find(&mut *efs, &root, "run.sh");
        assert_eq!(
            run.mode.map(|m| m & 0o7777),
            Some(0o755),
            "executable bit did not survive the import"
        );
        assert_eq!((run.uid, run.gid), (Some(1000), Some(100)));

        let key = find(&mut *efs, &private, "key.txt");
        assert_eq!(
            key.mode.map(|m| m & 0o7777),
            Some(0o600),
            "a 0600 file must not come back more permissive than the archive"
        );
        assert_eq!((key.uid, key.gid), (Some(1000), Some(100)));
    }

    /// `--no-permissions` means "ignore what the archive says", not "fall
    /// back to root:root 0644": new entries inherit ownership from the
    /// directory they land in, the same rule `rb-cli put` follows.
    #[test]
    fn no_permissions_inherits_from_the_parent_directory() {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("ext.img");
        std::fs::write(
            &img,
            crate::fs::ext_format::create_blank_ext2(16 * 1024 * 1024, "PERMS").unwrap(),
        )
        .unwrap();
        let archive = dir.path().join("perms.tar");
        std::fs::write(&archive, tar_with_permissions()).unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&img)
            .unwrap();
        let mut efs = crate::fs::open_editable_filesystem(f, 0, 0, None).unwrap();
        let root = efs.root().unwrap();
        // Import into a directory with a distinctive owner to inherit.
        // (Built through `create_directory` rather than `set_owner`, which
        // only SquashFS implements today.)
        efs.create_directory(
            &root,
            "dest",
            &crate::fs::filesystem::CreateDirectoryOptions {
                mode: Some(0o40755),
                uid: Some(42),
                gid: Some(43),
                ..Default::default()
            },
        )
        .unwrap();
        // Re-read it the way a caller would, so it carries its owner.
        let dest = efs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "dest")
            .unwrap();
        assert_eq!((dest.uid, dest.gid), (Some(42), Some(43)));

        let opts = TarImportOptions {
            apply_permissions: false,
            ..Default::default()
        };
        let stats = import_tar_from_path(&mut *efs, &dest, &archive, &opts, &|_| {}).unwrap();
        assert_eq!(stats.perms_applied, 0, "nothing archived should be applied");
        efs.sync_metadata().unwrap();

        let run = efs
            .list_directory(&dest)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "run.sh")
            .expect("run.sh missing");
        assert_ne!(
            run.mode.map(|m| m & 0o7777),
            Some(0o755),
            "the archive's mode should have been ignored"
        );
        assert_eq!(
            (run.uid, run.gid),
            (Some(42), Some(43)),
            "ownership should come from the parent directory"
        );
    }
}
