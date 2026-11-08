//! Check a converted image against its source: the same sectors when the size was kept, the
//! same files (names and sizes) when it was resized, refitted or is a disc.

use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use super::run::Hooks;
use super::{Classified, Format, ImageKind};
use crate::partition::PartitionTable;

/// Compare `dest` with `src`; the line says what matched, the error what did not.
pub fn verify(
    src: &Classified,
    format: Format,
    dest: &Path,
    reshaped: bool,
    hooks: &Hooks<'_>,
) -> Result<String, String> {
    match src.kind {
        ImageKind::Disc => disc_files(src.path.as_path(), dest),
        ImageKind::Floppy if reshaped => volume_files(src.path.as_path(), dest, hooks),
        ImageKind::HardDisk if reshaped => volume_files(src.path.as_path(), dest, hooks),
        _ => {
            let _ = format;
            same_sectors(src.path.as_path(), dest, hooks)
        }
    }
}

/// Both read back as plain sectors and compared, block by block.
fn same_sectors(src: &Path, dest: &Path, hooks: &Hooks<'_>) -> Result<String, String> {
    let open =
        |p: &Path| super::open_disk(p).map_err(|e| format!("cannot reopen {}: {e:#}", p.display()));
    let ((mut a, len_a), (mut b, len_b)) = (open(src)?, open(dest)?);
    // A container may pad the end (VHD rounds to a CHS size): compare the source's length.
    if len_b < len_a {
        return Err(format!(
            "the output holds {len_b} bytes, the source {len_a}"
        ));
    }
    a.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;
    b.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;
    const BLOCK: usize = 1 << 20;
    let (mut ba, mut bb) = (vec![0u8; BLOCK], vec![0u8; BLOCK]);
    let mut done = 0u64;
    while done < len_a {
        if (hooks.cancelled)() {
            return Err("cancelled".to_string());
        }
        let n = ((len_a - done) as usize).min(BLOCK);
        a.read_exact(&mut ba[..n])
            .map_err(|e| format!("reading the source: {e}"))?;
        b.read_exact(&mut bb[..n])
            .map_err(|e| format!("reading the output: {e}"))?;
        if ba[..n] != bb[..n] {
            let at = ba[..n]
                .iter()
                .zip(&bb[..n])
                .position(|(x, y)| x != y)
                .unwrap_or(0);
            return Err(format!("the data differs at byte {}", done + at as u64));
        }
        done += n as u64;
        (hooks.progress)(done, len_a);
    }
    Ok(format!(
        "Verified: all {} match.",
        super::classify::size_text(len_a)
    ))
}

/// Each volume's files, by path, with their sizes.
type Listing = BTreeMap<String, u64>;

/// Each volume on both sides lists the same files, and the output's filesystem checks clean.
fn volume_files(src: &Path, dest: &Path, hooks: &Hooks<'_>) -> Result<String, String> {
    let before = volumes(src, false)?;
    let after = volumes(dest, true)?;
    let mut files = 0usize;
    for (index, (name, listing)) in &before {
        if (hooks.cancelled)() {
            return Err("cancelled".to_string());
        }
        let Some((_, out)) = after.get(index) else {
            return Err(format!(
                "partition {index} ({name}) is missing from the output"
            ));
        };
        if let Some(problem) = compare(listing, out) {
            return Err(format!("partition {index} ({name}): {problem}"));
        }
        files += listing.len();
    }
    Ok(format!(
        "Verified: {} volume(s), {files} file(s) match and check clean.",
        before.len()
    ))
}

fn compare(a: &Listing, b: &Listing) -> Option<String> {
    for (path, size) in a {
        match b.get(path) {
            None => return Some(format!("{path} is missing")),
            Some(s) if s != size => return Some(format!("{path} is {s} bytes, was {size}")),
            _ => {}
        }
    }
    b.keys()
        .find(|p| !a.contains_key(*p))
        .map(|p| format!("{p} was not on the source"))
}

/// Every volume the engine can open: index to (filesystem name, files).
fn volumes(path: &Path, check: bool) -> Result<BTreeMap<usize, (String, Listing)>, String> {
    let (mut reader, _) =
        super::open_disk(path).map_err(|e| format!("cannot reopen {}: {e:#}", path.display()))?;
    let table = PartitionTable::detect(&mut reader).map_err(|e| e.to_string())?;
    let parts: Vec<(usize, u64, u8, Option<String>)> = match &table {
        PartitionTable::None { .. } => vec![(0, 0, 0, None)],
        t => t
            .partitions()
            .into_iter()
            .filter(|p| !p.is_extended_container)
            .map(|p| {
                (
                    p.index,
                    p.start_lba * 512,
                    p.gate_type_byte(),
                    p.partition_type_string,
                )
            })
            .collect(),
    };
    let mut out = BTreeMap::new();
    for (index, offset, gate, type_string) in parts {
        let (r, _) = super::open_disk(path).map_err(|e| format!("{e:#}"))?;
        let Ok(mut fs) = crate::fs::open_filesystem(r, offset, gate, type_string.as_deref()) else {
            continue;
        };
        let name = fs.fs_type().to_string();
        let mut listing = Listing::new();
        let root = fs.root().map_err(|e| format!("{name}: {e}"))?;
        walk(fs.as_mut(), &root, &mut listing).map_err(|e| format!("{name}: {e}"))?;
        if check {
            if let Some(Ok(r)) = fs.fsck() {
                if !r.is_clean() {
                    let first = r
                        .errors
                        .first()
                        .map(|e| e.message.clone())
                        .unwrap_or_default();
                    return Err(format!(
                        "the output's {name} volume (partition {index}) has {} error(s): {first}",
                        r.errors.len()
                    ));
                }
            }
        }
        out.insert(index, (name, listing));
    }
    Ok(out)
}

fn walk(
    fs: &mut dyn disk_core::filesystem::Filesystem,
    dir: &disk_core::entry::FileEntry,
    out: &mut Listing,
) -> Result<(), disk_core::filesystem::FilesystemError> {
    use disk_core::entry::EntryType;
    for child in fs.list_directory(dir)? {
        match child.entry_type {
            EntryType::Directory => {
                out.insert(format!("{}/", child.path), 0);
                walk(fs, &child, out)?;
            }
            EntryType::File => {
                out.insert(child.path.clone(), child.size);
            }
            _ => {}
        }
    }
    Ok(())
}

/// A disc's files on both sides; an audio disc only has to open.
#[cfg(feature = "optical")]
fn disc_files(src: &Path, dest: &Path) -> Result<String, String> {
    let list = |p: &Path| -> Result<Option<Listing>, String> {
        let info = opticaldiscs::detect::DiscImageInfo::open(p)
            .map_err(|e| format!("cannot reopen {}: {e}", p.display()))?;
        let Ok(mut fs) = opticaldiscs::browse::open_disc_filesystem(&info) else {
            return Ok(None);
        };
        let mut out = Listing::new();
        let root = fs.root().map_err(|e| e.to_string())?;
        disc_walk(fs.as_mut(), &root, &mut out)?;
        Ok(Some(out))
    };
    match (list(src)?, list(dest)?) {
        (Some(a), Some(b)) => match compare(&a, &b) {
            None => Ok(format!("Verified: {} file(s) match.", a.len())),
            Some(problem) => Err(problem),
        },
        (None, _) => {
            Ok("Verified: the output opens (the disc has no files to compare).".to_string())
        }
        (Some(_), None) => Err("the output's files cannot be read".to_string()),
    }
}

#[cfg(feature = "optical")]
fn disc_walk(
    fs: &mut dyn opticaldiscs::browse::filesystem::Filesystem,
    dir: &opticaldiscs::browse::entry::FileEntry,
    out: &mut Listing,
) -> Result<(), String> {
    use opticaldiscs::browse::entry::EntryType;
    for child in fs.list_directory(dir).map_err(|e| e.to_string())? {
        if child.entry_type == EntryType::Directory {
            out.insert(format!("{}/", child.path), 0);
            disc_walk(fs, &child, out)?;
        } else {
            out.insert(child.path.clone(), child.size);
        }
    }
    Ok(())
}

#[cfg(not(feature = "optical"))]
fn disc_files(_src: &Path, dest: &Path) -> Result<String, String> {
    std::fs::metadata(dest).map_err(|e| e.to_string())?;
    Ok("Not verified: this copy was built without optical disc support.".to_string())
}
