//! Import a `.zip` archive's contents INTO a disk image's filesystem, the ZIP counterpart of
//! [`crate::tar_import`].
//!
//! Members stream from the archive into the shared [`Importer`], one at a time, so a large
//! archive never sits in RAM. A zip made by macOS (Finder's Compress, `ditto -c -k`) stores
//! each file's resource fork and Finder info as an AppleDouble sidecar, either under
//! `__MACOSX/<dir>/._<name>` or as `._<name>` beside the file. With the `appledouble`
//! feature those sidecars are read first and joined to their files, so a Mac volume gets
//! one forked file; without it they are counted and dropped. The `__MACOSX` tree itself is
//! never imported.
//!
//! Info-ZIP's MacZip (`Mac3` extra fields) is a different format and is not handled here;
//! rusty-backup routes it to `arc-mac`.
//!
//! Zip dates carry no time zone; they are read as UTC, so the same archive imports the
//! same way on every host. The caller owns the sync / commit lifecycle, as for a tar import.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use anyhow::{Context, Result};
use disk_core::attrs::AttrOverrides;
use disk_core::entry::FileEntry;
use disk_core::filesystem::{EditableFilesystem, ImportedResourceFork};

use crate::import_sink::{
    is_appledouble, safe_components, ImportItem, ImportOptions, ImportStats, Importer,
};
use crate::zip_compat::ZipEntryName as _;

/// The folder macOS keeps zipped AppleDouble sidecars under.
const MACOSX_DIR: &str = "__MACOSX";
/// A sidecar bigger than this is not read; its file lands without a fork.
const MAX_SIDECAR_BYTES: u64 = 16 * 1024 * 1024;
const S_IFMT: u32 = 0o170000;
const S_IFLNK: u32 = 0o120000;

/// True when `path` starts with a ZIP signature (a local header, or an empty archive's end record).
pub fn looks_like_zip(path: &Path) -> bool {
    let mut head = [0u8; 4];
    match File::open(path).and_then(|mut f| f.read_exact(&mut head)) {
        Ok(()) => &head == b"PK\x03\x04" || &head == b"PK\x05\x06",
        Err(_) => false,
    }
}

/// One member, as the import walks it.
enum Kind {
    Dir,
    File,
    Symlink,
}

struct Member {
    index: usize,
    comps: Vec<String>,
    kind: Kind,
    size: u64,
    mode: Option<u32>,
    mtime: Option<u64>,
}

/// The members to import, and the indices of the AppleDouble sidecars keyed by their file's path.
struct Plan {
    members: Vec<Member>,
    sidecars: Vec<(Vec<String>, usize, u64)>,
}

fn open(path: &Path) -> Result<zip::ZipArchive<BufReader<File>>> {
    let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    zip::ZipArchive::new(BufReader::new(f))
        .with_context(|| format!("reading {} as ZIP", path.display()))
}

fn plan<R: Read + std::io::Seek>(zip: &mut zip::ZipArchive<R>) -> Result<Plan> {
    let mut members = Vec::new();
    let mut sidecars = Vec::new();
    for index in 0..zip.len() {
        let zf = zip
            .by_index_raw(index)
            .with_context(|| format!("reading ZIP entry {index}"))?;
        let name = zf.entry_name();
        let comps = match safe_components(Path::new(&name)) {
            Some(c) if !c.is_empty() => c,
            // Empty names, `..` and absolute paths are never written.
            _ => continue,
        };
        let leaf_is_sidecar =
            comps.last().is_some_and(|l| is_appledouble(l.as_str())) && !zf.is_dir();
        if comps[0] == MACOSX_DIR {
            if leaf_is_sidecar {
                sidecars.push((primary_of(&comps[1..]), index, zf.size()));
            }
            continue;
        }
        if leaf_is_sidecar {
            sidecars.push((primary_of(comps.as_slice()), index, zf.size()));
            continue;
        }
        let mode = zf.unix_mode();
        let kind = if zf.is_dir() {
            Kind::Dir
        } else if mode.is_some_and(|m| m & S_IFMT == S_IFLNK) {
            Kind::Symlink
        } else {
            Kind::File
        };
        let mtime = zf.last_modified().and_then(|d| {
            unix_seconds(
                d.year(),
                d.month(),
                d.day(),
                d.hour(),
                d.minute(),
                d.second(),
            )
        });
        members.push(Member {
            index,
            comps,
            kind,
            size: zf.size(),
            mode,
            mtime,
        });
    }
    Ok(Plan { members, sidecars })
}

/// `dir/._name` -> `dir/name`.
fn primary_of(sidecar: &[String]) -> Vec<String> {
    let mut p = sidecar.to_vec();
    if let Some(last) = p.last_mut() {
        *last = last[2..].to_string();
    }
    p
}

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's `days_from_civil`).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// A zip timestamp as Unix seconds, read as UTC; `None` for the DOS "no date" value.
fn unix_seconds(year: u16, month: u8, day: u8, hour: u8, minute: u8, second: u8) -> Option<u64> {
    if month == 0 || day == 0 {
        return None;
    }
    let days = days_from_civil(i64::from(year), i64::from(month), i64::from(day));
    let secs = days * 86400 + i64::from(hour) * 3600 + i64::from(minute) * 60 + i64::from(second);
    u64::try_from(secs).ok().filter(|s| *s > 0)
}

fn overrides(m: &Member, apply: bool) -> AttrOverrides {
    AttrOverrides {
        mode: m.mode.filter(|_| apply).map(|v| v & 0o7777),
        unix_times: m.mtime.map(disk_core::times::UnixTimes::mtime_only),
        ..Default::default()
    }
}

/// Read every sidecar and parse it; the ones that do not parse are counted as skipped.
fn read_forks<R: Read + std::io::Seek>(
    zip: &mut zip::ZipArchive<R>,
    sidecars: &[(Vec<String>, usize, u64)],
    stats: &mut ImportStats,
) -> Result<HashMap<Vec<String>, ImportedResourceFork>> {
    let mut forks = HashMap::new();
    for (primary, index, size) in sidecars {
        let parsed = if *size <= MAX_SIDECAR_BYTES {
            let mut bytes = Vec::new();
            zip.by_index(*index)
                .with_context(|| format!("reading ZIP entry {index}"))?
                .read_to_end(&mut bytes)
                .with_context(|| format!("reading ZIP entry {index}"))?;
            parse_sidecar(&bytes)
        } else {
            None
        };
        match parsed {
            Some(fork) => {
                forks.insert(primary.clone(), fork);
            }
            None => stats.appledouble_skipped += 1,
        }
    }
    Ok(forks)
}

#[cfg(feature = "appledouble")]
fn parse_sidecar(bytes: &[u8]) -> Option<ImportedResourceFork> {
    fs_mac::resource_fork::parse_appledouble(bytes)
}

#[cfg(not(feature = "appledouble"))]
fn parse_sidecar(_bytes: &[u8]) -> Option<ImportedResourceFork> {
    None
}

/// Unpack the ZIP at `path` under `dest`. Like [`crate::tar_import::import_tar_from_path_into`],
/// bulk mode stays with the caller.
pub fn import_zip_from_path_into(
    efs: &mut dyn EditableFilesystem,
    dest: &FileEntry,
    path: &Path,
    opts: &ImportOptions,
    progress: &dyn Fn(&ImportStats),
) -> Result<ImportStats> {
    let mut zip = open(path)?;
    let plan = plan(&mut zip)?;
    let mut sink = Importer::new(dest);
    sink.implicit_dir_times = crate::tar_import::archive_times(path);
    let mut forks = read_forks(&mut zip, &plan.sidecars, &mut sink.stats)?;
    let raw = path.display().to_string();
    for m in &plan.members {
        let display = format!("{raw}:{}", m.comps.join("/"));
        let over = overrides(m, opts.apply_permissions);
        match m.kind {
            Kind::Dir => sink.push(
                efs,
                m.comps.as_slice(),
                ImportItem::Dir,
                &over,
                opts,
                display.as_str(),
            )?,
            Kind::Symlink => {
                let mut target = String::new();
                zip.by_index(m.index)
                    .with_context(|| format!("reading {display}"))?
                    .take(4096)
                    .read_to_string(&mut target)
                    .with_context(|| format!("reading {display}"))?;
                sink.push(
                    efs,
                    m.comps.as_slice(),
                    ImportItem::Symlink { target },
                    &over,
                    opts,
                    display.as_str(),
                )?;
            }
            Kind::File => {
                let fork = forks.remove(&m.comps);
                if fork.is_some() {
                    sink.stats.appledouble_paired += 1;
                }
                let mut data = zip
                    .by_index(m.index)
                    .with_context(|| format!("reading {display}"))?;
                sink.push(
                    efs,
                    m.comps.as_slice(),
                    ImportItem::File {
                        size: m.size,
                        data: &mut data,
                        mac_fork: fork.as_ref(),
                    },
                    &over,
                    opts,
                    display.as_str(),
                )?;
            }
        }
        progress(&sink.stats);
    }
    // A sidecar whose file is not in the archive has nothing to join.
    sink.stats.appledouble_skipped += forks.len() as u64;
    Ok(sink.stats)
}

/// `(files, dirs, content bytes)` the archive expands to, from its central directory alone.
pub fn measure_zip_expanded(path: &Path) -> Result<(u64, u64, u64)> {
    let mut zip = open(path)?;
    let plan = plan(&mut zip)?;
    let mut totals = (0u64, 0u64, 0u64);
    for m in &plan.members {
        match m.kind {
            Kind::Dir => totals.1 += 1,
            Kind::File => {
                totals.0 += 1;
                totals.2 += m.size;
            }
            Kind::Symlink => totals.0 += 1,
        }
    }
    // A fork is content too: a classic Mac app can keep all its code there.
    for (_, _, size) in &plan.sidecars {
        totals.2 += size;
    }
    Ok(totals)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zip_dates_read_as_utc() {
        assert_eq!(unix_seconds(1970, 1, 1, 0, 0, 1), Some(1));
        assert_eq!(unix_seconds(2000, 3, 1, 12, 0, 0), Some(951_912_000));
        assert_eq!(unix_seconds(1980, 0, 0, 0, 0, 0), None);
    }

    /// A Finder-style zip: a folder, a file whose fork rides in `__MACOSX`, and a symlink.
    fn mac_zip(dir: &Path) -> std::path::PathBuf {
        use std::io::Write as _;
        use zip::write::SimpleFileOptions;
        let path = dir.join("app.zip");
        let mut w = zip::ZipWriter::new(File::create(&path).unwrap());
        let o = SimpleFileOptions::default().unix_permissions(0o644);
        w.add_directory("Docs/", o).unwrap();
        w.start_file("Docs/HELLO.TXT", o).unwrap();
        w.write_all(b"hello").unwrap();
        w.add_directory("__MACOSX/Docs/", o).unwrap();
        w.start_file("__MACOSX/Docs/._HELLO.TXT", o).unwrap();
        #[cfg(feature = "appledouble")]
        w.write_all(&fs_mac::resource_fork::build_appledouble(
            b"TEXT",
            b"ttxt",
            fs_mac::resource_fork::MacFileDates::default(),
            b"RSRC",
        ))
        .unwrap();
        w.add_symlink("Docs/link", "HELLO.TXT", o).unwrap();
        w.finish().unwrap();
        path
    }

    fn import(efs: &mut dyn EditableFilesystem, zip: &Path) -> ImportStats {
        let root = efs.root().unwrap();
        let stats =
            import_zip_from_path_into(efs, &root, zip, &ImportOptions::default(), &|_| {}).unwrap();
        efs.sync_metadata().unwrap();
        stats
    }

    #[cfg(feature = "appledouble")]
    #[test]
    fn a_finder_zip_lands_on_hfs_with_its_fork() {
        let dir = tempfile::tempdir().unwrap();
        let zip = mac_zip(dir.path());
        assert!(looks_like_zip(&zip));
        let img = fs_mac::hfs::create_blank_hfs(4 * 1024 * 1024, 512, "T").unwrap();
        let mut efs =
            crate::test_open::open_editable(std::io::Cursor::new(img), 0, 0, None).unwrap();
        let stats = import(&mut *efs, &zip);
        assert_eq!(
            (stats.appledouble_paired, stats.appledouble_skipped),
            (1, 0)
        );
        let root = efs.root().unwrap();
        let fs = efs.as_filesystem_mut();
        let top: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert_eq!(top, ["Docs"], "no __MACOSX folder");
        let docs = fs.list_directory(&root).unwrap().remove(0);
        let kids = fs.list_directory(&docs).unwrap();
        let hello = kids
            .iter()
            .find(|e| e.name == "HELLO.TXT")
            .expect("HELLO.TXT");
        assert_eq!(hello.resource_fork_size, Some(4), "{hello:?}");
        assert_eq!(hello.type_code, Some(*b"TEXT"));
    }

    #[test]
    fn a_zip_lands_on_ufs_with_folders_and_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        let zip = mac_zip(dir.path());
        let img = fs_unix::ufs_format::create_blank_ufs1(&fs_unix::ufs_format::Ufs1FormatParams {
            size_bytes: 8 * 1024 * 1024,
            cg_layout: fs_unix::ufs::CgLayout::Bsd43,
            endian: fs_unix::ufs::UfsEndian::Big,
            ..Default::default()
        })
        .unwrap();
        let mut efs =
            crate::test_open::open_editable(std::io::Cursor::new(img), 0, 0, None).unwrap();
        let stats = import(&mut *efs, &zip);
        assert_eq!((stats.files, stats.symlinks), (1, 1), "{stats:?}");
        assert_eq!(measure_zip_expanded(&zip).unwrap().0, 2);
        let root = efs.root().unwrap();
        let fs = efs.as_filesystem_mut();
        let docs = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "Docs")
            .unwrap();
        let mut names: Vec<String> = fs
            .list_directory(&docs)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        names.sort();
        assert_eq!(names, ["HELLO.TXT", "link"]);
    }

    #[test]
    fn a_sidecar_names_its_file() {
        let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        assert_eq!(primary_of(&s(&["dir", "._App"])), s(&["dir", "App"]));
    }
}
