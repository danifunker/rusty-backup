//! Shared archive open + extraction logic used by both the CLI (`sit` verb)
//! and the GUI archive-browse tab.

use anyhow::{bail, Context, Result};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::fs::binhex;
use crate::fs::resource_fork::{self, sanitize_filename};

use super::stuffit::{self, StuffItArchive, StuffItEntry};
use super::stuffit5;

/// Container format for an extracted file's forks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForkFormat {
    /// One `.hqx` per file (both forks + Finder info).
    BinHex,
    /// One `.bin` MacBinary III per file.
    MacBinary,
    /// Data fork + `._name` AppleDouble sidecar.
    AppleDouble,
    /// Data fork + `name.rsrc` sidecar (resource fork only).
    Raw,
}

impl ForkFormat {
    pub const ALL: [ForkFormat; 4] = [
        ForkFormat::BinHex,
        ForkFormat::MacBinary,
        ForkFormat::AppleDouble,
        ForkFormat::Raw,
    ];

    pub fn label(&self) -> &'static str {
        match self {
            ForkFormat::BinHex => "BinHex (.hqx)",
            ForkFormat::MacBinary => "MacBinary (.bin)",
            ForkFormat::AppleDouble => "AppleDouble (._name)",
            ForkFormat::Raw => "Raw + .rsrc",
        }
    }
}

/// Result of an extraction run.
#[derive(Debug, Default, Clone, Copy)]
pub struct ExtractStats {
    pub files: usize,
    pub skipped: usize,
}

/// Read an archive file and parse it, transparently BinHex-decoding a
/// `.sit.hqx` wrapper and routing classic StuffIt (`SIT!`), StuffIt 5, and
/// `.sea` to the right parser. Returns the decoded archive bytes (offsets in
/// the entries are relative to these) plus the parsed directory.
pub fn open(path: &Path) -> Result<(Vec<u8>, StuffItArchive)> {
    let raw = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    open_bytes(raw)
}

/// Like [`open`] but from in-memory bytes (already-read file contents).
/// Routes through [`super::detect::detect_mac_archive`] so detection is
/// magic-driven rather than extension-driven: a loose `.hqx` carrying one
/// Mac file is synthesized into a single-entry archive so the rest of the
/// extract / browse path needs no special case.
pub fn open_bytes(raw: Vec<u8>) -> Result<(Vec<u8>, StuffItArchive)> {
    use super::detect::{detect_mac_archive, MacArchiveKind};

    match detect_mac_archive(&raw) {
        Some(MacArchiveKind::BinHexSingleFile) => {
            // BinHex carrying one Mac file with no SIT/SEA inside.
            // Synthesize a single-entry StuffIt archive that points at
            // the decoded forks (method = 0 / store, fork bytes laid
            // out back-to-back in the returned buffer).
            let bh = binhex::parse_binhex(&raw)?;
            Ok(synth_single_file_archive(bh))
        }
        Some(MacArchiveKind::BinHexOverSit) | Some(MacArchiveKind::BinHexOverSea) => {
            // Peel the HQX, then parse the inner SIT / SIT5 / SEA.
            let bh = binhex::parse_binhex(&raw)?;
            parse_sit_family(bh.data_fork)
        }
        Some(MacArchiveKind::BinHexOverCompactPro) => {
            // Peel the HQX, then parse the inner Compact Pro archive.
            let bh = binhex::parse_binhex(&raw)?;
            let archive = super::compactpro::parse(&bh.data_fork)?;
            Ok((bh.data_fork, archive))
        }
        Some(MacArchiveKind::CompactPro) => {
            let archive = super::compactpro::parse(&raw)?;
            Ok((raw, archive))
        }
        Some(MacArchiveKind::Mar) => super::mar::parse(&raw),
        Some(MacArchiveKind::MacZip) => super::maczip::parse(&raw),
        Some(MacArchiveKind::MacBinary) => {
            let mb = super::macbinary::parse(&raw)?;
            // Peel: a MacBinary's data fork is frequently itself an archive we
            // can go deeper on (e.g. a MacBinary III wrapping a StuffIt 5
            // archive). Mirror the BinHex-over-SIT behavior so the entries the
            // user sees are the inner archive's, not one opaque blob.
            match detect_mac_archive(&mb.data_fork) {
                Some(MacArchiveKind::Sit)
                | Some(MacArchiveKind::Sit5)
                | Some(MacArchiveKind::Sea) => parse_sit_family(mb.data_fork),
                Some(MacArchiveKind::CompactPro) => {
                    let archive = super::compactpro::parse(&mb.data_fork)?;
                    Ok((mb.data_fork, archive))
                }
                // Plain Mac file (app, document, wrapped disk image): one entry.
                // A wrapped disk image is surfaced via detect_mountable_image on
                // the single entry's data fork, exactly like other archives.
                _ => Ok(synth_from_macbinary(mb)),
            }
        }
        Some(MacArchiveKind::Sit) | Some(MacArchiveKind::Sit5) | Some(MacArchiveKind::Sea) => {
            parse_sit_family(raw)
        }
        None => {
            if is_stuffitx(&raw) {
                bail!(
                    "StuffIt X (.sitx) recognized, but native extraction is not yet \
                     implemented (its catalog + data streams use the Brimstone / \
                     PPMd-variant-G codec). Use `unar` for now."
                );
            }
            bail!("not a recognized Mac archive (BinHex / StuffIt / SEA)");
        }
    }
}

/// Pick the right SIT/SIT5/SEA parser for already-unwrapped bytes.
fn parse_sit_family(bytes: Vec<u8>) -> Result<(Vec<u8>, StuffItArchive)> {
    let archive = if stuffit5::is_stuffit5(&bytes) {
        stuffit5::parse(&bytes)?
    } else if stuffit::find_sea_archive(&bytes).is_some() {
        stuffit::parse(&bytes)?
    } else {
        bail!("not a recognized StuffIt archive (classic SIT! / StuffIt 5)");
    };
    Ok((bytes, archive))
}

/// Manufacture a one-entry `StuffItArchive` from a BinHex file that
/// doesn't wrap a SIT/SEA. The returned bytes hold the data fork
/// followed by the resource fork; the entry's `ForkInfo` uses
/// method = 0 (store) so [`stuffit::decompress_fork`] returns them
/// verbatim. Keeps the Archives tab / Extract All path unchanged.
fn synth_single_file_archive(bh: binhex::BinHexFile) -> (Vec<u8>, StuffItArchive) {
    synth_single_entry(
        bh.name,
        bh.type_code,
        bh.creator_code,
        bh.flags,
        0,
        0,
        &bh.data_fork,
        &bh.resource_fork,
    )
}

/// Manufacture a one-entry archive from a parsed MacBinary file. MacBinary
/// holds exactly one Mac file's two forks plus Finder info, so it slots into
/// the family the same way a single-file BinHex does — but it also carries
/// real create/modify dates, which are preserved here.
fn synth_from_macbinary(mb: super::macbinary::MacBinaryFile) -> (Vec<u8>, StuffItArchive) {
    synth_single_entry(
        mb.filename,
        mb.type_code,
        mb.creator_code,
        mb.finder_flags,
        mb.create_date,
        mb.modify_date,
        &mb.data_fork,
        &mb.resource_fork,
    )
}

/// Shared single-entry archive builder: lays the data fork then the resource
/// fork back-to-back in the returned buffer and points a method-0 (store)
/// entry at them, so [`stuffit::decompress_fork`] returns each verbatim. Used
/// by the loose-BinHex and MacBinary single-file paths.
#[allow(clippy::too_many_arguments)]
fn synth_single_entry(
    name: String,
    type_code: [u8; 4],
    creator_code: [u8; 4],
    finder_flags: u16,
    create_date: u32,
    mod_date: u32,
    data_fork: &[u8],
    resource_fork: &[u8],
) -> (Vec<u8>, StuffItArchive) {
    let data_len = data_fork.len() as u32;
    let rsrc_len = resource_fork.len() as u32;
    let data_crc = stuffit::crc16_arc(data_fork);
    let rsrc_crc = stuffit::crc16_arc(resource_fork);
    let mut buf = Vec::with_capacity(data_fork.len() + resource_fork.len());
    buf.extend_from_slice(data_fork);
    buf.extend_from_slice(resource_fork);
    let entry = stuffit::StuffItEntry {
        path: vec![name.clone()],
        name,
        is_dir: false,
        type_code,
        creator_code,
        finder_flags,
        create_date,
        mod_date,
        data: Some(stuffit::ForkInfo {
            method: 0,
            codec: stuffit::ForkCodec::StuffIt,
            encrypted: false,
            uncompressed_len: data_len,
            compressed_len: data_len,
            crc: data_crc,
            crc32: 0,
            offset: 0,
        }),
        rsrc: if rsrc_len > 0 {
            Some(stuffit::ForkInfo {
                method: 0,
                codec: stuffit::ForkCodec::StuffIt,
                encrypted: false,
                uncompressed_len: rsrc_len,
                compressed_len: rsrc_len,
                crc: rsrc_crc,
                crc32: 0,
                offset: data_len as u64,
            })
        } else {
            None
        },
    };
    (
        buf,
        StuffItArchive {
            entries: vec![entry],
        },
    )
}

/// Detect the StuffIt X container ("StuffIt!" / "StuffIt?"). Distinct from
/// StuffIt 5, whose 8th byte is a space ("StuffIt (c)1997…").
pub fn is_stuffitx(data: &[u8]) -> bool {
    data.len() >= 8 && &data[..7] == b"StuffIt" && (data[7] == b'!' || data[7] == b'?')
}

/// Extract every file in `archive` to `dest`, rebuilding the directory tree.
/// `progress(done, total, name)` is called before each file entry.
pub fn extract_all(
    bytes: &[u8],
    archive: &StuffItArchive,
    dest: &Path,
    format: ForkFormat,
    progress: impl FnMut(usize, usize, &str),
    log: impl FnMut(String),
) -> Result<ExtractStats> {
    extract_filtered(bytes, archive, dest, format, |_| true, progress, log)
}

/// Like [`extract_all`] but only writes entries for which `keep(idx)`
/// returns true (where `idx` indexes into `archive.entries`). Folder
/// markers are always written so the rebuilt tree is rooted correctly;
/// file entries the predicate rejects increment neither `files` nor
/// `skipped` and don't show up in the progress callback. Used by the
/// GUI Archives tab's "Extract Selected..." button (Workflow D.3).
pub fn extract_filtered(
    bytes: &[u8],
    archive: &StuffItArchive,
    dest: &Path,
    format: ForkFormat,
    mut keep: impl FnMut(usize) -> bool,
    mut progress: impl FnMut(usize, usize, &str),
    mut log: impl FnMut(String),
) -> Result<ExtractStats> {
    std::fs::create_dir_all(dest).with_context(|| format!("creating {}", dest.display()))?;

    let total = archive
        .entries
        .iter()
        .enumerate()
        .filter(|(i, e)| !e.is_dir && keep(*i))
        .count();

    // Raw format only: an entry's resource fork is written to a `name.rsrc`
    // sidecar, which collides when a *sibling* entry is literally named
    // `name.rsrc` (e.g. classic THINK projects ship `Foo.pi` alongside a
    // `Foo.pi.rsrc` resource file). Reserve every data-fork destination up
    // front so sidecars dodge them — data-fork names always keep their
    // natural path, sidecars yield to a non-colliding `.rsrc.N` variant.
    let reserved_data_paths: HashSet<PathBuf> = if format == ForkFormat::Raw {
        archive
            .entries
            .iter()
            .enumerate()
            .filter(|(i, e)| !e.is_dir && keep(*i))
            .map(|(_, e)| entry_target(dest, e))
            .collect()
    } else {
        HashSet::new()
    };
    let mut used_sidecars: HashSet<PathBuf> = HashSet::new();

    let mut stats = ExtractStats::default();
    let mut done = 0usize;

    for (idx, e) in archive.entries.iter().enumerate() {
        let target = entry_target(dest, e);

        if e.is_dir {
            // Always create directory markers — a subsequent kept file
            // might land underneath them.
            std::fs::create_dir_all(&target)?;
            continue;
        }
        if !keep(idx) {
            continue;
        }
        progress(done, total, &e.name);
        done += 1;
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let data = match decompress_named(bytes, e.data.as_ref(), e, &mut log) {
            Ok(d) => d,
            Err(()) => {
                stats.skipped += 1;
                continue;
            }
        };
        let rsrc = match decompress_named(bytes, e.rsrc.as_ref(), e, &mut log) {
            Ok(d) => d,
            Err(()) => {
                stats.skipped += 1;
                continue;
            }
        };

        // Compact Pro stores one CRC-32 per file over (resource fork ++ data
        // fork); verify it now that both forks are decompressed. This is the
        // format's only integrity check, so it covers single-fork, dual-fork,
        // and empty entries alike.
        if let Some(expected) = compactpro_entry_crc(e) {
            if let Err(err) = super::compactpro::verify_entry_crc(&rsrc, &data, expected) {
                log(format!("Skipped {}: {err}", e.display_path()));
                stats.skipped += 1;
                continue;
            }
        }

        // Resolve the raw resource-fork sidecar path, dodging any sibling
        // data fork that already owns `name.rsrc`. Other formats prefix the
        // sidecar (`._name`) or change its extension (`.hqx` / `.bin`), so
        // they never collide and pass `None`.
        let raw_rsrc_path = if format == ForkFormat::Raw && !rsrc.is_empty() {
            let natural = with_added_extension(&target, "rsrc");
            let chosen = raw_rsrc_sidecar_path(&target, &reserved_data_paths, &mut used_sidecars);
            if chosen != natural {
                log(format!(
                    "Note: resource fork of '{}' written as '{}' to avoid overwriting a sibling file named '{}'",
                    e.display_path(),
                    chosen.file_name().unwrap_or_default().to_string_lossy(),
                    natural.file_name().unwrap_or_default().to_string_lossy(),
                ));
            }
            Some(chosen)
        } else {
            None
        };

        write_entry(&target, e, &data, &rsrc, format, raw_rsrc_path.as_deref())?;
        stats.files += 1;
    }

    Ok(stats)
}

/// The per-file CRC-32 for a Compact Pro entry (carried identically on both
/// forks), or `None` for StuffIt entries which checksum each fork on decode.
fn compactpro_entry_crc(e: &StuffItEntry) -> Option<u32> {
    [e.rsrc.as_ref(), e.data.as_ref()]
        .into_iter()
        .flatten()
        .find(|f| matches!(f.codec, stuffit::ForkCodec::CompactPro { .. }))
        .map(|f| f.crc32)
}

fn decompress_named(
    bytes: &[u8],
    fork: Option<&stuffit::ForkInfo>,
    e: &StuffItEntry,
    log: &mut impl FnMut(String),
) -> Result<Vec<u8>, ()> {
    match fork {
        Some(f) if f.uncompressed_len > 0 => {
            let result = match f.codec {
                stuffit::ForkCodec::StuffIt => stuffit::decompress_fork(bytes, f),
                stuffit::ForkCodec::CompactPro { .. } => {
                    super::compactpro::decompress_fork(bytes, f)
                }
            };
            match result {
                Ok(d) => Ok(d),
                Err(err) => {
                    log(format!("Skipped {}: {err}", e.display_path()));
                    Err(())
                }
            }
        }
        _ => Ok(Vec::new()),
    }
}

/// Write one extracted file's forks to the host in the chosen container format.
///
/// `raw_rsrc_path` overrides where a [`ForkFormat::Raw`] resource sidecar is
/// written; the orchestrator passes a collision-free path here (see
/// [`raw_rsrc_sidecar_path`]). It is ignored for every other format, and a
/// `None` falls back to the natural `name.rsrc` next to `target`.
pub fn write_entry(
    target: &Path,
    entry: &StuffItEntry,
    data: &[u8],
    rsrc: &[u8],
    format: ForkFormat,
    raw_rsrc_path: Option<&Path>,
) -> Result<()> {
    match format {
        ForkFormat::BinHex => {
            let bh = binhex::BinHexFile {
                name: entry.name.clone(),
                type_code: entry.type_code,
                creator_code: entry.creator_code,
                flags: entry.finder_flags,
                data_fork: data.to_vec(),
                resource_fork: rsrc.to_vec(),
            };
            std::fs::write(
                with_added_extension(target, "hqx"),
                binhex::build_binhex(&bh).as_bytes(),
            )?;
        }
        ForkFormat::MacBinary => {
            let mb = resource_fork::build_macbinary(
                &entry.name,
                &entry.type_code,
                &entry.creator_code,
                data,
                rsrc,
            );
            std::fs::write(with_added_extension(target, "bin"), mb)?;
        }
        ForkFormat::AppleDouble => {
            std::fs::write(target, data)?;
            if !rsrc.is_empty() || entry.type_code != [0; 4] || entry.creator_code != [0; 4] {
                let ad =
                    resource_fork::build_appledouble(&entry.type_code, &entry.creator_code, rsrc);
                std::fs::write(sidecar_path(target, "._"), ad)?;
            }
        }
        ForkFormat::Raw => {
            std::fs::write(target, data)?;
            if !rsrc.is_empty() {
                let rsrc_dst = raw_rsrc_path
                    .map(Path::to_path_buf)
                    .unwrap_or_else(|| with_added_extension(target, "rsrc"));
                std::fs::write(rsrc_dst, rsrc)?;
            }
        }
    }
    Ok(())
}

/// Destination path for an entry's data fork: `dest` joined with the entry's
/// path components, each [`sanitize_filename`]d. Shared by the extraction loop
/// and the up-front data-path reservation so both agree on every name.
fn entry_target(dest: &Path, entry: &StuffItEntry) -> PathBuf {
    let mut rel = PathBuf::new();
    for comp in &entry.path {
        rel.push(sanitize_filename(comp));
    }
    dest.join(rel)
}

/// Pick a raw resource-fork sidecar path that won't clobber any data fork.
/// The natural name is `<target>.rsrc`; if a different entry's data fork
/// already owns that path (it's in `reserved`) or a previous sidecar took it
/// (`used`), fall back to `<target>.rsrc.1`, `.2`, ... until free. Records the
/// chosen path in `used` so two sidecars can't collide either.
fn raw_rsrc_sidecar_path(
    target: &Path,
    reserved: &HashSet<PathBuf>,
    used: &mut HashSet<PathBuf>,
) -> PathBuf {
    let base = with_added_extension(target, "rsrc");
    let mut candidate = base.clone();
    let mut n = 1u32;
    while reserved.contains(&candidate) || used.contains(&candidate) {
        candidate = with_added_extension(&base, &n.to_string());
        n += 1;
    }
    used.insert(candidate.clone());
    candidate
}

/// Append `.ext` to a path (keeping any existing extension).
fn with_added_extension(path: &Path, ext: &str) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".");
    name.push(ext);
    path.with_file_name(name)
}

/// Build a `._name`-style sidecar path next to `target`.
fn sidecar_path(target: &Path, prefix: &str) -> PathBuf {
    let name = target.file_name().unwrap_or_default().to_string_lossy();
    target.with_file_name(format!("{prefix}{name}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `open_bytes` on a loose `.hqx` (BinHex around plain Mac file
    /// bytes, no SIT inside) synthesizes a one-entry archive that
    /// decompresses to the original forks. Proves the Workflow D.1
    /// "accept loose .hqx" path.
    #[test]
    fn open_bytes_synthesizes_single_entry_archive_for_loose_binhex() {
        let bh = binhex::BinHexFile {
            name: "Document.txt".into(),
            type_code: *b"TEXT",
            creator_code: *b"ttxt",
            flags: 0x0042,
            data_fork: b"hello mac world".to_vec(),
            resource_fork: b"RSRC".to_vec(),
        };
        let hqx = binhex::build_binhex(&bh).into_bytes();

        let (buf, archive) = open_bytes(hqx).expect("loose hqx should open");
        assert_eq!(archive.entries.len(), 1, "synthesized archive is one entry");
        let entry = &archive.entries[0];
        assert_eq!(entry.name, "Document.txt");
        assert_eq!(entry.type_code, *b"TEXT");
        assert_eq!(entry.creator_code, *b"ttxt");
        assert_eq!(entry.finder_flags, 0x0042);
        assert!(!entry.is_dir);

        let data = stuffit::decompress_fork(&buf, entry.data.as_ref().unwrap())
            .expect("data fork round-trips through method=0");
        assert_eq!(data, b"hello mac world");

        let rsrc = stuffit::decompress_fork(&buf, entry.rsrc.as_ref().unwrap())
            .expect("rsrc fork round-trips through method=0");
        assert_eq!(rsrc, b"RSRC");
    }

    /// A loose `.hqx` with an empty resource fork should synthesize an
    /// entry with `rsrc = None` (matching how real Mac files without
    /// resource forks present in StuffIt archives).
    #[test]
    fn open_bytes_empty_rsrc_omits_fork() {
        let bh = binhex::BinHexFile {
            name: "data.bin".into(),
            type_code: [0; 4],
            creator_code: [0; 4],
            flags: 0,
            data_fork: vec![1, 2, 3, 4],
            resource_fork: Vec::new(),
        };
        let hqx = binhex::build_binhex(&bh).into_bytes();
        let (_buf, archive) = open_bytes(hqx).expect("open");
        assert!(
            archive.entries[0].rsrc.is_none(),
            "empty rsrc folds to None"
        );
        assert!(archive.entries[0].data.is_some(), "data fork still present");
    }

    /// `extract_filtered` writes only the entries the predicate keeps;
    /// folder markers always materialize so subsequent kept files have
    /// somewhere to land. Proves the Workflow D.3 "Extract Selected"
    /// data path.
    #[test]
    fn extract_filtered_writes_only_kept_entries() {
        use super::super::stuffit::{
            build_archive_tree, StuffItInput, StuffItInputNode, WriteMethod,
        };
        // Build a 3-file archive: file1 + file2 at root, file3 under
        // subdir/. Then extract only file1 and file3.
        let mk = |name: &str, body: &[u8]| StuffItInput {
            name: name.into(),
            type_code: [0; 4],
            creator_code: [0; 4],
            finder_flags: 0,
            create_date: 0,
            mod_date: 0,
            data_fork: body.to_vec(),
            resource_fork: Vec::new(),
        };
        let tree = vec![
            StuffItInputNode::File(mk("file1.txt", b"AAA")),
            StuffItInputNode::File(mk("file2.txt", b"BBB")),
            StuffItInputNode::Folder {
                name: "subdir".into(),
                finder_flags: 0,
                create_date: 0,
                mod_date: 0,
                children: vec![StuffItInputNode::File(mk("file3.txt", b"CCC"))],
            },
        ];
        let arc_bytes = build_archive_tree(&tree, WriteMethod::Store).expect("build");
        let (bytes, archive) = open_bytes(arc_bytes).expect("open");

        // Find the indices of file1.txt and file3.txt in the parsed
        // archive (folder markers + entry ordering can vary).
        let keep_names: std::collections::HashSet<&str> =
            ["file1.txt", "file3.txt"].into_iter().collect();
        let keep_idx: std::collections::HashSet<usize> = archive
            .entries
            .iter()
            .enumerate()
            .filter(|(_, e)| keep_names.contains(e.name.as_str()))
            .map(|(i, _)| i)
            .collect();

        let dir = tempfile::tempdir().expect("tempdir");
        let stats = extract_filtered(
            &bytes,
            &archive,
            dir.path(),
            ForkFormat::Raw,
            |i| keep_idx.contains(&i),
            |_, _, _| {},
            |_| {},
        )
        .expect("extract");
        assert_eq!(stats.files, 2);
        assert_eq!(stats.skipped, 0);

        assert!(dir.path().join("file1.txt").exists());
        assert!(
            !dir.path().join("file2.txt").exists(),
            "file2 should be skipped"
        );
        assert!(dir.path().join("subdir/file3.txt").exists());
    }

    /// Raw extraction must not lose a resource fork when a sibling entry is
    /// literally named `<name>.rsrc` and would otherwise overwrite the
    /// sidecar. The data fork of the `.rsrc`-named file keeps its natural
    /// path; the colliding resource fork is parked at `<name>.rsrc.1`.
    /// (Reproduces the macppp `ppp.pi` / `ppp.pi.rsrc` data loss from the
    /// real archive.)
    #[test]
    fn raw_extract_preserves_forks_on_dot_rsrc_name_collision() {
        use super::super::stuffit::{build_archive_tree, StuffItInputNode, WriteMethod};
        let mk = |name: &str, data: &[u8], rsrc: &[u8]| super::super::stuffit::StuffItInput {
            name: name.into(),
            type_code: [0; 4],
            creator_code: [0; 4],
            finder_flags: 0,
            create_date: 0,
            mod_date: 0,
            data_fork: data.to_vec(),
            resource_fork: rsrc.to_vec(),
        };
        // `Doc` carries a resource fork; `Doc.rsrc` is a *separate* file
        // whose data fork would land on `Doc`'s sidecar path.
        let tree = vec![
            StuffItInputNode::File(mk("Doc", b"DOC-DATA", b"DOC-RSRC")),
            StuffItInputNode::File(mk("Doc.rsrc", b"SIBLING-DATA", b"")),
        ];
        let arc_bytes = build_archive_tree(&tree, WriteMethod::Store).expect("build");
        let (bytes, archive) = open_bytes(arc_bytes).expect("open");

        let dir = tempfile::tempdir().expect("tempdir");
        let stats = extract_all(
            &bytes,
            &archive,
            dir.path(),
            ForkFormat::Raw,
            |_, _, _| {},
            |_| {},
        )
        .expect("extract");
        assert_eq!(stats.files, 2);
        assert_eq!(stats.skipped, 0);

        // The sibling file's data fork keeps `Doc.rsrc` intact.
        assert_eq!(
            std::fs::read(dir.path().join("Doc.rsrc")).unwrap(),
            b"SIBLING-DATA",
            "sibling data fork must not be clobbered"
        );
        // `Doc`'s data fork is untouched.
        assert_eq!(std::fs::read(dir.path().join("Doc")).unwrap(), b"DOC-DATA");
        // `Doc`'s resource fork survives at the de-conflicted sidecar.
        assert_eq!(
            std::fs::read(dir.path().join("Doc.rsrc.1")).unwrap(),
            b"DOC-RSRC",
            "Doc resource fork must be preserved at a non-colliding path"
        );
    }

    /// Build a MacBinary II archive (valid CRC) for the open_bytes tests.
    fn build_macbinary_ii(name: &[u8], flags: u16, data: &[u8], rsrc: &[u8]) -> Vec<u8> {
        use byteorder::{BigEndian, ByteOrder};
        let mut hdr = [0u8; 128];
        hdr[1] = name.len() as u8;
        hdr[2..2 + name.len()].copy_from_slice(name);
        hdr[65..69].copy_from_slice(b"APPL");
        hdr[69..73].copy_from_slice(b"Po.P");
        hdr[73] = (flags >> 8) as u8;
        hdr[101] = (flags & 0xFF) as u8;
        BigEndian::write_u32(&mut hdr[83..87], data.len() as u32);
        BigEndian::write_u32(&mut hdr[87..91], rsrc.len() as u32);
        BigEndian::write_u32(&mut hdr[91..95], 0x1111_1111); // create
        BigEndian::write_u32(&mut hdr[95..99], 0x2222_2222); // modify
        hdr[122] = 129;
        hdr[123] = 129;
        let crc = crate::fs::resource_fork::macbinary_crc16(&hdr[0..124]);
        BigEndian::write_u16(&mut hdr[124..126], crc);
        let mut out = hdr.to_vec();
        out.extend_from_slice(data);
        while out.len() % 128 != 0 {
            out.push(0);
        }
        out.extend_from_slice(rsrc);
        while out.len() % 128 != 0 {
            out.push(0);
        }
        out
    }

    /// A plain MacBinary file (no inner archive) synthesizes a one-entry
    /// archive whose forks decompress verbatim and whose Finder flags + dates
    /// are preserved. Proves Phase 2 of docs/native_mac_archives.md.
    #[test]
    fn open_bytes_synthesizes_single_entry_from_macbinary() {
        let mb = build_macbinary_ii(b"Prince", 0x2000, b"data-fork-bytes", b"RSRCDATA");
        let (buf, archive) = open_bytes(mb).expect("MacBinary should open");
        assert_eq!(archive.entries.len(), 1);
        let e = &archive.entries[0];
        assert_eq!(e.name, "Prince");
        assert_eq!(&e.type_code, b"APPL");
        assert_eq!(&e.creator_code, b"Po.P");
        assert_eq!(e.finder_flags, 0x2000, "hasBundle preserved");
        assert_eq!(e.create_date, 0x1111_1111);
        assert_eq!(e.mod_date, 0x2222_2222);
        let data = stuffit::decompress_fork(&buf, e.data.as_ref().unwrap()).unwrap();
        assert_eq!(data, b"data-fork-bytes");
        let rsrc = stuffit::decompress_fork(&buf, e.rsrc.as_ref().unwrap()).unwrap();
        assert_eq!(rsrc, b"RSRCDATA");
    }

    /// A MacBinary whose data fork is itself a StuffIt archive peels through to
    /// the inner archive's entries (Phase 3 peel), rather than surfacing one
    /// opaque blob.
    #[test]
    fn open_bytes_peels_macbinary_over_sit() {
        use super::super::stuffit::{
            build_archive_tree, StuffItInput, StuffItInputNode, WriteMethod,
        };
        let mk = |name: &str, body: &[u8]| StuffItInput {
            name: name.into(),
            type_code: [0; 4],
            creator_code: [0; 4],
            finder_flags: 0,
            create_date: 0,
            mod_date: 0,
            data_fork: body.to_vec(),
            resource_fork: Vec::new(),
        };
        let inner = build_archive_tree(
            &[
                StuffItInputNode::File(mk("inner1.txt", b"AAA")),
                StuffItInputNode::File(mk("inner2.txt", b"BBB")),
            ],
            WriteMethod::Store,
        )
        .expect("build inner SIT");
        let mb = build_macbinary_ii(b"Bundle.sit.bin", 0, &inner, b"");
        let (_buf, archive) = open_bytes(mb).expect("MacBinary-over-SIT should open");
        let names: std::collections::HashSet<&str> =
            archive.entries.iter().map(|e| e.name.as_str()).collect();
        assert!(
            names.contains("inner1.txt") && names.contains("inner2.txt"),
            "should expand to inner SIT entries, got {names:?}"
        );
    }

    /// `open_bytes` on truly opaque bytes returns the new
    /// magic-driven error message rather than the legacy SIT-specific one.
    #[test]
    fn open_bytes_rejects_non_archive_bytes_with_clear_message() {
        let junk = b"this is not a Mac archive of any kind".to_vec();
        let err = open_bytes(junk).expect_err("should reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("not a recognized Mac archive"),
            "expected magic-driven error, got: {msg}"
        );
    }
}
