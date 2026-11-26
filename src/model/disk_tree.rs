//! A GNU `tree`-style listing of a whole disk or of one partition: the Inspect browser's Tree, for
//! every volume on the disk at once. Disc images list each of their filesystems (a hybrid disc's
//! sides), and `rb://` images are listed by the daemon that serves them. Used by `rb-cli tree`.

use std::path::Path;

use anyhow::{anyhow, Result};

use crate::cli::img_at::PartSelector;
use crate::cli::resolve::FsDispatchOverride;

/// One volume's listing, or why it could not be listed.
#[derive(Debug)]
pub struct VolumeTree {
    /// Where it is: "Partition @2 / @s2 (MBR): FAT16 ... @ LBA 34816, ..." or a disc's side.
    pub label: String,
    pub tree: Result<String, String>,
}

/// How to read the volumes.
#[derive(Default)]
pub struct TreeOptions<'a> {
    /// Each entry's filesystem ID too (CNID, inode, cluster), as the browser's View menu offers.
    pub ids: bool,
    pub password: Option<&'a str>,
    /// `--fs-type`: read every listed volume as this filesystem.
    pub fs_override: Option<&'a FsDispatchOverride>,
    /// The disk image to open inside a `.zip` holding several.
    pub inside: Option<&'a str>,
}

/// The trees of the volume `selector` picks, or with `None` of every volume on the disk.
pub fn trees(
    path: &Path,
    selector: Option<PartSelector>,
    opts: &TreeOptions,
) -> Result<Vec<VolumeTree>> {
    #[cfg(feature = "remote")]
    if let Some(r) = crate::remote::RemoteRef::parse(&path.to_string_lossy()) {
        return remote_trees(&r, selector, opts.ids);
    }
    #[cfg(feature = "optical")]
    if crate::cli::optical_hint::is_browsable_optical(path) {
        return disc_trees(path, selector, opts.ids);
    }
    let count = match &selector {
        Some(_) => 0,
        None => partition_count(path, opts)?,
    };
    if selector.is_some() || count == 0 {
        return Ok(vec![one(path, selector, opts)?]);
    }
    // One position that won't resolve is reported on its row, not fatal to the rest (MJB-012).
    Ok((1..=count as u32)
        .map(|n| {
            one(path, Some(PartSelector::Position(n)), opts).unwrap_or_else(|e| VolumeTree {
                label: format!("Partition @{n}"),
                tree: Err(format!("{e:#}")),
            })
        })
        .collect())
}

/// How many partitions `@N` numbers on this disk; 0 for a bare volume.
fn partition_count(path: &Path, opts: &TreeOptions) -> Result<usize> {
    if crate::cli::backup_edit::is_backup_folder(path) {
        return crate::cli::backup_edit::partition_count(path);
    }
    let pw = opts.password.map(str::as_bytes);
    let mut reader =
        crate::model::source_reader::open_peeled_read_with_entry(path, pw, opts.inside)?;
    Ok(
        match crate::partition::PartitionTable::detect(&mut reader) {
            Ok(table) => crate::partition::partitions_with_volumes(&mut reader, &table).len(),
            // `--fs-type` on a disk with no table: the whole image is the volume.
            Err(_) if opts.fs_override.is_some_and(|o| o.fs_type.is_some()) => 0,
            Err(e) => return Err(anyhow!(e)),
        },
    )
}

/// One volume of a local disk; a volume that does not open is reported, not fatal.
fn one(path: &Path, selector: Option<PartSelector>, opts: &TreeOptions) -> Result<VolumeTree> {
    let pw = opts.password.map(str::as_bytes);
    let fs_type = opts.fs_override.and_then(|o| o.fs_type.as_deref());
    let (reader, mut ctx) = crate::cli::resolve::resolve_partition_streaming_forced_inside(
        path,
        selector,
        pw,
        fs_type,
        opts.inside,
    )?;
    if let Some(o) = opts.fs_override {
        o.apply(&mut ctx);
    }
    let tree = match ctx.open_ro(reader, opts.password) {
        Ok(mut fs) => listing(fs.as_mut(), opts.ids),
        Err(e) => Err(format!("no readable filesystem: {e}")),
    };
    Ok(VolumeTree {
        label: ctx.label,
        tree,
    })
}

fn listing(fs: &mut dyn disk_core::filesystem::Filesystem, ids: bool) -> Result<String, String> {
    let text = match ids {
        true => crate::fs::tree::format_tree_with_ids(fs),
        false => crate::fs::tree::format_tree(fs),
    };
    text.map_err(|e| format!("cannot list the tree: {e}"))
}

/// A disc image: each filesystem it offers (a hybrid's ISO 9660 and HFS sides), or side `@N`.
#[cfg(feature = "optical")]
fn disc_trees(path: &Path, selector: Option<PartSelector>, ids: bool) -> Result<Vec<VolumeTree>> {
    let src = crate::model::commander_descend::DiscSource {
        path: path.to_path_buf(),
        physical: false,
    };
    let open = crate::model::optical_browse::open_source(&src).map_err(|e| anyhow!(e))?;
    let sides: Vec<usize> = match selector {
        None => (0..open.labels.len().max(1)).collect(),
        Some(PartSelector::Position(n)) if n >= 1 && (n as usize) <= open.labels.len() => {
            vec![n as usize - 1]
        }
        Some(sel) => {
            return Err(anyhow!(
                "{sel}: this disc has {} filesystem(s), numbered @1..@{}",
                open.labels.len(),
                open.labels.len()
            ))
        }
    };
    if ids {
        log::warn!("--ids does not apply to disc images");
    }
    Ok(sides
        .into_iter()
        .map(|i| {
            let mut disc = open.disc.clone();
            disc.selected = i;
            let name = open.labels.get(i).cloned().unwrap_or_default();
            VolumeTree {
                label: format!("Filesystem @{}: {name}", i + 1),
                tree: disc.tree_listing(),
            }
        })
        .collect())
}

/// An image on an `rb-cli serve` daemon, listed over the wire one folder at a time.
#[cfg(feature = "remote")]
fn remote_trees(
    r: &crate::remote::RemoteRef,
    selector: Option<PartSelector>,
    ids: bool,
) -> Result<Vec<VolumeTree>> {
    use crate::remote::{RemoteBlockReader, RemoteConnection, RemoteFilesystem};
    let conn = RemoteConnection::connect_shared(&r.addr())?;
    let selectors: Vec<Option<PartSelector>> = match selector {
        Some(sel) => vec![Some(sel)],
        None => {
            let mut reader = RemoteBlockReader::open(std::sync::Arc::clone(&conn), &r.path)?;
            // LVM logical volumes number after the table's rows, as the local listing has (MJB-019).
            let count = match crate::partition::PartitionTable::detect(&mut reader) {
                Ok(t) => crate::partition::partitions_with_volumes(&mut reader, &t).len(),
                Err(_) => 0,
            };
            match count {
                0 => vec![None],
                n => (1..=n as u32)
                    .map(|n| Some(PartSelector::Position(n)))
                    .collect(),
            }
        }
    };
    Ok(selectors
        .into_iter()
        .map(|sel| {
            let label = match &sel {
                Some(s) => format!("rb://{}{}{s}", r.addr(), r.path),
                None => format!("rb://{}{}", r.addr(), r.path),
            };
            let opened =
                RemoteFilesystem::on_connection(std::sync::Arc::clone(&conn), &r.path, sel);
            let tree = match opened {
                Ok((mut fs, _, _)) => listing(&mut fs, ids),
                Err(e) => Err(format!("no readable filesystem: {e:#}")),
            };
            VolumeTree { label, tree }
        })
        .collect())
}
