//! `api hfs` — Classic-HFS image operations (create, browse, edit
//! single-partition `.dsk` and APM-wrapped images).
//!
//! Mac paths on the CLI use `/` as the separator. `/` is illegal in HFS
//! filenames so there's no ambiguity. The HFS native separator `:` is
//! not accepted to keep the surface small and shell-friendly.

use anyhow::{anyhow, bail, Context, Result};
use clap::Subcommand;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::cli::io::{open_image_ro, open_image_rw};
use crate::cli::parse::{parse_size, pick_block_size, split_mac_path, ZeroReader};
use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
use crate::fs::hfs::{create_blank_hfs_sized, validate_hfs_integrity, HfsFilesystem};
use crate::partition::apm::Apm;

#[derive(Subcommand, Debug)]
pub enum HfsCommand {
    /// Create a fresh blank HFS volume at the given path.
    New {
        /// Image file to create. Overwritten if it already exists.
        image: PathBuf,
        /// Volume size, accepting plain bytes or `K`/`KiB`/`M`/`MiB` suffixes
        /// (e.g. `800K`, `5M`). Defaults to 800K (an 800 KiB floppy).
        #[arg(long, default_value = "800K")]
        size: String,
        /// HFS volume name (1..=27 Mac Roman bytes). Defaults to `MacIIBench`.
        #[arg(long, default_value = "MacIIBench")]
        name: String,
        /// HFS allocation block size in bytes. Must be a non-zero multiple of
        /// 512. When unset, the smallest size that keeps `total_blocks <=
        /// 65535` is chosen automatically (e.g. 512 for floppies, larger for
        /// multi-MiB SCSI images).
        #[arg(long = "block-size")]
        block_size: Option<u32>,
    },
    /// Print volume name, sizes, and counts for an HFS image.
    Info {
        image: PathBuf,
        /// APM partition index to open (1-based). If unset and the image is
        /// an APM disk, the sole Apple_HFS partition is used.
        #[arg(long)]
        partition: Option<u32>,
    },
    /// List a directory inside the HFS volume.
    Ls {
        image: PathBuf,
        /// Mac path (use `/` separators). Defaults to root.
        #[arg(default_value = "/")]
        path: String,
        #[arg(long)]
        partition: Option<u32>,
    },
    /// Copy a host file into the HFS volume.
    Put {
        image: PathBuf,
        /// Source file on the host.
        host_file: PathBuf,
        /// Destination Mac path inside the volume. The parent directory must
        /// already exist.
        mac_path: String,
        /// HFS 4-character type code. Defaults to `BINA`.
        #[arg(long = "type", default_value = "BINA")]
        type_code: String,
        /// HFS 4-character creator code. Defaults to `????`.
        #[arg(long, default_value = "????")]
        creator: String,
        /// Overwrite an existing entry at the destination path.
        #[arg(long)]
        force: bool,
        #[arg(long)]
        partition: Option<u32>,
    },
    /// Pre-allocate a zero-filled file at the given Mac path. Useful for
    /// reserving a results file the boot ROM will fill in.
    PutZero {
        image: PathBuf,
        mac_path: String,
        /// Number of zero bytes to allocate.
        size: u64,
        #[arg(long = "type", default_value = "BINA")]
        type_code: String,
        #[arg(long, default_value = "????")]
        creator: String,
        #[arg(long)]
        force: bool,
        #[arg(long)]
        partition: Option<u32>,
    },
    /// Extract an HFS file to the host.
    Get {
        image: PathBuf,
        mac_path: String,
        host_file: PathBuf,
        #[arg(long)]
        partition: Option<u32>,
    },
    /// Delete a file from the HFS volume.
    Rm {
        image: PathBuf,
        mac_path: String,
        #[arg(long)]
        partition: Option<u32>,
    },
    /// Overwrite the 1024-byte boot block region at offset 0. The source
    /// must be at most 1024 bytes and is written verbatim — no padding,
    /// no HFS B-tree touch. Operates on the file's byte 0 regardless of
    /// any APM wrapping.
    PutBoot { image: PathBuf, bb_file: PathBuf },
    /// Run the lightweight HFS integrity check on the image.
    Validate {
        image: PathBuf,
        #[arg(long)]
        partition: Option<u32>,
    },
}

pub fn run(verb: HfsCommand) -> Result<()> {
    match verb {
        HfsCommand::New {
            image,
            size,
            name,
            block_size,
        } => cmd_new(image, &size, &name, block_size),
        HfsCommand::Info { image, partition } => cmd_info(image, partition),
        HfsCommand::Ls {
            image,
            path,
            partition,
        } => cmd_ls(image, &path, partition),
        HfsCommand::Put {
            image,
            host_file,
            mac_path,
            type_code,
            creator,
            force,
            partition,
        } => cmd_put(
            image,
            Some(host_file),
            &mac_path,
            &type_code,
            &creator,
            None,
            force,
            partition,
        ),
        HfsCommand::PutZero {
            image,
            mac_path,
            size,
            type_code,
            creator,
            force,
            partition,
        } => cmd_put(
            image,
            None,
            &mac_path,
            &type_code,
            &creator,
            Some(size),
            force,
            partition,
        ),
        HfsCommand::Get {
            image,
            mac_path,
            host_file,
            partition,
        } => cmd_get(image, &mac_path, host_file, partition),
        HfsCommand::Rm {
            image,
            mac_path,
            partition,
        } => cmd_rm(image, &mac_path, partition),
        HfsCommand::PutBoot { image, bb_file } => cmd_put_boot(image, bb_file),
        HfsCommand::Validate { image, partition } => cmd_validate(image, partition),
    }
}

/// Resolve the byte offset to open as an HFS partition. If the file starts
/// with an APM signature ("ER"), parse it and pick the requested Apple_HFS
/// partition (1-based index), or the sole Apple_HFS partition when `partition`
/// is None. Returns offset=0 (raw HFS at byte 0) when there's no APM. The
/// optional second element describes the selected APM partition, for logging.
pub fn resolve_hfs_offset(
    file: &mut std::fs::File,
    partition: Option<u32>,
) -> Result<(u64, Option<String>)> {
    let mut sig = [0u8; 2];
    file.seek(SeekFrom::Start(0))?;
    if file.read(&mut sig)? < 2 {
        return Ok((0, None));
    }
    let is_apm = sig == [b'E', b'R'];
    if !is_apm {
        if partition.is_some() {
            bail!("--partition specified but image is not an APM disk");
        }
        return Ok((0, Some("Partition: raw HFS @ byte 0".to_string())));
    }

    file.seek(SeekFrom::Start(0))?;
    let apm = Apm::parse(file).map_err(|e| anyhow!("parsing APM: {e}"))?;
    let bs = apm.ddr.block_size as u64;
    if bs == 0 {
        bail!("APM DDR block_size is 0");
    }

    let (idx, entry) = match partition {
        Some(idx) => {
            if idx == 0 || (idx as usize) > apm.entries.len() {
                bail!(
                    "partition index {idx} out of range (have {} entries)",
                    apm.entries.len()
                );
            }
            let e = &apm.entries[idx as usize - 1];
            if e.partition_type != "Apple_HFS" {
                bail!("partition {idx} is {:?}, not Apple_HFS", e.partition_type);
            }
            (idx as usize, e)
        }
        None => {
            let hfs: Vec<(usize, &_)> = apm
                .entries
                .iter()
                .enumerate()
                .filter(|(_, e)| e.partition_type == "Apple_HFS")
                .collect();
            match hfs.len() {
                0 => bail!("no Apple_HFS partition found in APM disk"),
                1 => (hfs[0].0 + 1, hfs[0].1),
                _ => {
                    let indices: Vec<String> =
                        hfs.iter().map(|(i, _)| (i + 1).to_string()).collect();
                    bail!(
                        "multiple Apple_HFS partitions found (indices {}); pass --partition N",
                        indices.join(", ")
                    );
                }
            }
        }
    };

    let label = format!(
        "Partition {} (APM): {} {:?} @ block {}, {} blocks",
        idx, entry.partition_type, entry.name, entry.start_block, entry.block_count
    );
    Ok((entry.start_block as u64 * bs, Some(label)))
}

// ---------------------------------------------------------------------------
// new
// ---------------------------------------------------------------------------

pub(crate) fn cmd_new(
    image: PathBuf,
    size_arg: &str,
    name: &str,
    block_size: Option<u32>,
) -> Result<()> {
    cmd_new_sized(image, size_arg, name, block_size, None, None)
}

/// Variant of [`cmd_new`] that exposes B-tree sizing overrides. When
/// `catalog_bytes` / `extents_bytes` are `None`, falls back to
/// [`crate::fs::hfs::default_btree_sizes`] (~0.5 % of volume, clump-
/// aligned, 24-block floor) so steady-state file activity doesn't
/// trigger B-tree extensions.
pub(crate) fn cmd_new_sized(
    image: PathBuf,
    size_arg: &str,
    name: &str,
    block_size: Option<u32>,
    catalog_bytes: Option<u32>,
    extents_bytes: Option<u32>,
) -> Result<()> {
    let size = parse_size(size_arg)?;
    if size < 1024 {
        bail!("size {size} is too small for an HFS volume");
    }
    let block_size = match block_size {
        Some(bs) => {
            if bs == 0 || bs % 512 != 0 {
                bail!("block-size must be a non-zero multiple of 512 (got {bs})");
            }
            bs
        }
        None => pick_block_size(size),
    };

    let (default_cat, default_ext) = crate::fs::hfs::default_btree_sizes(size, block_size);
    let catalog_bytes = catalog_bytes.unwrap_or(default_cat);
    let extents_bytes = extents_bytes.unwrap_or(default_ext);

    let bytes = create_blank_hfs_sized(size, block_size, name, extents_bytes, catalog_bytes)
        .map_err(|e| anyhow!("failed to format HFS volume: {e}"))?;

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&image)
        .with_context(|| format!("opening {} for write", image.display()))?;
    file.write_all(&bytes)
        .with_context(|| format!("writing {}", image.display()))?;
    file.flush().ok();

    println!(
        "Created HFS volume {} ({} bytes, block_size={})",
        image.display(),
        bytes.len(),
        block_size
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// info / ls
// ---------------------------------------------------------------------------

pub(crate) fn cmd_info(image: PathBuf, partition: Option<u32>) -> Result<()> {
    let mut file = open_image_ro(&image)?;
    let (offset, label) = resolve_hfs_offset(&mut file, partition)?;
    if let Some(l) = &label {
        eprintln!("{l}");
    }
    let fs = HfsFilesystem::open(file, offset).map_err(|e| anyhow!("opening HFS: {e}"))?;
    let s = fs.volume_summary();
    println!("Volume:      {}", s.volume_name);
    println!("Block size:  {} bytes", s.block_size);
    println!("Total blocks:{}", s.total_blocks);
    println!("Free blocks: {}", s.free_blocks);
    println!("Used bytes:  {}", s.used_bytes);
    println!("Files:       {}", s.file_count);
    println!("Folders:     {}", s.folder_count);
    Ok(())
}

pub(crate) fn cmd_ls(image: PathBuf, path: &str, partition: Option<u32>) -> Result<()> {
    let mut file = open_image_ro(&image)?;
    let (offset, label) = resolve_hfs_offset(&mut file, partition)?;
    if let Some(l) = &label {
        eprintln!("{l}");
    }
    let mut fs = HfsFilesystem::open(file, offset).map_err(|e| anyhow!("opening HFS: {e}"))?;
    let entry = resolve_path(&mut fs, path)?;
    if !entry.is_directory() {
        bail!("not a directory: {path}");
    }
    let children = fs
        .list_directory(&entry)
        .map_err(|e| anyhow!("list_directory: {e}"))?;
    for c in children {
        let kind = if c.is_directory() { "DIR " } else { "FILE" };
        let t = c.type_code.as_deref().unwrap_or("    ");
        let cr = c.creator_code.as_deref().unwrap_or("    ");
        println!("{kind}  {:>10}  {t} {cr}  {}", c.size, c.name);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// put / get / rm
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub(crate) fn cmd_put(
    image: PathBuf,
    host_file: Option<PathBuf>,
    mac_path: &str,
    type_code: &str,
    creator: &str,
    zero_pad: Option<u64>,
    force: bool,
    partition: Option<u32>,
) -> Result<()> {
    let (parent_path, name) = split_mac_path(mac_path)?;
    if name.is_empty() {
        bail!("destination path has no filename");
    }

    let mut file = open_image_rw(&image)?;
    let (offset, label) = resolve_hfs_offset(&mut file, partition)?;
    if let Some(l) = &label {
        eprintln!("{l}");
    }
    let mut fs = HfsFilesystem::open(file, offset).map_err(|e| anyhow!("opening HFS: {e}"))?;

    let parent = resolve_path(&mut fs, &parent_path)?;
    if !parent.is_directory() {
        bail!("parent is not a directory: {parent_path}");
    }

    // Duplicate check (so we can honor --force consistently).
    let existing = fs
        .list_directory(&parent)
        .map_err(|e| anyhow!("list_directory: {e}"))?
        .into_iter()
        .find(|e| e.name == name);
    if let Some(ref e) = existing {
        if !force {
            bail!("{mac_path} already exists (pass --force to overwrite)");
        }
        fs.delete_entry(&parent, e)
            .map_err(|e| anyhow!("delete existing: {e}"))?;
    }

    let options = CreateFileOptions {
        type_code: Some(type_code.to_string()),
        creator_code: Some(creator.to_string()),
        ..Default::default()
    };

    if let Some(n) = zero_pad {
        let mut zr = ZeroReader { remaining: n };
        fs.create_file(&parent, &name, &mut zr, n, &options)
            .map_err(|e| anyhow!("create_file: {e}"))?;
    } else {
        let host = host_file.ok_or_else(|| {
            anyhow!("either provide a host file or use --zero-pad N to pre-allocate zeros")
        })?;
        let meta = std::fs::metadata(&host).with_context(|| format!("stat {}", host.display()))?;
        let len = meta.len();
        let mut hf =
            std::fs::File::open(&host).with_context(|| format!("open {}", host.display()))?;
        fs.create_file(&parent, &name, &mut hf, len, &options)
            .map_err(|e| anyhow!("create_file: {e}"))?;
    }

    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    Ok(())
}

pub(crate) fn cmd_get(
    image: PathBuf,
    mac_path: &str,
    host_file: PathBuf,
    partition: Option<u32>,
) -> Result<()> {
    let mut file = open_image_ro(&image)?;
    let (offset, label) = resolve_hfs_offset(&mut file, partition)?;
    if let Some(l) = &label {
        eprintln!("{l}");
    }
    let mut fs = HfsFilesystem::open(file, offset).map_err(|e| anyhow!("opening HFS: {e}"))?;
    let entry = resolve_path(&mut fs, mac_path)?;
    if entry.is_directory() {
        bail!("{mac_path} is a directory");
    }
    let mut out = std::fs::File::create(&host_file)
        .with_context(|| format!("creating {}", host_file.display()))?;
    fs.write_file_to(&entry, &mut out)
        .map_err(|e| anyhow!("write_file_to: {e}"))?;
    Ok(())
}

pub(crate) fn cmd_rm(image: PathBuf, mac_path: &str, partition: Option<u32>) -> Result<()> {
    let (parent_path, name) = split_mac_path(mac_path)?;
    if name.is_empty() {
        bail!("path has no filename");
    }
    let mut file = open_image_rw(&image)?;
    let (offset, label) = resolve_hfs_offset(&mut file, partition)?;
    if let Some(l) = &label {
        eprintln!("{l}");
    }
    let mut fs = HfsFilesystem::open(file, offset).map_err(|e| anyhow!("opening HFS: {e}"))?;
    let parent = resolve_path(&mut fs, &parent_path)?;
    let children = fs
        .list_directory(&parent)
        .map_err(|e| anyhow!("list_directory: {e}"))?;
    let entry = children
        .into_iter()
        .find(|c| c.name == name)
        .ok_or_else(|| anyhow!("not found: {mac_path}"))?;
    fs.delete_entry(&parent, &entry)
        .map_err(|e| anyhow!("delete_entry: {e}"))?;
    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// put-boot / validate
// ---------------------------------------------------------------------------

pub(crate) fn cmd_put_boot(image: PathBuf, bb_file: PathBuf) -> Result<()> {
    let bb = std::fs::read(&bb_file).with_context(|| format!("reading {}", bb_file.display()))?;
    if bb.len() > 1024 {
        bail!(
            "boot block source is {} bytes; HFS boot region is 1024 bytes max",
            bb.len()
        );
    }
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open(&image)
        .with_context(|| format!("opening {} for write", image.display()))?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&bb)?;
    file.flush()?;
    Ok(())
}

pub(crate) fn cmd_validate(image: PathBuf, partition: Option<u32>) -> Result<()> {
    let mut file = open_image_ro(&image)?;
    let (offset, label) = resolve_hfs_offset(&mut file, partition)?;
    if let Some(l) = &label {
        eprintln!("{l}");
    }
    validate_hfs_integrity(&mut file, offset, &mut |line| println!("{line}"))
        .map_err(|e| anyhow!("{e}"))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

pub fn resolve_path<R: Read + Seek + Send>(
    fs: &mut HfsFilesystem<R>,
    path: &str,
) -> Result<crate::fs::entry::FileEntry> {
    let mut current = fs.root().map_err(|e| anyhow!("root: {e}"))?;
    let trimmed = path.trim_start_matches('/').trim_end_matches('/');
    if trimmed.is_empty() {
        return Ok(current);
    }
    for component in trimmed.split('/') {
        if component.is_empty() {
            continue;
        }
        let children = fs
            .list_directory(&current)
            .map_err(|e| anyhow!("list_directory: {e}"))?;
        let next = children
            .into_iter()
            .find(|c| c.name == component)
            .ok_or_else(|| anyhow!("path component not found: {component}"))?;
        current = next;
    }
    Ok(current)
}
