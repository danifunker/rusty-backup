//! Command-line surface for rusty-backup.
//!
//! Everything currently lives under an `api` top-level namespace that is
//! explicitly unstable: it exists so scripted consumers (build pipelines,
//! automated tests) have *something* to call today, without committing to
//! the eventual GUI-mirroring grammar (`inspect`, `backup`, `restore`,
//! `optical`, `browse`, `shell`, ...). Expect verbs under `api` to be
//! renamed or moved when that final structure lands.
//!
//! Mac paths on the CLI use `/` as the separator. `/` is illegal in HFS
//! filenames so there's no ambiguity. The HFS native separator `:` is
//! not accepted to keep the surface small and shell-friendly.

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
use crate::fs::hfs::{create_blank_hfs_sized, validate_hfs_integrity, HfsFilesystem};

#[derive(Parser, Debug)]
#[command(
    name = "rusty-backup-cli",
    about = "Headless image-construction CLI for rusty-backup",
    disable_help_subcommand = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Unstable scratch namespace for low-level operations. Grammar inside
    /// `api` is expected to churn — do not depend on it from durable scripts.
    Api {
        #[command(subcommand)]
        group: ApiGroup,
    },
}

#[derive(Subcommand, Debug)]
pub enum ApiGroup {
    /// Classic-HFS image operations (create, browse, edit single-partition .dsk images).
    Hfs {
        #[command(subcommand)]
        verb: HfsCommand,
    },
    /// SGI/IRIX disk operations.
    Sgi {
        #[command(subcommand)]
        verb: SgiCommand,
    },
}

#[derive(Subcommand, Debug)]
pub enum SgiCommand {
    /// Re-encode an IRIX disk image into a CHD whose logical size matches
    /// the SGI volume header's used floor. Drops trailing zero padding
    /// past `max(first + blocks)` over all non-empty partition entries.
    /// Accepts a raw `.img` or an existing `.chd` as input; always writes
    /// a CHD. Refuses to overwrite the source or an existing output file.
    Shrink {
        /// Source image (raw `.img` or `.chd`). Must contain an SGI
        /// volume header at sector 0.
        input: PathBuf,
        /// Destination CHD path. Must end in `.chd`, must not already
        /// exist, and must not resolve to the same file as `input`.
        output: PathBuf,
    },
}

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
    Info { image: PathBuf },
    /// List a directory inside the HFS volume.
    Ls {
        image: PathBuf,
        /// Mac path (use `/` separators). Defaults to root.
        #[arg(default_value = "/")]
        path: String,
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
    },
    /// Extract an HFS file to the host.
    Get {
        image: PathBuf,
        mac_path: String,
        host_file: PathBuf,
    },
    /// Delete a file from the HFS volume.
    Rm { image: PathBuf, mac_path: String },
    /// Overwrite the 1024-byte boot block region at offset 0. The source
    /// must be at most 1024 bytes and is written verbatim — no padding,
    /// no HFS B-tree touch.
    PutBoot { image: PathBuf, bb_file: PathBuf },
    /// Run the lightweight HFS integrity check on the image.
    Validate { image: PathBuf },
}

/// Run the parsed CLI and exit. Returns only when nothing matched (which
/// can't happen with the current grammar) so callers can fall through to
/// the GUI launcher.
pub fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Api { group } => match group {
            ApiGroup::Hfs { verb } => run_hfs(verb),
            ApiGroup::Sgi { verb } => run_sgi(verb),
        },
    }
}

fn run_hfs(verb: HfsCommand) -> Result<()> {
    match verb {
        HfsCommand::New {
            image,
            size,
            name,
            block_size,
        } => cmd_new(image, &size, &name, block_size),
        HfsCommand::Info { image } => cmd_info(image),
        HfsCommand::Ls { image, path } => cmd_ls(image, &path),
        HfsCommand::Put {
            image,
            host_file,
            mac_path,
            type_code,
            creator,
            force,
        } => cmd_put(
            image,
            Some(host_file),
            &mac_path,
            &type_code,
            &creator,
            None,
            force,
        ),
        HfsCommand::PutZero {
            image,
            mac_path,
            size,
            type_code,
            creator,
            force,
        } => cmd_put(
            image,
            None,
            &mac_path,
            &type_code,
            &creator,
            Some(size),
            force,
        ),
        HfsCommand::Get {
            image,
            mac_path,
            host_file,
        } => cmd_get(image, &mac_path, host_file),
        HfsCommand::Rm { image, mac_path } => cmd_rm(image, &mac_path),
        HfsCommand::PutBoot { image, bb_file } => cmd_put_boot(image, bb_file),
        HfsCommand::Validate { image } => cmd_validate(image),
    }
}

// ---------------------------------------------------------------------------
// new
// ---------------------------------------------------------------------------

fn cmd_new(image: PathBuf, size_arg: &str, name: &str, block_size: Option<u32>) -> Result<()> {
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

    let bytes = create_blank_hfs_sized(size, block_size, name, 0, 0)
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

/// Smallest 512-byte-multiple block size that keeps total_blocks <= 65535.
fn pick_block_size(volume_bytes: u64) -> u32 {
    let mut bs: u32 = 512;
    while volume_bytes / bs as u64 > 65535 {
        bs = bs.saturating_mul(2);
        if bs == 0 {
            return 1 << 16;
        }
    }
    bs
}

fn parse_size(s: &str) -> Result<u64> {
    let s = s.trim();
    if s.is_empty() {
        bail!("empty size");
    }
    let (num_part, mult): (&str, u64) =
        if let Some(rest) = s.strip_suffix("KiB").or_else(|| s.strip_suffix('K')) {
            (rest, 1024)
        } else if let Some(rest) = s.strip_suffix("MiB").or_else(|| s.strip_suffix('M')) {
            (rest, 1024 * 1024)
        } else if let Some(rest) = s.strip_suffix("GiB").or_else(|| s.strip_suffix('G')) {
            (rest, 1024 * 1024 * 1024)
        } else if let Some(rest) = s.strip_suffix('B') {
            (rest, 1)
        } else {
            (s, 1)
        };
    let n: u64 = num_part
        .trim()
        .parse()
        .map_err(|_| anyhow!("invalid size {s:?}"))?;
    n.checked_mul(mult)
        .ok_or_else(|| anyhow!("size {s:?} overflows u64"))
}

// ---------------------------------------------------------------------------
// info / ls
// ---------------------------------------------------------------------------

fn cmd_info(image: PathBuf) -> Result<()> {
    let file = open_image_ro(&image)?;
    let fs = HfsFilesystem::open(file, 0).map_err(|e| anyhow!("opening HFS: {e}"))?;
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

fn cmd_ls(image: PathBuf, path: &str) -> Result<()> {
    let file = open_image_ro(&image)?;
    let mut fs = HfsFilesystem::open(file, 0).map_err(|e| anyhow!("opening HFS: {e}"))?;
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

fn cmd_put(
    image: PathBuf,
    host_file: Option<PathBuf>,
    mac_path: &str,
    type_code: &str,
    creator: &str,
    zero_pad: Option<u64>,
    force: bool,
) -> Result<()> {
    let (parent_path, name) = split_mac_path(mac_path)?;
    if name.is_empty() {
        bail!("destination path has no filename");
    }

    let file = open_image_rw(&image)?;
    let mut fs = HfsFilesystem::open(file, 0).map_err(|e| anyhow!("opening HFS: {e}"))?;

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

fn cmd_get(image: PathBuf, mac_path: &str, host_file: PathBuf) -> Result<()> {
    let file = open_image_ro(&image)?;
    let mut fs = HfsFilesystem::open(file, 0).map_err(|e| anyhow!("opening HFS: {e}"))?;
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

fn cmd_rm(image: PathBuf, mac_path: &str) -> Result<()> {
    let (parent_path, name) = split_mac_path(mac_path)?;
    if name.is_empty() {
        bail!("path has no filename");
    }
    let file = open_image_rw(&image)?;
    let mut fs = HfsFilesystem::open(file, 0).map_err(|e| anyhow!("opening HFS: {e}"))?;
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

fn cmd_put_boot(image: PathBuf, bb_file: PathBuf) -> Result<()> {
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

fn cmd_validate(image: PathBuf) -> Result<()> {
    let mut file = open_image_ro(&image)?;
    validate_hfs_integrity(&mut file, 0, &mut |line| println!("{line}"))
        .map_err(|e| anyhow!("{e}"))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// sgi
// ---------------------------------------------------------------------------

fn run_sgi(verb: SgiCommand) -> Result<()> {
    match verb {
        SgiCommand::Shrink { input, output } => cmd_sgi_shrink(input, output),
    }
}

fn cmd_sgi_shrink(input: PathBuf, output: PathBuf) -> Result<()> {
    use std::sync::atomic::AtomicBool;

    let cancel = AtomicBool::new(false);
    let mut last_mib: u64 = u64::MAX;
    let mut progress_cb = |bytes: u64| {
        let mib = bytes / (1024 * 1024);
        if mib != last_mib {
            eprint!("\rprogress: {mib} MiB written  ");
            let _ = std::io::stderr().flush();
            last_mib = mib;
        }
    };
    let cancel_check = || cancel.load(std::sync::atomic::Ordering::Relaxed);
    let mut log_cb = |msg: &str| {
        eprintln!("\n  {msg}");
    };

    let report = crate::rbformats::chd::shrink_sgi_disk_to_chd(
        &input,
        &output,
        &mut progress_cb,
        &cancel_check,
        &mut log_cb,
    )?;

    eprintln!();
    println!("Shrunk {} -> {}", input.display(), output.display());
    println!(
        "  source logical: {} bytes ({:.3} GiB)",
        report.source_logical_size,
        report.source_logical_size as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    println!(
        "  new logical:    {} bytes ({:.3} GiB)",
        report.new_logical_size,
        report.new_logical_size as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    println!(
        "  floor:          sector {} (max first+blocks across SGI partitions)",
        report.partition_floor_sectors
    );
    println!("  dropped:        {} bytes", report.bytes_dropped());
    Ok(())
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn open_image_ro(path: &PathBuf) -> Result<std::fs::File> {
    std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))
}

fn open_image_rw(path: &PathBuf) -> Result<std::fs::File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("opening {} for read/write", path.display()))
}

fn split_mac_path(p: &str) -> Result<(String, String)> {
    let normalized = p.trim_end_matches('/');
    if normalized.is_empty() || normalized == "/" {
        return Ok(("/".into(), String::new()));
    }
    let (parent, name) = match normalized.rsplit_once('/') {
        Some((par, n)) => (par, n),
        None => ("", normalized),
    };
    let parent = if parent.is_empty() {
        "/".into()
    } else {
        parent.to_string()
    };
    Ok((parent, name.to_string()))
}

fn resolve_path<R: Read + Seek + Send>(
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

/// Adapter that pretends to be a file of `remaining` zero bytes.
struct ZeroReader {
    remaining: u64,
}

impl Read for ZeroReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.remaining == 0 {
            return Ok(0);
        }
        let n = buf.len().min(self.remaining as usize);
        for b in &mut buf[..n] {
            *b = 0;
        }
        self.remaining -= n as u64;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_size_bytes() {
        assert_eq!(parse_size("819200").unwrap(), 819200);
        assert_eq!(parse_size("800K").unwrap(), 819200);
        assert_eq!(parse_size("800KiB").unwrap(), 819200);
        assert_eq!(parse_size("5M").unwrap(), 5 * 1024 * 1024);
        assert_eq!(parse_size("1G").unwrap(), 1024 * 1024 * 1024);
        assert!(parse_size("").is_err());
        assert!(parse_size("nope").is_err());
    }

    #[test]
    fn split_mac_path_cases() {
        assert_eq!(split_mac_path("/foo").unwrap(), ("/".into(), "foo".into()));
        assert_eq!(
            split_mac_path("/foo/bar.txt").unwrap(),
            ("/foo".into(), "bar.txt".into())
        );
        assert_eq!(split_mac_path("/").unwrap(), ("/".into(), String::new()));
    }

    #[test]
    fn pick_block_size_floppy_and_large() {
        assert_eq!(pick_block_size(819_200), 512);
        // 60 MiB needs > 512: 60M/512 = 122880 > 65535, so bs jumps to 1024.
        assert_eq!(pick_block_size(60 * 1024 * 1024), 1024);
    }
}
