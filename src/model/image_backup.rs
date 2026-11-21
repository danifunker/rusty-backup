//! A disk image read as a backup, so the Restore engine can write it with resized partitions.
//!
//! [`virtualize`] writes a folder with the files a single-file backup carries: `metadata.json`
//! (`layout: single-file-chd`), the parsed partition-table sidecar and nothing else. Its
//! `container` names the image itself (an absolute path, so `folder.join` keeps it), and a wrapped
//! image (`.img.xz`, VHD, ...) is decoded once to `disk.img` in the folder. Restore then reads the
//! partitions in place through `source_reader::open_disk_container`, which needs no `chd`.
//!
//! The "Minimum" each partition offers is its filesystem's last used byte: the data stays where it
//! is, so a shrink may only trim the free tail. MBR (primary partitions), GPT and APM disks are
//! offered; the label schemes and MBR logical partitions are refused, and write as-is instead.
//!
//! Windows' swap and hibernation files ([`SWAP_FILE_NAMES`]) are rebuilt at boot, and one at the
//! end of a volume pins its minimum there. [`virtualize`] lists any in a partition's root in
//! `swap-files.json`; [`drop_swap_files`] deletes them, once the user agrees, from the folder's own
//! copy of the disk (a flat image is copied in first, so the user's file is never written).

use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

use crate::backup::metadata::{AlignmentMetadata, BackupLayout, BackupMetadata, PartitionMetadata};
use crate::partition::{detect_alignment, PartitionTable};

/// The container file a virtual backup keeps beside its metadata when the image needed decoding.
pub const DECODED_NAME: &str = "disk.img";

/// The sidecar listing the swap files found in the image's partitions.
pub const SWAP_FILES_NAME: &str = "swap-files.json";

/// Root-level files Windows rebuilds at boot: NT's pagefile and hibernation file, the Windows 8+
/// swapfile, Windows 9x's and Windows 3.x's swap files.
pub const SWAP_FILE_NAMES: &[&str] = &[
    "PAGEFILE.SYS",
    "HIBERFIL.SYS",
    "SWAPFILE.SYS",
    "WIN386.SWP",
    "386SPART.PAR",
];

/// A swap file in one of the image's partitions.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SwapFile {
    /// The partition's `index` in the metadata.
    pub partition: usize,
    pub name: String,
    pub size: u64,
}

/// True for a file (not a folder, not a `.cbk`) that [`virtualize`] may take.
pub fn is_image_file(path: &Path) -> bool {
    path.is_file()
        && !crate::rbformats::cbk::is_cbk(path)
        && !path
            .file_name()
            .is_some_and(|n| n.eq_ignore_ascii_case("metadata.json"))
}

/// Write `folder` so it loads as a single-file backup of `image`; returns the container path.
pub fn virtualize(image: &Path, folder: &Path, log: &mut dyn FnMut(String)) -> Result<PathBuf> {
    std::fs::create_dir_all(folder).with_context(|| format!("creating {}", folder.display()))?;
    let container = container_for(image, folder, log)?;
    let file =
        File::open(&container).with_context(|| format!("opening {}", container.display()))?;
    let disk_size = file.metadata().map(|m| m.len()).unwrap_or(0);
    let table = PartitionTable::detect(&mut BufReader::new(&file))
        .with_context(|| format!("reading the partition table of {}", image.display()))?;
    match &table {
        PartitionTable::Mbr(mbr) => crate::backup::format::export_mbr_json(mbr, folder)?,
        PartitionTable::Gpt { gpt, .. } => sidecar(folder, "gpt.json", gpt)?,
        PartitionTable::Apm(apm) => sidecar(folder, "apm.json", apm)?,
        PartitionTable::None { .. } => {}
        other => bail!(
            "resizing on write handles MBR, GPT and APM disks, and this one has {}; \
             write it as-is instead",
            other.type_name()
        ),
    }
    let parts = table.partitions();
    if parts.iter().any(|p| p.is_logical) {
        bail!(
            "this MBR disk has logical partitions, which resizing on write does not move yet; \
             write it as-is instead"
        );
    }
    let alignment = detect_alignment(&table);
    let swap: Vec<SwapFile> = parts
        .iter()
        .filter(|p| !p.is_extended_container)
        .flat_map(|p| find_swap_files(&file, p))
        .collect();
    for s in &swap {
        log(format!(
            "Partition {}: {} ({} bytes) is rebuilt at boot",
            s.partition, s.name, s.size
        ));
    }
    if !swap.is_empty() {
        sidecar(folder, SWAP_FILES_NAME, &swap)?;
    }
    let partitions = parts
        .iter()
        .filter(|p| !p.is_extended_container)
        .map(|p| {
            let offset = p.start_lba * 512;
            let clone = || file.try_clone().map(BufReader::new);
            let minimum = clone().ok().and_then(|r| {
                crate::fs::effective_partition_size_reported(
                    r,
                    offset,
                    p.partition_type_byte,
                    p.partition_type_string.as_deref(),
                )
                .ok()
                .map(|m| m.min(p.size_bytes))
            });
            let hfsplus_signature = clone()
                .ok()
                .and_then(|mut r| crate::fs::probe_hfsplus_signature(&mut r, offset));
            PartitionMetadata {
                index: p.index,
                type_name: p.type_name.clone(),
                partition_type_byte: p.partition_type_byte,
                start_lba: p.start_lba,
                start_byte: None,
                original_size_bytes: p.size_bytes,
                imaged_size_bytes: p.size_bytes,
                compressed_files: vec![],
                checksum: String::new(),
                resized: false,
                compacted: false,
                is_logical: false,
                partition_type_string: p.partition_type_string.clone(),
                minimum_size_bytes: minimum,
                defragmented_min_size_bytes: None,
                hfsplus_signature,
                defragmented_clone: false,
            }
        })
        .collect::<Vec<_>>();
    for p in &partitions {
        log(format!(
            "Partition {}: {} ({} bytes), minimum {}",
            p.index,
            p.type_name,
            p.original_size_bytes,
            p.minimum_size_bytes
                .map_or("unknown".to_string(), |m| format!("{m} bytes")),
        ));
    }
    let metadata = BackupMetadata {
        version: 1,
        created: chrono::Utc::now().to_rfc3339(),
        source_device: image.display().to_string(),
        source_size_bytes: disk_size,
        partition_table_type: table.type_name().to_string(),
        checksum_type: "none".to_string(),
        compression_type: "none".to_string(),
        split_size_mib: None,
        sector_by_sector: true,
        layout: BackupLayout::SingleFileChd,
        container: Some(container.display().to_string()),
        container_logical_size: Some(disk_size),
        container_sha1: None,
        size_policy: None,
        alignment: AlignmentMetadata {
            detected_type: format!("{}", alignment.alignment_type),
            first_partition_lba: alignment.first_lba,
            alignment_sectors: alignment.alignment_sectors,
            heads: alignment.heads,
            sectors_per_track: alignment.sectors_per_track,
        },
        partitions,
        bad_sectors: vec![],
        extended_container: None,
    };
    sidecar(folder, "metadata.json", &metadata)?;
    Ok(container)
}

/// The swap files in `p`'s root directory; none when it does not open.
fn find_swap_files(file: &File, p: &crate::partition::PartitionInfo) -> Vec<SwapFile> {
    let Ok(clone) = file.try_clone() else {
        return Vec::new();
    };
    let Ok(mut fs) = crate::fs::open_filesystem(
        BufReader::new(clone),
        p.start_lba * 512,
        p.partition_type_byte,
        p.partition_type_string.as_deref(),
    ) else {
        return Vec::new();
    };
    let Ok(root) = fs.root() else {
        return Vec::new();
    };
    fs.list_directory(&root)
        .unwrap_or_default()
        .into_iter()
        .filter(|e| {
            e.is_file()
                && SWAP_FILE_NAMES
                    .iter()
                    .any(|n| n.eq_ignore_ascii_case(&e.name))
        })
        .map(|e| SwapFile {
            partition: p.index,
            name: e.name,
            size: e.size,
        })
        .collect()
}

/// The swap files [`virtualize`] found in the image at `folder`; none for any other backup.
pub fn swap_files(folder: &Path) -> Vec<SwapFile> {
    std::fs::read(folder.join(SWAP_FILES_NAME))
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default()
}

/// Delete the swap files from the folder's copy of the disk and recompute each partition's
/// minimum. A flat image referenced in place is copied into the folder first.
pub fn drop_swap_files(folder: &Path, log: &mut dyn FnMut(String)) -> Result<()> {
    let swap = swap_files(folder);
    if swap.is_empty() {
        return Ok(());
    }
    let meta_path = folder.join("metadata.json");
    let mut metadata: BackupMetadata = serde_json::from_slice(
        &std::fs::read(&meta_path).with_context(|| format!("reading {}", meta_path.display()))?,
    )
    .context("parsing metadata.json")?;
    let container = folder.join(metadata.container.as_deref().unwrap_or(DECODED_NAME));
    let own = folder.join(DECODED_NAME);
    if container != own {
        log(format!(
            "Copying {} to a temporary file so its swap files can be dropped",
            container.display()
        ));
        std::fs::copy(&container, &own)
            .with_context(|| format!("copying {}", container.display()))?;
        metadata.container = Some(own.display().to_string());
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&own)
        .with_context(|| format!("opening {}", own.display()))?;
    for pm in metadata.partitions.iter_mut() {
        let names: Vec<&SwapFile> = swap.iter().filter(|s| s.partition == pm.index).collect();
        if names.is_empty() {
            continue;
        }
        let offset = pm.start_lba * 512;
        let ctx = crate::fs::EditContext {
            partition_len: Some(pm.original_size_bytes),
            ..Default::default()
        };
        let mut efs = crate::fs::open_editable_filesystem_with(
            file.try_clone()?,
            offset,
            ctx,
            pm.partition_type_byte,
            pm.partition_type_string.as_deref(),
        )
        .map_err(|e| anyhow::anyhow!("partition {}: opening it to edit: {e}", pm.index))?;
        let root = efs
            .as_filesystem_mut()
            .root()
            .map_err(|e| anyhow::anyhow!("partition {}: reading its root: {e}", pm.index))?;
        let entries = efs
            .as_filesystem_mut()
            .list_directory(&root)
            .map_err(|e| anyhow::anyhow!("partition {}: listing its root: {e}", pm.index))?;
        for s in names {
            let Some(entry) = entries
                .iter()
                .find(|e| e.name.eq_ignore_ascii_case(&s.name))
            else {
                continue;
            };
            efs.delete_entry(&root, entry)
                .map_err(|e| anyhow::anyhow!("partition {}: deleting {}: {e}", pm.index, s.name))?;
            log(format!(
                "Partition {}: dropped {} ({} bytes)",
                pm.index, s.name, s.size
            ));
        }
        efs.sync_metadata()
            .map_err(|e| anyhow::anyhow!("partition {}: saving the deletion: {e}", pm.index))?;
        drop(efs);
        pm.minimum_size_bytes = crate::fs::effective_partition_size_reported(
            BufReader::new(file.try_clone()?),
            offset,
            pm.partition_type_byte,
            pm.partition_type_string.as_deref(),
        )
        .ok()
        .map(|m| m.min(pm.original_size_bytes));
        log(format!(
            "Partition {}: minimum now {}",
            pm.index,
            pm.minimum_size_bytes
                .map_or("unknown".to_string(), |m| format!("{m} bytes"))
        ));
    }
    file.sync_all().context("syncing the copy")?;
    sidecar(folder, "metadata.json", &metadata)?;
    std::fs::remove_file(folder.join(SWAP_FILES_NAME)).context("clearing the swap-file list")?;
    Ok(())
}

fn sidecar<T: serde::Serialize>(folder: &Path, name: &str, value: &T) -> Result<()> {
    let json = serde_json::to_string_pretty(value).with_context(|| format!("encoding {name}"))?;
    std::fs::write(folder.join(name), json).with_context(|| format!("writing {name}"))
}

/// The image itself when it is flat; otherwise its disk decoded once into `folder`.
fn container_for(image: &Path, folder: &Path, log: &mut dyn FnMut(String)) -> Result<PathBuf> {
    let image =
        std::fs::canonicalize(image).with_context(|| format!("finding {}", image.display()))?;
    let Some(kind) = crate::backup::container_needing_decode(&image) else {
        return Ok(image);
    };
    let out = folder.join(DECODED_NAME);
    log(format!(
        "Decoding the {kind} image to {} so its partitions can be resized",
        out.display()
    ));
    let mut w =
        BufWriter::new(File::create(&out).with_context(|| format!("creating {}", out.display()))?);
    if crate::rbformats::xz::is_xz_path(&image) {
        crate::rbformats::xz::decode_into(&image, &mut w)
            .with_context(|| format!("decoding {}", image.display()))?;
    } else {
        let (mut r, _) = crate::model::source_reader::open_disk_container(&image)?;
        std::io::copy(&mut r, &mut w).with_context(|| format!("decoding {}", image.display()))?;
    }
    w.flush().context("flushing the decoded image")?;
    Ok(out)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A 32 MiB MBR disk with a FAT16 partition at 1 MiB holding one file near its start.
    pub(crate) fn fat_disk(dir: &Path) -> PathBuf {
        use crate::model::format::{format_into, FsKind, Window};
        const MIB: u64 = 1024 * 1024;
        let path = dir.join("card.img");
        let mut f = File::create(&path).unwrap();
        f.set_len(32 * MIB).unwrap();
        let mut mbr = [0u8; 512];
        mbr[446 + 4] = 0x06;
        mbr[446 + 8..446 + 12].copy_from_slice(&2048u32.to_le_bytes());
        mbr[446 + 12..446 + 16].copy_from_slice(&((24 * MIB / 512) as u32).to_le_bytes());
        mbr[510] = 0x55;
        mbr[511] = 0xAA;
        f.write_all(&mbr).unwrap();
        let opts = crate::model::disk_builder::RowFormat::new(FsKind::Fat).opts;
        let mut w = Window::new(&mut f, MIB, 24 * MIB);
        format_into(&mut w, 24 * MIB, FsKind::Fat, &opts).unwrap();
        drop(f);
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut efs = crate::fs::open_editable_filesystem(f, MIB, 0x06, None).unwrap();
        let root = efs.as_filesystem_mut().root().unwrap();
        let data = vec![0x42u8; 20_000];
        efs.create_file(
            &root,
            "HELLO.TXT",
            &mut std::io::Cursor::new(&data),
            data.len() as u64,
            &Default::default(),
        )
        .unwrap();
        efs.sync_metadata().unwrap();
        path
    }

    /// Restore the virtual backup of `image` onto a `target_size` file with `choice` for partition 0.
    fn restore(
        image: &Path,
        target_size: u64,
        choice: crate::restore::RestoreSizeChoice,
    ) -> PathBuf {
        use crate::restore::{
            RestoreAlignment, RestoreConfig, RestorePartitionSize, RestoreProgress,
        };
        let dir = image.parent().unwrap();
        let loaded = crate::model::restore_plan::load_backup(image).expect("loads as a backup");
        let target = dir.join(format!("target-{target_size}.img"));
        let cfg = RestoreConfig {
            backup_folder: loaded.folder.clone(),
            target_path: target.clone(),
            target_is_device: false,
            target_size,
            alignment: RestoreAlignment::Original,
            partition_sizes: vec![RestorePartitionSize {
                index: 0,
                size_choice: choice,
            }],
            write_zeros_to_unused: false,
            install_mac_driver: true,
        };
        let progress = std::sync::Arc::new(std::sync::Mutex::new(RestoreProgress::new()));
        crate::restore::run_restore(cfg, progress).expect("restore");
        drop(loaded);
        target
    }

    /// Restore an already-virtualized `folder` as-is onto a file of `size` bytes.
    fn restore_folder(folder: &Path, size: u64) -> PathBuf {
        use crate::restore::{RestoreAlignment, RestoreConfig, RestoreProgress};
        let target = folder.parent().unwrap().join("from-folder.img");
        let cfg = RestoreConfig {
            backup_folder: folder.to_path_buf(),
            target_path: target.clone(),
            target_is_device: false,
            target_size: size,
            alignment: RestoreAlignment::Original,
            partition_sizes: vec![],
            write_zeros_to_unused: false,
            install_mac_driver: true,
        };
        let progress = std::sync::Arc::new(std::sync::Mutex::new(RestoreProgress::new()));
        crate::restore::run_restore(cfg, progress).expect("restore");
        target
    }

    /// The partition's size and HELLO.TXT read back from a restored disk.
    fn read_back(disk: &Path) -> (u64, Vec<u8>) {
        let mut f = File::open(disk).unwrap();
        let table = PartitionTable::detect(&mut BufReader::new(&mut f)).unwrap();
        let p = table.partitions()[0].clone();
        let mut fs = crate::fs::open_filesystem(f, p.start_lba * 512, 0, None).unwrap();
        let root = fs.root().unwrap();
        let hello = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "HELLO.TXT");
        (
            p.size_bytes,
            fs.read_file(&hello.expect("HELLO.TXT"), usize::MAX)
                .unwrap(),
        )
    }

    #[test]
    fn an_image_grows_onto_a_bigger_card_and_shrinks_onto_a_smaller_one() {
        use crate::restore::RestoreSizeChoice;
        const MIB: u64 = 1024 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let image = fat_disk(dir.path());

        let grown = restore(&image, 64 * MIB, RestoreSizeChoice::FillRemaining);
        let (size, hello) = read_back(&grown);
        assert!(size > 60 * MIB, "grew to {size}");
        assert_eq!(hello, vec![0x42u8; 20_000]);

        // The engine puts this FAT16's floor near 16 MiB of its 24, so 12 MiB is refused.
        let shrunk = restore(&image, 20 * MIB, RestoreSizeChoice::Minimum);
        assert!(std::fs::metadata(&shrunk).unwrap().len() <= 20 * MIB);
        let (size, hello) = read_back(&shrunk);
        assert!(size < 20 * MIB, "shrank to {size}");
        assert_eq!(hello, vec![0x42u8; 20_000]);
    }

    #[test]
    fn an_xz_image_restores_grown() {
        use crate::restore::RestoreSizeChoice;
        const MIB: u64 = 1024 * 1024;
        let dir = tempfile::tempdir().unwrap();
        let image = fat_disk(dir.path());
        let mut packed = Vec::new();
        lzma_rs::xz_compress(
            &mut std::io::Cursor::new(std::fs::read(&image).unwrap()),
            &mut packed,
        )
        .unwrap();
        let xz = dir.path().join("card.img.xz");
        std::fs::write(&xz, packed).unwrap();
        let (size, hello) = read_back(&restore(&xz, 48 * MIB, RestoreSizeChoice::FillRemaining));
        assert!(size > 40 * MIB, "grew to {size}");
        assert_eq!(hello, vec![0x42u8; 20_000]);
    }

    /// [`fat_disk`] with a 20 MiB PAGEFILE.SYS after HELLO.TXT, pinning the minimum past it.
    pub(crate) fn fat_disk_with_pagefile(dir: &Path) -> PathBuf {
        let image = fat_disk(dir);
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&image)
            .unwrap();
        let mut efs = crate::fs::open_editable_filesystem(f, 1024 * 1024, 0x06, None).unwrap();
        let root = efs.as_filesystem_mut().root().unwrap();
        let swap = vec![0x77u8; 20 * 1024 * 1024];
        let (len, opts) = (swap.len() as u64, Default::default());
        efs.create_file(
            &root,
            "PAGEFILE.SYS",
            &mut std::io::Cursor::new(&swap),
            len,
            &opts,
        )
        .unwrap();
        efs.sync_metadata().unwrap();
        image
    }

    #[test]
    fn a_pagefile_is_listed_and_dropping_it_lowers_the_minimum_on_a_copy() {
        let dir = tempfile::tempdir().unwrap();
        let image = fat_disk_with_pagefile(dir.path());
        let before = std::fs::read(&image).unwrap();
        let folder = dir.path().join("virtual");
        virtualize(&image, &folder, &mut |_| {}).unwrap();
        let listed = swap_files(&folder);
        assert_eq!(listed.len(), 1, "{listed:?}");
        assert_eq!(listed[0].name.to_ascii_uppercase(), "PAGEFILE.SYS");
        let min = |folder: &Path| -> u64 {
            let m: BackupMetadata =
                serde_json::from_slice(&std::fs::read(folder.join("metadata.json")).unwrap())
                    .unwrap();
            m.partitions[0].minimum_size_bytes.unwrap()
        };
        let pinned = min(&folder);
        assert!(pinned > 20 * 1024 * 1024, "{pinned}");

        drop_swap_files(&folder, &mut |_| {}).unwrap();
        let freed = min(&folder);
        // Down to this FAT16's floor (about 16 MiB), the size its cluster count needs.
        assert!(freed < pinned - 3 * 1024 * 1024, "{freed} vs {pinned}");
        assert!(swap_files(&folder).is_empty());
        assert_eq!(
            std::fs::read(&image).unwrap(),
            before,
            "the user's image is never written"
        );
        let (size, hello) = read_back(&restore_folder(&folder, 32 * 1024 * 1024));
        assert!(size > 0);
        assert_eq!(hello, vec![0x42u8; 20_000]);
    }

    #[test]
    fn a_flat_image_is_referenced_in_place_with_a_minimum() {
        let dir = tempfile::tempdir().unwrap();
        let image = fat_disk(dir.path());
        let folder = dir.path().join("virtual");
        let container = virtualize(&image, &folder, &mut |_| {}).unwrap();
        assert_eq!(container, std::fs::canonicalize(&image).unwrap());
        assert!(!folder.join(DECODED_NAME).exists());
        assert!(folder.join("mbr.json").exists());
        let m: BackupMetadata =
            serde_json::from_str(&std::fs::read_to_string(folder.join("metadata.json")).unwrap())
                .unwrap();
        assert_eq!(m.partition_table_type, "MBR");
        let p = &m.partitions[0];
        assert_eq!(
            (p.start_lba, p.original_size_bytes),
            (2048, 24 * 1024 * 1024)
        );
        let min = p.minimum_size_bytes.expect("FAT has a trim point");
        assert!(min > 0 && min < p.original_size_bytes, "{min}");
    }

    #[test]
    fn an_xz_image_is_decoded_once_into_the_folder() {
        let dir = tempfile::tempdir().unwrap();
        let image = fat_disk(dir.path());
        let raw = std::fs::read(&image).unwrap();
        let mut packed = Vec::new();
        lzma_rs::xz_compress(&mut std::io::Cursor::new(&raw), &mut packed).unwrap();
        let xz = dir.path().join("card.img.xz");
        std::fs::write(&xz, packed).unwrap();
        let folder = dir.path().join("virtual");
        let container = virtualize(&xz, &folder, &mut |_| {}).unwrap();
        assert_eq!(container, folder.join(DECODED_NAME));
        assert_eq!(std::fs::read(&container).unwrap(), raw);
    }
}
