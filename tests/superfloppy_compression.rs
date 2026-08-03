//! Superfloppy (partition-table-less) backups honour `--format`.
//!
//! Backup used to force `CompressionType::None` for a superfloppy *and* record
//! `compression_type: "none"` in metadata regardless of the requested codec.
//! When the force was lifted the metadata still lied, so restore fed the
//! compressed bytes to the target verbatim — a tiny "backup" that restored to
//! garbage ("Bad magic number in super-block").
//!
//! The invariant these tests pin: whatever codec is chosen, the restored image
//! is byte-identical to what the raw path produces, and metadata names the
//! codec that was actually written.

use std::sync::{Arc, Mutex};

use rusty_backup::backup::{
    run_backup, BackupConfig, BackupProgress, ChecksumType, CompressionType,
};
use rusty_backup::restore::{RestoreAlignment, RestoreConfig, RestoreProgress};

fn backup_config(
    src: &std::path::Path,
    dest: &std::path::Path,
    name: &str,
    compression: CompressionType,
) -> BackupConfig {
    BackupConfig {
        source_path: src.to_path_buf(),
        destination_dir: dest.to_path_buf(),
        backup_name: name.to_string(),
        compression,
        checksum: ChecksumType::Crc32,
        split_size_mib: None,
        sector_by_sector: false,
        partition_filter: None,
        chd_options: None,
        size_policy: None,
        partition_target_sizes: None,
        shrink_to_minimum: false,
        precomputed_minimum_sizes: None,
        defrag_partition_indices: None,
        defrag_fat: false,
        keep_swap: false,
    }
}

/// Back up `src` with `compression`, restore it, and return
/// (restored bytes, metadata compression_type, data file names).
fn round_trip(
    work: &std::path::Path,
    src: &std::path::Path,
    tag: &str,
    compression: CompressionType,
) -> (Vec<u8>, String, Vec<String>) {
    let backups = work.join(format!("backups-{tag}"));
    std::fs::create_dir_all(&backups).unwrap();
    run_backup(
        backup_config(src, &backups, tag, compression),
        Arc::new(Mutex::new(BackupProgress::default())),
    )
    .unwrap_or_else(|e| panic!("{tag}: backup failed: {e:?}"));

    let folder = backups.join(tag);
    let meta: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(folder.join("metadata.json")).unwrap())
            .unwrap();
    let recorded = meta["compression_type"].as_str().unwrap().to_string();
    let files: Vec<String> = meta["partitions"][0]["compressed_files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();

    // Every file metadata names must actually be on disk — a stale name here
    // is how a "successful" backup turns into an unrestorable one.
    for f in &files {
        assert!(
            folder.join(f).exists(),
            "{tag}: metadata names {f}, which does not exist"
        );
    }

    let target = work.join(format!("restored-{tag}.img"));
    let src_size = std::fs::metadata(src).unwrap().len();
    rusty_backup::restore::run_restore(
        RestoreConfig {
            backup_folder: folder,
            target_path: target.clone(),
            target_is_device: false,
            target_size: src_size,
            alignment: RestoreAlignment::Original,
            partition_sizes: Vec::new(),
            write_zeros_to_unused: false,
        },
        Arc::new(Mutex::new(RestoreProgress::default())),
    )
    .unwrap_or_else(|e| panic!("{tag}: restore failed: {e:?}"));

    (std::fs::read(&target).unwrap(), recorded, files)
}

/// A FAT16 superfloppy: no partition table, one volume filling the image.
fn fat_superfloppy(path: &std::path::Path) {
    let blob = rusty_backup::fs::fat::create_blank_fat(16 * 1024 * 1024, Some("SFTEST")).unwrap();
    std::fs::write(path, &blob).unwrap();

    use rusty_backup::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut fs = rusty_backup::fs::fat::FatFilesystem::open(file, 0).unwrap();
    let root = fs.root().unwrap();
    // Compressible but not all-zero, so a codec difference would show up.
    let payload: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();
    let mut data = &payload[..];
    fs.create_file(
        &root,
        "DOC.BIN",
        &mut data,
        payload.len() as u64,
        &CreateFileOptions::default(),
    )
    .unwrap();
    EditableFilesystem::sync_metadata(&mut fs).unwrap();
}

#[test]
fn superfloppy_honours_every_codec_and_restores_identically() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().to_path_buf();
    let src = work.join("source.img");
    fat_superfloppy(&src);
    let source_bytes = std::fs::read(&src).unwrap();

    // Raw is the reference: it is the shape the superfloppy path always
    // supported, so every codec must reproduce it byte for byte.
    let (raw_out, raw_type, raw_files) = round_trip(&work, &src, "raw", CompressionType::None);
    assert_eq!(raw_type, "none");
    assert!(
        raw_files[0].ends_with(".img"),
        "raw superfloppy keeps the friendly .img extension, got {}",
        raw_files[0]
    );
    assert_eq!(
        raw_out, source_bytes,
        "raw superfloppy restore must be byte-identical to the source"
    );

    for (tag, compression, ext) in [
        ("zstd", CompressionType::Zstd, ".zst"),
        ("gzip", CompressionType::Gzip, ".gz"),
        ("lz4", CompressionType::Lz4, ".lz4"),
    ] {
        let (out, recorded, files) = round_trip(&work, &src, tag, compression);
        assert_eq!(
            recorded,
            compression.as_str(),
            "{tag}: metadata must name the codec actually written"
        );
        assert!(
            files[0].ends_with(ext),
            "{tag}: expected a {ext} member, got {}",
            files[0]
        );
        assert_eq!(
            out, raw_out,
            "{tag}: restored image must match the raw restore byte for byte"
        );
        assert_eq!(out, source_bytes, "{tag}: restore must match the source");
    }
}

/// The compressed member has to be genuinely smaller — otherwise the codec
/// silently degraded to a raw copy and the whole feature is a no-op.
#[test]
fn compressed_superfloppy_member_is_smaller_than_raw() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().to_path_buf();
    let src = work.join("source.img");
    fat_superfloppy(&src);

    let size_of = |tag: &str, compression: CompressionType| -> u64 {
        let backups = work.join(format!("backups-{tag}"));
        std::fs::create_dir_all(&backups).unwrap();
        run_backup(
            backup_config(&src, &backups, tag, compression),
            Arc::new(Mutex::new(BackupProgress::default())),
        )
        .unwrap();
        let folder = backups.join(tag);
        let meta: serde_json::Value =
            serde_json::from_reader(std::fs::File::open(folder.join("metadata.json")).unwrap())
                .unwrap();
        meta["partitions"][0]["compressed_files"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| {
                std::fs::metadata(folder.join(f.as_str().unwrap()))
                    .unwrap()
                    .len()
            })
            .sum()
    };

    let raw = size_of("raw", CompressionType::None);
    let zstd = size_of("zstd", CompressionType::Zstd);
    assert!(
        zstd < raw / 2,
        "zstd superfloppy member ({zstd} bytes) should be far smaller than raw ({raw} bytes)"
    );
}
