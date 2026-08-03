//! `--split-size` backups round-trip.
//!
//! Two defects, both silent:
//!
//! * Restore read only `compressed_files[0]`. `--split-size` cuts one byte
//!   stream across `partition-0.raw` + `.001` + …, so everything past the first
//!   member was dropped and the image restored truncated while reporting
//!   success.
//! * `--split-size` was accepted alongside `--format chd`, which cannot work:
//!   a `.chd` is a self-contained container (header, hunk map, embedded
//!   SHA-1s), so byte-chunks of one are not readable by chdman, MAME or this
//!   tool. The old code tried to split it anyway and produced a backup with no
//!   data files at all. The combination is now refused up front.

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
    split_mib: Option<u32>,
) -> BackupConfig {
    BackupConfig {
        source_path: src.to_path_buf(),
        destination_dir: dest.to_path_buf(),
        backup_name: name.to_string(),
        compression,
        checksum: ChecksumType::Crc32,
        split_size_mib: split_mib,
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

/// A FAT16 superfloppy carrying `payload_bytes` of incompressible data, so the
/// backup stream is guaranteed to exceed a 1 MiB split boundary whatever the
/// codec does with it.
fn source_image(path: &std::path::Path, payload_bytes: usize) {
    let blob = rusty_backup::fs::fat::create_blank_fat(32 * 1024 * 1024, Some("SPLIT")).unwrap();
    std::fs::write(path, &blob).unwrap();

    use rusty_backup::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut fs = rusty_backup::fs::fat::FatFilesystem::open(file, 0).unwrap();
    let root = fs.root().unwrap();
    // A cheap xorshift keeps this incompressible without pulling in rand.
    let mut state: u64 = 0x243F_6A88_85A3_08D3;
    let payload: Vec<u8> = (0..payload_bytes)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect();
    let mut data = &payload[..];
    fs.create_file(
        &root,
        "RANDOM.BIN",
        &mut data,
        payload.len() as u64,
        &CreateFileOptions::default(),
    )
    .unwrap();
    EditableFilesystem::sync_metadata(&mut fs).unwrap();
}

/// Back up with `--split-size 1 MiB`, restore, and return
/// (restored bytes, member file names).
fn split_round_trip(
    work: &std::path::Path,
    src: &std::path::Path,
    tag: &str,
    compression: CompressionType,
) -> (Vec<u8>, Vec<String>) {
    let backups = work.join(format!("backups-{tag}"));
    std::fs::create_dir_all(&backups).unwrap();
    run_backup(
        backup_config(src, &backups, tag, compression, Some(1)),
        Arc::new(Mutex::new(BackupProgress::default())),
    )
    .unwrap_or_else(|e| panic!("{tag}: backup failed: {e:?}"));

    let folder = backups.join(tag);
    let meta: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(folder.join("metadata.json")).unwrap())
            .unwrap();
    let members: Vec<String> = meta["partitions"][0]["compressed_files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();

    assert!(
        !members.is_empty(),
        "{tag}: backup recorded no data files at all"
    );
    for m in &members {
        let path = folder.join(m);
        assert!(
            path.exists(),
            "{tag}: member {m} is missing from the folder"
        );
        assert!(
            std::fs::metadata(&path).unwrap().len() > 0,
            "{tag}: member {m} is empty"
        );
    }

    let target = work.join(format!("restored-{tag}.img"));
    rusty_backup::restore::run_restore(
        RestoreConfig {
            backup_folder: folder,
            target_path: target.clone(),
            target_is_device: false,
            target_size: std::fs::metadata(src).unwrap().len(),
            alignment: RestoreAlignment::Original,
            partition_sizes: Vec::new(),
            write_zeros_to_unused: false,
        },
        Arc::new(Mutex::new(RestoreProgress::default())),
    )
    .unwrap_or_else(|e| panic!("{tag}: restore failed: {e:?}"));

    (std::fs::read(&target).unwrap(), members)
}

#[test]
fn raw_split_backup_restores_every_member() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().to_path_buf();
    let src = work.join("source.img");
    source_image(&src, 5 * 1024 * 1024);
    let expected = std::fs::read(&src).unwrap();

    let (restored, members) = split_round_trip(&work, &src, "raw", CompressionType::None);
    assert!(
        members.len() > 1,
        "expected a genuine split, got {members:?}"
    );
    assert_eq!(
        restored,
        expected,
        "restore dropped {} of {} members",
        members.len() - 1,
        members.len()
    );
}

/// The user-visible consequence: a file larger than one split member has to
/// read back whole. A byte-compare alone would not say *which* bytes were lost.
#[test]
fn a_file_spanning_several_members_reads_back_intact() {
    use rusty_backup::fs::filesystem::Filesystem;

    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().to_path_buf();
    let src = work.join("source.img");
    source_image(&src, 5 * 1024 * 1024);

    let expected = {
        let mut fs =
            rusty_backup::fs::fat::FatFilesystem::open(std::fs::File::open(&src).unwrap(), 0)
                .unwrap();
        let root = fs.root().unwrap();
        let entry = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name.eq_ignore_ascii_case("RANDOM.BIN"))
            .expect("payload present in the source");
        fs.read_file(&entry, entry.size as usize).unwrap()
    };
    assert!(expected.len() > 4 * 1024 * 1024, "payload spans members");

    let (_, members) = split_round_trip(&work, &src, "spanning", CompressionType::None);
    assert!(members.len() > 1, "expected a genuine split");

    let restored_path = work.join("restored-spanning.img");
    let mut fs =
        rusty_backup::fs::fat::FatFilesystem::open(std::fs::File::open(&restored_path).unwrap(), 0)
            .expect("restored image still mounts");
    let root = fs.root().unwrap();
    let entry = fs
        .list_directory(&root)
        .unwrap()
        .into_iter()
        .find(|e| e.name.eq_ignore_ascii_case("RANDOM.BIN"))
        .expect("payload survived the restore");
    let got = fs.read_file(&entry, entry.size as usize).unwrap();
    assert_eq!(got.len(), expected.len(), "payload length changed");
    assert_eq!(got, expected, "payload bytes changed across the split");
}

#[cfg(feature = "chd")]
#[test]
fn chd_rejects_split_size_up_front() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().to_path_buf();
    let src = work.join("source.img");
    source_image(&src, 5 * 1024 * 1024);
    let backups = work.join("backups-chd");
    std::fs::create_dir_all(&backups).unwrap();

    let err = run_backup(
        backup_config(&src, &backups, "chd", CompressionType::Chd, Some(1)),
        Arc::new(Mutex::new(BackupProgress::default())),
    )
    .expect_err("splitting a CHD must be refused, not attempted");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("--split-size") && msg.contains("chd"),
        "the error should name both the flag and the format: {msg}"
    );

    // Refused before any work: no half-written backup folder left behind.
    assert!(
        !backups.join("chd").join("metadata.json").exists(),
        "a refused backup must not leave a metadata.json"
    );

    // Without --split-size the same source backs up and restores fine.
    let backups_ok = work.join("backups-chd-ok");
    std::fs::create_dir_all(&backups_ok).unwrap();
    run_backup(
        backup_config(&src, &backups_ok, "ok", CompressionType::Chd, None),
        Arc::new(Mutex::new(BackupProgress::default())),
    )
    .expect("an unsplit CHD backup still works");
    let folder = backups_ok.join("ok");
    let meta: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(folder.join("metadata.json")).unwrap())
            .unwrap();
    let members = meta["partitions"][0]["compressed_files"]
        .as_array()
        .unwrap();
    assert_eq!(members.len(), 1, "a CHD backup is always one container");

    let target = work.join("restored-chd.img");
    rusty_backup::restore::run_restore(
        RestoreConfig {
            backup_folder: folder,
            target_path: target.clone(),
            target_is_device: false,
            target_size: std::fs::metadata(&src).unwrap().len(),
            alignment: RestoreAlignment::Original,
            partition_sizes: Vec::new(),
            write_zeros_to_unused: false,
        },
        Arc::new(Mutex::new(RestoreProgress::default())),
    )
    .expect("restore the unsplit CHD");
    assert_eq!(
        std::fs::read(&target).unwrap(),
        std::fs::read(&src).unwrap(),
        "unsplit CHD restore must be byte-identical"
    );
}
