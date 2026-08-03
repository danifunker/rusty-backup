//! `--split-size` backups round-trip.
//!
//! Two defects, both silent:
//!
//! * Restore read only `compressed_files[0]`. `--split-size` cuts one byte
//!   stream across `partition-0.raw` + `.001` + …, so everything past the first
//!   member was dropped and the image restored truncated while reporting
//!   success.
//! * `chd::split_file` wrote chunk 0 to the path it was reading from, so
//!   `File::create` truncated the source mid-read: a split CHD backup produced
//!   a `metadata.json` with an empty file list and no data at all.

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
fn chd_split_backup_keeps_its_data() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().to_path_buf();
    let src = work.join("source.img");
    source_image(&src, 5 * 1024 * 1024);
    let expected = std::fs::read(&src).unwrap();

    let (restored, members) = split_round_trip(&work, &src, "chd", CompressionType::Chd);
    assert!(
        members.len() > 1,
        "expected the CHD to split, got {members:?}"
    );
    // split_file used to truncate the .chd it was reading from, leaving none.
    assert!(
        members.iter().all(|m| m.ends_with(".chd")),
        "unexpected member names: {members:?}"
    );
    assert_eq!(restored, expected, "split CHD restore lost data");
}

/// `split_file` must not clobber the file it is reading: chunk 0's path is the
/// source's own path, so the source has to be staged aside first.
#[cfg(feature = "chd")]
#[test]
fn split_file_does_not_destroy_its_source() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("partition-0");
    let source = dir.path().join("partition-0.chd");
    let content: Vec<u8> = (0..(3 * 1024 * 1024u32)).map(|i| (i % 253) as u8).collect();
    std::fs::write(&source, &content).unwrap();

    let files = rusty_backup::rbformats::split_file_for_test(&source, &base, "chd", 1024 * 1024)
        .expect("split");
    assert!(files.len() >= 3, "expected several chunks, got {files:?}");

    let mut joined = Vec::new();
    for f in &files {
        joined.extend_from_slice(&std::fs::read(dir.path().join(f)).unwrap());
    }
    assert_eq!(
        joined, content,
        "chunks must rejoin into the original bytes"
    );
    assert!(
        !dir.path().join("partition-0.chd.splitting").exists(),
        "the staging file must be cleaned up"
    );
}
