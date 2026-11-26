//! A fixed VHD exported from a backup folder ends in its footer and leaves the partitions
//! whole. The footer used to be written at the cursor the hidden-sector patch left behind,
//! over the last partition's second sector (RBF-001). A resized partition takes its
//! filesystem with it: the export used to cut the data and leave the BPB at the old size (RBF-003).

use std::sync::{Arc, Mutex};

use rusty_backup::backup::{
    metadata::BackupMetadata, run_backup, BackupConfig, BackupProgress, ChecksumType,
    CompressionType,
};

struct Backed {
    _work: tempfile::TempDir,
    folder: std::path::PathBuf,
    meta: BackupMetadata,
    mbr: [u8; 512],
    disk: Vec<u8>,
    vhd: std::path::PathBuf,
}

const START: u32 = 2048;
const SECTORS: u32 = 30720;

fn back_up_a_fat_disk() -> Backed {
    let work = tempfile::tempdir().unwrap();
    let src = work.path().join("disk.img");
    let (start, sectors) = (START, SECTORS);
    let mut disk = vec![0u8; (start + sectors) as usize * 512];
    let fat = fs_pc::fat::create_blank_fat(sectors as u64 * 512, Some("VHD")).unwrap();
    disk[start as usize * 512..][..fat.len()].copy_from_slice(&fat);
    let e = 446;
    disk[e + 4] = 0x06;
    disk[e + 8..e + 12].copy_from_slice(&start.to_le_bytes());
    disk[e + 12..e + 16].copy_from_slice(&sectors.to_le_bytes());
    disk[510] = 0x55;
    disk[511] = 0xAA;
    std::fs::write(&src, &disk).unwrap();

    let backups = work.path().join("backups");
    std::fs::create_dir_all(&backups).unwrap();
    run_backup(
        BackupConfig {
            source_path: src.clone(),
            destination_dir: backups.clone(),
            backup_name: "b".into(),
            compression: CompressionType::Zstd,
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
        },
        Arc::new(Mutex::new(BackupProgress::default())),
    )
    .unwrap();
    let folder = backups.join("b");
    let meta: BackupMetadata =
        serde_json::from_reader(std::fs::File::open(folder.join("metadata.json")).unwrap())
            .unwrap();
    let mut mbr = [0u8; 512];
    mbr.copy_from_slice(&std::fs::read(folder.join("mbr.bin")).unwrap()[..512]);

    let vhd = work.path().join("disk.vhd");
    Backed {
        _work: work,
        folder,
        meta,
        mbr,
        disk,
        vhd,
    }
}

fn export(b: &Backed, sizes: &[part_tables::PartitionSizeOverride]) -> Vec<u8> {
    rusty_backup::rbformats::vhd::export_whole_disk_vhd(
        &b.folder,
        Some(&b.meta),
        Some(&b.mbr),
        sizes,
        &b.vhd,
        |_| {},
        || false,
        |_| {},
    )
    .unwrap();
    std::fs::read(&b.vhd).unwrap()
}

#[test]
fn a_fixed_vhd_from_a_backup_ends_in_its_footer() {
    let b = back_up_a_fat_disk();
    let out = export(&b, &[]);
    assert_eq!(&out[out.len() - 512..out.len() - 504], b"conectix");
    let p = START as usize * 512;
    assert_eq!(
        out[p + 512..p + 1024],
        b.disk[p + 512..p + 1024],
        "partition sector 1 intact"
    );
}

#[test]
fn a_shrunk_partition_takes_its_fat_with_it() {
    let b = back_up_a_fat_disk();
    let index = b.meta.partitions[0].index;
    let new_sectors = 20480u64;
    let sizes = [part_tables::PartitionSizeOverride::size_only(
        index,
        START as u64,
        SECTORS as u64 * 512,
        new_sectors * 512,
    )];
    let out = export(&b, &sizes);
    let bpb = &out[START as usize * 512..][..512];
    let small = u16::from_le_bytes([bpb[19], bpb[20]]) as u64;
    let large = u32::from_le_bytes([bpb[32], bpb[33], bpb[34], bpb[35]]) as u64;
    let total = if small != 0 { small } else { large };
    assert_eq!(total, new_sectors, "the BPB follows the partition");
}
