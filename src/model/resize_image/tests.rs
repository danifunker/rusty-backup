use super::*;
use crate::model::job::{Job, NoNotify};
use crate::partition::type_catalog::TableKind;

const MIB: u64 = 1024 * 1024;

fn disk(dir: &Path, kind: TableKind, disk_mib: u64, parts_mib: &[u64]) -> PathBuf {
    crate::controller::test_fixtures::fat_disk(dir, kind, disk_mib, parts_mib)
}

fn table(path: &Path) -> PartitionTable {
    PartitionTable::detect(&mut std::fs::File::open(path).unwrap()).unwrap()
}

fn request(path: &Path, disk_mib: u64, sizes: &[(usize, u64)]) -> ResizeImageRequest {
    let t = table(path);
    ResizeImageRequest {
        source: path.to_path_buf(),
        copy_to: None,
        partitions: t.partitions(),
        table: t,
        alignment_sectors: 2048,
        disk_size: disk_mib * MIB,
        sizes: sizes.iter().map(|&(i, m)| (i, m * MIB)).collect(),
        check_after: true,
    }
}

fn run_job(req: ResizeImageRequest) -> (Result<(), String>, SharedProgress) {
    let shared = SharedProgress::default();
    let held = Arc::clone(&shared);
    let job = Job::spawn("resize", Arc::new(NoNotify), move |ctx| {
        Ok(run(&req, ctx, &held).map_err(|e| format!("{e:#}")))
    });
    job.wait();
    let result = job.take_result().unwrap_or(Err("no result".into()));
    (result, shared)
}

fn sizes_of(path: &Path) -> Vec<u64> {
    table(path)
        .partitions()
        .iter()
        .map(|p| p.size_bytes)
        .collect()
}

fn all_clean(shared: &SharedProgress) {
    let p = shared.lock().unwrap();
    assert!(!p.checks.is_empty(), "the check after ran");
    for (i, v) in &p.checks {
        assert_eq!(*v, Verdict::Clean, "partition {i}");
    }
}

#[test]
fn a_copy_grows_the_disk_and_a_partition_leaving_free_space() {
    let dir = tempfile::tempdir().unwrap();
    let src = disk(dir.path(), TableKind::Mbr, 64, &[16, 16]);
    let out = dir.path().join("disk-resized.img");
    let mut req = request(&src, 96, &[(1, 40)]);
    req.copy_to = Some(out.clone());
    let (result, shared) = run_job(req);
    result.unwrap();
    assert_eq!(
        std::fs::metadata(&src).unwrap().len(),
        64 * MIB,
        "the source is untouched"
    );
    assert_eq!(sizes_of(&src), [16 * MIB, 16 * MIB]);
    assert_eq!(std::fs::metadata(&out).unwrap().len(), 96 * MIB);
    assert_eq!(sizes_of(&out), [16 * MIB, 40 * MIB]);
    all_clean(&shared);
    let labels: Vec<Step> = shared.lock().unwrap().steps.iter().map(|s| s.0).collect();
    assert_eq!(
        labels,
        [
            Step::Copy,
            Step::Grow,
            Step::Partitions,
            Step::Finish,
            Step::Check
        ]
    );
}

#[test]
fn a_gpt_disk_grows_in_place_with_its_backup_table_at_the_new_end() {
    let dir = tempfile::tempdir().unwrap();
    let path = disk(dir.path(), TableKind::Gpt, 64, &[16, 16]);
    let (result, shared) = run_job(request(&path, 128, &[(0, 32)]));
    result.unwrap();
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 128 * MIB);
    assert_eq!(sizes_of(&path), [32 * MIB, 16 * MIB]);
    let mut f = std::fs::File::open(&path).unwrap();
    let mut header = [0u8; 8];
    f.seek(SeekFrom::Start(128 * MIB - SECTOR)).unwrap();
    f.read_exact(&mut header).unwrap();
    assert_eq!(&header, b"EFI PART", "the backup header is the last sector");
    f.seek(SeekFrom::Start(64 * MIB - SECTOR)).unwrap();
    f.read_exact(&mut header).unwrap();
    assert_eq!(header, [0u8; 8], "the old backup header is cleared");
    all_clean(&shared);
}

#[test]
fn a_disk_and_its_last_partition_shrink() {
    let dir = tempfile::tempdir().unwrap();
    let path = disk(dir.path(), TableKind::Mbr, 64, &[16, 16]);
    let (result, shared) = run_job(request(&path, 32, &[(1, 14)]));
    result.unwrap();
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 32 * MIB);
    assert_eq!(sizes_of(&path), [16 * MIB, 14 * MIB]);
    all_clean(&shared);
}

#[test]
fn a_shrink_the_filesystem_refuses_is_found_before_a_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = disk(dir.path(), TableKind::Mbr, 64, &[16, 16]);
    let before = std::fs::read(&path).unwrap();
    // 8 MiB would turn this FAT16 volume into FAT12, which FAT cannot do in place.
    let err = plan(&request(&path, 64, &[(1, 8)])).unwrap_err();
    assert!(
        format!("{err:#}").contains("partition 2 cannot shrink"),
        "{err:#}"
    );
    assert_eq!(std::fs::read(&path).unwrap(), before, "nothing was written");
}

#[test]
fn a_disk_too_small_for_its_partitions_is_refused_before_a_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = disk(dir.path(), TableKind::Gpt, 64, &[16, 16]);
    let before = std::fs::read(&path).unwrap();
    // The partitions end at 33 MiB and GPT needs 33 sectors after them.
    let err = plan(&request(&path, 33, &[])).unwrap_err();
    assert!(format!("{err}").contains("the partitions need"), "{err}");
    let (result, _) = run_job(request(&path, 33, &[]));
    assert!(result.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before, "nothing was written");
}

#[test]
fn a_vhd_keeps_one_footer_at_its_new_end() {
    let dir = tempfile::tempdir().unwrap();
    let raw = disk(dir.path(), TableKind::Mbr, 64, &[16, 16]);
    let path = dir.path().join("disk.vhd");
    let mut bytes = std::fs::read(&raw).unwrap();
    bytes.extend_from_slice(&dsk_vm::vhd::build_vhd_footer(64 * MIB));
    std::fs::write(&path, bytes).unwrap();
    let (result, shared) = run_job(request(&path, 96, &[(1, 40)]));
    result.unwrap();
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(bytes.len() as u64, 96 * MIB + SECTOR);
    assert_eq!(&bytes[bytes.len() - 512..bytes.len() - 504], b"conectix");
    let at: Vec<usize> = (0..bytes.len() / 512)
        .filter(|i| bytes[i * 512..].starts_with(b"conectix"))
        .collect();
    assert_eq!(
        at.len(),
        1,
        "no stray footer is left in the free space: {at:?}"
    );
    assert_eq!(sizes_of(&path), [16 * MIB, 40 * MIB]);
    all_clean(&shared);
}
