use super::*;
use crate::model::job::NoNotify;

const MIB: u64 = 1024 * 1024;

fn ctl() -> BuildDiskController {
    BuildDiskController::new(Arc::new(NoNotify))
}

fn act(c: &mut BuildDiskController, a: BuildDiskAction) -> Vec<Effect> {
    c.apply(a)
}

fn image_output(c: &mut BuildDiskController, path: &Path) {
    act(c, BuildDiskAction::SetOutputIsDevice(false));
    act(
        c,
        BuildDiskAction::Picked {
            purpose: purpose::OUTPUT_IMAGE,
            path: Some(path.to_path_buf()),
        },
    );
}

fn fake_device(path: &Path, size: u64) -> DiskDevice {
    std::fs::File::create(path).unwrap().set_len(size).unwrap();
    DiskDevice {
        name: "card".to_string(),
        path: path.to_path_buf(),
        size_bytes: size,
        is_removable: true,
        is_read_only: false,
        is_system: false,
        bus_protocol: "USB".to_string(),
        media_name: "Test card".to_string(),
        partitions: Vec::new(),
    }
}

fn walk_to(c: &mut BuildDiskController, step: Step) {
    for _ in 0..10 {
        if c.step() == step {
            return;
        }
        let before = c.step();
        act(c, BuildDiskAction::Next);
        assert_ne!(
            c.step(),
            before,
            "stuck at {:?}: {:?}",
            before,
            c.next_blocked()
        );
    }
    panic!("never reached {step:?}");
}

fn open_fs(path: &Path, offset: u64) {
    let f = std::fs::File::open(path).unwrap();
    crate::fs::open_filesystem(f, offset, 0, None).expect("opens");
}

#[test]
fn a_rows_source_is_picked_then_measured_in_a_job_and_replaces_its_format() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("p.img");
    std::fs::write(&src, vec![0u8; MIB as usize]).unwrap();
    let mut c = ctl();
    act(
        &mut c,
        BuildDiskAction::SetRowFs {
            row: 0,
            fs: Some(FsKind::Fat),
        },
    );
    let fx = act(
        &mut c,
        BuildDiskAction::Table(BuildAction::PickRowSource(0)),
    );
    assert!(matches!(fx.as_slice(), [Effect::PickFile(s)] if s.purpose == purpose::ROW_SOURCE));
    act(
        &mut c,
        BuildDiskAction::Picked {
            purpose: purpose::ROW_SOURCE,
            path: Some(src.clone()),
        },
    );
    c.wait();
    let v = c.table_view();
    assert_eq!(v.rows[0].source.as_deref(), Some(src.as_path()));
    assert_eq!(v.rows[0].source_size, MIB);
    assert_eq!(c.row_content(0), RowContent::Image);
    act(
        &mut c,
        BuildDiskAction::Table(BuildAction::ClearRowSource(0)),
    );
    assert_eq!(c.row_content(0), RowContent::Empty);
}

#[test]
fn partitioned_steps_gain_options_only_when_a_row_formats() {
    let mut c = ctl();
    assert!(!c.steps().contains(&Step::Options));
    act(
        &mut c,
        BuildDiskAction::SetRowFs {
            row: 0,
            fs: Some(FsKind::Fat),
        },
    );
    assert!(c.steps().contains(&Step::Options));
    act(&mut c, BuildDiskAction::SetKind(DiskKind::Volume));
    assert_eq!(
        c.steps(),
        vec![
            Step::Kind,
            Step::Output,
            Step::Disk,
            Step::Options,
            Step::Review,
            Step::Build
        ]
    );
}

#[test]
fn next_is_blocked_with_a_reason_and_back_keeps_entries() {
    let mut c = ctl();
    act(&mut c, BuildDiskAction::Next);
    assert_eq!(c.step(), Step::Output);
    assert!(c.next_blocked().unwrap().contains("save"));
    act(&mut c, BuildDiskAction::Next);
    assert_eq!(c.step(), Step::Output, "blocked");
    image_output(&mut c, Path::new("/tmp/x.img"));
    act(&mut c, BuildDiskAction::Next);
    assert_eq!(c.step(), Step::Disk);
    act(&mut c, BuildDiskAction::SetSize("nonsense".into()));
    assert!(c.next_blocked().is_some());
    act(&mut c, BuildDiskAction::SetSize("64M".into()));
    assert!(c.next_blocked().is_none());
    act(&mut c, BuildDiskAction::Back);
    assert_eq!(c.step(), Step::Output);
    assert_eq!(c.image(), Some(Path::new("/tmp/x.img")));
    assert_eq!(c.size_text(), "64M");
}

#[test]
fn a_volume_size_outside_the_filesystem_limits_blocks_with_the_reason() {
    let mut c = ctl();
    act(&mut c, BuildDiskAction::SetKind(DiskKind::Volume));
    act(&mut c, BuildDiskAction::SetVolumeFs(FsKind::Prodos));
    act(&mut c, BuildDiskAction::SetSize("64M".into()));
    image_output(&mut c, Path::new("/tmp/x.img"));
    walk_to(&mut c, Step::Disk);
    let why = c.next_blocked().expect("ProDOS tops out at 32 MiB");
    assert!(why.contains("ProDOS"), "{why}");
    act(&mut c, BuildDiskAction::SetSize("5M".into()));
    assert!(c.next_blocked().is_none());
}

#[test]
fn a_bad_label_blocks_the_options_step() {
    let mut c = ctl();
    act(&mut c, BuildDiskAction::SetKind(DiskKind::Volume));
    act(&mut c, BuildDiskAction::SetVolumeFs(FsKind::Fat));
    let mut o = c.format_targets()[0].opts.clone();
    o.label = "THIS LABEL IS FAR TOO LONG".into();
    act(&mut c, BuildDiskAction::SetVolumeOptions(o));
    assert!(c.format_targets()[0].error.is_some());
}

#[test]
fn a_fat_volume_builds_into_an_image_file() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("vol.img");
    let mut c = ctl();
    act(&mut c, BuildDiskAction::SetKind(DiskKind::Volume));
    act(&mut c, BuildDiskAction::SetVolumeFs(FsKind::Fat));
    act(&mut c, BuildDiskAction::SetSize("16M".into()));
    image_output(&mut c, &img);
    walk_to(&mut c, Step::Review);
    act(&mut c, BuildDiskAction::Next);
    assert_eq!(c.step(), Step::Build);
    c.wait();
    let log = c.take_log();
    assert_eq!(c.outcome(), Some(&Ok(())), "{log:?}");
    assert_eq!(c.built_image(), Some(img.as_path()));
    assert_eq!(std::fs::metadata(&img).unwrap().len(), 16 * MIB);
    open_fs(&img, 0);
}

#[test]
fn a_partitioned_disk_formats_each_row() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("disk.img");
    let mut c = ctl();
    act(&mut c, BuildDiskAction::SetSize("64M".into()));
    image_output(&mut c, &img);
    act(
        &mut c,
        BuildDiskAction::Table(BuildAction::SetRowSize {
            row: 0,
            text: "24M".into(),
        }),
    );
    act(&mut c, BuildDiskAction::Table(BuildAction::AddRow));
    act(
        &mut c,
        BuildDiskAction::SetRowFs {
            row: 0,
            fs: Some(FsKind::Fat),
        },
    );
    act(
        &mut c,
        BuildDiskAction::SetRowFs {
            row: 1,
            fs: Some(FsKind::Ext),
        },
    );
    assert_eq!(c.table_view().rows[0].type_text, "04", "FAT16 under 32 MiB");
    walk_to(&mut c, Step::Review);
    act(&mut c, BuildDiskAction::Next);
    c.wait();
    let log = c.take_log();
    assert_eq!(c.outcome(), Some(&Ok(())), "{log:?}");
    let f = std::fs::File::open(&img).unwrap();
    let mut r = std::io::BufReader::new(f);
    let table = crate::partition::PartitionTable::detect(&mut r).expect("table");
    let parts = table.partitions();
    assert_eq!(parts.len(), 2);
    for p in parts {
        open_fs(&img, p.start_lba * 512);
    }
}

#[test]
fn a_device_build_needs_its_path_typed_and_writes_every_formatted_byte() {
    let dir = tempfile::tempdir().unwrap();
    let dev = dir.path().join("card.bin");
    let device = fake_device(&dev, 32 * MIB);
    // Old data on the card that a sparse format would leave showing through.
    std::fs::write(&dev, vec![0xA5u8; 32 * MIB as usize]).unwrap();
    let mut c = ctl();
    act(&mut c, BuildDiskAction::SetKind(DiskKind::Volume));
    act(&mut c, BuildDiskAction::SetVolumeFs(FsKind::Fat));
    act(&mut c, BuildDiskAction::SetOutputIsDevice(true));
    act(&mut c, BuildDiskAction::SelectDevice { row: 0, device });
    walk_to(&mut c, Step::Review);
    assert!(c.next_blocked().unwrap().contains("Type the device path"));
    act(&mut c, BuildDiskAction::Next);
    assert_eq!(c.step(), Step::Review, "refused without the typed path");
    act(
        &mut c,
        BuildDiskAction::SetConfirmText(dev.display().to_string()),
    );
    assert!(c.next_blocked().is_none());
    act(&mut c, BuildDiskAction::Next);
    c.wait();
    let log = c.take_log();
    assert_eq!(c.outcome(), Some(&Ok(())), "{log:?}");
    let bytes = std::fs::read(&dev).unwrap();
    assert!(!bytes.contains(&0xA5) || bytes.iter().filter(|&&b| b == 0xA5).count() < 64);
    assert!(
        bytes[bytes.len() - 4096..].iter().all(|&b| b == 0),
        "the tail was zeroed"
    );
    open_fs(&dev, 0);
}

#[test]
fn the_system_disk_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut device = fake_device(&dir.path().join("sys"), 8 * MIB);
    device.is_system = true;
    let mut c = ctl();
    act(&mut c, BuildDiskAction::SetOutputIsDevice(true));
    act(&mut c, BuildDiskAction::SelectDevice { row: 0, device });
    walk_to(&mut c, Step::Output);
    assert!(c.next_blocked().unwrap().contains("system disk"));
}
