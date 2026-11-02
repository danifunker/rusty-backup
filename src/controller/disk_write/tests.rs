use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::*;

fn ctl() -> DiskWriteController {
    DiskWriteController::new(DiskWriteServices {
        jobs: JobRegistry::new(),
        notify: Arc::new(NoNotify),
    })
}

/// A disk-image file standing in for a device: tests never touch a real disk.
fn fake_device(path: &Path) -> DiskDevice {
    DiskDevice {
        name: "fake".into(),
        path: path.to_path_buf(),
        size_bytes: std::fs::metadata(path).unwrap().len(),
        is_removable: true,
        is_read_only: false,
        is_system: false,
        bus_protocol: "USB".into(),
        media_name: "Fake".into(),
        partitions: Vec::new(),
    }
}

/// A patterned source image of `len` bytes.
fn source(dir: &Path, len: usize) -> PathBuf {
    let p = dir.join("src.img");
    let bytes: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
    std::fs::write(&p, bytes).unwrap();
    p
}

/// A zeroed disk with one MBR partition at LBA 2048 of `sectors` sectors.
fn mbr_disk(dir: &Path, len: usize, sectors: u32) -> PathBuf {
    let p = dir.join("disk.img");
    let mut d = vec![0u8; len];
    d[0x1BE + 4] = 0x06;
    d[0x1BE + 8..0x1BE + 12].copy_from_slice(&2048u32.to_le_bytes());
    d[0x1BE + 12..0x1BE + 16].copy_from_slice(&sectors.to_le_bytes());
    d[510] = 0x55;
    d[511] = 0xAA;
    std::fs::write(&p, d).unwrap();
    p
}

fn answer_name(c: &mut DiskWriteController, fx: Vec<Effect>) {
    let name = match fx.as_slice() {
        [Effect::AskText(spec)] => {
            assert!((spec.validator)(spec, "wrong").is_err());
            assert!((spec.validator)(spec, spec.taken[0].as_str()).is_ok());
            spec.taken[0].clone()
        }
        other => panic!("expected the device-name prompt, got {other:?}"),
    };
    c.apply(DiskWriteAction::Answered {
        purpose: purpose::DEVICE_NAME,
        text: Some(name),
    });
}

#[test]
fn an_image_file_target_is_written_byte_for_byte_after_a_yes() {
    let dir = tempfile::tempdir().unwrap();
    let src = source(dir.path(), 3 << 20);
    let out = dir.path().join("out.img");
    let mut c = ctl();
    c.apply(DiskWriteAction::Open(WriteOpen::image(
        src.clone(),
        3 << 20,
    )));
    c.wait_idle();
    assert!(c.is_open());
    assert!(!c.can_write(), "no target yet");
    c.apply(DiskWriteAction::SetTargetImage(out.clone()));
    let fx = c.apply(DiskWriteAction::Write);
    assert!(matches!(fx.as_slice(), [Effect::Confirm(s)] if s.purpose == purpose::OVERWRITE_FILE));
    c.apply(DiskWriteAction::Confirmed {
        purpose: purpose::OVERWRITE_FILE,
        yes: true,
    });
    assert!(c.is_writing());
    c.wait_finished();
    assert_eq!(c.result(), Some(&Ok(3 << 20)));
    assert_eq!(std::fs::read(&out).unwrap(), std::fs::read(&src).unwrap());
    assert!(c.has_run());
}

#[test]
fn a_superfloppy_gets_a_synthesized_mbr_after_the_device_name_is_typed() {
    let dir = tempfile::tempdir().unwrap();
    let src = source(dir.path(), 2 << 20);
    let disk = dir.path().join("dev.img");
    std::fs::write(&disk, vec![0u8; 8 << 20]).unwrap();
    let mut c = ctl();
    c.apply(DiskWriteAction::Open(WriteOpen {
        path: src.clone(),
        size_bytes: None,
        fs_hint: "FAT16".into(),
        has_partition_table: false,
        superfloppy: true,
    }));
    c.wait_idle();
    assert_eq!(c.source().map(|s| s.size_bytes), Some(2 << 20));
    assert!(c.form().synthesize && c.can_synthesize());
    assert_eq!(c.form().type_byte, 0x06);
    c.apply(DiskWriteAction::SelectDevice {
        row: 0,
        device: fake_device(&disk),
    });
    c.wait_idle();
    assert!(c.target_partitions().is_empty());

    // A wrong name writes nothing.
    let fx = c.apply(DiskWriteAction::Write);
    assert!(matches!(fx.as_slice(), [Effect::AskText(_)]));
    c.apply(DiskWriteAction::Answered {
        purpose: purpose::DEVICE_NAME,
        text: Some("nope".into()),
    });
    assert!(!c.is_writing());
    assert!(c
        .take_log()
        .iter()
        .any(|(_, m)| m.contains("does not match")));

    let fx = c.apply(DiskWriteAction::Write);
    answer_name(&mut c, fx);
    c.wait_finished();
    assert!(matches!(c.result(), Some(Ok(_))), "{:?}", c.result());
    let d = std::fs::read(&disk).unwrap();
    assert_eq!(&d[510..512], &[0x55, 0xAA]);
    assert_eq!(d[0x1BE + 4], 0x06);
    assert_eq!(&d[1 << 20..3 << 20], &std::fs::read(&src).unwrap()[..]);
}

#[test]
fn a_partition_write_keeps_the_table_and_refuses_a_source_too_big() {
    let dir = tempfile::tempdir().unwrap();
    let disk = mbr_disk(dir.path(), 8 << 20, 4096);
    let table_before = std::fs::read(&disk).unwrap()[..512].to_vec();
    let mut c = ctl();
    let big = source(dir.path(), 3 << 20);
    c.apply(DiskWriteAction::Open(WriteOpen::image(big, 3 << 20)));
    c.wait_idle();
    c.apply(DiskWriteAction::SelectDevice {
        row: 2,
        device: fake_device(&disk),
    });
    c.wait_idle();
    let parts = c.target_partitions();
    assert_eq!(parts.len(), 1);
    c.apply(DiskWriteAction::SetTargetPartition(Some(parts[0].index)));
    assert!(c.partition_warning().is_some());
    assert!(c.apply(DiskWriteAction::Write).is_empty());
    assert!(c.error().unwrap_or("").contains("holds only"));

    let small = dir.path().join("small.img");
    std::fs::write(&small, vec![0xA5u8; 1 << 20]).unwrap();
    c.apply(DiskWriteAction::Open(WriteOpen::image(small, 1 << 20)));
    c.wait_idle();
    assert!(
        c.target_device().is_none(),
        "a new source starts with no target"
    );
    c.apply(DiskWriteAction::SelectDevice {
        row: 2,
        device: fake_device(&disk),
    });
    c.wait_idle();
    c.apply(DiskWriteAction::CycleTargetPartition);
    assert_eq!(c.target_partition(), Some(parts[0].index));
    let fx = c.apply(DiskWriteAction::Write);
    answer_name(&mut c, fx);
    c.wait_finished();
    assert_eq!(c.result(), Some(&Ok(1 << 20)));
    let d = std::fs::read(&disk).unwrap();
    assert_eq!(&d[..512], &table_before[..]);
    assert!(d[1 << 20..2 << 20].iter().all(|&b| b == 0xA5));
    assert!(d[2 << 20..].iter().all(|&b| b == 0));
}

#[test]
fn a_refreshed_device_list_moves_the_target_with_its_row() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.img");
    let b = dir.path().join("b.img");
    std::fs::write(&a, vec![0u8; 1 << 20]).unwrap();
    std::fs::write(&b, vec![0u8; 2 << 20]).unwrap();
    let mut c = ctl();
    c.apply(DiskWriteAction::Open(WriteOpen::image(a.clone(), 1 << 20)));
    c.wait_idle();
    c.apply(DiskWriteAction::SelectDevice {
        row: 0,
        device: fake_device(&a),
    });
    c.reconcile_devices(&[fake_device(&b)]);
    assert_eq!(c.target_device().map(|d| d.path.clone()), Some(b));
    c.reconcile_devices(&[]);
    assert!(c.target_device().is_none());
    c.wait_idle();
    c.apply(DiskWriteAction::Close);
    assert!(!c.is_open());
}

#[test]
fn the_write_region_cycles_whole_disk_then_each_partition() {
    use super::target::next_region;
    let parts = [0, 1, 2];
    let mut sel = None;
    for want in [Some(0), Some(1), Some(2), None] {
        sel = next_region(&parts, sel);
        assert_eq!(sel, want);
    }
    // No table offers only the whole disk; a vanished choice falls back rather than sticking.
    assert_eq!(next_region(&[], None), None);
    assert_eq!(next_region(&[], Some(3)), None);
    assert_eq!(next_region(&[0], Some(9)), None);
}
