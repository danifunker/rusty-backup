use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::*;
use crate::controller::Controller as _;

fn controller() -> RestoreController {
    RestoreController::new(RestoreServices {
        jobs: JobRegistry::new(),
        notify: Arc::new(NoNotify),
    })
}

/// A 2 MiB FAT superfloppy in `dir`.
fn blank_fat(dir: &Path) -> PathBuf {
    let path = dir.join("vol.img");
    let bytes = fs_pc::fat::create_blank_fat(2 * 1024 * 1024, Some("T")).unwrap();
    std::fs::write(&path, bytes).unwrap();
    path
}

/// A zstd backup of a blank FAT image, made through the Backup controller.
fn backup_of(dir: &Path) -> (PathBuf, PathBuf) {
    use crate::controller::backup::{BackupAction, BackupController, BackupServices};
    let img = blank_fat(dir);
    let out = dir.join("out");
    std::fs::create_dir_all(&out).unwrap();
    let mut b = BackupController::new(BackupServices {
        jobs: JobRegistry::new(),
        notify: Arc::new(NoNotify),
    });
    b.apply(BackupAction::OpenImage {
        path: img.clone(),
        rename: false,
    });
    b.wait_idle();
    b.apply(BackupAction::SetDestination(Some(out.clone())));
    b.apply(BackupAction::SetName("b".into()));
    b.apply(BackupAction::SetResize(false));
    b.apply(BackupAction::Start(None));
    b.wait_finished();
    assert_eq!(b.last_result(), Some(&Ok(())));
    (img, out.join("b"))
}

fn open(ctl: &mut RestoreController, folder: PathBuf) {
    ctl.apply(RestoreAction::OpenBackup(folder));
    ctl.wait_idle();
}

#[test]
fn a_folder_without_a_backup_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = controller();
    open(&mut ctl, dir.path().to_path_buf());
    assert!(!ctl.has_backup());
    assert_eq!(
        ctl.load_error(),
        Some("No backup found (metadata.json or Clonezilla image)")
    );
    assert_eq!(
        ctl.take_requests(),
        vec![HostRequest::RememberRecent(dir.path().to_path_buf())]
    );
}

#[test]
fn loading_a_backup_fills_the_size_rows() {
    let dir = tempfile::tempdir().unwrap();
    let (_, folder) = backup_of(dir.path());
    let mut ctl = controller();
    open(&mut ctl, folder.clone());
    assert!(ctl.has_backup());
    assert_eq!(ctl.backup_folder(), Some(folder.as_path()));
    assert_eq!(ctl.size_rows().len(), 1);
    assert_eq!(ctl.size_rows()[0].mode, SizeMode::Original);
    assert!(ctl.backup_summary().is_some());
    assert!(ctl
        .take_log()
        .iter()
        .any(|(_, m)| m.contains("Loaded backup")));
    assert_eq!(ctl.source_size(), Some(2 * 1024 * 1024));
    // The same folder again does not reload it.
    ctl.take_requests();
    ctl.apply(RestoreAction::OpenBackup(folder));
    assert!(ctl.take_requests().is_empty());
}

#[test]
fn start_asks_before_overwriting_and_restores_the_disk() {
    let dir = tempfile::tempdir().unwrap();
    let (img, folder) = backup_of(dir.path());
    let mut ctl = controller();
    assert_eq!(ctl.validate(), Err("Choose a backup folder.".to_string()));
    open(&mut ctl, folder);
    assert_eq!(ctl.validate(), Err("Choose a target device.".to_string()));
    ctl.apply(RestoreAction::SetTargetIsDevice(
        TargetSlot::FullDisk,
        false,
    ));
    assert_eq!(
        ctl.validate(),
        Err("Enter a target image path.".to_string())
    );
    let target = dir.path().join("restored.img");
    let fx = ctl.apply(RestoreAction::PickTargetImage(TargetSlot::FullDisk));
    assert!(
        matches!(&fx[..], [Effect::PickFile(s)] if s.purpose == purpose::TARGET_IMAGE && s.save)
    );
    ctl.apply(RestoreAction::Picked {
        purpose: purpose::TARGET_IMAGE,
        path: Some(target.clone()),
    });
    assert!(ctl.can_start());
    let fx = ctl.apply(RestoreAction::Start);
    match &fx[..] {
        [Effect::Confirm(spec)] => {
            assert_eq!(spec.purpose, purpose::RESTORE);
            assert!(spec
                .message
                .contains("restored.img will be permanently overwritten"));
        }
        other => panic!("expected a confirmation, got {other:?}"),
    }
    assert!(!ctl.is_running());
    ctl.apply(RestoreAction::Confirmed {
        purpose: purpose::RESTORE,
        yes: true,
    });
    assert!(ctl.is_running());
    ctl.wait_finished();
    assert_eq!(ctl.last_result(), Some(&Ok(())));
    assert!(ctl.progress().is_none());
    let log = ctl.take_log();
    assert!(log
        .iter()
        .any(|(_, m)| m == "Restore completed successfully."));
    assert_eq!(
        std::fs::read(&target).unwrap(),
        std::fs::read(&img).unwrap()
    );
}

#[test]
fn a_device_target_drops_the_trailing_free_space() {
    let dir = tempfile::tempdir().unwrap();
    let (_, folder) = backup_of(dir.path());
    let mut ctl = controller();
    open(&mut ctl, folder);
    ctl.apply(RestoreAction::SetTargetIsDevice(
        TargetSlot::FullDisk,
        false,
    ));
    assert!(ctl.expand_shown());
    ctl.apply(RestoreAction::SetExpand(true));
    ctl.apply(RestoreAction::SetExpandMib(3));
    assert_eq!(ctl.target_size(), 5 * 1024 * 1024);
    ctl.apply(RestoreAction::SetTargetIsDevice(TargetSlot::FullDisk, true));
    assert!(!ctl.expand().enabled);
    assert!(!ctl.expand_shown());
}

#[test]
fn a_dropped_file_is_a_partition_image() {
    let dir = tempfile::tempdir().unwrap();
    let img = blank_fat(dir.path());
    let mut ctl = controller();
    ctl.apply(RestoreAction::OpenDropped(img.clone()));
    ctl.wait_idle();
    assert_eq!(ctl.mode(), RestoreMode::SinglePartition);
    assert_eq!(ctl.source_image(), Some(img.as_path()));
    assert!(ctl.has_other_source());
    assert_eq!(ctl.partition_source_size(), Some(2 * 1024 * 1024));
    assert_eq!(ctl.validate(), Err("Choose a target.".to_string()));
}

#[test]
fn write_image_measures_the_source_and_hands_it_to_the_shell() {
    let dir = tempfile::tempdir().unwrap();
    let img = blank_fat(dir.path());
    let mut ctl = controller();
    ctl.apply(RestoreAction::SetMode(RestoreMode::WriteImage));
    let fx = ctl.apply(RestoreAction::PickWriteImage);
    assert!(matches!(&fx[..], [Effect::PickFile(s)] if s.purpose == purpose::WRITE_IMAGE));
    ctl.apply(RestoreAction::Picked {
        purpose: purpose::WRITE_IMAGE,
        path: Some(img.clone()),
    });
    ctl.wait_idle();
    assert_eq!(ctl.write_image(), Some((img.as_path(), 2 * 1024 * 1024)));
    // A table-less FAT volume: the writer may wrap it, but starts unticked.
    assert!(ctl.write_image_superfloppy().is_some());
    ctl.apply(RestoreAction::WriteImageTarget);
    let reqs = ctl.take_requests();
    let [HostRequest::WriteImage(open)] = reqs.as_slice() else {
        panic!("{reqs:?}");
    };
    assert_eq!(open.path, img);
    assert_eq!(open.size_bytes, Some(2 * 1024 * 1024));
    assert!(open.superfloppy && !open.has_partition_table && !open.synthesize);
}

#[test]
fn size_rows_offer_their_modes_and_clamp_a_custom_size() {
    let dir = tempfile::tempdir().unwrap();
    let (_, folder) = backup_of(dir.path());
    let mut ctl = controller();
    open(&mut ctl, folder);
    let row = ctl.size_rows()[0].clone();
    // The only row is the last, so it may fill.
    let modes = ctl.size_mode_choices(row.index);
    assert_eq!(modes.first(), Some(&SizeMode::Original));
    assert!(modes.contains(&SizeMode::Custom) && modes.contains(&SizeMode::FillRemaining));
    assert_eq!(
        modes.contains(&SizeMode::Minimum),
        row.minimum_size < row.original_size
    );
    // Into Custom starts at the original size; a typed size below the minimum is clamped.
    ctl.apply(RestoreAction::SetPartitionSize {
        index: row.index,
        mode: SizeMode::Custom,
        custom_mib: 0,
    });
    let mib = (row.original_size / (1024 * 1024)).max(1) as u32;
    assert_eq!(ctl.size_rows()[0].custom_mib, mib);
    ctl.apply(RestoreAction::SetPartitionSize {
        index: row.index,
        mode: SizeMode::Custom,
        custom_mib: 0,
    });
    let r = &ctl.size_rows()[0];
    assert_eq!(r.custom_mib, 0);
    assert_eq!(r.custom_mib_effective(), r.custom_bounds_mib().0);
    assert!(r.effective_size() >= 1024 * 1024);
}

#[test]
fn alignment_and_free_space_fields_are_clamped() {
    let dir = tempfile::tempdir().unwrap();
    let (_, folder) = backup_of(dir.path());
    let mut ctl = controller();
    open(&mut ctl, folder.clone());
    ctl.apply(RestoreAction::SetCustomAlignment(1_000_000));
    assert_eq!(ctl.alignment().1, ALIGN_SECTORS_MAX);
    ctl.apply(RestoreAction::SetExpandMib(u32::MAX));
    assert_eq!(ctl.expand().free_mib, ctl.expand_cap_mib());
    let mut nd = ctl.new_disk();
    nd.custom_alignment = 99_999;
    ctl.apply(RestoreAction::SetNewDisk(nd));
    assert_eq!(ctl.new_disk().custom_alignment, ALIGN_SECTORS_MAX);
    // The default image target sits beside the backup.
    let name = folder.file_name().unwrap().to_string_lossy().into_owned();
    assert_eq!(
        ctl.default_target_image(),
        Some(
            folder
                .parent()
                .unwrap()
                .join(format!("{name}_restored.img"))
        )
    );
}

#[test]
fn the_remote_target_turns_on_only_when_built() {
    let mut ctl = controller();
    ctl.apply(RestoreAction::SetRemote(true));
    assert_eq!(ctl.remote_active(), RestoreController::remote_available());
    if !RestoreController::remote_available() {
        let log = ctl.take_log();
        assert!(log
            .iter()
            .any(|(_, m)| m == crate::controller::backup::REMOTE_NOT_BUILT));
    }
}

fn fake_device(path: &Path) -> DiskDevice {
    DiskDevice {
        name: "disk".into(),
        path: path.to_path_buf(),
        size_bytes: 8 * 1024 * 1024,
        is_removable: true,
        is_read_only: false,
        is_system: false,
        bus_protocol: "test".into(),
        media_name: "Test Disk".into(),
        partitions: Vec::new(),
    }
}

#[test]
fn a_refreshed_list_drops_the_old_disks_scan() {
    let dir = tempfile::tempdir().unwrap();
    let a = fake_device(dir.path().join("a").as_path());
    let b = fake_device(dir.path().join("b").as_path());
    let mut ctl = controller();
    ctl.apply(RestoreAction::SelectDevice {
        slot: TargetSlot::SinglePartition,
        row: 0,
        device: a.clone(),
    });
    ctl.reconcile_devices(&[a]);
    assert_eq!(ctl.device_row(TargetSlot::SinglePartition), Some(0));
    ctl.reconcile_devices(std::slice::from_ref(&b));
    assert_eq!(
        ctl.device(TargetSlot::SinglePartition)
            .map(|d| d.path.clone()),
        Some(b.path)
    );
    ctl.reconcile_devices(&[]);
    assert!(ctl.device(TargetSlot::SinglePartition).is_none());
}

/// A 16 MiB HFS volume at 512-byte blocks on an MBR disk, backed up through the Backup controller.
fn hfs_backup(dir: &Path) -> PathBuf {
    use crate::controller::backup::{BackupAction, BackupController, BackupServices};
    const MIB: u64 = 1024 * 1024;
    let img = dir.join("hfs.img");
    let mut disk = std::io::Cursor::new(vec![0u8; (17 * MIB) as usize]);
    {
        let mut part = crate::model::format::Window::new(&mut disk, MIB, 16 * MIB);
        fs_mac::hfs::write_blank_hfs_into(&mut part, 0, 16 * MIB, 512, "Mac").unwrap();
    }
    let mut raw = disk.into_inner();
    raw[446 + 4] = 0xAF;
    raw[446 + 8..446 + 12].copy_from_slice(&2048u32.to_le_bytes());
    raw[446 + 12..446 + 16].copy_from_slice(&((16 * MIB / 512) as u32).to_le_bytes());
    raw[510] = 0x55;
    raw[511] = 0xAA;
    std::fs::write(&img, raw).unwrap();
    let out = dir.join("out");
    std::fs::create_dir_all(&out).unwrap();
    let mut b = BackupController::new(BackupServices {
        jobs: JobRegistry::new(),
        notify: Arc::new(NoNotify),
    });
    b.apply(BackupAction::OpenImage {
        path: img,
        rename: false,
    });
    b.wait_idle();
    b.apply(BackupAction::SetDestination(Some(out.clone())));
    b.apply(BackupAction::SetName("b".into()));
    b.apply(BackupAction::SetResize(false));
    b.apply(BackupAction::Start(None));
    b.wait_finished();
    assert_eq!(b.last_result(), Some(&Ok(())));
    out.join("b")
}

/// The first switch into Custom only seeds the original size, so the size goes in second.
fn custom_size(ctl: &mut RestoreController, mib: u32) {
    let index = ctl.size_rows()[0].index;
    for _ in 0..2 {
        ctl.apply(RestoreAction::SetPartitionSize {
            index,
            mode: SizeMode::Custom,
            custom_mib: mib,
        });
    }
}

#[test]
fn caveats_follow_the_size_and_a_volume_past_its_limit_cannot_start() {
    let dir = tempfile::tempdir().unwrap();
    let folder = hfs_backup(dir.path());
    let mut ctl = controller();
    open(&mut ctl, folder);
    let target = dir.path().join("r.img");
    ctl.apply(RestoreAction::SetTargetIsDevice(
        TargetSlot::FullDisk,
        false,
    ));
    ctl.apply(RestoreAction::SetTargetImage(
        TargetSlot::FullDisk,
        Some(target),
    ));
    assert!(ctl.size_caveats().is_empty(), "nothing to say at 16 MiB");
    assert!(ctl.validate().is_ok());

    custom_size(&mut ctl, 3 * 1024);
    let lines: Vec<String> = ctl.size_caveats().iter().map(|c| c.line()).collect();
    assert!(lines.iter().any(|l| l.contains("rebuilt")), "{lines:?}");
    assert!(lines.iter().any(|l| l.contains("System 7.5")), "{lines:?}");
    assert!(lines[0].starts_with("Partition 1: "), "{lines:?}");
    assert!(ctl.validate().is_ok());

    custom_size(&mut ctl, 5 * 1024);
    assert_eq!(ctl.size_caveats()[0].caveat.level, CaveatLevel::Blocked);
    assert!(ctl.validate().unwrap_err().contains("Classic HFS stops"));
    assert!(!ctl.can_start());
}

#[test]
fn a_single_partition_restore_rebuilds_an_outgrown_hfs() {
    use crate::model::restore_plan::{single_partition_config, PartitionSource};
    use crate::restore::RestoreProgress;
    use disk_core::filesystem::Filesystem as _;
    const MIB: u64 = 1024 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("hfs.img");
    let mut file = std::fs::File::create(&source).unwrap();
    fs_mac::hfs::write_blank_hfs_into(&mut file, 0, 16 * MIB, 512, "Mac").unwrap();
    file.set_len(16 * MIB).unwrap();
    let target = dir.path().join("disk.img");
    std::fs::File::create(&target)
        .unwrap()
        .set_len(64 * MIB)
        .unwrap();

    let mut config =
        single_partition_config(&PartitionSource::Image(&source), &target, 2048, 48 * MIB);
    config.target_is_device = false;
    let progress = Arc::new(Mutex::new(RestoreProgress::new()));
    crate::restore::single::run_single_partition_restore(config, progress).unwrap();

    let mut disk = std::fs::File::open(&target).unwrap();
    let mut fs = fs_mac::hfs::HfsFilesystem::open(&mut disk, MIB).unwrap();
    assert_eq!(fs.volume_summary().volume_name, "Mac");
    assert!(fs.fsck().unwrap().errors.is_empty());
    assert!(fs.total_size() > 40 * MIB, "{}", fs.total_size());
}

#[test]
fn a_new_apm_disk_gets_a_mac_driver() {
    use crate::model::restore_plan::{new_disk_config, PartitionSource};
    use crate::restore::RestoreProgress;
    const MIB: u64 = 1024 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("hfs.img");
    let mut file = std::fs::File::create(&source).unwrap();
    fs_mac::hfs::write_blank_hfs_into(&mut file, 0, 16 * MIB, 4096, "Mac").unwrap();
    file.set_len(16 * MIB).unwrap();
    let target = dir.path().join("disk.img");
    let choice = NewDiskChoice {
        table: NewTableType::Apm,
        alignment: AlignmentChoice::Modern1MB,
        custom_alignment: 2048,
        bootable: true,
    };
    let config = new_disk_config(
        &PartitionSource::Image(&source),
        &target,
        false,
        None,
        &choice,
    );
    let progress = Arc::new(Mutex::new(RestoreProgress::new()));
    crate::restore::single::run_single_partition_restore(config, progress).unwrap();

    let mut disk = std::fs::File::open(&target).unwrap();
    let apm = part_tables::apm::Apm::parse(&mut disk).unwrap();
    assert_eq!(apm.ddr.driver_count, 1);
    assert!(apm
        .entries
        .iter()
        .any(|e| e.partition_type == "Apple_Driver43"));
}

#[test]
fn a_backup_that_lost_its_mac_driver_offers_the_bundled_one() {
    use crate::controller::backup::{BackupAction, BackupController, BackupServices};
    use part_tables::apm::{build_minimal_apm, DriverInfo};
    const MIB: u64 = 1024 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let mut apm = build_minimal_apm(
        &[
            ("Apple_Driver43".to_string(), 64, 32),
            ("Apple_HFS".to_string(), 96, 32768),
        ],
        512,
        32864,
    );
    apm.ddr.driver_count = 1;
    apm.ddr.driver_info = vec![DriverInfo {
        block: 64,
        size: 19,
        kind: 1,
    }];
    let mut disk = std::io::Cursor::new(vec![0u8; 32864 * 512]);
    {
        let mut part = crate::model::format::Window::new(&mut disk, 96 * 512, 16 * MIB);
        fs_mac::hfs::write_blank_hfs_into(&mut part, 0, 16 * MIB, 512, "Mac").unwrap();
    }
    let mut raw = disk.into_inner();
    let head = apm.build_apm_blocks(Some(32864));
    raw[..head.len()].copy_from_slice(&head);
    let img = dir.path().join("mac.img");
    std::fs::write(&img, raw).unwrap();

    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let mut b = BackupController::new(BackupServices {
        jobs: JobRegistry::new(),
        notify: Arc::new(NoNotify),
    });
    b.apply(BackupAction::OpenImage {
        path: img,
        rename: false,
    });
    b.wait_idle();
    b.apply(BackupAction::SetDestination(Some(out.clone())));
    b.apply(BackupAction::SetName("b".into()));
    b.apply(BackupAction::SetResize(false));
    b.apply(BackupAction::Start(None));
    b.wait_finished();
    assert_eq!(b.last_result(), Some(&Ok(())));
    let folder = out.join("b");

    let mut ctl = controller();
    open(&mut ctl, folder.clone());
    assert!(!ctl.mac_driver_missing(), "a new backup keeps it");

    std::fs::remove_file(folder.join("apm-extras.bin")).unwrap();
    let mut ctl = controller();
    open(&mut ctl, folder);
    assert!(ctl.mac_driver_missing());
    let says = |ctl: &RestoreController, what: &str| {
        ctl.size_caveats().iter().any(|c| c.line().contains(what))
    };
    assert!(says(&ctl, "bundled Apple SCSI driver"));
    ctl.apply(RestoreAction::SetInstallMacDriver(false));
    assert!(says(&ctl, "cannot start up"));
}
