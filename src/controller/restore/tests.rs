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
    ctl.apply(RestoreAction::WriteImageTarget);
    assert_eq!(
        ctl.take_requests(),
        vec![HostRequest::WriteImage {
            path: img,
            size: 2 * 1024 * 1024
        }]
    );
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
fn building_on_a_device_asks_for_its_name() {
    let dir = tempfile::tempdir().unwrap();
    let dev = fake_device(dir.path().join("dev").as_path());
    let mut ctl = controller();
    ctl.apply(RestoreAction::SetMode(RestoreMode::BuildDisk));
    ctl.apply(RestoreAction::BuildDefine);
    assert!(!ctl.builder_open(), "no target yet");
    ctl.apply(RestoreAction::SetTargetIsDevice(
        TargetSlot::BuildDisk,
        true,
    ));
    ctl.apply(RestoreAction::SelectDevice {
        slot: TargetSlot::BuildDisk,
        row: 0,
        device: dev.clone(),
    });
    ctl.apply(RestoreAction::BuildDefine);
    assert!(ctl.builder_open());
    let fx = ctl.apply(RestoreAction::BuildApply);
    let spec = match &fx[..] {
        [Effect::AskText(spec)] => spec.clone(),
        other => panic!("expected the typed confirmation, got {other:?}"),
    };
    assert!(spec.validate("nope").is_err());
    assert!(spec.validate(dev.display_name().as_str()).is_ok());
    ctl.apply(RestoreAction::Answered {
        purpose: purpose::BUILD,
        text: Some("nope".into()),
    });
    assert!(!ctl.is_running());
    assert!(ctl.builder_open());
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
