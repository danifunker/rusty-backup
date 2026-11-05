use std::path::PathBuf;
use std::sync::Arc;

use super::*;
use crate::controller::Controller as _;

fn controller() -> BackupController {
    BackupController::new(BackupServices {
        jobs: JobRegistry::new(),
        notify: Arc::new(NoNotify),
    })
}

/// A 2 MiB FAT superfloppy in `dir`.
fn blank_fat(dir: &std::path::Path) -> PathBuf {
    let path = dir.join("vol.img");
    let bytes = fs_pc::fat::create_blank_fat(2 * 1024 * 1024, Some("T")).unwrap();
    std::fs::write(&path, bytes).unwrap();
    path
}

fn open(ctl: &mut BackupController, path: PathBuf) {
    ctl.apply(BackupAction::OpenImage { path, rename: true });
    ctl.wait_idle();
}

#[test]
fn opening_an_image_previews_it_and_names_the_backup() {
    let dir = tempfile::tempdir().unwrap();
    let img = blank_fat(dir.path());
    let mut ctl = controller();
    open(&mut ctl, img.clone());
    assert_eq!(ctl.image_path(), Some(img.as_path()));
    assert_eq!(
        ctl.take_requests(),
        vec![HostRequest::RememberRecent(img.clone())]
    );
    assert_eq!(ctl.partition_count(), 1);
    assert_eq!(ctl.selected_count(), 1);
    assert!(ctl.name().contains("vol"), "{}", ctl.name());
    assert!(ctl.take_log().iter().any(|(_, m)| m.contains("Source has 1")));
    assert_eq!(ctl.default_destination(), Some(dir.path().to_path_buf()));
}

#[test]
fn start_is_refused_until_the_form_is_complete() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = controller();
    assert_eq!(ctl.validate(), Err("Select a source first.".to_string()));
    open(&mut ctl, blank_fat(dir.path()));
    assert_eq!(
        ctl.validate(),
        Err("Choose a destination folder.".to_string())
    );
    ctl.apply(BackupAction::SetDestination(Some(dir.path().to_path_buf())));
    ctl.apply(BackupAction::SetName("  ".into()));
    assert_eq!(ctl.validate(), Err("Enter a backup name.".to_string()));
    ctl.apply(BackupAction::SetName("b".into()));
    ctl.apply(BackupAction::SelectNone);
    assert!(!ctl.can_start());
    ctl.apply(BackupAction::Start(None));
    assert_eq!(
        ctl.last_result(),
        Some(&Err("Select at least one partition.".to_string()))
    );
    ctl.apply(BackupAction::SelectAll);
    assert!(ctl.can_start());
}

#[test]
fn a_missing_image_is_not_found() {
    let mut ctl = controller();
    open(&mut ctl, PathBuf::from("/nonexistent/rb-backup-test/missing.img"));
    assert_eq!(ctl.validate(), Err("Source not found.".to_string()));
    assert!(ctl.load_error().is_some());
}

#[test]
fn sector_by_sector_turns_resize_off() {
    let mut ctl = controller();
    assert!(ctl.options().resize);
    ctl.apply(BackupAction::SetSectorBySector(true));
    assert!(!ctl.options().resize);
    ctl.apply(BackupAction::SetResize(true));
    assert!(!ctl.options().resize);
}

#[test]
fn pickers_are_effects() {
    let mut ctl = controller();
    let fx = ctl.apply(BackupAction::PickImage);
    assert!(matches!(&fx[..], [Effect::PickFile(s)] if s.purpose == purpose::SOURCE_IMAGE));
    let fx = ctl.apply(BackupAction::PickDestination);
    assert!(matches!(&fx[..], [Effect::PickFolder(s)] if s.purpose == purpose::DESTINATION));
    let dir = tempfile::tempdir().unwrap();
    ctl.apply(BackupAction::Picked {
        purpose: purpose::DESTINATION,
        path: Some(dir.path().to_path_buf()),
    });
    assert_eq!(ctl.destination(), Some(dir.path()));
}

#[test]
fn a_zstd_backup_runs_to_the_end() {
    let dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let mut ctl = controller();
    open(&mut ctl, blank_fat(dir.path()));
    ctl.apply(BackupAction::SetDestination(Some(out.path().to_path_buf())));
    ctl.apply(BackupAction::SetName("b".into()));
    ctl.apply(BackupAction::Start(None));
    assert!(ctl.is_running());
    ctl.wait_finished();
    assert_eq!(ctl.last_result(), Some(&Ok(())));
    assert!(out.path().join("b").join("metadata.json").exists());
    assert!(ctl.progress().is_none());
}

#[test]
fn start_during_the_preview_waits_for_it() {
    let dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let mut ctl = controller();
    ctl.apply(BackupAction::SetDestination(Some(out.path().to_path_buf())));
    ctl.apply(BackupAction::OpenImage {
        path: blank_fat(dir.path()),
        rename: true,
    });
    ctl.apply(BackupAction::SetName("w".into()));
    ctl.apply(BackupAction::Start(None));
    // The preview may already be done; either way the backup starts and ends.
    ctl.wait_idle();
    ctl.wait_finished();
    assert_eq!(ctl.last_result(), Some(&Ok(())));
    assert!(out.path().join("w").join("metadata.json").exists());
}

#[test]
fn vhd_asks_for_its_layout() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = controller();
    open(&mut ctl, blank_fat(dir.path()));
    ctl.apply(BackupAction::SetDestination(Some(dir.path().to_path_buf())));
    ctl.apply(BackupAction::SetFormat(CompressionType::Vhd));
    ctl.apply(BackupAction::Start(None));
    assert!(ctl.vhd_open());
    assert!(ctl.vhd_whole_disk());
    assert_eq!(ctl.vhd_rows().len(), 1);
    let row = ctl.vhd_rows()[0].clone();
    ctl.apply(BackupAction::VhdSize {
        index: row.index,
        mode: SizeMode::Custom,
        custom_mib: 1,
    });
    assert_eq!(ctl.vhd_rows()[0].effective_size(), 1024 * 1024);
    ctl.apply(BackupAction::VhdClose);
    assert!(!ctl.vhd_open());
    assert!(!ctl.is_running());
}

#[test]
fn closing_the_source_forgets_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = controller();
    open(&mut ctl, blank_fat(dir.path()));
    ctl.apply(BackupAction::CloseSource);
    assert!(!ctl.has_source());
    assert_eq!(ctl.partition_count(), 0);
    assert!(ctl.rows().is_empty());
}

#[test]
fn a_vanished_device_is_cleared() {
    let mut ctl = controller();
    let device = crate::device::DiskDevice {
        name: "sdz".into(),
        path: PathBuf::from("/dev/sdz-rb-test"),
        size_bytes: 1 << 30,
        is_removable: true,
        is_read_only: false,
        is_system: false,
        bus_protocol: String::new(),
        media_name: String::new(),
        partitions: Vec::new(),
    };
    ctl.apply(BackupAction::SelectDevice {
        slot: 0,
        device: device.clone(),
    });
    assert_eq!(ctl.device_slot(), Some(0));
    assert!(ctl.can_scan_device());
    ctl.reconcile_devices(std::slice::from_ref(&device));
    assert_eq!(ctl.device_slot(), Some(0));
    ctl.reconcile_devices(&[]);
    assert_eq!(ctl.device_slot(), None);
    assert!(ctl
        .take_log()
        .iter()
        .any(|(_, m)| m.contains("no longer listed")));
}

#[test]
fn a_vhd_custom_size_starts_at_the_minimum_and_is_clamped() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = controller();
    open(&mut ctl, blank_fat(dir.path()));
    ctl.apply(BackupAction::SetDestination(Some(dir.path().to_path_buf())));
    ctl.apply(BackupAction::SetFormat(CompressionType::Vhd));
    ctl.apply(BackupAction::Start(None));
    let row = ctl.vhd_rows()[0].clone();
    assert_eq!(row.mode_choices()[0], SizeMode::Original);
    assert_eq!(row.mode_choices().last(), Some(&SizeMode::Custom));
    let (lo, hi) = row.custom_bounds_mib();
    assert!(lo >= 1 && hi >= lo);
    ctl.apply(BackupAction::VhdSize {
        index: row.index,
        mode: SizeMode::Custom,
        custom_mib: 999,
    });
    assert_eq!(ctl.vhd_rows()[0].custom_mib, lo);
    ctl.apply(BackupAction::VhdSize {
        index: row.index,
        mode: SizeMode::Custom,
        custom_mib: 1_000_000,
    });
    assert_eq!(ctl.vhd_rows()[0].custom_mib, 1_000_000);
    assert_eq!(ctl.vhd_rows()[0].effective_size(), hi as u64 * 1024 * 1024);
}

#[test]
fn remote_drive_opens_only_when_built() {
    let mut ctl = controller();
    ctl.apply(BackupAction::RemoteOpen);
    assert_eq!(
        ctl.remote_dialog_open(),
        BackupController::remote_available()
    );
    if !BackupController::remote_available() {
        assert!(ctl.take_log().iter().any(|(_, m)| m == REMOTE_NOT_BUILT));
    }
}
