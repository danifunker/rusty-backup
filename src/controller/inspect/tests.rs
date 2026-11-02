//! The Inspect controller without a view: open, results, tools and questions.

use super::*;
use crate::controller::Controller as _;
use crate::model::job::NoNotify;

fn controller() -> InspectController {
    InspectController::new(InspectServices {
        jobs: JobRegistry::new(),
        notify: Arc::new(NoNotify),
    })
}

/// Tick until no job runs, or give up after a few seconds.
fn settle(ctl: &mut InspectController) -> Vec<Effect> {
    let mut fx = Vec::new();
    for _ in 0..400 {
        fx.extend(ctl.tick());
        if ctl.loading().is_none() && ctl.tool_jobs().is_empty() && !ctl.probing() {
            fx.extend(ctl.tick());
            return fx;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!("jobs did not finish");
}

#[test]
fn an_image_opens_into_one_superfloppy_row_with_a_volume_label() {
    let dir = tempfile::tempdir().unwrap();
    let img = crate::controller::commander::blank_fat_image(dir.path());
    let mut ctl = controller();
    ctl.apply(InspectAction::Open(InspectSource::Image {
        path: img.clone(),
        temp: None,
    }));
    settle(&mut ctl);
    assert_eq!(ctl.image_path(), Some(img.as_path()));
    assert_eq!(ctl.partitions().len(), 1);
    let summary = ctl.table_summary().unwrap();
    assert!(summary.superfloppy);
    let part = ctl.partitions()[0].clone();
    assert!(ctl.row_actions(&part).browse);
    assert_eq!(ctl.volume_label(part.index), Some("T"));
    let log: Vec<String> = ctl.take_log().into_iter().map(|(_, m)| m).collect();
    assert!(log.iter().any(|l| l.starts_with("Inspecting ")), "{log:?}");
}

#[test]
fn an_image_file_is_prepared_in_a_job_then_opened() {
    use std::io::Write as _;
    let dir = tempfile::tempdir().unwrap();
    let img = crate::controller::commander::blank_fat_image(dir.path());
    // Inspect keeps a gzip-wrapped Amiga hard file as a container, so edits go back into it.
    let hdz = dir.path().join("vol.hdz");
    let mut gz = flate2::write::GzEncoder::new(
        std::fs::File::create(&hdz).unwrap(),
        flate2::Compression::fast(),
    );
    gz.write_all(&std::fs::read(&img).unwrap()).unwrap();
    gz.finish().unwrap();
    let mut ctl = controller();
    ctl.apply(InspectAction::Open(InspectSource::ImageFile(img.clone())));
    assert!(ctl.is_preparing() && ctl.loading().is_some());
    settle(&mut ctl);
    assert_eq!(ctl.image_path(), Some(img.as_path()));
    assert_eq!(ctl.partitions().len(), 1);
    ctl.apply(InspectAction::Load(InspectSource::ImageFile(hdz.clone())));
    settle(&mut ctl);
    assert_eq!(ctl.image_path(), Some(hdz.as_path()));
    assert_eq!(ctl.partitions().len(), 1);
}

fn opened_fat() -> (tempfile::TempDir, PathBuf, InspectController) {
    let dir = tempfile::tempdir().unwrap();
    let img = crate::controller::commander::blank_fat_image(dir.path());
    let mut ctl = controller();
    ctl.apply(InspectAction::Load(InspectSource::Image {
        path: img.clone(),
        temp: None,
    }));
    settle(&mut ctl);
    ctl.take_requests();
    ctl.take_log();
    (dir, img, ctl)
}

#[test]
fn switching_with_unsaved_browse_edits_asks_first_and_cancel_keeps_the_source() {
    let (dir, img, mut ctl) = opened_fat();
    let other = dir.path().join("other.img");
    std::fs::copy(img.as_path(), other.as_path()).unwrap();
    ctl.set_browse_dirty(true);
    let fx = ctl.apply(InspectAction::Open(InspectSource::Image {
        path: other.clone(),
        temp: None,
    }));
    assert!(matches!(&fx[..], [Effect::Confirm(c)] if c.purpose == purpose::DISCARD_EDITS));
    ctl.apply(InspectAction::Confirmed {
        purpose: purpose::DISCARD_EDITS,
        yes: false,
    });
    assert_eq!(ctl.image_path(), Some(img.as_path()));
    assert!(ctl.take_requests().is_empty());

    ctl.apply(InspectAction::Open(InspectSource::Image {
        path: other.clone(),
        temp: None,
    }));
    ctl.apply(InspectAction::Confirmed {
        purpose: purpose::DISCARD_EDITS,
        yes: true,
    });
    assert_eq!(ctl.image_path(), Some(other.as_path()));
    assert!(matches!(
        ctl.take_requests().first(),
        Some(HostRequest::CloseBrowse)
    ));
}

#[test]
fn a_mac_archive_goes_to_the_archives_tab_and_changes_nothing() {
    let (dir, img, mut ctl) = opened_fat();
    let sit = dir.path().join("stuff.sit");
    std::fs::write(sit.as_path(), b"SIT!").unwrap();
    ctl.apply(InspectAction::Open(InspectSource::Image {
        path: sit.clone(),
        temp: None,
    }));
    assert_eq!(ctl.image_path(), Some(img.as_path()));
    let reqs = ctl.take_requests();
    assert!(matches!(&reqs[..], [HostRequest::OpenArchive(p)] if *p == sit));
}

#[test]
fn closing_a_backup_tells_the_host_and_a_missing_folder_reports_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = controller();
    ctl.apply(InspectAction::Open(InspectSource::Backup {
        folder: dir.path().join("nope"),
    }));
    settle(&mut ctl);
    assert!(ctl.last_error().is_some());
    assert!(ctl.backup_folder().is_some());
    ctl.take_requests();
    ctl.apply(InspectAction::Close);
    let reqs = ctl.take_requests();
    assert!(reqs.iter().any(|r| matches!(r, HostRequest::BackupClosed)));
    assert!(!ctl.has_source());
}

#[test]
fn a_fat_volume_checks_clean_and_its_row_offers_browse_and_check() {
    let (_dir, _img, mut ctl) = opened_fat();
    let part = ctl.partitions()[0].clone();
    let acts = ctl.row_actions(&part);
    assert!(acts.browse && acts.check);
    assert!(!acts.defragment && !acts.resize_alto && !acts.expand_hfs);
    ctl.apply(InspectAction::Check(part.index));
    settle(&mut ctl);
    let view = ctl.fsck_view().expect("the check window opens");
    assert!(view.result.is_clean());
    ctl.apply(InspectAction::CloseCheck);
    assert!(ctl.fsck_view().is_none());
}

#[test]
fn browse_hands_the_host_a_session_it_may_edit_in_place() {
    let (_dir, img, mut ctl) = opened_fat();
    let index = ctl.partitions()[0].index;
    ctl.apply(InspectAction::Browse(index));
    let reqs = ctl.take_requests();
    let Some(HostRequest::OpenBrowse(open)) = reqs.into_iter().next() else {
        panic!("no browse request");
    };
    assert_eq!(open.session.source_path.as_deref(), Some(img.as_path()));
    assert!(matches!(open.edit, BrowseEdit::InPlace));
}

#[test]
fn export_formats_force_their_layout() {
    let (_dir, _img, mut ctl) = opened_fat();
    ctl.apply(InspectAction::OpenExport);
    assert!(ctl.export_open());
    ctl.export_settings_mut().whole_disk = false;
    ctl.export_settings_mut().format = ExportFormat::Qcow2;
    assert!(ctl.export_settings_mut().whole_disk);
    ctl.export_settings_mut().format = ExportFormat::Hfv;
    assert!(!ctl.export_settings_mut().whole_disk);
    let fx = ctl.apply(InspectAction::StartExport(
        crate::rbformats::chd_options::ChdOptions::defaults_for(ChdProfile::Hd),
    ));
    assert!(matches!(&fx[..], [Effect::PickFolder(p)] if p.purpose == purpose::EXPORT_HFV));
    assert!(!ctl.export_open());
    // A cancelled picker starts nothing.
    ctl.apply(InspectAction::Picked {
        purpose: purpose::EXPORT_HFV,
        path: None,
    });
    assert!(!ctl.export_running());
}

#[test]
fn the_word_swap_refuses_to_overwrite_its_source() {
    let (_dir, img, mut ctl) = opened_fat();
    let fx = ctl.apply(InspectAction::SwapWordOrder);
    let Some(Effect::PickFile(spec)) = fx.first() else {
        panic!("no picker");
    };
    assert_eq!(spec.file_name, "vol-swab16.img");
    ctl.apply(InspectAction::Picked {
        purpose: purpose::SWAB_DEST,
        path: Some(img.clone()),
    });
    assert!(!ctl.swabbing());
    let log = ctl.take_log();
    assert!(log
        .iter()
        .any(|(l, m)| *l == LogLevel::Error && m.contains("different destination")));
}

#[test]
fn a_device_that_left_the_list_is_closed() {
    let mut ctl = controller();
    let identity = crate::device::DeviceIdentity {
        path: PathBuf::from("/nonexistent/rb-test-device"),
        size_bytes: 1024,
        media_name: "Test".into(),
    };
    ctl.apply(InspectAction::Load(InspectSource::Device {
        slot: 0,
        identity,
    }));
    settle(&mut ctl);
    assert_eq!(ctl.device_slot(), Some(0));
    ctl.reconcile_devices(&[]);
    assert!(!ctl.has_source());
    let log = ctl.take_log();
    assert!(log.iter().any(|(_, m)| m.contains("no longer listed")));
}

#[test]
fn the_layout_bar_shows_each_volume_and_trailing_free_space() {
    let (_dir, _img, mut ctl) = opened_fat();
    let layout = ctl.layout();
    assert_eq!(layout.len(), 1);
    assert_eq!(layout[0].kind, SegmentKind::Partition);
    assert_eq!(layout[0].label, "T");
}

#[test]
fn a_defragment_on_a_volume_that_is_not_human68k_asks_nothing() {
    let (_dir, _img, mut ctl) = opened_fat();
    let index = ctl.partitions()[0].index;
    assert!(ctl.apply(InspectAction::Defragment(index)).is_empty());
}
