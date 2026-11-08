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
    assert_eq!(layout[0].kind, SegmentKind::Partition { color_index: 0 });
    assert_eq!(layout[0].partition, Some(ctl.partitions()[0].index));
    assert_eq!(layout[0].label, "T");
}

#[test]
fn a_selected_partition_stays_until_the_source_changes() {
    let (_dir, _img, mut ctl) = opened_fat();
    let index = ctl.partitions()[0].index;
    ctl.apply(InspectAction::SelectPartition(Some(index)));
    assert_eq!(ctl.selected_partition(), Some(index));
    ctl.apply(InspectAction::SelectPartition(Some(index + 99)));
    assert_eq!(ctl.selected_partition(), None);
    ctl.apply(InspectAction::SelectPartition(Some(index)));
    ctl.apply(InspectAction::Close);
    assert_eq!(ctl.selected_partition(), None);
}

#[test]
fn a_row_carries_what_the_list_shows() {
    let (_dir, _img, ctl) = opened_fat();
    let rows = ctl.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].number, "1");
    assert_eq!(rows[0].volume, "T");
    assert!(rows[0].actions.browse && !rows[0].container && !rows[0].locked);
}

#[test]
fn a_picked_image_or_folder_opens_and_a_cancel_changes_nothing() {
    let (_dir, img, mut ctl) = opened_fat();
    let fx = ctl.apply(InspectAction::ChooseImage);
    assert!(matches!(&fx[..], [Effect::PickFile(s)] if s.purpose == purpose::OPEN_IMAGE));
    let fx = ctl.apply(InspectAction::ChooseBackupFolder);
    assert!(matches!(&fx[..], [Effect::PickFolder(s)] if s.purpose == purpose::OPEN_BACKUP));
    ctl.apply(InspectAction::Picked {
        purpose: purpose::OPEN_IMAGE,
        path: None,
    });
    assert_eq!(ctl.image_path(), Some(img.as_path()));
    ctl.set_browse_dirty(true);
    let fx = ctl.apply(InspectAction::Picked {
        purpose: purpose::OPEN_BACKUP,
        path: Some(PathBuf::from("/nonexistent/rb-test-backup")),
    });
    assert!(matches!(&fx[..], [Effect::Confirm(_)]));
}

#[test]
fn a_defragment_on_a_volume_that_is_not_human68k_asks_nothing() {
    let (_dir, _img, mut ctl) = opened_fat();
    let index = ctl.partitions()[0].index;
    assert!(ctl.apply(InspectAction::Defragment(index)).is_empty());
}

#[test]
fn the_export_window_lists_formats_and_its_size_rows_follow_their_rules() {
    let (_dir, _img, mut ctl) = opened_fat();
    ctl.apply(InspectAction::OpenExport);
    let formats = ctl.export_formats();
    assert_eq!(formats.len(), EXPORT_FORMATS.len());
    assert!(formats
        .iter()
        .all(|(c, on)| *on && !c.label.is_empty() && c.label.is_ascii()));
    assert!(per_partition_refusal(ExportFormat::Qcow2).is_some());
    assert!(per_partition_refusal(ExportFormat::Vhd).is_none());
    let s = ctl.export_settings_mut();
    s.format = ExportFormat::Qcow2;
    s.set_whole_disk(false);
    assert!(s.whole_disk, "QCOW2 is whole disk only");
    s.format = ExportFormat::Raw;
    s.set_whole_disk(false);
    assert!(!s.whole_disk);
    let cfg = &mut s.configs[0];
    let modes = cfg.mode_choices();
    assert_eq!(modes.first(), Some(&SizeMode::Original));
    assert_eq!(modes.last(), Some(&SizeMode::Custom));
    assert!(!modes.contains(&SizeMode::FillRemaining));
    let (lo, hi) = cfg.custom_bounds_mib();
    assert_eq!(lo, (cfg.minimum_size / (1024 * 1024)).max(1) as u32);
    assert_eq!(hi, crate::model::size_mode::CUSTOM_MIB_MAX);
    cfg.set_choice(SizeMode::Custom);
    assert_eq!(cfg.custom_size_mib, lo, "Custom starts at the minimum");
    cfg.custom_size_mib = u32::MAX;
    assert_eq!(cfg.effective_size(), u64::from(hi) * 1024 * 1024);
}

#[test]
fn the_source_label_close_and_expand_preview_follow_the_source() {
    let (_dir, img, mut ctl) = opened_fat();
    let name = img.file_name().unwrap().to_string_lossy().to_string();
    assert!(ctl.source_label(&[]).ends_with(name.as_str()));
    assert!(ctl.can_close() && ctl.can_reinspect());
    let size = std::fs::metadata(&img).unwrap().len();
    assert_eq!(ctl.expand_image_preview(3), (size, size + 3 * 1024 * 1024));
    assert_eq!(
        expand_image_bytes(u32::MAX),
        u64::from(EXPAND_IMAGE_MIB_MAX) * 1024 * 1024
    );
    ctl.apply(InspectAction::Close);
    assert!(!ctl.can_close() && !ctl.can_reinspect());
    assert_eq!(ctl.source_label(&[]), "Select a device or image...");
}

#[test]
fn add_partition_needs_a_gap_and_the_dialog_sources_follow_the_image() {
    let dir = tempfile::tempdir().unwrap();
    let img = crate::controller::test_fixtures::mbr_with_gap(dir.path());
    let mut ctl = controller();
    ctl.apply(InspectAction::Open(InspectSource::Image {
        path: img.clone(),
        temp: None,
    }));
    settle(&mut ctl);
    assert!(ctl.can_edit_table() && ctl.can_add_partition());
    assert!(ctl.add_partition_hint().contains("gap at LBA"));
    assert_eq!(ctl.alto_resize_source(), Ok(img.clone()));
    assert_eq!(ctl.convert_source(), Some(img.clone()));
    assert!(!ctl.summarizing_hfs());
    ctl.apply(InspectAction::AddPartition);
    let v = ctl.table_editor().expect("the editor opens");
    assert!(!v.add_start.is_empty() && !v.add_size.is_empty());

    let (_d, _fat, mut fat) = opened_fat();
    assert!(
        !fat.can_add_partition(),
        "a superfloppy has no table to add to"
    );
    assert_eq!(fat.add_partition_hint(), "No free space available");
    fat.apply(InspectAction::AddPartition);
    assert!(fat.table_editor().is_none());
    let xdf = crate::controller::test_fixtures::blank_xdf(dir.path());
    fat.apply(InspectAction::Open(InspectSource::Image {
        path: xdf.clone(),
        temp: None,
    }));
    settle(&mut fat);
    assert_eq!(fat.convert_source(), Some(xdf));
    fat.apply(InspectAction::Close);
    assert!(fat.alto_resize_source().is_err());
}

#[test]
fn a_squashfs_image_browses_as_squashfs_not_hfs() {
    let dir = tempfile::tempdir().unwrap();
    let img = crate::controller::test_fixtures::squashfs_volume(dir.path());
    let mut ctl = controller();
    ctl.apply(InspectAction::Load(InspectSource::Image {
        path: img,
        temp: None,
    }));
    settle(&mut ctl);
    ctl.take_requests();
    let index = ctl.partitions()[0].index;
    ctl.apply(InspectAction::Browse(index));
    let Some(HostRequest::OpenBrowse(open)) = ctl.take_requests().into_iter().next() else {
        panic!("no browse request");
    };
    let mut fs = open.session.open().expect("opens as SquashFS");
    let root = fs.root().unwrap();
    let names: Vec<String> = fs
        .list_directory(&root)
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(names, ["a.txt"]);
}
