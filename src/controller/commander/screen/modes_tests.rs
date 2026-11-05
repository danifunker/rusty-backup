//! The Inspect browse view's modes: Edit Mode on a plain image, a CHD and a zstd backup, the
//! unsaved-edits question, CHD Info and "Open as Disk Image".

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use super::*;
use crate::controller::browser::explorer_tests::{fat_volume, root_file};
use crate::controller::browser::{PaneProfile, PaneServices};
use crate::controller::commander::CommanderAction;
use crate::controller::keys::KeyMap;
use crate::controller::Controller;
use crate::model::edit_queue::StagedEdit;

fn settle(ctl: &mut CommanderController) {
    let start = Instant::now();
    ctl.tick();
    while ctl.is_working() {
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "a job never ended"
        );
        std::thread::sleep(Duration::from_millis(5));
        ctl.tick();
    }
}

fn session(path: &Path) -> BrowseSession {
    BrowseSession {
        source_path: Some(path.to_path_buf()),
        ..Default::default()
    }
}

fn inspect(session: BrowseSession) -> CommanderController {
    let mut ctl = CommanderController::single(
        PaneServices::headless(),
        KeyMap::for_this_platform(),
        PaneProfile::inspect(),
    );
    ctl.open_inspect(session, "vol");
    settle(&mut ctl);
    assert!(ctl.pane(Side::Left).listing.is_loaded(), "{}", ctl.status());
    ctl
}

fn mode(ctl: &mut CommanderController, m: ModeAction) {
    ctl.apply(CommanderAction::Mode(m));
    settle(ctl);
}

fn stage_folder(ctl: &mut CommanderController, name: &str) {
    let parent = ctl.pane(Side::Left).entry_at("/").unwrap();
    ctl.host_pane().queue.push(StagedEdit::CreateDirectory {
        parent,
        name: name.into(),
        qnx2_attrs: None,
    });
}

fn root_names(path: &Path) -> Vec<String> {
    let mut fs = session(path).open().unwrap();
    let root = fs.root().unwrap();
    let list = fs.list_directory(&root).unwrap();
    list.into_iter().map(|e| e.name).collect()
}

#[test]
fn edit_mode_on_an_image_probes_reads_free_space_and_asks_before_dropping_edits() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = inspect(session(&fat_volume(dir.path())));
    mode(&mut ctl, ModeAction::ToggleEditMode);
    assert!(!ctl.edit_mode(), "not editable until the host says so");
    ctl.inspect_edit_in_place();
    assert!(ctl.pane(Side::Left).is_read_only());
    mode(&mut ctl, ModeAction::ToggleEditMode);
    assert!(ctl.edit_mode() && !ctl.pane(Side::Left).is_read_only());
    settle(&mut ctl);
    assert!(ctl.space().is_some_and(|s| s.free > 0 && s.after.is_none()));
    stage_folder(&mut ctl, "NEW");
    assert!(ctl.space().unwrap().after.is_some());
    mode(&mut ctl, ModeAction::ToggleEditMode);
    assert!(ctl.unsaved_shown() && ctl.edit_mode());
    mode(&mut ctl, ModeAction::Unsaved(UnsavedChoice::Discard));
    assert!(!ctl.unsaved_shown() && !ctl.edit_mode());
    assert!(ctl.pane(Side::Left).queue.is_empty());
}

#[test]
fn closing_with_edits_asks_and_apply_writes_then_closes() {
    let dir = tempfile::tempdir().unwrap();
    let img = fat_volume(dir.path());
    let mut ctl = inspect(session(&img));
    ctl.inspect_edit_in_place();
    mode(&mut ctl, ModeAction::ToggleEditMode);
    let host = dir.path().join("NOTE.TXT");
    std::fs::write(&host, b"note").unwrap();
    let add = BrowserPaneAction::AddHostPaths {
        paths: vec![host],
        folder: None,
    };
    ctl.apply(CommanderAction::Pane(Side::Left, add));
    assert!(ctl.has_unsaved_edits());
    mode(&mut ctl, ModeAction::RequestClose);
    assert!(ctl.unsaved_shown() && !ctl.take_close_request());
    mode(&mut ctl, ModeAction::Unsaved(UnsavedChoice::Apply));
    settle(&mut ctl);
    assert!(ctl.take_close_request());
    assert_eq!(root_file(&img, "NOTE.TXT").as_deref(), Some(&b"note"[..]));
    assert_eq!(
        ctl.take_mode_notice().as_deref(),
        Some("Applied 1 edit(s) successfully")
    );
}

/// An uncompressed CHD holding `img`.
#[cfg(feature = "chd")]
fn chd_of(img: &Path, dir: &Path) -> PathBuf {
    use libchdman_rs::hd::{create_from_reader, HdCreateOptions};
    let data = std::fs::read(img).unwrap();
    let chd = dir.join("disk.chd");
    let opts = HdCreateOptions {
        logical_size: data.len() as u64,
        hunk_size: 4096,
        unit_size: 512,
        codecs: [0u32; 4],
        geometry: None,
        ident: None,
    };
    let mut src = std::io::Cursor::new(&data);
    create_from_reader(&mut src, &chd, opts, &mut |_| {}, &|| false).unwrap();
    chd
}

#[cfg(feature = "chd")]
#[test]
fn a_chd_is_backed_up_edited_and_its_writes_count_as_unsaved_until_edit_mode_ends() {
    let dir = tempfile::tempdir().unwrap();
    let chd = chd_of(&fat_volume(dir.path()), dir.path());
    let mut ctl = inspect(session(&chd));
    assert!(ctl.is_chd_source());
    mode(&mut ctl, ModeAction::ChdInfo);
    assert!(ctl.chd_info().is_some_and(|t| !t.is_empty()));
    mode(&mut ctl, ModeAction::CloseChdInfo);
    assert!(ctl.chd_info().is_none());

    ctl.inspect_edit_in_place();
    mode(&mut ctl, ModeAction::ToggleEditMode);
    assert!(ctl.editing_chd(), "{:?}", ctl.take_mode_notice());
    assert!(crate::rbformats::chd_edit::backup_path_for(&chd).exists());
    stage_folder(&mut ctl, "NEWDIR");
    ctl.apply(CommanderAction::Mode(ModeAction::Apply));
    settle(&mut ctl);
    assert!(ctl.pane(Side::Left).queue.is_empty());
    assert!(
        ctl.has_unsaved_edits(),
        "a written diff waits for the flatten"
    );
    mode(&mut ctl, ModeAction::ToggleEditMode);
    assert!(ctl.unsaved_shown());
    mode(&mut ctl, ModeAction::Unsaved(UnsavedChoice::Apply));
    assert!(!ctl.edit_mode() && !ctl.has_unsaved_edits());
    assert!(root_names(&chd).contains(&"NEWDIR".to_string()));
}

#[test]
fn a_zstd_backup_partition_is_decompressed_edited_and_recompressed() {
    let dir = tempfile::tempdir().unwrap();
    let img = fat_volume(dir.path());
    let size = std::fs::metadata(&img).unwrap().len();
    let base = dir.path().join("partition-0");
    let mut log = |_: &str| {};
    let files = crate::rbformats::compress_file_to_archive(
        &img,
        &base,
        "zstd",
        None,
        &mut |_| {},
        &|| false,
        &mut log,
    )
    .unwrap();
    let archive = dir.path().join(&files[0]);
    let s = BrowseSession::streaming(&archive, 0, None).unwrap();
    let mut ctl = inspect(s);
    ctl.inspect_edit_archive(ArchiveEditContext {
        archive_path: archive.clone(),
        compression_type: "zstd".into(),
        original_size: size,
        compacted: false,
        metadata_path: PathBuf::new(),
        partition_index: 0,
        checksum_type: "sha256".into(),
    });
    mode(&mut ctl, ModeAction::ToggleEditMode);
    assert!(ctl.editing_archive(), "{:?}", ctl.take_mode_notice());
    let temp = dir.path().join("partition-0-edit.img");
    assert!(temp.exists());
    stage_folder(&mut ctl, "ZDIR");
    ctl.apply(CommanderAction::Mode(ModeAction::Apply));
    settle(&mut ctl);
    settle(&mut ctl);
    assert!(!ctl.edit_mode());
    assert_eq!(
        ctl.take_mode_notice().as_deref(),
        Some("Changes saved successfully.")
    );
    assert!(!temp.exists());
    let out = dir.path().join("check.img");
    crate::rbformats::decompress_partition_to_file(
        &archive,
        "zstd",
        &out,
        size,
        false,
        &mut |_| {},
        &|| false,
    )
    .unwrap();
    assert!(root_names(&out).contains(&"ZDIR".to_string()));
}

#[test]
fn open_as_disk_image_copies_the_file_out_for_the_host() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = inspect(session(&fat_volume(dir.path())));
    mode(&mut ctl, ModeAction::OpenNested("/HELLO.TXT".into()));
    settle(&mut ctl);
    let (path, _guard) = ctl.take_nested_open().expect("handed over");
    assert_eq!(std::fs::read(path).unwrap(), b"hello\r\n");
    stage_folder(&mut ctl, "X");
    mode(&mut ctl, ModeAction::OpenNested("/HELLO.TXT".into()));
    assert!(ctl.take_nested_open().is_none());
    assert!(ctl.status().starts_with("Apply or discard"));
}

#[test]
fn edit_mode_on_squashfs_asks_for_a_size_budget_first() {
    use crate::controller::commander::{BudgetAction, BudgetMode};
    use fs_squashfs::squashfs_write::{write_squashfs, BuildNode, BuildOptions};
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("vol.sqfs");
    let root = BuildNode::dir(
        "",
        0o755,
        vec![BuildNode::file("a.txt", 0o644, b"hi".to_vec())],
    );
    let mut out = std::fs::File::create(&img).unwrap();
    write_squashfs(&mut out, &root, &BuildOptions::default()).unwrap();
    drop(out);
    let mut ctl = inspect(session(&img));
    ctl.inspect_edit_in_place();
    mode(&mut ctl, ModeAction::ToggleEditMode);
    let form = *ctl.size_budget().expect("the budget dialog is up");
    assert_eq!(form.mode(), BudgetMode::Fit);
    assert!(!ctl.edit_mode());
    mode(
        &mut ctl,
        ModeAction::Budget(BudgetAction::SetMode(BudgetMode::Headroom)),
    );
    assert_eq!(
        ctl.size_budget().map(|f| f.mode()),
        Some(BudgetMode::Headroom)
    );
    mode(&mut ctl, ModeAction::Budget(BudgetAction::Start));
    assert!(
        ctl.size_budget().is_none() && ctl.edit_mode(),
        "{:?}",
        ctl.take_mode_error()
    );
}

/// A zstd backup of a FAT volume: the archive, its decoded size, and its edit context.
fn zstd_backup(dir: &Path) -> (PathBuf, u64, ArchiveEditContext) {
    let img = fat_volume(dir);
    let size = std::fs::metadata(&img).unwrap().len();
    let base = dir.join("partition-0");
    let mut log = |_: &str| {};
    let files = crate::rbformats::compress_file_to_archive(
        &img,
        &base,
        "zstd",
        None,
        &mut |_| {},
        &|| false,
        &mut log,
    )
    .unwrap();
    let archive = dir.join(&files[0]);
    let ctx = ArchiveEditContext {
        archive_path: archive.clone(),
        compression_type: "zstd".into(),
        original_size: size,
        compacted: false,
        metadata_path: PathBuf::new(),
        partition_index: 0,
        checksum_type: "sha256".into(),
    };
    (archive, size, ctx)
}

fn archive_names(archive: &Path, size: u64, dir: &Path) -> Vec<String> {
    let out = dir.join("check.img");
    crate::rbformats::decompress_partition_to_file(
        archive,
        "zstd",
        &out,
        size,
        false,
        &mut |_| {},
        &|| false,
    )
    .unwrap();
    root_names(&out)
}

/// The TUI Explorer writes each edit at once, so the temp copy holds what the close must save.
#[test]
fn the_explorer_edits_a_zstd_backup_in_edit_mode_and_saves_it_on_close() {
    let dir = tempfile::tempdir().unwrap();
    let (archive, size, ctx) = zstd_backup(dir.path());
    let mut ctl = CommanderController::single(
        PaneServices::headless(),
        KeyMap::explorer_terminal(),
        PaneProfile::explorer(),
    );
    ctl.open_inspect(BrowseSession::streaming(&archive, 0, None).unwrap(), "vol");
    settle(&mut ctl);
    ctl.inspect_edit_archive(ctx);
    let ctrl = crate::controller::keys::Mods {
        ctrl: true,
        ..Default::default()
    };
    ctl.apply(CommanderAction::Key(
        crate::controller::keys::Key::Char('w'),
        ctrl,
    ));
    settle(&mut ctl);
    assert!(ctl.editing_archive(), "{:?}", ctl.take_mode_notice());
    let host = dir.path().join("NOTE.TXT");
    std::fs::write(&host, b"note").unwrap();
    let add = BrowserPaneAction::AddHostPaths {
        paths: vec![host],
        folder: None,
    };
    ctl.apply(CommanderAction::Pane(Side::Left, add));
    settle(&mut ctl);
    assert!(ctl.pane(Side::Left).queue.is_empty(), "written at once");
    assert!(ctl.has_unsaved_edits(), "the temp copy is not saved yet");
    let esc = crate::controller::keys::Key::Escape;
    let fx = ctl.apply(CommanderAction::Key(esc, Default::default()));
    assert!(fx.is_empty() && ctl.unsaved_shown(), "{fx:?}");
    mode(&mut ctl, ModeAction::Unsaved(UnsavedChoice::Apply));
    settle(&mut ctl);
    assert!(ctl.take_close_request(), "{:?}", ctl.take_mode_notice());
    assert!(archive_names(&archive, size, dir.path()).contains(&"NOTE.TXT".to_string()));
}

#[test]
fn discarding_a_written_archive_copy_leaves_the_archive_as_it_was() {
    let dir = tempfile::tempdir().unwrap();
    let (archive, size, ctx) = zstd_backup(dir.path());
    let mut ctl = CommanderController::single(
        PaneServices::headless(),
        KeyMap::explorer_terminal(),
        PaneProfile::explorer(),
    );
    ctl.open_inspect(BrowseSession::streaming(&archive, 0, None).unwrap(), "vol");
    settle(&mut ctl);
    ctl.inspect_edit_archive(ctx);
    mode(&mut ctl, ModeAction::ToggleEditMode);
    let temp = dir.path().join("partition-0-edit.img");
    assert!(temp.exists());
    let host = dir.path().join("NOTE.TXT");
    std::fs::write(&host, b"note").unwrap();
    let add = BrowserPaneAction::AddHostPaths {
        paths: vec![host],
        folder: None,
    };
    ctl.apply(CommanderAction::Pane(Side::Left, add));
    settle(&mut ctl);
    mode(&mut ctl, ModeAction::ToggleEditMode);
    assert!(ctl.unsaved_shown());
    mode(&mut ctl, ModeAction::Unsaved(UnsavedChoice::Discard));
    settle(&mut ctl);
    assert!(!ctl.edit_mode() && !ctl.has_unsaved_edits());
    assert!(!temp.exists());
    assert!(!archive_names(&archive, size, dir.path()).contains(&"NOTE.TXT".to_string()));
}

#[cfg(feature = "chd")]
#[test]
fn closing_a_chd_with_a_written_diff_flattens_it_before_closing() {
    let dir = tempfile::tempdir().unwrap();
    use libchdman_rs::hd::{create_from_reader, HdCreateOptions};
    let data = std::fs::read(fat_volume(dir.path())).unwrap();
    let chd = dir.path().join("zdisk.chd");
    let opts = HdCreateOptions {
        logical_size: data.len() as u64,
        hunk_size: 4096,
        unit_size: 512,
        codecs: [libchdman_rs::CHD_CODEC_ZLIB, 0, 0, 0],
        geometry: None,
        ident: None,
    };
    let mut src = std::io::Cursor::new(&data);
    create_from_reader(&mut src, &chd, opts, &mut |_| {}, &|| false).unwrap();
    let mut ctl = inspect(session(&chd));
    ctl.inspect_edit_in_place();
    mode(&mut ctl, ModeAction::ToggleEditMode);
    assert!(ctl.editing_chd(), "{:?}", ctl.take_mode_notice());
    stage_folder(&mut ctl, "KEPT");
    ctl.apply(CommanderAction::Mode(ModeAction::Apply));
    settle(&mut ctl);
    mode(&mut ctl, ModeAction::RequestClose);
    assert!(ctl.unsaved_shown());
    mode(&mut ctl, ModeAction::Unsaved(UnsavedChoice::Apply));
    settle(&mut ctl);
    assert!(ctl.take_close_request(), "{:?}", ctl.take_mode_notice());
    assert!(root_names(&chd).contains(&"KEPT".to_string()));
}
