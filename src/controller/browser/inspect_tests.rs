//! The browser pane under the Inspect profile: a host-built session, the combined tree, the side panel.

use std::path::Path;
use std::time::{Duration, Instant};

use super::explorer_tests::fat_volume;
use super::*;
use crate::controller::commander::{CommanderAction, CommanderController, Side};
use crate::controller::keys::KeyMap;
use crate::controller::{Controller, Effect};
use crate::model::browse_session::BrowseSession;

type A = BrowserPaneAction;

pub(super) fn session(img: &Path) -> BrowseSession {
    BrowseSession {
        source_path: Some(img.to_path_buf()),
        ..Default::default()
    }
}

/// Ticks until no job runs (the side panel's delayed read included).
pub(super) fn settle(ctl: &mut CommanderController) -> Vec<Effect> {
    let start = Instant::now();
    let mut fx = ctl.tick();
    while ctl.is_working() {
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "a job never ended"
        );
        std::thread::sleep(Duration::from_millis(5));
        fx.extend(ctl.tick());
    }
    fx
}

pub(super) fn inspect(img: &Path) -> CommanderController {
    let mut ctl = CommanderController::single(
        PaneServices::headless(),
        KeyMap::for_this_platform(),
        PaneProfile::inspect(),
    );
    ctl.open_fixed_session(session(img), "vol.img");
    settle(&mut ctl);
    let pane = ctl.pane(Side::Left);
    assert!(pane.listing.is_loaded(), "{:?}", pane.source_error());
    ctl
}

fn row(ctl: &CommanderController, name: &str) -> DisplayRow {
    let rows = ctl.pane(Side::Left).rows();
    let found = rows.iter().find(|r| r.name == name);
    found.cloned().unwrap_or_else(|| panic!("no row {name}"))
}

pub(super) fn pane(ctl: &mut CommanderController, a: BrowserPaneAction) -> Vec<Effect> {
    ctl.apply(CommanderAction::Pane(Side::Left, a))
}

#[test]
fn a_fixed_session_opens_as_one_tree_with_details_beside() {
    let dir = tempfile::tempdir().unwrap();
    let ctl = inspect(&fat_volume(dir.path()));
    let p = ctl.pane(Side::Left);
    assert_eq!(p.listing_style(), ListingStyle::CombinedTree);
    assert_eq!(p.profile().detail, DetailPlacement::SidePanel);
    assert!(p.volume().fs_type.starts_with("FAT"), "{:?}", p.volume());
    let sub = row(&ctl, "SUB");
    assert!(sub.expandable && !sub.expanded && sub.depth == 0);
    assert_eq!(p.row_entry(&sub).map(|e| e.path), Some("/SUB".to_string()));
}

#[test]
fn folders_expand_in_place_and_survive_a_reopen_of_the_same_session() {
    let dir = tempfile::tempdir().unwrap();
    let img = fat_volume(dir.path());
    let mut ctl = inspect(&img);
    let sub = row(&ctl, "SUB").node_id;
    pane(&mut ctl, A::ToggleExpand(sub));
    settle(&mut ctl);
    let deep = row(&ctl, "DEEP");
    assert!(deep.in_folder && deep.depth == 1, "{deep:?}");
    let entry = ctl.pane(Side::Left).row_entry(&deep).unwrap();
    assert_eq!(entry.path, "/SUB/DEEP");
    // The host reopens the same source (Edit Mode, a write): the tree and the queue stay.
    ctl.host_pane()
        .queue
        .push(crate::model::edit_queue::StagedEdit::CreateDirectory {
            parent: entry,
            name: "NEW".into(),
            qnx2_attrs: None,
        });
    ctl.open_fixed_session(session(&img), "vol.img");
    settle(&mut ctl);
    assert!(row(&ctl, "SUB").expanded);
    assert_eq!(row(&ctl, "DEEP").depth, 1);
    assert_eq!(ctl.pane(Side::Left).queue.len(), 1);
}

#[test]
fn the_side_panel_follows_the_cursor_and_reads_the_preview() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = inspect(&fat_volume(dir.path()));
    pane(
        &mut ctl,
        A::Click {
            name: "HELLO.TXT".into(),
            ctrl: false,
            shift: false,
        },
    );
    settle(&mut ctl);
    let d = ctl.side_detail(Side::Left).expect("the side panel shows");
    assert_eq!(d.entry.name, "HELLO.TXT");
    assert!(!d.loading);
    assert_eq!(d.preview.as_deref(), Some(&b"hello\r\n"[..]));
    assert!(d.editable, "a side panel's details take metadata edits");
}

#[test]
fn edit_mode_off_refuses_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = inspect(&fat_volume(dir.path()));
    ctl.host_pane().set_read_only(true);
    let fx = pane(&mut ctl, A::RequestNewFolder);
    assert!(
        fx.is_empty() && ctl.status().contains("not available"),
        "{fx:?} {}",
        ctl.status()
    );
    ctl.host_pane().set_read_only(false);
    let fx = pane(&mut ctl, A::RequestNewFolder);
    assert!(matches!(&fx[..], [Effect::AskText(_)]), "{fx:?}");
}

#[test]
fn a_staged_folder_shows_its_staged_contents_beneath_it() {
    use crate::model::edit_queue::StagedEdit;
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = inspect(&fat_volume(dir.path()));
    let root = ctl.pane(Side::Left).entry_at("/").expect("the root entry");
    let staged_dir =
        |parent: &disk_core::entry::FileEntry, name: &str| StagedEdit::CreateDirectory {
            parent: parent.clone(),
            name: name.into(),
            qnx2_attrs: None,
        };
    ctl.host_pane().queue.push(staged_dir(&root, "NEWDIR"));
    let new_dir = ctl.host_pane().queue.pending_adds_for("/")[0].clone();
    ctl.host_pane().queue.push(staged_dir(&new_dir, "INNER"));
    ctl.tick();
    let outer = row(&ctl, "NEWDIR");
    let inner = row(&ctl, "INNER");
    assert_eq!((outer.kind, outer.depth), (RowKind::PendingAdd, 0));
    assert_eq!((inner.kind, inner.depth), (RowKind::PendingAdd, 1));
    let p = ctl.pane(Side::Left);
    assert_eq!(
        p.row_entry(&inner).map(|e| e.path),
        Some("/NEWDIR/INNER".into())
    );
}

#[test]
fn the_side_panel_takes_edits_only_while_writable_and_covers_staged_adds() {
    use crate::model::edit_queue::StagedEdit;
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = inspect(&fat_volume(dir.path()));
    ctl.host_pane().set_read_only(true);
    pane(
        &mut ctl,
        A::Click {
            name: "HELLO.TXT".into(),
            ctrl: false,
            shift: false,
        },
    );
    settle(&mut ctl);
    assert!(!ctl.side_detail(Side::Left).unwrap().editable);
    ctl.host_pane().set_read_only(false);
    settle(&mut ctl);
    assert!(ctl.side_detail(Side::Left).unwrap().editable);
    let root = ctl.pane(Side::Left).entry_at("/").unwrap();
    ctl.host_pane().queue.push(StagedEdit::CreateDirectory {
        parent: root,
        name: "NEWDIR".into(),
        qnx2_attrs: None,
    });
    ctl.tick();
    let id = row(&ctl, "NEWDIR").node_id;
    pane(&mut ctl, A::CursorTo(id));
    settle(&mut ctl);
    let d = ctl
        .side_detail(Side::Left)
        .expect("a staged add has details");
    assert_eq!(d.entry.name, "NEWDIR");
    assert!(d.preview.is_none() && !d.loading);
}

#[test]
fn staged_host_files_wait_for_apply_then_land_and_the_tree_reopens() {
    use super::explorer_tests::root_file;
    let dir = tempfile::tempdir().unwrap();
    let img = fat_volume(dir.path());
    let host = dir.path().join("A.TXT");
    std::fs::write(&host, b"from host").unwrap();
    let mut ctl = inspect(&img);
    let sub = row(&ctl, "SUB").node_id;
    pane(&mut ctl, A::ToggleExpand(sub));
    settle(&mut ctl);
    let picked = A::Picked {
        purpose: purpose::IMPORT,
        path: Some(host.clone()),
    };
    ctl.host_pane().set_read_only(true);
    pane(&mut ctl, picked.clone());
    assert!(
        ctl.pane(Side::Left).queue.is_empty(),
        "Edit Mode off stages nothing"
    );
    ctl.host_pane().set_read_only(false);
    pane(&mut ctl, picked);
    settle(&mut ctl);
    assert_eq!(ctl.pane(Side::Left).queue.len(), 1, "staged, not written");
    assert!(root_file(&img, "A.TXT").is_none());
    assert_eq!(row(&ctl, "A.TXT").kind, RowKind::PendingAdd);
    pane(&mut ctl, A::Apply);
    settle(&mut ctl);
    let p = ctl.pane(Side::Left);
    assert!(
        p.queue.is_empty() && p.apply_report().is_none(),
        "{:?}",
        p.apply_report()
    );
    assert_eq!(root_file(&img, "A.TXT").unwrap(), b"from host");
    assert_eq!(row(&ctl, "A.TXT").kind, RowKind::Normal);
    assert!(row(&ctl, "SUB").expanded, "the reopen keeps the tree");
}

#[test]
fn the_text_editor_saves_a_file_deep_in_the_tree_back_to_its_folder_on_apply() {
    let dir = tempfile::tempdir().unwrap();
    let img = fat_volume(dir.path());
    {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&img)
            .unwrap();
        let mut fs = crate::fs::open_editable_filesystem(f, 0, 0, None).unwrap();
        let root = fs.root().unwrap();
        let sub = fs.list_directory(&root).unwrap();
        let sub = sub.into_iter().find(|e| e.name == "SUB").unwrap();
        let mut data: &[u8] = b"one\r\n";
        fs.create_file(&sub, "NOTE.TXT", &mut data, 5, &Default::default())
            .unwrap();
        fs.sync_metadata().unwrap();
    }
    let mut ctl = inspect(&img);
    let sub = row(&ctl, "SUB").node_id;
    pane(&mut ctl, A::ToggleExpand(sub));
    settle(&mut ctl);
    let note = row(&ctl, "NOTE.TXT").node_id;
    pane(&mut ctl, A::CursorTo(note));
    ctl.host_pane().set_read_only(true);
    let said = pane(&mut ctl, A::EditText);
    assert!(
        said.is_empty() && ctl.status().contains("not available"),
        "Edit Mode off"
    );
    ctl.host_pane().set_read_only(false);
    pane(&mut ctl, A::EditText);
    settle(&mut ctl);
    let doc = ctl.take_text_doc().expect("the file was read");
    assert_eq!((doc.folder.as_str(), doc.text.as_str()), ("/SUB", "one\n"));
    pane(
        &mut ctl,
        A::SaveText {
            name: doc.name,
            folder: doc.folder,
            text: "two\n".into(),
            shape: doc.shape,
        },
    );
    assert_eq!(ctl.pane(Side::Left).queue.len(), 1, "a save is staged");
    pane(&mut ctl, A::Apply);
    settle(&mut ctl);
    assert!(ctl.pane(Side::Left).queue.is_empty());
    let f = std::fs::File::open(&img).unwrap();
    let mut fs = fs_pc::fat::FatFilesystem::open(f, 0).unwrap();
    use disk_core::filesystem::Filesystem as _;
    let root = fs.root().unwrap();
    let sub = fs.list_directory(&root).unwrap();
    let sub = sub.into_iter().find(|e| e.name == "SUB").unwrap();
    let kids = fs.list_directory(&sub).unwrap();
    let note = kids.into_iter().find(|e| e.name == "NOTE.TXT").unwrap();
    assert_eq!(fs.read_file(&note, 64).unwrap(), b"two\r\n");
    assert!(crate::controller::browser::explorer_tests::root_file(&img, "NOTE.TXT").is_none());
}

#[test]
fn marked_rows_from_an_expanded_folder_export_through_the_screen() {
    use crate::controller::commander::screen::purpose as screen_purpose;
    use crate::controller::commander::ExportFormat;
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = inspect(&fat_volume(dir.path()));
    let sub = row(&ctl, "SUB").node_id;
    pane(&mut ctl, A::ToggleExpand(sub));
    settle(&mut ctl);
    let deep = row(&ctl, "DEEP").node_id;
    pane(
        &mut ctl,
        A::TreeClick {
            node_id: deep,
            ctrl: true,
            shift: false,
        },
    );
    let fx = ctl.apply(CommanderAction::Export {
        from: Side::Left,
        format: Some(ExportFormat::LooseFiles),
    });
    assert!(matches!(&fx[..], [Effect::PickFolder(p)] if p.purpose == screen_purpose::EXPORT));
    let out = dir.path().join("out");
    std::fs::create_dir(&out).unwrap();
    ctl.apply(CommanderAction::Picked {
        purpose: screen_purpose::EXPORT,
        path: Some(out.clone()),
    });
    settle(&mut ctl);
    assert!(out.join("DEEP").is_dir(), "{}", ctl.status());
}

/// Marks `HELLO.TXT` (top level) and `/SUB/DEEP` (below an expanded folder) without moving the cursor.
fn mark_both_levels(ctl: &mut CommanderController) {
    let sub = row(ctl, "SUB").node_id;
    pane(ctl, A::ToggleExpand(sub.clone()));
    settle(ctl);
    pane(ctl, A::CursorTo(sub.clone()));
    pane(ctl, A::Mark(row(ctl, "HELLO.TXT").node_id));
    pane(ctl, A::Mark(row(ctl, "DEEP").node_id));
    let p = ctl.pane(Side::Left);
    assert_eq!(
        p.rows()[p.cursor_index().unwrap()].node_id,
        sub,
        "a mark keeps the cursor"
    );
    let mut names: Vec<String> = p.selected_entries().into_iter().map(|e| e.name).collect();
    names.sort();
    assert_eq!(names, ["DEEP", "HELLO.TXT"], "one selection across levels");
    assert!(p.row_selected(&row(ctl, "HELLO.TXT")) && p.row_selected(&row(ctl, "DEEP")));
}

#[test]
fn marks_mix_levels_and_delete_and_export_act_on_all_of_them() {
    use crate::controller::commander::screen::purpose as screen_purpose;
    use crate::controller::commander::ExportFormat;
    use crate::model::edit_queue::StagedEdit;
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = inspect(&fat_volume(dir.path()));
    mark_both_levels(&mut ctl);
    // A ctrl-click on a deep row keeps the top-level mark too.
    let deep = row(&ctl, "DEEP").node_id;
    let ctrl = |node_id: String| A::TreeClick {
        node_id,
        ctrl: true,
        shift: false,
    };
    pane(&mut ctl, ctrl(deep.clone()));
    pane(&mut ctl, ctrl(deep));
    assert_eq!(ctl.pane(Side::Left).selected_entries().len(), 2);

    let out = dir.path().join("out");
    std::fs::create_dir(&out).unwrap();
    ctl.apply(CommanderAction::Export {
        from: Side::Left,
        format: Some(ExportFormat::LooseFiles),
    });
    ctl.apply(CommanderAction::Picked {
        purpose: screen_purpose::EXPORT,
        path: Some(out.clone()),
    });
    settle(&mut ctl);
    // Each entry keeps its folder below the common parent.
    assert!(out.join("SUB").join("DEEP").is_dir(), "{}", ctl.status());
    assert_eq!(std::fs::read(out.join("HELLO.TXT")).unwrap(), b"hello\r\n");

    // DEEP was never listed, so it may hold something: the delete asks first.
    let fx = pane(&mut ctl, A::Delete);
    assert!(matches!(&fx[..], [Effect::Confirm(c)] if c.purpose == purpose::DELETE_FOLDERS));
    pane(
        &mut ctl,
        A::Confirmed {
            purpose: purpose::DELETE_FOLDERS,
            yes: true,
        },
    );
    let p = ctl.pane(Side::Left);
    let staged: Vec<(String, String)> = p
        .queue
        .iter()
        .map(|e| match e {
            StagedEdit::DeleteEntry { parent, entry }
            | StagedEdit::DeleteRecursive { parent, entry } => {
                (parent.path.clone(), entry.name.clone())
            }
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        staged,
        [
            ("/".into(), "HELLO.TXT".into()),
            ("/SUB".into(), "DEEP".into())
        ]
    );
}

#[test]
fn marks_from_both_levels_copy_and_checksum_together() {
    let dir = tempfile::tempdir().unwrap();
    let host = tempfile::tempdir().unwrap();
    let mut ctl = CommanderController::headless();
    ctl.apply(CommanderAction::Pane(
        Side::Left,
        A::OpenImage(fat_volume(dir.path())),
    ));
    ctl.apply(CommanderAction::Pane(
        Side::Right,
        A::OpenHostFolder(host.path().to_path_buf()),
    ));
    settle(&mut ctl);
    pane(&mut ctl, A::SetListingStyle(ListingStyle::CombinedTree));
    mark_both_levels(&mut ctl);
    ctl.apply(CommanderAction::Copy(Side::Left));
    settle(&mut ctl);
    assert!(host.path().join("DEEP").is_dir(), "{}", ctl.status());
    assert!(host.path().join("HELLO.TXT").is_file(), "{}", ctl.status());
    ctl.apply(CommanderAction::Checksums(Side::Left));
    settle(&mut ctl);
    let sums = ctl.checksums().expect("the checksum window");
    let rows: Vec<&str> = sums.rows.iter().map(|r| r.name.as_str()).collect();
    assert_eq!(rows, ["HELLO.TXT"], "folders are skipped");
}

#[test]
fn delete_asks_before_a_full_folder_and_takes_the_cursor_row_when_nothing_is_marked() {
    use crate::model::edit_queue::StagedEdit;
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = inspect(&fat_volume(dir.path()));
    let id = row(&ctl, "HELLO.TXT").node_id;
    pane(&mut ctl, A::CursorTo(id));
    let fx = pane(&mut ctl, A::Delete);
    assert!(fx.is_empty(), "a file needs no question: {fx:?}");
    assert_eq!(ctl.pane(Side::Left).queue.len(), 1);
    pane(&mut ctl, A::ClearSelection);
    let id = row(&ctl, "SUB").node_id;
    pane(&mut ctl, A::CursorTo(id));
    let fx = pane(&mut ctl, A::Delete);
    let Some(Effect::Confirm(spec)) = fx.first() else {
        panic!("{fx:?}");
    };
    assert_eq!(spec.purpose, purpose::DELETE_FOLDERS);
    assert!(
        spec.message.contains("'SUB' and all its contents"),
        "{}",
        spec.message
    );
    pane(
        &mut ctl,
        A::Confirmed {
            purpose: purpose::DELETE_FOLDERS,
            yes: false,
        },
    );
    assert_eq!(ctl.pane(Side::Left).queue.len(), 1, "no stages nothing");
    pane(&mut ctl, A::Delete);
    pane(
        &mut ctl,
        A::Confirmed {
            purpose: purpose::DELETE_FOLDERS,
            yes: true,
        },
    );
    let p = ctl.pane(Side::Left);
    assert!(matches!(
        p.queue.iter().last(),
        Some(StagedEdit::DeleteRecursive { entry, .. }) if entry.name == "SUB"
    ));
    assert_eq!(p.staged_summary(), ["2 item(s) deleted"]);
}

#[test]
fn host_paths_stage_into_a_folder_and_list_what_could_not_be_staged() {
    let dir = tempfile::tempdir().unwrap();
    let img = fat_volume(dir.path());
    let host = dir.path().join("TREE");
    std::fs::create_dir(&host).unwrap();
    std::fs::write(host.join("A.TXT"), b"aaa").unwrap();
    let missing = dir.path().join("GONE.TXT");
    let mut ctl = inspect(&img);
    let add = A::AddHostPaths {
        paths: vec![host.clone(), missing.clone()],
        folder: Some("/SUB".into()),
    };
    ctl.host_pane().set_read_only(true);
    pane(&mut ctl, add.clone());
    assert!(ctl.pane(Side::Left).queue.is_empty(), "Edit Mode off");
    ctl.host_pane().set_read_only(false);
    pane(&mut ctl, add);
    let p = ctl.pane(Side::Left);
    let adds = p.queue.pending_adds_for("/SUB");
    assert_eq!(adds.len(), 1, "{:?}", p.edit_lines());
    assert_eq!(p.queue.pending_adds_for("/SUB/TREE")[0].name, "A.TXT");
    assert_eq!(
        p.staged_summary(),
        ["1 file(s) added, 1 folder(s) added", "Net data: +3 B"]
    );
    let errors = p.staging_errors();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].0, missing.display().to_string());
    assert!(
        ctl.status().contains("1 could not be staged"),
        "{}",
        ctl.status()
    );
    pane(&mut ctl, A::DismissStagingErrors);
    assert!(ctl.pane(Side::Left).staging_errors().is_empty());
}

#[test]
fn check_tree_and_journal_run_as_jobs_and_hand_over_plain_data() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = inspect(&fat_volume(dir.path()));
    let sub = row(&ctl, "SUB").node_id;
    pane(&mut ctl, A::ToggleExpand(sub));
    settle(&mut ctl);
    pane(&mut ctl, A::Fsck);
    assert!(ctl.pane(Side::Left).tool_working().is_some());
    settle(&mut ctl);
    let report = ctl
        .pane(Side::Left)
        .tool_report()
        .expect("a report")
        .clone();
    let check = report.check.expect("the check as data");
    assert!(check.clean && check.errors.is_empty(), "{check:?}");
    assert!(
        check.stats_line.starts_with("Files: "),
        "{}",
        check.stats_line
    );
    assert!(report.repair.is_none());
    pane(&mut ctl, A::DismissToolReport);

    pane(&mut ctl, A::ListTree { ids: false });
    settle(&mut ctl);
    let tree = ctl.host_pane().take_tree_text().expect("the tree");
    assert!(
        tree.text.contains("HELLO.TXT") && tree.text.contains("DEEP"),
        "{}",
        tree.text
    );
    assert!(!tree.ids);
    assert!(
        ctl.host_pane().take_tree_text().is_none(),
        "handed over once"
    );

    pane(&mut ctl, A::ShowJournal);
    settle(&mut ctl);
    assert!(ctl.host_pane().take_journal().is_none());
    assert!(ctl.status().contains("not journaled"), "{}", ctl.status());
}

#[test]
fn repair_needs_edit_mode_and_reports_what_it_did() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = inspect(&fat_volume(dir.path()));
    ctl.host_pane().set_read_only(true);
    pane(&mut ctl, A::Repair);
    assert!(ctl.status().contains("not available"), "{}", ctl.status());
    ctl.host_pane().set_read_only(false);
    let fx = pane(&mut ctl, A::Repair);
    assert!(matches!(&fx[..], [Effect::Confirm(c)] if c.purpose == purpose::REPAIR));
    pane(
        &mut ctl,
        A::Confirmed {
            purpose: purpose::REPAIR,
            yes: true,
        },
    );
    settle(&mut ctl);
    let report = ctl
        .pane(Side::Left)
        .tool_report()
        .expect("a report")
        .clone();
    assert!(report.repair.is_some(), "{:?}", report.lines);
    assert!(report.check.is_some_and(|c| c.clean), "{:?}", report.lines);
    assert!(
        ctl.pane(Side::Left).listing.is_loaded(),
        "reopened after the repair"
    );
}

#[test]
fn boot_blocks_pick_a_donor_and_report_one_without_them() {
    let dir = tempfile::tempdir().unwrap();
    let img = fat_volume(dir.path());
    let mut ctl = inspect(&img);
    let fx = pane(&mut ctl, A::BootBlocks);
    assert!(matches!(&fx[..], [Effect::PickFile(p)] if p.purpose == purpose::BOOT_DONOR));
    pane(
        &mut ctl,
        A::Picked {
            purpose: purpose::BOOT_DONOR,
            path: Some(img),
        },
    );
    settle(&mut ctl);
    assert!(ctl.pane(Side::Left).queue.is_empty());
    assert!(ctl.status().contains("boot blocks:"), "{}", ctl.status());
    assert_eq!(ctl.pane(Side::Left).boot_blocks_present(), None, "not HFS");
}

#[test]
fn extract_picks_a_folder_reviews_existing_files_and_runs_as_the_copy_job() {
    use crate::controller::commander::screen::purpose as screen_purpose;
    use crate::controller::commander::{ExtractOptions, OnConflict};
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = inspect(&fat_volume(dir.path()));
    let out = dir.path().join("out");
    std::fs::create_dir(&out).unwrap();
    let extract = |path: &str, dest: Option<std::path::PathBuf>| CommanderAction::Extract {
        from: Side::Left,
        path: path.into(),
        options: ExtractOptions::default(),
        dest,
    };
    let fx = ctl.apply(extract("/SUB", None));
    assert!(matches!(&fx[..], [Effect::PickFolder(p)] if p.purpose == screen_purpose::EXTRACT));
    ctl.apply(CommanderAction::Picked {
        purpose: screen_purpose::EXTRACT,
        path: Some(out.clone()),
    });
    settle(&mut ctl);
    assert!(out.join("SUB").join("DEEP").is_dir(), "{}", ctl.status());

    // A file that already exists goes to the review: Skip keeps it, Replace writes it.
    std::fs::write(out.join("HELLO.TXT"), b"old").unwrap();
    ctl.apply(extract("/HELLO.TXT", Some(out.clone())));
    assert_eq!(
        ctl.host_conflict().map(|n| n.len()),
        Some(1),
        "{}",
        ctl.status()
    );
    ctl.apply(CommanderAction::HostConflict(OnConflict::Skip));
    settle(&mut ctl);
    assert_eq!(std::fs::read(out.join("HELLO.TXT")).unwrap(), b"old");
    ctl.apply(extract("/HELLO.TXT", Some(out.clone())));
    ctl.apply(CommanderAction::HostConflict(OnConflict::Replace));
    settle(&mut ctl);
    assert_eq!(std::fs::read(out.join("HELLO.TXT")).unwrap(), b"hello\r\n");
    assert!(
        ctl.status().contains("Copied 1 file(s)"),
        "{}",
        ctl.status()
    );
}

#[test]
fn one_entry_or_the_whole_volume_exports_and_tar_follows_the_picked_name() {
    use crate::controller::commander::screen::purpose as screen_purpose;
    use crate::controller::commander::ExportFormat;
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = inspect(&fat_volume(dir.path()));
    let fx = ctl.apply(CommanderAction::ExportEntry {
        from: Side::Left,
        path: "/".into(),
        format: Some(ExportFormat::TarGz),
    });
    let Some(Effect::PickFile(spec)) = fx.first() else {
        panic!("{fx:?}");
    };
    assert_eq!(spec.file_name, "T.tar.gz", "named after the volume");
    let tar = dir.path().join("vol.tar");
    ctl.apply(CommanderAction::Picked {
        purpose: screen_purpose::EXPORT,
        path: Some(tar.clone()),
    });
    settle(&mut ctl);
    let bytes = std::fs::read(&tar).unwrap();
    assert_eq!(&bytes[257..262], b"ustar", "a plain tar, as the name says");
    let names = String::from_utf8_lossy(&bytes).to_string();
    assert!(
        names.contains("HELLO.TXT") && names.contains("SUB/"),
        "{}",
        ctl.status()
    );

    ctl.apply(CommanderAction::ExportEntry {
        from: Side::Left,
        path: "/HELLO.TXT".into(),
        format: Some(ExportFormat::StuffItHqx),
    });
    let hqx = dir.path().join("HELLO.sit.hqx");
    ctl.apply(CommanderAction::Picked {
        purpose: screen_purpose::EXPORT,
        path: Some(hqx.clone()),
    });
    settle(&mut ctl);
    let text = std::fs::read_to_string(&hqx).unwrap();
    assert!(text.contains("BinHex"), "{}", &text[..text.len().min(80)]);
}

#[test]
fn a_mac_archive_is_sniffed_and_read_on_the_shared_reader() {
    use arc_mac::stuffit::{build_archive_tree, StuffItInput, StuffItInputNode, WriteMethod};
    let dir = tempfile::tempdir().unwrap();
    let img = fat_volume(dir.path());
    let sit = build_archive_tree(
        &[StuffItInputNode::File(StuffItInput {
            name: "inner.txt".into(),
            type_code: *b"TEXT",
            creator_code: *b"ttxt",
            finder_flags: 0,
            create_date: 0,
            mod_date: 0,
            data_fork: b"x".to_vec(),
            resource_fork: Vec::new(),
        })],
        WriteMethod::Store,
    )
    .unwrap();
    {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&img)
            .unwrap();
        let mut fs = crate::fs::open_editable_filesystem(f, 0, 0, None).unwrap();
        let root = fs.root().unwrap();
        let mut data: &[u8] = &sit;
        let len = sit.len() as u64;
        fs.create_file(&root, "ARC.SIT", &mut data, len, &Default::default())
            .unwrap();
        fs.sync_metadata().unwrap();
    }
    let mut ctl = inspect(&img);
    pane(&mut ctl, A::SniffArchive("/HELLO.TXT".into()));
    pane(&mut ctl, A::SniffArchive("/ARC.SIT".into()));
    settle(&mut ctl);
    assert_eq!(ctl.pane(Side::Left).archive_sniff("/HELLO.TXT"), Some(None));
    pane(&mut ctl, A::SniffArchive("/ARC.SIT".into()));
    settle(&mut ctl);
    let kind = ctl.pane(Side::Left).archive_sniff("/ARC.SIT");
    assert!(matches!(kind, Some(Some(_))), "{kind:?}");
    pane(
        &mut ctl,
        A::ReadArchive {
            path: "/ARC.SIT".into(),
            purpose: ArchiveUse::Browse,
        },
    );
    settle(&mut ctl);
    let viewer = ctl.pane(Side::Left).archive_viewer().expect("the viewer");
    assert_eq!(viewer.entry_name, "ARC.SIT");
    assert_eq!((viewer.file_count(), viewer.marked_files()), (1, 1));
    assert_eq!(viewer.rows[0].path, "inner.txt");
    pane(
        &mut ctl,
        A::ReadArchive {
            path: "/HELLO.TXT".into(),
            purpose: ArchiveUse::Extract,
        },
    );
    settle(&mut ctl);
    assert!(ctl.pane(Side::Left).archive_save().is_none());
    assert!(
        ctl.status().contains("not a recognized Mac archive"),
        "{}",
        ctl.status()
    );
}

/// A FAT volume `T` with `/SUB/A.TXT`, `/SUB/AOK.TXT`, `/SUB/B.TXT` and `/HELLO.TXT`; under
/// `out`, folders sit where A.TXT, B.TXT and HELLO.TXT would land, so writing each fails.
fn three_failures(dir: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let img = dir.join("vol.img");
    let bytes = fs_pc::fat::create_blank_fat(2 * 1024 * 1024, Some("T")).unwrap();
    std::fs::write(&img, bytes).unwrap();
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&img)
        .unwrap();
    let mut fs = crate::fs::open_editable_filesystem(f, 0, 0, None).unwrap();
    let root = fs.root().unwrap();
    let sub = fs
        .create_directory(&root, "SUB", &Default::default())
        .unwrap();
    for (parent, name) in [(&sub, "A.TXT"), (&sub, "AOK.TXT"), (&sub, "B.TXT")] {
        let mut data: &[u8] = b"data";
        fs.create_file(parent, name, &mut data, 4, &Default::default())
            .unwrap();
    }
    let mut data: &[u8] = b"hello";
    fs.create_file(&root, "HELLO.TXT", &mut data, 5, &Default::default())
        .unwrap();
    fs.sync_metadata().unwrap();
    let out = dir.join("out");
    for blocker in ["T/SUB/A.TXT", "T/SUB/B.TXT", "T/HELLO.TXT"] {
        std::fs::create_dir_all(out.join(blocker)).unwrap();
    }
    (img, out)
}

/// Extract the whole volume into `out` over the blockers, answering each failure from `answers`;
/// returns the messages asked.
fn extract_answering(ctl: &mut CommanderController, out: &Path, answers: &[usize]) -> Vec<String> {
    use crate::controller::commander::screen::purpose as screen_purpose;
    use crate::controller::commander::{ExtractOptions, OnConflict};
    ctl.apply(CommanderAction::Extract {
        from: Side::Left,
        path: "/".into(),
        options: ExtractOptions::default(),
        dest: Some(out.to_path_buf()),
    });
    // The volume's folder exists, so the conflict review comes first.
    assert!(ctl.host_conflict().is_some(), "{}", ctl.status());
    ctl.apply(CommanderAction::HostConflict(OnConflict::Replace));
    let mut asked = Vec::new();
    let mut answers = answers.iter();
    let start = Instant::now();
    loop {
        for e in ctl.tick() {
            let Effect::Choose(spec) = e else { continue };
            assert_eq!(spec.purpose, screen_purpose::ENTRY_FAILED);
            let labels: Vec<&str> = spec.choices.iter().map(|c| c.label.as_str()).collect();
            assert_eq!(labels, ["Skip", "Continue with all errors", "Stop"]);
            asked.push(spec.message.clone());
            let choice = *answers.next().expect("asked more often than expected");
            ctl.apply(CommanderAction::Chose {
                purpose: spec.purpose,
                choice,
            });
        }
        if !ctl.is_working() {
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(60), "never ended");
        std::thread::sleep(Duration::from_millis(5));
    }
    settle(ctl);
    asked
}

#[test]
fn skip_asks_again_at_every_failed_entry_and_lists_them_at_the_end() {
    let dir = tempfile::tempdir().unwrap();
    let (img, out) = three_failures(dir.path());
    let mut ctl = inspect(&img);
    let asked = extract_answering(&mut ctl, &out, &[0, 0, 0]);
    assert_eq!(asked.len(), 3, "{asked:?}");
    assert!(
        asked[0].starts_with("Could not extract /SUB/A.TXT: "),
        "{asked:?}"
    );
    assert!(asked[2].starts_with("Could not extract /HELLO.TXT: "));
    assert_eq!(std::fs::read(out.join("T/SUB/AOK.TXT")).unwrap(), b"data");
    let report = ctl.skipped_report().expect("the skipped list");
    assert_eq!(report.skipped.len(), 3);
    assert!(report.skipped[0].starts_with("/SUB/A.TXT ("));
    assert_eq!(report.stopped, None);
    assert_eq!(
        ctl.status(),
        "Copied 1 file(s) to the extract folder; skipped 3."
    );
    assert!(ctl.modal_open());
    ctl.apply(CommanderAction::SkippedReportOk);
    assert!(ctl.skipped_report().is_none());
}

#[test]
fn continue_with_all_errors_asks_once_and_skips_the_rest() {
    let dir = tempfile::tempdir().unwrap();
    let (img, out) = three_failures(dir.path());
    let mut ctl = inspect(&img);
    let asked = extract_answering(&mut ctl, &out, &[1]);
    assert_eq!(asked.len(), 1, "{asked:?}");
    let report = ctl.skipped_report().expect("the skipped list");
    let paths: Vec<&str> = report
        .skipped
        .iter()
        .map(|s| s.split(' ').next().unwrap())
        .collect();
    assert_eq!(paths, ["/SUB/A.TXT", "/SUB/B.TXT", "/HELLO.TXT"]);
    assert!(out.join("T/SUB/AOK.TXT").is_file());
}

#[test]
fn stop_ends_the_extraction_and_keeps_what_was_written() {
    let dir = tempfile::tempdir().unwrap();
    let (img, out) = three_failures(dir.path());
    let mut ctl = inspect(&img);
    let asked = extract_answering(&mut ctl, &out, &[0, 2]);
    assert_eq!(asked.len(), 2, "{asked:?}");
    let report = ctl.skipped_report().expect("the skipped list");
    assert_eq!(report.skipped.len(), 1);
    let stopped = report.stopped.as_deref().unwrap_or("");
    assert!(stopped.starts_with("/SUB/B.TXT ("), "{stopped}");
    assert!(ctl.status().starts_with("Stopped at /SUB/B.TXT ("));
    let said = ctl.status();
    assert!(
        said.ends_with("Copied 1 file(s) to the extract folder; skipped 1."),
        "{said}"
    );
    // AOK.TXT came before the stop; nothing after it ran.
    assert!(out.join("T/SUB/AOK.TXT").is_file());
    assert!(out.join("T/HELLO.TXT").is_dir());
}

#[test]
fn cancel_while_asking_ends_the_copy_cancelled() {
    use crate::controller::commander::{ExtractOptions, OnConflict};
    let dir = tempfile::tempdir().unwrap();
    let (img, out) = three_failures(dir.path());
    let mut ctl = inspect(&img);
    ctl.apply(CommanderAction::Extract {
        from: Side::Left,
        path: "/".into(),
        options: ExtractOptions::default(),
        dest: Some(out.clone()),
    });
    ctl.apply(CommanderAction::HostConflict(OnConflict::Replace));
    let start = Instant::now();
    while !ctl.tick().iter().any(|e| matches!(e, Effect::Choose(_))) {
        assert!(start.elapsed() < Duration::from_secs(60), "never asked");
        std::thread::sleep(Duration::from_millis(5));
    }
    ctl.apply(CommanderAction::CancelCopy);
    settle(&mut ctl);
    assert_eq!(ctl.status(), "Copy to the extract folder cancelled.");
    assert!(ctl.skipped_report().is_none());
}
