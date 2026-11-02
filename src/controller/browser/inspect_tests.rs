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

fn session(img: &Path) -> BrowseSession {
    BrowseSession {
        source_path: Some(img.to_path_buf()),
        ..Default::default()
    }
}

/// Ticks until no job runs (the side panel's delayed read included).
fn settle(ctl: &mut CommanderController) -> Vec<Effect> {
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

fn inspect(img: &Path) -> CommanderController {
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

fn pane(ctl: &mut CommanderController, a: BrowserPaneAction) -> Vec<Effect> {
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

    pane(&mut ctl, A::Delete);
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
