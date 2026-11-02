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
