use std::path::Path;
use std::time::{Duration, Instant};

use super::*;
use crate::controller::browser::pane::source::tests::mbr_fat16;
use crate::controller::keys::{Key, Mods};

type A = CommanderAction;
type P = BrowserPaneAction;

/// Ticks until neither pane nor the screen has a job running.
fn settle(ctl: &mut CommanderController) -> Vec<Effect> {
    let start = Instant::now();
    let mut fx = ctl.tick();
    loop {
        let checksumming = ctl.checksums.as_ref().is_some_and(|c| c.job.is_some());
        let busy = ctl.left.is_busy() || ctl.right.is_busy() || ctl.detail_job.is_some();
        let busy = busy || ctl.is_working();
        if !busy && ctl.copy.is_none() && !checksumming {
            return fx;
        }
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "a job never finished"
        );
        std::thread::sleep(Duration::from_millis(5));
        fx.extend(ctl.tick());
    }
}

fn open_image(ctl: &mut CommanderController, side: Side, dir: &Path) {
    ctl.apply(A::Pane(side, P::OpenImage(mbr_fat16(dir))));
    settle(ctl);
    assert!(ctl.pane(side).listing.is_loaded(), "{}", ctl.status());
}

fn open_host(ctl: &mut CommanderController, side: Side, dir: &Path) {
    ctl.apply(A::Pane(side, P::OpenHostFolder(dir.to_path_buf())));
    settle(ctl);
    assert!(ctl.pane(side).listing.is_host(), "{}", ctl.status());
}

fn files(ctl: &CommanderController, side: Side) -> Vec<String> {
    let entries = ctl.pane(side).listing.entries().iter();
    entries
        .filter(|e| e.is_file())
        .map(|e| e.name.clone())
        .collect()
}

fn select(ctl: &mut CommanderController, side: Side, name: &str) {
    ctl.apply(A::Pane(
        side,
        P::Click {
            name: name.into(),
            ctrl: false,
            shift: false,
        },
    ));
}

fn key(code: Key) -> CommanderAction {
    A::Key(code, Mods::default())
}

fn sit_with(names: &[&str]) -> Vec<u8> {
    use arc_mac::stuffit::{build_archive_tree, StuffItInput, StuffItInputNode, WriteMethod};
    let file = |name: &str| {
        StuffItInputNode::File(StuffItInput {
            name: name.into(),
            type_code: *b"TEXT",
            creator_code: *b"ttxt",
            finder_flags: 0,
            create_date: 0,
            mod_date: 0,
            data_fork: b"hello".to_vec(),
            resource_fork: Vec::new(),
        })
    };
    let tree: Vec<_> = names.iter().map(|n| file(n)).collect();
    build_archive_tree(&tree, WriteMethod::Store).unwrap()
}

#[test]
fn a_host_to_image_copy_stages_adds_on_the_image_pane() {
    let img = tempfile::tempdir().unwrap();
    let host = tempfile::tempdir().unwrap();
    std::fs::write(host.path().join("NEW.TXT"), b"new").unwrap();
    let mut ctl = CommanderController::headless();
    open_host(&mut ctl, Side::Left, host.path());
    open_image(&mut ctl, Side::Right, img.path());
    select(&mut ctl, Side::Left, "NEW.TXT");
    assert!(ctl.can_copy(Side::Left));
    ctl.apply(A::Copy(Side::Left));
    assert!(
        ctl.status()
            .starts_with("Staged copy of 1 item(s) into the R pane"),
        "{}",
        ctl.status()
    );
    assert_eq!(ctl.pane(Side::Right).queue.len(), 1);
    let rows = ctl.right.display_rows();
    let row = rows.iter().find(|r| r.name == "NEW.TXT").unwrap();
    assert_eq!(row.kind, crate::controller::commander::RowKind::PendingAdd);
}

#[test]
fn an_image_to_host_conflict_is_reviewed_then_replaced_or_skipped() {
    let img = tempfile::tempdir().unwrap();
    let host = tempfile::tempdir().unwrap();
    let mut ctl = CommanderController::headless();
    open_image(&mut ctl, Side::Left, img.path());
    let name = files(&ctl, Side::Left)[0].clone();
    let existing = host.path().join(&name);
    std::fs::write(&existing, b"old").unwrap();
    open_host(&mut ctl, Side::Right, host.path());

    // Skip keeps the host file.
    select(&mut ctl, Side::Left, &name);
    ctl.apply(A::Copy(Side::Left));
    assert_eq!(
        ctl.host_conflict(),
        Some(&[name.clone()][..]),
        "{}",
        ctl.status()
    );
    assert!(ctl.modal_open());
    ctl.apply(A::HostConflict(OnConflict::Skip));
    settle(&mut ctl);
    assert_eq!(std::fs::read(&existing).unwrap(), b"old");
    assert!(ctl.status().contains("skipped 1"), "{}", ctl.status());

    // Cancel writes nothing; Replace overwrites with the image's bytes.
    ctl.apply(A::Copy(Side::Left));
    ctl.apply(A::HostConflictCancel);
    assert!(ctl.host_conflict().is_none());
    assert_eq!(ctl.status(), "Copy cancelled; nothing was written.");
    ctl.apply(A::Copy(Side::Left));
    ctl.apply(A::HostConflict(OnConflict::Replace));
    assert!(ctl.copy_job().is_some());
    settle(&mut ctl);
    assert_ne!(
        std::fs::read(&existing).unwrap(),
        b"old",
        "{}",
        ctl.status()
    );
    assert!(
        ctl.status().starts_with("Copied 1 file(s)"),
        "{}",
        ctl.status()
    );
    assert!(ctl.log().iter().any(|l| l.contains("Copied 1 file(s)")));
}

#[test]
fn an_image_to_image_copy_legalises_names_and_undo_unstages_it() {
    let img = tempfile::tempdir().unwrap();
    let arc = tempfile::tempdir().unwrap();
    let sit = arc.path().join("names.sit");
    std::fs::write(&sit, sit_with(&["a*b?.txt", "ok.txt"])).unwrap();
    let mut ctl = CommanderController::headless();
    ctl.apply(A::Pane(Side::Left, P::OpenImage(sit)));
    settle(&mut ctl);
    assert!(ctl.pane(Side::Left).is_archive(), "{}", ctl.status());
    open_image(&mut ctl, Side::Right, img.path());
    let before = ctl.pane(Side::Right).queue.len();
    for name in ["a*b?.txt", "ok.txt"] {
        ctl.apply(A::Pane(
            Side::Left,
            P::Click {
                name: name.into(),
                ctrl: true,
                shift: false,
            },
        ));
    }
    ctl.apply(A::Copy(Side::Left));
    assert_eq!(
        ctl.copy_job().map(|(t, _)| t),
        Some("Copying to other pane")
    );
    settle(&mut ctl);
    let notice = ctl.rename_notice().expect("a rename notice");
    assert_eq!(notice.dest, Side::Right);
    assert_eq!(
        notice.outcome.renamed.len(),
        1,
        "{:?}",
        notice.outcome.renamed
    );
    assert_eq!(notice.outcome.renamed[0].from, "a*b?.txt");
    assert_eq!(ctl.pane(Side::Right).queue.len(), before + 2);
    // A key does nothing while the notice waits.
    ctl.apply(key(Key::Tab));
    assert_eq!(ctl.active(), Side::Left);
    ctl.apply(A::RenameNoticeUndo);
    assert!(ctl.rename_notice().is_none());
    assert_eq!(ctl.pane(Side::Right).queue.len(), before);
    assert_eq!(ctl.status(), "Copy removed from the staged edits.");
}

#[test]
fn export_asks_for_a_folder_and_the_answer_runs_the_export_job() {
    let img = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let mut ctl = CommanderController::headless();
    open_image(&mut ctl, Side::Left, img.path());
    let name = files(&ctl, Side::Left)[0].clone();
    select(&mut ctl, Side::Left, &name);
    let fx = ctl.apply(A::Export {
        from: Side::Left,
        format: None,
    });
    match fx.as_slice() {
        [Effect::PickFolder(spec)] => assert_eq!(spec.purpose, purpose::EXPORT),
        other => panic!("expected a folder picker, got {other:?}"),
    }
    ctl.apply(A::Picked {
        purpose: purpose::EXPORT,
        path: Some(out.path().to_path_buf()),
    });
    assert!(ctl.copy_job().is_some(), "{}", ctl.status());
    settle(&mut ctl);
    assert!(out.path().join(&name).is_file(), "{}", ctl.status());

    // A single-file format asks for a file to save, named after the selection.
    let fx = ctl.apply(A::Export {
        from: Side::Left,
        format: Some(ExportFormat::Zip),
    });
    match fx.as_slice() {
        [Effect::PickFile(spec)] => assert!(spec.save && spec.file_name.ends_with(".zip")),
        other => panic!("expected a save picker, got {other:?}"),
    }
    ctl.apply(A::Picked {
        purpose: purpose::EXPORT,
        path: None,
    });
    assert_eq!(ctl.status(), "Export cancelled.");
}

#[test]
fn checksums_run_as_a_job_and_copy_to_the_clipboard() {
    let host = tempfile::tempdir().unwrap();
    std::fs::write(host.path().join("abc.txt"), b"abc").unwrap();
    let mut ctl = CommanderController::headless();
    open_host(&mut ctl, Side::Left, host.path());
    select(&mut ctl, Side::Left, "abc.txt");
    ctl.apply(A::Checksums(Side::Left));
    assert!(ctl.checksums().is_some(), "{}", ctl.status());
    settle(&mut ctl);
    let c = ctl.checksums().unwrap();
    assert!(c.running().is_none() && c.error.is_none());
    let values = c.rows[0].values.clone().unwrap();
    assert_eq!(values[0], "352441c2");
    let fx = ctl.apply(A::CopyChecksum {
        file: 0,
        algo: HashAlgo::Md5,
    });
    assert_eq!(
        fx,
        [Effect::CopyToClipboard(
            "900150983cd24fb0d6963f7d28e17f72".into()
        )]
    );
    match ctl.apply(A::CopyChecksums).as_slice() {
        [Effect::CopyToClipboard(text)] => {
            assert!(
                text.starts_with("abc.txt") && text.contains("352441c2"),
                "{text}"
            )
        }
        other => panic!("expected the clipboard, got {other:?}"),
    }
    ctl.apply(key(Key::Escape));
    assert!(ctl.checksums().is_none(), "Esc closes the window first");
}

#[test]
fn close_asks_while_edits_are_staged() {
    let img = tempfile::tempdir().unwrap();
    let mut ctl = CommanderController::headless();
    assert_eq!(ctl.apply(A::RequestClose), [Effect::CloseScreen]);
    open_image(&mut ctl, Side::Left, img.path());
    let name = files(&ctl, Side::Left)[0].clone();
    select(&mut ctl, Side::Left, &name);
    ctl.apply(A::Delete);
    assert_eq!(ctl.pane(Side::Left).queue.len(), 1);
    let fx = ctl.apply(A::RequestClose);
    match fx.as_slice() {
        [Effect::Confirm(spec)] => assert_eq!(spec.purpose, purpose::CLOSE_UNSAVED),
        other => panic!("expected a confirmation, got {other:?}"),
    }
    let no = ctl.apply(A::Confirmed {
        purpose: purpose::CLOSE_UNSAVED,
        yes: false,
    });
    assert!(no.is_empty());
    assert_eq!(ctl.pane(Side::Left).queue.len(), 1);
    let yes = ctl.apply(A::Confirmed {
        purpose: purpose::CLOSE_UNSAVED,
        yes: true,
    });
    assert_eq!(yes, [Effect::CloseScreen]);
    assert!(ctl.pane(Side::Left).queue.is_empty());
}

#[test]
fn quit_asks_while_edits_are_staged() {
    let img = tempfile::tempdir().unwrap();
    let mut ctl = CommanderController::headless();
    assert!(!ctl.has_staged_edits());
    assert_eq!(ctl.apply(A::RequestQuit), [Effect::Quit]);
    open_image(&mut ctl, Side::Left, img.path());
    let name = files(&ctl, Side::Left)[0].clone();
    select(&mut ctl, Side::Left, &name);
    ctl.apply(A::Delete);
    assert!(ctl.has_staged_edits());
    match ctl.apply(A::RequestQuit).as_slice() {
        [Effect::Confirm(spec)] => assert_eq!(spec.purpose, purpose::QUIT_UNSAVED),
        other => panic!("expected a confirmation, got {other:?}"),
    }
    let yes = ctl.apply(A::Confirmed {
        purpose: purpose::QUIT_UNSAVED,
        yes: true,
    });
    assert_eq!(yes, [Effect::Quit]);
    assert!(!ctl.has_staged_edits());
}

#[test]
fn keys_copy_switch_panes_and_delete_on_the_active_pane() {
    let img = tempfile::tempdir().unwrap();
    let host = tempfile::tempdir().unwrap();
    std::fs::write(host.path().join("KEY.TXT"), b"k").unwrap();
    let mut ctl = CommanderController::headless();
    open_host(&mut ctl, Side::Left, host.path());
    open_image(&mut ctl, Side::Right, img.path());
    assert!(ctl.binds(Key::F(6), Mods::default()));
    select(&mut ctl, Side::Left, "KEY.TXT");
    ctl.apply(key(Key::F(6)));
    assert_eq!(ctl.pane(Side::Right).queue.len(), 1, "{}", ctl.status());

    ctl.apply(key(Key::Tab));
    assert_eq!(ctl.active(), Side::Right);
    let victim = files(&ctl, Side::Right)[0].clone();
    select(&mut ctl, Side::Right, &victim);
    ctl.apply(key(Key::F(8)));
    assert_eq!(ctl.pane(Side::Right).queue.len(), 2, "{}", ctl.status());
    assert!(
        ctl.status().starts_with("[R] toggled delete"),
        "{}",
        ctl.status()
    );

    // F1 toggles the help; its rows come from the key map.
    ctl.apply(key(Key::F(1)));
    assert!(ctl.help_shown());
    assert!(ctl
        .key_help()
        .iter()
        .any(|(l, k)| *l == Command::Copy.label() && k == "F6"));
}

#[test]
fn file_info_opens_from_a_double_click_and_stages_metadata_on_its_pane() {
    let img = tempfile::tempdir().unwrap();
    let mut ctl = CommanderController::headless();
    open_image(&mut ctl, Side::Left, img.path());
    let name = files(&ctl, Side::Left)[0].clone();
    let node = ctl
        .left
        .rows()
        .iter()
        .find(|r| r.name == name)
        .unwrap()
        .node_id
        .clone();
    ctl.apply(A::Pane(Side::Left, P::Activate(node)));
    assert!(ctl.detail().expect("File Info opened").loading);
    settle(&mut ctl);
    let d = ctl.detail().expect("File Info stays open");
    assert!(d.editable && d.entry.name == name && d.preview.is_some() && !d.loading);
    let entry = d.entry.clone();
    ctl.apply(A::StageMetadata(Box::new(MetadataEdit::Dates {
        entry,
        create: 1,
        modify: 2,
        backup: 3,
    })));
    let result = ctl.detail().and_then(|d| d.result.clone());
    assert_eq!(
        result.as_deref(),
        Some(&*format!("Staged date change on '{name}'"))
    );
    assert_eq!(ctl.pane(Side::Left).queue.len(), 1);
}

#[test]
fn a_pane_prompt_answer_returns_to_the_pane_that_asked() {
    let host = tempfile::tempdir().unwrap();
    std::fs::write(host.path().join("DOOMED.TXT"), b"x").unwrap();
    let mut ctl = CommanderController::headless();
    ctl.apply(A::Focus(Side::Left));
    open_host(&mut ctl, Side::Right, host.path());
    select(&mut ctl, Side::Right, "DOOMED.TXT");
    let fx = ctl.apply(A::Pane(Side::Right, P::Delete));
    let purpose = match fx.as_slice() {
        [Effect::Confirm(spec)] => spec.purpose,
        other => panic!("expected a confirmation, got {other:?}"),
    };
    assert_eq!(ctl.asker(purpose), Some(Side::Right));
    ctl.apply(A::Confirmed { purpose, yes: true });
    settle(&mut ctl);
    assert!(!host.path().join("DOOMED.TXT").exists(), "{}", ctl.status());
    assert!(
        ctl.log().iter().any(|l| l.contains("[R] deleted 1")),
        "{:?}",
        ctl.log()
    );
}

fn ctrl(c: char) -> CommanderAction {
    let mods = Mods {
        ctrl: true,
        ..Mods::default()
    };
    A::Key(Key::Char(c), mods)
}

#[test]
fn keyboard_copy_and_delete_act_on_the_cursor_row_when_nothing_is_marked() {
    let img = tempfile::tempdir().unwrap();
    let host = tempfile::tempdir().unwrap();
    std::fs::write(host.path().join("CUR.TXT"), b"c").unwrap();
    let mut ctl = CommanderController::headless();
    open_host(&mut ctl, Side::Left, host.path());
    open_image(&mut ctl, Side::Right, img.path());
    // A host pane starts on `..`, which is never copied.
    ctl.apply(key(Key::F(6)));
    assert!(ctl.pane(Side::Right).queue.is_empty());
    ctl.apply(key(Key::Down));
    ctl.apply(key(Key::F(6)));
    assert_eq!(ctl.pane(Side::Right).queue.len(), 1, "{}", ctl.status());
    ctl.apply(key(Key::Tab));
    ctl.apply(key(Key::End));
    ctl.apply(key(Key::F(8)));
    assert!(
        ctl.pane(Side::Right).queue.is_empty(),
        "removed from staging"
    );
}

#[test]
fn source_sort_partition_and_log_commands() {
    let img = tempfile::tempdir().unwrap();
    let mut ctl = CommanderController::headless();
    match ctl.apply(ctrl('o')).as_slice() {
        [Effect::PickFile(spec)] => assert_eq!(
            spec.purpose,
            crate::controller::commander::purpose::OPEN_IMAGE
        ),
        other => panic!("expected an image picker, got {other:?}"),
    }
    assert_eq!(
        ctl.asker(crate::controller::commander::purpose::OPEN_IMAGE),
        Some(Side::Left)
    );
    let path = mbr_fat16(img.path());
    ctl.apply(A::Picked {
        purpose: crate::controller::commander::purpose::OPEN_IMAGE,
        path: Some(path),
    });
    settle(&mut ctl);
    assert!(ctl.pane(Side::Left).listing.is_loaded(), "{}", ctl.status());
    assert!(!ctl.is_working());

    ctl.apply(ctrl('s'));
    assert!(ctl.pane(Side::Left).listing.is_descending());
    assert!(
        ctl.status().contains("Name, descending"),
        "{}",
        ctl.status()
    );
    ctl.apply(ctrl('s'));
    assert_eq!(ctl.pane(Side::Left).listing.sort_column(), SortColumn::Size);

    ctl.apply(ctrl('p'));
    assert_eq!(
        ctl.partitions_shown(),
        None,
        "one partition: nothing to choose"
    );
    ctl.apply(ctrl('l'));
    assert!(ctl.log_shown());
    ctl.apply(key(Key::Escape));
    assert!(!ctl.log_shown());
    assert_eq!(ctl.key_hint(Command::Checksums), "Ctrl+G");
}

#[test]
fn a_detail_prompt_validates_and_stages_on_the_pane() {
    let img = tempfile::tempdir().unwrap();
    let mut ctl = CommanderController::headless();
    open_image(&mut ctl, Side::Left, img.path());
    ctl.apply(key(Key::End));
    ctl.apply(key(Key::F(3)));
    assert!(ctl.detail().is_some(), "{}", ctl.status());
    let spec = match ctl.apply(A::EditDetail(DetailField::Mode)).as_slice() {
        [Effect::AskText(spec)] => spec.clone(),
        other => panic!("expected a prompt, got {other:?}"),
    };
    assert_eq!(spec.validate(""), Err(String::new()));
    assert!(spec.validate("9").is_err());
    assert!(spec.validate("644").is_ok());
    let fx = ctl.apply(A::Answered {
        purpose: spec.purpose,
        text: Some("99999".into()),
    });
    assert!(
        matches!(fx.as_slice(), [Effect::AskText(s)] if s.error.is_some()),
        "a refused answer asks again: {fx:?}"
    );
    ctl.apply(A::Answered {
        purpose: spec.purpose,
        text: Some("644".into()),
    });
    assert_eq!(ctl.pane(Side::Left).queue.len(), 1);
    let result = ctl.detail().and_then(|d| d.result.clone()).unwrap();
    assert!(result.contains("644"), "{result}");
}

#[test]
fn keymap_problems_reach_the_status_and_the_log() {
    let mut ctl = CommanderController::headless();
    ctl.report_keymap_problems(Vec::new());
    assert!(ctl.log().is_empty());
    ctl.report_keymap_problems(vec!["unknown key `F99`".into()]);
    assert_eq!(ctl.status(), "keybindings: unknown key `F99`");
    assert_eq!(ctl.log().len(), 1);
    ctl.report_keymap_problems(vec!["a".into(), "b".into()]);
    assert!(ctl.status().contains("2 problems"), "{}", ctl.status());
    assert_eq!(ctl.log().len(), 3);
}

fn ctrl_shift(c: char) -> CommanderAction {
    let mods = Mods {
        ctrl: true,
        shift: true,
        ..Mods::default()
    };
    A::Key(Key::Char(c), mods)
}

fn cursor_name(ctl: &CommanderController, side: Side) -> String {
    let pane = ctl.pane(side);
    pane.rows()[pane.cursor_index().unwrap()].name.clone()
}

#[test]
fn quick_view_follows_the_active_cursor_and_gives_the_other_pane_back() {
    let left = tempfile::tempdir().unwrap();
    let right = tempfile::tempdir().unwrap();
    std::fs::write(left.path().join("a.txt"), b"alpha").unwrap();
    std::fs::write(left.path().join("b.txt"), b"bravo").unwrap();
    std::fs::write(right.path().join("r.txt"), b"right").unwrap();
    let mut ctl = CommanderController::headless();
    open_host(&mut ctl, Side::Left, left.path());
    open_host(&mut ctl, Side::Right, right.path());
    select(&mut ctl, Side::Right, "r.txt");
    ctl.apply(A::Focus(Side::Left));
    ctl.apply(key(Key::Down));
    assert_eq!(cursor_name(&ctl, Side::Left), "a.txt");

    ctl.apply(ctrl('k'));
    assert!(ctl.quick_view(), "{}", ctl.status());
    let d = ctl.quick_view_detail().unwrap();
    assert_eq!((d.side, d.entry.name.as_str()), (Side::Left, "a.txt"));
    assert!(
        d.loading && ctl.follow_pending(),
        "the preview waits for the cursor to rest"
    );
    std::thread::sleep(FOLLOW_DELAY);
    settle(&mut ctl);
    let d = ctl.quick_view_detail().unwrap();
    assert_eq!(d.preview.as_deref(), Some(&b"alpha"[..]));

    // The cursor moves on: the detail follows; moving fast reads only where it rests.
    ctl.apply(key(Key::Down));
    assert_eq!(ctl.quick_view_detail().unwrap().entry.name, "b.txt");
    std::thread::sleep(FOLLOW_DELAY);
    settle(&mut ctl);
    let d = ctl.quick_view_detail().unwrap();
    assert_eq!(d.preview.as_deref(), Some(&b"bravo"[..]));

    // The other pane's own state was never touched; Ctrl+K again shows it as it was.
    assert_eq!(ctl.pane(Side::Right).listing.selection(), ["r.txt"]);
    ctl.apply(ctrl('k'));
    assert!(!ctl.quick_view() && ctl.quick_view_detail().is_none());
    assert_eq!(ctl.pane(Side::Right).listing.selection(), ["r.txt"]);
    assert!(!ctl.follow_pending());
}

#[test]
fn primary_k_is_quick_view_and_primary_shift_k_connects() {
    let mut ctl = CommanderController::headless();
    assert!(ctl.binds(Key::Char('k'), ctrl_mods()));
    let fx = ctl.apply(ctrl('k'));
    assert!(fx.is_empty() && ctl.quick_view());
    let fx = ctl.apply(ctrl_shift('k'));
    #[cfg(feature = "remote")]
    assert!(
        matches!(&fx[..], [Effect::AskText(s)] if s.purpose == crate::controller::browser::purpose::CONNECT),
        "{fx:?}"
    );
    #[cfg(not(feature = "remote"))]
    assert!(fx.is_empty() && ctl.status().contains("remote"));
    // The View menu lists the quick view with its chord and the active pane's styles.
    ctl.apply(key(Key::F(9)));
    assert!(ctl.view_menu_shown());
    let menu = ctl.view_menu();
    let qv = menu.iter().find(|i| i.label == "Quick view").unwrap();
    assert!(qv.checked && qv.hint.ends_with("+K") && !qv.hint.contains("Shift"));
    let styles = menu
        .iter()
        .filter(|i| i.kind == ViewItemKind::Style)
        .count();
    assert_eq!(styles, 3);
    ctl.apply(key(Key::Escape));
    assert!(!ctl.view_menu_shown());
}

#[test]
fn left_and_right_stay_free_in_a_flat_list_and_expand_in_a_tree() {
    let host = tempfile::tempdir().unwrap();
    std::fs::create_dir(host.path().join("dir")).unwrap();
    std::fs::write(host.path().join("dir/in.txt"), b"x").unwrap();
    let mut ctl = CommanderController::headless();
    open_host(&mut ctl, Side::Left, host.path());
    assert!(
        !ctl.binds(Key::Right, Mods::default()),
        "a flat list leaves the arrows to the view"
    );
    ctl.apply(A::Pane(
        Side::Left,
        P::SetListingStyle(crate::controller::browser::ListingStyle::CombinedTree),
    ));
    assert!(ctl.binds(Key::Right, Mods::default()));
    ctl.apply(key(Key::Down));
    assert_eq!(cursor_name(&ctl, Side::Left), "dir");
    ctl.apply(key(Key::Right));
    settle(&mut ctl);
    ctl.apply(key(Key::Right));
    assert_eq!(cursor_name(&ctl, Side::Left), "in.txt");
}

#[test]
fn tab_visits_a_folder_tree_then_its_list_then_the_other_pane() {
    use crate::controller::browser::ListingStyle;
    let host = tempfile::tempdir().unwrap();
    std::fs::create_dir(host.path().join("dir")).unwrap();
    let mut ctl = CommanderController::headless();
    open_host(&mut ctl, Side::Left, host.path());
    open_host(&mut ctl, Side::Right, host.path());
    ctl.apply(A::Pane(
        Side::Right,
        P::SetListingStyle(ListingStyle::TreeAndList),
    ));
    ctl.apply(key(Key::Tab));
    assert_eq!(ctl.active(), Side::Right);
    assert!(ctl.pane(Side::Right).tree_focused());
    ctl.apply(key(Key::Tab));
    assert_eq!(ctl.active(), Side::Right);
    assert!(!ctl.pane(Side::Right).tree_focused());
    ctl.apply(key(Key::Tab));
    assert_eq!(ctl.active(), Side::Left);
    // A click in the list takes the keyboard back from the tree.
    ctl.apply(A::Pane(Side::Right, P::FolderTreeClick(0)));
    assert!(ctl.pane(Side::Right).tree_focused());
    select(&mut ctl, Side::Right, "dir");
    assert!(!ctl.pane(Side::Right).tree_focused());
}

fn ctrl_mods() -> Mods {
    Mods {
        ctrl: true,
        ..Mods::default()
    }
}
