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
    // A FAT volume defaults to Zip; loose files ask for a folder.
    ctl.apply(A::SetExportFormat(ExportFormat::LooseFiles));
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
    A::Key(Key::Char(c), ctrl_mods())
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
    let primary = if cfg!(target_os = "macos") {
        "Cmd"
    } else {
        "Ctrl"
    };
    assert_eq!(ctl.key_hint(Command::Checksums), format!("{primary}+G"));
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
        shift: true,
        ..ctrl_mods()
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

/// The `Primary` modifier: Cmd on macOS, Ctrl elsewhere.
fn ctrl_mods() -> Mods {
    let mac = cfg!(target_os = "macos");
    Mods {
        ctrl: !mac,
        cmd: mac,
        ..Mods::default()
    }
}

#[test]
fn the_export_default_follows_the_filesystem_family() {
    use ExportFormat as F;
    let cases = [
        ("FAT16", F::Zip),
        ("NTFS", F::Zip),
        ("HFS", F::MacArchive),
        ("HFS+", F::MacArchive),
        ("MFS", F::MacArchive),
        ("ext4", F::TarGz),
        ("UFS", F::TarGz),
        ("Alto BFS", F::LooseFiles),
        ("ProDOS", F::LooseFiles),
        ("ISO 9660", F::Zip),
        ("UDF", F::Zip),
    ];
    for (fs, want) in cases {
        assert_eq!(default_export_format(fs), want, "{fs}");
    }
}

#[test]
fn a_volume_exports_to_one_archive_starting_from_its_filesystems_format() {
    use ExportFormat as F;
    assert_eq!(default_volume_export("ext4"), F::TarGz);
    assert_eq!(default_volume_export("ISO 9660"), F::Zip);
    assert_eq!(default_volume_export("ProDOS"), F::Zip);
    assert_eq!(default_volume_export("HFS+"), F::MacArchive);
    assert!(volume_export_formats("FAT16")
        .iter()
        .all(|f| f.is_single_file()));
    assert!(!volume_export_formats("FAT16").contains(&F::StuffIt));
    assert!(volume_export_formats("HFS").contains(&F::StuffIt));
}

#[test]
fn an_export_format_is_named_or_read_off_the_output_name() {
    use std::path::Path;
    use ExportFormat as F;
    assert_eq!(export_format_named("TGZ"), Some(F::TarGz));
    assert_eq!(export_format_named(".zip"), Some(F::Zip));
    assert_eq!(export_format_named("rar"), None);
    assert_eq!(
        export_format_of_path(Path::new("a/b.TAR.ZST")),
        Some(F::TarZstd)
    );
    assert_eq!(
        export_format_of_path(Path::new("disc.sit.hqx")),
        Some(F::StuffItHqx)
    );
    assert_eq!(export_format_of_path(Path::new("plain.tar")), Some(F::Tar));
    assert_eq!(export_format_of_path(Path::new("noext")), None);
}

#[test]
fn an_explicit_export_pick_survives_a_refresh_but_not_another_volume() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = CommanderController::headless();
    open_image(&mut ctl, Side::Left, dir.path());
    assert_eq!(ctl.export_format(), ExportFormat::Zip);
    ctl.apply(A::SetExportFormat(ExportFormat::TarZstd));
    ctl.apply(A::Pane(Side::Left, P::Reload));
    settle(&mut ctl);
    assert_eq!(ctl.export_format(), ExportFormat::TarZstd);
    assert_eq!(
        export_choices()[ctl.export_choice_index()].format,
        ExportFormat::TarZstd
    );
    let other = tempfile::tempdir().unwrap();
    open_image(&mut ctl, Side::Left, other.path());
    assert_eq!(ctl.export_format(), ExportFormat::Zip);
}

#[test]
fn file_info_offers_the_mac_fields_and_stages_type_creator_and_a_date() {
    let dir = tempfile::tempdir().unwrap();
    let img = crate::controller::test_fixtures::hfs_volume_with_text(dir.path());
    let mut ctl = CommanderController::headless();
    ctl.apply(A::Pane(Side::Left, P::OpenImage(img)));
    settle(&mut ctl);
    let node = ctl
        .left
        .rows()
        .iter()
        .find(|r| r.name == "README")
        .expect("README listed")
        .node_id
        .clone();
    ctl.apply(A::Pane(Side::Left, P::Activate(node)));
    settle(&mut ctl);
    let fields = ctl.detail_fields();
    for f in [
        DetailField::TypeCreator,
        DetailField::Created,
        DetailField::Modified,
        DetailField::Backup,
    ] {
        assert!(fields.contains(&f), "{f:?} offered: {fields:?}");
    }
    assert!(!fields.contains(&DetailField::ProdosType));
    let meta = ctl.detail_metadata().unwrap();
    assert_eq!(meta.type_creator.as_ref().unwrap().type_text, "TEXT");
    let spec = match ctl
        .apply(A::EditDetail(DetailField::TypeCreator))
        .as_slice()
    {
        [Effect::AskText(spec)] => spec.clone(),
        other => panic!("expected a prompt, got {other:?}"),
    };
    assert_eq!(spec.initial, "TEXT ttxt");
    assert!(spec.validate("TOOLONG x").is_err());
    let suggestions = ctl.suggestions_for(spec.purpose);
    assert!(
        suggestions.iter().any(|(t, _)| t == "APPL ttxt"),
        "{suggestions:?}"
    );
    ctl.apply(A::Answered {
        purpose: spec.purpose,
        text: Some("PICT 8BIM".into()),
    });
    let meta = ctl.detail_metadata().unwrap();
    assert_eq!(meta.type_creator.unwrap().creator_text, "8BIM");
    let spec = match ctl.apply(A::EditDetail(DetailField::Backup)).as_slice() {
        [Effect::AskText(spec)] => spec.clone(),
        other => panic!("expected a prompt, got {other:?}"),
    };
    ctl.apply(A::Answered {
        purpose: spec.purpose,
        text: Some("2001-02-03 04:05:06".into()),
    });
    let dates = ctl.detail_metadata().unwrap().dates.unwrap();
    assert_eq!(dates[2].as_deref(), Some("2001-02-03 04:05:06"));
    assert!(dates[1].is_some(), "the modified date is kept");
    assert_eq!(ctl.pane(Side::Left).queue.len(), 2);
    let fx = ctl.apply(A::EditDetail(DetailField::ProdosType));
    assert!(fx.is_empty());
    let result = ctl.detail().and_then(|d| d.result.clone()).unwrap();
    assert!(result.contains("no ProDOS type"), "{result}");
}

#[test]
fn printable_shows_control_characters_as_escapes() {
    assert_eq!(super::printable("Icon\r"), "Icon\\x0D");
    assert_eq!(super::printable("plain.txt"), "plain.txt");
}

#[test]
fn copy_hint_says_why_a_pane_cannot_take_a_copy() {
    let ctl = CommanderController::headless();
    let into = ctl.copy_hint(Side::Right);
    assert!(into.contains("right"), "{into}");
    assert!(ctl.delete_hint().contains('L'));
}

#[test]
fn a_closed_pane_names_nothing_and_a_host_pane_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = CommanderController::headless();
    let closed = ctl.pane(Side::Left).source_label();
    assert_eq!(closed, "Open image, backup, or folder...");
    ctl.left.listing.load_host_root(dir.path().into()).unwrap();
    let pane = ctl.pane(Side::Left);
    assert_eq!(pane.source_label(), "Local folder");
    assert_eq!(pane.path_prefix(), "host ");
    assert_eq!(pane.volume_title(), "local folder");
}

#[test]
fn the_edit_bar_and_the_edits_window_follow_the_queue() {
    let img = tempfile::tempdir().unwrap();
    let mut ctl = CommanderController::headless();
    open_image(&mut ctl, Side::Left, img.path());
    let name = files(&ctl, Side::Left)[0].clone();
    select(&mut ctl, Side::Left, &name);
    ctl.apply(A::Delete);
    let bar = ctl.edit_bar(Side::Left);
    assert_eq!(bar.staged, 1);
    assert_eq!(bar.apply_label, "Apply Edits (1)");
    assert!(bar.deletable);
    ctl.apply(A::Pane(Side::Left, P::ShowEdits(true)));
    let windows = ctl.pane_windows(Side::Left, false);
    let edits = windows
        .iter()
        .find(|w| w.kind == WindowKind::Edits)
        .unwrap();
    let Part::Items { lines, actions, .. } = &edits.parts[1] else {
        panic!(
            "the edits are listed with their own button: {:?}",
            edits.parts
        );
    };
    assert_eq!(lines.len(), 1);
    assert!(lines[0].starts_with("1. "), "{lines:?}");
    for a in actions[0].clone() {
        ctl.apply(a);
    }
    assert!(ctl.pane(Side::Left).queue.is_empty());
    assert_eq!(ctl.edit_bar(Side::Left).staged, 0);
}

#[test]
fn the_mark_bar_counts_marked_rows() {
    let img = tempfile::tempdir().unwrap();
    let mut ctl = CommanderController::headless();
    open_image(&mut ctl, Side::Left, img.path());
    assert_eq!(ctl.mark_bar(Side::Left).count, 0);
    let name = files(&ctl, Side::Left)[0].clone();
    let id = ctl
        .pane(Side::Left)
        .rows()
        .iter()
        .find(|r| r.name == name)
        .map(|r| r.node_id.clone())
        .unwrap();
    ctl.apply(A::Pane(Side::Left, P::Mark(id)));
    let bar = ctl.mark_bar(Side::Left);
    assert_eq!(bar.count, 1);
    assert_eq!(bar.label, "1 selected");
}

#[test]
fn the_checksum_window_copies_each_value() {
    let img = tempfile::tempdir().unwrap();
    let mut ctl = CommanderController::headless();
    open_image(&mut ctl, Side::Left, img.path());
    let name = files(&ctl, Side::Left)[0].clone();
    select(&mut ctl, Side::Left, &name);
    ctl.apply(A::Checksums(Side::Left));
    settle(&mut ctl);
    let windows = ctl.pane_windows(Side::Left, false);
    let sums = windows
        .iter()
        .find(|w| w.kind == WindowKind::Checksums)
        .unwrap();
    let Some(Part::Items { lines, actions, .. }) = sums.parts.last() else {
        panic!("one line per value: {:?}", sums.parts);
    };
    assert_eq!(lines.len(), HashAlgo::ALL.len(), "{lines:?}");
    let fx = ctl.apply(actions[0][0].clone());
    assert!(
        matches!(fx.as_slice(), [Effect::CopyToClipboard(_)]),
        "{fx:?}"
    );
}

#[test]
fn submit_reads_the_fields_and_the_picked_option() {
    use crate::controller::browser::{LineEnding, TextShape};
    use crate::model::text_edit::TextEncoding;
    let shape = TextShape {
        encoding: TextEncoding::Utf8,
        ending: LineEnding::Lf,
        mixed_endings: false,
    };
    let form = Form::Editor {
        name: "A.TXT".into(),
        folder: "/".into(),
        shape,
    };
    let values = [(FieldId::EditorText, "one\ntwo".to_string())];
    match submit(&form, &values, 1).as_slice() {
        [A::Pane(Side::Left, P::SaveText { text, shape, .. })] => {
            assert_eq!(text, "one\ntwo");
            assert_eq!(shape.ending, LineEnding::CrLf);
        }
        other => panic!("{other:?}"),
    }
    let values = [(FieldId::Password, "secret".to_string())];
    assert!(matches!(
        submit(&Form::Password, &values, 0).as_slice(),
        [A::Mode(ModeAction::Password(p))] if p == "secret"
    ));
}

#[test]
fn a_live_field_sends_only_what_parses() {
    assert!(matches!(
        field_action(FieldId::BudgetAbsolute, " 12 "),
        Some(A::Mode(ModeAction::Budget(BudgetAction::SetAbsoluteMib(
            12
        ))))
    ));
    assert!(field_action(FieldId::BudgetHeadroom, "lots").is_none());
    assert!(field_action(FieldId::BudgetHeadroom, "0").is_none());
    assert!(field_action(FieldId::Password, "x").is_none());
}

#[test]
fn a_drop_opens_in_an_empty_pane_copies_into_a_volume_and_asks_only_for_an_image() {
    let img = tempfile::tempdir().unwrap();
    let mut ctl = CommanderController::headless();
    open_image(&mut ctl, Side::Left, img.path());
    let text = std::path::PathBuf::from("C:/somewhere/NOTES.TXT");
    let disk = std::path::PathBuf::from("C:/somewhere/game.hfv");
    let opens = |plan: Option<DropPlan>, side: Side| match plan {
        Some(DropPlan::Do(a)) => {
            matches!(a.as_slice(), [A::Focus(s), A::OpenDropped(_)] if *s == side)
        }
        _ => false,
    };
    assert!(opens(
        ctl.drop_plan(Side::Right, std::slice::from_ref(&disk)),
        Side::Right
    ));
    match ctl.drop_plan(Side::Left, std::slice::from_ref(&text)) {
        Some(DropPlan::Do(a)) => {
            assert!(
                matches!(a.as_slice(), [A::Focus(Side::Left), A::Pane(Side::Left, P::AddPicked(p))] if p == std::slice::from_ref(&text))
            )
        }
        other => panic!("a plain file copies in: {other:?}"),
    }
    match ctl.drop_plan(Side::Left, std::slice::from_ref(&disk)) {
        Some(DropPlan::Ask { choices, .. }) => {
            assert_eq!(choices.len(), 2);
            assert!(matches!(
                choices[0].1.as_slice(),
                [_, A::Pane(_, P::AddPicked(_))]
            ));
            assert!(matches!(choices[1].1.as_slice(), [_, A::OpenDropped(_)]));
        }
        other => panic!("an image on a volume asks: {other:?}"),
    }
    assert!(matches!(
        ctl.drop_plan(Side::Left, &[disk, text]),
        Some(DropPlan::Do(_))
    ));
    assert!(ctl.drop_plan(Side::Left, &[]).is_none());
}

#[test]
fn the_export_bar_exports_the_marked_rows_else_the_volume_in_its_one_format() {
    let img = tempfile::tempdir().unwrap();
    let mut ctl = CommanderController::headless();
    open_image(&mut ctl, Side::Left, img.path());
    let bar = ctl.export_bar(Side::Left);
    assert_eq!(bar.button, "Export Volume...");
    // Loose files in each fork form first, then the archives a FAT volume can fill.
    let labels: Vec<&str> = bar.choices.iter().map(|c| c.label.as_str()).collect();
    assert!(labels[0].starts_with("Files, "), "{labels:?}");
    assert!(!labels.contains(&"StuffIt (.sit)"), "{labels:?}");
    assert_eq!(bar.choices[bar.pick].format, ExportFormat::Zip);
    let tar = bar
        .choices
        .iter()
        .position(|c| c.format == ExportFormat::Tar);
    let choice = tar.unwrap();
    ctl.apply(A::PickExport {
        from: Side::Left,
        choice,
    });
    assert_eq!(ctl.export_bar(Side::Left).pick, choice);
    // Nothing marked: the whole volume, named after it.
    match ctl.apply(A::ExportFromBar(Side::Left)).as_slice() {
        [Effect::PickFile(spec)] => assert!(spec.file_name.ends_with(".tar"), "{spec:?}"),
        other => panic!("expected a save picker, got {other:?}"),
    }
    ctl.apply(A::Picked {
        purpose: purpose::EXPORT,
        path: None,
    });
    let name = files(&ctl, Side::Left)[0].clone();
    select(&mut ctl, Side::Left, &name);
    assert_eq!(ctl.export_bar(Side::Left).button, "Export 1 Selected...");
}

#[test]
fn the_preview_shows_a_large_files_head_and_an_empty_data_forks_resource_fork() {
    let detail = |size: u64, data: Vec<u8>, rsrc: Option<Vec<u8>>| FileDetail {
        side: Side::Left,
        entry: FileEntry::new_file("F".into(), "/F".into(), size, 0),
        editable: false,
        fs_type: "HFS".into(),
        loading: false,
        preview: Some(data),
        rsrc_preview: rsrc,
        xattrs: Vec::new(),
        id_names: IdNameMap::default(),
        size_plan: None,
        result: None,
        serial: 0,
    };
    let big = detail(
        3 * MAX_PREVIEW_SIZE as u64,
        vec![b'a'; MAX_PREVIEW_SIZE],
        None,
    );
    match big.panel_preview() {
        PanelPreview::Text(t) => {
            assert!(t.starts_with("(The first 1.0 MiB of 3.0 MiB.)"), "{t:.60}")
        }
        other => panic!("expected text, got {other:?}"),
    }
    let app = detail(0, Vec::new(), Some(vec![0, 0, 1, 0]));
    match app.panel_preview() {
        PanelPreview::Hex(h) => assert!(h.starts_with("(The data fork is empty;"), "{h}"),
        other => panic!("expected hex, got {other:?}"),
    }
    assert_eq!(
        detail(0, Vec::new(), None).panel_preview(),
        PanelPreview::Note("Empty file.".into())
    );
}
