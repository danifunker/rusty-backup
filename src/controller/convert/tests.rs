use super::*;
use crate::model::job::NoNotify;

fn floppy(path: &Path) {
    std::fs::write(
        path,
        fs_pc::fat::create_dos_floppy(1_474_560, Some("F")).unwrap(),
    )
    .unwrap();
}

fn controller() -> ConvertController {
    ConvertController::new(Arc::new(NoNotify))
}

fn settle(c: &mut ConvertController) {
    c.wait_idle();
    c.tick();
}

#[test]
fn a_folder_lists_each_kind_and_converts_into_a_mirrored_tree() {
    let src = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    std::fs::create_dir(src.path().join("sub")).unwrap();
    floppy(&src.path().join("a.img"));
    floppy(&src.path().join("sub/b.img"));
    let mut c = controller();
    c.apply(ConvertAction::SetSource(src.path().to_path_buf()));
    settle(&mut c);
    assert_eq!(c.items().len(), 1, "one level deep by default");
    c.apply(ConvertAction::SetRecursive(true));
    settle(&mut c);
    assert_eq!(c.items().len(), 2);
    assert_eq!(c.shown_kinds(), vec![ImageKind::Floppy]);
    assert_eq!(
        c.start_problem().as_deref(),
        Some("Choose an output folder.")
    );
    c.apply(ConvertAction::Picked {
        purpose: purpose::OUTPUT_FOLDER,
        path: Some(out.path().to_path_buf()),
    });
    c.apply(ConvertAction::SetFormat(ImageKind::Floppy, Format::Woz));
    let rows = c.rows();
    assert!(
        rows[0].skipped && rows[0].outcome.contains("140K"),
        "{rows:?}"
    );
    assert!(c.start_problem().is_some());
    c.apply(ConvertAction::SetFormat(ImageKind::Floppy, Format::Dc42));
    assert_eq!(c.extension(ImageKind::Floppy), "dsk");
    assert_eq!(c.rows()[1].outcome, "-> b.dsk");
    assert_eq!(c.start_label(), "Convert 2 files");
    c.apply(ConvertAction::Start(ChdChoices::default()));
    settle(&mut c);
    assert_eq!(
        c.last_result(),
        Some(&Ok(RunResult {
            succeeded: 2,
            failed: 0,
            skipped: 0
        }))
    );
    assert!(out.path().join("sub/b.dsk").is_file());

    // Again: both outputs exist, so Start asks.
    let effects = c.apply(ConvertAction::Start(ChdChoices::default()));
    assert!(matches!(effects.as_slice(), [Effect::Choose(q)] if q.purpose == purpose::CONFLICTS));
    c.apply(ConvertAction::Chose {
        purpose: purpose::CONFLICTS,
        index: 0,
    });
    assert!(
        !c.is_running(),
        "Skip with every output existing converts nothing"
    );
}

#[test]
fn one_file_saves_beside_itself_and_follows_the_extension() {
    let dir = tempfile::tempdir().unwrap();
    let f = dir.path().join("disk.img");
    floppy(&f);
    let mut c = controller();
    c.apply(ConvertAction::SetSource(f.clone()));
    settle(&mut c);
    assert!(!c.source_is_folder());
    assert_eq!(c.output(), None, "Raw would overwrite the source");
    c.apply(ConvertAction::SetFormat(ImageKind::Floppy, Format::D88));
    assert_eq!(c.output(), Some(dir.path().join("disk.d88").as_path()));
    c.apply(ConvertAction::SetFloppySize(FloppySize::Refit(737_280)));
    c.apply(ConvertAction::SetVerify(true));
    c.apply(ConvertAction::Start(ChdChoices::default()));
    settle(&mut c);
    let log: Vec<String> = c.take_log().into_iter().map(|(_, m)| m).collect();
    assert!(
        log.iter().any(|l| l.contains("Refitted to 720 KB")),
        "{log:?}"
    );
    assert!(log.iter().any(|l| l.contains("Verified")), "{log:?}");
    assert!(dir.path().join("disk.d88").is_file());
}

#[test]
fn the_slow_filesystem_help_names_them() {
    let text = hints::measure_slow();
    assert!(text.contains("HFS+,") && text.contains(" ext,"), "{text}");
}
