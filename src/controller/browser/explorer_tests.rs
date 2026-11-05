//! The browser pane under the Explorer profile: folder tree, volume tools, writes without staging.

use std::path::{Path, PathBuf};

use super::pane::source::tests::{messages, settle};
use super::*;
use crate::controller::commander::{CommanderAction, CommanderController, Side};
use crate::controller::keys::{Key, KeyMap, Mods};
use crate::controller::{Controller, Effect};

type A = BrowserPaneAction;

/// A bare 2 MiB FAT volume holding `/SUB/DEEP` and a CRLF text file `/HELLO.TXT`.
pub(crate) fn fat_volume(dir: &Path) -> PathBuf {
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
    let opts = disk_core::filesystem::CreateDirectoryOptions::default();
    let sub = fs.create_directory(&root, "SUB", &opts).unwrap();
    fs.create_directory(&sub, "DEEP", &opts).unwrap();
    let mut data: &[u8] = b"hello\r\n";
    fs.create_file(&root, "HELLO.TXT", &mut data, 7, &Default::default())
        .unwrap();
    fs.sync_metadata().unwrap();
    img
}

/// A root file's bytes read straight from a FAT image, or `None` when it is gone.
pub(crate) fn root_file(img: &Path, name: &str) -> Option<Vec<u8>> {
    use disk_core::filesystem::Filesystem;
    let f = std::fs::File::open(img).unwrap();
    let mut fs = fs_pc::fat::FatFilesystem::open(f, 0).unwrap();
    let root = fs.root().unwrap();
    let e = fs
        .list_directory(&root)
        .unwrap()
        .into_iter()
        .find(|e| e.name.eq_ignore_ascii_case(name))?;
    Some(fs.read_file(&e, usize::MAX).unwrap())
}

fn explorer(img: &Path) -> BrowserPaneController {
    let mut vm =
        BrowserPaneController::with_profile(PaneServices::headless(), PaneProfile::explorer());
    vm.open_fixed(img.to_path_buf(), None);
    settle(&mut vm);
    assert!(vm.listing.is_loaded(), "{:?}", vm.source_error());
    vm
}

fn click(vm: &mut BrowserPaneController, name: &str) {
    vm.apply(A::Click {
        name: name.into(),
        ctrl: false,
        shift: false,
    });
}

fn tree_names(vm: &BrowserPaneController) -> Vec<String> {
    vm.folder_tree()
        .iter()
        .map(|n| n.entry.name.clone())
        .collect()
}

#[test]
fn the_folder_tree_follows_the_listing_and_moves_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut vm = explorer(&fat_volume(dir.path()));
    assert!(vm.tree_focused(), "the Explorer starts in its tree");
    assert_eq!(vm.folder_tree().len(), 2, "{:?}", tree_names(&vm));
    assert!(vm.folder_tree()[0].expanded);
    // Down in the tree opens SUB in the listing; Open expands it.
    vm.apply(A::CursorDown);
    assert!(
        vm.listing.cwd_path().ends_with("SUB"),
        "{}",
        vm.listing.cwd_path()
    );
    vm.apply(A::ActivateCursor);
    assert!(tree_names(&vm).contains(&"DEEP".to_string()));
    // Entering a folder from the listing moves the tree cursor with it.
    vm.apply(A::SwitchFocus);
    assert!(!vm.tree_focused());
    vm.apply(A::Enter("DEEP".into()));
    let at = vm.tree_cursor();
    assert_eq!(vm.folder_tree()[at].entry.name, "DEEP");
    // Parent in the tree goes up a level (DEEP has nothing to collapse).
    vm.apply(A::SwitchFocus);
    vm.apply(A::Up);
    assert!(vm.listing.cwd_path().ends_with("SUB"));
    // Parent on an expanded folder collapses it first.
    vm.apply(A::Up);
    assert!(!tree_names(&vm).contains(&"DEEP".to_string()));
    assert!(vm.listing.cwd_path().ends_with("SUB"));
}

#[test]
fn a_commander_pane_offers_none_of_the_tools() {
    let dir = tempfile::tempdir().unwrap();
    let mut vm = BrowserPaneController::new();
    vm.apply(A::OpenImage(fat_volume(dir.path())));
    settle(&mut vm);
    for a in [
        A::SwitchFocus,
        A::Fsck,
        A::Repair,
        A::Bless,
        A::EditText,
        A::Import,
        A::EditMetadata,
    ] {
        let said = messages(&vm.apply(a.clone()));
        assert_eq!(said.len(), 1, "{a:?}");
        assert!(
            said[0].ends_with("is not available in this view."),
            "{a:?}: {said:?}"
        );
    }
    assert!(vm.folder_tree().is_empty() && vm.transforms().is_empty());
}

#[test]
fn fsck_reports_and_repair_asks_first() {
    let dir = tempfile::tempdir().unwrap();
    let mut vm = explorer(&fat_volume(dir.path()));
    vm.apply(A::Fsck);
    assert!(vm.is_busy());
    settle(&mut vm);
    let report = vm.tool_report().expect("a report").clone();
    assert!(
        report.lines.iter().any(|l| l.starts_with("Result:")),
        "{report:?}"
    );
    vm.apply(A::DismissToolReport);
    assert!(vm.tool_report().is_none());
    let fx = vm.apply(A::Repair);
    assert!(
        matches!(&fx[..], [Effect::Confirm(c)] if c.purpose == purpose::REPAIR && c.destructive),
        "{fx:?}"
    );
    vm.apply(A::Confirmed {
        purpose: purpose::REPAIR,
        yes: false,
    });
    assert!(!vm.is_busy());
}

#[test]
fn the_text_editor_reads_and_saves_through_the_queue_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let img = fat_volume(dir.path());
    let mut vm = explorer(&img);
    vm.apply(A::SwitchFocus);
    click(&mut vm, "HELLO.TXT");
    vm.apply(A::EditText);
    settle(&mut vm);
    let doc = vm.take_text_doc().expect("the file was read");
    assert_eq!(doc.text, "hello\n", "endings are LF in the editor");
    assert!(vm.take_text_doc().is_none(), "handed over once");
    vm.apply(A::SaveText {
        name: doc.name,
        folder: doc.folder,
        text: "bye\n".into(),
        shape: doc.shape,
    });
    settle(&mut vm);
    assert!(vm.queue.is_empty() && vm.apply_review().is_none());
    assert_eq!(root_file(&img, "HELLO.TXT").unwrap(), b"bye\r\n");
}

#[test]
fn import_writes_and_a_taken_name_gets_the_review() {
    let dir = tempfile::tempdir().unwrap();
    let img = fat_volume(dir.path());
    let host = dir.path().join("A.TXT");
    std::fs::write(&host, b"first").unwrap();
    let mut vm = explorer(&img);
    let fx = vm.apply(A::Import);
    assert!(matches!(&fx[..], [Effect::PickFile(p)] if p.purpose == purpose::IMPORT));
    let picked = |vm: &mut BrowserPaneController| {
        vm.apply(A::Picked {
            purpose: purpose::IMPORT,
            path: Some(host.clone()),
        });
        settle(vm);
    };
    picked(&mut vm);
    assert_eq!(root_file(&img, "A.TXT").unwrap(), b"first");
    std::fs::write(&host, b"second-longer").unwrap();
    picked(&mut vm);
    assert!(vm.apply_review().is_some(), "a taken name asks");
    vm.apply(A::ApplyReviewed(OnConflict::Skip));
    settle(&mut vm);
    assert_eq!(root_file(&img, "A.TXT").unwrap(), b"first");
    picked(&mut vm);
    vm.apply(A::ApplyReviewed(OnConflict::Replace));
    settle(&mut vm);
    assert_eq!(root_file(&img, "A.TXT").unwrap(), b"second-longer");
}

#[test]
fn the_metadata_form_writes_posix_fields_on_an_ext2_volume() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("ext.img");
    let bytes = fs_unix::ext_format::create_blank_ext2(16 * 1024 * 1024, "TUI").unwrap();
    std::fs::write(&img, bytes).unwrap();
    {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&img)
            .unwrap();
        let mut fs = crate::fs::open_editable_filesystem(f, 0, 0, None).unwrap();
        let root = fs.root().unwrap();
        let mut data: &[u8] = b"hi";
        fs.create_file(&root, "hi.txt", &mut data, 2, &Default::default())
            .unwrap();
        fs.sync_metadata().unwrap();
    }
    let mut vm = explorer(&img);
    vm.apply(A::SwitchFocus);
    click(&mut vm, "hi.txt");
    vm.apply(A::EditMetadata);
    let form = vm.metadata_form().expect("the form opens").clone();
    assert!(form.initial.type_code.is_empty(), "ext has no type code");
    let mut bad = form.initial.clone();
    bad.mode = "9999".into();
    vm.apply(A::SubmitMetadata(Box::new(bad)));
    let err = vm.metadata_form().and_then(|f| f.error.clone());
    assert!(err.unwrap().starts_with("Bad mode"));
    let mut fields = form.initial.clone();
    fields.mode = "750".into();
    fields.owner = "4242:4243".into();
    vm.apply(A::SubmitMetadata(Box::new(fields)));
    assert!(vm.metadata_form().is_none());
    settle(&mut vm);
    assert!(vm.queue.is_empty(), "{:?}", vm.apply_report());
    let e = vm
        .listing
        .entries()
        .iter()
        .find(|e| e.name == "hi.txt")
        .unwrap()
        .clone();
    assert_eq!(e.mode.map(|m| m & 0o7777), Some(0o750));
    assert_eq!((e.uid, e.gid), (Some(4242), Some(4243)));
}

#[test]
fn bless_asks_for_a_folder_and_transforms_name_the_image() {
    let dir = tempfile::tempdir().unwrap();
    let img = fat_volume(dir.path());
    let mut vm = explorer(&img);
    let said = messages(&vm.apply(A::Bless));
    assert_eq!(said, ["select a folder to bless as the System Folder."]);
    vm.apply(A::CursorDown);
    let fx = vm.apply(A::Bless);
    assert!(matches!(&fx[..], [Effect::Confirm(c)] if c.purpose == purpose::BLESS));
    let t = vm.transforms();
    assert_eq!(t.len(), 4);
    assert!(t[0].1.contains(&img.display().to_string()), "{t:?}");
}

#[test]
fn the_single_pane_screen_routes_keys_to_its_one_pane() {
    let dir = tempfile::tempdir().unwrap();
    let img = fat_volume(dir.path());
    let mut ctl = CommanderController::single(
        PaneServices::headless(),
        KeyMap::explorer_terminal(),
        PaneProfile::explorer(),
    );
    assert!(ctl.is_single());
    ctl.open_fixed(img, None);
    let start = std::time::Instant::now();
    while ctl.is_working() {
        assert!(start.elapsed().as_secs() < 60);
        std::thread::sleep(std::time::Duration::from_millis(5));
        ctl.tick();
    }
    assert!(ctl.pane(Side::Left).tree_focused());
    ctl.apply(CommanderAction::Key(Key::Tab, Mods::default()));
    assert!(
        !ctl.pane(Side::Left).tree_focused(),
        "Tab switches tree and list"
    );
    assert_eq!(ctl.active(), Side::Left);
    assert!(
        !ctl.binds(Key::F(6), Mods::default()),
        "no second pane to copy to"
    );
    let ctrl = Mods {
        ctrl: true,
        ..Mods::default()
    };
    ctl.apply(CommanderAction::Key(Key::Char('f'), ctrl));
    assert!(ctl.is_working(), "Ctrl+F checks the filesystem");
    // Esc asks before closing even with nothing staged.
    while ctl.is_working() {
        std::thread::sleep(std::time::Duration::from_millis(5));
        ctl.tick();
    }
    ctl.apply(CommanderAction::Pane(Side::Left, A::DismissToolReport));
    let fx = ctl.apply(CommanderAction::Key(Key::Escape, Mods::default()));
    assert!(
        matches!(&fx[..], [Effect::Confirm(c)] if !c.destructive),
        "{fx:?}"
    );
    let help = ctl.key_help();
    assert!(
        help.iter()
            .any(|(l, k)| *l == "Check filesystem" && k == "Ctrl+F"),
        "{help:?}"
    );
    assert!(!help.iter().any(|(l, _)| *l == "Copy"));
}
