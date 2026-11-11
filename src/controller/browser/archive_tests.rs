//! The Inspect pane's archives: the viewer's extract, saving as stored or decoded, tar and Mac archive imports.

use std::path::{Path, PathBuf};

use super::explorer_tests::{fat_volume, root_file};
use super::inspect_tests::{inspect, pane, settle};
use super::*;
use crate::controller::commander::{CommanderAction, CommanderController, Side};
use crate::controller::{Controller, Effect};

type A = BrowserPaneAction;

/// A stored StuffIt archive of `a.txt` and `b.txt`.
fn sit_bytes() -> Vec<u8> {
    use arc_mac::stuffit::{build_archive_tree, StuffItInput, StuffItInputNode, WriteMethod};
    let file = |name: &str, data: &[u8]| {
        StuffItInputNode::File(StuffItInput {
            name: name.into(),
            type_code: *b"TEXT",
            creator_code: *b"ttxt",
            finder_flags: 0,
            create_date: 0,
            mod_date: 0,
            data_fork: data.to_vec(),
            resource_fork: Vec::new(),
        })
    };
    let nodes = [file("a.txt", b"alpha"), file("b.txt", b"beta")];
    build_archive_tree(&nodes, WriteMethod::Store).unwrap()
}

/// Write `name` holding `data` into the root of the volume image `img`.
fn put_file(img: &Path, name: &str, data: &[u8]) {
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(img)
        .unwrap();
    let mut fs = crate::fs::open_editable_filesystem(f, 0, 0, None).unwrap();
    let root = fs.root().unwrap();
    let mut src: &[u8] = data;
    fs.create_file(
        &root,
        name,
        &mut src,
        data.len() as u64,
        &Default::default(),
    )
    .unwrap();
    fs.sync_metadata().unwrap();
}

/// The one picker the effects ask for: its purpose, and whether it saves.
fn picker(fx: &[Effect]) -> (&'static str, bool) {
    let found = fx.iter().find_map(|e| match e {
        Effect::PickFile(s) | Effect::PickFolder(s) => Some((s.purpose, s.save)),
        _ => None,
    });
    found.unwrap_or_else(|| panic!("no picker in {fx:?}"))
}

fn answer(ctl: &mut CommanderController, purpose: &'static str, path: PathBuf) {
    let fx = ctl.apply(CommanderAction::Picked {
        purpose,
        path: Some(path),
    });
    assert!(fx.is_empty(), "{fx:?}");
    settle(ctl);
}

fn notice(ctl: &mut CommanderController) -> String {
    ctl.host_pane().take_archive_notice().unwrap_or_default()
}

fn archive_volume(dir: &Path) -> CommanderController {
    let img = fat_volume(dir);
    put_file(&img, "ARC.SIT", &sit_bytes());
    inspect(&img)
}

#[test]
fn the_archive_viewer_extracts_the_marked_entries_into_a_picked_folder() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = archive_volume(dir.path());
    let read = A::ReadArchive {
        path: "/ARC.SIT".into(),
        purpose: ArchiveUse::Browse,
    };
    pane(&mut ctl, read);
    settle(&mut ctl);
    pane(&mut ctl, A::ArchiveMark(1));
    let format = ForkFormat::Raw;
    pane(
        &mut ctl,
        A::ArchiveFormat {
            viewer: true,
            format,
        },
    );
    let v = ctl.pane(Side::Left).archive_viewer().unwrap();
    assert_eq!((v.file_count(), v.marked_files(), v.format), (2, 1, format));
    let fx = pane(&mut ctl, A::ArchiveExtract { all: false });
    let (purpose, save) = picker(&fx);
    assert!(!save);
    let out = dir.path().join("out");
    std::fs::create_dir(&out).unwrap();
    answer(&mut ctl, purpose, out.clone());
    assert!(notice(&mut ctl).starts_with("Extracted 1 file(s)"));
    assert_eq!(std::fs::read(out.join("a.txt")).unwrap(), b"alpha");
    assert!(!out.join("b.txt").exists());
    pane(&mut ctl, A::CloseArchive);
    assert!(ctl.pane(Side::Left).archive_viewer().is_none());
}

#[test]
fn a_mac_archive_on_the_volume_saves_as_stored_or_decoded() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = archive_volume(dir.path());
    let read = || A::ReadArchive {
        path: "/ARC.SIT".into(),
        purpose: ArchiveUse::Extract,
    };
    pane(&mut ctl, read());
    settle(&mut ctl);
    assert_eq!(
        ctl.pane(Side::Left).archive_save().unwrap().entry_name,
        "ARC.SIT"
    );
    let (purpose, save) = picker(&pane(&mut ctl, A::SaveArchive { raw: true }));
    assert!(save);
    let raw = dir.path().join("copy.sit");
    answer(&mut ctl, purpose, raw.clone());
    assert!(ctl.pane(Side::Left).archive_save().is_none());
    assert!(notice(&mut ctl).starts_with("Wrote"));
    assert_eq!(std::fs::read(&raw).unwrap(), sit_bytes());

    pane(&mut ctl, read());
    settle(&mut ctl);
    let format = ForkFormat::Raw;
    pane(
        &mut ctl,
        A::ArchiveFormat {
            viewer: false,
            format,
        },
    );
    let (purpose, _) = picker(&pane(&mut ctl, A::SaveArchive { raw: false }));
    let out = dir.path().join("decoded");
    std::fs::create_dir(&out).unwrap();
    answer(&mut ctl, purpose, out.clone());
    assert_eq!(std::fs::read(out.join("b.txt")).unwrap(), b"beta");
}

fn tar_of(dir: &Path, name: &str, data: &[u8]) -> PathBuf {
    let path = dir.join("in.tar");
    let mut b = tar::Builder::new(std::fs::File::create(&path).unwrap());
    let mut h = tar::Header::new_ustar();
    h.set_size(data.len() as u64);
    h.set_mode(0o644);
    h.set_cksum();
    b.append_data(&mut h, name, data).unwrap();
    b.finish().unwrap();
    path
}

#[test]
fn a_dropped_tar_imports_its_contents_or_is_staged_as_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let img = fat_volume(dir.path());
    let tar = tar_of(dir.path(), "T1.TXT", b"from tar");
    let mut ctl = inspect(&img);
    pane(&mut ctl, A::AddPicked(vec![tar.clone()]));
    settle(&mut ctl);
    assert_eq!(
        ctl.pane(Side::Left).pending_tar().as_deref(),
        Some("in.tar")
    );
    assert!(ctl.pane(Side::Left).queue.is_empty());
    pane(&mut ctl, A::ChooseTar(TarChoice::AsFile));
    assert_eq!(ctl.pane(Side::Left).queue.len(), 1);
    ctl.host_pane().queue.clear();

    pane(&mut ctl, A::AddPicked(vec![tar]));
    settle(&mut ctl);
    pane(&mut ctl, A::ChooseTar(TarChoice::Import));
    settle(&mut ctl);
    assert!(ctl.pane(Side::Left).pending_tar().is_none());
    let said = notice(&mut ctl);
    assert!(said.starts_with("Imported 1 files"), "{said}");
    assert_eq!(root_file(&img, "T1.TXT").as_deref(), Some(&b"from tar"[..]));
    assert!(ctl.pane(Side::Left).entry_at("/T1.TXT").is_some());
}

#[test]
fn a_picked_binhex_expands_on_an_hfs_volume_and_stays_plain_elsewhere() {
    let dir = tempfile::tempdir().unwrap();
    let hqx = dir.path().join("x.hqx");
    let text = crate::model::archive_import::tests::hqx("Note", b"data");
    std::fs::write(&hqx, text).unwrap();

    let fat = fat_volume(dir.path());
    let mut ctl = inspect(&fat);
    pane(&mut ctl, A::AddPicked(vec![hqx.clone()]));
    settle(&mut ctl);
    assert!(ctl.pane(Side::Left).pending_import().is_none());
    assert_eq!(ctl.pane(Side::Left).queue.len(), 1);

    let img = dir.path().join("vol.hfv");
    let bytes = fs_mac::hfs::create_blank_hfs(4 * 1024 * 1024, 512, "T").unwrap();
    std::fs::write(&img, bytes).unwrap();
    let mut ctl = inspect(&img);
    pane(&mut ctl, A::AddPicked(vec![hqx]));
    settle(&mut ctl);
    let (name, kind) = ctl.pane(Side::Left).pending_import().expect("asks first");
    assert_eq!(
        (name.as_str(), kind),
        ("x.hqx", MacArchiveKind::BinHexSingleFile)
    );
    pane(&mut ctl, A::ChooseImport(ImportChoice::Expand));
    settle(&mut ctl);
    assert_eq!(notice(&mut ctl), "Imported contents of x.hqx");
    let adds = ctl.pane(Side::Left).queue.pending_adds_for("/");
    assert_eq!(
        adds.iter().map(|e| e.name.as_str()).collect::<Vec<_>>(),
        ["Note"]
    );
}

/// An Installer Tome holding one file whose data fork is stored (block type 1).
fn stored_tome(name: &str, data: &[u8]) -> Vec<u8> {
    let mut t = vec![0u8; 0x24 + 0x80];
    t[0..4].copy_from_slice(&arc_mac::tome::TOME_MAGIC);
    t[0x1b] = 1;
    let r = 0x24;
    t[r + 6] = name.len() as u8;
    t[r + 7..r + 7 + name.len()].copy_from_slice(name.as_bytes());
    t[r + 0x26..r + 0x2e].copy_from_slice(b"TEXTttxt");
    let fork = [data.len() as u32, t.len() as u32, data.len() as u32 + 4, 0];
    for (k, v) in fork.iter().enumerate() {
        t[r + 0x3c + 4 * k..r + 0x40 + 4 * k].copy_from_slice(&v.to_be_bytes());
    }
    t.extend_from_slice(&[1, 0, 0, 0]);
    t.extend_from_slice(data);
    t
}

#[test]
fn a_picked_installer_tome_with_no_extension_expands_on_an_hfs_volume() {
    let dir = tempfile::tempdir().unwrap();
    let tome = dir.path().join("Installation Tome");
    std::fs::write(&tome, stored_tome("Read Me", b"hello")).unwrap();
    let img = dir.path().join("vol.hfv");
    let bytes = fs_mac::hfs::create_blank_hfs(4 * 1024 * 1024, 512, "T").unwrap();
    std::fs::write(&img, bytes).unwrap();
    let mut ctl = inspect(&img);
    pane(&mut ctl, A::AddPicked(vec![tome]));
    settle(&mut ctl);
    let (name, kind) = ctl.pane(Side::Left).pending_import().expect("asks first");
    assert_eq!(kind, MacArchiveKind::InstallerTome);
    let prompt = crate::controller::browser::import_prompt(&name, kind);
    assert_eq!(
        (prompt.title, prompt.expand, prompt.convert_only),
        ("Expand archive?", "Expand", None)
    );
    assert_eq!(
        prompt.intro,
        "Installation Tome is an Installer Tome archive."
    );
    pane(&mut ctl, A::ChooseImport(ImportChoice::Expand));
    settle(&mut ctl);
    assert_eq!(notice(&mut ctl), "Imported contents of Installation Tome");
    let adds = ctl.pane(Side::Left).queue.pending_adds_for("/");
    assert_eq!(
        adds.iter().map(|e| e.name.as_str()).collect::<Vec<_>>(),
        ["Read Me"]
    );
}

#[test]
fn the_tree_listing_saves_to_a_picked_file_in_a_job() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = inspect(&fat_volume(dir.path()));
    let fx = pane(&mut ctl, A::SaveTree);
    assert!(fx.iter().all(|e| matches!(e, Effect::Message(_))), "{fx:?}");
    pane(&mut ctl, A::ListTree { ids: false });
    settle(&mut ctl);
    assert!(ctl.host_pane().take_tree_text().is_some());
    let (purpose, save) = picker(&pane(&mut ctl, A::SaveTree));
    assert!(save);
    let out = dir.path().join("tree.txt");
    answer(&mut ctl, purpose, out.clone());
    assert!(notice(&mut ctl).starts_with("Saved the tree"));
    assert!(std::fs::read_to_string(out).unwrap().contains("HELLO.TXT"));
}

#[test]
fn prodos_lock_stages_the_access_byte_of_the_cursor_entry_once() {
    use crate::model::edit_queue::StagedEdit;
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = inspect(&fat_volume(dir.path()));
    let node = ctl
        .pane(Side::Left)
        .rows()
        .iter()
        .find(|r| r.name == "HELLO.TXT")
        .unwrap()
        .node_id
        .clone();
    pane(&mut ctl, A::CursorTo(node));
    pane(&mut ctl, A::SetProdosLock(true));
    pane(&mut ctl, A::SetProdosLock(false));
    let q = &ctl.pane(Side::Left).queue;
    assert_eq!(q.len(), 1);
    let staged = q.iter().next().unwrap();
    assert!(
        matches!(staged, StagedEdit::SetProdosAccess { access: 0xC3, .. }),
        "{staged:?}"
    );
}

#[test]
fn the_archive_windows_tick_entries_and_pick_the_fork_format_through_their_actions() {
    use crate::controller::commander::{Part, Press, WindowKind};
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = archive_volume(dir.path());
    let read = A::ReadArchive {
        path: "/ARC.SIT".into(),
        purpose: ArchiveUse::Browse,
    };
    pane(&mut ctl, read);
    settle(&mut ctl);
    let viewer = |ctl: &CommanderController| {
        let w = ctl.pane_windows(Side::Left, false);
        w.into_iter()
            .find(|w| w.kind == WindowKind::ArchiveViewer)
            .unwrap()
    };
    let w = viewer(&ctl);
    let Some(Part::Checklist { ticked, send, .. }) = w.parts.get(1) else {
        panic!("the entries are a tick-box list: {:?}", w.parts);
    };
    let file = ticked.iter().position(|t| t.is_some()).unwrap();
    let was = ticked[file].unwrap();
    for a in send[file].clone() {
        ctl.apply(a);
    }
    let w = viewer(&ctl);
    let Some(Part::Checklist { ticked, .. }) = w.parts.get(1) else {
        panic!("{:?}", w.parts);
    };
    // Tick it back, so at least one entry is marked for Extract Selected.
    let send_again = |w: &crate::controller::commander::PaneWindow, i: usize| match w.parts.get(1) {
        Some(Part::Checklist { send, .. }) => send[i].clone(),
        _ => Vec::new(),
    };
    assert_eq!(ticked[file], Some(!was));
    for a in send_again(&w, file) {
        ctl.apply(a);
    }
    let w = viewer(&ctl);
    let extract = w
        .buttons
        .iter()
        .find(|b| b.label.starts_with("Extract Selected"))
        .unwrap();
    assert!(extract.enabled, "{}", extract.label);
    assert!(matches!(extract.press, Press::Send(_)));
    let Some(Part::Select { options, send, .. }) = w.parts.get(2) else {
        panic!("the fork format is a drop-down: {:?}", w.parts);
    };
    let raw = options
        .iter()
        .position(|o| o == ForkFormat::Raw.label())
        .unwrap();
    for a in send[raw].clone() {
        ctl.apply(a);
    }
    assert_eq!(
        ctl.pane(Side::Left).archive_viewer().unwrap().format,
        ForkFormat::Raw
    );
}
