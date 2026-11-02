use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::*;
use crate::controller::optical::browse::{purpose as bpurpose, FileContent, FileEntry};
use crate::model::job::NoNotify;

fn controller() -> OpticalController {
    OpticalController::new(Arc::new(NoNotify))
}

/// The committed hybrid ISO 9660 + HFS+ disc (`tests/fixtures/optical/hybrid_rsrc.iso.zst`).
#[cfg(feature = "native-zstd")]
fn hybrid_disc(dir: &Path) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let packed = std::fs::read(root.join("tests/fixtures/optical/hybrid_rsrc.iso.zst")).unwrap();
    let iso = zstd::stream::decode_all(std::io::Cursor::new(packed)).unwrap();
    let path = dir.join("disc.iso");
    std::fs::write(&path, iso).unwrap();
    path
}

fn child(ctl: &OpticalController, folder: &str, name: &str) -> FileEntry {
    ctl.browse()
        .children(folder)
        .unwrap_or_else(|| panic!("{folder} not listed"))
        .iter()
        .find(|e| e.name == name)
        .unwrap_or_else(|| panic!("{name} not in {folder}"))
        .clone()
}

#[test]
fn start_waits_for_a_source_and_an_output() {
    let mut ctl = controller();
    assert_eq!(ctl.task(), Task::Convert);
    assert!(!ctl.can_start());
    ctl.apply(OpticalAction::SelectDrive(Some(3)));
    assert_eq!(ctl.selected_drive(), None, "no such drive");
    ctl.apply(OpticalAction::Start(None));
    assert!(!ctl.is_running());
    ctl.apply(OpticalAction::SetSourceMode(SourceMode::PhysicalDrive));
    assert_eq!(ctl.task(), Task::Rip);
    assert!(ctl.format_choices().iter().all(|&(_, on)| on));
    let fx = ctl.apply(OpticalAction::PickOutput);
    assert!(matches!(&fx[..], [Effect::PickFile(s)] if s.save && s.file_name == "disc.iso"));
}

#[cfg(feature = "native-zstd")]
#[test]
fn an_iso_converts_to_bin_cue_with_the_engines_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let iso = hybrid_disc(dir.path());
    let mut ctl = controller();
    ctl.apply(OpticalAction::OpenImage(iso.clone()));
    assert_eq!(
        ctl.take_requests(),
        vec![HostRequest::RememberRecent(iso.clone())]
    );
    ctl.wait_idle();
    let disc = ctl.disc().expect("detected");
    assert_eq!(disc.format, "ISO image", "{disc:?}");
    assert!(!ctl.format_ok(OutputFormat::Iso), "never to its own format");
    ctl.apply(OpticalAction::SetOutputFormat(OutputFormat::Iso));
    assert_eq!(ctl.output_format(), OutputFormat::Iso, "the default stays");
    ctl.apply(OpticalAction::SetOutputFormat(OutputFormat::BinCue));
    let fx = ctl.apply(OpticalAction::PickOutput);
    assert!(matches!(&fx[..], [Effect::PickFile(s)] if s.file_name == "disc.cue"));
    let cue = dir.path().join("out.cue");
    ctl.apply(OpticalAction::Picked {
        purpose: purpose::OUTPUT,
        path: Some(cue.clone()),
    });
    assert!(ctl.can_start());
    ctl.apply(OpticalAction::Start(None));
    assert!(ctl.is_running());
    ctl.wait_idle();
    assert_eq!(ctl.last_result(), Some(&Ok(())));
    let log: Vec<String> = ctl.take_log().into_iter().map(|(_, m)| m).collect();
    assert!(log.iter().any(|l| l.starts_with("Starting conversion: ")));
    assert!(log.iter().any(|l| l == "Conversion completed successfully"));

    let engine_cue = dir.path().join("engine.cue");
    crate::optical::convert::iso_to_bincue(
        iso.as_path(),
        engine_cue.with_extension("bin").as_path(),
        engine_cue.as_path(),
        Arc::new(std::sync::Mutex::new(
            crate::optical::convert::ConvertProgress::new(),
        )),
    )
    .unwrap();
    let ours = std::fs::read(cue.with_extension("bin")).unwrap();
    assert_eq!(
        ours,
        std::fs::read(engine_cue.with_extension("bin")).unwrap()
    );
    assert_eq!(ours.len() as u64 % 2352, 0);
}

#[cfg(feature = "native-zstd")]
#[test]
fn the_browser_reads_the_hfs_side_and_extracts_a_forked_file() {
    let dir = tempfile::tempdir().unwrap();
    let iso = hybrid_disc(dir.path());
    let mut ctl = controller();
    ctl.apply(OpticalAction::OpenImage(iso));
    ctl.wait_idle();
    ctl.apply(OpticalAction::BrowseContents);
    assert!(ctl.browse().is_active());
    ctl.wait_idle();
    assert_eq!(ctl.browse().error(), None);
    let hfs = ctl
        .browse()
        .fs_choices()
        .iter()
        .position(|l| l.contains("HFS"))
        .expect("an HFS side");
    ctl.apply(OpticalAction::Browse(DiscBrowseAction::SelectFs(hfs)));
    ctl.wait_idle();
    assert!(ctl.browse().is_hfs());
    let root = ctl.browse().root().unwrap().path.clone();
    let apps = child(&ctl, root.as_str(), "Apps");
    ctl.apply(OpticalAction::Browse(DiscBrowseAction::Expand(
        apps.clone(),
    )));
    ctl.wait_idle();
    let app = child(&ctl, apps.path.as_str(), "GameApp");
    ctl.apply(OpticalAction::Browse(DiscBrowseAction::Select(app.clone())));
    ctl.wait_idle();
    assert_eq!(
        ctl.browse().content(),
        Some(&FileContent::Text("data fork bytes".into()))
    );

    let fx = ctl.apply(OpticalAction::Browse(DiscBrowseAction::Extract));
    assert!(matches!(&fx[..], [Effect::PickFolder(s)] if s.purpose == bpurpose::EXTRACT_TO));
    let out = dir.path().join("out");
    std::fs::create_dir(&out).unwrap();
    ctl.apply(OpticalAction::Picked {
        purpose: bpurpose::EXTRACT_TO,
        path: Some(out.clone()),
    });
    ctl.wait_idle();
    assert_eq!(
        ctl.browse().result(),
        Some("Extraction complete: 1 files extracted.")
    );
    assert_eq!(
        std::fs::read(out.join("GameApp")).unwrap(),
        b"data fork bytes"
    );
    let ad = std::fs::read(out.join("._GameApp")).unwrap();
    assert!(ad.len() > 5000, "AppleDouble carries the 5000-byte fork");

    ctl.apply(OpticalAction::Browse(DiscBrowseAction::Tree));
    ctl.wait_idle();
    assert!(ctl.browse().tree_text().unwrap().contains("GameApp"));
    let fx = ctl.apply(OpticalAction::Browse(DiscBrowseAction::SaveTree));
    assert!(matches!(&fx[..], [Effect::PickFile(s)] if s.file_name == "tree.txt"));
    let tree = dir.path().join("tree.txt");
    ctl.apply(OpticalAction::Picked {
        purpose: bpurpose::SAVE_TREE,
        path: Some(tree.clone()),
    });
    ctl.wait_idle();
    assert!(std::fs::read_to_string(tree).unwrap().contains("GameApp"));

    let fx = ctl.apply(OpticalAction::Browse(DiscBrowseAction::Export));
    assert!(fx.is_empty());
    assert_eq!(ctl.browse().error(), Some("Nothing selected to export."));
    ctl.apply(OpticalAction::Browse(DiscBrowseAction::SetMarked {
        entry: app,
        marked: true,
    }));
    ctl.apply(OpticalAction::Browse(DiscBrowseAction::SetExportFormat(
        browse::ExportFormat::LooseFiles,
    )));
    let fx = ctl.apply(OpticalAction::Browse(DiscBrowseAction::Export));
    assert!(matches!(&fx[..], [Effect::PickFolder(s)] if s.purpose == bpurpose::EXPORT_FOLDER));
    let exported = dir.path().join("exported");
    std::fs::create_dir(&exported).unwrap();
    ctl.apply(OpticalAction::Picked {
        purpose: bpurpose::EXPORT_FOLDER,
        path: Some(exported.clone()),
    });
    ctl.wait_idle();
    assert!(
        ctl.browse()
            .result()
            .is_some_and(|r| r.starts_with("Exported 1 item(s) to ")),
        "{:?} {:?}",
        ctl.browse().result(),
        ctl.browse().error()
    );

    ctl.apply(OpticalAction::CloseDisc);
    assert!(!ctl.browse().is_active() && ctl.disc().is_none() && ctl.image().is_none());
}

/// An NKit image that cannot be reconstructed shows the convert-it-back hint, not a bare reader error.
#[test]
fn an_unreconstructable_nkit_iso_shows_the_actionable_hint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("game.nkit.iso");
    let mut buf = vec![0u8; 4 * 1024 * 1024];
    buf[0x1C..0x20].copy_from_slice(&[0xC2, 0x33, 0x9F, 0x3D]);
    buf[0x200..0x204].copy_from_slice(b"NKIT");
    std::fs::write(&path, &buf).unwrap();
    let mut ctl = controller();
    ctl.browse.open(crate::model::optical_browse::DiscSource {
        path,
        physical: false,
    });
    ctl.wait_idle();
    let err = ctl.browse().error().expect("an error");
    assert!(
        err.contains("NKit v1"),
        "expected the NKit hint, got: {err}"
    );
    assert!(err.contains("Convert it back"), "{err}");
}
