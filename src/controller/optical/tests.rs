use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::*;
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
fn browse_contents_asks_the_shell_for_the_disc_browser_and_a_new_disc_reopens_it() {
    let dir = tempfile::tempdir().unwrap();
    let iso = hybrid_disc(dir.path());
    let mut ctl = controller();
    ctl.apply(OpticalAction::BrowseContents);
    assert!(!ctl.is_browsing(), "no disc yet");
    ctl.apply(OpticalAction::OpenImage(iso.clone()));
    ctl.wait_idle();
    ctl.take_requests();
    ctl.apply(OpticalAction::BrowseContents);
    let src = DiscSource {
        path: iso.clone(),
        physical: false,
    };
    assert_eq!(
        ctl.take_requests(),
        vec![HostRequest::OpenBrowser(src.clone())]
    );
    assert!(ctl.is_browsing());
    ctl.apply(OpticalAction::BrowseContents);
    assert!(ctl.take_requests().is_empty(), "already open");

    let other = dir.path().join("copy.iso");
    std::fs::copy(&iso, &other).unwrap();
    ctl.apply(OpticalAction::OpenImage(other.clone()));
    assert_eq!(
        ctl.take_requests(),
        vec![
            HostRequest::RememberRecent(other.clone()),
            HostRequest::CloseBrowser
        ]
    );
    ctl.wait_idle();
    let reopened = DiscSource {
        path: other,
        physical: false,
    };
    assert_eq!(
        ctl.take_requests(),
        vec![HostRequest::OpenBrowser(reopened)]
    );

    ctl.apply(OpticalAction::BrowserClosed);
    assert!(!ctl.is_browsing());
    ctl.apply(OpticalAction::BrowseContents);
    ctl.apply(OpticalAction::CloseDisc);
    let reqs = ctl.take_requests();
    assert_eq!(reqs.last(), Some(&HostRequest::CloseBrowser));
    assert!(!ctl.is_browsing() && ctl.disc().is_none() && ctl.image().is_none());
}

/// The disc browser a shell opens on `HostRequest::OpenBrowser`, settled.
fn browser(src: DiscSource) -> crate::controller::commander::CommanderController {
    let services = crate::controller::browser::PaneServices::headless();
    let keys = crate::controller::keys::KeyMap::for_this_platform();
    let mut b = disc_browser(services, keys);
    b.open_disc(src);
    settle(&mut b);
    b
}

fn settle(b: &mut crate::controller::commander::CommanderController) -> Vec<Effect> {
    let start = std::time::Instant::now();
    let mut fx = b.tick();
    while b.is_working() {
        assert!(start.elapsed().as_secs() < 60, "a job never ended");
        std::thread::sleep(std::time::Duration::from_millis(5));
        fx.extend(b.tick());
    }
    fx
}

#[cfg(feature = "native-zstd")]
#[test]
fn the_disc_browser_reads_the_hfs_side_and_extracts_a_forked_file() {
    use crate::controller::browser::{purpose as ppurpose, BrowserPaneAction as P};
    use crate::controller::commander::screen::purpose as spurpose;
    use crate::controller::commander::{CommanderAction as C, ExportFormat, ExtractOptions, Side};
    let dir = tempfile::tempdir().unwrap();
    let iso = hybrid_disc(dir.path());
    let mut b = browser(DiscSource {
        path: iso,
        physical: false,
    });
    let pane = b.pane(Side::Left);
    assert_eq!(pane.source_error(), None);
    assert!(pane.is_optical() && pane.profile().read_only);
    let hfs = pane
        .partition_labels()
        .iter()
        .position(|l| l.contains("HFS"));
    b.apply(C::Pane(
        Side::Left,
        P::SelectPartition(hfs.expect("an HFS side")),
    ));
    settle(&mut b);
    assert_eq!(b.pane(Side::Left).volume().fs_type, "HFS+");
    let node = |b: &crate::controller::commander::CommanderController, path: &str| {
        let p = b.pane(Side::Left);
        let row = p
            .rows()
            .iter()
            .find(|r| p.row_entry(r).is_some_and(|e| e.path == path));
        row.map(|r| r.node_id.clone()).expect(path)
    };
    let apps = node(&b, "/Apps");
    b.apply(C::Pane(Side::Left, P::ToggleExpand(apps)));
    settle(&mut b);
    let app = node(&b, "/Apps/GameApp");
    b.apply(C::Pane(Side::Left, P::CursorTo(app.clone())));
    settle(&mut b);
    let detail = b.side_detail(Side::Left).expect("the side panel");
    assert_eq!(detail.preview.as_deref(), Some(&b"data fork bytes"[..]));

    let fx = b.apply(C::Extract {
        from: Side::Left,
        path: "/Apps/GameApp".into(),
        options: ExtractOptions::default(),
        dest: None,
    });
    assert!(matches!(&fx[..], [Effect::PickFolder(s)] if s.purpose == spurpose::EXTRACT));
    let out = dir.path().join("out");
    std::fs::create_dir(&out).unwrap();
    b.apply(C::Picked {
        purpose: spurpose::EXTRACT,
        path: Some(out.clone()),
    });
    settle(&mut b);
    assert!(b.status().starts_with("Copied 1 file(s)"), "{}", b.status());
    let data = std::fs::read(out.join("GameApp")).unwrap();
    assert_eq!(data, b"data fork bytes");
    let ad = std::fs::read(out.join("._GameApp")).unwrap();
    assert!(ad.len() > 5000, "AppleDouble carries the 5000-byte fork");

    b.apply(C::Pane(Side::Left, P::ListTree { ids: false }));
    settle(&mut b);
    let tree = b.host_pane().take_tree_text().expect("a tree");
    assert!(tree.text.contains("GameApp") && tree.text.contains("APPL/PLAY"));
    let fx = b.apply(C::Pane(Side::Left, P::SaveTree));
    assert!(matches!(&fx[..], [Effect::PickFile(s)] if s.purpose == ppurpose::TREE_SAVE));
    let saved = dir.path().join("tree.txt");
    b.apply(C::Pane(
        Side::Left,
        P::Picked {
            purpose: ppurpose::TREE_SAVE,
            path: Some(saved.clone()),
        },
    ));
    settle(&mut b);
    assert_eq!(std::fs::read_to_string(saved).unwrap(), tree.text);

    let fx = b.apply(C::Export {
        from: Side::Left,
        format: Some(ExportFormat::LooseFiles),
    });
    assert!(fx.is_empty(), "nothing selected");
    b.apply(C::Pane(Side::Left, P::Mark(app)));
    let fx = b.apply(C::Export {
        from: Side::Left,
        format: Some(ExportFormat::LooseFiles),
    });
    assert!(matches!(&fx[..], [Effect::PickFolder(s)] if s.purpose == spurpose::EXPORT));
    let exported = dir.path().join("exported");
    std::fs::create_dir(&exported).unwrap();
    b.apply(C::Picked {
        purpose: spurpose::EXPORT,
        path: Some(exported.clone()),
    });
    settle(&mut b);
    assert!(b.status().starts_with("Copied 1 file(s)"), "{}", b.status());
    assert!(exported.join("GameApp").exists() && exported.join("._GameApp").exists());

    b.apply(C::Pane(Side::Left, P::Delete));
    assert!(b.status().contains("not available"), "{}", b.status());
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
    let b = browser(DiscSource {
        path,
        physical: false,
    });
    let pane = b.pane(crate::controller::commander::Side::Left);
    let err = pane.source_error().expect("an error");
    assert!(
        err.contains("NKit v1"),
        "expected the NKit hint, got: {err}"
    );
    assert!(err.contains("Convert it back"), "{err}");
}

/// A drive that cannot be opened (unelevated, or gone) shows why where the listing would be.
#[test]
fn a_drive_the_browser_cannot_open_shows_the_error() {
    let dir = tempfile::tempdir().unwrap();
    let b = browser(DiscSource {
        path: dir.path().join("no-such-drive"),
        physical: true,
    });
    let pane = b.pane(crate::controller::commander::Side::Left);
    let err = pane.source_error().expect("an error");
    assert!(
        err.starts_with("Failed to open disc image") || err.contains("compiled without"),
        "{err}"
    );
    assert!(!pane.listing.is_loaded());
}

#[cfg(feature = "native-zstd")]
#[test]
fn start_names_what_it_waits_for_and_refuses_the_images_own_format() {
    let dir = tempfile::tempdir().unwrap();
    let iso = hybrid_disc(dir.path());
    let mut ctl = controller();
    assert_eq!(ctl.start_label(), "Start Convert");
    assert_eq!(ctl.start_refusal().as_deref(), Some("Choose a disc image."));
    ctl.apply(OpticalAction::SetSourceMode(SourceMode::PhysicalDrive));
    assert_eq!(ctl.start_label(), "Start Rip");
    assert_eq!(ctl.start_refusal().as_deref(), Some("Select a drive."));
    ctl.apply(OpticalAction::OpenImage(iso));
    assert_eq!(ctl.source_mode(), SourceMode::ImageFile);
    assert_eq!(
        ctl.start_refusal().as_deref(),
        Some("Detecting the disc...")
    );
    ctl.wait_idle();
    let own = "The image is already ISO; choose another output format.";
    assert_eq!(ctl.start_refusal().as_deref(), Some(own));
    assert_eq!(ctl.format_refusal(OutputFormat::Iso).as_deref(), Some(own));
    assert_eq!(ctl.format_refusal(OutputFormat::BinCue), None);
    ctl.set_output_path(dir.path().join("out.iso"));
    ctl.take_log();
    ctl.apply(OpticalAction::Start(None));
    assert!(!ctl.is_running(), "an ISO is not converted to ISO");
    let log: Vec<String> = ctl.take_log().into_iter().map(|(_, m)| m).collect();
    assert_eq!(log, [format!("Cannot start: {own}")]);
    ctl.apply(OpticalAction::SetOutputFormat(OutputFormat::BinCue));
    assert!(ctl.can_start(), "{:?}", ctl.start_refusal());
    let chd = ctl.format_refusal(OutputFormat::Chd);
    if cfg!(feature = "chd") {
        assert_eq!(chd, None);
    } else {
        assert_eq!(chd.as_deref(), Some(CHD_NOT_BUILT));
    }
    assert!(!ctl.can_chd_info() && !ctl.is_chd_image());
    assert_eq!(
        OpticalController::remote_available(),
        cfg!(feature = "remote")
    );
}

#[test]
fn an_image_that_does_not_open_is_not_converted() {
    let dir = tempfile::tempdir().unwrap();
    let mut ctl = controller();
    ctl.apply(OpticalAction::OpenImage(dir.path().join("missing.iso")));
    ctl.wait_idle();
    assert!(ctl.disc_error().is_some());
    ctl.set_output_path(dir.path().join("out.cue"));
    assert_eq!(
        ctl.start_refusal().as_deref(),
        Some("The image was not recognised as a disc.")
    );
}
