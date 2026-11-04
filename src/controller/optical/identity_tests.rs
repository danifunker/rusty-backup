//! What the disc browser writes, pinned: extraction (every fork mode), the export of the
//! marked entries (every format), the tree listing and the preview, on every filesystem
//! choice of five discs. `GOLDEN` was captured from the Optical tab's own browser
//! (`DiscBrowseController`, slice 16, fd5944fd) before slice 18 moved the tab onto the browser
//! pane; the pane must still produce it. Slice 19 recaptured the 50 extract cases its byte
//! rules change (docs/phase2_commander_plan.md § "Slice 19").
//!
//! Discs: `hybrid_rsrc.iso` (ISO 9660 + HFS+, a forked file), the same disc as BIN/CUE and as
//! CHD, `rock_ridge.iso` and that disc with a PReP boot partition patched into sector 0.
//! `tests/fixtures/optical/rock_ridge.iso.zst` was built with xorriso 1.5 (Rock Ridge + Joliet):
//!   docs/readme.txt, docs/MixedCase.TXT, bin/data.bin (3000 bytes), bin/run.sh (0755),
//!   link -> docs/readme.txt, empty.dat, deep/a/b/c/file.txt, "name with spaces.txt",
//!   "colon:name.txt" and "colon_name.txt" (one host name), a 63-character name;
//!   every date 2001-02-03 04:05:06 UTC, `xorriso -as mkisofs -R -J -V RRDISC`.
//!
//! Set `RB_IDENTITY_PRINT=1` to print the table instead of checking it.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use sha2::{Digest, Sha256};

use crate::controller::Controller;
use crate::fs::export_selection::ExportFormat;
use fs_mac::resource_fork::ResourceForkMode;

/// Files larger than this were not read for the old browser's preview.
const MAX_PREVIEW_SIZE: u64 = 1024 * 1024;

/// The old preview's classification: text when the bytes read as text, else binary.
enum FileContent {
    Binary(Vec<u8>),
    Text(String),
}

fn detect_content_type(data: &[u8]) -> FileContent {
    if data.is_empty() {
        return FileContent::Text(String::new());
    }
    if let Ok(text) = std::str::from_utf8(data) {
        let odd = text
            .chars()
            .filter(|c| !c.is_ascii_graphic() && !c.is_ascii_whitespace())
            .count();
        if odd * 10 < text.len() {
            return FileContent::Text(text.to_string());
        }
    }
    let printable = data
        .iter()
        .filter(|&&b| b.is_ascii_graphic() || b.is_ascii_whitespace())
        .count();
    if printable * 10 >= data.len() * 8 {
        let text = data
            .iter()
            .map(|&b| match b.is_ascii_graphic() || b.is_ascii_whitespace() {
                true => b as char,
                false => '.',
            })
            .collect();
        return FileContent::Text(text);
    }
    FileContent::Binary(data.to_vec())
}

fn unpack(name: &str, dir: &Path, out: &str) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let packed = std::fs::read(root.join("tests/fixtures/optical").join(name)).unwrap();
    let iso = zstd::stream::decode_all(std::io::Cursor::new(packed)).unwrap();
    let path = dir.join(out);
    std::fs::write(&path, iso).unwrap();
    path
}

fn convert(iso: &Path, out: &Path, format: crate::model::optical_ops::OutputFormat) {
    let progress = Arc::new(Mutex::new(
        crate::optical::convert::ConvertProgress::default(),
    ));
    let source = Some(opticaldiscs::formats::DiscFormat::Iso);
    crate::model::optical_ops::run_conversion(iso, out, source, format, None, progress).unwrap();
}

/// The discs, by case name.
pub(super) fn discs(dir: &Path) -> Vec<(&'static str, PathBuf)> {
    use crate::model::optical_ops::OutputFormat;
    let hybrid = unpack("hybrid_rsrc.iso.zst", dir, "hybrid.iso");
    let rr = unpack("rock_ridge.iso.zst", dir, "rr.iso");
    let prep = dir.join("prep.iso");
    let mut bytes = std::fs::read(&rr).unwrap();
    bytes[510] = 0x55;
    bytes[511] = 0xAA;
    let e = &mut bytes[0x1BE..0x1CE];
    e[4] = 0x41;
    e[8..12].copy_from_slice(&4u32.to_le_bytes());
    e[12..16].copy_from_slice(&6u32.to_le_bytes());
    for (i, b) in bytes[4 * 512..10 * 512].iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    std::fs::write(&prep, bytes).unwrap();
    let cue = dir.join("hybrid_cue.cue");
    convert(&hybrid, &cue, OutputFormat::BinCue);
    #[allow(unused_mut)]
    let mut out = vec![
        ("hybrid", hybrid.clone()),
        ("hybrid-cue", cue),
        ("rr", rr),
        ("rr-prep", prep),
    ];
    #[cfg(feature = "chd")]
    {
        let chd = dir.join("hybrid.chd");
        convert(&hybrid, &chd, OutputFormat::Chd);
        out.push(("hybrid-chd", chd));
    }
    out
}

/// A host tree as lines: `d path`, `f path sha256`, `l path -> target`, sorted.
pub(super) fn manifest(root: &Path) -> String {
    fn walk(base: &Path, dir: &Path, out: &mut Vec<String>) {
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        for e in rd.flatten() {
            let p = e.path();
            let rel = p.strip_prefix(base).unwrap().display().to_string();
            let meta = std::fs::symlink_metadata(&p).unwrap();
            if meta.file_type().is_symlink() {
                let t = std::fs::read_link(&p).unwrap();
                out.push(format!("l {rel} -> {}", t.display()));
            } else if meta.is_dir() {
                out.push(format!("d {rel}"));
                walk(base, &p, out);
            } else {
                out.push(format!("f {rel} {}", sha(&std::fs::read(&p).unwrap())));
            }
        }
    }
    let mut lines = Vec::new();
    walk(root, root, &mut lines);
    lines.sort();
    lines.join("\n")
}

pub(super) fn sha(data: &[u8]) -> String {
    let d = Sha256::digest(data);
    d.iter().map(|b| format!("{b:02x}")).collect()
}

/// The old preview's rendering of `data`, so a raw read compares with it.
fn preview_text(c: &FileContent) -> String {
    match c {
        FileContent::Text(t) => format!("text {}", sha(t.as_bytes())),
        FileContent::Binary(d) => format!("binary {}", sha(d.as_slice())),
    }
}

/// One way of driving a disc browser, so the same cases run against each.
pub(super) trait DiscBrowser {
    fn open(&mut self, disc: &Path);
    fn choices(&self) -> Vec<String>;
    fn select_fs(&mut self, i: usize);
    /// Children of a listed folder: (path, is_dir).
    fn children(&mut self, folder: &str) -> Vec<(String, bool)>;
    fn tree(&mut self) -> String;
    /// Extract `path` into `dest`; `Err` when it failed.
    fn extract(&mut self, path: &str, mode: ResourceForkMode, dest: &Path) -> Result<(), ()>;
    /// Export the root's children (all marked) into `dest`.
    fn export_all(
        &mut self,
        format: ExportFormat,
        mode: ResourceForkMode,
        dest: &Path,
    ) -> Result<(), ()>;
    /// The preview of a file, as `preview_text` renders it.
    fn preview(&mut self, path: &str) -> String;
}

/// Every case of one browser, as `case<TAB>digest` lines.
pub(super) fn run_cases(b: &mut dyn DiscBrowser) -> Vec<(String, String)> {
    let scratch = tempfile::tempdir().unwrap();
    let mut out = Vec::new();
    let mut n = 0;
    let mut fresh = || {
        n += 1;
        let d = scratch.path().join(format!("o{n}"));
        std::fs::create_dir_all(&d).unwrap();
        d
    };
    let modes = ResourceForkMode::ALL;
    for (disc, path) in discs(scratch.path()) {
        b.open(path.as_path());
        let choices = b.choices();
        out.push((
            format!("{disc} choices"),
            sha(choices.len().to_string().as_bytes()),
        ));
        for i in 0..choices.len() {
            b.select_fs(i);
            let case = |what: &str| format!("{disc} fs{i} {what}");
            out.push((case("tree"), sha(b.tree().as_bytes())));
            for mode in modes {
                let d = fresh();
                let r = b.extract("/", mode, d.as_path());
                let digest = match r {
                    Ok(()) => sha(manifest(d.as_path()).as_bytes()),
                    Err(()) => "ERR".to_string(),
                };
                out.push((case(&format!("extract / {mode:?}")), digest));
            }
            let top = b.children("/");
            let mut files = Vec::new();
            for (p, dir) in &top {
                let d = fresh();
                let digest = match b.extract(p.as_str(), ResourceForkMode::AppleDouble, &d) {
                    Ok(()) => sha(manifest(d.as_path()).as_bytes()),
                    Err(()) => "ERR".to_string(),
                };
                out.push((case(&format!("extract {p}")), digest));
                if *dir {
                    let kids = b.children(p.as_str());
                    files.extend(kids.into_iter().filter(|(_, d)| !d).map(|(p, _)| p));
                } else {
                    files.push(p.clone());
                }
            }
            for f in &files {
                out.push((case(&format!("preview {f}")), b.preview(f.as_str())));
            }
            for format in ExportFormat::ALL {
                let d = fresh();
                let dest = match format.file_extension() {
                    Some(ext) if format.is_single_file() => d.join(format!("out.{ext}")),
                    _ => d.clone(),
                };
                let r = b.export_all(format, ResourceForkMode::AppleDouble, &dest);
                let digest = match r {
                    Ok(()) => sha(manifest(d.as_path()).as_bytes()),
                    Err(()) => "ERR".to_string(),
                };
                out.push((case(&format!("export {format:?}")), digest));
            }
            for mode in modes {
                let d = fresh();
                let r = b.export_all(ExportFormat::LooseFiles, mode, &d);
                let digest = match r {
                    Ok(()) => sha(manifest(d.as_path()).as_bytes()),
                    Err(()) => "ERR".to_string(),
                };
                out.push((case(&format!("export loose {mode:?}")), digest));
            }
        }
    }
    out
}

/// What the pane shows differently, never what it writes: a Rock Ridge symlink has no preview.
const PANE_DIFFERS: &[(&str, &str)] = &[
    ("rr fs0 preview /link", "none"),
    ("rr-prep fs0 preview /link", "none"),
];

/// Check `got` against `GOLDEN`, `differs` excepted (or print it with `RB_IDENTITY_PRINT=1`).
pub(super) fn check(got: &[(String, String)], differs: &[(&str, &str)]) {
    if std::env::var("RB_IDENTITY_PRINT").is_ok() {
        for (c, d) in got {
            println!("    (\"{c}\", \"{d}\"),");
        }
        return;
    }
    let mut bad = Vec::new();
    for (c, d) in got {
        if differs.iter().any(|(g, v)| g == c && v == d) {
            continue;
        }
        match GOLDEN.iter().find(|(g, _)| g == c) {
            Some((_, want)) if want == d => {}
            Some((_, want)) => bad.push(format!("{c}: {d} (want {want})")),
            None => bad.push(format!("{c}: not in GOLDEN")),
        }
    }
    let chd = cfg!(feature = "chd");
    for (g, _) in GOLDEN {
        if (chd || !g.starts_with("hybrid-chd")) && !got.iter().any(|(c, _)| c == g) {
            bad.push(format!("{g}: case not run"));
        }
    }
    assert!(
        bad.is_empty(),
        "{} case(s) differ:\n{}",
        bad.len(),
        bad.join("\n")
    );
}

/// The browser pane under `PaneProfile::optical`, as the Optical tab hosts it since slice 18.
struct PaneBrowser {
    ctl: Option<crate::controller::commander::CommanderController>,
}

impl PaneBrowser {
    fn ctl(&mut self) -> &mut crate::controller::commander::CommanderController {
        self.ctl.as_mut().expect("open first")
    }

    /// Run jobs to the end; a failed entry is answered Stop, as the disc browser always stopped.
    fn settle(&mut self) {
        use crate::controller::commander::screen::purpose;
        use crate::controller::commander::CommanderAction;
        use crate::controller::Effect;
        let ctl = self.ctl();
        let start = std::time::Instant::now();
        let mut fx = ctl.tick();
        loop {
            for e in fx {
                if let Effect::Choose(spec) = e {
                    assert_eq!(spec.purpose, purpose::ENTRY_FAILED);
                    ctl.apply(CommanderAction::Chose {
                        purpose: spec.purpose,
                        choice: spec.dismiss,
                    });
                }
            }
            if !ctl.is_working() {
                break;
            }
            assert!(start.elapsed().as_secs() < 60, "a job never ended");
            std::thread::sleep(std::time::Duration::from_millis(5));
            fx = ctl.tick();
        }
    }

    fn pane_act(&mut self, a: crate::controller::browser::BrowserPaneAction) {
        use crate::controller::commander::{CommanderAction, Side};
        self.ctl().apply(CommanderAction::Pane(Side::Left, a));
        self.settle();
    }

    fn pane(&self) -> &crate::controller::browser::BrowserPaneController {
        let ctl = self.ctl.as_ref().expect("open first");
        ctl.pane(crate::controller::commander::Side::Left)
    }

    /// The node id of the row showing `path`.
    fn node(&self, path: &str) -> Option<(String, bool)> {
        let p = self.pane();
        let rows = p.rows();
        let row = rows
            .iter()
            .find(|r| !r.is_parent() && p.row_entry(r).is_some_and(|e| e.path == path))?;
        Some((row.node_id.clone(), row.expanded))
    }

    fn copied(&self) -> Result<(), ()> {
        let ctl = self.ctl.as_ref().expect("open first");
        match ctl.status().starts_with("Copied ") {
            true => Ok(()),
            false => Err(()),
        }
    }
}

impl DiscBrowser for PaneBrowser {
    fn open(&mut self, disc: &Path) {
        use crate::controller::browser::{DiscSource, PaneProfile, PaneServices};
        let mut ctl = crate::controller::commander::CommanderController::single(
            PaneServices::headless(),
            crate::controller::keys::KeyMap::for_this_platform(),
            PaneProfile::optical(),
        );
        ctl.open_disc(DiscSource {
            path: disc.to_path_buf(),
            physical: false,
        });
        self.ctl = Some(ctl);
        self.settle();
    }

    fn choices(&self) -> Vec<String> {
        self.pane().partition_labels()
    }

    fn select_fs(&mut self, i: usize) {
        use crate::controller::browser::BrowserPaneAction as A;
        self.pane_act(A::SelectPartition(i));
    }

    fn children(&mut self, folder: &str) -> Vec<(String, bool)> {
        use crate::controller::browser::BrowserPaneAction as A;
        if folder != "/" {
            if let Some((id, false)) = self.node(folder) {
                self.pane_act(A::ToggleExpand(id));
            }
        }
        let kids = self.pane().folder_children(folder).unwrap_or(&[]);
        kids.iter()
            .map(|k| (k.path.clone(), k.is_directory()))
            .collect()
    }

    fn tree(&mut self) -> String {
        use crate::controller::browser::BrowserPaneAction as A;
        self.pane_act(A::ListTree { ids: false });
        let t = self.ctl().host_pane().take_tree_text();
        t.map(|t| t.text).unwrap_or_default()
    }

    fn extract(&mut self, path: &str, mode: ResourceForkMode, dest: &Path) -> Result<(), ()> {
        use crate::controller::commander::{CommanderAction, ExtractOptions, Side};
        let options = ExtractOptions {
            fork_mode: mode,
            ..ExtractOptions::default()
        };
        self.ctl().apply(CommanderAction::Extract {
            from: Side::Left,
            path: path.to_string(),
            options,
            dest: Some(dest.to_path_buf()),
        });
        self.settle();
        self.copied()
    }

    fn export_all(
        &mut self,
        format: ExportFormat,
        mode: ResourceForkMode,
        dest: &Path,
    ) -> Result<(), ()> {
        use crate::controller::browser::BrowserPaneAction as A;
        use crate::controller::commander::screen::purpose;
        use crate::controller::commander::{CommanderAction, Side};
        self.pane_act(A::ClearSelection);
        let p = self.pane();
        let top: Vec<String> = p
            .rows()
            .iter()
            .filter(|r| r.depth == 0 && !r.is_parent())
            .map(|r| r.node_id.clone())
            .collect();
        for id in top {
            self.pane_act(A::Mark(id));
        }
        self.ctl().apply(CommanderAction::SetForkMode(mode));
        let fx = self.ctl().apply(CommanderAction::Export {
            from: Side::Left,
            format: Some(format),
        });
        if fx.is_empty() {
            return Err(());
        }
        self.ctl().apply(CommanderAction::Picked {
            purpose: purpose::EXPORT,
            path: Some(dest.to_path_buf()),
        });
        self.settle();
        self.copied()
    }

    fn preview(&mut self, path: &str) -> String {
        use crate::controller::browser::BrowserPaneAction as A;
        use crate::controller::commander::Side;
        let size = self.pane().entry_at(path).map(|e| e.size).unwrap_or(0);
        if size > MAX_PREVIEW_SIZE {
            return "too large".to_string();
        }
        let Some((id, _)) = self.node(path) else {
            return "no row".to_string();
        };
        self.pane_act(A::CursorTo(id));
        let ctl = self.ctl.as_ref().expect("open first");
        match ctl.side_detail(Side::Left).and_then(|d| d.preview.as_ref()) {
            Some(data) => preview_text(&detect_content_type(data)),
            None => "none".to_string(),
        }
    }
}

#[test]
fn the_browser_pane_writes_the_disc_browsers_golden_bytes() {
    let mut b = PaneBrowser { ctl: None };
    let got = run_cases(&mut b);
    check(&got, PANE_DIFFERS);
}

/// Case -> sha256 of its manifest (or of the tree text / choice count / preview).
#[rustfmt::skip]
pub(super) const GOLDEN: &[(&str, &str)] = &[
    ("hybrid choices", "d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35"),
    ("hybrid fs0 tree", "21289477a5420702770d5f54b445f6bf0fb5874cec5560fa7cac56a19415ff3f"),
    ("hybrid fs0 extract / Native", "ERR"),
    ("hybrid fs0 extract / AppleDouble", "9aa28ebabcbb05b8ef570cf0d788344df9404947361dcd84ee5368953183d7ba"),
    ("hybrid fs0 extract / MacBinary", "e1221ac823f93257423ad5ec74ff7522a183eb58d10ea667f49f87019578a428"),
    ("hybrid fs0 extract / BinHex", "90567bedec6b30504020b64a0033f4e4c9c2b862fe5b6d5eeadc48a809b6b8bf"),
    ("hybrid fs0 extract / SeparateRsrc", "60048f8758442bfb05a98f74194acbe9b24fad4977c557364884772349dcb8e8"),
    ("hybrid fs0 extract / DataForkOnly", "e57312a6ffc110ae20b052e5c58a5a94a1a7fdc5cc8b0042e35833926327230d"),
    ("hybrid fs0 extract /APPS", "c01b2afb78257839d460dd7ff4034535183b1067feba09c0359eb8439e6a9211"),
    ("hybrid fs0 extract /README.TXT", "c704938fb712aa0fe5c9e5ce7ce878792451ab108d2cda27bc1a834e517dc373"),
    ("hybrid fs0 preview /APPS/GAMEAPP", "text 36757b5d63a1a783a5b14d998d08dbdfb43e1798751fe5908e819f3a180469ca"),
    ("hybrid fs0 preview /README.TXT", "text 08d4cc190576fe63c81d6c7a383b4081924fbe15ccb62abe6399d61220d1d78f"),
    ("hybrid fs0 export LooseFiles", "9833a604562fb2cc8ef7cda33da64d70f5ff39aab5a13c54f783be641432c544"),
    ("hybrid fs0 export GzipPerFile", "ca7fac0ea5541e9ae3b7b739e9aceef1febd66ea1f9c56067883019d0a1f1861"),
    ("hybrid fs0 export ZstdPerFile", "d4975ec5fc873a9bfc9eabd5c0d254c6c1f5351a325080f8814793a04d0326c1"),
    ("hybrid fs0 export BinHex", "2a78fc27aba2d3e826830d6056fb5ba7a0b9486fb145ad5d2cef940b41077274"),
    ("hybrid fs0 export Tar", "2409f818007cde36a4e46a1d89470511f78bc30906ce99df480ac7ac98ee83d2"),
    ("hybrid fs0 export TarGz", "c67cb1a1d0209d561083dbbadd09e7d0ec07026d3095a4934052e9ea5cb119de"),
    ("hybrid fs0 export TarZstd", "4c838978d6a5c9a42f32556e04f5df9cd2cc2dac6a18e21a10f7c31daa3491c8"),
    ("hybrid fs0 export Zip", "e6e8211a87ee869ec9f54f5e49c9e6336f8cf2d62e3f3b00b4de177539270a57"),
    ("hybrid fs0 export StuffIt", "ca9ee00c516ae336fdcdf0b9b64aea952280219d741e96d68ef2a47b9e657144"),
    ("hybrid fs0 export MacArchive", "18936bf318a4fd958fa1433708d423d8fd3ec6b1197304576301629f6306af48"),
    ("hybrid fs0 export StuffItHqx", "a21c1485f0554b306c2cc8837ac9dfdb238a04e5070cc96c93b33b87b7697ba6"),
    ("hybrid fs0 export loose Native", "ERR"),
    ("hybrid fs0 export loose AppleDouble", "9833a604562fb2cc8ef7cda33da64d70f5ff39aab5a13c54f783be641432c544"),
    ("hybrid fs0 export loose MacBinary", "fb7ef7dcbd4f849fde895355beebbd03c0bd1a1315722a93e613d83f26451be7"),
    ("hybrid fs0 export loose BinHex", "2a78fc27aba2d3e826830d6056fb5ba7a0b9486fb145ad5d2cef940b41077274"),
    ("hybrid fs0 export loose SeparateRsrc", "6697719cdb3ae82e5d9b6089659c46cd843efe1dc5f5c6833f2a6b113b52ace9"),
    ("hybrid fs0 export loose DataForkOnly", "7ed6fbcbd762f1808241f9883dea50223f58309080cbce59e6e2d132c6fdeab9"),
    ("hybrid fs1 tree", "efab43bfb45b60054dbb561f470b5e965ab18cc53e7a4ba52525f8b491534be5"),
    ("hybrid fs1 extract / Native", "ERR"),
    ("hybrid fs1 extract / AppleDouble", "f901e67fdd57871e326779e2f7d36af1fef9a3da201a2ca8279b19333dca95d5"),
    ("hybrid fs1 extract / MacBinary", "e0fd4be4204d0e85973eec8c8ec1237a07d4c33d3caf9f68b12b0a245be1e20b"),
    ("hybrid fs1 extract / BinHex", "46b6c6292d072074a93993539dfda216aca675e43d886aecb73c6e2d5616f06b"),
    ("hybrid fs1 extract / SeparateRsrc", "c6cf86953cb23eff754f0a07baa311ccc47f58731cfb430de5391f00f5f9fdeb"),
    ("hybrid fs1 extract / DataForkOnly", "d51ba424b8385722d70eb19e4588a405e8e46a2b1512c1d5b5c8154828933488"),
    ("hybrid fs1 extract /Apps", "47ad8cc73ca5e04d268d45f9207dfd18d35df3f07626d0560661e2ba7d76e7e9"),
    ("hybrid fs1 extract /README.txt", "07c3da82b9cd6f7a70e263622a08520a650f9a6f1755b03519a43bfd98a7f187"),
    ("hybrid fs1 preview /Apps/GameApp", "text 36757b5d63a1a783a5b14d998d08dbdfb43e1798751fe5908e819f3a180469ca"),
    ("hybrid fs1 preview /README.txt", "text 08d4cc190576fe63c81d6c7a383b4081924fbe15ccb62abe6399d61220d1d78f"),
    ("hybrid fs1 export LooseFiles", "2e4d7c2e70056adae78b2b5ba03f9adf8120e7fa62f48b98fb18e8b14a97f5f2"),
    ("hybrid fs1 export GzipPerFile", "b1c8a07e53a2f5629105682a10f10b7c2a5fd099584def4595d2e9109c618c30"),
    ("hybrid fs1 export ZstdPerFile", "ee5cb882e35155c4e3c114532d3d0e9643e78d9d03fdd26cb86bd9d6016a1400"),
    ("hybrid fs1 export BinHex", "315362daa05ba94019df06d7a0f64323a359e5e9cbab20587d9bbf12e6eb9a33"),
    ("hybrid fs1 export Tar", "1ed9cd71d4098138508d1813e332b91dac5a60de486ec1e49aa22ad8bcd59fcc"),
    ("hybrid fs1 export TarGz", "8907b7e6550cb7548ba2a83a8f71e4515c8519bd8d2944be217ae97a667ca893"),
    ("hybrid fs1 export TarZstd", "c1c7ef29fd7cdc0a710f42a641e3f19d64fa430cb791528fa83b952601e0330d"),
    ("hybrid fs1 export Zip", "777940d5091996c3f14f3abbc1f85fd3b12e37780cbb22cb7b5b3b2dde59390c"),
    ("hybrid fs1 export StuffIt", "3d23055963da1ecfe5e8a787b46eff04f9933c9d56f45687589502a4c7d91f68"),
    ("hybrid fs1 export MacArchive", "edd9d239e7b986a786620555564e6e5facfa84cb7eb3fd2e291fd3c30a761cad"),
    ("hybrid fs1 export StuffItHqx", "9667b7bcaff56302b8870256e97c99f2b38423a431874cc987ad41a92a8a928c"),
    ("hybrid fs1 export loose Native", "ERR"),
    ("hybrid fs1 export loose AppleDouble", "2e4d7c2e70056adae78b2b5ba03f9adf8120e7fa62f48b98fb18e8b14a97f5f2"),
    ("hybrid fs1 export loose MacBinary", "ed9a79f4b54103e78fcc62ec44ed1d80005ce4bc951346070efa7066f636d026"),
    ("hybrid fs1 export loose BinHex", "315362daa05ba94019df06d7a0f64323a359e5e9cbab20587d9bbf12e6eb9a33"),
    ("hybrid fs1 export loose SeparateRsrc", "67ae8c744e9111d7fb9e4fb20975877419dffa6da041af6e5d3f88fe0a3177ae"),
    ("hybrid fs1 export loose DataForkOnly", "b4541ad5de4fcc9325261dcb5356f0ea123aa7e495206599f14d5916150a64f9"),
    ("hybrid-cue choices", "d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35"),
    ("hybrid-cue fs0 tree", "21289477a5420702770d5f54b445f6bf0fb5874cec5560fa7cac56a19415ff3f"),
    ("hybrid-cue fs0 extract / Native", "ERR"),
    ("hybrid-cue fs0 extract / AppleDouble", "9aa28ebabcbb05b8ef570cf0d788344df9404947361dcd84ee5368953183d7ba"),
    ("hybrid-cue fs0 extract / MacBinary", "e1221ac823f93257423ad5ec74ff7522a183eb58d10ea667f49f87019578a428"),
    ("hybrid-cue fs0 extract / BinHex", "90567bedec6b30504020b64a0033f4e4c9c2b862fe5b6d5eeadc48a809b6b8bf"),
    ("hybrid-cue fs0 extract / SeparateRsrc", "60048f8758442bfb05a98f74194acbe9b24fad4977c557364884772349dcb8e8"),
    ("hybrid-cue fs0 extract / DataForkOnly", "e57312a6ffc110ae20b052e5c58a5a94a1a7fdc5cc8b0042e35833926327230d"),
    ("hybrid-cue fs0 extract /APPS", "c01b2afb78257839d460dd7ff4034535183b1067feba09c0359eb8439e6a9211"),
    ("hybrid-cue fs0 extract /README.TXT", "c704938fb712aa0fe5c9e5ce7ce878792451ab108d2cda27bc1a834e517dc373"),
    ("hybrid-cue fs0 preview /APPS/GAMEAPP", "text 36757b5d63a1a783a5b14d998d08dbdfb43e1798751fe5908e819f3a180469ca"),
    ("hybrid-cue fs0 preview /README.TXT", "text 08d4cc190576fe63c81d6c7a383b4081924fbe15ccb62abe6399d61220d1d78f"),
    ("hybrid-cue fs0 export LooseFiles", "9833a604562fb2cc8ef7cda33da64d70f5ff39aab5a13c54f783be641432c544"),
    ("hybrid-cue fs0 export GzipPerFile", "ca7fac0ea5541e9ae3b7b739e9aceef1febd66ea1f9c56067883019d0a1f1861"),
    ("hybrid-cue fs0 export ZstdPerFile", "d4975ec5fc873a9bfc9eabd5c0d254c6c1f5351a325080f8814793a04d0326c1"),
    ("hybrid-cue fs0 export BinHex", "2a78fc27aba2d3e826830d6056fb5ba7a0b9486fb145ad5d2cef940b41077274"),
    ("hybrid-cue fs0 export Tar", "2409f818007cde36a4e46a1d89470511f78bc30906ce99df480ac7ac98ee83d2"),
    ("hybrid-cue fs0 export TarGz", "c67cb1a1d0209d561083dbbadd09e7d0ec07026d3095a4934052e9ea5cb119de"),
    ("hybrid-cue fs0 export TarZstd", "4c838978d6a5c9a42f32556e04f5df9cd2cc2dac6a18e21a10f7c31daa3491c8"),
    ("hybrid-cue fs0 export Zip", "e6e8211a87ee869ec9f54f5e49c9e6336f8cf2d62e3f3b00b4de177539270a57"),
    ("hybrid-cue fs0 export StuffIt", "ca9ee00c516ae336fdcdf0b9b64aea952280219d741e96d68ef2a47b9e657144"),
    ("hybrid-cue fs0 export MacArchive", "18936bf318a4fd958fa1433708d423d8fd3ec6b1197304576301629f6306af48"),
    ("hybrid-cue fs0 export StuffItHqx", "a21c1485f0554b306c2cc8837ac9dfdb238a04e5070cc96c93b33b87b7697ba6"),
    ("hybrid-cue fs0 export loose Native", "ERR"),
    ("hybrid-cue fs0 export loose AppleDouble", "9833a604562fb2cc8ef7cda33da64d70f5ff39aab5a13c54f783be641432c544"),
    ("hybrid-cue fs0 export loose MacBinary", "fb7ef7dcbd4f849fde895355beebbd03c0bd1a1315722a93e613d83f26451be7"),
    ("hybrid-cue fs0 export loose BinHex", "2a78fc27aba2d3e826830d6056fb5ba7a0b9486fb145ad5d2cef940b41077274"),
    ("hybrid-cue fs0 export loose SeparateRsrc", "6697719cdb3ae82e5d9b6089659c46cd843efe1dc5f5c6833f2a6b113b52ace9"),
    ("hybrid-cue fs0 export loose DataForkOnly", "7ed6fbcbd762f1808241f9883dea50223f58309080cbce59e6e2d132c6fdeab9"),
    ("hybrid-cue fs1 tree", "efab43bfb45b60054dbb561f470b5e965ab18cc53e7a4ba52525f8b491534be5"),
    ("hybrid-cue fs1 extract / Native", "ERR"),
    ("hybrid-cue fs1 extract / AppleDouble", "f901e67fdd57871e326779e2f7d36af1fef9a3da201a2ca8279b19333dca95d5"),
    ("hybrid-cue fs1 extract / MacBinary", "e0fd4be4204d0e85973eec8c8ec1237a07d4c33d3caf9f68b12b0a245be1e20b"),
    ("hybrid-cue fs1 extract / BinHex", "46b6c6292d072074a93993539dfda216aca675e43d886aecb73c6e2d5616f06b"),
    ("hybrid-cue fs1 extract / SeparateRsrc", "c6cf86953cb23eff754f0a07baa311ccc47f58731cfb430de5391f00f5f9fdeb"),
    ("hybrid-cue fs1 extract / DataForkOnly", "d51ba424b8385722d70eb19e4588a405e8e46a2b1512c1d5b5c8154828933488"),
    ("hybrid-cue fs1 extract /Apps", "47ad8cc73ca5e04d268d45f9207dfd18d35df3f07626d0560661e2ba7d76e7e9"),
    ("hybrid-cue fs1 extract /README.txt", "07c3da82b9cd6f7a70e263622a08520a650f9a6f1755b03519a43bfd98a7f187"),
    ("hybrid-cue fs1 preview /Apps/GameApp", "text 36757b5d63a1a783a5b14d998d08dbdfb43e1798751fe5908e819f3a180469ca"),
    ("hybrid-cue fs1 preview /README.txt", "text 08d4cc190576fe63c81d6c7a383b4081924fbe15ccb62abe6399d61220d1d78f"),
    ("hybrid-cue fs1 export LooseFiles", "2e4d7c2e70056adae78b2b5ba03f9adf8120e7fa62f48b98fb18e8b14a97f5f2"),
    ("hybrid-cue fs1 export GzipPerFile", "b1c8a07e53a2f5629105682a10f10b7c2a5fd099584def4595d2e9109c618c30"),
    ("hybrid-cue fs1 export ZstdPerFile", "ee5cb882e35155c4e3c114532d3d0e9643e78d9d03fdd26cb86bd9d6016a1400"),
    ("hybrid-cue fs1 export BinHex", "315362daa05ba94019df06d7a0f64323a359e5e9cbab20587d9bbf12e6eb9a33"),
    ("hybrid-cue fs1 export Tar", "1ed9cd71d4098138508d1813e332b91dac5a60de486ec1e49aa22ad8bcd59fcc"),
    ("hybrid-cue fs1 export TarGz", "8907b7e6550cb7548ba2a83a8f71e4515c8519bd8d2944be217ae97a667ca893"),
    ("hybrid-cue fs1 export TarZstd", "c1c7ef29fd7cdc0a710f42a641e3f19d64fa430cb791528fa83b952601e0330d"),
    ("hybrid-cue fs1 export Zip", "777940d5091996c3f14f3abbc1f85fd3b12e37780cbb22cb7b5b3b2dde59390c"),
    ("hybrid-cue fs1 export StuffIt", "3d23055963da1ecfe5e8a787b46eff04f9933c9d56f45687589502a4c7d91f68"),
    ("hybrid-cue fs1 export MacArchive", "edd9d239e7b986a786620555564e6e5facfa84cb7eb3fd2e291fd3c30a761cad"),
    ("hybrid-cue fs1 export StuffItHqx", "9667b7bcaff56302b8870256e97c99f2b38423a431874cc987ad41a92a8a928c"),
    ("hybrid-cue fs1 export loose Native", "ERR"),
    ("hybrid-cue fs1 export loose AppleDouble", "2e4d7c2e70056adae78b2b5ba03f9adf8120e7fa62f48b98fb18e8b14a97f5f2"),
    ("hybrid-cue fs1 export loose MacBinary", "ed9a79f4b54103e78fcc62ec44ed1d80005ce4bc951346070efa7066f636d026"),
    ("hybrid-cue fs1 export loose BinHex", "315362daa05ba94019df06d7a0f64323a359e5e9cbab20587d9bbf12e6eb9a33"),
    ("hybrid-cue fs1 export loose SeparateRsrc", "67ae8c744e9111d7fb9e4fb20975877419dffa6da041af6e5d3f88fe0a3177ae"),
    ("hybrid-cue fs1 export loose DataForkOnly", "b4541ad5de4fcc9325261dcb5356f0ea123aa7e495206599f14d5916150a64f9"),
    ("rr choices", "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b"),
    ("rr fs0 tree", "a2989c8802ac4c7965515721f07972fbc7c636df923ad96fab074bf297653d03"),
    ("rr fs0 extract / Native", "409cb7ef7aa82735bae4439cde51a3330acde4c4cad5b8e5e4756940e01cc645"),
    ("rr fs0 extract / AppleDouble", "409cb7ef7aa82735bae4439cde51a3330acde4c4cad5b8e5e4756940e01cc645"),
    ("rr fs0 extract / MacBinary", "409cb7ef7aa82735bae4439cde51a3330acde4c4cad5b8e5e4756940e01cc645"),
    ("rr fs0 extract / BinHex", "d3a7f3c56eb0c30935b25461f16f6d3949c768334af34b498b6e631f830fed92"),
    ("rr fs0 extract / SeparateRsrc", "409cb7ef7aa82735bae4439cde51a3330acde4c4cad5b8e5e4756940e01cc645"),
    ("rr fs0 extract / DataForkOnly", "409cb7ef7aa82735bae4439cde51a3330acde4c4cad5b8e5e4756940e01cc645"),
    ("rr fs0 extract /bin", "ed937cc218314fac215be5a7dee3f554c009845ecac9681375bb32db75699ec1"),
    ("rr fs0 extract /deep", "4ae9b79240e9f3afe24de396535a080556185bf4b928af826ce8a97605f3703d"),
    ("rr fs0 extract /docs", "ad52929259579286b77f36331ec805da3d390c112cbf3b0c9aac46f856edffd5"),
    ("rr fs0 extract /a_rather_long_file_name_that_exceeds_the_iso_level_one_limits.text", "d6ea47905c3a570740a213c4b00cb9f6e60579561d3d1bf2c530f7d3cd90f248"),
    ("rr fs0 extract /colon:name.txt", "c1ad87c96f7a2d56e535a278300e0f0834f33c796974ef78bb94b0b7ce856b23"),
    ("rr fs0 extract /colon_name.txt", "2814ce475ae8b28dc13bcb8b9ca06429f365c447ccd4db364752491c4a02b7fb"),
    ("rr fs0 extract /empty.dat", "3c3bf8947d6ea35e14172e083930b5c6ee6edb2d2d55685f631e7fd2c94aaba0"),
    ("rr fs0 extract /link", "ced5c6d445dc881a272f0eff4b634c78a4ffbb354465ed90d28e00cbb5c9482a"),
    ("rr fs0 extract /name with spaces.txt", "ae166d3dbb2590b698510097d22acc750caff542d9a9821f9a535bf4d75abca6"),
    ("rr fs0 preview /bin/data.bin", "binary d3859081b6ebe8d1e0ff6387a734eeb63afe2142343e77be4f836b961e7b141f"),
    ("rr fs0 preview /bin/run.sh", "text 299001868fb8c02fd431c336c6d058f5558c5dff5b5af5e6fe04b870a6a9cbba"),
    ("rr fs0 preview /docs/MixedCase.TXT", "text 922923753e690a2de0c0ffb3357aadcef4878d3b6a5d3f2f5898ae00b5e651ee"),
    ("rr fs0 preview /docs/readme.txt", "text a9907cf0b1782ca1a142c9693df550113c728e55bc0b1a8b9a314800df73a3c4"),
    ("rr fs0 preview /a_rather_long_file_name_that_exceeds_the_iso_level_one_limits.text", "text 67b3de4297c69a4e46c5cc2f2b83c08fa4091be2bc7c89aa00f33733a0c3783b"),
    ("rr fs0 preview /colon:name.txt", "text 2cf7dfa85271cc3692d6572705aa84342f5b87ee90386b97d96eb37bbe2850c8"),
    ("rr fs0 preview /colon_name.txt", "text 10af960b268d98ed1e95acc1c1d7e9a655b0967acca44fdc4e4af2559ae05ce3"),
    ("rr fs0 preview /empty.dat", "text e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
    ("rr fs0 preview /link", "text e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
    ("rr fs0 preview /name with spaces.txt", "text e47fbedb2823cf1ae4d4cdb8273635be2024cb870588e259c9b23d76ae49d484"),
    ("rr fs0 export LooseFiles", "9df9670f21d68fa1646322004d5f220d3b25e1854b17bcbc922b4a6f5d475d0f"),
    ("rr fs0 export GzipPerFile", "cc482e4ccc0215769a2d41a6d0dfa9345b44a977fd898e71c946da66425caa47"),
    ("rr fs0 export ZstdPerFile", "2ddb0988776136875327402dcca2d27c5f958ec45742e048c5371486fc69d164"),
    ("rr fs0 export BinHex", "3dd383327a168a4e0fa292ebb09bf803ca0e2482667efde66a701159aafd8738"),
    ("rr fs0 export Tar", "4ec1fb99165c1ae751322b984309bf64397dfc3f6b0340a0291bd4ee2230a984"),
    ("rr fs0 export TarGz", "f77a0c909cd06b07b50b23eb92659ac2387be46abe54a47ccc6ff487ea4081a1"),
    ("rr fs0 export TarZstd", "b3755cdfd7b2ccf25cbdc757be8d560f8dcb03e58e25e5b951f419b3a0fbf4ec"),
    ("rr fs0 export Zip", "4a6cd317e16aebcec2a29c951f593e7ac88990b4f5e934052e404bdd7a54959b"),
    ("rr fs0 export StuffIt", "f0daa3f33a63605297e654632642e61699e3153b60cfdbb8a627304dae6da82b"),
    ("rr fs0 export MacArchive", "b9cdca38ba8b22a1d92a86f2c6b7810bd82b8d3b11267f07f5d82e9c5ac60b33"),
    ("rr fs0 export StuffItHqx", "1cfb39080cec6a3b848d76d61954dd7ad89ac63bfa84023a3eefc790e90690e7"),
    ("rr fs0 export loose Native", "9df9670f21d68fa1646322004d5f220d3b25e1854b17bcbc922b4a6f5d475d0f"),
    ("rr fs0 export loose AppleDouble", "9df9670f21d68fa1646322004d5f220d3b25e1854b17bcbc922b4a6f5d475d0f"),
    ("rr fs0 export loose MacBinary", "9df9670f21d68fa1646322004d5f220d3b25e1854b17bcbc922b4a6f5d475d0f"),
    ("rr fs0 export loose BinHex", "3dd383327a168a4e0fa292ebb09bf803ca0e2482667efde66a701159aafd8738"),
    ("rr fs0 export loose SeparateRsrc", "9df9670f21d68fa1646322004d5f220d3b25e1854b17bcbc922b4a6f5d475d0f"),
    ("rr fs0 export loose DataForkOnly", "9df9670f21d68fa1646322004d5f220d3b25e1854b17bcbc922b4a6f5d475d0f"),
    ("rr-prep choices", "d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35"),
    ("rr-prep fs0 tree", "a2989c8802ac4c7965515721f07972fbc7c636df923ad96fab074bf297653d03"),
    ("rr-prep fs0 extract / Native", "409cb7ef7aa82735bae4439cde51a3330acde4c4cad5b8e5e4756940e01cc645"),
    ("rr-prep fs0 extract / AppleDouble", "409cb7ef7aa82735bae4439cde51a3330acde4c4cad5b8e5e4756940e01cc645"),
    ("rr-prep fs0 extract / MacBinary", "409cb7ef7aa82735bae4439cde51a3330acde4c4cad5b8e5e4756940e01cc645"),
    ("rr-prep fs0 extract / BinHex", "d3a7f3c56eb0c30935b25461f16f6d3949c768334af34b498b6e631f830fed92"),
    ("rr-prep fs0 extract / SeparateRsrc", "409cb7ef7aa82735bae4439cde51a3330acde4c4cad5b8e5e4756940e01cc645"),
    ("rr-prep fs0 extract / DataForkOnly", "409cb7ef7aa82735bae4439cde51a3330acde4c4cad5b8e5e4756940e01cc645"),
    ("rr-prep fs0 extract /bin", "ed937cc218314fac215be5a7dee3f554c009845ecac9681375bb32db75699ec1"),
    ("rr-prep fs0 extract /deep", "4ae9b79240e9f3afe24de396535a080556185bf4b928af826ce8a97605f3703d"),
    ("rr-prep fs0 extract /docs", "ad52929259579286b77f36331ec805da3d390c112cbf3b0c9aac46f856edffd5"),
    ("rr-prep fs0 extract /a_rather_long_file_name_that_exceeds_the_iso_level_one_limits.text", "d6ea47905c3a570740a213c4b00cb9f6e60579561d3d1bf2c530f7d3cd90f248"),
    ("rr-prep fs0 extract /colon:name.txt", "c1ad87c96f7a2d56e535a278300e0f0834f33c796974ef78bb94b0b7ce856b23"),
    ("rr-prep fs0 extract /colon_name.txt", "2814ce475ae8b28dc13bcb8b9ca06429f365c447ccd4db364752491c4a02b7fb"),
    ("rr-prep fs0 extract /empty.dat", "3c3bf8947d6ea35e14172e083930b5c6ee6edb2d2d55685f631e7fd2c94aaba0"),
    ("rr-prep fs0 extract /link", "ced5c6d445dc881a272f0eff4b634c78a4ffbb354465ed90d28e00cbb5c9482a"),
    ("rr-prep fs0 extract /name with spaces.txt", "ae166d3dbb2590b698510097d22acc750caff542d9a9821f9a535bf4d75abca6"),
    ("rr-prep fs0 preview /bin/data.bin", "binary d3859081b6ebe8d1e0ff6387a734eeb63afe2142343e77be4f836b961e7b141f"),
    ("rr-prep fs0 preview /bin/run.sh", "text 299001868fb8c02fd431c336c6d058f5558c5dff5b5af5e6fe04b870a6a9cbba"),
    ("rr-prep fs0 preview /docs/MixedCase.TXT", "text 922923753e690a2de0c0ffb3357aadcef4878d3b6a5d3f2f5898ae00b5e651ee"),
    ("rr-prep fs0 preview /docs/readme.txt", "text a9907cf0b1782ca1a142c9693df550113c728e55bc0b1a8b9a314800df73a3c4"),
    ("rr-prep fs0 preview /a_rather_long_file_name_that_exceeds_the_iso_level_one_limits.text", "text 67b3de4297c69a4e46c5cc2f2b83c08fa4091be2bc7c89aa00f33733a0c3783b"),
    ("rr-prep fs0 preview /colon:name.txt", "text 2cf7dfa85271cc3692d6572705aa84342f5b87ee90386b97d96eb37bbe2850c8"),
    ("rr-prep fs0 preview /colon_name.txt", "text 10af960b268d98ed1e95acc1c1d7e9a655b0967acca44fdc4e4af2559ae05ce3"),
    ("rr-prep fs0 preview /empty.dat", "text e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
    ("rr-prep fs0 preview /link", "text e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
    ("rr-prep fs0 preview /name with spaces.txt", "text e47fbedb2823cf1ae4d4cdb8273635be2024cb870588e259c9b23d76ae49d484"),
    ("rr-prep fs0 export LooseFiles", "9df9670f21d68fa1646322004d5f220d3b25e1854b17bcbc922b4a6f5d475d0f"),
    ("rr-prep fs0 export GzipPerFile", "cc482e4ccc0215769a2d41a6d0dfa9345b44a977fd898e71c946da66425caa47"),
    ("rr-prep fs0 export ZstdPerFile", "2ddb0988776136875327402dcca2d27c5f958ec45742e048c5371486fc69d164"),
    ("rr-prep fs0 export BinHex", "3dd383327a168a4e0fa292ebb09bf803ca0e2482667efde66a701159aafd8738"),
    ("rr-prep fs0 export Tar", "4ec1fb99165c1ae751322b984309bf64397dfc3f6b0340a0291bd4ee2230a984"),
    ("rr-prep fs0 export TarGz", "f77a0c909cd06b07b50b23eb92659ac2387be46abe54a47ccc6ff487ea4081a1"),
    ("rr-prep fs0 export TarZstd", "b3755cdfd7b2ccf25cbdc757be8d560f8dcb03e58e25e5b951f419b3a0fbf4ec"),
    ("rr-prep fs0 export Zip", "4a6cd317e16aebcec2a29c951f593e7ac88990b4f5e934052e404bdd7a54959b"),
    ("rr-prep fs0 export StuffIt", "f0daa3f33a63605297e654632642e61699e3153b60cfdbb8a627304dae6da82b"),
    ("rr-prep fs0 export MacArchive", "b9cdca38ba8b22a1d92a86f2c6b7810bd82b8d3b11267f07f5d82e9c5ac60b33"),
    ("rr-prep fs0 export StuffItHqx", "1cfb39080cec6a3b848d76d61954dd7ad89ac63bfa84023a3eefc790e90690e7"),
    ("rr-prep fs0 export loose Native", "9df9670f21d68fa1646322004d5f220d3b25e1854b17bcbc922b4a6f5d475d0f"),
    ("rr-prep fs0 export loose AppleDouble", "9df9670f21d68fa1646322004d5f220d3b25e1854b17bcbc922b4a6f5d475d0f"),
    ("rr-prep fs0 export loose MacBinary", "9df9670f21d68fa1646322004d5f220d3b25e1854b17bcbc922b4a6f5d475d0f"),
    ("rr-prep fs0 export loose BinHex", "3dd383327a168a4e0fa292ebb09bf803ca0e2482667efde66a701159aafd8738"),
    ("rr-prep fs0 export loose SeparateRsrc", "9df9670f21d68fa1646322004d5f220d3b25e1854b17bcbc922b4a6f5d475d0f"),
    ("rr-prep fs0 export loose DataForkOnly", "9df9670f21d68fa1646322004d5f220d3b25e1854b17bcbc922b4a6f5d475d0f"),
    ("rr-prep fs1 tree", "6bbef5e7b55f5df411e2c9c5f649794316405a54ac2f422990daee684c33843c"),
    ("rr-prep fs1 extract / Native", "8419e15da8d4452489ed3152667b83079a0a4f3ecd02efd261483d16a55a082f"),
    ("rr-prep fs1 extract / AppleDouble", "8419e15da8d4452489ed3152667b83079a0a4f3ecd02efd261483d16a55a082f"),
    ("rr-prep fs1 extract / MacBinary", "8419e15da8d4452489ed3152667b83079a0a4f3ecd02efd261483d16a55a082f"),
    ("rr-prep fs1 extract / BinHex", "1ac048abac1b399018212e724f0e705474b1835bd0c67decdb4c5e7ce71d0139"),
    ("rr-prep fs1 extract / SeparateRsrc", "8419e15da8d4452489ed3152667b83079a0a4f3ecd02efd261483d16a55a082f"),
    ("rr-prep fs1 extract / DataForkOnly", "8419e15da8d4452489ed3152667b83079a0a4f3ecd02efd261483d16a55a082f"),
    ("rr-prep fs1 extract /prep-boot-1.img", "5c547913f0dc17fec2741948b8d6617825c3c629c33623af640012ccf70c7888"),
    ("rr-prep fs1 preview /prep-boot-1.img", "binary 5f24b2f16026ec7d0450a5a08283d3cfd47302fe859f579ed79fe7d2663b73f9"),
    ("rr-prep fs1 export LooseFiles", "5c547913f0dc17fec2741948b8d6617825c3c629c33623af640012ccf70c7888"),
    ("rr-prep fs1 export GzipPerFile", "38c584ab5abefcebc656020b79905a8309385a72df7deb2e3a5836a79735e2b1"),
    ("rr-prep fs1 export ZstdPerFile", "06b528350b006f9822c76022e35b5cf37a9ee9c902804f21249750e4797d95c0"),
    ("rr-prep fs1 export BinHex", "245b0509f96ebc70442f71a32865cc90244e71186f6c9006af9356e383232a2b"),
    ("rr-prep fs1 export Tar", "534439c656a49b357e254fdee00fc1bda760eb8bd4c3085d298db94a33f98a1b"),
    ("rr-prep fs1 export TarGz", "26f9d1af5c9b890e5c080f92d5c19ca4a306227443d023b0d593724a63686d89"),
    ("rr-prep fs1 export TarZstd", "47499ab17c6f7799b81eabc9712d0ca830c11fc97258e9b3924af1c037a547b4"),
    ("rr-prep fs1 export Zip", "0b0624a5ca4508e43481c7ed2bb687d4ff7f2c00c38bb252c94759caa23b0ca2"),
    ("rr-prep fs1 export StuffIt", "909a484c753251e5882eea237d0a0a19d9d3f7297f188fb817c40515e88f1915"),
    ("rr-prep fs1 export MacArchive", "48606a60600fc64bc7a17023280d548203ee2c73e314366732c57237413818ac"),
    ("rr-prep fs1 export StuffItHqx", "c578daa63f8bd0fdd40af9a33a9e38bf73ede4643b6c213677f6fcdb5e8c0732"),
    ("rr-prep fs1 export loose Native", "5c547913f0dc17fec2741948b8d6617825c3c629c33623af640012ccf70c7888"),
    ("rr-prep fs1 export loose AppleDouble", "5c547913f0dc17fec2741948b8d6617825c3c629c33623af640012ccf70c7888"),
    ("rr-prep fs1 export loose MacBinary", "5c547913f0dc17fec2741948b8d6617825c3c629c33623af640012ccf70c7888"),
    ("rr-prep fs1 export loose BinHex", "245b0509f96ebc70442f71a32865cc90244e71186f6c9006af9356e383232a2b"),
    ("rr-prep fs1 export loose SeparateRsrc", "5c547913f0dc17fec2741948b8d6617825c3c629c33623af640012ccf70c7888"),
    ("rr-prep fs1 export loose DataForkOnly", "5c547913f0dc17fec2741948b8d6617825c3c629c33623af640012ccf70c7888"),
    ("hybrid-chd choices", "d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35"),
    ("hybrid-chd fs0 tree", "21289477a5420702770d5f54b445f6bf0fb5874cec5560fa7cac56a19415ff3f"),
    ("hybrid-chd fs0 extract / Native", "ERR"),
    ("hybrid-chd fs0 extract / AppleDouble", "9aa28ebabcbb05b8ef570cf0d788344df9404947361dcd84ee5368953183d7ba"),
    ("hybrid-chd fs0 extract / MacBinary", "e1221ac823f93257423ad5ec74ff7522a183eb58d10ea667f49f87019578a428"),
    ("hybrid-chd fs0 extract / BinHex", "90567bedec6b30504020b64a0033f4e4c9c2b862fe5b6d5eeadc48a809b6b8bf"),
    ("hybrid-chd fs0 extract / SeparateRsrc", "60048f8758442bfb05a98f74194acbe9b24fad4977c557364884772349dcb8e8"),
    ("hybrid-chd fs0 extract / DataForkOnly", "e57312a6ffc110ae20b052e5c58a5a94a1a7fdc5cc8b0042e35833926327230d"),
    ("hybrid-chd fs0 extract /APPS", "c01b2afb78257839d460dd7ff4034535183b1067feba09c0359eb8439e6a9211"),
    ("hybrid-chd fs0 extract /README.TXT", "c704938fb712aa0fe5c9e5ce7ce878792451ab108d2cda27bc1a834e517dc373"),
    ("hybrid-chd fs0 preview /APPS/GAMEAPP", "text 36757b5d63a1a783a5b14d998d08dbdfb43e1798751fe5908e819f3a180469ca"),
    ("hybrid-chd fs0 preview /README.TXT", "text 08d4cc190576fe63c81d6c7a383b4081924fbe15ccb62abe6399d61220d1d78f"),
    ("hybrid-chd fs0 export LooseFiles", "9833a604562fb2cc8ef7cda33da64d70f5ff39aab5a13c54f783be641432c544"),
    ("hybrid-chd fs0 export GzipPerFile", "ca7fac0ea5541e9ae3b7b739e9aceef1febd66ea1f9c56067883019d0a1f1861"),
    ("hybrid-chd fs0 export ZstdPerFile", "d4975ec5fc873a9bfc9eabd5c0d254c6c1f5351a325080f8814793a04d0326c1"),
    ("hybrid-chd fs0 export BinHex", "2a78fc27aba2d3e826830d6056fb5ba7a0b9486fb145ad5d2cef940b41077274"),
    ("hybrid-chd fs0 export Tar", "2409f818007cde36a4e46a1d89470511f78bc30906ce99df480ac7ac98ee83d2"),
    ("hybrid-chd fs0 export TarGz", "c67cb1a1d0209d561083dbbadd09e7d0ec07026d3095a4934052e9ea5cb119de"),
    ("hybrid-chd fs0 export TarZstd", "4c838978d6a5c9a42f32556e04f5df9cd2cc2dac6a18e21a10f7c31daa3491c8"),
    ("hybrid-chd fs0 export Zip", "e6e8211a87ee869ec9f54f5e49c9e6336f8cf2d62e3f3b00b4de177539270a57"),
    ("hybrid-chd fs0 export StuffIt", "ca9ee00c516ae336fdcdf0b9b64aea952280219d741e96d68ef2a47b9e657144"),
    ("hybrid-chd fs0 export MacArchive", "18936bf318a4fd958fa1433708d423d8fd3ec6b1197304576301629f6306af48"),
    ("hybrid-chd fs0 export StuffItHqx", "a21c1485f0554b306c2cc8837ac9dfdb238a04e5070cc96c93b33b87b7697ba6"),
    ("hybrid-chd fs0 export loose Native", "ERR"),
    ("hybrid-chd fs0 export loose AppleDouble", "9833a604562fb2cc8ef7cda33da64d70f5ff39aab5a13c54f783be641432c544"),
    ("hybrid-chd fs0 export loose MacBinary", "fb7ef7dcbd4f849fde895355beebbd03c0bd1a1315722a93e613d83f26451be7"),
    ("hybrid-chd fs0 export loose BinHex", "2a78fc27aba2d3e826830d6056fb5ba7a0b9486fb145ad5d2cef940b41077274"),
    ("hybrid-chd fs0 export loose SeparateRsrc", "6697719cdb3ae82e5d9b6089659c46cd843efe1dc5f5c6833f2a6b113b52ace9"),
    ("hybrid-chd fs0 export loose DataForkOnly", "7ed6fbcbd762f1808241f9883dea50223f58309080cbce59e6e2d132c6fdeab9"),
    ("hybrid-chd fs1 tree", "efab43bfb45b60054dbb561f470b5e965ab18cc53e7a4ba52525f8b491534be5"),
    ("hybrid-chd fs1 extract / Native", "ERR"),
    ("hybrid-chd fs1 extract / AppleDouble", "f901e67fdd57871e326779e2f7d36af1fef9a3da201a2ca8279b19333dca95d5"),
    ("hybrid-chd fs1 extract / MacBinary", "e0fd4be4204d0e85973eec8c8ec1237a07d4c33d3caf9f68b12b0a245be1e20b"),
    ("hybrid-chd fs1 extract / BinHex", "46b6c6292d072074a93993539dfda216aca675e43d886aecb73c6e2d5616f06b"),
    ("hybrid-chd fs1 extract / SeparateRsrc", "c6cf86953cb23eff754f0a07baa311ccc47f58731cfb430de5391f00f5f9fdeb"),
    ("hybrid-chd fs1 extract / DataForkOnly", "d51ba424b8385722d70eb19e4588a405e8e46a2b1512c1d5b5c8154828933488"),
    ("hybrid-chd fs1 extract /Apps", "47ad8cc73ca5e04d268d45f9207dfd18d35df3f07626d0560661e2ba7d76e7e9"),
    ("hybrid-chd fs1 extract /README.txt", "07c3da82b9cd6f7a70e263622a08520a650f9a6f1755b03519a43bfd98a7f187"),
    ("hybrid-chd fs1 preview /Apps/GameApp", "text 36757b5d63a1a783a5b14d998d08dbdfb43e1798751fe5908e819f3a180469ca"),
    ("hybrid-chd fs1 preview /README.txt", "text 08d4cc190576fe63c81d6c7a383b4081924fbe15ccb62abe6399d61220d1d78f"),
    ("hybrid-chd fs1 export LooseFiles", "2e4d7c2e70056adae78b2b5ba03f9adf8120e7fa62f48b98fb18e8b14a97f5f2"),
    ("hybrid-chd fs1 export GzipPerFile", "b1c8a07e53a2f5629105682a10f10b7c2a5fd099584def4595d2e9109c618c30"),
    ("hybrid-chd fs1 export ZstdPerFile", "ee5cb882e35155c4e3c114532d3d0e9643e78d9d03fdd26cb86bd9d6016a1400"),
    ("hybrid-chd fs1 export BinHex", "315362daa05ba94019df06d7a0f64323a359e5e9cbab20587d9bbf12e6eb9a33"),
    ("hybrid-chd fs1 export Tar", "1ed9cd71d4098138508d1813e332b91dac5a60de486ec1e49aa22ad8bcd59fcc"),
    ("hybrid-chd fs1 export TarGz", "8907b7e6550cb7548ba2a83a8f71e4515c8519bd8d2944be217ae97a667ca893"),
    ("hybrid-chd fs1 export TarZstd", "c1c7ef29fd7cdc0a710f42a641e3f19d64fa430cb791528fa83b952601e0330d"),
    ("hybrid-chd fs1 export Zip", "777940d5091996c3f14f3abbc1f85fd3b12e37780cbb22cb7b5b3b2dde59390c"),
    ("hybrid-chd fs1 export StuffIt", "3d23055963da1ecfe5e8a787b46eff04f9933c9d56f45687589502a4c7d91f68"),
    ("hybrid-chd fs1 export MacArchive", "edd9d239e7b986a786620555564e6e5facfa84cb7eb3fd2e291fd3c30a761cad"),
    ("hybrid-chd fs1 export StuffItHqx", "9667b7bcaff56302b8870256e97c99f2b38423a431874cc987ad41a92a8a928c"),
    ("hybrid-chd fs1 export loose Native", "ERR"),
    ("hybrid-chd fs1 export loose AppleDouble", "2e4d7c2e70056adae78b2b5ba03f9adf8120e7fa62f48b98fb18e8b14a97f5f2"),
    ("hybrid-chd fs1 export loose MacBinary", "ed9a79f4b54103e78fcc62ec44ed1d80005ce4bc951346070efa7066f636d026"),
    ("hybrid-chd fs1 export loose BinHex", "315362daa05ba94019df06d7a0f64323a359e5e9cbab20587d9bbf12e6eb9a33"),
    ("hybrid-chd fs1 export loose SeparateRsrc", "67ae8c744e9111d7fb9e4fb20975877419dffa6da041af6e5d3f88fe0a3177ae"),
    ("hybrid-chd fs1 export loose DataForkOnly", "b4541ad5de4fcc9325261dcb5356f0ea123aa7e495206599f14d5916150a64f9"),
];
