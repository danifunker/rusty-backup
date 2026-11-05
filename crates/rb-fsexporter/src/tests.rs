use super::*;
use disk_core::filesystem::FilesystemError;
use std::collections::HashMap;
use std::io::Write;

/// A tree held in memory: path -> (entry, data, resource fork), children by parent path.
struct Mem {
    label: Option<String>,
    nodes: HashMap<String, (FileEntry, Vec<u8>, Vec<u8>)>,
    kids: HashMap<String, Vec<String>>,
    broken: Vec<String>,
}

impl Mem {
    fn new(label: Option<&str>) -> Self {
        let mut m = Mem {
            label: label.map(str::to_string),
            nodes: HashMap::new(),
            kids: HashMap::new(),
            broken: Vec::new(),
        };
        m.nodes
            .insert("/".into(), (FileEntry::root(), Vec::new(), Vec::new()));
        m
    }

    fn parent(path: &str) -> String {
        match path.rfind('/') {
            Some(0) | None => "/".into(),
            Some(i) => path[..i].to_string(),
        }
    }

    fn add(&mut self, e: FileEntry, data: &[u8], rsrc: &[u8]) {
        let p = e.path.clone();
        self.kids
            .entry(Self::parent(&p))
            .or_default()
            .push(p.clone());
        self.nodes.insert(p, (e, data.to_vec(), rsrc.to_vec()));
    }

    fn file(&mut self, path: &str, data: &[u8]) {
        let name = path.rsplit('/').next().unwrap().to_string();
        let e = FileEntry::new_file(name, path.into(), data.len() as u64, 0);
        self.add(e, data, b"");
    }

    fn dir(&mut self, path: &str) {
        let name = path.rsplit('/').next().unwrap().to_string();
        self.add(FileEntry::new_directory(name, path.into(), 0), b"", b"");
    }

    fn entry(&self, path: &str) -> FileEntry {
        self.nodes[path].0.clone()
    }
}

impl Filesystem for Mem {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(self.entry("/"))
    }
    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let kids = self.kids.get(&entry.path).cloned().unwrap_or_default();
        Ok(kids.iter().map(|p| self.nodes[p].0.clone()).collect())
    }
    fn read_file(&mut self, entry: &FileEntry, max: usize) -> Result<Vec<u8>, FilesystemError> {
        if self.broken.contains(&entry.path) {
            return Err(FilesystemError::Parse("bad sector".into()));
        }
        let mut d = self.nodes[&entry.path].1.clone();
        d.truncate(max);
        Ok(d)
    }
    fn write_resource_fork_to(
        &mut self,
        entry: &FileEntry,
        w: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let r = &self.nodes[&entry.path].2;
        w.write_all(r)?;
        Ok(r.len() as u64)
    }
    fn volume_label(&self) -> Option<&str> {
        self.label.as_deref()
    }
    fn fs_type(&self) -> &str {
        "mem"
    }
    fn total_size(&self) -> u64 {
        0
    }
    fn used_size(&self) -> u64 {
        0
    }
}

/// `d path`, `f path content`, `l path -> target`, sorted.
fn listing(root: &Path) -> Vec<String> {
    fn walk(base: &Path, dir: &Path, out: &mut Vec<String>) {
        for e in std::fs::read_dir(dir).unwrap().flatten() {
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
                let body = String::from_utf8_lossy(&std::fs::read(&p).unwrap()).into_owned();
                out.push(format!("f {rel} {body}"));
            }
        }
    }
    let mut v = Vec::new();
    walk(root, root, &mut v);
    v.sort();
    v
}

fn sample() -> Mem {
    let mut m = Mem::new(Some("MY DISK  "));
    m.dir("/docs");
    m.file("/docs/a.txt", b"alpha");
    m.dir("/docs/deep");
    m.file("/docs/deep/b.txt", b"beta");
    m.file("/top.txt", b"top");
    m
}

#[derive(Default)]
struct Log {
    lines: Vec<String>,
    cancel_after: Option<usize>,
    wrote: usize,
}

impl Host for Log {
    fn cancelled(&self) -> bool {
        self.cancel_after.is_some_and(|n| self.wrote >= n)
    }
    fn wrote(&mut self, entry: &FileEntry, bytes: u64, total: &Summary) {
        self.wrote = total.files;
        self.lines.push(format!("wrote {} {bytes}", entry.path));
    }
    fn failed(&mut self, entry: &FileEntry, err: &anyhow::Error) {
        self.lines.push(format!("failed {} {err}", entry.path));
    }
    fn exists(&mut self, path: &Path) {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        self.lines.push(format!("exists {name}"));
    }
    fn collision(&mut self, name: &str, outcome: Collision<'_>) {
        self.lines.push(format!("collision {name} {outcome:?}"));
    }
}

#[test]
fn a_volume_root_becomes_a_folder_named_after_the_volume() {
    let mut fs = sample();
    let dest = tempfile::tempdir().unwrap();
    let opts = Options::new(ResourceForkMode::AppleDouble);
    let root = fs.entry("/");
    let s = extract(&mut fs, &root, dest.path(), &opts, &mut NoHost).unwrap();
    assert_eq!(s.files, 3);
    assert_eq!(
        listing(dest.path()),
        [
            "d MY DISK",
            "d MY DISK/docs",
            "d MY DISK/docs/deep",
            "f MY DISK/docs/a.txt alpha",
            "f MY DISK/docs/deep/b.txt beta",
            "f MY DISK/top.txt top",
        ]
    );
}

#[test]
fn an_unnamed_volume_keeps_the_underscore_and_contents_mode_skips_the_folder() {
    let mut fs = sample();
    fs.label = Some("   ".into());
    let root = fs.entry("/");
    let dest = tempfile::tempdir().unwrap();
    let mut opts = Options::new(ResourceForkMode::AppleDouble);
    extract(&mut fs, &root, dest.path(), &opts, &mut NoHost).unwrap();
    assert!(dest.path().join("_/top.txt").is_file());
    let flat = tempfile::tempdir().unwrap();
    opts.volume_folder = false;
    extract(&mut fs, &root, flat.path(), &opts, &mut NoHost).unwrap();
    assert!(flat.path().join("top.txt").is_file());
    assert_eq!(volume_folder_name(Some("A/B.")), Some("A_B".to_string()));
    assert_eq!(top_name(&fs.entry("/docs"), Some("X")), "docs");
}

#[test]
fn contents_without_recursion_writes_only_the_files() {
    let mut fs = sample();
    let docs = fs.entry("/docs");
    let dest = tempfile::tempdir().unwrap();
    let mut opts = Options::new(ResourceForkMode::DataForkOnly);
    opts.recursive = false;
    extract_contents(&mut fs, &docs, dest.path(), &opts, &mut NoHost).unwrap();
    assert_eq!(listing(dest.path()), ["f a.txt alpha"]);
}

#[cfg(unix)]
#[test]
fn symlinks_are_host_symlinks_or_skipped() {
    let mut fs = sample();
    let link = FileEntry::new_symlink("ln".into(), "/ln".into(), 0, 0, "docs/a.txt".into());
    fs.add(link, b"", b"");
    let ln = fs.entry("/ln");
    let dest = tempfile::tempdir().unwrap();
    let mut opts = Options::new(ResourceForkMode::AppleDouble);
    extract(&mut fs, &ln, dest.path(), &opts, &mut NoHost).unwrap();
    // A second run replaces the link rather than failing on it.
    extract(&mut fs, &ln, dest.path(), &opts, &mut NoHost).unwrap();
    assert_eq!(listing(dest.path()), ["l ln -> docs/a.txt"]);
    let skip = tempfile::tempdir().unwrap();
    opts.symlinks = Symlinks::Skip;
    let s = extract(&mut fs, &ln, skip.path(), &opts, &mut NoHost).unwrap();
    assert_eq!(s.files, 0);
    assert!(listing(skip.path()).is_empty());
}

#[test]
fn a_refused_symlink_is_a_text_file_with_the_target() {
    let dest = tempfile::tempdir().unwrap();
    // A link with no target can't be made anywhere, so it takes the fallback.
    write_symlink("", &dest.path().join("empty")).unwrap();
    assert_eq!(listing(dest.path()), ["f empty "]);
}

#[test]
fn a_bad_entry_is_skipped_or_stops_the_run() {
    let mut fs = sample();
    fs.broken = vec!["/docs/a.txt".into()];
    let root = fs.entry("/");
    let dest = tempfile::tempdir().unwrap();
    let mut opts = Options::new(ResourceForkMode::AppleDouble);
    let mut log = Log::default();
    let s = extract(&mut fs, &root, dest.path(), &opts, &mut log).unwrap();
    assert_eq!(s.files, 2);
    assert!(log
        .lines
        .iter()
        .any(|l| l.starts_with("failed /docs/a.txt")));
    opts.on_error = OnError::Stop;
    let stop = tempfile::tempdir().unwrap();
    assert!(extract(&mut fs, &root, stop.path(), &opts, &mut NoHost).is_err());
}

/// A host that answers each failure from a script and records what it was asked.
struct Asker {
    answers: Vec<ErrorChoice>,
    asked: Vec<String>,
    skipped: Vec<String>,
}

impl Asker {
    fn new(answers: &[ErrorChoice]) -> Self {
        Asker {
            answers: answers.to_vec(),
            asked: Vec::new(),
            skipped: Vec::new(),
        }
    }
}

impl Host for Asker {
    fn on_error(&mut self, entry: &FileEntry, _err: &anyhow::Error) -> Option<ErrorChoice> {
        self.asked.push(entry.path.clone());
        Some(self.answers.remove(0))
    }
    fn failed(&mut self, entry: &FileEntry, err: &anyhow::Error) {
        self.skipped.push(format!("{} {err:#}", entry.path));
    }
}

/// `sample()` with three unreadable files, met in this order.
fn three_bad() -> Mem {
    let mut fs = sample();
    fs.file("/docs/c.txt", b"gamma");
    fs.file("/z.txt", b"zed");
    fs.broken = vec!["/docs/a.txt".into(), "/docs/c.txt".into(), "/z.txt".into()];
    fs
}

#[test]
fn skip_asks_again_at_each_failure() {
    let mut fs = three_bad();
    let root = fs.entry("/");
    let dest = tempfile::tempdir().unwrap();
    let mut opts = Options::new(ResourceForkMode::AppleDouble);
    // The host's answer wins over the policy.
    opts.on_error = OnError::Stop;
    let mut host = Asker::new(&[ErrorChoice::Skip; 3]);
    let s = extract(&mut fs, &root, dest.path(), &opts, &mut host).unwrap();
    assert_eq!(s.files, 2);
    assert_eq!(host.asked, ["/docs/a.txt", "/docs/c.txt", "/z.txt"]);
    assert_eq!(host.skipped.len(), 3);
}

#[test]
fn skip_all_skips_every_later_failure_without_asking() {
    let mut fs = three_bad();
    let root = fs.entry("/");
    let dest = tempfile::tempdir().unwrap();
    let opts = Options::new(ResourceForkMode::AppleDouble);
    let mut host = Asker::new(&[ErrorChoice::SkipAll]);
    let s = extract(&mut fs, &root, dest.path(), &opts, &mut host).unwrap();
    assert_eq!(s.files, 2);
    assert_eq!(host.asked, ["/docs/a.txt"]);
    assert_eq!(
        host.skipped,
        [
            "/docs/a.txt extracting 'a.txt': parse error: bad sector",
            "/docs/c.txt extracting 'c.txt': parse error: bad sector",
            "/z.txt extracting 'z.txt': parse error: bad sector"
        ]
    );
    assert!(dest.path().join("MY DISK/top.txt").is_file());
}

#[test]
fn stop_ends_the_run_and_keeps_what_was_written() {
    let mut fs = three_bad();
    let root = fs.entry("/");
    let dest = tempfile::tempdir().unwrap();
    let opts = Options::new(ResourceForkMode::AppleDouble);
    let mut host = Asker::new(&[ErrorChoice::Skip, ErrorChoice::Stop]);
    let err = extract(&mut fs, &root, dest.path(), &opts, &mut host).unwrap_err();
    assert!(format!("{err:#}").contains("bad sector"));
    assert_eq!(host.asked, ["/docs/a.txt", "/docs/c.txt"]);
    assert_eq!(
        host.skipped,
        ["/docs/a.txt extracting 'a.txt': parse error: bad sector"]
    );
    // /docs/deep came before the second failure and stays; /top.txt and /z.txt never ran.
    assert!(dest.path().join("MY DISK/docs/deep/b.txt").is_file());
    assert!(!dest.path().join("MY DISK/top.txt").exists());
}

#[test]
fn a_host_that_does_not_answer_follows_the_policy() {
    let mut fs = three_bad();
    let root = fs.entry("/");
    let dest = tempfile::tempdir().unwrap();
    let mut opts = Options::new(ResourceForkMode::AppleDouble);
    let mut log = Log::default();
    extract(&mut fs, &root, dest.path(), &opts, &mut log).unwrap();
    let failed = log.lines.iter().filter(|l| l.starts_with("failed "));
    assert_eq!(failed.count(), 3);
    opts.on_error = OnError::Stop;
    let mut log = Log::default();
    assert!(extract(&mut fs, &root, dest.path(), &opts, &mut log).is_err());
    assert!(!log.lines.iter().any(|l| l.starts_with("failed ")));
}

#[test]
fn existing_files_are_overwritten_skipped_or_refused() {
    let mut fs = sample();
    let top = fs.entry("/top.txt");
    let dest = tempfile::tempdir().unwrap();
    std::fs::write(dest.path().join("top.txt"), b"old").unwrap();
    let mut opts = Options::new(ResourceForkMode::AppleDouble);
    opts.existing = Existing::Skip;
    let mut log = Log::default();
    let s = extract(&mut fs, &top, dest.path(), &opts, &mut log).unwrap();
    assert_eq!(
        (s.files, log.lines.as_slice()),
        (0, &["exists top.txt".to_string()][..])
    );
    opts.existing = Existing::Fail;
    assert!(extract(&mut fs, &top, dest.path(), &opts, &mut NoHost).is_err());
    opts.existing = Existing::Overwrite;
    extract(&mut fs, &top, dest.path(), &opts, &mut NoHost).unwrap();
    assert_eq!(listing(dest.path()), ["f top.txt top"]);
    let conflicts = existing_outputs(&top, dest.path(), "top.txt", opts.fork_mode, false);
    assert_eq!(conflicts, [dest.path().join("top.txt")]);
}

#[test]
fn existing_outputs_names_a_finder_info_only_sidecar() {
    let mut fs = Mem::new(None);
    fs.file("/doc", b"x");
    let mut doc = fs.entry("/doc");
    let dest = tempfile::tempdir().unwrap();
    std::fs::write(dest.path().join("doc"), b"old").unwrap();
    std::fs::write(dest.path().join("._doc"), b"old").unwrap();
    let ad = ResourceForkMode::AppleDouble;
    assert_eq!(
        existing_outputs(&doc, dest.path(), "doc", ad, true),
        [dest.path().join("doc")]
    );
    doc.type_code = Some(*b"TEXT");
    let want = [dest.path().join("doc"), dest.path().join("._doc")];
    assert_eq!(existing_outputs(&doc, dest.path(), "doc", ad, true), want);
    doc.type_code = None;
    doc.creator_code = Some(*b"ttxt");
    assert_eq!(existing_outputs(&doc, dest.path(), "doc", ad, false), want);
}

#[test]
fn case_collisions_are_renamed_skipped_or_refused() {
    let mut fs = Mem::new(None);
    fs.file("/Read.me", b"1");
    fs.file("/READ.ME", b"2");
    let root = fs.entry("/");
    for (policy, want) in [
        (CaseCollision::Rename, vec!["f READ.ME~1 2", "f Read.me 1"]),
        (CaseCollision::Skip, vec!["f Read.me 1"]),
        (CaseCollision::Fail, vec!["f Read.me 1"]),
    ] {
        let dest = tempfile::tempdir().unwrap();
        let mut opts = Options::new(ResourceForkMode::AppleDouble);
        opts.case_collision = Some(policy);
        opts.volume_folder = false;
        let mut log = Log::default();
        extract(&mut fs, &root, dest.path(), &opts, &mut log).unwrap();
        assert_eq!(listing(dest.path()), want, "{policy:?}");
        assert!(log.lines.iter().any(|l| l.starts_with("collision READ.ME")));
    }
}

#[test]
fn forks_follow_the_mode_and_the_finder_info_choice() {
    let mut fs = Mem::new(None);
    let mut app = FileEntry::new_file("App".into(), "/App".into(), 4, 0);
    app.resource_fork_size = Some(4);
    app.type_code = Some(*b"APPL");
    fs.add(app, b"data", b"RSRC");
    let mut txt = FileEntry::new_file("Note".into(), "/Note".into(), 2, 0);
    txt.type_code = Some(*b"TEXT");
    fs.add(txt, b"hi", b"");
    let root = fs.entry("/");
    let names = |opts: &Options, fs: &mut Mem| {
        let dest = tempfile::tempdir().unwrap();
        extract(fs, &root, dest.path(), opts, &mut NoHost).unwrap();
        let v = listing(dest.path());
        v.iter()
            .map(|l| l.split(' ').nth(1).unwrap().to_string())
            .collect::<Vec<_>>()
    };
    let mut opts = Options::new(ResourceForkMode::AppleDouble);
    opts.volume_folder = false;
    assert_eq!(names(&opts, &mut fs), ["._App", "._Note", "App", "Note"]);
    opts.finder_info_sidecar = false;
    assert_eq!(names(&opts, &mut fs), ["._App", "App", "Note"]);
    opts.fork_mode = ResourceForkMode::MacBinary;
    assert_eq!(names(&opts, &mut fs), ["App.bin", "Note"]);
    opts.fork_mode = ResourceForkMode::BinHex;
    assert_eq!(names(&opts, &mut fs), ["App.hqx", "Note.hqx"]);
    opts.fork_mode = ResourceForkMode::SeparateRsrc;
    assert_eq!(names(&opts, &mut fs), ["App", "App.rsrc", "Note"]);
}

struct Upper;

impl Host for Upper {
    fn host_name(&self, entry: &FileEntry, base: String) -> String {
        match entry.is_file() {
            true => base.to_uppercase(),
            false => base,
        }
    }
    fn write_file(
        &mut self,
        fs: &mut dyn Filesystem,
        entry: &FileEntry,
        dir: &Path,
        name: &str,
    ) -> Option<Result<u64>> {
        let data = fs.read_file(entry, usize::MAX).ok()?;
        let r = std::fs::write(dir.join(format!("{name}.x")), &data).map(|_| data.len() as u64);
        Some(r.map_err(anyhow::Error::from))
    }
}

#[test]
fn the_host_names_and_writes_files() {
    let mut fs = sample();
    let docs = fs.entry("/docs");
    let dest = tempfile::tempdir().unwrap();
    let opts = Options::new(ResourceForkMode::AppleDouble);
    extract(&mut fs, &docs, dest.path(), &opts, &mut Upper).unwrap();
    assert_eq!(
        listing(dest.path()),
        [
            "d docs",
            "d docs/deep",
            "f docs/A.TXT.x alpha",
            "f docs/deep/B.TXT.x beta"
        ]
    );
}

#[test]
fn cancel_stops_at_the_next_entry_and_count_totals_the_tree() {
    let mut fs = sample();
    let root = fs.entry("/");
    assert_eq!(count(&mut fs, &root).unwrap(), (3, 12));
    let dest = tempfile::tempdir().unwrap();
    let opts = Options::new(ResourceForkMode::AppleDouble);
    let mut log = Log {
        cancel_after: Some(1),
        ..Log::default()
    };
    let err = extract(&mut fs, &root, dest.path(), &opts, &mut log).unwrap_err();
    assert!(err.is::<Cancelled>());
    assert_eq!(log.wrote, 1);
}

#[cfg(unix)]
#[test]
fn posix_modes_apply_when_asked() {
    use std::os::unix::fs::PermissionsExt;
    let mut fs = Mem::new(None);
    let mut e = FileEntry::new_file("run.sh".into(), "/run.sh".into(), 2, 0);
    e.mode = Some(0o100755);
    fs.add(e, b"#!", b"");
    let f = fs.entry("/run.sh");
    let dest = tempfile::tempdir().unwrap();
    let mut opts = Options::new(ResourceForkMode::AppleDouble);
    opts.posix_modes = true;
    extract(&mut fs, &f, dest.path(), &opts, &mut NoHost).unwrap();
    let mode = std::fs::metadata(dest.path().join("run.sh"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o755);
}
