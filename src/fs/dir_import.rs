//! Copy a host folder into an editable filesystem. The walker, the archive and
//! gunzip expansion and the conflict policy live in the MIT `rb-fsimporter` crate;
//! this shim keeps the app's signatures and plugs in what only the app has: Mac
//! archives (through `arc-mac`) and disk images (through the app's dispatcher).

use std::path::Path;

use anyhow::Result;
use disk_core::entry::FileEntry;
use disk_core::filesystem::EditableFilesystem;
use fs_tar::import_sink::{ImportOptions, ImportPreflight, ImportStats, Importer};

pub use rb_fsimporter::{
    projected_volume_bytes, Cancelled, DirImportOptions, DirImportPreflight, DirImportStats,
    Expander, Host, ImportConflict, NoHost,
};

/// Largest zip read whole to look for MacZip fields; MacZip archives are floppy-era small.
const MACZIP_PROBE_MAX: u64 = 256 * 1024 * 1024;

/// Mac archives and disk images, expanded the way the rest of the app opens them.
pub struct AppExpander;

impl Expander for AppExpander {
    fn is_mac_archive(&self, path: &Path) -> bool {
        super::mac_archive_import::looks_like_mac_archive(path)
    }

    fn is_maczip(&self, path: &Path) -> bool {
        let small = std::fs::metadata(path).is_ok_and(|m| m.len() <= MACZIP_PROBE_MAX);
        small
            && std::fs::read(path)
                .map(|b| arc_mac::maczip::is_maczip(b.as_slice()))
                .unwrap_or(false)
    }

    fn measure_mac_archive(&self, path: &Path) -> Result<(u64, u64, u64)> {
        super::mac_archive_import::measure_mac_archive_expanded(path)
    }

    fn import_mac_archive(
        &self,
        efs: &mut dyn EditableFilesystem,
        dest: &FileEntry,
        path: &Path,
        opts: &ImportOptions,
        progress: &dyn Fn(&ImportStats),
    ) -> Result<ImportStats> {
        super::mac_archive_import::import_mac_archive_from_path_into(
            efs, dest, path, opts, progress,
        )
    }

    fn is_disk_image(&self, path: &Path) -> bool {
        super::image_import::looks_like_disk_image(path)
    }

    fn image_has_volumes(&self, path: &Path) -> bool {
        !super::image_import::volumes(path).is_empty()
    }

    fn measure_image(&self, path: &Path) -> Result<Option<(u64, u64, u64)>> {
        super::image_import::measure_image_expanded(path)
    }

    fn import_image(
        &self,
        efs: &mut dyn EditableFilesystem,
        sink: &mut Importer,
        comps: &[String],
        path: &Path,
        opts: &ImportOptions,
        progress: &dyn Fn(&ImportStats),
    ) -> Result<ImportStats> {
        let vols = super::image_import::volumes(path);
        super::image_import::import_image_into(efs, sink, comps, path, &vols, opts, progress)
    }
}

/// A progress callback as a [`Host`] that never cancels.
struct ProgressHost<'a>(&'a dyn Fn(&ImportStats));

impl Host for ProgressHost<'_> {
    fn progress(&self, stats: &ImportStats) {
        (self.0)(stats)
    }
}

/// Import everything under `root` into `dest`; see [`rb_fsimporter::import_dir_with`].
pub fn import_dir(
    efs: &mut dyn EditableFilesystem,
    dest: &FileEntry,
    root: &Path,
    opts: &DirImportOptions,
    progress: &dyn Fn(&ImportStats),
) -> Result<ImportStats> {
    rb_fsimporter::import_dir_with(
        efs,
        dest,
        root,
        opts,
        Some(&AppExpander),
        &ProgressHost(progress),
    )
}

/// What an import would skip and write, without writing; see [`rb_fsimporter::preflight_dir_with`].
pub fn preflight_dir(
    efs: &dyn EditableFilesystem,
    root: &Path,
    opts: &DirImportOptions,
) -> Result<ImportPreflight> {
    rb_fsimporter::preflight_dir_with(efs, root, opts, Some(&AppExpander))
}

/// `(files, dirs, bytes)` under `root`, archives measured as they unpack when `expand_archives`.
pub fn measure_dir(root: &Path, expand_archives: bool, flatten: bool) -> Result<(u64, u64, u64)> {
    measure_dir_for(
        root,
        &DirImportOptions {
            expand_archives,
            flatten_archives: flatten,
            ..Default::default()
        },
    )
}

/// [`measure_dir`] driven by the import options themselves.
pub fn measure_dir_for(root: &Path, opts: &DirImportOptions) -> Result<(u64, u64, u64)> {
    rb_fsimporter::measure_dir_with(root, opts, Some(&AppExpander))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_gz(path: &Path, data: &[u8]) {
        use std::io::Write as _;
        let f = std::fs::File::create(path).unwrap();
        let mut enc = flate2::write::GzEncoder::new(f, flate2::Compression::default());
        enc.write_all(data).unwrap();
        enc.finish().unwrap();
    }

    /// Archive dates for the test fixtures: 1994-01-04 and 1998-07-14.
    const ARCHIVE_DATE: u64 = 757_641_600;
    const MEMBER_DATE: u64 = 900_374_400;

    fn blank_next_ufs() -> Box<dyn EditableFilesystem> {
        let img = fs_unix::ufs_format::create_blank_ufs1(&fs_unix::ufs_format::Ufs1FormatParams {
            size_bytes: 8 * 1024 * 1024,
            cg_layout: fs_unix::ufs::CgLayout::Bsd43,
            endian: fs_unix::ufs::UfsEndian::Big,
            ..Default::default()
        })
        .unwrap();
        crate::fs::open_editable_filesystem(std::io::Cursor::new(img), 0, 0, None).unwrap()
    }

    fn entry_at(efs: &mut dyn EditableFilesystem, path: &[&str]) -> FileEntry {
        let mut e = efs.root().unwrap();
        for name in path {
            e = efs
                .list_directory(&e)
                .unwrap()
                .into_iter()
                .find(|c| c.name == *name)
                .unwrap_or_else(|| panic!("{name} missing"));
        }
        e
    }

    fn set_mtime(path: &Path, secs: u64) {
        let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        f.set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs))
            .unwrap();
    }

    /// Directories an archive never dates take the archive's own date; its files keep theirs.
    #[test]
    fn expanded_archive_directories_take_the_archive_date() {
        let tmp = tempfile::tempdir().unwrap();
        let tgz = tmp.path().join("pkg.tar.gz");
        {
            let f = std::fs::File::create(&tgz).unwrap();
            let enc = flate2::write::GzEncoder::new(f, flate2::Compression::default());
            let mut b = tar::Builder::new(enc);
            let mut h = tar::Header::new_gnu();
            h.set_size(3);
            h.set_mode(0o644);
            h.set_mtime(MEMBER_DATE);
            h.set_cksum();
            // No entries for `deep/` or `deep/er/`: both are implicit parents.
            b.append_data(&mut h, "deep/er/one.txt", &b"one"[..])
                .unwrap();
            let mut u = tar::Header::new_gnu();
            u.set_size(3);
            u.set_mode(0o644);
            u.set_mtime(0);
            u.set_cksum();
            b.append_data(&mut u, "deep/undated.txt", &b"two"[..])
                .unwrap();
            b.into_inner().unwrap().finish().unwrap();
        }
        set_mtime(&tgz, ARCHIVE_DATE);

        let mut efs = blank_next_ufs();
        let root = efs.root().unwrap();
        let opts = DirImportOptions {
            expand_archives: true,
            ..Default::default()
        };
        import_dir(&mut *efs, &root, tmp.path(), &opts, &|_| {}).unwrap();
        for dir in [&["pkg"][..], &["pkg", "deep"], &["pkg", "deep", "er"]] {
            assert_eq!(
                entry_at(&mut *efs, dir).modified_unix,
                Some(ARCHIVE_DATE),
                "{dir:?}"
            );
        }
        let file = entry_at(&mut *efs, &["pkg", "deep", "er", "one.txt"]);
        assert_eq!(file.modified_unix, Some(MEMBER_DATE));
        let undated = entry_at(&mut *efs, &["pkg", "deep", "undated.txt"]);
        assert_eq!(
            undated.modified_unix,
            Some(ARCHIVE_DATE),
            "mtime 0 takes the archive date"
        );
    }

    /// --expand-gunzip dates the output from the gzip header, else from the .gz file.
    #[test]
    fn gunzipped_files_keep_the_original_date() {
        use std::io::Write as _;
        let tmp = tempfile::tempdir().unwrap();
        let stamped = tmp.path().join("notes.txt.gz");
        let f = std::fs::File::create(&stamped).unwrap();
        let mut enc = flate2::GzBuilder::new()
            .mtime(MEMBER_DATE as u32)
            .write(f, flate2::Compression::default());
        enc.write_all(b"dated").unwrap();
        enc.finish().unwrap();
        set_mtime(&stamped, ARCHIVE_DATE);
        let plain = tmp.path().join("plain.bin.gz");
        write_gz(&plain, b"undated");
        set_mtime(&plain, ARCHIVE_DATE);

        let mut efs = blank_next_ufs();
        let root = efs.root().unwrap();
        let opts = DirImportOptions {
            expand_gunzip: true,
            ..Default::default()
        };
        import_dir(&mut *efs, &root, tmp.path(), &opts, &|_| {}).unwrap();
        assert_eq!(
            entry_at(&mut *efs, &["notes.txt"]).modified_unix,
            Some(MEMBER_DATE)
        );
        assert_eq!(
            entry_at(&mut *efs, &["plain.bin"]).modified_unix,
            Some(ARCHIVE_DATE)
        );
    }

    fn blank_hfs() -> Box<dyn EditableFilesystem> {
        let img = fs_mac::hfs::create_blank_hfs(8 * 1024 * 1024, 512, "T").unwrap();
        crate::fs::open_editable_filesystem(std::io::Cursor::new(img), 0, 0, None).unwrap()
    }

    fn names(efs: &mut dyn EditableFilesystem, path: &[&str]) -> Vec<String> {
        let dir = entry_at(efs, path);
        let mut v: Vec<String> = efs
            .list_directory(&dir)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        v.sort();
        v
    }

    /// A zip the way macOS Finder writes one: the fork in `__MACOSX/Docs/._HELLO.TXT`.
    fn write_finder_zip(path: &Path) {
        use std::io::Write as _;
        let o = zip::write::SimpleFileOptions::default();
        let mut w = zip::ZipWriter::new(std::fs::File::create(path).unwrap());
        w.start_file("Docs/HELLO.TXT", o).unwrap();
        w.write_all(b"hello").unwrap();
        w.start_file("__MACOSX/Docs/._HELLO.TXT", o).unwrap();
        let ad = fs_mac::resource_fork::build_appledouble(
            b"TEXT",
            b"ttxt",
            fs_mac::resource_fork::MacFileDates::default(),
            b"RSRC",
        );
        w.write_all(&ad).unwrap();
        w.finish().unwrap();
    }

    fn opts(archives: bool, images: bool) -> DirImportOptions {
        DirImportOptions {
            expand_archives: archives,
            expand_disk_images: images,
            ..Default::default()
        }
    }

    #[test]
    fn a_finder_zip_expands_with_its_forks_and_no_macosx_folder() {
        let tmp = tempfile::tempdir().unwrap();
        write_finder_zip(&tmp.path().join("app.zip"));
        let mut efs = blank_hfs();
        let root = efs.root().unwrap();
        let stats = import_dir(&mut *efs, &root, tmp.path(), &opts(true, false), &|_| {}).unwrap();
        efs.sync_metadata().unwrap();
        assert_eq!(stats.archives_expanded, 1);
        assert_eq!(names(&mut *efs, &["app"]), ["Docs"]);
        let hello = entry_at(&mut *efs, &["app", "Docs", "HELLO.TXT"]);
        assert_eq!(hello.resource_fork_size, Some(4), "{hello:?}");
        assert_eq!(hello.type_code, Some(*b"TEXT"));
        let (files, _, bytes) = measure_dir_for(tmp.path(), &opts(true, false)).unwrap();
        assert_eq!(files, 1);
        assert!(bytes >= 5, "content and fork are measured: {bytes}");
    }

    #[test]
    fn an_applesingle_file_lands_as_a_forked_file_under_its_mac_name() {
        let tmp = tempfile::tempdir().unwrap();
        let mut finfo = [0u8; 16];
        finfo[0..4].copy_from_slice(b"TEXT");
        finfo[4..8].copy_from_slice(b"ttxt");
        let entries: [(u32, &[u8]); 4] =
            [(1, b"hello"), (2, b"RSRC"), (3, b"Read Me"), (9, &finfo)];
        let mut bytes = vec![0x00, 0x05, 0x16, 0x00, 0x00, 0x02, 0x00, 0x00];
        bytes.extend_from_slice(&[0u8; 16]);
        bytes.extend_from_slice(&(entries.len() as u16).to_be_bytes());
        let mut offset = 26 + 12 * entries.len() as u32;
        for (id, body) in entries {
            bytes.extend_from_slice(&id.to_be_bytes());
            bytes.extend_from_slice(&offset.to_be_bytes());
            bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
            offset += body.len() as u32;
        }
        for (_, body) in entries {
            bytes.extend_from_slice(body);
        }
        std::fs::write(tmp.path().join("readme.as"), bytes).unwrap();
        let mut efs = blank_hfs();
        let root = efs.root().unwrap();
        import_dir(&mut *efs, &root, tmp.path(), &opts(false, false), &|_| {}).unwrap();
        efs.sync_metadata().unwrap();
        let f = entry_at(&mut *efs, &["Read Me"]);
        assert_eq!((f.size, f.resource_fork_size), (5, Some(4)), "{f:?}");
    }

    /// A FAT floppy image holding one file.
    fn write_fat_image(path: &Path) {
        let img = fs_pc::fat::create_blank_fat(1440 * 1024, Some("FLOPPY")).unwrap();
        std::fs::write(path, img).unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let mut fs = crate::fs::open_editable_filesystem(file, 0, 0, None).unwrap();
        let root = fs.root().unwrap();
        let mut data: &[u8] = b"from the floppy";
        fs.create_file(&root, "HELLO.TXT", &mut data, 15, &Default::default())
            .unwrap();
        fs.sync_metadata().unwrap();
    }

    #[test]
    fn a_disk_image_expands_into_a_folder_and_junk_stays_a_file() {
        let tmp = tempfile::tempdir().unwrap();
        write_fat_image(&tmp.path().join("floppy.img"));
        std::fs::write(tmp.path().join("junk.img"), vec![0x5au8; 4096]).unwrap();

        // Without the option an image is just a file.
        let mut efs = blank_next_ufs();
        let root = efs.root().unwrap();
        import_dir(&mut *efs, &root, tmp.path(), &opts(true, false), &|_| {}).unwrap();
        assert_eq!(names(&mut *efs, &[]), ["floppy.img", "junk.img"]);

        let mut efs = blank_next_ufs();
        let root = efs.root().unwrap();
        let stats = import_dir(&mut *efs, &root, tmp.path(), &opts(false, true), &|_| {}).unwrap();
        efs.sync_metadata().unwrap();
        assert_eq!(stats.archives_expanded, 1);
        assert_eq!(names(&mut *efs, &[]), ["floppy", "junk.img"]);
        let mut out = Vec::new();
        let hello = entry_at(&mut *efs, &["floppy", "HELLO.TXT"]);
        efs.write_file_to(&hello, &mut out).unwrap();
        assert_eq!(out, b"from the floppy");
        let (files, _, _) = measure_dir_for(tmp.path(), &opts(false, true)).unwrap();
        assert_eq!(files, 2, "the floppy's file plus junk.img");
    }
}
