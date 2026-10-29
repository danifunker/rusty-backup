//! Editing Sun QIC tape images (`SUN2TAPE`): rebuild the whole tape, keep the original.
//!
//! A tape has no free space to allocate from, so editing works like SquashFS: every
//! tape file is held in memory (a QIC cartridge is at most ~150 MB), an edit rewrites
//! the one tape file it touches, and `sync_metadata` writes a new image. Tar sections
//! are edited as member lists ([`super::tar_section`]); `compress(1)`-ed ones are
//! decompressed, edited and recompressed; the UFS miniroot goes through the UFS writer.
//! Boot blocks, the table of contents and other plain tape files are copied as they are,
//! and the tape-file table in block 0 is rewritten for the new sizes.
//!
//! The rebuilt tape is written beside the original first; only then is the original
//! renamed to `<name>.bak` (`.bak.1`, `.bak.2`, ... when that exists) and the new tape
//! moved into its name, so a failure at any point leaves the original in place.
//!
//! Not verified on Sun hardware yet: the plan (docs/PLAN-hpux-writes.md, W4) is a Sun-2 /
//! Sun-3 MiSTer core booting and listing an edited tape.

use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
use super::sun2tape::{TapeFileKind, TapeFilesystem};
use super::tar_section::TarSection;
use super::ufs::UfsFilesystem;

const BLOCK: usize = 512;

/// One tape file: where it sits in the table and its (possibly edited) bytes.
struct TapeBody {
    volume: usize,
    kind: TapeFileKind,
    bytes: Vec<u8>,
}

/// An editable `SUN2TAPE` image, rebuilt on sync.
pub struct TapeEditor {
    path: PathBuf,
    head: Vec<u8>,
    volumes: usize,
    bodies: Vec<TapeBody>,
    /// Display node path of each tape file (`/NN-name` or `/volN/NN-name`).
    nodes: Vec<String>,
    view: Option<TapeFilesystem<Cursor<Vec<u8>>>>,
    dirty: bool,
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn parent_of(path: &str) -> &str {
    match path.rsplit_once('/') {
        Some(("", _)) | None => "/",
        Some((p, _)) => p,
    }
}

/// Walk `path` from the root of `fs`.
fn resolve(fs: &mut dyn Filesystem, path: &str) -> Result<FileEntry, FilesystemError> {
    let mut cur = fs.root()?;
    for part in path.split('/').filter(|p| !p.is_empty()) {
        cur = fs
            .list_directory(&cur)?
            .into_iter()
            .find(|e| e.name == part)
            .ok_or_else(|| FilesystemError::NotFound(path.to_string()))?;
    }
    Ok(cur)
}

impl TapeEditor {
    /// Load a tape for editing. `path` must be the image file itself, which is replaced on sync.
    pub fn open<R: Read + Seek>(
        mut reader: R,
        offset: u64,
        path: Option<&Path>,
    ) -> Result<Self, FilesystemError> {
        let path = path.ok_or_else(|| {
            FilesystemError::Unsupported(
                "Sun tape: edit a .qic as a standalone file (it is rebuilt and replaced)".into(),
            )
        })?;
        reader.seek(SeekFrom::Start(offset))?;
        let mut image = Vec::new();
        reader.read_to_end(&mut image)?;
        let view = TapeFilesystem::open(Cursor::new(image.clone()), 0)?;
        let volumes = u32::from_be_bytes([image[12], image[13], image[14], image[15]]) as usize;
        let mut bodies = Vec::new();
        let mut nodes = Vec::new();
        for f in view.files() {
            let end = (f.start + f.len) as usize;
            bodies.push(TapeBody {
                volume: f.volume,
                kind: f.kind,
                bytes: image
                    .get(f.start as usize..end)
                    .ok_or_else(|| {
                        FilesystemError::InvalidData("Sun tape: file past the image".into())
                    })?
                    .to_vec(),
            });
            nodes.push(if volumes > 1 {
                format!("/vol{}/{}", f.volume + 1, f.name)
            } else {
                format!("/{}", f.name)
            });
        }
        Ok(Self {
            path: path.to_path_buf(),
            head: image[..BLOCK].to_vec(),
            volumes,
            bodies,
            nodes,
            view: Some(view),
            dirty: false,
        })
    }

    /// The whole tape: block 0's table rewritten for the current file sizes, then the files.
    pub fn build_image(&self) -> Result<Vec<u8>, FilesystemError> {
        let mut head = self.head.clone();
        let mut out = vec![0u8; BLOCK];
        let mut block = 1u32;
        for v in 0..self.volumes {
            let files: Vec<&TapeBody> = self.bodies.iter().filter(|b| b.volume == v).collect();
            let w = 4 + 32 * v;
            if files.len() + 1 > 31 {
                return Err(FilesystemError::InvalidData(
                    "Sun tape: too many files in a volume".into(),
                ));
            }
            let mut put = |word: usize, value: u32| {
                head[word * 4..word * 4 + 4].copy_from_slice(&value.to_be_bytes());
            };
            put(w, files.len() as u32);
            put(w + 1, block);
            for (k, f) in files.iter().enumerate() {
                out.extend_from_slice(&f.bytes);
                out.resize(out.len().div_ceil(BLOCK) * BLOCK, 0);
                block = (out.len() / BLOCK) as u32;
                put(w + 2 + k, block);
            }
        }
        out[..BLOCK].copy_from_slice(&head);
        Ok(out)
    }

    fn view(&mut self) -> Result<&mut TapeFilesystem<Cursor<Vec<u8>>>, FilesystemError> {
        if self.view.is_none() {
            self.view = Some(TapeFilesystem::open(Cursor::new(self.build_image()?), 0)?);
        }
        Ok(self.view.as_mut().expect("just built"))
    }

    /// The tape file a path lies in and the path inside it; editing a tape file itself is refused.
    fn locate(&self, path: &str) -> Result<(usize, String), FilesystemError> {
        for (i, node) in self.nodes.iter().enumerate() {
            if let Some(rest) = path.strip_prefix(node.as_str()) {
                if rest.is_empty() {
                    return Ok((i, "/".to_string()));
                }
                if rest.starts_with('/') {
                    return Ok((i, rest.to_string()));
                }
            }
        }
        Err(FilesystemError::Unsupported(format!(
            "Sun tape: {path} is not inside a tar or UFS tape file; tape files themselves are not added or removed"
        )))
    }

    /// Apply `edit` to tape file `i`, through its tar member list or its UFS volume.
    fn edit(
        &mut self,
        i: usize,
        tar: impl FnOnce(&mut TarSection) -> Result<(), FilesystemError>,
        ufs: impl FnOnce(&mut UfsFilesystem<Cursor<Vec<u8>>>) -> Result<(), FilesystemError>,
    ) -> Result<(), FilesystemError> {
        let body = &mut self.bodies[i];
        match body.kind {
            TapeFileKind::Tar | TapeFileKind::CompressedTar => {
                let packed = body.kind == TapeFileKind::CompressedTar;
                let plain = if packed {
                    crate::macarchive::stuffit_lzw::decompress_unix_z(&body.bytes)
                        .map_err(|e| FilesystemError::InvalidData(format!("Sun tape: {e}")))?
                } else {
                    body.bytes.clone()
                };
                let mut section = TarSection::parse(&plain)?;
                tar(&mut section)?;
                let rebuilt = section.to_bytes()?;
                body.bytes = if packed {
                    crate::macarchive::stuffit_lzw::compress_unix_z(&rebuilt)
                } else {
                    rebuilt
                };
            }
            TapeFileKind::Ufs => {
                let mut fs = UfsFilesystem::open(Cursor::new(std::mem::take(&mut body.bytes)), 0)?;
                let r = ufs(&mut fs);
                fs.sync_metadata()?;
                body.bytes = fs.into_inner().into_inner();
                r?;
            }
            _ => {
                return Err(FilesystemError::Unsupported(
                    "Sun tape: only tar, compressed tar and UFS tape files can be edited".into(),
                ))
            }
        }
        self.view = None;
        self.dirty = true;
        Ok(())
    }

    /// An entry by its full path, after an edit.
    fn entry_at(&mut self, path: &str) -> Result<FileEntry, FilesystemError> {
        resolve(self.view()?, path)
    }

    /// Rename the original to the first free `<name>.bak[.N]`, then move `new` into its name.
    fn replace_original(&self, new: &Path) -> Result<PathBuf, FilesystemError> {
        let name = self
            .path
            .file_name()
            .ok_or_else(|| FilesystemError::InvalidData("Sun tape: no file name".into()))?
            .to_string_lossy()
            .into_owned();
        let mut backup = self.path.with_file_name(format!("{name}.bak"));
        let mut n = 1;
        while backup.exists() {
            backup = self.path.with_file_name(format!("{name}.bak.{n}"));
            n += 1;
        }
        std::fs::rename(&self.path, &backup)?;
        if let Err(e) = std::fs::rename(new, &self.path) {
            // Put the original back rather than leave no tape under its name.
            let _ = std::fs::rename(&backup, &self.path);
            return Err(e.into());
        }
        Ok(backup)
    }
}

impl Filesystem for TapeEditor {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        self.view()?.root()
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        self.view()?.list_directory(entry)
    }

    fn read_file(&mut self, entry: &FileEntry, max: usize) -> Result<Vec<u8>, FilesystemError> {
        self.view()?.read_file(entry, max)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        w: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        self.view()?.write_file_to(entry, w)
    }

    fn volume_label(&self) -> Option<&str> {
        None
    }

    fn fs_type(&self) -> &str {
        "Sun QIC tape"
    }

    fn total_size(&self) -> u64 {
        self.bodies
            .iter()
            .map(|b| b.bytes.len() as u64)
            .sum::<u64>()
            + BLOCK as u64
    }

    fn used_size(&self) -> u64 {
        self.total_size()
    }
}

impl EditableFilesystem for TapeEditor {
    fn as_filesystem(&self) -> &dyn Filesystem {
        self
    }

    fn as_filesystem_mut(&mut self) -> &mut dyn Filesystem {
        self
    }

    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn Read,
        data_len: u64,
        options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let (i, inner_parent) = self.locate(&parent.path)?;
        let mut body = Vec::with_capacity(data_len as usize);
        data.take(data_len).read_to_end(&mut body)?;
        let inner = format!("{}/{name}", inner_parent.trim_end_matches('/'));
        let mode = options.mode.unwrap_or(0o644) & 0o7777;
        let (uid, gid) = (options.uid.unwrap_or(0), options.gid.unwrap_or(0));
        let mtime = options.unix_times.and_then(|t| t.mtime).unwrap_or_else(now);
        let ufs_body = body.clone();
        self.edit(
            i,
            |t| t.add_file(&inner, body, mode, uid.into(), gid.into(), mtime),
            |u| {
                let p = resolve(u, &inner_parent)?;
                u.create_file(&p, name, &mut &ufs_body[..], ufs_body.len() as u64, options)
                    .map(|_| ())
            },
        )?;
        self.entry_at(&format!("{}/{name}", parent.path.trim_end_matches('/')))
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let (i, inner_parent) = self.locate(&parent.path)?;
        let inner = format!("{}/{name}", inner_parent.trim_end_matches('/'));
        let mode = options.mode.unwrap_or(0o755) & 0o7777;
        let (uid, gid) = (options.uid.unwrap_or(0), options.gid.unwrap_or(0));
        self.edit(
            i,
            |t| t.add_dir(&inner, mode, uid.into(), gid.into(), now()),
            |u| {
                let p = resolve(u, &inner_parent)?;
                u.create_directory(&p, name, options).map(|_| ())
            },
        )?;
        self.entry_at(&format!("{}/{name}", parent.path.trim_end_matches('/')))
    }

    fn delete_entry(
        &mut self,
        _parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        let (i, inner) = self.locate(&entry.path)?;
        if inner == "/" {
            return Err(FilesystemError::Unsupported(
                "Sun tape: tape files themselves are not removed".into(),
            ));
        }
        self.edit(
            i,
            |t| t.remove(&inner),
            |u| {
                let p = resolve(u, parent_of(&inner))?;
                let e = resolve(u, &inner)?;
                u.delete_entry(&p, &e)
            },
        )
    }

    fn rename(
        &mut self,
        _parent: &FileEntry,
        entry: &FileEntry,
        new_name: &str,
    ) -> Result<(), FilesystemError> {
        let (i, inner) = self.locate(&entry.path)?;
        if inner == "/" {
            return Err(FilesystemError::Unsupported(
                "Sun tape: tape files are named by the tape's table of contents".into(),
            ));
        }
        let to = format!("{}/{new_name}", parent_of(&inner).trim_end_matches('/'));
        self.edit(
            i,
            |t| t.rename(&inner, &to),
            |u| {
                let p = resolve(u, parent_of(&inner))?;
                let e = resolve(u, &inner)?;
                u.rename(&p, &e, new_name)
            },
        )
    }

    fn set_permissions(&mut self, entry: &FileEntry, mode: u32) -> Result<(), FilesystemError> {
        let (i, inner) = self.locate(&entry.path)?;
        self.edit(
            i,
            |t| t.set_mode(&inner, mode),
            |u| {
                let e = resolve(u, &inner)?;
                u.set_permissions(&e, mode)
            },
        )
    }

    fn set_owner(&mut self, entry: &FileEntry, uid: u32, gid: u32) -> Result<(), FilesystemError> {
        let (i, inner) = self.locate(&entry.path)?;
        self.edit(
            i,
            |t| t.set_owner(&inner, uid.into(), gid.into()),
            |u| {
                let e = resolve(u, &inner)?;
                u.set_owner(&e, uid, gid)
            },
        )
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        if !self.dirty {
            return Ok(());
        }
        let image = self.build_image()?;
        let dir = self
            .path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let mut tmp = tempfile::Builder::new()
            .prefix(".rb-tape-")
            .tempfile_in(dir)?;
        tmp.write_all(&image)?;
        tmp.as_file().sync_all()?;
        // A fresh temp file is 0600; the rebuilt tape keeps the original's permissions.
        if let Ok(meta) = std::fs::metadata(&self.path) {
            let _ = std::fs::set_permissions(tmp.path(), meta.permissions());
        }
        let (_, tmp_path) = tmp.keep().map_err(|e| FilesystemError::Io(e.error))?;
        let backup = self.replace_original(&tmp_path)?;
        log::info!("Sun tape: original kept as {}", backup.display());
        self.dirty = false;
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        // The tape is rebuilt to whatever size its contents need.
        Ok(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tar_member(name: &str, body: &[u8]) -> Vec<u8> {
        let mut b = tar::Builder::new(Vec::new());
        let mut h = tar::Header::new_old();
        h.set_size(body.len() as u64);
        h.set_mode(0o644);
        h.set_cksum();
        b.append_data(&mut h, name, body).unwrap();
        b.into_inner().unwrap()
    }

    /// One volume: a text file, a V7 tar and a compressed tar.
    fn sample() -> Vec<u8> {
        let mut img = vec![0u8; BLOCK];
        img[0..8].copy_from_slice(b"SUN2TAPE");
        img[8..12].copy_from_slice(&1u32.to_be_bytes());
        img[12..16].copy_from_slice(&1u32.to_be_bytes());
        let files = [
            b"SunOS 4.1.1 tape\n".to_vec(),
            tar_member("etc/motd", b"hello\n"),
            crate::macarchive::stuffit_lzw::compress_unix_z(&tar_member("usr/bin/ls", b"ls")),
        ];
        let mut blocks = vec![1u32];
        let mut body = Vec::new();
        for f in &files {
            body.extend_from_slice(f);
            body.resize(body.len().div_ceil(BLOCK) * BLOCK, 0);
            blocks.push(1 + (body.len() / BLOCK) as u32);
        }
        img[16..20].copy_from_slice(&(files.len() as u32).to_be_bytes());
        for (k, b) in blocks.iter().enumerate() {
            img[20 + k * 4..24 + k * 4].copy_from_slice(&b.to_be_bytes());
        }
        img.extend_from_slice(&body);
        img
    }

    #[test]
    fn edits_rebuild_the_tape_and_keep_a_backup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.qic");
        let original = sample();
        std::fs::write(&path, &original).unwrap();
        let mut ed = TapeEditor::open(Cursor::new(original.clone()), 0, Some(&path)).unwrap();
        let root = ed.root().unwrap();
        let top = ed.list_directory(&root).unwrap();
        let names: Vec<&str> = top.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, ["00-text.txt", "01-archive.tar", "02-archive.tar.Z"]);

        // A plain tar: add a file and a directory, rename and remove.
        let etc = resolve(&mut ed, "/01-archive.tar/etc").unwrap();
        let big: Vec<u8> = (0..5000u32).map(|i| i as u8).collect();
        let f = ed
            .create_file(
                &etc,
                "added",
                &mut &big[..],
                big.len() as u64,
                &Default::default(),
            )
            .unwrap();
        assert_eq!(ed.read_file(&f, usize::MAX).unwrap(), big);
        ed.create_directory(&etc, "sub", &Default::default())
            .unwrap();
        let motd = resolve(&mut ed, "/01-archive.tar/etc/motd").unwrap();
        ed.rename(&etc, &motd, "motd.old").unwrap();
        // The compressed tar is decompressed, edited and recompressed.
        let bin = resolve(&mut ed, "/02-archive.tar.Z/usr/bin").unwrap();
        ed.create_file(&bin, "cat", &mut &b"cat"[..], 3, &Default::default())
            .unwrap();
        // Tape files themselves cannot be added or removed.
        assert!(ed.delete_entry(&root, &top[0]).is_err());
        ed.sync_metadata().unwrap();

        assert_eq!(
            std::fs::read(dir.path().join("t.qic.bak")).unwrap(),
            original
        );
        let rebuilt = std::fs::read(&path).unwrap();
        let mut tape = TapeFilesystem::open(Cursor::new(rebuilt), 0).unwrap();
        let get = |t: &mut TapeFilesystem<Cursor<Vec<u8>>>, p: &str| {
            let e = resolve(t, p).unwrap();
            t.read_file(&e, usize::MAX).unwrap()
        };
        assert_eq!(get(&mut tape, "/01-archive.tar/etc/added"), big);
        assert_eq!(get(&mut tape, "/01-archive.tar/etc/motd.old"), b"hello\n");
        assert_eq!(get(&mut tape, "/02-archive.tar.Z/usr/bin/cat"), b"cat");
        assert_eq!(get(&mut tape, "/02-archive.tar.Z/usr/bin/ls"), b"ls");
        assert!(get(&mut tape, "/00-text.txt").starts_with(b"SunOS"));

        // A second sync never overwrites the first backup.
        let mut ed =
            TapeEditor::open(Cursor::new(std::fs::read(&path).unwrap()), 0, Some(&path)).unwrap();
        let etc = resolve(&mut ed, "/01-archive.tar/etc").unwrap();
        ed.create_directory(&etc, "again", &Default::default())
            .unwrap();
        ed.sync_metadata().unwrap();
        assert!(dir.path().join("t.qic.bak.1").exists());
        assert_eq!(
            std::fs::read(dir.path().join("t.qic.bak")).unwrap(),
            original
        );
    }

    #[test]
    fn a_tape_opened_without_its_own_file_is_refused() {
        assert!(TapeEditor::open(Cursor::new(sample()), 0, None).is_err());
    }
}
