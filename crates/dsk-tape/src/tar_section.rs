//! An editable tar archive, for rebuilding the tar sections of a tape.
//!
//! Members keep their original bytes (headers, any extension headers before them, data
//! and padding) until they change, so an untouched member round-trips exactly, SunOS V7
//! headers included. New and renamed members get V7 (pre-POSIX) headers, which every
//! tar since 1979 reads; their names are limited to 99 bytes. The archive is written
//! back with two zero blocks and padded to whole 10240-byte records, as `tar` does.
//!
//! Member bytes are byte ranges of a backing file, never copies in memory (TAP-001):
//! [`TarSection::from_file`] indexes a tar on disk, [`TarSection::add_file_from`] adds a
//! file's contents from one, and [`TarSection::write_to`] streams the rebuilt archive.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use disk_core::filesystem::FilesystemError;

const BLOCK: usize = 512;
const RECORD: usize = 20 * BLOCK;

/// Where a run of member bytes lives: `len` bytes at `at` in `file`.
#[derive(Debug, Clone)]
struct Span {
    file: Arc<File>,
    at: u64,
    len: u64,
}

impl Span {
    fn copy_to(&self, w: &mut dyn Write) -> Result<u64, FilesystemError> {
        let mut f: &File = &self.file;
        f.seek(SeekFrom::Start(self.at))?;
        let n = std::io::copy(&mut f.take(self.len), w)?;
        if n != self.len {
            return Err(invalid("tar: backing file ended early".into()));
        }
        Ok(n)
    }
}

/// One archive member.
#[derive(Debug, Clone)]
struct Member {
    /// Path as stored, without a leading `./` or `/` and without a trailing `/`.
    path: String,
    dir: bool,
    /// The member's original bytes; `None` once it has been changed or added.
    raw: Option<Span>,
    data: Option<Span>,
    mode: u32,
    uid: u64,
    gid: u64,
    mtime: u64,
    /// The original entry type byte, kept for links and other non-file members.
    kind: u8,
    link: Option<Vec<u8>>,
}

/// An editable tar archive.
#[derive(Debug, Clone)]
pub struct TarSection {
    members: Vec<Member>,
    /// Whether the archive spells its names `./x` (SunOS install tapes do); new names follow suit.
    dot_prefix: bool,
    /// Raw bytes of root members (`./`), which have no path of their own but are kept on rebuild.
    root_raw: Vec<Span>,
}

fn clean(path: &str) -> String {
    path.trim_start_matches("./").trim_matches('/').to_string()
}

fn invalid(msg: String) -> FilesystemError {
    FilesystemError::InvalidData(msg)
}

/// An anonymous temp file holding `bytes`.
fn spool(bytes: &[u8]) -> Result<File, FilesystemError> {
    let mut f = tempfile::tempfile()?;
    f.write_all(bytes)?;
    Ok(f)
}

/// Zero-pad a stream of `n` bytes so far to a multiple of `unit`; returns the bytes added.
fn pad(w: &mut dyn Write, n: u64, unit: u64) -> Result<u64, FilesystemError> {
    let add = n.div_ceil(unit) * unit - n;
    disk_core::zeros::write_zeros(w, add)?;
    Ok(add)
}

impl TarSection {
    /// Parse `bytes` (an uncompressed tar) into members.
    pub fn parse(bytes: &[u8]) -> Result<Self, FilesystemError> {
        Self::from_file(spool(bytes)?)
    }

    /// Index the uncompressed tar in `file`; members stay byte ranges of it.
    pub fn from_file(file: File) -> Result<Self, FilesystemError> {
        let file = Arc::new(file);
        let file_len = file.metadata()?.len();
        let span = |at: u64, end: u64| Span {
            file: Arc::clone(&file),
            at,
            len: end.min(file_len).saturating_sub(at),
        };
        let mut reader: &File = &file;
        reader.seek(SeekFrom::Start(0))?;
        let mut archive = tar::Archive::new(reader);
        let mut members = Vec::new();
        let mut root_raw = Vec::new();
        let mut dotted = 0usize;
        let mut prev_end = 0u64;
        let entries = archive
            .entries()
            .map_err(|e| invalid(format!("tar: {e}")))?;
        for entry in entries {
            let entry = entry.map_err(|e| invalid(format!("tar entry: {e}")))?;
            let header = entry.header().clone();
            let size = entry.size();
            let data_at = entry.raw_file_position();
            let end = data_at + size.div_ceil(BLOCK as u64) * BLOCK as u64;
            // As `bytes.get(prev_end..end.min(len))` did: `None` only when the range is inverted.
            let raw = (prev_end <= end.min(file_len)).then(|| span(prev_end, end));
            prev_end = end;
            let path_bytes = entry.path_bytes().into_owned();
            if path_bytes.starts_with(b"./") {
                dotted += 1;
            }
            let path = clean(&String::from_utf8_lossy(&path_bytes));
            let kind = header.entry_type().as_byte();
            let dir = header.entry_type().is_dir() || path_bytes.last() == Some(&b'/');
            let data = Some(span(data_at, data_at + size));
            let link = entry.link_name_bytes().map(|l| l.into_owned());
            if path.is_empty() {
                root_raw.extend(raw);
                continue;
            }
            members.push(Member {
                path,
                dir,
                raw,
                data,
                mode: header.mode().unwrap_or(if dir { 0o755 } else { 0o644 }),
                uid: header.uid().unwrap_or(0),
                gid: header.gid().unwrap_or(0),
                mtime: header.mtime().unwrap_or(0),
                kind,
                link,
            });
        }
        let dot_prefix = dotted * 2 > members.len();
        Ok(Self {
            members,
            dot_prefix,
            root_raw,
        })
    }

    /// Serialize: original bytes for untouched members, V7 headers for the rest.
    pub fn to_bytes(&self) -> Result<Vec<u8>, FilesystemError> {
        let mut out = Vec::new();
        self.write_to(&mut out)?;
        Ok(out)
    }

    /// Stream the rebuilt archive into `w`; returns its length.
    pub fn write_to(&self, w: &mut dyn Write) -> Result<u64, FilesystemError> {
        let mut n: u64 = 0;
        for raw in &self.root_raw {
            n += raw.copy_to(w)?;
        }
        for m in &self.members {
            if let Some(raw) = &m.raw {
                n += raw.copy_to(w)?;
                continue;
            }
            let dot = if self.dot_prefix { "./" } else { "" };
            let name = if m.dir {
                format!("{dot}{}/", m.path)
            } else {
                format!("{dot}{}", m.path)
            };
            if name.len() > 99 {
                return Err(invalid(format!(
                    "tar: {name} is longer than a V7 header's 99-byte name"
                )));
            }
            let mut h = tar::Header::new_old();
            h.as_old_mut().name[..name.len()].copy_from_slice(name.as_bytes());
            h.set_mode(m.mode);
            h.set_uid(m.uid);
            h.set_gid(m.gid);
            h.set_mtime(m.mtime);
            h.set_entry_type(tar::EntryType::new(m.kind));
            if let Some(link) = &m.link {
                if link.len() > 99 {
                    return Err(invalid(format!("tar: link target of {name} is too long")));
                }
                h.as_old_mut().linkname[..link.len()].copy_from_slice(link);
            }
            let body = m.data.as_ref().filter(|_| !m.dir && m.link.is_none());
            h.set_size(body.map_or(0, |b| b.len));
            h.set_cksum();
            w.write_all(h.as_bytes())?;
            n += BLOCK as u64;
            if let Some(b) = body {
                n += b.copy_to(w)?;
            }
            n += pad(w, n, BLOCK as u64)?;
        }
        w.write_all(&[0u8; 2 * BLOCK])?;
        n += 2 * BLOCK as u64;
        n += pad(w, n, RECORD as u64)?;
        Ok(n)
    }

    fn find(&self, path: &str) -> Option<usize> {
        let p = clean(path);
        self.members.iter().position(|m| m.path == p)
    }

    /// Whether `dir` is the root or names a directory (a member, or a prefix of one).
    fn dir_exists(&self, dir: &str) -> bool {
        let d = clean(dir);
        d.is_empty()
            || self
                .members
                .iter()
                .any(|m| (m.dir && m.path == d) || m.path.starts_with(&format!("{d}/")))
    }

    fn check_new(&self, path: &str) -> Result<(), FilesystemError> {
        let p = clean(path);
        // Taken by a member, or by a directory that only exists implicitly through its contents.
        if p.is_empty() || self.find(&p).is_some() || self.dir_exists(&p) {
            return Err(FilesystemError::AlreadyExists(p));
        }
        let parent = p.rsplit_once('/').map_or("", |(d, _)| d);
        if !self.dir_exists(parent) {
            return Err(FilesystemError::NotFound(parent.to_string()));
        }
        Ok(())
    }

    /// Add a regular file.
    pub fn add_file(
        &mut self,
        path: &str,
        data: Vec<u8>,
        mode: u32,
        uid: u64,
        gid: u64,
        mtime: u64,
    ) -> Result<(), FilesystemError> {
        let len = data.len() as u64;
        self.add_file_from(path, spool(&data)?, len, mode, uid, gid, mtime)
    }

    /// Add a regular file whose contents are the first `len` bytes of `file`.
    #[allow(clippy::too_many_arguments)]
    pub fn add_file_from(
        &mut self,
        path: &str,
        file: File,
        len: u64,
        mode: u32,
        uid: u64,
        gid: u64,
        mtime: u64,
    ) -> Result<(), FilesystemError> {
        self.check_new(path)?;
        self.members.push(Member {
            path: clean(path),
            dir: false,
            raw: None,
            data: Some(Span {
                file: Arc::new(file),
                at: 0,
                len,
            }),
            mode,
            uid,
            gid,
            mtime,
            kind: b'0',
            link: None,
        });
        Ok(())
    }

    /// Add a directory.
    pub fn add_dir(
        &mut self,
        path: &str,
        mode: u32,
        uid: u64,
        gid: u64,
        mtime: u64,
    ) -> Result<(), FilesystemError> {
        self.check_new(path)?;
        self.members.push(Member {
            path: clean(path),
            dir: true,
            raw: None,
            data: None,
            mode,
            uid,
            gid,
            mtime,
            kind: b'5',
            link: None,
        });
        Ok(())
    }

    /// Remove a member; a directory only when nothing is inside it.
    pub fn remove(&mut self, path: &str) -> Result<(), FilesystemError> {
        let p = clean(path);
        if self
            .members
            .iter()
            .any(|m| m.path.starts_with(&format!("{p}/")))
        {
            return Err(invalid(format!("tar: directory {p} is not empty")));
        }
        let before = self.members.len();
        self.members.retain(|m| m.path != p);
        if self.members.len() == before {
            return Err(FilesystemError::NotFound(p));
        }
        Ok(())
    }

    /// Rename a member in place; a directory takes everything inside it along.
    pub fn rename(&mut self, from: &str, to: &str) -> Result<(), FilesystemError> {
        let (from, to) = (clean(from), clean(to));
        self.check_new(&to)?;
        let mut hit = false;
        for m in &mut self.members {
            let new = if m.path == from {
                Some(to.clone())
            } else {
                m.path
                    .strip_prefix(&format!("{from}/"))
                    .map(|rest| format!("{to}/{rest}"))
            };
            if let Some(n) = new {
                m.path = n;
                m.raw = None;
                hit = true;
            }
        }
        if hit {
            Ok(())
        } else {
            Err(FilesystemError::NotFound(from))
        }
    }

    /// Change a member's mode bits.
    pub fn set_mode(&mut self, path: &str, mode: u32) -> Result<(), FilesystemError> {
        let i = self
            .find(path)
            .ok_or_else(|| FilesystemError::NotFound(clean(path)))?;
        self.members[i].mode = mode & 0o7777;
        self.members[i].raw = None;
        Ok(())
    }

    /// Change a member's owner.
    pub fn set_owner(&mut self, path: &str, uid: u64, gid: u64) -> Result<(), FilesystemError> {
        let i = self
            .find(path)
            .ok_or_else(|| FilesystemError::NotFound(clean(path)))?;
        self.members[i].uid = uid;
        self.members[i].gid = gid;
        self.members[i].raw = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// A member's contents, for assertions.
    fn data(t: &TarSection, i: usize) -> Vec<u8> {
        let mut out = Vec::new();
        if let Some(d) = &t.members[i].data {
            d.copy_to(&mut out).unwrap();
        }
        out
    }

    fn v7(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut b = tar::Builder::new(Vec::new());
        for (name, body) in entries {
            let mut h = tar::Header::new_old();
            h.set_size(body.len() as u64);
            h.set_mode(0o644);
            h.set_mtime(500_000_000);
            h.set_cksum();
            b.append_data(&mut h, name, *body).unwrap();
        }
        b.into_inner().unwrap()
    }

    #[test]
    fn a_leading_dot_slash_entry_survives_a_rebuild() {
        let mut h = tar::Header::new_old();
        h.as_old_mut().name[..2].copy_from_slice(b"./");
        h.set_entry_type(tar::EntryType::Directory);
        h.set_mode(0o755);
        h.set_size(0);
        h.set_cksum();
        let mut bytes = h.as_bytes().to_vec();
        bytes.extend_from_slice(&v7(&[("./a", b"hi")]));
        let mut sec = TarSection::parse(&bytes).unwrap();
        sec.add_file("b", b"new".to_vec(), 0o644, 0, 0, 0).unwrap();
        let out = sec.to_bytes().unwrap();
        assert_eq!(&out[..BLOCK], h.as_bytes());
    }

    #[test]
    fn untouched_members_keep_their_bytes_and_edits_read_back() {
        let original = v7(&[("etc/motd", b"hello\n"), ("etc/rc", b"#!/bin/sh\n")]);
        let mut t = TarSection::parse(&original).unwrap();
        // Unchanged: the members come back byte for byte.
        let same = t.to_bytes().unwrap();
        assert_eq!(
            &same[..original.len() - 1024],
            &original[..original.len() - 1024]
        );

        t.add_dir("usr", 0o755, 0, 0, 1).unwrap();
        t.add_file("usr/new.txt", b"new".to_vec(), 0o644, 0, 0, 1)
            .unwrap();
        t.rename("etc/motd", "etc/motd.old").unwrap();
        t.remove("etc/rc").unwrap();
        assert!(t.add_file("nodir/x", vec![], 0o644, 0, 0, 1).is_err());
        assert!(t.remove("usr").is_err(), "a non-empty directory stays");

        let bytes = t.to_bytes().unwrap();
        assert_eq!(bytes.len() % RECORD, 0);
        let fs = fs_tar::mem_archive::open_tar(bytes.clone(), None).unwrap();
        let mut fs: Box<dyn disk_core::filesystem::Filesystem> = Box::new(fs);
        let root = fs.root().unwrap();
        let names: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(names.contains(&"usr".to_string()) && names.contains(&"etc".to_string()));
        // A fresh parse sees exactly the edited member list.
        let again = TarSection::parse(&bytes).unwrap();
        let paths: Vec<&str> = again.members.iter().map(|m| m.path.as_str()).collect();
        assert_eq!(paths, ["etc/motd.old", "usr", "usr/new.txt"]);
        assert_eq!(data(&again, 2), b"new");
    }

    #[test]
    fn new_names_follow_a_dot_slash_archive() {
        // `append_data` would strip the `./`, so the names are written into the headers directly.
        let mut b = tar::Builder::new(Vec::new());
        for (name, body) in [("./etc/motd", &b"hi\n"[..]), ("./etc/rc", b"x")] {
            let mut h = tar::Header::new_old();
            h.as_old_mut().name[..name.len()].copy_from_slice(name.as_bytes());
            h.set_size(body.len() as u64);
            h.set_mode(0o644);
            h.set_cksum();
            b.append(&h, body).unwrap();
        }
        let original = b.into_inner().unwrap();
        let mut t = TarSection::parse(&original).unwrap();
        t.add_file("etc/new", b"n".to_vec(), 0o644, 0, 0, 1)
            .unwrap();
        let bytes = t.to_bytes().unwrap();
        let mut a = tar::Archive::new(Cursor::new(bytes));
        let names: Vec<String> = a
            .entries()
            .unwrap()
            .map(|e| String::from_utf8_lossy(&e.unwrap().path_bytes()).into_owned())
            .collect();
        assert_eq!(names, ["./etc/motd", "./etc/rc", "./etc/new"]);
    }
}
