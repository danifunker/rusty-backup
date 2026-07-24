//! Structural verification of a SquashFS image.
//!
//! **Not an fsck.** SquashFS carries no checksums anywhere — not on the
//! superblock, not on a metadata block, not on a data block — so there is
//! nothing to check content *against* and nothing to repair *from*. What can
//! go wrong is structural: a metadata block that won't decompress, an inode
//! reference that points outside the table, a directory entry naming an inode
//! that isn't there, a data block whose declared compressed length overruns the
//! image. Every one of those surfaces the same way — the reader fails when it
//! tries to follow the broken link.
//!
//! So verification is a **complete traversal that touches every byte the image
//! claims to hold**: walk the whole directory tree (which decompresses the
//! inode and directory tables and resolves every dirent), read every file to
//! the end (which decompresses every data block and fragment), and read every
//! entry's extended attributes (which cross-references the xattr table). If the
//! image is sound the walk completes; if it is not, the first broken link is
//! reported with the path where it was found.

use std::io::{Read, Seek};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{Filesystem, FilesystemError};
use super::squashfs::SquashfsFilesystem;

/// What a clean traversal counted. A tally, not a judgement: reaching the end at
/// all is the pass, and the numbers are there so a caller can show that the
/// image really was walked rather than trivially accepted.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SquashfsVerifyReport {
    pub files: u64,
    pub directories: u64,
    pub symlinks: u64,
    /// Device nodes, FIFOs and sockets.
    pub special: u64,
    /// Total uncompressed data-fork bytes read back and discarded.
    pub data_bytes: u64,
    /// Extended attributes read across all entries.
    pub xattrs: u64,
}

/// A writer that counts and discards — so reading a file to the end decompresses
/// every block without the verifier holding a whole file in memory.
struct CountingSink(u64);

impl std::io::Write for CountingSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0 += buf.len() as u64;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Walk `fs` in full, decompressing everything, and return the tally — or the
/// first structural failure, with the path at which it was found.
///
/// The reader is the authority on what "broken" means: this drives it over the
/// entire image and reports the point where it first could not follow the
/// structure.
pub fn verify_squashfs<R: Read + Seek + Send>(
    fs: &mut SquashfsFilesystem<R>,
) -> Result<SquashfsVerifyReport, FilesystemError> {
    let mut report = SquashfsVerifyReport::default();
    let root = fs.root()?;
    // Iterative walk with an explicit stack: a distro rootfs is shallow, but a
    // hostile or damaged image could nest deeply enough to blow a recursive
    // one, and the verifier of all things must not itself crash on bad input.
    let mut stack = vec![root];
    while let Some(entry) = stack.pop() {
        verify_entry(fs, &entry, &mut stack, &mut report)?;
    }
    Ok(report)
}

/// Verify one entry, pushing any child directories onto `stack`.
fn verify_entry<R: Read + Seek + Send>(
    fs: &mut SquashfsFilesystem<R>,
    entry: &FileEntry,
    stack: &mut Vec<FileEntry>,
    report: &mut SquashfsVerifyReport,
) -> Result<(), FilesystemError> {
    // Every entry's xattrs cross-reference the xattr table; reading them is part
    // of touching the whole structure.
    let context = |e: FilesystemError, what: &str| {
        FilesystemError::InvalidData(format!("squashfs verify: {what} at '{}': {e}", entry.path))
    };
    report.xattrs += fs
        .list_xattrs(entry)
        .map_err(|e| context(e, "reading extended attributes"))?
        .len() as u64;

    match entry.entry_type {
        EntryType::Directory => {
            report.directories += 1;
            let children = fs
                .list_directory(entry)
                .map_err(|e| context(e, "listing directory"))?;
            stack.extend(children);
        }
        EntryType::File => {
            report.files += 1;
            // Read the whole file, discarding the bytes: this is what forces
            // every data block and every shared fragment to decompress.
            let mut sink = CountingSink(0);
            fs.write_file_to(entry, &mut sink)
                .map_err(|e| context(e, "reading file data"))?;
            report.data_bytes += sink.0;
        }
        EntryType::Symlink => report.symlinks += 1,
        EntryType::Special => report.special += 1,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::squashfs_write::{
        write_squashfs, BuildKind, BuildNode, BuildOptions, FileContent,
    };
    use crate::fs::xattr::Xattr;
    use std::io::Cursor;

    fn sample_image() -> Vec<u8> {
        let tree = BuildNode {
            name: String::new(),
            mode: 0o755,
            uid: 0,
            gid: 0,
            mtime: 0,
            xattrs: Vec::new(),
            kind: BuildKind::Dir(vec![
                BuildNode::file("readme", 0o644, b"hello\n".to_vec()),
                BuildNode::symlink("link", "readme"),
                BuildNode::dir(
                    "bin",
                    0o755,
                    vec![BuildNode {
                        name: "ping".into(),
                        mode: 0o755,
                        uid: 0,
                        gid: 0,
                        mtime: 0,
                        xattrs: vec![Xattr {
                            name: "security.capability".into(),
                            value: vec![1, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0],
                        }],
                        // Compressible, so its data blocks are gzip-compressed
                        // — a corrupt-block test needs a block that *decompresses*,
                        // and an incompressible file is stored raw (no checksum,
                        // nothing to fail; that undetectability is by design).
                        kind: BuildKind::File(FileContent::Bytes(vec![0u8; 300_000])),
                    }],
                ),
            ]),
        };
        let mut cur = Cursor::new(Vec::new());
        write_squashfs(&mut cur, &tree, &BuildOptions::default()).expect("build");
        cur.into_inner()
    }

    #[test]
    fn a_sound_image_verifies_and_counts_everything() {
        let mut fs = SquashfsFilesystem::open(Cursor::new(sample_image()), 0).expect("open");
        let report = verify_squashfs(&mut fs).expect("must verify clean");
        assert_eq!(report.files, 2, "readme + bin/ping");
        assert_eq!(report.directories, 2, "root + bin");
        assert_eq!(report.symlinks, 1);
        assert_eq!(report.data_bytes, 6 + 300_000, "every data block read back");
        assert_eq!(report.special, 0);
        assert_eq!(report.xattrs, 1, "ping's capability");
    }

    /// Corrupting a data block's bytes must be caught: a multi-block file whose
    /// compressed payload no longer decompresses fails when the verifier reads
    /// it, even though nothing else in the image is wrong.
    #[test]
    fn a_corrupt_data_block_is_reported_with_its_path() {
        let mut img = sample_image();
        // The compressed data blocks are written right after the 96-byte
        // superblock, while the tables the reader needs to *open* the image live
        // at the end. Smearing a run just past the superblock corrupts a data
        // block without touching those tables — so the image opens but the file
        // no longer inflates, which is exactly the case only a full traversal
        // catches.
        for b in &mut img[100..200] {
            *b ^= 0xFF;
        }
        let mut fs = SquashfsFilesystem::open(Cursor::new(img), 0)
            .expect("the tables are intact, so open must still succeed");
        let err = verify_squashfs(&mut fs).expect_err("a corrupt data block must not verify clean");
        assert!(
            err.to_string().contains("squashfs verify:") && err.to_string().contains("bin/ping"),
            "expected a verify-scoped error naming the file, got: {err}"
        );
    }
}
