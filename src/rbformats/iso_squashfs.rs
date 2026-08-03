//! Finding a SquashFS payload inside an ISO 9660 image.
//!
//! Every Linux live CD ships its root filesystem as a SquashFS inside the ISO —
//! `casper/filesystem.squashfs` on Ubuntu, `live/filesystem.squashfs` on Debian,
//! `LiveOS/squashfs.img` on Fedora. It is an ordinary contiguous file on the
//! disc, so — like an AppImage's appended payload — it can be opened where it
//! lies rather than extracted first.
//!
//! Two things make this narrower than the AppImage case:
//!
//! - **Read the disc through opticaldiscs, not by hand.** The names on a live CD
//!   are Rock Ridge (lowercase, long), while the base ISO 9660 records hold
//!   uppercased 8.3 stand-ins; resolving the real path means walking Rock Ridge,
//!   which the optical layer already does. So this is gated on the `optical`
//!   feature — a build that cannot read ISOs at all has nothing to add here.
//! - **The payload cannot grow.** It sits between other files on the disc, so a
//!   rebuilt SquashFS may be *at most* the original extent, never larger. That
//!   is a [`PayloadSlice::bounded`](crate::rbformats::payload_slice) window, and
//!   the size budget refuses a rebuild that would overrun it — exactly the
//!   refusal an AppImage's tail never needs.
//!
//! Only a **plain** `.iso` (2048-byte cooked sectors) is handled, because there
//! the file byte offset of a file is simply its LBA times 2048. A BIN/CUE or CHD
//! optical image interleaves sync/header bytes into every 2352-byte sector, so
//! the payload is not a contiguous run in the backing file and cannot be
//! windowed; those are read through the optical browser instead.

#![cfg(feature = "optical")]

use std::path::Path;

use opticaldiscs::browse::entry::EntryType;
use opticaldiscs::browse::open_disc_filesystem;
use opticaldiscs::detect::DiscImageInfo;
use opticaldiscs::formats::DiscFormat;

/// ISO 9660 logical sector size. A plain `.iso` stores exactly this per sector
/// with no per-sector overhead, so a file's byte offset is `lba * SECTOR`.
const ISO_SECTOR: u64 = 2048;

/// Does `path` open with an ISO 9660 primary volume descriptor?
///
/// Reads five bytes: the PVD sits at sector 16 of a 2048-byte-sector image,
/// so its `CD001` standard identifier is at byte 0x8001. Deliberately a
/// content check rather than an extension check — a live CD saved as
/// `.img` is still an ISO — and deliberately cheap, because it runs for
/// every path the source router looks at.
fn looks_like_iso9660(path: &Path) -> bool {
    use std::io::{Read, Seek, SeekFrom};
    let Ok(mut f) = std::fs::File::open(path) else {
        return false;
    };
    if f.seek(SeekFrom::Start(0x8001)).is_err() {
        return false;
    }
    let mut magic = [0u8; 5];
    f.read_exact(&mut magic).is_ok() && &magic == b"CD001"
}

/// Where a SquashFS lives inside an ISO, and how large its extent is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsoSquashfs {
    /// Byte offset of the payload in the ISO file.
    pub offset: u64,
    /// Length of the file's ISO extent — the ceiling a rebuild may not exceed.
    pub len: u64,
}

/// Find the first SquashFS payload inside the ISO at `path`.
///
/// Enumerates the disc's files rather than looking for a fixed path, because
/// the location differs across distributions (`casper/` vs `live/` vs
/// `LiveOS/`). Each candidate's offset is *verified* by parsing a SquashFS
/// superblock there — the ISO says where a file's bytes are, and this confirms
/// those bytes are a filesystem before anything downstream trusts them.
///
/// `None` when the image is not a plain ISO, holds no SquashFS, or cannot be
/// read.
pub fn find_squashfs(path: &Path) -> Option<IsoSquashfs> {
    // Confirm this is an ISO 9660 image *before* handing the path to
    // opticaldiscs. `DiscImageInfo::open` sniffs every container it knows,
    // and its CHD probe reaches MAME's `cdrom_file` constructor, which
    // THROWS for a CHD carrying no CD metadata — an ordinary hard-disk CHD.
    // A C++ exception cannot unwind into Rust, so the process aborts rather
    // than returning an error. Since this function only ever succeeds on a
    // plain ISO (the `DiscFormat` check below), the gate costs no coverage.
    if !looks_like_iso9660(path) {
        return None;
    }
    let info = DiscImageInfo::open(path).ok()?;
    // Only a plain ISO has file offset == lba * 2048; see the module docs.
    if info.format != DiscFormat::Iso {
        return None;
    }
    let mut fs = open_disc_filesystem(&info).ok()?;

    // A read handle onto the raw ISO for the superblock probe. Opening it once
    // and reusing it keeps the walk from reopening the file per candidate.
    let mut raw = std::fs::File::open(path).ok()?;

    // Iterative directory walk; live-CD trees are shallow but this avoids any
    // recursion-depth worry on a hostile image.
    let root = fs.root().ok()?;
    let mut stack = vec![root];
    while let Some(dir) = stack.pop() {
        let children = fs.list_directory(&dir).ok()?;
        for child in children {
            match child.entry_type {
                EntryType::Directory => stack.push(child),
                EntryType::File => {
                    let offset = child.location.checked_mul(ISO_SECTOR)?;
                    if crate::fs::squashfs::SquashfsFilesystem::detect(&mut raw, offset) {
                        return Some(IsoSquashfs {
                            offset,
                            len: child.size,
                        });
                    }
                }
            }
        }
    }
    None
}

/// Whether `path` is an ISO holding a SquashFS we can open.
pub fn is_squashfs_bearing_iso(path: &Path) -> bool {
    // Cheap gate first: only look inside something that is actually an ISO, so
    // the common non-ISO file pays nothing.
    let ext_is_iso = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.eq_ignore_ascii_case("iso"))
        .unwrap_or(false);
    ext_is_iso && find_squashfs(path).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal but valid plain ISO 9660 with one file — a real SquashFS
    /// — in the root directory, and confirm the locator finds it at the right
    /// offset with the right length.
    ///
    /// This exercises the whole path through opticaldiscs' own ISO parser, so a
    /// change in how it reports a file's `location` would be caught here.
    #[test]
    fn finds_a_squashfs_file_in_a_plain_iso() {
        use crate::fs::squashfs_write::{write_squashfs, BuildNode, BuildOptions};
        use std::io::Write;

        // The payload we will embed.
        let payload = {
            let tree = BuildNode::dir(
                "",
                0o755,
                vec![BuildNode::file("hello", 0o644, b"live cd root\n".to_vec())],
            );
            let mut cur = std::io::Cursor::new(Vec::new());
            write_squashfs(&mut cur, &tree, &BuildOptions::default()).expect("build");
            cur.into_inner()
        };

        // Lay out a plain ISO by hand: system area (16 sectors), PVD at 16,
        // terminator at 17, root directory at 18, the payload at 19.
        const PAYLOAD_LBA: u32 = 19;
        let root_lba = 18u32;
        let mut iso = vec![0u8; 20 * ISO_SECTOR as usize];

        // PVD at sector 16, with the root directory record pointing at 18.
        let pvd =
            opticaldiscs::iso9660::build_test_pvd_sector("LIVECD", root_lba, ISO_SECTOR as u32);
        iso[16 * ISO_SECTOR as usize..16 * ISO_SECTOR as usize + pvd.len()].copy_from_slice(&pvd);
        // Volume descriptor set terminator at sector 17.
        iso[17 * ISO_SECTOR as usize] = 0xFF;
        iso[17 * ISO_SECTOR as usize + 1..17 * ISO_SECTOR as usize + 6].copy_from_slice(b"CD001");

        // Root directory at sector 18: "." , ".." , then FILE.SQFS;1.
        let dir = &mut iso[root_lba as usize * ISO_SECTOR as usize..];
        let mut off = 0usize;
        let emit =
            |dir: &mut [u8], off: &mut usize, name: &[u8], lba: u32, size: u32, is_dir: bool| {
                let reclen = 33 + name.len() + (1 - name.len() % 2);
                dir[*off] = reclen as u8;
                dir[*off + 2..*off + 6].copy_from_slice(&lba.to_le_bytes());
                dir[*off + 6..*off + 10].copy_from_slice(&lba.to_be_bytes());
                dir[*off + 10..*off + 14].copy_from_slice(&size.to_le_bytes());
                dir[*off + 14..*off + 18].copy_from_slice(&size.to_be_bytes());
                dir[*off + 25] = if is_dir { 0x02 } else { 0x00 };
                dir[*off + 32] = name.len() as u8;
                dir[*off + 33..*off + 33 + name.len()].copy_from_slice(name);
                *off += reclen;
            };
        emit(dir, &mut off, &[0x00], root_lba, ISO_SECTOR as u32, true);
        emit(dir, &mut off, &[0x01], root_lba, ISO_SECTOR as u32, true);
        emit(
            dir,
            &mut off,
            b"FILE.SQFS;1",
            PAYLOAD_LBA,
            payload.len() as u32,
            false,
        );

        // The payload itself at sector 19.
        let base = PAYLOAD_LBA as usize * ISO_SECTOR as usize;
        // Grow the buffer to hold the whole payload.
        if base + payload.len() > iso.len() {
            iso.resize(base + payload.len(), 0);
        }
        iso[base..base + payload.len()].copy_from_slice(&payload);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("live.iso");
        std::fs::File::create(&path)
            .unwrap()
            .write_all(&iso)
            .unwrap();

        let found = find_squashfs(&path).expect("locator must find the payload");
        assert_eq!(found.offset, PAYLOAD_LBA as u64 * ISO_SECTOR);
        assert_eq!(found.len, payload.len() as u64);
        assert!(is_squashfs_bearing_iso(&path));

        // And the offset really opens as a filesystem.
        let file = std::fs::File::open(&path).unwrap();
        let mut sqfs = crate::fs::squashfs::SquashfsFilesystem::open(file, found.offset)
            .expect("open payload at the located offset");
        use crate::fs::filesystem::Filesystem;
        let root = sqfs.root().unwrap();
        let names: Vec<String> = sqfs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(names.contains(&"hello".to_string()), "got: {names:?}");
    }

    /// A plain data ISO with no SquashFS must not be mistaken for one, so the
    /// generic open path is never hijacked away from normal ISO browsing.
    #[test]
    fn a_plain_data_iso_is_not_reported() {
        use std::io::Write;
        let mut iso = vec![0u8; 20 * ISO_SECTOR as usize];
        let pvd = opticaldiscs::iso9660::build_test_pvd_sector("DATA", 18, ISO_SECTOR as u32);
        iso[16 * ISO_SECTOR as usize..16 * ISO_SECTOR as usize + pvd.len()].copy_from_slice(&pvd);
        iso[17 * ISO_SECTOR as usize] = 0xFF;
        iso[17 * ISO_SECTOR as usize + 1..17 * ISO_SECTOR as usize + 6].copy_from_slice(b"CD001");
        // Empty root directory (just . and ..) — no files at all.
        let dir = &mut iso[18 * ISO_SECTOR as usize..];
        dir[0] = 34;
        dir[2..6].copy_from_slice(&18u32.to_le_bytes());
        dir[25] = 0x02;
        dir[32] = 1;

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("data.iso");
        std::fs::File::create(&path)
            .unwrap()
            .write_all(&iso)
            .unwrap();
        assert!(find_squashfs(&path).is_none());
        assert!(!is_squashfs_bearing_iso(&path));
    }

    /// A hard-disk CHD must never reach opticaldiscs' container sniffer.
    ///
    /// `is_container_path` calls `find_squashfs` for **every** path the
    /// source router sees, and `DiscImageInfo::open` probes a CHD as a CD.
    /// MAME's `cdrom_file` constructor throws for a CHD with no CD metadata,
    /// and a C++ exception cannot unwind into Rust — the process aborts.
    /// Opening any hard-disk CHD (an SGI EFS disk, say) killed the app.
    ///
    /// If this regresses the whole test binary dies rather than failing an
    /// assertion, which is exactly as loud as the bug deserves.
    #[test]
    #[cfg(feature = "chd")]
    fn a_hard_disk_chd_is_never_probed_as_a_disc_image() {
        let tmp = tempfile::TempDir::new().unwrap();
        // A real hard-disk CHD (no CD metadata) built the way `convert` does.
        let data = vec![0x5Au8; 512 * 1024];
        let base = tmp.path().join("hd");
        crate::rbformats::chd::compress_chd(
            &mut std::io::Cursor::new(&data),
            &base,
            data.len() as u64,
            None,
            &mut |_| {},
            &|| false,
            &mut |_| {},
        )
        .expect("build a hard-disk CHD");
        let chd = base.with_extension("chd");
        assert!(!looks_like_iso9660(&chd), "a CHD is not an ISO");
        assert_eq!(find_squashfs(&chd), None);
        assert!(!is_squashfs_bearing_iso(&chd));
        // And the shared guard refuses it rather than letting C++ throw.
        assert!(crate::optical::open_disc_image(&chd).is_err());
    }

    /// The ISO gate is a content check, so a live CD saved under another
    /// extension still gets looked inside.
    #[test]
    fn the_iso_gate_reads_content_not_the_extension() {
        let tmp = tempfile::TempDir::new().unwrap();
        let named_img = tmp.path().join("livecd.img");
        let mut iso = vec![0u8; 0x8006];
        iso[0x8001..0x8006].copy_from_slice(b"CD001");
        std::fs::write(&named_img, &iso).unwrap();
        assert!(looks_like_iso9660(&named_img));

        let not_iso = tmp.path().join("random.iso");
        std::fs::write(&not_iso, vec![0u8; 0x9000]).unwrap();
        assert!(
            !looks_like_iso9660(&not_iso),
            "extension alone must not pass"
        );
    }
}
