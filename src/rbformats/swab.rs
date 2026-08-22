//! Whole-image 16-bit word byte-swap.
//!
//! Some vintage disk controllers transfer 16-bit words with the two bytes
//! reversed, so an image captured through one comes out byte-swapped within
//! every word — every structure and every byte of file data alike. The SGI
//! IRIS 2000 / 3000 disks this was written for are the motivating case (see
//! [`crate::partition::sgi_dklabel`]), but nothing here is SGI-specific: the
//! transform is a pure `swab16` over the whole file.
//!
//! The operation is an **involution** — applying it twice returns the original
//! bytes — so one code path converts in both directions and there is no
//! "which way round am I going" flag to get wrong.
//!
//! A trailing odd byte (an image whose length is not a multiple of two) has no
//! word to pair with and is copied through untouched.
//!
//! The in-memory primitive lives in [`crate::partition::sgi_dklabel`], which
//! needs it to fix up individual blocks on read; this module is the streaming
//! file-level wrapper so a large image never lands in RAM at once.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::partition::sgi_dklabel::swab16_in_place;

/// I/O chunk size. Even, so a 16-bit word can never straddle two chunks.
const CHUNK: usize = 1024 * 1024;

/// Swap the bytes of every 16-bit word from `src` into a new file at `dst`.
/// `progress` is called with `(bytes_done, total_bytes)`. Returns bytes written.
pub fn swab16_file(src: &Path, dst: &Path, progress: &mut dyn FnMut(u64, u64)) -> io::Result<u64> {
    let mut input = File::open(src)?;
    let total = input.seek(SeekFrom::End(0))?;
    input.seek(SeekFrom::Start(0))?;
    let mut output = File::create(dst)?;

    let mut buf = vec![0u8; CHUNK];
    let mut done = 0u64;
    loop {
        let n = read_up_to(&mut input, &mut buf)?;
        if n == 0 {
            break;
        }
        swab16_in_place(&mut buf[..n]);
        output.write_all(&buf[..n])?;
        done += n as u64;
        progress(done, total);
    }
    output.flush()?;
    Ok(done)
}

/// Swap the bytes of every 16-bit word of `path`, rewriting it in place.
/// `progress` is called with `(bytes_done, total_bytes)`. Returns bytes rewritten.
pub fn swab16_file_in_place(path: &Path, progress: &mut dyn FnMut(u64, u64)) -> io::Result<u64> {
    let mut f = OpenOptions::new().read(true).write(true).open(path)?;
    let total = f.seek(SeekFrom::End(0))?;
    f.seek(SeekFrom::Start(0))?;

    let mut buf = vec![0u8; CHUNK];
    let mut done = 0u64;
    loop {
        let n = read_up_to(&mut f, &mut buf)?;
        if n == 0 {
            break;
        }
        swab16_in_place(&mut buf[..n]);
        f.seek(SeekFrom::Start(done))?;
        f.write_all(&buf[..n])?;
        done += n as u64;
        f.seek(SeekFrom::Start(done))?;
        progress(done, total);
    }
    f.flush()?;
    Ok(done)
}

/// Fill `buf` as far as the reader allows, returning the byte count. Short
/// reads are stitched so a chunk only ends early at true end-of-file.
fn read_up_to(r: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_tmp(dir: &tempfile::TempDir, name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let p = dir.path().join(name);
        std::fs::write(&p, bytes).unwrap();
        p
    }

    fn read_all(p: &Path) -> Vec<u8> {
        let mut v = Vec::new();
        File::open(p).unwrap().read_to_end(&mut v).unwrap();
        v
    }

    #[test]
    fn swaps_every_word() {
        let dir = tempfile::tempdir().unwrap();
        let src = write_tmp(&dir, "a.img", &[0x07, 0x00, 0x59, 0x29]);
        let dst = dir.path().join("b.img");
        let n = swab16_file(&src, &dst, &mut |_, _| {}).unwrap();
        assert_eq!(n, 4);
        assert_eq!(read_all(&dst), vec![0x00, 0x07, 0x29, 0x59]);
    }

    #[test]
    fn is_an_involution() {
        let dir = tempfile::tempdir().unwrap();
        let original: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
        let src = write_tmp(&dir, "a.img", &original);
        let once = dir.path().join("b.img");
        let twice = dir.path().join("c.img");
        swab16_file(&src, &once, &mut |_, _| {}).unwrap();
        swab16_file(&once, &twice, &mut |_, _| {}).unwrap();
        assert_ne!(read_all(&once), original);
        assert_eq!(read_all(&twice), original);
    }

    #[test]
    fn odd_trailing_byte_is_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let src = write_tmp(&dir, "a.img", &[0xAA, 0xBB, 0xCC]);
        let dst = dir.path().join("b.img");
        swab16_file(&src, &dst, &mut |_, _| {}).unwrap();
        assert_eq!(read_all(&dst), vec![0xBB, 0xAA, 0xCC]);
    }

    #[test]
    fn spans_multiple_chunks_without_straddling_a_word() {
        let dir = tempfile::tempdir().unwrap();
        let original: Vec<u8> = (0..(CHUNK * 2 + 512)).map(|i| (i % 253) as u8).collect();
        let src = write_tmp(&dir, "a.img", &original);
        let once = dir.path().join("b.img");
        let twice = dir.path().join("c.img");
        swab16_file(&src, &once, &mut |_, _| {}).unwrap();
        swab16_file(&once, &twice, &mut |_, _| {}).unwrap();
        assert_eq!(read_all(&twice), original);
    }

    #[test]
    fn in_place_matches_file_to_file() {
        let dir = tempfile::tempdir().unwrap();
        let original: Vec<u8> = (0..5000u32).map(|i| (i % 247) as u8).collect();
        let a = write_tmp(&dir, "a.img", &original);
        let b = write_tmp(&dir, "b.img", &original);
        let out = dir.path().join("out.img");
        swab16_file(&a, &out, &mut |_, _| {}).unwrap();
        swab16_file_in_place(&b, &mut |_, _| {}).unwrap();
        assert_eq!(read_all(&b), read_all(&out));
    }

    #[test]
    fn empty_file_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let src = write_tmp(&dir, "a.img", &[]);
        let dst = dir.path().join("b.img");
        assert_eq!(swab16_file(&src, &dst, &mut |_, _| {}).unwrap(), 0);
        assert!(read_all(&dst).is_empty());
    }

    #[test]
    fn progress_reaches_the_total() {
        let dir = tempfile::tempdir().unwrap();
        let src = write_tmp(&dir, "a.img", &vec![0u8; 3000]);
        let dst = dir.path().join("b.img");
        let mut last = (0u64, 0u64);
        swab16_file(&src, &dst, &mut |d, t| last = (d, t)).unwrap();
        assert_eq!(last, (3000, 3000));
    }
}
