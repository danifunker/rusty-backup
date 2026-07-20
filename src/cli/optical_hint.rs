//! Friendly redirect when a generic in-image verb (`ls`, `get`, `du`) is
//! pointed at an optical disc image it can't drive.
//!
//! Those verbs go through the block-image path (`resolve_partition_streaming`
//! → `open_filesystem`), which assumes 512-byte geometry and mis-parses a
//! 2048-byte optical disc — producing a cryptic error like
//! `bad MDB signature: 0x0000`. Optical media needs the `opticaldiscs`
//! track/session handling behind the `optical` verb group, so when the normal
//! open fails on something that looks like a disc image we point the user at
//! `optical browse` / `optical extract` instead.

use std::path::Path;

/// Wrap an open error with an optical redirect when `path` looks like an
/// optical disc image. Called only on the *failure* branch, so a disc image we
/// can genuinely open the normal way is never intercepted.
pub fn with_optical_hint(err: anyhow::Error, path: &Path) -> anyhow::Error {
    match optical_redirect_hint(path) {
        Some(hint) => anyhow::anyhow!("{err}\n\n{hint}"),
        None => err,
    }
}

/// Returns the redirect message if `path` is (or looks like) an optical disc
/// image, else `None`.
///
/// With the `optical` feature the crate authoritatively confirms the container
/// (and names the on-disc filesystem); without it we fall back to a
/// conservative extension check and note that this build lacks optical support.
fn optical_redirect_hint(path: &Path) -> Option<String> {
    #[cfg(feature = "optical")]
    {
        if let Ok(info) = opticaldiscs::detect::DiscImageInfo::open(path) {
            let mut msg = format!(
                "This looks like an optical disc image ({}). The generic ls/get/du \
                 verbs only handle block-device (hard-disk / floppy) images; use the \
                 optical verbs instead:\n  \
                 rb-cli optical browse {p}        # list the file tree\n  \
                 rb-cli optical extract {p} --to DIR   # pull files out",
                fs_label(&info),
                p = path.display(),
            );
            // Hybrid Mac/PC discs keep the resource-fork side behind --filesystem hfs.
            if !info.hybrid_filesystems.is_empty() {
                msg.push_str(
                    "\nThis is a hybrid Mac/PC disc — add `--filesystem hfs` to reach the \
                     Apple (resource-fork) side.",
                );
            }
            return Some(msg);
        }
        None
    }
    #[cfg(not(feature = "optical"))]
    {
        if has_optical_extension(path) {
            return Some(format!(
                "{} looks like an optical disc image, which the generic ls/get/du verbs \
                 can't open. This build was compiled without optical support — rebuild \
                 with `--features optical` to get the `rb-cli optical browse` / `extract` \
                 verbs.",
                path.display(),
            ));
        }
        None
    }
}

/// True when `path` is an NKit-processed GameCube/Wii image (the `NKIT` magic
/// sits at byte 0x200). NKit *identifies* as a normal GC/Wii disc — its header
/// is intact — but it scrubs and rearranges the partition data, which the `nod`
/// reader behind `opticaldiscs` can't reconstruct, so browse/extract are refused
/// for these. Detecting them lets us replace the opaque "Unsupported filesystem"
/// error with something actionable.
pub fn is_nkit_image(path: &Path) -> bool {
    use std::io::{Read, Seek, SeekFrom};
    let Ok(mut f) = std::fs::File::open(path) else {
        return false;
    };
    if f.seek(SeekFrom::Start(0x200)).is_err() {
        return false;
    }
    let mut magic = [0u8; 4];
    f.read_exact(&mut magic).is_ok() && &magic == b"NKIT"
}

/// If `path` is an NKit image, wrap `err` with a clear explanation + the
/// conversion the user needs; otherwise return `err` unchanged. Call on the
/// failure branch of an optical open so a normal disc is never intercepted.
pub fn with_nkit_hint(err: anyhow::Error, path: &Path) -> anyhow::Error {
    if is_nkit_image(path) {
        anyhow::anyhow!(
            "{} is an NKit-scrubbed GameCube/Wii image. NKit removes and rearranges the \
             disc data (keeping only the header), and the underlying reader can't \
             reconstruct the filesystem — so it can't be browsed or extracted directly. \
             Convert it back to a full image first (a `.iso` or `.rvz`, e.g. with the NKit \
             tool or Dolphin's \"Convert File\"), then open that.",
            path.display()
        )
    } else {
        err
    }
}

/// Human label for the disc's primary filesystem (optical builds only).
#[cfg(feature = "optical")]
fn fs_label(info: &opticaldiscs::detect::DiscImageInfo) -> String {
    format!("{:?}", info.filesystem)
}

/// Conservative optical-only extension set for the no-optical build. Deliberately
/// excludes ambiguous extensions that are also block images (`.img`, `.bin`,
/// `.chd`) so we never mislabel a hard-disk image.
#[cfg(not(feature = "optical"))]
fn has_optical_extension(path: &Path) -> bool {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());
    matches!(
        ext.as_deref(),
        Some("iso" | "cue" | "nrg" | "mdf" | "mds" | "ccd" | "cdi" | "toc")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn detects_nkit_by_magic_at_0x200() {
        let dir = tempfile::tempdir().unwrap();
        // NKit image: real GC header up front, "NKIT" magic at byte 0x200.
        let nkit = dir.path().join("game.nkit.iso");
        let mut buf = vec![0u8; 0x210];
        buf[0x200..0x204].copy_from_slice(b"NKIT");
        buf[0x204..0x208].copy_from_slice(b" v01");
        std::fs::File::create(&nkit)
            .unwrap()
            .write_all(&buf)
            .unwrap();
        assert!(is_nkit_image(&nkit));

        // A plain image (zeros at 0x200) is not NKit.
        let plain = dir.path().join("game.iso");
        std::fs::write(&plain, vec![0u8; 0x210]).unwrap();
        assert!(!is_nkit_image(&plain));

        // Too-short file: not NKit, no panic.
        let tiny = dir.path().join("tiny.iso");
        std::fs::write(&tiny, vec![0u8; 16]).unwrap();
        assert!(!is_nkit_image(&tiny));

        // The hint fires only for NKit paths.
        let base = anyhow::anyhow!("Unsupported filesystem");
        assert!(
            with_nkit_hint(anyhow::anyhow!("Unsupported filesystem"), &nkit)
                .to_string()
                .contains("NKit-scrubbed")
        );
        assert_eq!(
            with_nkit_hint(base, &plain).to_string(),
            "Unsupported filesystem"
        );
    }
}
