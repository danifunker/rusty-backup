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
