//! Where a new file's POSIX mode / uid / gid actually come from.
//!
//! Every Unix filesystem we can edit — ext, UFS, XFS, EFS, Minix, and SquashFS
//! once its edit path lands — already honours `CreateFileOptions`'s `mode`,
//! `uid` and `gid`. Until now *nothing set them*: `rb-cli put` left all three at
//! `None`, so each driver fell back to its own `unwrap_or` and every added file
//! silently became **root:root 0644**.
//!
//! That is defensible as a last resort and wrong as a default. Replacing
//! `/etc/shadow` should not turn it world-readable; dropping a script into
//! `/usr/local/bin` should not make it non-executable; and replacing a
//! capability-bearing binary should not quietly strip its ownership.
//!
//! This module is the one place that decides, so the CLI, GUI and TUI cannot
//! drift. The rule, highest priority first:
//!
//! | Attribute | Precedence |
//! |---|---|
//! | `mode` | explicit flag -> file being replaced -> host file's own bits -> fallback |
//! | `uid`  | explicit flag -> file being replaced -> parent directory -> 0 |
//! | `gid`  | explicit flag -> file being replaced -> parent directory -> 0 |
//!
//! **Replacing an existing file inherits from it**, because that is the common
//! edit — swap a config, replace a binary — and the file already carries the
//! answer. **A genuinely new file takes ownership from its parent directory**,
//! which is how the surrounding tree is almost always laid out. The host file's
//! permission bits are consulted for `mode` only: macOS carries those faithfully,
//! but its uid/gid (typically 501:20) are meaningless inside a Linux image.
//!
//! Each resolved value records *where it came from* so the operation can say so
//! rather than guessing silently — see [`ResolvedAttrs::describe`].

use super::entry::FileEntry;

/// Which input supplied a resolved attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttrSource {
    /// An explicit `--mode` / `--uid` / `--gid`, or the GUI's equivalent field.
    Explicit,
    /// Inherited from the file being overwritten.
    Replaced,
    /// Inherited from the containing directory.
    Parent,
    /// Taken from the host file's own permission bits (`mode` only).
    HostFile,
    /// Nothing else applied.
    Fallback,
}

impl AttrSource {
    /// ASCII-only label for log lines (no Unicode glyphs — see CONTRIBUTING).
    pub fn label(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::Replaced => "from replaced file",
            Self::Parent => "from parent dir",
            Self::HostFile => "from host file",
            Self::Fallback => "default",
        }
    }
}

/// Caller-supplied overrides. `None` means "decide for me".
#[derive(Debug, Default, Clone, Copy)]
pub struct AttrOverrides {
    /// Permission bits; the file-type bits are the driver's business.
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
}

impl AttrOverrides {
    /// True when the caller specified nothing at all.
    pub fn is_empty(&self) -> bool {
        self.mode.is_none() && self.uid.is_none() && self.gid.is_none()
    }
}

/// The decision, with provenance per field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedAttrs {
    /// Permission bits only (`0o7777`).
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub mode_from: AttrSource,
    pub uid_from: AttrSource,
    pub gid_from: AttrSource,
}

impl ResolvedAttrs {
    /// One-line ASCII summary naming where each value came from, so a `put`
    /// that inherits `0600 root:root` from the file it replaced says so instead
    /// of leaving the user to discover it later.
    pub fn describe(&self) -> String {
        format!(
            "mode {:04o} ({}), uid {} ({}), gid {} ({})",
            self.mode,
            self.mode_from.label(),
            self.uid,
            self.uid_from.label(),
            self.gid,
            self.gid_from.label(),
        )
    }

    /// The mode as `CreateFileOptions::mode` wants it for a **regular file** —
    /// permission bits OR'd with `S_IFREG`.
    ///
    /// Every driver takes that field as the *complete* mode (ext, UFS and EFS
    /// default it to `0o100644` and write it straight into the inode), so
    /// handing them bare permission bits would produce an inode with no type.
    /// Going through these two helpers keeps that from being rediscovered.
    pub fn file_mode(&self) -> u32 {
        0o100_000 | (self.mode & 0o7777)
    }

    /// The mode for a **directory** — permission bits OR'd with `S_IFDIR`.
    pub fn dir_mode(&self) -> u32 {
        0o040_000 | (self.mode & 0o7777)
    }

    /// True when every field fell through to its last-resort value — the case
    /// worth warning about, because it is the old silent `root:root 0644`.
    pub fn is_all_fallback(&self) -> bool {
        self.mode_from == AttrSource::Fallback
            && self.uid_from == AttrSource::Fallback
            && self.gid_from == AttrSource::Fallback
    }
}

/// Permission bits carried by a browse entry, if it has any.
fn perms_of(entry: &FileEntry) -> Option<u32> {
    entry.mode.map(|m| m & 0o7777)
}

/// Resolve the POSIX attributes for a file about to be created.
///
/// * `replacing` — the entry being overwritten, when this is a replace.
/// * `parent` — the directory it is being created in.
/// * `host_mode` — the source host file's mode, when the content came from one.
/// * `fallback_mode` — last resort (`0o644` for files, `0o755` for directories).
///
/// See the module docs for the precedence table.
pub fn resolve_attrs(
    overrides: &AttrOverrides,
    replacing: Option<&FileEntry>,
    parent: Option<&FileEntry>,
    host_mode: Option<u32>,
    fallback_mode: u32,
) -> ResolvedAttrs {
    let (mode, mode_from) = if let Some(m) = overrides.mode {
        (m & 0o7777, AttrSource::Explicit)
    } else if let Some(m) = replacing.and_then(perms_of) {
        (m, AttrSource::Replaced)
    } else if let Some(m) = host_mode {
        (m & 0o7777, AttrSource::HostFile)
    } else {
        (fallback_mode & 0o7777, AttrSource::Fallback)
    };

    let (uid, uid_from) = resolve_id(
        overrides.uid,
        replacing.and_then(|e| e.uid),
        parent.and_then(|e| e.uid),
    );
    let (gid, gid_from) = resolve_id(
        overrides.gid,
        replacing.and_then(|e| e.gid),
        parent.and_then(|e| e.gid),
    );

    ResolvedAttrs {
        mode,
        uid,
        gid,
        mode_from,
        uid_from,
        gid_from,
    }
}

/// Add execute wherever read is already granted — exactly `chmod a+X`.
///
/// A directory's execute bit means "may be traversed", so a directory with read
/// but no execute can be listed and not entered, which is almost never what
/// anyone wants. Note this deliberately does **not** grant execute to classes
/// that have no read: forcing `o+x` on everything would widen `/root` (0700),
/// `~/.ssh` (0700) and `/etc/ssl/private` — a real security regression, not a
/// convenience.
pub fn add_execute_where_read(mode: u32) -> u32 {
    let mut out = mode;
    for shift in [6, 3, 0] {
        if mode >> shift & 0o4 != 0 {
            out |= 0o1 << shift;
        }
    }
    out
}

/// The extended attributes a replacement file should inherit from the file it
/// displaces.
///
/// The counterpart of [`resolve_attrs`] for the attributes that don't live in
/// the inode. Overwriting is delete-then-create, and the create knows nothing
/// about what was there — so on a filesystem that stores xattrs, replacing a
/// binary that carried `security.capability` produced one without it: a file
/// that still runs, still looks right, and no longer has the privilege it needs.
/// The mode and ownership rules already say a replacement inherits from what it
/// replaces; this applies the same rule to xattrs.
///
/// Pass the result to [`CreateFileOptions::xattrs`](
/// crate::fs::filesystem::CreateFileOptions::xattrs). Returns empty for a new
/// file, or on a filesystem that stores no xattrs — both correctly meaning
/// "nothing to carry over".
///
/// Note the caller must capture this **before** deleting the old entry.
pub fn inherited_xattrs(
    fs: &mut dyn crate::fs::filesystem::Filesystem,
    replacing: Option<&FileEntry>,
) -> Vec<crate::fs::xattr::Xattr> {
    let Some(entry) = replacing else {
        return Vec::new();
    };
    if !fs.supports_xattrs() {
        return Vec::new();
    }
    // A file we cannot read attributes off is not a reason to abort the write;
    // the worst case is the status quo, which is that they are dropped.
    fs.list_xattrs(entry).unwrap_or_default()
}

/// Resolve the POSIX attributes for a **directory** about to be created.
///
/// Differs from [`resolve_attrs`] in two ways that matter:
///
/// * a new directory inherits its mode from the **parent directory**, so a
///   subdirectory created inside a 0700 tree stays 0700 instead of being
///   widened to a blanket 0755;
/// * any non-explicit mode gets [`add_execute_where_read`] applied, so a
///   directory is always traversable by whoever can read it.
///
/// An explicit `--mode` is obeyed verbatim, including a mode with no execute
/// bits: the caller asked for it, and silently "fixing" an explicit request is
/// worse than honouring a strange one.
pub fn resolve_dir_attrs(
    overrides: &AttrOverrides,
    replacing: Option<&FileEntry>,
    parent: Option<&FileEntry>,
) -> ResolvedAttrs {
    let (mode, mode_from) = if let Some(m) = overrides.mode {
        (m & 0o7777, AttrSource::Explicit)
    } else if let Some(m) = replacing.and_then(perms_of) {
        (add_execute_where_read(m), AttrSource::Replaced)
    } else if let Some(m) = parent.and_then(perms_of) {
        (add_execute_where_read(m), AttrSource::Parent)
    } else {
        (0o755, AttrSource::Fallback)
    };

    let (uid, uid_from) = resolve_id(
        overrides.uid,
        replacing.and_then(|e| e.uid),
        parent.and_then(|e| e.uid),
    );
    let (gid, gid_from) = resolve_id(
        overrides.gid,
        replacing.and_then(|e| e.gid),
        parent.and_then(|e| e.gid),
    );

    ResolvedAttrs {
        mode,
        uid,
        gid,
        mode_from,
        uid_from,
        gid_from,
    }
}

/// Shared precedence for uid and gid: explicit, then the replaced file, then
/// the parent directory, then 0. The host file's ids are deliberately not
/// consulted — a macOS 501:20 means nothing inside a Linux image.
fn resolve_id(
    explicit: Option<u32>,
    replaced: Option<u32>,
    parent: Option<u32>,
) -> (u32, AttrSource) {
    if let Some(v) = explicit {
        (v, AttrSource::Explicit)
    } else if let Some(v) = replaced {
        (v, AttrSource::Replaced)
    } else if let Some(v) = parent {
        (v, AttrSource::Parent)
    } else {
        (0, AttrSource::Fallback)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::entry::FileEntry;

    fn entry_with(mode: u32, uid: u32, gid: u32) -> FileEntry {
        let mut e = FileEntry::new_file("f".into(), "/f".into(), 0, 0);
        e.mode = Some(mode);
        e.uid = Some(uid);
        e.gid = Some(gid);
        e
    }

    fn bare_dir() -> FileEntry {
        FileEntry::new_directory("d".into(), "/d".into(), 0)
    }

    #[test]
    fn explicit_overrides_win_over_everything() {
        let replaced = entry_with(0o100_600, 7, 8);
        let parent = entry_with(0o040_755, 9, 10);
        let got = resolve_attrs(
            &AttrOverrides {
                mode: Some(0o755),
                uid: Some(1),
                gid: Some(2),
            },
            Some(&replaced),
            Some(&parent),
            Some(0o644),
            0o644,
        );
        assert_eq!((got.mode, got.uid, got.gid), (0o755, 1, 2));
        assert_eq!(got.mode_from, AttrSource::Explicit);
        assert_eq!(got.uid_from, AttrSource::Explicit);
        assert_eq!(got.gid_from, AttrSource::Explicit);
    }

    /// The case that matters most: replacing a file keeps its permissions and
    /// ownership. Overwriting /etc/shadow must not widen it to 0644.
    #[test]
    fn replacing_a_file_inherits_its_mode_and_ownership() {
        let shadow = entry_with(0o100_600, 0, 42);
        let parent = entry_with(0o040_755, 0, 0);
        let got = resolve_attrs(
            &AttrOverrides::default(),
            Some(&shadow),
            Some(&parent),
            // The host copy is a world-readable 0644 -- must NOT win.
            Some(0o644),
            0o644,
        );
        assert_eq!(got.mode, 0o600, "replaced file's mode must win over host's");
        assert_eq!(got.uid, 0);
        assert_eq!(got.gid, 42);
        assert_eq!(got.mode_from, AttrSource::Replaced);
        assert_eq!(got.gid_from, AttrSource::Replaced);
    }

    /// A genuinely new file takes ownership from the directory it lands in, and
    /// its mode from the host file (macOS does carry those bits faithfully).
    #[test]
    fn new_file_takes_parent_ownership_and_host_mode() {
        let parent = entry_with(0o040_755, 0, 20);
        let got = resolve_attrs(
            &AttrOverrides::default(),
            None,
            Some(&parent),
            Some(0o755),
            0o644,
        );
        assert_eq!(got.mode, 0o755);
        assert_eq!(got.mode_from, AttrSource::HostFile);
        assert_eq!(got.uid, 0);
        assert_eq!(got.gid, 20);
        assert_eq!(got.gid_from, AttrSource::Parent);
    }

    /// A filesystem whose entries carry no POSIX metadata at all (FAT, HFS)
    /// must still resolve to something sane rather than panicking.
    #[test]
    fn falls_back_when_nothing_carries_metadata() {
        let parent = bare_dir();
        let got = resolve_attrs(&AttrOverrides::default(), None, Some(&parent), None, 0o644);
        assert_eq!((got.mode, got.uid, got.gid), (0o644, 0, 0));
        assert!(got.is_all_fallback());
    }

    /// Inheriting must strip the file-type bits: a replaced *file* entry
    /// carries 0o100600, and only the 0o600 belongs in the new file's mode.
    #[test]
    fn inherited_mode_drops_the_type_bits() {
        let replaced = entry_with(0o100_640, 0, 0);
        let got = resolve_attrs(
            &AttrOverrides::default(),
            Some(&replaced),
            None,
            None,
            0o644,
        );
        assert_eq!(got.mode, 0o640);
        assert_eq!(got.mode & 0o170_000, 0, "type bits must not survive");
    }

    /// Setuid and sticky bits live in the 0o7777 range and must be preserved
    /// when inheriting -- dropping the setuid bit off /bin/mount would break it.
    #[test]
    fn inherited_mode_keeps_setuid_and_sticky() {
        let replaced = entry_with(0o104_755, 0, 0);
        let got = resolve_attrs(
            &AttrOverrides::default(),
            Some(&replaced),
            None,
            None,
            0o644,
        );
        assert_eq!(got.mode, 0o4755);
    }

    /// `+X` grants execute only where read already is, so it can never widen a
    /// private directory.
    #[test]
    fn add_execute_where_read_never_widens_a_private_dir() {
        // /root and ~/.ssh: owner-only, and must stay that way.
        assert_eq!(add_execute_where_read(0o700), 0o700);
        assert_eq!(add_execute_where_read(0o600), 0o700, "owner r -> owner x");
        // A group-readable, other-invisible dir gains group x, not other x.
        assert_eq!(add_execute_where_read(0o640), 0o750);
        // Already-correct modes are untouched.
        assert_eq!(add_execute_where_read(0o755), 0o755);
        // Nothing readable, nothing granted.
        assert_eq!(add_execute_where_read(0o000), 0o000);
        // setuid/sticky bits ride along unchanged.
        assert_eq!(add_execute_where_read(0o1644), 0o1755);
    }

    /// A new directory inside a 0700 tree must stay 0700, not become 0755.
    #[test]
    fn new_dir_inherits_parent_mode_rather_than_a_blanket_0755() {
        let private = entry_with(0o040_700, 0, 0);
        let got = resolve_dir_attrs(&AttrOverrides::default(), None, Some(&private));
        assert_eq!(got.mode, 0o700, "a subdir of /root must not be widened");
        assert_eq!(got.mode_from, AttrSource::Parent);
    }

    /// A directory that would otherwise be readable-but-not-enterable gets its
    /// execute bits fixed up.
    #[test]
    fn new_dir_becomes_traversable_by_whoever_can_read_it() {
        let odd = entry_with(0o040_644, 0, 0);
        let got = resolve_dir_attrs(&AttrOverrides::default(), None, Some(&odd));
        assert_eq!(got.mode, 0o755);
    }

    /// An explicit mode is obeyed verbatim, even a strange one -- silently
    /// "fixing" what the caller asked for is worse than honouring it.
    #[test]
    fn explicit_dir_mode_is_not_second_guessed() {
        let got = resolve_dir_attrs(
            &AttrOverrides {
                mode: Some(0o644),
                ..Default::default()
            },
            None,
            None,
        );
        assert_eq!(got.mode, 0o644);
        assert_eq!(got.mode_from, AttrSource::Explicit);
    }

    /// One `AttrOverrides` is `Copy`, so a bulk import applies the same
    /// decision to every member without re-deriving it per file.
    #[test]
    fn overrides_apply_uniformly_across_a_batch() {
        let ov = AttrOverrides {
            mode: Some(0o640),
            uid: Some(0),
            gid: Some(0),
        };
        let parents = [entry_with(0o040_755, 99, 99), entry_with(0o040_700, 5, 5)];
        for parent in &parents {
            let got = resolve_attrs(&ov, None, Some(parent), Some(0o777), 0o644);
            assert_eq!((got.mode, got.uid, got.gid), (0o640, 0, 0));
        }
    }

    /// The drivers write `CreateFileOptions::mode` straight into the inode, so
    /// the type bits have to be there. A file created with bare `0o644` would
    /// have no type at all.
    #[test]
    fn mode_helpers_carry_the_type_bits() {
        let got = resolve_attrs(
            &AttrOverrides {
                mode: Some(0o640),
                ..Default::default()
            },
            None,
            None,
            None,
            0o644,
        );
        assert_eq!(got.file_mode(), 0o100_640);
        assert_eq!(got.dir_mode(), 0o040_640);
        // ext / UFS / EFS all default this field to 0o100644; our file mode has
        // to be the same shape.
        assert_eq!(got.file_mode() & 0o170_000, 0o100_000);
    }

    #[test]
    fn describe_names_every_source() {
        let replaced = entry_with(0o100_600, 0, 42);
        let parent = entry_with(0o040_755, 0, 20);
        let got = resolve_attrs(
            &AttrOverrides {
                uid: Some(5),
                ..Default::default()
            },
            Some(&replaced),
            Some(&parent),
            None,
            0o644,
        );
        let text = got.describe();
        assert!(text.contains("mode 0600 (from replaced file)"), "{text}");
        assert!(text.contains("uid 5 (explicit)"), "{text}");
        assert!(text.contains("gid 42 (from replaced file)"), "{text}");
        // ASCII only -- the GUI font has no glyph coverage beyond it.
        assert!(text.is_ascii(), "log text must stay ASCII: {text}");
    }
}
