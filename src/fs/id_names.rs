//! Resolve numeric uid/gid to names by reading the image's own `/etc/passwd`
//! and `/etc/group`.
//!
//! A Linux disk image *contains* the mapping it was built with, so we don't have
//! to guess: `/etc/passwd` maps uid → user name, `/etc/group` maps gid → group
//! name. Showing `root` / `www-data` instead of `0` / `33` in the browse view
//! and File Info is a small thing that makes a Linux rootfs far more legible.
//!
//! This is **best-effort and advisory**. Plenty of images have no `/etc`
//! (a data partition, a non-Linux filesystem, a bare application squashfs), and
//! an id with no entry simply falls back to the number. Nothing here is load-
//! bearing — it never blocks an operation, and a wrong or missing name changes
//! only what's displayed, never what's written.

use std::collections::HashMap;

use super::entry::EntryType;
use super::filesystem::Filesystem;

/// A cap on how much of `/etc/passwd` or `/etc/group` we read. These files are
/// kilobytes even on a busy system; anything larger is not the file we think it
/// is, so stop rather than pull a huge blob into memory.
const MAX_DB_BYTES: usize = 4 * 1024 * 1024;

/// uid → user name and gid → group name, parsed from an image's account files.
#[derive(Debug, Default, Clone)]
pub struct IdNameMap {
    users: HashMap<u32, String>,
    groups: HashMap<u32, String>,
}

impl IdNameMap {
    /// Read `/etc/passwd` and `/etc/group` from `fs` and build the maps. Missing
    /// or unreadable files yield an empty map rather than an error — this is a
    /// display nicety, never a hard dependency.
    pub fn from_filesystem(fs: &mut dyn Filesystem) -> Self {
        let mut map = Self::default();
        if let Some(bytes) = read_file_at_path(fs, &["etc", "passwd"]) {
            map.users = parse_colon_db(&bytes);
        }
        if let Some(bytes) = read_file_at_path(fs, &["etc", "group"]) {
            map.groups = parse_colon_db(&bytes);
        }
        map
    }

    /// True when neither file yielded a single entry — the caller should just
    /// show raw numbers.
    pub fn is_empty(&self) -> bool {
        self.users.is_empty() && self.groups.is_empty()
    }

    /// The user name for `uid`, if known.
    pub fn user(&self, uid: u32) -> Option<&str> {
        self.users.get(&uid).map(String::as_str)
    }

    /// The group name for `gid`, if known.
    pub fn group(&self, gid: u32) -> Option<&str> {
        self.groups.get(&gid).map(String::as_str)
    }

    /// `"root:wheel (0:0)"` when both names are known, degrading gracefully to
    /// `"root:0 (0:0)"` or `"0:0"` as names are missing. Always shows the raw
    /// numbers so nothing is hidden.
    pub fn format_owner(&self, uid: u32, gid: u32) -> String {
        match (self.user(uid), self.group(gid)) {
            (Some(u), Some(g)) => format!("{u}:{g} ({uid}:{gid})"),
            (Some(u), None) => format!("{u}:{gid} ({uid}:{gid})"),
            (None, Some(g)) => format!("{uid}:{g} ({uid}:{gid})"),
            (None, None) => format!("{uid}:{gid}"),
        }
    }
}

/// Parse a colon-separated account database (`/etc/passwd` or `/etc/group`)
/// into `id -> name`. Both files share the shape `name:x:id:...`, so one parser
/// covers both. Blank lines, comments (`#`), and malformed lines are skipped.
fn parse_colon_db(bytes: &[u8]) -> HashMap<u32, String> {
    let mut out = HashMap::new();
    let text = String::from_utf8_lossy(bytes);
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split(':');
        let (Some(name), Some(_passwd), Some(id_str)) =
            (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        if name.is_empty() {
            continue;
        }
        if let Ok(id) = id_str.trim().parse::<u32>() {
            // First entry wins — matches getpwuid/getgrgid, which return the
            // first matching line for a duplicated id.
            out.entry(id).or_insert_with(|| name.to_string());
        }
    }
    out
}

/// Walk `fs` from its root along `components` and read the file there. Returns
/// `None` for any missing component, a non-file target, or a read error — all
/// of which just mean "no mapping available".
fn read_file_at_path(fs: &mut dyn Filesystem, components: &[&str]) -> Option<Vec<u8>> {
    let mut current = fs.root().ok()?;
    for (i, comp) in components.iter().enumerate() {
        let is_last = i + 1 == components.len();
        let children = fs.list_directory(&current).ok()?;
        let next = children.into_iter().find(|e| &e.name == comp)?;
        if is_last {
            // `/etc/passwd` is commonly a symlink on some distros; the FS
            // read_file follows or returns the target, but for our purposes a
            // plain regular file is what we expect.
            if next.entry_type != EntryType::File {
                return None;
            }
            return fs.read_file(&next, MAX_DB_BYTES).ok();
        }
        if next.entry_type != EntryType::Directory {
            return None;
        }
        current = next;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_passwd_and_group() {
        let passwd = b"# comment\nroot:x:0:0:root:/root:/bin/bash\n\
                       daemon:x:1:1:daemon:/usr/sbin:/usr/sbin/nologin\n\
                       www-data:x:33:33:www-data:/var/www:/usr/sbin/nologin\n\
                       malformed line without colons\n";
        let users = parse_colon_db(passwd);
        assert_eq!(users.get(&0).map(String::as_str), Some("root"));
        assert_eq!(users.get(&33).map(String::as_str), Some("www-data"));
        assert_eq!(users.len(), 3, "comment + malformed line must be skipped");

        let group = b"root:x:0:\nwheel:x:0:\n\
                      staff:x:50:member1,member2\n";
        let groups = parse_colon_db(group);
        // First entry for a duplicated gid wins (root before wheel).
        assert_eq!(groups.get(&0).map(String::as_str), Some("root"));
        assert_eq!(groups.get(&50).map(String::as_str), Some("staff"));
    }

    #[test]
    fn format_owner_degrades_gracefully() {
        let mut m = IdNameMap::default();
        m.users.insert(0, "root".into());
        m.groups.insert(0, "wheel".into());
        assert_eq!(m.format_owner(0, 0), "root:wheel (0:0)");
        // Known user, unknown group.
        assert_eq!(m.format_owner(0, 99), "root:99 (0:99)");
        // Unknown user, known group.
        assert_eq!(m.format_owner(0, 0), "root:wheel (0:0)");
        m.users.remove(&0);
        assert_eq!(m.format_owner(0, 0), "0:wheel (0:0)");
        m.groups.remove(&0);
        assert_eq!(m.format_owner(0, 0), "0:0");
    }

    #[test]
    fn empty_when_no_entries() {
        assert!(IdNameMap::default().is_empty());
    }

    /// A blank/whitespace name field must not produce an empty-string name.
    #[test]
    fn rejects_empty_name_field() {
        let db = b":x:5:\n";
        assert!(parse_colon_db(db).is_empty());
    }

    /// End to end over a real filesystem: build a SquashFS that carries its own
    /// `/etc/passwd` and `/etc/group`, then resolve names out of it. Exercises
    /// `read_file_at_path` walking `root -> etc -> passwd` through the actual
    /// `Filesystem` trait, not just the parser.
    #[test]
    fn resolves_names_from_a_real_image() {
        use crate::fs::squashfs::SquashfsFilesystem;
        use crate::fs::squashfs_write::{write_squashfs, BuildNode, BuildOptions};
        use std::io::Cursor;

        let passwd = b"root:x:0:0:root:/root:/bin/bash\n\
                       alice:x:1000:1000:Alice:/home/alice:/bin/bash\n"
            .to_vec();
        let group = b"root:x:0:\nstaff:x:50:alice\n".to_vec();
        let tree = BuildNode::dir(
            "/",
            0o755,
            vec![
                BuildNode::dir(
                    "etc",
                    0o755,
                    vec![
                        BuildNode::file("passwd", 0o644, passwd),
                        BuildNode::file("group", 0o644, group),
                    ],
                ),
                BuildNode::dir("home", 0o755, vec![]),
            ],
        );
        let mut cur = Cursor::new(Vec::new());
        write_squashfs(&mut cur, &tree, &BuildOptions::default()).expect("build image");
        cur.set_position(0);
        let mut fs = SquashfsFilesystem::open(cur, 0).expect("open");

        let map = IdNameMap::from_filesystem(&mut fs);
        assert!(!map.is_empty(), "should have parsed the account files");
        assert_eq!(map.user(0), Some("root"));
        assert_eq!(map.user(1000), Some("alice"));
        assert_eq!(map.group(50), Some("staff"));
        assert_eq!(map.user(1234), None, "unknown uid stays unresolved");
        assert_eq!(map.format_owner(1000, 50), "alice:staff (1000:50)");
    }

    /// An image with no `/etc/passwd` (a data volume, a non-Linux FS) must
    /// resolve to an empty map, never an error.
    #[test]
    fn image_without_etc_yields_empty_map() {
        use crate::fs::squashfs::SquashfsFilesystem;
        use crate::fs::squashfs_write::{write_squashfs, BuildNode, BuildOptions};
        use std::io::Cursor;

        let tree = BuildNode::dir(
            "/",
            0o755,
            vec![BuildNode::file("data.bin", 0o644, vec![0u8; 16])],
        );
        let mut cur = Cursor::new(Vec::new());
        write_squashfs(&mut cur, &tree, &BuildOptions::default()).expect("build");
        cur.set_position(0);
        let mut fs = SquashfsFilesystem::open(cur, 0).expect("open");
        assert!(IdNameMap::from_filesystem(&mut fs).is_empty());
    }
}
