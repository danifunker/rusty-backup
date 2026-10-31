//! Moved to the `disk-core` crate; re-exported so `crate::fs::id_names` paths keep working.

pub use disk_core::id_names::*;

#[cfg(test)]
mod tests {
    use super::*;

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
