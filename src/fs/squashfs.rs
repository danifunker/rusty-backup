//! Moved to the `fs-squashfs` crate; re-exported so `crate::fs::squashfs` paths keep working.

pub use fs_squashfs::squashfs::*;

#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom};

    /// The routing layer must recognise a SquashFS partition on its own —
    /// callers go through `open_filesystem`, never the concrete type.
    #[test]
    fn dispatch_routes_a_real_image_to_squashfs() {
        let path = std::env::var("RB_SQUASHFS_ISO").unwrap_or_else(|_| {
            "/Users/dani/Downloads/ubuntu-12.04-desktop-powerpc.iso".to_string()
        });
        let Ok(mut file) = std::fs::File::open(&path) else {
            eprintln!("{path} not present — skipping");
            return;
        };
        let Some(offset) = find_squashfs_offset(&mut file) else {
            return;
        };

        // Type byte 0 is the auto-detect path; 0x83 is how an appliance image
        // stamps a Linux root partition.
        for type_byte in [0x00u8, 0x83u8] {
            let f = std::fs::File::open(&path).expect("reopen");
            let mut fs = crate::fs::open_filesystem(f, offset, type_byte, None)
                .unwrap_or_else(|e| panic!("dispatch failed for type 0x{type_byte:02X}: {e}"));
            assert_eq!(fs.fs_type(), "SquashFS");
            let root = fs.root().expect("root");
            assert!(!fs.list_directory(&root).expect("list").is_empty());
        }

        // And the 0x83 probe should name it for the partition table view.
        let mut f = std::fs::File::open(&path).expect("reopen");
        assert_eq!(
            crate::fs::probe_0x83_fs_type(&mut f, offset),
            Some("SquashFS")
        );
    }

    /// Scan 2048-byte boundaries for a SquashFS superblock.
    fn find_squashfs_offset(file: &mut std::fs::File) -> Option<u64> {
        use std::io::Read as _;
        let mut pos = 0u64;
        let mut buf = vec![0u8; 1 << 22];
        loop {
            file.seek(SeekFrom::Start(pos)).ok()?;
            let n = file.read(&mut buf).ok()?;
            if n == 0 {
                return None;
            }
            let mut i = 0;
            while i + 4 <= n {
                if buf[i..i + 4] == 0x7371_7368u32.to_le_bytes() {
                    return Some(pos + i as u64);
                }
                i += 2048;
            }
            pos += n as u64;
        }
    }
}
