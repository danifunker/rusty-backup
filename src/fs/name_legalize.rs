//! Moved to the `disk-core` crate; re-exported so `crate::fs::name_legalize` paths keep working.

pub use disk_core::name_legalize::*;

#[cfg(test)]
mod tests {
    use super::*;

    /// Names seen on real classic-Mac and DOS volumes, pushed through the real validators.
    #[test]
    fn vintage_names_become_legal_on_every_real_destination() {
        use crate::fs::filesystem::Filesystem;
        use std::io::Cursor;
        let corpus = [
            "Acquire/Export",
            "Schwarz/Weiss",
            "MacDRUMS Instruments/Tracks",
            "Icon\r",
            "Read Me!",
            "R\u{e9}sum\u{e9}\u{2122} \u{192}",
            "\u{F8FF} Menu Items",
            "a:b",
            "CON",
            "lpt1.txt",
            "..",
            "   ",
            "trailing.",
            "*?<>|",
            "1st file",
            "file.tar.gz",
            "Anti-Aliased PICT",
            "An extraordinarily long name that exceeds every limit.html",
        ];
        let fat = crate::fs::fat::FatFilesystem::open(
            Cursor::new(crate::fs::fat::create_blank_fat(737280, Some("T")).unwrap()),
            0,
        )
        .unwrap();
        let hfs = crate::fs::hfs::HfsFilesystem::open(
            Cursor::new(crate::fs::hfs::create_blank_hfs(8 << 20, 4096, "T").unwrap()),
            0,
        )
        .unwrap();
        let prodos = crate::fs::prodos::ProDosFilesystem::open(
            Cursor::new(crate::fs::prodos::create_blank_prodos(143_360, "T").unwrap()),
            0,
        )
        .unwrap();
        let mfs = crate::fs::mfs::MfsFilesystem::open(
            Cursor::new(crate::fs::mfs::create_blank_mfs(409_600, "T").unwrap()),
            0,
        )
        .unwrap();
        type Check<'a> = Box<NameValidator<'a>>;
        let validators: Vec<(&str, Check<'_>)> = vec![
            ("FAT", Box::new(|n: &str| fat.validate_name(n))),
            ("HFS", Box::new(|n: &str| hfs.validate_name(n))),
            ("ProDOS", Box::new(|n: &str| prodos.validate_name(n))),
            ("MFS", Box::new(|n: &str| mfs.validate_name(n))),
            (
                "Atari DOS",
                Box::new(|n: &str| crate::fs::atari_dos::encode_name(n).map(|_| ())),
            ),
            (
                "CBM",
                Box::new(|n: &str| crate::fs::cbm::encode_petscii_name(n).map(|_| ())),
            ),
            ("HPFS", Box::new(crate::fs::hpfs::validate_hpfs_name)),
            (
                "ext",
                Box::new(|n: &str| validate_posix_name(n, 255, "ext")),
            ),
        ];
        for (fs, v) in &validators {
            for name in corpus {
                let out = legalize_name(v.as_ref(), name)
                    .unwrap_or_else(|e| panic!("{fs}: no legal name for {name:?}: {e}"));
                assert!(v(&out).is_ok(), "{fs}: {name:?} -> {out:?} still invalid");
            }
        }
        // HFS keeps slashes (legal there); FAT does not.
        let hfs_v = |n: &str| hfs.validate_name(n);
        assert_eq!(
            legalize_name(&hfs_v, "Acquire/Export").unwrap(),
            "Acquire/Export"
        );
        let fat_v = |n: &str| fat.validate_name(n);
        assert_eq!(
            legalize_name(&fat_v, "Acquire/Export").unwrap(),
            "Acquire_Export"
        );
        assert_eq!(legalize_name(&fat_v, "Icon\r").unwrap(), "Icon_");
        assert_eq!(legalize_name(&fat_v, "CON").unwrap(), "CON_");
        let prodos_v = |n: &str| prodos.validate_name(n);
        assert_eq!(legalize_name(&prodos_v, "Read Me!").unwrap(), "Read.Me.");
    }
}
