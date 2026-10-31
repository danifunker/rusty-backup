//! Moved to the `fs-8bit` crate; re-exported so `crate::fs::lif` paths keep working.

pub use fs_8bit::lif::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
    use byteorder::{BigEndian, ByteOrder};
    use std::io::Cursor;

    fn hpux_lif() -> Vec<u8> {
        let mut img = vec![0u8; 256 * 256];
        BigEndian::write_u16(&mut img[0..2], 0x8000);
        img[2..8].copy_from_slice(b"ISL10 ");
        BigEndian::write_u32(&mut img[8..12], 16);
        BigEndian::write_u16(&mut img[12..14], 0x1000);
        BigEndian::write_u32(&mut img[16..20], 8);
        BigEndian::write_u16(&mut img[20..22], 1);
        BigEndian::write_u32(&mut img[24..28], 1);
        BigEndian::write_u32(&mut img[28..32], 1);
        BigEndian::write_u32(&mut img[32..36], 128);
        BigEndian::write_u32(&mut img[240..244], 32 * 256);
        BigEndian::write_u32(&mut img[244..248], 2048);
        BigEndian::write_u32(&mut img[248..252], 0x100);
        // Something below the files that must survive, like an LVM record.
        img[4 * 256..4 * 256 + 8].copy_from_slice(b"LVMREC01");
        let d = 16 * 256;
        img[d..d + 10].copy_from_slice(b"ISL       ");
        BigEndian::write_u16(&mut img[d + 10..d + 12], 0xCE00);
        BigEndian::write_u32(&mut img[d + 12..d + 16], 32);
        BigEndian::write_u32(&mut img[d + 16..d + 20], 4);
        BigEndian::write_u16(&mut img[d + 26..d + 28], 0x8001);
        BigEndian::write_i16(&mut img[d + 42..d + 44], -1);
        img[32 * 256..36 * 256].fill(0x11);
        img
    }

    fn put(fs: &mut LifFilesystem<Cursor<Vec<u8>>>, name: &str, data: &[u8]) {
        let root = fs.root().unwrap();
        let opts = CreateFileOptions::default();
        fs.create_file(&root, name, &mut &data[..], data.len() as u64, &opts)
            .unwrap();
        fs.sync_metadata().unwrap();
    }

    fn replace(fs: &mut LifFilesystem<Cursor<Vec<u8>>>, name: &str, data: &[u8]) {
        use crate::fs::replace::{create_or_replace, OnConflict, ReplacePolicy};
        let root = fs.root().unwrap();
        let policy = ReplacePolicy {
            on_conflict: OnConflict::Replace,
            ..Default::default()
        };
        let opts = CreateFileOptions::default();
        create_or_replace(
            fs,
            &root,
            name,
            &mut &data[..],
            data.len() as u64,
            &opts,
            policy,
        )
        .unwrap();
        fs.sync_metadata().unwrap();
    }

    /// The app's staged replace keeps a file's type, implementation word and IPL pointer.
    #[test]
    fn staged_replace_keeps_type_impl_word_and_ipl() {
        let mut img = hpux_lif();
        BigEndian::write_u32(&mut img[16 * 256 + 28..], 0xFFF0_0000);
        let mut fs = LifFilesystem::open_editable(Cursor::new(img), 0, None).unwrap();
        put(&mut fs, "LABEL", b"x");
        let mut img = fs.into_inner().into_inner();
        let dir = BigEndian::read_u32(&img[8..12]) as usize * 256;
        let label = parse_lif(&mut Cursor::new(&img), 0)
            .unwrap()
            .files
            .iter()
            .find(|f| f.name == "LABEL")
            .unwrap()
            .slot;
        BigEndian::write_u16(&mut img[dir + label * 32 + 10..], 0xA271);
        let mut fs = LifFilesystem::open_editable(Cursor::new(img), 0, None).unwrap();
        replace(&mut fs, "label", b"y");
        replace(&mut fs, "isl", &[0x44; 5000]);
        let img = fs.into_inner().into_inner();
        let vol = parse_lif(&mut Cursor::new(&img), 0).unwrap();
        let names: Vec<_> = vol.files.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["ISL", "LABEL"], "no temporaries left behind");
        let get = |n: &str| vol.files.iter().find(|f| f.name == n).unwrap().clone();
        assert_eq!(get("LABEL").file_type, 0xA271);
        let isl = get("ISL");
        assert_eq!(isl.file_type, 0xCE00);
        assert_eq!(
            BigEndian::read_u32(&img[dir + isl.slot * 32 + 28..]),
            0xFFF0_0000
        );
        assert_eq!(BigEndian::read_u32(&img[240..]) as u64, isl.start_byte);
        assert_eq!(BigEndian::read_u32(&img[244..]), 6144);
    }
}
