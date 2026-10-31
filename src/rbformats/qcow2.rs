//! Moved to the `dsk-vm` crate; re-exported so `crate::rbformats::qcow2` paths keep working.

pub use dsk_vm::qcow2::*;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// **Phase 2 acceptance test** (docs/virtualization-formats.md, session 2.4).
    ///
    /// Round-trip a UTM-shape APM + classic-HFS disk through every QCOW2
    /// surface: build the source in memory → `export_qcow2` → reopen as a
    /// `Qcow2Reader<File>` → host an `EditableFilesystem` over the inner HFS
    /// partition → create a file via the HFS editor → close → `qemu-img
    /// check` → reopen read-only → confirm the new file is visible with the
    /// expected payload. This exercises the writer, the in-place editor
    /// (allocate-on-write into clusters the writer left unmapped), and the
    /// `Send + 'static` host requirement of `open_editable_filesystem` all in
    /// one path.
    #[test]
    fn acceptance_apm_hfs_round_trip_through_qcow2_edit() {
        use crate::fs::filesystem::CreateFileOptions;
        use crate::fs::hfs::create_blank_hfs;
        use crate::fs::hfs_clone::emit_apm_disk_with_hfs;
        use crate::partition::apm::build_minimal_apm;

        // 1. Build a small APM disk with one Apple_HFS partition. 4096
        //    sectors × 512 B = 2 MiB. Apple_HFS at sector 96 (= 0xC000).
        let total_blocks: u32 = 4096;
        let hfs_start: u32 = 96;
        let mut apm = build_minimal_apm(
            &[
                ("Apple_Driver43".into(), 32, 8),
                ("Apple_HFS".into(), hfs_start, total_blocks - hfs_start),
            ],
            512,
            total_blocks,
        );
        apm.entries[2].name = "MacOS".into();
        let apm_blocks = apm.build_apm_blocks(Some(total_blocks));
        let mut source = vec![0u8; total_blocks as usize * 512];
        source[..apm_blocks.len()].copy_from_slice(&apm_blocks);
        let hfs = create_blank_hfs((total_blocks - hfs_start) as u64 * 512, 4096, "Acceptance")
            .expect("create_blank_hfs");
        let mut raw_disk: Vec<u8> = Vec::new();
        let src_len = source.len() as u64;
        emit_apm_disk_with_hfs(
            &mut Cursor::new(source),
            src_len,
            &hfs,
            &mut Cursor::new(&mut raw_disk),
        )
        .expect("emit_apm_disk_with_hfs");
        let disk_size = raw_disk.len() as u64;
        let hfs_partition_offset = (hfs_start as u64) * 512;

        // 2. Export the raw disk to a QCOW2 tempfile via the writer.
        let tmp = tempfile::NamedTempFile::new().unwrap();
        {
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(tmp.path())
                .unwrap();
            export_qcow2(
                &mut Cursor::new(&raw_disk),
                &mut f,
                disk_size,
                0, // default 64 KiB clusters
                &mut |_| {},
                &|| false,
            )
            .expect("export_qcow2");
        }

        // 3. Open the QCOW2 read-write as a `Qcow2Reader<File>` and host the
        //    Apple_HFS partition through `open_editable_filesystem`. This is
        //    the same shape the GUI / CLI would use once browse-view edit
        //    learns container readers — Session 2.4 ships the engine path;
        //    the browse-view integration is an incremental follow-up.
        let payload = b"hello from qcow2 in-place edit acceptance test\n";
        let file_name = "GREETING.TXT";
        {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(tmp.path())
                .unwrap();
            let reader = Qcow2Reader::open(f).expect("Qcow2Reader::open r+w");
            let mut efs = crate::fs::open_editable_filesystem(
                reader,
                hfs_partition_offset,
                0, // partition_type byte ignored for APM (type_string drives)
                Some("Apple_HFS"),
            )
            .expect("open_editable_filesystem over Qcow2Reader");
            let root = efs.root().expect("root");
            let opts = CreateFileOptions::default();
            let mut data_reader = std::io::Cursor::new(payload);
            efs.create_file(
                &root,
                file_name,
                &mut data_reader,
                payload.len() as u64,
                &opts,
            )
            .expect("create_file");
            efs.sync_metadata().expect("sync_metadata");
        }

        // 4. External validator: qemu-img check must be happy with the edited
        //    image (refcounts consistent, no truncation). Gated like all the
        //    other qemu-img tests — skip cleanly when qemu-img isn't present.
        match crate::rbformats::qemu_img_test::qemu_img_check(tmp.path()) {
            Ok(true) => {}
            Ok(false) => eprintln!("skipping qemu-img check — `qemu-img` not on PATH"),
            Err(e) => panic!("qemu-img check failed on edited APM+HFS image: {e}"),
        }

        // 5. Reopen the QCOW2 read-only, walk the HFS catalog, find the file.
        let f = std::fs::File::open(tmp.path()).unwrap();
        let reader = Qcow2Reader::open(f).unwrap();
        let mut fs = crate::fs::open_filesystem(reader, hfs_partition_offset, 0, Some("Apple_HFS"))
            .expect("open_filesystem after edit");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let entry = entries
            .iter()
            .find(|e| e.name == file_name)
            .unwrap_or_else(|| panic!("file {file_name} not visible after round-trip"));
        let buf = fs.read_file(entry, payload.len() * 2).unwrap();
        assert_eq!(buf, payload, "payload mismatch after QCOW2 edit + reopen");
    }
}
