fn slice(hidden: u64, sectors: u64, fs: &str) -> GhoPartitionSlice {
    GhoPartitionSlice {
        boot_record_index: 0,
        record_range: 0..0,
        partition_size: sectors * 512,
        hidden_sectors: hidden,
        fs_type: fs.to_string(),
        reserved_record_index: None,
    }
}

fn entry(sector: &[u8; 512], i: usize) -> (u8, u32, u32) {
    let o = 446 + i * 16;
    let le = |a: usize| u32::from_le_bytes(sector[a..a + 4].try_into().unwrap());
    (sector[o + 4], le(o + 8), le(o + 12))
}

#[test]
fn the_reader_is_send_without_an_unsafe_impl() {
    fn is_send<T: Send>() {}
    is_send::<GhoReader>();
}

#[test]
fn two_primaries_get_two_mbr_entries_and_no_ebr() {
    let t =
        synthesize_partition_table(&[slice(63, 1000, "FAT16"), slice(1063, 500, "NTFS")]).unwrap();
    assert_eq!(t.len(), 1, "no EBR over the first partition's last track");
    assert_eq!(entry(&t[&0], 0), (0x0E, 63, 1000));
    assert_eq!(entry(&t[&0], 1), (0x07, 1063, 500));
    // With a free gap the second is logical, as Ghost's source disks have it.
    let t =
        synthesize_partition_table(&[slice(63, 1000, "FAT16"), slice(1126, 500, "NTFS")]).unwrap();
    assert_eq!(entry(&t[&0], 1).0, MBR_TYPE_EXTENDED_LBA);
    assert_eq!(entry(&t[&1063], 0), (0x07, 63, 500));
}

#[test]
fn logicals_need_room_for_their_ebr() {
    let five: Vec<_> = (0..5)
        .map(|i| slice(63 + i * 1063, 1000, "FAT32"))
        .collect();
    let t = synthesize_partition_table(&five).unwrap();
    assert_eq!(t.len(), 5);
    assert_eq!(entry(&t[&0], 1).0, MBR_TYPE_EXTENDED_LBA);
    assert_eq!(entry(&t[&1063], 0), (0x0C, 63, 1000));
    let tight: Vec<_> = (0..5)
        .map(|i| slice(63 + i * 1000, 1000, "FAT32"))
        .collect();
    assert!(synthesize_partition_table(&tight).is_err());
    assert!(
        synthesize_partition_table(&[slice(63, 1000, "FAT32"), slice(500, 10, "FAT32")]).is_err()
    );
}

#[test]
fn an_ntfs_boot_sector_is_sized_from_its_own_field() {
    let mut b = [0u8; 512];
    b[3..11].copy_from_slice(b"NTFS    ");
    b[11..13].copy_from_slice(&512u16.to_le_bytes());
    b[28..32].copy_from_slice(&2048u32.to_le_bytes());
    b[0x28..0x30].copy_from_slice(&99_999u64.to_le_bytes());
    assert_eq!(vbr_geometry(&b), (2048, 100_000 * 512, "NTFS".to_string()));
}

/// A synthetic High-compression NTFS Ghost stream: junk, the MFT run header, `mft`, a tail.
fn ghost_ntfs_stream(mft: &[u8], cluster: u64, block: usize) -> Vec<u8> {
    let mut plain = vec![0x55u8; 300];
    let mut hdr = [0u8; NTFS_RUN_HEADER_SIZE];
    hdr[..16].copy_from_slice(&NTFS_RUN_HEADER_ENTRY1);
    hdr[16] = 0x0E;
    hdr[17] = 0x02;
    hdr[31] = 0x0F;
    hdr[32] = 0x0F;
    hdr[33..41].copy_from_slice(&(mft.len() as u64 / cluster).to_le_bytes());
    hdr[41] = 0x0E;
    plain.extend_from_slice(&hdr);
    plain.extend_from_slice(mft);
    plain.extend(disk_core::repeat_n(0xA5u8, 5000));
    let mut file = vec![0u8; 512];
    for piece in plain.chunks(block) {
        let mut enc = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
        std::io::Write::write_all(&mut enc, piece).unwrap();
        file.extend_from_slice(&enc.finish().unwrap());
        file.extend_from_slice(&[0, 0]);
    }
    file.extend_from_slice(&[0u8; 64]);
    file
}

#[test]
fn compressed_ntfs_mft_parses_the_same_whatever_the_block_split() {
    use fs_pc::ntfs::{apply_fixup, parse_mft_attributes, parse_vbr};
    use fs_pc::ntfs_format::{create_ntfs, NtfsFormatParams, NtfsGeometry};
    let mut img = std::io::Cursor::new(Vec::new());
    create_ntfs(
        &mut img,
        &NtfsFormatParams {
            total_size: 16 * 1024 * 1024,
            geometry: NtfsGeometry::with_cluster_size(4096, 512).unwrap(),
            mft_records_hint: 128,
            label: None,
        },
    )
    .unwrap();
    let img = img.into_inner();
    let mut vbr_raw = [0u8; 512];
    vbr_raw.copy_from_slice(&img[..512]);
    let vbr = parse_vbr(&vbr_raw).unwrap();
    let cluster = vbr.bytes_per_sector * vbr.sectors_per_cluster;
    let at = (vbr.mft_cluster * cluster) as usize;
    let mft = &img[at..at + 16 * 1024];

    // What the MFT says, parsed straight from the image.
    let rs = vbr.mft_record_size as usize;
    let mut want = Vec::new();
    for rec in mft.chunks_exact(rs) {
        let num = u32::from_le_bytes([rec[44], rec[45], rec[46], rec[47]]);
        if &rec[..4] != b"FILE" || num == 8 {
            continue;
        }
        let mut r = rec.to_vec();
        let _ = apply_fixup(&mut r);
        for a in parse_mft_attributes(&r, vbr.mft_record_size) {
            if a.resident {
                continue;
            }
            for (i, dr) in a.data_runs.iter().enumerate() {
                let skip = (num == 0 && a.attr_type == 0x80 && i == 0)
                    || (num == 1 && a.attr_type == 0x80);
                if !skip && dr.cluster_offset > 0 && dr.length > 0 {
                    want.push((dr.cluster_offset as u64, dr.length));
                }
            }
        }
    }
    assert!(!want.is_empty());

    let dir = tempfile::tempdir().unwrap();
    // 700-byte blocks split the run header and every MFT record across blocks.
    for block in [700usize, 1 << 20] {
        let path = dir.path().join(format!("t{block}.gho"));
        let file = ghost_ntfs_stream(mft, cluster, block);
        std::fs::write(&path, &file).unwrap();
        let mut reader = SpanReader::open(&[path]).unwrap();
        let (state, index) = open_ntfs_compressed_mft_only(
            &mut reader,
            512,
            file.len() as u64,
            GhoCompression::High,
            &vbr,
            &vbr_raw,
        )
        .unwrap();
        let lazy = state.lazy_scan.expect("lazy scan state");
        assert_eq!(lazy.data_run_queue, want, "block size {block}");
        let mds = 300 + NTFS_RUN_HEADER_SIZE as u64;
        assert!(index
            .runs
            .iter()
            .any(|r| r.lcn_start == vbr.mft_cluster && r.file_offset == mds));
        assert_eq!(lazy.prev_data_end, mds + mft.len() as u64);
        // What follows the MFT stays for the lazy scan, starting with the tail's first byte.
        assert_eq!(lazy.stream.first(), Some(&0xA5));
    }
}
use super::*;
use std::io::{Cursor, Write};

#[test]
fn gap_records_are_found_across_the_scan_window() {
    use fs_pc::ntfs::{apply_fixup, parse_mft_attributes, parse_vbr};
    use fs_pc::ntfs_format::{create_ntfs, NtfsFormatParams, NtfsGeometry};
    let mut img = std::io::Cursor::new(Vec::new());
    create_ntfs(
        &mut img,
        &NtfsFormatParams {
            total_size: 16 * 1024 * 1024,
            geometry: NtfsGeometry::with_cluster_size(4096, 512).unwrap(),
            mft_records_hint: 128,
            label: None,
        },
    )
    .unwrap();
    let img = img.into_inner();
    let mut vbr_raw = [0u8; 512];
    vbr_raw.copy_from_slice(&img[..512]);
    let vbr = parse_vbr(&vbr_raw).unwrap();
    let cluster = vbr.bytes_per_sector * vbr.sectors_per_cluster;
    let at = (vbr.mft_cluster * cluster) as usize;
    let mft = &img[at..at + 16 * 1024];
    let rs = vbr.mft_record_size as usize;
    let mut want = Vec::new();
    for rec in mft.chunks_exact(rs) {
        let num = u32::from_le_bytes([rec[44], rec[45], rec[46], rec[47]]);
        if &rec[..4] != b"FILE" || num == 8 {
            continue;
        }
        let mut r = rec.to_vec();
        let _ = apply_fixup(&mut r);
        for a in parse_mft_attributes(&r, vbr.mft_record_size) {
            if !a.resident {
                for dr in &a.data_runs {
                    if dr.cluster_offset > 0 && dr.length > 0 {
                        want.push((dr.cluster_offset as u64, dr.length));
                    }
                }
            }
        }
    }
    // Records start 300 bytes before the 4 MiB mark, so one straddles it.
    let mut gap = vec![0x11u8; 4 * 1024 * 1024 - 300];
    gap.extend_from_slice(mft);
    gap.extend(disk_core::repeat_n(0x22u8, 1000));
    let mut got = Vec::new();
    parse_inline_mft_records(
        &mut Cursor::new(gap.clone()),
        0,
        gap.len() as u64,
        &vbr,
        &mut got,
    );
    assert_eq!(got, want);
}

/// Helper: a minimal `CachedNtfsScan` for cache round-trip tests.
fn dummy_scan(run_count: usize) -> CachedNtfsScan {
    CachedNtfsScan {
        runs: (0..run_count as u64)
            .map(|i| NtfsGhoClusterRun {
                lcn_start: i * 10,
                cluster_count: 10,
                file_offset: i * 4096,
                seq: 0,
            })
            .collect(),
        blocks: Vec::new(),
        compression: GhoCompression::High,
        total_decompressed: 1024,
        volume_size: 1 << 20,
        cluster_size: 4096,
        vbr: [0u8; 512],
        bitmap_runs: Vec::new(),
        synth_bitmap: Vec::new(),
        boot_code: Vec::new(),
    }
}

#[test]
fn ntfs_backup_boot_sector_lives_in_the_trailing_sector() {
    // A reconstructed NTFS volume must run to total_sectors + 1 sectors, with
    // the VBR copy (backup boot sector) in that final trailing sector — where
    // NTFS, chkdsk, and the Windows boot path all look for it. total_sectors
    // (VBR 0x28) deliberately EXCLUDES that sector, so the served volume is
    // one sector longer than index.volume_size.
    let bps: u64 = 512;
    let total_sectors: u64 = 100;
    let cluster_size: u64 = 4096;

    let mut vbr = [0u8; 512];
    vbr[3..11].copy_from_slice(b"NTFS    ");
    vbr[0x0B..0x0D].copy_from_slice(&(bps as u16).to_le_bytes());
    vbr[0x28..0x30].copy_from_slice(&total_sectors.to_le_bytes());
    vbr[0xAB] = 0xAB; // a recognizable boot-code byte past the BPB
    vbr[510] = 0x55;
    vbr[511] = 0xAA;

    let mut index = NtfsGhoIndex {
        runs: Vec::new(), // no mapped runs -> normal reads return zeros
        volume_size: total_sectors * bps,
        cluster_size,
        vbr,
        bitmap_runs: Vec::new(),
        synth_bitmap: Vec::new(),
        boot_code: Vec::new(),
    };

    // The served length includes the trailing backup-boot sector.
    assert_eq!(ntfs_served_size(&index), total_sectors * bps + bps);

    // runs is empty, so `inner` is never actually read below; SpanReader still
    // needs a backing file to exist.
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("dummy.bin");
    std::fs::write(&p, [0u8; 4096]).unwrap();
    let mut inner = SpanReader::open(std::slice::from_ref(&p)).unwrap();
    let mut hint = 0usize;
    let mut comp: Option<NtfsCompressedState> = None;

    let mut read_at = |inner: &mut SpanReader, index: &mut NtfsGhoIndex, pos: u64| -> Vec<u8> {
        let mut out = [0u8; 512];
        let n =
            read_ntfs_file_aware_into(inner, index, &mut hint, &mut comp, pos, &mut out).unwrap();
        out[..n].to_vec()
    };

    // The backup boot sector at `volume_size` mirrors the VBR exactly.
    let backup = read_at(&mut inner, &mut index, total_sectors * bps);
    assert_eq!(backup.len(), bps as usize);
    assert_eq!(
        &backup[..],
        &vbr[..],
        "trailing sector must be the VBR copy"
    );

    // The OLD (buggy) location, one sector earlier, must now be ordinary
    // volume data (zeros here) — NOT a stray, misplaced VBR copy.
    let old_loc = read_at(&mut inner, &mut index, (total_sectors - 1) * bps);
    assert!(
        old_loc.iter().all(|&b| b == 0),
        "the sector before the end must not carry a misplaced backup VBR"
    );

    // Sector 0 remains the primary VBR.
    let sector0 = read_at(&mut inner, &mut index, 0);
    assert_eq!(&sector0[..16], &vbr[..16], "sector 0 must be the VBR");
}

#[test]
fn match_inline_run_consumes_fresh_group_in_mft_order() {
    use std::collections::HashSet;
    // Reproduces the $Secure (file 9) scenario from smallNTFS.GHO: a stale
    // orphaned (1310297,1) entry from an earlier gap sits ahead of the
    // freshest gap's two same-size runs — $SDH (1310298) then $SII (1310299)
    // in MFT attribute order. The two cc=1 stream runs that follow must be
    // matched to 1310298 then 1310299, in that order. Matching from the back
    // would swap them; matching from the front of the whole list would grab
    // the stale 1310297.
    let mut list = vec![(1310297u64, 1u64), (1310298, 1), (1310299, 1)];
    let mut group_start = 1; // freshest gap appended entries 1..=2
    let assigned: HashSet<u64> = HashSet::new();

    let first = match_inline_run(&mut list, &mut group_start, 1, &assigned).unwrap();
    assert_eq!(first, 1310298, "$SDH must take the lower (first) LCN");
    let mut assigned = HashSet::new();
    assigned.insert(first);
    let second = match_inline_run(&mut list, &mut group_start, 1, &assigned).unwrap();
    assert_eq!(second, 1310299, "$SII must take the higher (second) LCN");
}

#[test]
fn match_inline_run_falls_back_to_older_orphans() {
    use std::collections::HashSet;
    // Fresh group has no cc=3 match, so an older orphaned entry is used.
    let mut list = vec![(100u64, 3u64), (200, 1), (201, 1)];
    let mut group_start = 1;
    let assigned: HashSet<u64> = HashSet::new();
    let lcn = match_inline_run(&mut list, &mut group_start, 3, &assigned).unwrap();
    assert_eq!(lcn, 100);
    // Removing an entry before group_start shifts the boundary down.
    assert_eq!(group_start, 0);
}

#[test]
fn match_inline_run_skips_assigned_and_preserves_fragment_order() {
    use std::collections::HashSet;
    // A fragmented file's $DATA: four same-size extents in MFT order. They
    // must be handed out in that order, not reversed.
    let mut list = vec![(50u64, 4u64), (10, 4), (90, 4), (30, 4)];
    let mut group_start = 0;
    let mut assigned: HashSet<u64> = HashSet::new();
    let mut got = Vec::new();
    for _ in 0..4 {
        let lcn = match_inline_run(&mut list, &mut group_start, 4, &assigned).unwrap();
        assigned.insert(lcn);
        got.push(lcn);
    }
    assert_eq!(got, vec![50, 10, 90, 30]);
}

#[test]
fn ntfs_scan_key_tracks_len_and_mtime() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.gho");
    std::fs::write(&a, b"hello").unwrap();

    let k1 = NtfsScanKey::from_span_set(std::slice::from_ref(&a)).expect("key");
    // Same file, unchanged -> identical key.
    let k2 = NtfsScanKey::from_span_set(std::slice::from_ref(&a)).expect("key");
    assert_eq!(k1, k2);

    // Different length -> different key (so a re-backup is never reused).
    std::fs::write(&a, b"hello world").unwrap();
    let k3 = NtfsScanKey::from_span_set(std::slice::from_ref(&a)).expect("key");
    assert_ne!(k1, k3);

    // Missing file -> no key (caching disabled).
    let missing = dir.path().join("nope.gho");
    assert!(NtfsScanKey::from_span_set(std::slice::from_ref(&missing)).is_none());
}

#[test]
fn ntfs_scan_cache_round_trips_and_evicts() {
    // Use unique synthetic keys so this test doesn't collide with others
    // sharing the process-wide cache.
    let key = |tag: &str| NtfsScanKey(vec![(PathBuf::from(tag), 1, 1)]);

    let k = key("round-trip-unique-archive");
    assert!(ntfs_scan_cache_get(&k).is_none());

    let index = NtfsGhoIndex {
        runs: dummy_scan(3).runs,
        volume_size: 1 << 20,
        cluster_size: 4096,
        vbr: [0u8; 512],
        bitmap_runs: Vec::new(),
        synth_bitmap: Vec::new(),
        boot_code: Vec::new(),
    };
    let comp = NtfsCompressedState {
        blocks: Vec::new(),
        compression: GhoCompression::High,
        total_decompressed: 1024,
        cache_block: None,
        cache_buf: Vec::new(),
        lazy_scan: None,
        cache_key: Some(k.clone()),
    };
    ntfs_scan_cache_store(&k, &index, &comp);

    let got = ntfs_scan_cache_get(&k).expect("cached");
    assert_eq!(got.runs.len(), 3);
    assert_eq!(got.volume_size, 1 << 20);

    // Insert more than the cap of distinct keys; the first inserted one
    // must be evicted (FIFO).
    for i in 0..NTFS_SCAN_CACHE_CAP + 2 {
        let kk = key(&format!("evict-unique-{i}"));
        let c = NtfsCompressedState {
            blocks: Vec::new(),
            compression: GhoCompression::High,
            total_decompressed: 0,
            cache_block: None,
            cache_buf: Vec::new(),
            lazy_scan: None,
            cache_key: Some(kk.clone()),
        };
        ntfs_scan_cache_store(&kk, &index, &c);
    }
    // The very first evict key is gone; the most recent survives.
    assert!(ntfs_scan_cache_get(&key("evict-unique-0")).is_none());
    assert!(
        ntfs_scan_cache_get(&key(&format!("evict-unique-{}", NTFS_SCAN_CACHE_CAP + 1))).is_some()
    );
}

/// Build a synthetic GHO header for testing. Returns the prefix-sector
/// (512 bytes) so callers can drop in description bytes at 0xFF if they
/// like.
fn build_header(comp: u8, image_type: u8, password_flag: u8, description: Option<&str>) -> Vec<u8> {
    let mut buf = vec![0u8; 512];
    buf[0] = 0xFE;
    buf[1] = 0xEF;
    buf[2] = 0x01; // container version
    buf[3] = comp;
    buf[4..8].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
    buf[8] = 0x01;
    buf[9] = 0x01;
    buf[10] = image_type;
    buf[11] = password_flag;
    if password_flag == 0x01 {
        // 16-byte verifier follows the prefix at offset 12.
        buf[12..28].fill(0xAB);
    }
    if let Some(desc) = description {
        let bytes = desc.as_bytes();
        let max = 512 - 0xFF - 1; // leave room for NUL
        assert!(bytes.len() <= max);
        buf[0xFF..0xFF + bytes.len()].copy_from_slice(bytes);
        // NUL terminator (buf is already zeroed).
    }
    buf
}

#[test]
fn parses_minimal_uncompressed_file_aware_header() {
    let buf = build_header(0x00, 0x00, 0x00, None);
    let header = GhoContainerHeader::parse(&mut Cursor::new(buf)).unwrap();
    assert_eq!(header.container_version, 0x01);
    assert_eq!(header.compression, GhoCompression::None);
    assert_eq!(header.serial, 0xDEAD_BEEF);
    assert_eq!(header.flags, [0x01, 0x01]);
    assert_eq!(header.image_type, GhoImageType::FileAware);
    assert!(!header.password_protected);
    assert!(header.password_verifier.is_none());
    assert_eq!(header.description, None);
}

#[test]
fn parses_high_compression_sector_header() {
    let buf = build_header(0x03, 0x01, 0x00, None);
    let header = GhoContainerHeader::parse(&mut Cursor::new(buf)).unwrap();
    assert_eq!(header.compression, GhoCompression::High);
    assert_eq!(header.image_type, GhoImageType::Sector);
}

#[test]
fn parses_password_verifier() {
    let buf = build_header(0x00, 0x00, 0x01, None);
    let header = GhoContainerHeader::parse(&mut Cursor::new(buf)).unwrap();
    assert!(header.password_protected);
    assert_eq!(header.password_verifier, Some([0xAB; 16]));
}

#[test]
fn parses_nul_terminated_description() {
    let buf = build_header(0x00, 0x00, 0x00, Some("PartitionBackup no compression"));
    let header = GhoContainerHeader::parse(&mut Cursor::new(buf)).unwrap();
    assert_eq!(
        header.description.as_deref(),
        Some("PartitionBackup no compression")
    );
}

#[test]
fn rejects_wrong_magic() {
    let mut buf = build_header(0x00, 0x00, 0x00, None);
    buf[0] = 0xCA;
    buf[1] = 0xFE;
    let err = GhoContainerHeader::parse(&mut Cursor::new(buf)).unwrap_err();
    assert!(format!("{err:#}").contains("not a Norton Ghost container"));
}

#[test]
fn rejects_unknown_password_flag() {
    let mut buf = build_header(0x00, 0x00, 0x00, None);
    buf[11] = 0x42;
    let err = GhoContainerHeader::parse(&mut Cursor::new(buf)).unwrap_err();
    assert!(format!("{err:#}").contains("password_flag"));
}

#[test]
fn surfaces_unknown_compression_byte_via_other() {
    let buf = build_header(0x07, 0x00, 0x00, None);
    let header = GhoContainerHeader::parse(&mut Cursor::new(buf)).unwrap();
    assert_eq!(header.compression, GhoCompression::Other(0x07));
}

#[test]
fn surfaces_unknown_image_type_byte_via_other() {
    let buf = build_header(0x00, 0x05, 0x00, None);
    let header = GhoContainerHeader::parse(&mut Cursor::new(buf)).unwrap();
    assert_eq!(header.image_type, GhoImageType::Other(0x05));
}

// ---------- materializer-stub error-shape tests ----------

fn write_to_temp(bytes: &[u8]) -> std::path::PathBuf {
    // Keep the directory without `TempDir::keep` (tempfile 3.20+; the 1.73 builds lock older).
    let guard = tempfile::tempdir().unwrap();
    let dir = guard.path().to_path_buf();
    std::mem::forget(guard);
    let path = dir.join("test.GHO");
    let mut f = File::create(&path).unwrap();
    f.write_all(bytes).unwrap();
    path
}

#[test]
fn materialize_password_protected_errors_cleanly() {
    let buf = build_header(0x00, 0x00, 0x01, None);
    let path = write_to_temp(&buf);
    let err = materialize_gho_to_temp(&path).unwrap_err();
    let msg = format!("{err:#}").to_lowercase();
    assert!(
        msg.contains("password"),
        "error should mention password, got: {msg}"
    );
}

#[test]
fn materialize_sector_mode_empty_returns_empty_image() {
    // SECTOR mode with no data sectors past the container header is
    // a valid (if degenerate) input; decoder produces a zero-length
    // output rather than erroring.
    let buf = build_header(0x00, 0x01, 0x00, None);
    let path = write_to_temp(&buf);
    let mat = materialize_gho_to_temp(&path).expect("SECTOR mode should decode");
    assert_eq!(mat.logical_size, 0);
    assert_eq!(mat.partition_count, 1);
}

#[test]
fn materialize_file_aware_empty_stream_errors_on_missing_boot_sector() {
    // A bare file-aware header with no inner records has no boot
    // sector → the emitter can't size the output image. We expect a
    // clean error mentioning "boot sector", not a panic.
    let buf = build_header(0x00, 0x00, 0x00, None);
    let path = write_to_temp(&buf);
    let err = materialize_gho_to_temp(&path).unwrap_err();
    let msg = format!("{err:#}").to_lowercase();
    assert!(
        msg.contains("boot sector") || msg.contains("ntfs") || msg.contains("record stream"),
        "expected boot-sector / NTFS-fallback error, got: {msg}"
    );
}

#[test]
fn materialize_unknown_compression_byte_errors_cleanly() {
    let buf = build_header(0x07, 0x00, 0x00, None);
    let path = write_to_temp(&buf);
    let err = materialize_gho_to_temp(&path).unwrap_err();
    let msg = format!("{err:#}").to_lowercase();
    assert!(msg.contains("compression"));
}

// ---------- checked-in fixture parse guard ----------

/// Small real Norton Ghost containers checked into tests/fixtures/
/// (zstd-compressed). They run on every clone and in CI, so they are
/// the minimum header-parse regression guard. Between them they cover
/// both compression variants
/// that exist at a checkable-in size (none + high) and both Ghost header
/// generations (7.5 + 11.x). Tuple: (file, compression, image_type, pwd).
const REPO_GHO_FIXTURES: &[(&str, u8, u8, u8)] = &[
    // 75MULTIU.GHO: Ghost 7.5, compression=none, fulldisk, no password.
    ("test_gho_75_none.gho.zst", 0x00, 0x00, 0x00),
    // 11comp.GHO: Ghost 11.x, compression=high, multi-partition file-aware,
    // no password. Plaintext twin of test_gho_11_high_pw.gho.zst below.
    ("test_gho_11_high.gho.zst", 0x03, 0x00, 0x00),
    // 4.GHO: Ghost 11.5, high, multi-partition file-aware, PASSWORD-
    // protected (password "password"). Encrypted twin of the fixture
    // above — same record skeleton, bodies enciphered.
    ("test_gho_11_high_pw.gho.zst", 0x03, 0x00, 0x01),
];

fn load_zst_fixture(name: &str) -> Vec<u8> {
    let path = format!("{}/../../tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    let compressed =
        std::fs::read(&path).unwrap_or_else(|e| panic!("reading repo fixture {path}: {e}"));
    let mut decoder = zstd::stream::read::Decoder::new(Cursor::new(compressed))
        .unwrap_or_else(|e| panic!("zstd decoder for {path}: {e}"));
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .unwrap_or_else(|e| panic!("decompressing repo fixture {path}: {e}"));
    out
}

/// Header-parse regression guard against the checked-in fixtures.
#[test]
fn parses_checked_in_gho_fixtures() {
    for (name, comp, image_type, pwd) in REPO_GHO_FIXTURES {
        let bytes = load_zst_fixture(name);
        let header = GhoContainerHeader::parse(&mut Cursor::new(bytes))
            .unwrap_or_else(|e| panic!("parsing {name}: {e:#}"));
        assert_eq!(header.container_version, 0x01, "{name}: container_version");
        assert_eq!(header.flags, [0x01, 0x01], "{name}: flags");
        assert_eq!(
            header.compression.as_byte(),
            *comp,
            "{name}: compression (got {:#04x}, want {:#04x})",
            header.compression.as_byte(),
            comp
        );
        assert_eq!(
            header.image_type.as_byte(),
            *image_type,
            "{name}: image_type (got {:#04x}, want {:#04x})",
            header.image_type.as_byte(),
            image_type
        );
        assert_eq!(
            header.password_protected,
            *pwd == 0x01,
            "{name}: password_protected"
        );
    }
}

// ---------- record-stream parser (5.5b) ----------

fn build_record_header_bytes(type_code: u16, marker: u16, body_len: u16) -> [u8; 10] {
    let mut buf = [0u8; 10];
    buf[0..2].copy_from_slice(&type_code.to_le_bytes());
    buf[2..4].copy_from_slice(&marker.to_le_bytes());
    buf[4..8].copy_from_slice(&GHO_RECORD_MAGIC.to_le_bytes());
    buf[8..10].copy_from_slice(&body_len.to_le_bytes());
    buf
}

#[test]
fn parses_75_style_record_header() {
    // Ghost 7.5: marker = 0x0000
    let bytes = build_record_header_bytes(0x0017, 0x0000, 0x0200);
    let header = GhoRecordHeader::parse_bytes(&bytes).unwrap();
    assert_eq!(header.type_code, 0x0017);
    assert_eq!(header.marker, 0x0000);
    assert_eq!(header.body_len, 0x0200);
}

#[test]
fn parses_115_style_record_header() {
    // Ghost 11.5: marker = 0x95FD
    let bytes = build_record_header_bytes(0x0017, 0x95FD, 0x0200);
    let header = GhoRecordHeader::parse_bytes(&bytes).unwrap();
    assert_eq!(header.type_code, 0x0017);
    assert_eq!(header.marker, 0x95FD);
    assert_eq!(header.body_len, 0x0200);
}

#[test]
fn rejects_record_header_with_wrong_magic() {
    let mut bytes = build_record_header_bytes(0x0017, 0x0000, 0x0200);
    bytes[4] = 0xFF; // clobber magic
    let err = GhoRecordHeader::parse_bytes(&bytes).unwrap_err();
    assert!(format!("{err:#}").contains("record magic"));
}

#[test]
fn record_iter_walks_two_records_and_stops_at_eof() {
    // Two records back to back: type 0x0017 with 4-byte body 'b1b1',
    // type 0x0004 with 3-byte body 'b2b'.
    let mut buf = Vec::new();
    buf.extend_from_slice(&build_record_header_bytes(0x0017, 0x95FD, 4));
    buf.extend_from_slice(b"b1b1");
    buf.extend_from_slice(&build_record_header_bytes(0x0004, 0x95FD, 3));
    buf.extend_from_slice(b"b2b");

    let mut iter = GhoRecordIter::new(Cursor::new(buf), 0).unwrap();
    let r0 = iter.next().unwrap().unwrap();
    assert_eq!(r0.type_code, 0x0017);
    assert_eq!(r0.body_len, 4);
    let r1 = iter.next().unwrap().unwrap();
    assert_eq!(r1.type_code, 0x0004);
    assert_eq!(r1.body_len, 3);
    assert!(iter.next().is_none(), "iterator should stop cleanly at EOF");
    assert_eq!(iter.records_read(), 2);
}

#[test]
fn record_iter_surfaces_error_on_corrupt_magic_mid_stream() {
    // First record OK; second record has bad magic.
    let mut buf = Vec::new();
    buf.extend_from_slice(&build_record_header_bytes(0x0017, 0x0000, 0));
    buf.extend_from_slice(&build_record_header_bytes(0x0004, 0x0000, 0));
    // Clobber the magic in the second header.
    let second = 10;
    buf[second + 4] = 0;

    let mut iter = GhoRecordIter::new(Cursor::new(buf), 0).unwrap();
    let _ = iter.next().unwrap().unwrap();
    let err = iter.next().unwrap().unwrap_err();
    assert!(format!("{err:#}").contains("record magic"));
}

#[test]
fn find_inner_stream_start_locates_magic_in_synthetic_stream() {
    // Container header (12 bytes) + 100 bytes of pad + first record.
    let mut buf = vec![0u8; 12 + 100];
    buf[0] = 0xFE;
    buf[1] = 0xEF;
    buf.extend_from_slice(&build_record_header_bytes(0x0017, 0x0000, 0));
    let start = find_inner_stream_start(&mut Cursor::new(buf), 12).unwrap();
    assert_eq!(
        start,
        12 + 100,
        "header offset should land 4 bytes before magic"
    );
}

#[test]
fn find_inner_stream_start_handles_magic_straddling_chunks() {
    // Place magic so the first 3 bytes land at the end of a 4096-byte
    // scan chunk and the 4th byte lands in the next chunk. This
    // exercises the tail-carry path.
    let header_end = 12;
    let pad_to_boundary = 4096 - 3 + 4; // ensure magic straddles boundary
    let mut buf = vec![0u8; header_end + pad_to_boundary];
    buf.extend_from_slice(&build_record_header_bytes(0x0017, 0x95FD, 0));
    let start = find_inner_stream_start(&mut Cursor::new(buf), header_end as u64).unwrap();
    assert_eq!(start, (header_end + pad_to_boundary) as u64);
}

#[test]
fn find_inner_stream_start_errors_when_magic_absent() {
    let buf = vec![0u8; 32 * 1024]; // all zeros, no magic
    let err = find_inner_stream_start(&mut Cursor::new(buf), 12).unwrap_err();
    assert!(format!("{err:#}").contains("not found"));
}

// ---------- Fast-LZ block decoder (5.5c) ----------

#[test]
fn fast_lz_hash_matches_reference_values() {
    // h(0,0,0) trivially = 0.
    assert_eq!(fast_lz_hash(0, 0, 0), 0);
    // h(1,0,0): v = 0 ^ (16 * (0 ^ (16 * 1))) = 256.
    //   -24993 * 256 = -6398208 → as u32 0xFF9E5F00, >> 4 → 0x0FF9E5F0,
    //   & 0xFFF = 0x5F0.
    assert_eq!(fast_lz_hash(1, 0, 0), 0x5F0);
    // Different orderings of the same 3 bytes hash differently — the
    // formula is order-sensitive.
    assert_ne!(
        fast_lz_hash(0xAB, 0xCD, 0xEF),
        fast_lz_hash(0xEF, 0xCD, 0xAB)
    );
}

#[test]
fn fast_lz_decompresses_uncompressed_block_via_prefix_byte_one() {
    // data[0] == 1 → stored verbatim past the 4-byte prefix.
    let payload = b"hello, ghost";
    let mut input = vec![1u8, 0, 0, 0];
    input.extend_from_slice(payload);
    let mut dst = vec![0u8; FAST_LZ_BLOCK_SIZE + 1024];
    let n = fast_lz_decompress(&input, input.len(), &mut dst).unwrap();
    assert_eq!(n, payload.len());
    assert_eq!(&dst[..n], payload);
}

#[test]
fn fast_lz_uncompressed_rejects_too_short_input() {
    // comp_len < 4 → no room for the 4-byte prefix.
    let input = [1u8, 0, 0];
    let mut dst = vec![0u8; FAST_LZ_BLOCK_SIZE + 1024];
    let err = fast_lz_decompress(&input, 3, &mut dst).unwrap_err();
    assert!(format!("{err:#}").contains("too short"));
}

#[test]
fn fast_lz_rejects_truncated_input_when_comp_len_exceeds_data() {
    let input = [2u8, 0, 0, 0];
    let mut dst = vec![0u8; FAST_LZ_BLOCK_SIZE + 1024];
    let err = fast_lz_decompress(&input, 8, &mut dst).unwrap_err();
    assert!(format!("{err:#}").contains("truncated"));
}

#[test]
fn fast_lz_zero_complen_rejected() {
    let mut dst = vec![0u8; FAST_LZ_BLOCK_SIZE + 1024];
    let err = fast_lz_decompress(&[1u8, 0, 0, 0], 0, &mut dst).unwrap_err();
    assert!(format!("{err:#}").contains("truncated"));
}

#[test]
fn fast_lz_decompresses_literal_only_payload() {
    // Construct: 4-byte non-1 prefix, then a 16-bit control word of
    // 0x0000 (all bits 0 → all 16 tokens literal), then 16 literal
    // bytes, then 32 bytes of zero padding so the near_end safeguard
    // does not kick in mid-batch. The decoder should emit exactly
    // the 16 literal bytes for the first control word; subsequent
    // padding-driven control words produce extra zero literals which
    // are also valid output but not asserted on.
    let mut input = vec![2u8, 0, 0, 0]; // prefix, type byte != 1
    input.extend_from_slice(&[0x00, 0x00]); // control word: all literals
    input.extend_from_slice(b"ABCDEFGHIJKLMNOP"); // 16 literals
    input.extend(disk_core::repeat_n(0u8, 64)); // pad past near_end window

    let mut dst = vec![0u8; FAST_LZ_BLOCK_SIZE + 1024];
    let n = fast_lz_decompress(&input, input.len(), &mut dst).unwrap();
    assert!(n >= 16, "decoder should emit at least the 16 literals");
    assert_eq!(&dst[..16], b"ABCDEFGHIJKLMNOP");
}

// ---------- 5.6 — walker + decode-to-temp ----------

/// Build a synthetic file-aware container in our fixture-corpus
/// dialect: container header + one boot-sector record (0x0017) +
/// N data-block records (0x0002) each carrying `block_payload`.
fn build_single_partition_corpus_dialect(
    compression_byte: u8,
    boot_sector: &[u8],
    data_blocks: &[Vec<u8>],
) -> Vec<u8> {
    let mut out = build_header(compression_byte, 0x00, 0x00, None);
    out.resize(GHO_SECTOR_SIZE as usize, 0);
    // Boot-sector record.
    assert_eq!(boot_sector.len(), 512);
    out.extend_from_slice(&build_record_header_bytes(GHO_REC_BOOT_SECTOR, 0, 512));
    out.extend_from_slice(boot_sector);
    // Data-block records.
    for body in data_blocks {
        assert!(body.len() <= u16::MAX as usize);
        out.extend_from_slice(&build_record_header_bytes(
            GHO_REC_DATA_BLOCK,
            0,
            body.len() as u16,
        ));
        out.extend_from_slice(body);
    }
    out
}

#[test]
fn parse_gho_image_counts_partitions_and_data_blocks() {
    // The walker is kept as infrastructure for the deferred file-aware
    // reconstruction; today only the synthetic shape exercises it.
    let boot = vec![0u8; 512];
    let blocks = vec![vec![0xAA; 32768], vec![0xBB; 32768]];
    let buf = build_single_partition_corpus_dialect(0x00, &boot, &blocks);
    let mut cur = Cursor::new(&buf);
    let header = GhoContainerHeader::parse(&mut cur).unwrap();
    let img = parse_gho_image(&mut cur, buf.len() as u64, &header).unwrap();
    assert_eq!(img.partition_count, 1);
    assert_eq!(img.data_blocks().count(), 2);
    assert_eq!(img.records.len(), 3);
    assert_eq!(img.records[0].type_code, GHO_REC_BOOT_SECTOR);
    assert_eq!(img.records[1].type_code, GHO_REC_DATA_BLOCK);
    assert_eq!(img.records[1].body_len, 32768);
}

/// Build a synthetic SECTOR-mode UNCOMPRESSED GHO: container header,
/// some zero padding, a FEEF sub-header (the data-start marker), then
/// raw sectors. Mirrors the real corpus layout.
fn build_sector_mode_uncompressed(sectors: &[[u8; 512]]) -> Vec<u8> {
    let mut out = build_header(0x00, 0x01, 0x00, None);
    // Pad to sector 4, then a FEEF sub-header at sector 4, data at 5.
    out.resize(4 * GHO_SECTOR_SIZE as usize, 0);
    let mut feef = [0u8; 512];
    feef[0] = 0xFE;
    feef[1] = 0xEF;
    out.extend_from_slice(&feef);
    for s in sectors {
        out.extend_from_slice(s);
    }
    out
}

#[test]
fn materialize_sector_mode_uncompressed_round_trips_sectors() {
    let mut s0 = [0u8; 512];
    s0[0..3].copy_from_slice(b"\xeb\x58\x90"); // FAT BPB jump
    s0[510] = 0x55;
    s0[511] = 0xAA;
    let mut s1 = [0u8; 512];
    s1[..16].copy_from_slice(b"second sector...");
    let buf = build_sector_mode_uncompressed(&[s0, s1]);
    let path = write_to_temp(&buf);
    let mat = materialize_gho_to_temp(&path).expect("SECTOR mode should decode");
    assert_eq!(mat.partition_count, 1);
    let written = std::fs::read(&mat.temp_path).unwrap();
    assert_eq!(written.len(), 1024);
    assert_eq!(&written[510..512], &[0x55, 0xAA]);
    assert_eq!(&written[512..528], b"second sector...");
}

#[test]
fn find_sector_data_start_lands_after_last_feef_subheader() {
    // FEEF at sector 3 → data should start at sector 4.
    let mut buf = vec![0u8; 6 * 512];
    buf[3 * 512] = 0xFE;
    buf[3 * 512 + 1] = 0xEF;
    let start = find_sector_data_start(&mut Cursor::new(&buf), buf.len() as u64).unwrap();
    assert_eq!(start, 4 * GHO_SECTOR_SIZE);
}

#[test]
fn find_sector_data_start_falls_back_to_first_nonzero_when_no_feef() {
    let mut buf = vec![0u8; 5 * 512];
    buf[2 * 512] = 0xCD; // non-zero, but not FEEF
    let start = find_sector_data_start(&mut Cursor::new(&buf), buf.len() as u64).unwrap();
    assert_eq!(start, 2 * GHO_SECTOR_SIZE);
}

// ---------- 5.6-span — span-set discovery + multi-file decode ----------

#[test]
fn split_gho_dot_numeric_parses_win7_style_names() {
    assert_eq!(
        split_gho_dot_numeric("Win7_86xAMB.GHO.001"),
        Some(("Win7_86xAMB".to_string(), 1))
    );
    assert_eq!(
        split_gho_dot_numeric("Win7_86xAMB.GHO.066"),
        Some(("Win7_86xAMB".to_string(), 66))
    );
    // Case-insensitive on `.GHO`.
    assert_eq!(
        split_gho_dot_numeric("name.gho.042"),
        Some(("name".to_string(), 42))
    );
    // Doesn't trigger on plain `.gho`.
    assert_eq!(split_gho_dot_numeric("disk.gho"), None);
    // Doesn't trigger on `.ghs` extension.
    assert_eq!(split_gho_dot_numeric("disk.ghs"), None);
}

#[test]
fn strip_trailing_numeric_handles_corpus_stems() {
    assert_eq!(strip_trailing_numeric("SECTO001"), "SECTO");
    assert_eq!(strip_trailing_numeric("gh11-001"), "gh11-");
    assert_eq!(strip_trailing_numeric("gh11pwd"), "gh11pwd");
    assert_eq!(strip_trailing_numeric("hipwd005"), "hipwd");
    assert_eq!(strip_trailing_numeric("SECTOR"), "SECTOR");
}

/// Synthetic directory with `disk.GHO` + `disk001.GHS` + `disk002.GHS`.
/// Picking ANY one of the three should return the same ordered set.
#[test]
fn discover_span_set_finds_ghs_siblings() {
    let dir = tempfile::tempdir().unwrap();
    let primary = dir.path().join("disk.GHO");
    let s1 = dir.path().join("disk001.GHS");
    let s2 = dir.path().join("disk002.GHS");
    std::fs::write(&primary, b"primary").unwrap();
    std::fs::write(&s1, b"span1").unwrap();
    std::fs::write(&s2, b"span2").unwrap();
    for pick in [&primary, &s1, &s2] {
        let set = discover_gho_span_set(pick).unwrap();
        assert_eq!(
            set.len(),
            3,
            "picking {} should yield 3 files",
            pick.display()
        );
        assert_eq!(set[0], primary, "primary must be first");
        assert_eq!(set[1], s1);
        assert_eq!(set[2], s2);
    }
}

/// Numeric-suffix layout: `name.GHO.001`, `.002`, `.003`. Picking any
/// one yields the set sorted by numeric tail.
#[test]
fn discover_span_set_finds_dot_numeric_siblings() {
    let dir = tempfile::tempdir().unwrap();
    let p1 = dir.path().join("name.GHO.001");
    let p2 = dir.path().join("name.GHO.002");
    let p3 = dir.path().join("name.GHO.003");
    std::fs::write(&p1, b"a").unwrap();
    std::fs::write(&p2, b"b").unwrap();
    std::fs::write(&p3, b"c").unwrap();
    for pick in [&p1, &p2, &p3] {
        let set = discover_gho_span_set(pick).unwrap();
        assert_eq!(set, vec![p1.clone(), p2.clone(), p3.clone()]);
    }
}

/// Single-file case: no siblings → set is just the picked file.
#[test]
fn discover_span_set_singleton_when_no_siblings() {
    let dir = tempfile::tempdir().unwrap();
    let f = dir.path().join("lonely.GHO");
    std::fs::write(&f, b"alone").unwrap();
    let set = discover_gho_span_set(&f).unwrap();
    assert_eq!(set, vec![f]);
}

#[test]
fn span_reader_concatenates_and_skips_container_headers() {
    // Build 3 synthetic span files:
    //   primary: 512-byte header (FE EF ...) + 100 bytes "A..."
    //   span 1:  512-byte header (FE EF ...) + 50 bytes "B..."
    //   span 2:  512-byte header (FE EF ...) + 25 bytes "C..."
    // SpanReader should expose: full primary (612 bytes) +
    // span1 minus its 512-byte header (50 bytes) +
    // span2 minus its 512-byte header (25 bytes) = 687 bytes total.
    let dir = tempfile::tempdir().unwrap();
    let make = |name: &str, fill: u8, body_len: usize| -> std::path::PathBuf {
        let path = dir.path().join(name);
        let mut data = vec![0u8; 512];
        data[0] = 0xFE;
        data[1] = 0xEF;
        data.extend_from_slice(&vec![fill; body_len]);
        std::fs::write(&path, &data).unwrap();
        path
    };
    let p0 = make("set.GHO", b'A', 100);
    let p1 = make("set001.GHS", b'B', 50);
    let p2 = make("set002.GHS", b'C', 25);

    let mut r = SpanReader::open(&[p0, p1, p2]).unwrap();
    assert_eq!(r.total_len(), 612 + 50 + 25);

    let mut all = Vec::new();
    r.read_to_end(&mut all).unwrap();
    assert_eq!(all.len() as u64, r.total_len());

    // Primary's header byte at offset 0 should be FE.
    assert_eq!(all[0], 0xFE);
    // Last byte of primary (offset 611) is 'A'.
    assert_eq!(all[611], b'A');
    // Bytes 612..662 are 'B' (50 from span 1, header skipped).
    assert!(all[612..662].iter().all(|&b| b == b'B'), "span 1 body");
    // Bytes 662..687 are 'C' (25 from span 2, header skipped).
    assert!(all[662..687].iter().all(|&b| b == b'C'), "span 2 body");

    // Seek round-trip.
    r.seek(SeekFrom::Start(612)).unwrap();
    let mut probe = [0u8; 1];
    r.read_exact(&mut probe).unwrap();
    assert_eq!(probe[0], b'B', "seek-then-read should land in span 1");
}

// ---------- file-aware record body parsers ----------

#[test]
fn record_type_predicates_classify_known_codes() {
    // Boot sector
    assert!(is_boot_sector_record(0x0017));
    assert!(is_boot_sector_record(0x0717));
    assert!(is_boot_sector_record(0xae17)); // 7.5 full-disk header copy
    assert!(is_boot_sector_record(0x0117)); // 7.5 full-disk partition copy
    assert!(!is_boot_sector_record(0x0004));

    // Dir entries
    assert!(is_dir_entry_record(0x0004));
    assert!(is_dir_entry_record(0x0104));
    assert!(is_dir_entry_record(0x0704));
    assert!(is_dir_entry_record(0xae04)); // 7.5 full-disk header section
    assert!(!is_dir_entry_record(0x0017));
    assert!(!is_dir_entry_record(0x0102));

    // Data blocks
    assert!(is_data_block_record(0x0002));
    assert!(is_data_block_record(0x0102));
    assert!(!is_data_block_record(0x0103));
    assert!(!is_data_block_record(0x0104));

    // Checksum
    assert!(is_checksum_record(0x0103));
    assert!(!is_checksum_record(0x0102));
}

#[test]
fn parse_fat_dir_entry_body_decodes_lfn_slot() {
    // Sequence-1 LFN holding 'A','c','c' UTF-16LE. attr=0x0F at offset 11.
    let mut body = [0u8; 56];
    body[0] = 0x41; // sequence + LFN flag
    body[1] = b'A';
    body[2] = 0;
    body[3] = b'c';
    body[4] = 0;
    body[5] = b'c';
    body[6] = 0;
    body[11] = 0x0F; // LFN attr
    body[32..36].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());

    let parsed = parse_fat_dir_entry_body(&body).unwrap();
    assert!(parsed.is_lfn_slot());
    assert!(!parsed.is_directory());
    assert_eq!(parsed.first_cluster(), 0);
    assert_eq!(parsed.file_size(), 0);
    assert_eq!(parsed.entry_hash, 0xDEAD_BEEF);
}

#[test]
fn parse_fat_dir_entry_body_decodes_8_3_directory() {
    // "MYDOCU~1" with attr=0x10 (dir), first_cluster=3 (FAT32 split:
    // lo at 26-27, hi at 20-21), file_size=0.
    let mut body = [0u8; 56];
    body[..11].copy_from_slice(b"MYDOCU~1   ");
    body[11] = 0x10; // directory
    body[26..28].copy_from_slice(&3u16.to_le_bytes()); // cluster lo
    body[20..22].copy_from_slice(&0u16.to_le_bytes()); // cluster hi
    body[32..36].copy_from_slice(&0xCAFEu32.to_le_bytes());

    let parsed = parse_fat_dir_entry_body(&body).unwrap();
    assert!(!parsed.is_lfn_slot());
    assert!(parsed.is_directory());
    assert!(!parsed.is_volume_label());
    assert_eq!(parsed.first_cluster(), 3);
    assert_eq!(parsed.file_size(), 0); // directory size always 0
    assert_eq!(parsed.entry_hash, 0xCAFE);
}

#[test]
fn parse_fat_dir_entry_body_decodes_regular_file() {
    // "MSPAINT EXE" with size 0x54000 (344064), cluster 0x040000 (256K).
    let mut body = [0u8; 56];
    body[..11].copy_from_slice(b"MSPAINT EXE");
    body[11] = 0x20; // archive
    body[26..28].copy_from_slice(&0x0000u16.to_le_bytes()); // cluster lo
    body[20..22].copy_from_slice(&0x0004u16.to_le_bytes()); // cluster hi
    body[28..32].copy_from_slice(&0x00054000u32.to_le_bytes()); // size

    let parsed = parse_fat_dir_entry_body(&body).unwrap();
    assert!(!parsed.is_lfn_slot());
    assert!(!parsed.is_directory());
    assert_eq!(parsed.first_cluster(), 0x0004_0000);
    assert_eq!(parsed.file_size(), 344_064);
}

#[test]
fn parse_fat_dir_entry_body_recognises_empty_and_deleted_slots() {
    let body_empty = [0u8; 56];
    let parsed = parse_fat_dir_entry_body(&body_empty).unwrap();
    assert!(parsed.is_empty_slot());
    assert!(!parsed.is_deleted());

    let mut body_del = [0u8; 56];
    body_del[0] = 0xE5;
    let parsed = parse_fat_dir_entry_body(&body_del).unwrap();
    assert!(parsed.is_deleted());
    assert!(!parsed.is_empty_slot());
}

#[test]
fn parse_fat_dir_entry_body_rejects_too_short() {
    let err = parse_fat_dir_entry_body(&[0u8; 35]).unwrap_err();
    assert!(format!("{err}").contains(">= 36"));
}

#[test]
fn parse_checksum_record_body_extracts_duplicated_value() {
    let mut body = [0u8; 20];
    body[..4].copy_from_slice(&0x12345678u32.to_le_bytes());
    body[4..8].copy_from_slice(&0x12345678u32.to_le_bytes());
    let v = parse_checksum_record_body(&body).unwrap();
    assert_eq!(v, 0x12345678);
}

#[test]
fn parse_checksum_record_body_errors_on_mismatch() {
    let mut body = [0u8; 20];
    body[..4].copy_from_slice(&0xAAAAu32.to_le_bytes());
    body[4..8].copy_from_slice(&0xBBBBu32.to_le_bytes());
    let err = parse_checksum_record_body(&body).unwrap_err();
    assert!(format!("{err}").contains("mismatched"));
}

// ---------- file-aware directory walker ----------

/// Unit-level: build a synthetic file-aware GHO with boot sector +
/// one dir + one file, walk it, and confirm the tree shape.
#[test]
fn walk_file_aware_tree_assembles_synthetic_stream() {
    use std::io::Cursor;

    // Build a minimal byte stream: container header + boot sector
    // + dir entries + file content + checksum + EOF.
    let mut buf = Vec::new();
    // container prefix at sector 0
    let mut prefix = vec![0u8; 512];
    prefix[0] = 0xFE;
    prefix[1] = 0xEF;
    prefix[2] = 0x01; // version
    prefix[3] = 0x00; // compression=None
    prefix[10] = 0x00; // image_type=FileAware
    buf.extend_from_slice(&prefix);

    // Helper to write a record header.
    let write_record = |buf: &mut Vec<u8>, type_code: u16, body: &[u8]| {
        buf.extend_from_slice(&type_code.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes()); // marker
        buf.extend_from_slice(&GHO_RECORD_MAGIC.to_le_bytes());
        buf.extend_from_slice(&(body.len() as u16).to_le_bytes());
        buf.extend_from_slice(body);
    };
    // Build a 512-byte FAT32 boot sector: just enough for the
    // walker to extract root_cluster.
    let mut boot = [0u8; 512];
    boot[44..48].copy_from_slice(&2u32.to_le_bytes());
    boot[82..87].copy_from_slice(b"FAT32");
    write_record(&mut buf, GHO_REC_BOOT_SECTOR, &boot);

    // 8.3 entry for "HELLO.TXT" in root, cluster=5, size=11.
    let mut e = [0u8; 56];
    e[..11].copy_from_slice(b"HELLO   TXT");
    e[11] = 0x20; // archive
    e[26..28].copy_from_slice(&5u16.to_le_bytes()); // cluster lo
    e[28..32].copy_from_slice(&11u32.to_le_bytes()); // size
    write_record(&mut buf, GHO_REC_DIR_ENTRY_ROOT, &e);

    // File content for HELLO.TXT (11 bytes).
    write_record(&mut buf, GHO_REC_FILE_TAIL, b"hello world");

    // Checksum.
    let mut c = [0u8; 20];
    c[..4].copy_from_slice(&0x12345678u32.to_le_bytes());
    c[4..8].copy_from_slice(&0x12345678u32.to_le_bytes());
    write_record(&mut buf, GHO_REC_FILE_CHECKSUM, &c);

    let mut cur = Cursor::new(buf.clone());
    let header = GhoContainerHeader::parse(&mut cur).unwrap();
    let image = parse_gho_image(&mut cur, buf.len() as u64, &header).unwrap();
    let tree = walk_file_aware_tree(&mut cur, &image).unwrap();
    assert_eq!(tree.root_cluster, 2);
    assert_eq!(tree.entries.len(), 1);
    let hello = &tree.entries[0];
    assert_eq!(hello.short_name, "HELLO.TXT");
    assert!(!hello.is_directory());
    assert_eq!(hello.parent_cluster, 2);
    assert_eq!(hello.file_size, 11);
    assert_eq!(hello.content_record_offsets.len(), 1);
    assert_eq!(hello.checksum, Some(0x12345678));
}

// ---------- Slice C: file-aware FAT image emitter ----------

/// Build a synthetic file-aware GHO (boot sector + one root file +
/// one subdir with one file), run the emitter, then re-open the
/// resulting FAT image and verify the tree.
#[test]
fn emit_file_aware_fat_image_roundtrips_synthetic_stream() {
    use disk_core::filesystem::Filesystem;
    use fs_pc::fat::FatFilesystem;
    use std::io::Cursor;

    // ---------- build synthetic GHO bytes ----------
    let mut buf = Vec::new();
    let mut prefix = vec![0u8; 512];
    prefix[0] = 0xFE;
    prefix[1] = 0xEF;
    prefix[2] = 0x01;
    prefix[3] = 0x00; // compression=None
    prefix[10] = 0x00; // image_type=FileAware
    buf.extend_from_slice(&prefix);

    let write_record = |buf: &mut Vec<u8>, type_code: u16, body: &[u8]| {
        buf.extend_from_slice(&type_code.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(&GHO_RECORD_MAGIC.to_le_bytes());
        buf.extend_from_slice(&(body.len() as u16).to_le_bytes());
        buf.extend_from_slice(body);
    };

    // FAT16 boot sector sized for a ~4 MiB partition (so the
    // emitter's create_blank_fat picks the same regime).
    // 4 MiB = 8192 sectors of 512.
    let mut boot = [0u8; 512];
    boot[11..13].copy_from_slice(&512u16.to_le_bytes()); // BytsPerSec
    boot[13] = 1; // SecPerClus
    boot[14..16].copy_from_slice(&1u16.to_le_bytes()); // RsvdSecCnt
    boot[16] = 2; // NumFATs
    boot[19..21].copy_from_slice(&8192u16.to_le_bytes()); // TotSec16
                                                          // Mark as FAT16 explicitly via fs-type-string @ 54.
    boot[54..62].copy_from_slice(b"FAT16   ");
    write_record(&mut buf, GHO_REC_BOOT_SECTOR, &boot);

    // Root file: HELLO.TXT, 11 bytes, cluster 5.
    let mut e = [0u8; 56];
    e[..11].copy_from_slice(b"HELLO   TXT");
    e[11] = 0x20;
    e[26..28].copy_from_slice(&5u16.to_le_bytes());
    e[28..32].copy_from_slice(&11u32.to_le_bytes());
    write_record(&mut buf, GHO_REC_DIR_ENTRY_ROOT, &e);
    write_record(&mut buf, GHO_REC_FILE_TAIL, b"hello world");
    let mut c = [0u8; 20];
    c[..4].copy_from_slice(&0xdeadbeefu32.to_le_bytes());
    c[4..8].copy_from_slice(&0xdeadbeefu32.to_le_bytes());
    write_record(&mut buf, GHO_REC_FILE_CHECKSUM, &c);

    // Subdir SUB, cluster 7.
    let mut d = [0u8; 56];
    d[..11].copy_from_slice(b"SUB        ");
    d[11] = 0x10; // dir
    d[26..28].copy_from_slice(&7u16.to_le_bytes());
    write_record(&mut buf, GHO_REC_DIR_ENTRY_SUB, &d);

    // Descend into SUB: "." cluster=7, ".." cluster=0 (root).
    let mut dot = [0u8; 56];
    dot[..11].copy_from_slice(b".          ");
    dot[11] = 0x10;
    dot[26..28].copy_from_slice(&7u16.to_le_bytes());
    write_record(&mut buf, GHO_REC_DIR_ENTRY_SUB, &dot);
    let mut dotdot = [0u8; 56];
    dotdot[..11].copy_from_slice(b"..         ");
    dotdot[11] = 0x10;
    dotdot[26..28].copy_from_slice(&0u16.to_le_bytes());
    write_record(&mut buf, GHO_REC_DIR_ENTRY_SUB, &dotdot);

    // Inside SUB: NESTED.TXT, 6 bytes, cluster 9.
    let mut e2 = [0u8; 56];
    e2[..11].copy_from_slice(b"NESTED  TXT");
    e2[11] = 0x20;
    e2[26..28].copy_from_slice(&9u16.to_le_bytes());
    e2[28..32].copy_from_slice(&6u32.to_le_bytes());
    write_record(&mut buf, GHO_REC_DIR_ENTRY_SUB, &e2);
    write_record(&mut buf, GHO_REC_FILE_TAIL, b"hello!");
    let mut c2 = [0u8; 20];
    c2[..4].copy_from_slice(&0xcafef00du32.to_le_bytes());
    c2[4..8].copy_from_slice(&0xcafef00du32.to_le_bytes());
    write_record(&mut buf, GHO_REC_FILE_CHECKSUM, &c2);

    // ---------- parse + walk + emit ----------
    let mut cur = Cursor::new(buf.clone());
    let header = GhoContainerHeader::parse(&mut cur).unwrap();
    let image = parse_gho_image(&mut cur, buf.len() as u64, &header).unwrap();
    let tree = walk_file_aware_tree(&mut cur, &image).unwrap();
    assert_eq!(tree.entries.len(), 3); // HELLO, SUB, NESTED
    assert_eq!(tree.file_count(), 2);
    assert_eq!(tree.dir_count(), 1);

    let res = emit_file_aware_fat_image(&mut cur, &image, &tree, header.compression).unwrap();
    assert_eq!(res.files_emitted, 2);
    assert_eq!(res.dirs_emitted, 1);
    assert!(
        res.skipped.is_empty(),
        "no entries should have been skipped, got {:?}",
        res.skipped
    );
    assert!(!res.image.is_empty());

    // ---------- reopen the FAT image and verify contents ----------
    let mut img_cur = Cursor::new(res.image);
    let mut fs = FatFilesystem::open(&mut img_cur, 0).unwrap();
    let root = fs.root().unwrap();
    let root_entries = fs.list_directory(&root).unwrap();
    let names: Vec<&str> = root_entries.iter().map(|e| e.name.as_str()).collect();
    assert!(names.contains(&"HELLO.TXT"), "root listing: {:?}", names);
    assert!(names.contains(&"SUB"), "root listing: {:?}", names);

    let hello = root_entries.iter().find(|e| e.name == "HELLO.TXT").unwrap();
    let data = fs.read_file(hello, usize::MAX).unwrap();
    assert_eq!(&data, b"hello world");

    let sub = root_entries.iter().find(|e| e.name == "SUB").unwrap();
    let sub_entries = fs.list_directory(sub).unwrap();
    assert_eq!(sub_entries.len(), 1);
    assert_eq!(sub_entries[0].name, "NESTED.TXT");
    let nested = fs.read_file(&sub_entries[0], usize::MAX).unwrap();
    assert_eq!(&nested, b"hello!");
}

/// Open a synthetic file-aware GHO via the unified `GhoReader`,
/// pipe its bytes into a Vec, and re-open as a FAT image. The
/// reader must serve the same bytes as `emit_file_aware_fat_image`
/// (the in-RAM Vec path) up to volume-label byte differences.
#[test]
fn gho_reader_file_aware_streams_same_tree_as_emit() {
    use disk_core::filesystem::Filesystem;
    use fs_pc::fat::FatFilesystem;
    use std::io::Cursor;

    // Same synthetic stream the emit test builds.
    let mut buf = Vec::new();
    let mut prefix = vec![0u8; 512];
    prefix[0] = 0xFE;
    prefix[1] = 0xEF;
    prefix[2] = 0x01;
    prefix[3] = 0x00;
    prefix[10] = 0x00;
    buf.extend_from_slice(&prefix);
    let write_record = |buf: &mut Vec<u8>, type_code: u16, body: &[u8]| {
        buf.extend_from_slice(&type_code.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(&GHO_RECORD_MAGIC.to_le_bytes());
        buf.extend_from_slice(&(body.len() as u16).to_le_bytes());
        buf.extend_from_slice(body);
    };
    let mut boot = [0u8; 512];
    boot[11..13].copy_from_slice(&512u16.to_le_bytes());
    boot[13] = 1;
    boot[14..16].copy_from_slice(&1u16.to_le_bytes());
    boot[16] = 2;
    boot[19..21].copy_from_slice(&8192u16.to_le_bytes()); // 4 MiB
    boot[54..62].copy_from_slice(b"FAT16   ");
    write_record(&mut buf, GHO_REC_BOOT_SECTOR, &boot);
    let mut e = [0u8; 56];
    e[..11].copy_from_slice(b"HELLO   TXT");
    e[11] = 0x20;
    e[26..28].copy_from_slice(&5u16.to_le_bytes());
    e[28..32].copy_from_slice(&11u32.to_le_bytes());
    write_record(&mut buf, GHO_REC_DIR_ENTRY_ROOT, &e);
    write_record(&mut buf, GHO_REC_FILE_TAIL, b"hello world");
    let mut c = [0u8; 20];
    c[..4].copy_from_slice(&0xdeadbeefu32.to_le_bytes());
    c[4..8].copy_from_slice(&0xdeadbeefu32.to_le_bytes());
    write_record(&mut buf, GHO_REC_FILE_CHECKSUM, &c);

    // Write to a tempfile so GhoReader::open can pull it in.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("syn.gho");
    std::fs::write(&path, &buf).unwrap();

    let mut reader = GhoReader::open(&path).expect("GhoReader::open file-aware");
    let logical = reader.logical_size();
    assert!(
        logical >= 4 * 1024 * 1024,
        "logical size at least source partition"
    );
    let mut streamed = Vec::with_capacity(logical as usize);
    reader.read_to_end(&mut streamed).unwrap();
    assert_eq!(streamed.len() as u64, logical);
    // First sector should look like a FAT boot sector.
    assert_eq!(streamed[0], 0xEB);
    assert_eq!(&streamed[510..512], &[0x55, 0xAA]);

    // Reopen the streamed bytes as a real FAT image.
    let mut img_cur = Cursor::new(streamed);
    let mut fs = FatFilesystem::open(&mut img_cur, 0).unwrap();
    let root = fs.root().unwrap();
    let entries = fs.list_directory(&root).unwrap();
    let hello = entries
        .iter()
        .find(|e| e.name == "HELLO.TXT")
        .expect("HELLO.TXT must round-trip through the streaming reader");
    let data = fs.read_file(hello, usize::MAX).unwrap();
    assert_eq!(&data, b"hello world");
}

// ---------- GhoReader (streaming Read+Seek) ----------

#[test]
fn gho_reader_uncompressed_sector_matches_decode_to_temp() {
    let mut s0 = [0u8; 512];
    s0[0..3].copy_from_slice(b"\xeb\x58\x90");
    s0[510] = 0x55;
    s0[511] = 0xAA;
    let mut s1 = [0u8; 512];
    s1[..16].copy_from_slice(b"second sector...");
    let buf = build_sector_mode_uncompressed(&[s0, s1]);
    let path = write_to_temp(&buf);

    // Reader path
    let mut reader = GhoReader::open(&path).unwrap();
    assert_eq!(reader.logical_size(), 1024);
    let mut via_reader = Vec::new();
    reader.read_to_end(&mut via_reader).unwrap();

    // Temp-file path
    let mat = materialize_gho_to_temp(&path).unwrap();
    let via_temp = std::fs::read(&mat.temp_path).unwrap();

    assert_eq!(via_reader, via_temp);

    // Random-access seek
    let mut buf2 = [0u8; 16];
    reader.seek(SeekFrom::Start(512)).unwrap();
    reader.read_exact(&mut buf2).unwrap();
    assert_eq!(&buf2, b"second sector...");
}

#[test]
fn gho_reader_rejects_file_aware() {
    let buf = build_header(0x00, 0x00, 0x00, None);
    let path = write_to_temp(&buf);
    let err = GhoReader::open(&path).err().expect("must error");
    let msg = format!("{err:#}").to_lowercase();
    assert!(
        msg.contains("record stream") || msg.contains("ntfs") || msg.contains("sector"),
        "unexpected error: {msg}"
    );
}

#[test]
fn gho_reader_rejects_password_protected() {
    let buf = build_header(0x03, 0x01, 0x01, None);
    let path = write_to_temp(&buf);
    let err = GhoReader::open(&path).err().expect("must error");
    assert!(format!("{err:#}").to_lowercase().contains("password"));
}

#[test]
fn gho_reader_requires_password_when_protected() {
    let buf = build_header(0x00, 0x00, 0x01, None);
    let path = write_to_temp(&buf);
    let err = GhoReader::open_with_password(&path, None)
        .err()
        .expect("must error");
    let m = format!("{err:#}").to_lowercase();
    assert!(m.contains("password is required"), "got: {m}");
}

#[test]
fn gho_reader_rejects_wrong_password() {
    // build_header writes a 0xAB*16 verifier, which no real password
    // decrypts to "BinaryResearch\0", so any password is "wrong".
    let buf = build_header(0x00, 0x00, 0x01, None);
    let path = write_to_temp(&buf);
    let err = GhoReader::open_with_password(&path, Some(b"whatever"))
        .err()
        .expect("must error");
    let m = format!("{err:#}").to_lowercase();
    assert!(m.contains("incorrect password"), "got: {m}");
}

/// Definitive decryption check against real fixtures: every data-block
/// body of an encrypted GHO must decrypt (through the real `SpanReader`
/// path) bit-exact to the same body in its unencrypted twin. Ignored by
/// default — set `RB_GHO_PLAIN` + `RB_GHO_CIPHER` (and optionally
/// `RB_GHO_PASSWORD`, default "password") to run it.
#[test]
#[ignore = "needs external GHO twin fixtures via RB_GHO_PLAIN / RB_GHO_CIPHER"]
fn decrypts_real_fixture_data_blocks_bit_exact() {
    let (plain, cipher) = match (
        std::env::var("RB_GHO_PLAIN"),
        std::env::var("RB_GHO_CIPHER"),
    ) {
        (Ok(p), Ok(c)) => (std::path::PathBuf::from(p), std::path::PathBuf::from(c)),
        _ => {
            eprintln!("skip: set RB_GHO_PLAIN and RB_GHO_CIPHER");
            return;
        }
    };
    let pw = std::env::var("RB_GHO_PASSWORD").unwrap_or_else(|_| "password".into());

    // Cipher reader with decryption enabled (mirrors open_with_password).
    let mut csr = SpanReader::open(std::slice::from_ref(&cipher)).unwrap();
    let file_size = csr.total_len();
    let seed = gho_seed_from_password(pw.as_bytes());
    let header_end = (GHO_HEADER_PREFIX_LEN + GHO_PASSWORD_VERIFIER_LEN) as u64;
    let bodies = collect_gho_body_ranges(&mut csr, header_end, file_size).unwrap();
    csr.enable_decryption(seed, bodies);

    // Two plain readers: one to walk records, one to read bodies.
    let mut psr = SpanReader::open(std::slice::from_ref(&plain)).unwrap();
    let mut walk = SpanReader::open(std::slice::from_ref(&plain)).unwrap();
    let pstart = find_inner_stream_start(&mut walk, GHO_HEADER_PREFIX_LEN as u64).unwrap();
    let mut iter = GhoRecordIter::new(&mut walk, pstart).unwrap();

    let mut checked = 0u64;
    loop {
        let off = iter.current_offset();
        match iter.next() {
            Some(Ok(h)) => {
                if matches!(h.type_code, 0x0002 | 0x0102) && h.body_len > 0 {
                    let bs = off + GHO_RECORD_HEADER_LEN as u64;
                    let len = h.body_len as usize;
                    let mut pb = vec![0u8; len];
                    psr.seek(SeekFrom::Start(bs)).unwrap();
                    psr.read_exact(&mut pb).unwrap();
                    let mut cb = vec![0u8; len];
                    csr.seek(SeekFrom::Start(bs)).unwrap();
                    csr.read_exact(&mut cb).unwrap();
                    checked += 1;
                    assert_eq!(pb, cb, "data block at {bs} decrypted incorrectly");
                }
            }
            Some(Err(_)) | None => break,
        }
    }
    eprintln!("checked {checked} data blocks, all bit-exact");
    assert!(checked > 0, "no data blocks walked");
}

/// Checked-in version of the bit-exact decryption guard: the encrypted
/// multi-partition fixture `test_gho_11_high_pw.gho.zst` must decrypt (via
/// the real `SpanReader` body-decryption path, with the seed derived from
/// "password") to bodies bit-identical to its unencrypted twin
/// `test_gho_11_high.gho.zst`. Runs on every clone / in CI.
#[test]
fn checked_in_multipart_fixture_decrypts_bit_exact() {
    let plain = write_to_temp(&load_zst_fixture("test_gho_11_high.gho.zst"));
    let cipher = write_to_temp(&load_zst_fixture("test_gho_11_high_pw.gho.zst"));

    // Verify the seed against the cipher fixture's header verifier first.
    let header = GhoContainerHeader::parse(&mut File::open(&cipher).unwrap()).unwrap();
    assert!(
        header.password_protected,
        "cipher fixture must be encrypted"
    );
    let seed = gho_seed_from_password(b"password");
    assert!(
        gho_verify_seed(header.password_verifier.as_ref().unwrap(), seed),
        "password verifier mismatch"
    );

    // Cipher reader with decryption enabled (mirrors open_with_password).
    let mut csr = SpanReader::open(std::slice::from_ref(&cipher)).unwrap();
    let file_size = csr.total_len();
    let header_end = (GHO_HEADER_PREFIX_LEN + GHO_PASSWORD_VERIFIER_LEN) as u64;
    let bodies = collect_gho_body_ranges(&mut csr, header_end, file_size).unwrap();
    csr.enable_decryption(seed, bodies);

    // Walk the plaintext record stream; compare every data-block body.
    let mut psr = SpanReader::open(std::slice::from_ref(&plain)).unwrap();
    let mut walk = SpanReader::open(std::slice::from_ref(&plain)).unwrap();
    let pstart = find_inner_stream_start(&mut walk, GHO_HEADER_PREFIX_LEN as u64).unwrap();
    let mut iter = GhoRecordIter::new(&mut walk, pstart).unwrap();

    let mut checked = 0u64;
    loop {
        let off = iter.current_offset();
        match iter.next() {
            Some(Ok(h)) => {
                if matches!(h.type_code, 0x0002 | 0x0102) && h.body_len > 0 {
                    let bs = off + GHO_RECORD_HEADER_LEN as u64;
                    let len = h.body_len as usize;
                    let mut pb = vec![0u8; len];
                    psr.seek(SeekFrom::Start(bs)).unwrap();
                    psr.read_exact(&mut pb).unwrap();
                    let mut cb = vec![0u8; len];
                    csr.seek(SeekFrom::Start(bs)).unwrap();
                    csr.read_exact(&mut cb).unwrap();
                    checked += 1;
                    assert_eq!(pb, cb, "data block at {bs} decrypted incorrectly");
                }
            }
            Some(Err(_)) | None => break,
        }
    }
    assert!(
        checked >= 10,
        "expected multiple multi-partition data blocks, walked {checked}"
    );
}

/// Exercise the `SpanReader` body-decryption layer directly: a synthetic
/// stream of `[container header][record header][encrypted body]...` must
/// read back with bodies decrypted and headers untouched, under both
/// sequential and random access.
#[test]
fn span_reader_decrypts_bodies_and_passes_headers_through() {
    use crate::gho_crypto::gho_encrypt_body;
    use std::io::Read as _;

    let seed = gho_seed_from_password(b"secret");
    let plain1: Vec<u8> = b"The quick brown fox jumps over the lazy dog 0123456789".to_vec();
    let plain2: Vec<u8> = (0..500u32)
        .map(|i| (i.wrapping_mul(37) ^ 0xC3) as u8)
        .collect();

    let rec_hdr = |typ: u16, body_len: u16| -> [u8; 10] {
        let mut h = [0u8; 10];
        h[0..2].copy_from_slice(&typ.to_le_bytes());
        h[4..8].copy_from_slice(&GHO_RECORD_MAGIC.to_le_bytes());
        h[8..10].copy_from_slice(&body_len.to_le_bytes());
        h
    };

    // On-disk stream: 28-byte container header, then two records.
    let mut disk = vec![0u8; 28];
    disk[0] = 0xFE;
    disk[1] = 0xEF;

    disk.extend_from_slice(&rec_hdr(0x0002, plain1.len() as u16));
    let body1_off = disk.len() as u64;
    let mut enc1 = plain1.clone();
    gho_encrypt_body(&mut enc1, seed);
    disk.extend_from_slice(&enc1);

    disk.extend_from_slice(&rec_hdr(0x0002, plain2.len() as u16));
    let body2_off = disk.len() as u64;
    let mut enc2 = plain2.clone();
    gho_encrypt_body(&mut enc2, seed);
    disk.extend_from_slice(&enc2);

    // Expected decrypted view: headers identical, bodies plaintext.
    let mut expected = disk.clone();
    expected[body1_off as usize..body1_off as usize + plain1.len()].copy_from_slice(&plain1);
    expected[body2_off as usize..body2_off as usize + plain2.len()].copy_from_slice(&plain2);

    let path = write_to_temp(&disk);
    let mut sr = SpanReader::open(std::slice::from_ref(&path)).unwrap();
    sr.enable_decryption(
        seed,
        vec![
            (body1_off, plain1.len() as u32),
            (body2_off, plain2.len() as u32),
        ],
    );

    // Sequential read of the whole stream.
    let mut got = Vec::new();
    sr.read_to_end(&mut got).unwrap();
    assert_eq!(got, expected, "sequential decrypt mismatch");

    // Random access into the middle of body2 (cache fill).
    sr.seek(SeekFrom::Start(body2_off + 123)).unwrap();
    let mut b = [0u8; 32];
    sr.read_exact(&mut b).unwrap();
    assert_eq!(&b, &plain2[123..155]);

    // Random access back into body1 (cache eviction + re-decrypt).
    sr.seek(SeekFrom::Start(body1_off + 4)).unwrap();
    let mut b1 = [0u8; 10];
    sr.read_exact(&mut b1).unwrap();
    assert_eq!(&b1, &plain1[4..14]);

    // Header bytes pass through unchanged.
    sr.seek(SeekFrom::Start(0)).unwrap();
    let mut hdr = [0u8; 2];
    sr.read_exact(&mut hdr).unwrap();
    assert_eq!(&hdr, &[0xFE, 0xEF]);
}

/// Uncompressed SECTOR-mode framing: a contiguous image encrypted in fixed
/// chunks (each reset to the seed), with a plaintext sub-header region
/// before `data_start`. Mirrors what Ghost writes for `-ia` + password.
#[test]
fn span_reader_chunked_sector_decryption() {
    use crate::gho_crypto::gho_encrypt_body;
    use std::io::Read as _;

    let seed = gho_seed_from_password(b"hunter2");
    let data_start = 0x600u64; // plaintext container + sub-header region
    let chunk = 64u64;

    // A plaintext "disk image" spanning several chunks (200 = 3*64 + 8, so
    // the final chunk is short — exercises the partial-tail path).
    let image: Vec<u8> = (0..200u32)
        .map(|i| (i.wrapping_mul(91) ^ 0x5A) as u8)
        .collect();

    // On disk: data_start plaintext bytes, then each `chunk`-sized slice of
    // the image encrypted independently (cipher reset per chunk).
    let mut disk = vec![0u8; data_start as usize];
    disk[0] = 0xFE;
    disk[1] = 0xEF;
    for off in (0..image.len()).step_by(chunk as usize) {
        let end = (off + chunk as usize).min(image.len());
        let mut seg = image[off..end].to_vec();
        gho_encrypt_body(&mut seg, seed);
        disk.extend_from_slice(&seg);
    }
    let end = disk.len() as u64;

    let path = write_to_temp(&disk);
    let mut sr = SpanReader::open(std::slice::from_ref(&path)).unwrap();
    sr.enable_chunked_decryption(seed, data_start, chunk, end);

    // Sequential read: plaintext sub-header passthrough + decrypted image.
    let mut got = Vec::new();
    sr.read_to_end(&mut got).unwrap();
    let mut expected = vec![0u8; data_start as usize];
    expected[0] = 0xFE;
    expected[1] = 0xEF;
    expected.extend_from_slice(&image);
    assert_eq!(got, expected, "sequential chunked decrypt mismatch");

    // Random access straddling a chunk boundary (image[60..70)).
    sr.seek(SeekFrom::Start(data_start + 60)).unwrap();
    let mut b = [0u8; 10];
    sr.read_exact(&mut b).unwrap();
    assert_eq!(&b, &image[60..70], "cross-chunk random read");

    // Into the short final chunk (image[195..200)).
    sr.seek(SeekFrom::Start(data_start + 195)).unwrap();
    let mut tail = [0u8; 5];
    sr.read_exact(&mut tail).unwrap();
    assert_eq!(&tail, &image[195..200], "final short chunk");

    // Sub-header bytes before data_start pass through unchanged.
    sr.seek(SeekFrom::Start(0)).unwrap();
    let mut hdr = [0u8; 2];
    sr.read_exact(&mut hdr).unwrap();
    assert_eq!(&hdr, &[0xFE, 0xEF]);
}

/// A `0x0703` continuation that *closes* the block stream is a terminator,
/// not a partition boundary. Ghost emits one at the end of a single-volume
/// sector image; counting it would synthesize a bogus empty 2nd partition
/// and mis-detect a superfloppy as MBR. A `0x0703` *between* blocks is a
/// real boundary and must survive.
#[test]
fn index_sector_blocks_drops_trailing_continuation() {
    use std::io::Cursor;

    // A compressed block as the indexer sees it: [u16 stored_len][body].
    // (Indexing only reads the length prefix; bodies aren't decompressed.)
    let block = |n: u16| -> Vec<u8> {
        let mut b = n.to_le_bytes().to_vec();
        b.resize(n as usize, 0);
        b
    };
    // A 0x0703 continuation record: type|marker|magic|body_len + body.
    let cont = || -> Vec<u8> {
        let mut h = GHO_REC_CONTINUATION.to_le_bytes().to_vec();
        h.extend_from_slice(&[0u8, 0u8]); // marker
        h.extend_from_slice(&GHO_RECORD_MAGIC.to_le_bytes());
        h.extend_from_slice(&20u16.to_le_bytes()); // body_len
        h.resize(h.len() + 20, 0);
        h
    };

    // Single volume: 3 blocks then a trailing continuation terminator.
    let mut s = Vec::new();
    for _ in 0..3 {
        s.extend(block(100));
    }
    s.extend(cont());
    let mut cur = Cursor::new(s.clone());
    let idx = index_sector_blocks(&mut cur, 0, s.len() as u64).unwrap();
    assert_eq!(idx.blocks.len(), 3);
    assert!(
        idx.partition_boundaries.is_empty(),
        "trailing 0x0703 must not be counted as a partition boundary"
    );

    // Two volumes: blocks | continuation | blocks | trailing continuation.
    let mut s2 = Vec::new();
    for _ in 0..2 {
        s2.extend(block(100));
    }
    s2.extend(cont());
    for _ in 0..2 {
        s2.extend(block(100));
    }
    s2.extend(cont());
    let mut cur2 = Cursor::new(s2.clone());
    let idx2 = index_sector_blocks(&mut cur2, 0, s2.len() as u64).unwrap();
    assert_eq!(idx2.blocks.len(), 4);
    assert_eq!(
        idx2.partition_boundaries,
        vec![2],
        "mid-stream boundary kept; trailing terminator dropped"
    );
}
