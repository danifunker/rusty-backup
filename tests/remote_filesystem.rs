//! End-to-end test for the Family F read path: a `RemoteFilesystem` browsing
//! and reading a real FAT image over a loopback `rb-cli serve` daemon.
//!
//! Binds a port-0 listener *before* spawning the daemon thread, so the connect
//! lands in the OS listen backlog with no sleep/race, then drives the adapter.
#![cfg(feature = "remote")]

use std::net::TcpListener;
use std::sync::Arc;

use rusty_backup::cli::resolve::resolve_partition_rw;
use rusty_backup::fs::filesystem::{CreateFileOptions, Filesystem};
use rusty_backup::fs::{fat, open_editable_filesystem};
use rusty_backup::remote::{serve_on, RemoteConnection, RemoteFilesystem, RemoteHostFilesystem};

/// Build an 8 MiB FAT image at `path` with a single file `file_name` full of
/// `byte` repeated `len` times, under volume label `vol`.
fn make_fat_image(path: &std::path::Path, vol: &str, file_name: &str, byte: u8, len: usize) {
    std::fs::write(
        path,
        fat::create_blank_fat(8 * 1024 * 1024, Some(vol)).unwrap(),
    )
    .unwrap();
    let payload = vec![byte; len];
    let (file, ctx, commit) = resolve_partition_rw(path, None).unwrap();
    let mut efs =
        open_editable_filesystem(file, ctx.offset, ctx.type_byte, ctx.type_string.as_deref())
            .unwrap();
    let parent = efs.root().unwrap();
    let mut data = &payload[..];
    efs.create_file(
        &parent,
        file_name,
        &mut data,
        payload.len() as u64,
        &CreateFileOptions::default(),
    )
    .unwrap();
    efs.sync_metadata().unwrap();
    drop(efs);
    commit.commit().unwrap();
}

#[test]
fn remote_filesystem_browses_and_reads_over_loopback() {
    // --- build a FAT image with a known file under a temp serve root ---
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let img = root.join("disk.img");
    std::fs::write(
        &img,
        fat::create_blank_fat(8 * 1024 * 1024, Some("TESTVOL")).unwrap(),
    )
    .unwrap();

    let payload = vec![0x42u8; 5000];
    {
        let (file, ctx, commit) = resolve_partition_rw(&img, None).unwrap();
        let mut efs =
            open_editable_filesystem(file, ctx.offset, ctx.type_byte, ctx.type_string.as_deref())
                .unwrap();
        let parent = efs.root().unwrap();
        let mut data = &payload[..];
        efs.create_file(
            &parent,
            "BLOB.BIN",
            &mut data,
            payload.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        efs.sync_metadata().unwrap();
        drop(efs);
        commit.commit().unwrap();
    }

    // A plain host file in the serve root, for the host-FS browser test.
    let note = b"a host-side note for the file browser\n";
    std::fs::write(root.join("notes.txt"), note).unwrap();

    // --- start the daemon on a pre-bound port-0 listener ---
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serve_root = root.clone();
    std::thread::spawn(move || {
        let _ = serve_on(listener, serve_root, None);
    });

    // --- drive RemoteFilesystem over the wire ---
    let (mut rfs, root_entry, root_children) =
        RemoteFilesystem::open(&addr, "/disk.img", None).unwrap();

    // Metadata from the OpenImage response.
    assert!(
        rfs.fs_type().starts_with("FAT"),
        "fs_type was {:?}",
        rfs.fs_type()
    );
    assert!(rfs.total_size() > 0);

    // Root listing (both the open() result and a fresh list_directory).
    assert!(root_children.iter().any(|e| e.name == "BLOB.BIN"));
    let listed = rfs.list_directory(&root_entry).unwrap();
    let blob = listed
        .iter()
        .find(|e| e.name == "BLOB.BIN")
        .expect("BLOB.BIN present in remote listing");
    assert_eq!(blob.size, payload.len() as u64);

    // Read the file back byte-exact via the streaming write_file_to path.
    let mut out = Vec::new();
    rfs.write_file_to(blob, &mut out).unwrap();
    assert_eq!(out, payload, "remote read must be byte-exact");

    // And via read_file with a cap.
    let capped = rfs.read_file(blob, 100).unwrap();
    assert_eq!(capped.len(), 100);
    assert_eq!(&capped[..], &payload[..100]);

    // --- the host-FS file browser (RemoteHostFilesystem) ---
    let (mut host_fs, host_root, host_children) = RemoteHostFilesystem::open(&addr, "/").unwrap();
    assert_eq!(host_fs.fs_type(), "remote-host");
    // The serve root holds disk.img (a file to "Open Image") + notes.txt.
    let names: Vec<&str> = host_children.iter().map(|e| e.name.as_str()).collect();
    assert!(names.contains(&"disk.img"), "host listing: {names:?}");
    assert!(names.contains(&"notes.txt"), "host listing: {names:?}");
    // A fresh list_directory(root) agrees.
    let relisted = host_fs.list_directory(&host_root).unwrap();
    let notes = relisted
        .iter()
        .find(|e| e.name == "notes.txt")
        .expect("notes.txt in host listing");
    // Read a host file off the remote, byte-exact.
    let mut got = Vec::new();
    host_fs.write_file_to(notes, &mut got).unwrap();
    assert_eq!(got, note, "host-file read must be byte-exact");
}

/// Proves the core of "switch images without reconnecting": two images opened
/// as separate handles on **one** [`RemoteConnection`], browsed and read
/// interleaved over the single socket, with handle bookkeeping that releases on
/// view drop.
#[test]
fn two_images_open_on_one_connection_without_reconnect() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    make_fat_image(&root.join("a.img"), "VOLA", "AAA.BIN", 0x11, 4096);
    make_fat_image(&root.join("b.img"), "VOLB", "BBB.BIN", 0x22, 4096);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serve_root = root.clone();
    std::thread::spawn(move || {
        let _ = serve_on(listener, serve_root, None);
    });

    // ONE connection; open BOTH images on it as distinct handles — no reconnect.
    let conn = RemoteConnection::connect_shared(&addr).unwrap();
    let (mut a, a_root, _) =
        RemoteFilesystem::on_connection(Arc::clone(&conn), "/a.img", None).unwrap();
    let (mut b, b_root, _) =
        RemoteFilesystem::on_connection(Arc::clone(&conn), "/b.img", None).unwrap();

    assert_ne!(a.handle(), b.handle(), "each open gets a fresh handle");
    assert_eq!(
        conn.lock().unwrap().open_handle_count(),
        2,
        "both images live on the one connection"
    );
    assert_eq!(a.volume_label(), Some("VOLA"));
    assert_eq!(b.volume_label(), Some("VOLB"));

    // Locate each blob, then interleave reads across the two images. If the
    // single socket were getting confused between handles, the bytes would not
    // come back clean.
    let a_blob = a
        .list_directory(&a_root)
        .unwrap()
        .into_iter()
        .find(|e| e.name == "AAA.BIN")
        .unwrap();
    let b_blob = b
        .list_directory(&b_root)
        .unwrap()
        .into_iter()
        .find(|e| e.name == "BBB.BIN")
        .unwrap();

    let mut a_out = Vec::new();
    a.write_file_to(&a_blob, &mut a_out).unwrap();
    let mut b_out = Vec::new();
    b.write_file_to(&b_blob, &mut b_out).unwrap();
    // Re-read A *after* B on the same socket — switching back needs no reconnect.
    let mut a_again = Vec::new();
    a.write_file_to(&a_blob, &mut a_again).unwrap();

    assert_eq!(a_out, vec![0x11u8; 4096]);
    assert_eq!(b_out, vec![0x22u8; 4096]);
    assert_eq!(a_again, a_out, "re-reading A after B is byte-exact");

    // Dropping a view releases its handle on the shared connection.
    drop(a);
    assert_eq!(conn.lock().unwrap().open_handle_count(), 1);
    drop(b);
    assert_eq!(conn.lock().unwrap().open_handle_count(), 0);
}

/// Drives the reusable browser core (`model::remote_browser::RemoteBrowser`)
/// through the full file-browser flow on a single connection: connect -> host
/// listing -> open image -> switch to another image without reconnecting ->
/// close back to host. Asserts the metadata, a byte-exact in-image read, and the
/// daemon's per-connection handle count at each step.
#[test]
fn browser_core_opens_switches_and_closes_on_one_connection() {
    use rusty_backup::model::remote_browser::{BrowseMode, RemoteBrowser};

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    make_fat_image(&root.join("a.img"), "VOLA", "AAA.BIN", 0x11, 4096);
    make_fat_image(&root.join("b.img"), "VOLB", "BBB.BIN", 0x22, 4096);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serve_root = root.clone();
    std::thread::spawn(move || {
        let _ = serve_on(listener, serve_root, None);
    });

    // Connect -> the host browser sees both images, no handles open yet.
    let (mut browser, mut current) = RemoteBrowser::connect(&addr, "/").unwrap();
    assert_eq!(*browser.mode(), BrowseMode::Host);
    assert!(current.is_host());
    let names: Vec<&str> = current.entries.iter().map(|e| e.name.as_str()).collect();
    assert!(
        names.contains(&"a.img") && names.contains(&"b.img"),
        "host listing: {names:?}"
    );
    assert_eq!(browser.connection().lock().unwrap().open_handle_count(), 0);

    // Open image A on the connection (reassigning `current` drops the host view).
    current = browser.open_image("/a.img", None, "/").unwrap();
    assert!(matches!(browser.mode(), BrowseMode::Image { .. }));
    assert!(current.fs_type.starts_with("FAT"), "{}", current.fs_type);
    assert_eq!(current.volume_label, "VOLA");
    assert_eq!(browser.connection().lock().unwrap().open_handle_count(), 1);

    // Read a file inside A byte-exact, straight through the boxed view.
    let a_root = current.fs.root().unwrap();
    let a_blob = current
        .fs
        .list_directory(&a_root)
        .unwrap()
        .into_iter()
        .find(|e| e.name == "AAA.BIN")
        .unwrap();
    let mut a_bytes = Vec::new();
    current.fs.write_file_to(&a_blob, &mut a_bytes).unwrap();
    assert_eq!(a_bytes, vec![0x11u8; 4096]);

    // Switch straight to image B with NO reconnect. Reassigning `current` drops
    // A's view (releasing its handle); B's handle replaces it -> still 1 open.
    current = browser.open_image("/b.img", None, "/").unwrap();
    assert_eq!(current.volume_label, "VOLB");
    assert_eq!(browser.connection().lock().unwrap().open_handle_count(), 1);

    // Close the image -> back to the host browser at the return directory; B's
    // handle is released when its view drops on reassignment.
    current = browser.close_image().unwrap();
    assert!(current.is_host());
    assert_eq!(*browser.mode(), BrowseMode::Host);
    assert_eq!(browser.connection().lock().unwrap().open_handle_count(), 0);

    drop(current);
    drop(browser);
}

/// fsck over the wire: `run_fsck_reader` opens a remote image's filesystem
/// through a `RemoteBlockReader` and runs its checker — read-only, no download.
#[test]
fn fsck_runs_over_block_reader() {
    use rusty_backup::model::fsck_runner::run_fsck_reader;
    use rusty_backup::remote::{RemoteBlockReader, RemoteConnection};

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    make_fat_image(&root.join("chk.img"), "CHKVOL", "DATA.BIN", 0x33, 1024);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serve_root = root.clone();
    std::thread::spawn(move || {
        let _ = serve_on(listener, serve_root, None);
    });

    let conn = RemoteConnection::connect_shared(&addr).unwrap();
    let reader = RemoteBlockReader::open(conn, "/chk.img").unwrap();
    // Opens the filesystem over the wire and runs its checker (Some) or reports
    // "not supported" (None) — either way it must not error.
    let res = run_fsck_reader(reader, 0, 0, None);
    assert!(res.is_ok(), "fsck over the block reader errored: {res:?}");
}

/// The exact per-partition **browse** path the Inspect GUI uses: a
/// `BrowseSession` with a remote source opens the filesystem over the block tier
/// (a `RemoteBlockReader`) and reads a file byte-exact. Editing is refused.
#[test]
fn browse_session_opens_remote_image_over_block_tier() {
    use rusty_backup::model::browse_session::BrowseSession;
    use rusty_backup::remote::RemoteConnection;

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    make_fat_image(&root.join("vol.img"), "BROWSEVOL", "HELLO.TXT", 0x5A, 2048);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serve_root = root.clone();
    std::thread::spawn(move || {
        let _ = serve_on(listener, serve_root, None);
    });

    let conn = RemoteConnection::connect_shared(&addr).unwrap();
    let session = BrowseSession {
        partition_offset: 0,
        partition_type: 0, // auto-detect superfloppy
        remote: Some((conn, "/vol.img".to_string())),
        ..Default::default()
    };

    // open() builds a RemoteBlockReader and parses the FS over the wire.
    let mut fs = session.open().unwrap();
    assert!(fs.fs_type().starts_with("FAT"), "fs_type {}", fs.fs_type());
    let root_e = fs.root().unwrap();
    let entries = fs.list_directory(&root_e).unwrap();
    let f = entries
        .iter()
        .find(|e| e.name == "HELLO.TXT")
        .expect("HELLO.TXT in remote volume");
    let mut got = Vec::new();
    fs.write_file_to(f, &mut got).unwrap();
    assert_eq!(
        got,
        vec![0x5A; 2048],
        "remote browse read must be byte-exact"
    );

    // Editing a remote image now opens read-write over the block tier (the
    // full round-trip is covered by `edit_remote_image_over_block_tier`).
    assert!(
        session.open_editable().is_ok(),
        "remote image opens editable over the block tier"
    );
}

/// Remote RESTORE: back up a partitioned FAT disk locally, then restore that
/// backup folder to an **image-file target on a remote daemon** over the block
/// tier (materialize-to-temp + WriteBlock push). Proves the landed remote image
/// is a full disk the engine can parse end to end — MBR + FAT partition + the
/// file byte-exact — i.e. the restore reached the daemon intact.
#[test]
fn restore_to_remote_image_round_trips() {
    use std::sync::Mutex;

    use rusty_backup::backup::{
        run_backup, BackupConfig, BackupProgress, ChecksumType, CompressionType,
    };
    use rusty_backup::fs::open_filesystem;
    use rusty_backup::model::restore_remote::restore_to_remote;
    use rusty_backup::partition::mbr::{build_minimal_mbr, Mbr};
    use rusty_backup::remote::RemoteConnection;
    use rusty_backup::restore::{RestoreAlignment, RestoreConfig, RestoreProgress};

    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().canonicalize().unwrap();

    // --- build a partitioned disk: MBR + one FAT16 partition with a file ---
    let part_lba = 2048u32;
    let part_bytes = 8 * 1024 * 1024usize;
    let part_sectors = (part_bytes / 512) as u32;
    let fat_blob = fat::create_blank_fat(part_bytes as u64, Some("RSTVOL")).unwrap();
    let mbr = build_minimal_mbr(
        0x5152_5354,
        &[(0x06, part_lba, part_sectors, true)],
        255,
        63,
    );
    let mut disk = vec![0u8; part_lba as usize * 512 + part_bytes];
    disk[..512].copy_from_slice(&mbr);
    disk[part_lba as usize * 512..].copy_from_slice(&fat_blob);
    let src_img = work.join("source.img");
    std::fs::write(&src_img, &disk).unwrap();

    let payload = vec![0x42u8; 11000]; // multi-cluster
    {
        let (file, ctx, commit) = resolve_partition_rw(&src_img, Some(1)).unwrap();
        let mut efs =
            open_editable_filesystem(file, ctx.offset, ctx.type_byte, ctx.type_string.as_deref())
                .unwrap();
        let parent = efs.root().unwrap();
        let mut data = &payload[..];
        efs.create_file(
            &parent,
            "DOC.BIN",
            &mut data,
            payload.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        efs.sync_metadata().unwrap();
        drop(efs);
        commit.commit().unwrap();
    }
    let disk_size = std::fs::metadata(&src_img).unwrap().len();

    // --- back it up locally (raw, sector-by-sector) ---
    let backups = work.join("backups");
    std::fs::create_dir_all(&backups).unwrap();
    let bcfg = BackupConfig {
        source_path: src_img.clone(),
        destination_dir: backups.clone(),
        backup_name: "rst".to_string(),
        compression: CompressionType::None,
        checksum: ChecksumType::Crc32,
        split_size_mib: None,
        sector_by_sector: true,
        partition_filter: None,
        chd_options: None,
        size_policy: None,
        partition_target_sizes: None,
        shrink_to_minimum: false,
        precomputed_minimum_sizes: None,
        defrag_partition_indices: None,
        defrag_fat: false,
    };
    run_backup(bcfg, Arc::new(Mutex::new(BackupProgress::default()))).expect("local backup");
    let backup_folder = backups.join("rst");
    assert!(
        backup_folder.join("metadata.json").exists(),
        "backup created"
    );

    // --- serve an (initially empty) restore-target root ---
    let serve_root = work.join("serve");
    std::fs::create_dir_all(&serve_root).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let sr = serve_root.clone();
    std::thread::spawn(move || {
        let _ = serve_on(listener, sr, None);
    });
    let conn = RemoteConnection::connect_shared(&addr).unwrap();

    // --- restore the backup to a remote image file over the wire ---
    let rcfg = RestoreConfig {
        backup_folder,
        target_path: std::path::PathBuf::new(),
        target_is_device: false,
        target_size: disk_size,
        alignment: RestoreAlignment::Original,
        partition_sizes: Vec::new(),
        write_zeros_to_unused: false,
    };
    let progress = Arc::new(Mutex::new(RestoreProgress::default()));
    restore_to_remote(
        rcfg,
        Arc::clone(&conn),
        "/restored.img",
        false,
        progress,
        None,
    )
    .expect("remote restore");

    // --- the landed remote image is a full, parseable disk ---
    let restored = serve_root.join("restored.img");
    assert_eq!(
        std::fs::metadata(&restored).unwrap().len(),
        disk_size,
        "restored image is the full disk size"
    );
    let mut f = std::fs::File::open(&restored).unwrap();
    let mut sector = [0u8; 512];
    use std::io::Read as _;
    f.read_exact(&mut sector).unwrap();
    let p = Mbr::parse(&sector)
        .unwrap()
        .entries
        .into_iter()
        .find(|e| !e.is_empty())
        .expect("a partition in the restored MBR");
    assert_eq!(p.partition_type, 0x06);
    let mut fs = open_filesystem(
        std::fs::File::open(&restored).unwrap(),
        p.start_lba as u64 * 512,
        p.partition_type,
        None,
    )
    .unwrap();
    let root_e = fs.root().unwrap();
    let doc = fs
        .list_directory(&root_e)
        .unwrap()
        .into_iter()
        .find(|e| e.name == "DOC.BIN")
        .expect("DOC.BIN in the restored partition");
    let mut got = Vec::new();
    fs.write_file_to(&doc, &mut got).unwrap();
    assert_eq!(
        got, payload,
        "restored file is byte-exact after the remote push"
    );
}

/// Remote RESIZE over the block tier: shrink a served FAT superfloppy's
/// filesystem in place through the read-write block reader, then re-open and
/// prove the volume now reports the smaller size and the file still round-trips
/// — `resize_filesystem_for` patching the BPB/FAT over the wire, never touching
/// the image's byte length.
#[test]
fn resize_remote_image_over_block_tier() {
    use rusty_backup::model::resize_remote::resize_remote_partition;
    use rusty_backup::remote::RemoteConnection;

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let img = root.join("resize.img");
    // 8 MiB FAT superfloppy with a multi-cluster file near the start.
    make_fat_image(&img, "RESIZEVOL", "KEEP.BIN", 0x5E, 9000);
    let image_len_before = std::fs::metadata(&img).unwrap().len();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serve_root = root.clone();
    std::thread::spawn(move || {
        let _ = serve_on(listener, serve_root, None);
    });

    let conn = RemoteConnection::connect_shared(&addr).unwrap();

    // Baseline FS size over the wire (before resize).
    let size_before = rusty_backup::model::browse_session::BrowseSession {
        partition_offset: 0,
        partition_type: 0,
        remote: Some((Arc::clone(&conn), "/resize.img".to_string())),
        ..Default::default()
    }
    .open()
    .unwrap()
    .total_size();

    // Shrink the filesystem to 6 MiB, in place over the wire.
    let new_size = 6 * 1024 * 1024;
    let mut log = Vec::new();
    let outcome =
        resize_remote_partition(Arc::clone(&conn), "/resize.img", None, new_size, &mut |s| {
            log.push(s.to_string())
        })
        .expect("remote resize");
    assert_eq!(outcome.partition_offset, 0, "superfloppy resizes at byte 0");

    // Re-open over the wire: the volume shrank and the file still round-trips.
    let session = rusty_backup::model::browse_session::BrowseSession {
        partition_offset: 0,
        partition_type: 0,
        remote: Some((Arc::clone(&conn), "/resize.img".to_string())),
        ..Default::default()
    };
    let mut fs = session.open().unwrap();
    let size_after = fs.total_size();
    assert!(
        size_after < size_before,
        "FS shrank over the wire ({size_before} -> {size_after})"
    );
    assert!(
        size_after <= new_size,
        "resized FS fits the requested {new_size} bytes (got {size_after})"
    );
    let root_e = fs.root().unwrap();
    let keep = fs
        .list_directory(&root_e)
        .unwrap()
        .into_iter()
        .find(|e| e.name == "KEEP.BIN")
        .expect("KEEP.BIN survives the resize");
    let mut got = Vec::new();
    fs.write_file_to(&keep, &mut got).unwrap();
    assert_eq!(
        got,
        vec![0x5E; 9000],
        "file round-trips after remote resize"
    );

    // The resize patched FS metadata in place — the image file did not change
    // length (a block-tier resize never grows/truncates the image).
    assert_eq!(
        std::fs::metadata(&img).unwrap().len(),
        image_len_before,
        "remote resize must not change the image's byte length"
    );
}

/// Remote EDITING over the block tier: open a served FAT image read-WRITE
/// through a `RemoteBlockReader`, add a file + sync, then re-open read-only and
/// prove the new file is present byte-exact, the original survived, and the
/// image was edited *in place* (size unchanged) — `open_editable_filesystem`
/// patching byte ranges over the wire.
#[test]
fn edit_remote_image_over_block_tier() {
    use rusty_backup::model::browse_session::BrowseSession;
    use rusty_backup::remote::{RemoteBlockReader, RemoteConnection};

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let img = root.join("edit.img");
    make_fat_image(&img, "EDITVOL", "ORIG.BIN", 0x11, 4096);
    let size_before = std::fs::metadata(&img).unwrap().len();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serve_root = root.clone();
    std::thread::spawn(move || {
        let _ = serve_on(listener, serve_root, None);
    });

    let conn = RemoteConnection::connect_shared(&addr).unwrap();
    let session = BrowseSession {
        partition_offset: 0,
        partition_type: 0, // superfloppy auto-detect
        remote: Some((Arc::clone(&conn), "/edit.img".to_string())),
        ..Default::default()
    };

    // Edit over the wire: open read-write, add a multi-cluster file, sync,
    // commit (a no-op for remote — writes already landed on the daemon).
    let new_payload = vec![0x77u8; 9000];
    {
        let (mut efs, commit) = session.open_editable().expect("open_editable remote");
        let parent = efs.root().unwrap();
        let mut data = &new_payload[..];
        efs.create_file(
            &parent,
            "NEW.BIN",
            &mut data,
            new_payload.len() as u64,
            &CreateFileOptions::default(),
        )
        .expect("create_file over the wire");
        efs.sync_metadata().expect("sync over the wire");
        drop(efs);
        commit.commit().expect("commit (no-op for remote)");
    }

    // Re-open read-only over the same connection and verify the edit landed.
    let mut fs = session.open().expect("re-open remote read-only");
    let root_e = fs.root().unwrap();
    let entries = fs.list_directory(&root_e).unwrap();
    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    assert!(names.contains(&"NEW.BIN"), "new file present: {names:?}");
    assert!(names.contains(&"ORIG.BIN"), "original survived: {names:?}");

    let new = entries.iter().find(|e| e.name == "NEW.BIN").unwrap();
    assert_eq!(new.size, new_payload.len() as u64, "new file size recorded");
    let mut got = Vec::new();
    fs.write_file_to(new, &mut got).unwrap();
    assert_eq!(got, new_payload, "remotely-written file is byte-exact");

    // The edit was in place — the image file did not grow.
    assert_eq!(
        std::fs::metadata(&img).unwrap().len(),
        size_before,
        "block-tier edit must not change the image size"
    );

    // A read-only block reader refuses writes, so an image opened for
    // inspection can't be corrupted by a stray write.
    use std::io::Write as _;
    let mut ro = RemoteBlockReader::open(Arc::clone(&conn), "/edit.img").unwrap();
    assert!(
        ro.write(&[0u8; 16]).is_err(),
        "writing a read-only block handle must fail"
    );
}

/// Remote fsck REPAIR over the block tier: serve an AFFS volume with a
/// corrupted allocation bitmap, repair it through a read-WRITE
/// `RemoteBlockReader`, and prove fsck reports it clean afterwards — the repair
/// patched the image in place over the wire (the read-only repair gap is now
/// closed). Mirrors the in-module `fsck_detects_and_repairs_bitmap_mismatch`
/// unit test, but driven entirely over the daemon.
#[test]
fn repair_remote_image_over_block_tier() {
    use rusty_backup::fs::affs::create_blank_affs;
    use rusty_backup::fs::affs_common::bitmap_checksum;
    use rusty_backup::model::fsck_runner::{run_fsck_reader, run_repair_reader};
    use rusty_backup::remote::{RemoteBlockReader, RemoteConnection};

    const BSIZE: usize = 512;
    const BITMAP_BLOCK: usize = 881; // 880K floppy: root=880, bitmap=881

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let img = root.join("affs.adf");

    // Blank 880K AFFS (FFS) volume, then allocate a file so low blocks (the
    // bitmap word covering blocks 2..33) are genuinely in use.
    std::fs::write(&img, create_blank_affs(901_120, 1, "RemoteRepair").unwrap()).unwrap();
    {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&img)
            .unwrap();
        let mut efs = open_editable_filesystem(file, 0, 0, None).unwrap();
        let parent = efs.root().unwrap();
        let payload = vec![0xCDu8; 1500];
        let mut src = &payload[..];
        efs.create_file(
            &parent,
            "DATA",
            &mut src,
            1500,
            &CreateFileOptions::default(),
        )
        .unwrap();
        efs.sync_metadata().unwrap();
    }

    // Corrupt the bitmap: flip the word covering blocks 2..33 to "all free"
    // (set bit = free on AFFS), then fix the page checksum so fsck reads a real
    // allocation mismatch rather than a checksum error.
    {
        let mut bytes = std::fs::read(&img).unwrap();
        let bm = BITMAP_BLOCK * BSIZE;
        bytes[bm + 4..bm + 8].copy_from_slice(&0xFFFF_FFFFu32.to_be_bytes());
        let sum = bitmap_checksum(&bytes[bm..bm + BSIZE]);
        bytes[bm..bm + 4].copy_from_slice(&sum.to_be_bytes());
        std::fs::write(&img, &bytes).unwrap();
    }

    // Serve it.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serve_root = root.clone();
    std::thread::spawn(move || {
        let _ = serve_on(listener, serve_root, None);
    });
    let conn = RemoteConnection::connect_shared(&addr).unwrap();

    // fsck over the wire detects the mismatch.
    let before = run_fsck_reader(
        RemoteBlockReader::open(Arc::clone(&conn), "/affs.adf").unwrap(),
        0,
        0,
        None,
    )
    .unwrap()
    .expect("AFFS provides a checker");
    assert!(!before.is_clean(), "fsck should detect the bitmap mismatch");

    // Repair over the wire (read-write reader); a fix is applied.
    let report = run_repair_reader(
        RemoteBlockReader::open_rw(Arc::clone(&conn), "/affs.adf").unwrap(),
        0,
        0,
        None,
    )
    .expect("remote repair");
    assert!(
        !report.fixes_applied.is_empty(),
        "repair applied a fix over the wire (applied={}, failed={})",
        report.fixes_applied.len(),
        report.fixes_failed.len(),
    );

    // Re-check over the wire: clean — the repair persisted on the daemon.
    let after = run_fsck_reader(
        RemoteBlockReader::open(Arc::clone(&conn), "/affs.adf").unwrap(),
        0,
        0,
        None,
    )
    .unwrap()
    .expect("checker");
    assert!(
        after.is_clean(),
        "fsck clean after remote repair; errors: {:?}",
        after.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
    );
}

/// The **block tier**: the existing engine parses a *remote* partitioned disk
/// image's MBR and opens the filesystem inside a partition — reading a file
/// byte-exact — entirely over a `RemoteBlockReader` (ranged reads), with no
/// download. This is what lets Inspect run its full pipeline against a remote
/// image.
#[test]
fn block_reader_parses_remote_partition_table_and_filesystem() {
    use std::io::{Read, Seek, SeekFrom};

    use rusty_backup::fs::open_filesystem;
    use rusty_backup::partition::mbr::{build_minimal_mbr, Mbr};
    use rusty_backup::remote::{RemoteBlockReader, RemoteConnection};

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let img = root.join("disk.img");

    // Build a partitioned disk: MBR + one FAT partition at LBA 2048.
    let part_lba = 2048u32;
    let part_bytes = 8 * 1024 * 1024usize;
    let part_sectors = (part_bytes / 512) as u32;
    let fat_blob = fat::create_blank_fat(part_bytes as u64, Some("REMOTEVOL")).unwrap();
    let mbr = build_minimal_mbr(
        0xDEAD_BEEF,
        &[(0x06, part_lba, part_sectors, true)],
        255,
        63,
    );
    let mut disk = vec![0u8; part_lba as usize * 512 + part_bytes];
    disk[..512].copy_from_slice(&mbr);
    disk[part_lba as usize * 512..].copy_from_slice(&fat_blob);
    std::fs::write(&img, &disk).unwrap();

    // Write a known file into partition 1.
    let payload = vec![0xABu8; 9000];
    {
        let (file, ctx, commit) = resolve_partition_rw(&img, Some(1)).unwrap();
        let mut efs =
            open_editable_filesystem(file, ctx.offset, ctx.type_byte, ctx.type_string.as_deref())
                .unwrap();
        let parent = efs.root().unwrap();
        let mut data = &payload[..];
        efs.create_file(
            &parent,
            "REMOTE.BIN",
            &mut data,
            payload.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        efs.sync_metadata().unwrap();
        drop(efs);
        commit.commit().unwrap();
    }
    let disk_final = std::fs::read(&img).unwrap();

    // Serve it.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serve_root = root.clone();
    std::thread::spawn(move || {
        let _ = serve_on(listener, serve_root, None);
    });

    let conn = RemoteConnection::connect_shared(&addr).unwrap();

    // 1. The block reader is byte-exact + seekable vs the local file.
    let mut reader = RemoteBlockReader::open(Arc::clone(&conn), "/disk.img").unwrap();
    assert_eq!(reader.len(), disk_final.len() as u64);
    for &off in &[
        0u64,
        512,
        1024 * 1024 + 3,
        part_lba as u64 * 512,
        disk_final.len() as u64 - 100,
    ] {
        reader.seek(SeekFrom::Start(off)).unwrap();
        let mut chunk = vec![0u8; 100];
        reader.read_exact(&mut chunk).unwrap();
        assert_eq!(
            chunk,
            &disk_final[off as usize..off as usize + 100],
            "block read mismatch at offset {off}"
        );
    }

    // 2. Parse the MBR over the wire.
    reader.seek(SeekFrom::Start(0)).unwrap();
    let mut sector = [0u8; 512];
    reader.read_exact(&mut sector).unwrap();
    let parsed = Mbr::parse(&sector).unwrap();
    let p = parsed
        .entries
        .iter()
        .find(|e| !e.is_empty())
        .expect("a partition in the remote MBR");
    assert_eq!(p.partition_type, 0x06);
    assert_eq!(p.start_lba, part_lba);

    // 3. Open the filesystem in that partition THROUGH the block reader and read
    //    the file byte-exact — the full engine stack over ranged reads.
    let part_off = p.start_lba as u64 * 512;
    let ptype = p.partition_type;
    let mut fs = open_filesystem(reader, part_off, ptype, None).unwrap();
    assert!(fs.fs_type().starts_with("FAT"), "fs_type {}", fs.fs_type());
    let root_e = fs.root().unwrap();
    let entries = fs.list_directory(&root_e).unwrap();
    let blob = entries
        .iter()
        .find(|e| e.name == "REMOTE.BIN")
        .expect("REMOTE.BIN in the remote partition");
    assert_eq!(blob.size, payload.len() as u64);
    let mut got = Vec::new();
    fs.write_file_to(blob, &mut got).unwrap();
    assert_eq!(
        got, payload,
        "remote file read via the block reader must be byte-exact"
    );
}

/// End-to-end remote backup: serve a partitioned image, pull a full backup to a
/// local destination over the block tier, and prove the captured partition is
/// byte-exact vs the source. This is the headline "remote-image BACKUP" path —
/// `run_backup_from` reading every byte through a `RemoteBlockReader`.
#[test]
fn run_backup_pulls_remote_image_byte_exact() {
    use std::sync::Mutex;

    use rusty_backup::backup::{
        run_backup_from, BackupConfig, BackupProgress, BackupSource, ChecksumType, CompressionType,
    };
    use rusty_backup::partition::mbr::build_minimal_mbr;

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let img = root.join("disk.img");

    // Build a partitioned disk: MBR + one FAT16 partition at LBA 2048, with a
    // known file inside it.
    let part_lba = 2048u32;
    let part_bytes = 8 * 1024 * 1024usize;
    let part_sectors = (part_bytes / 512) as u32;
    let fat_blob = fat::create_blank_fat(part_bytes as u64, Some("BKUPVOL")).unwrap();
    let mbr = build_minimal_mbr(
        0x1234_5678,
        &[(0x06, part_lba, part_sectors, true)],
        255,
        63,
    );
    let mut disk = vec![0u8; part_lba as usize * 512 + part_bytes];
    disk[..512].copy_from_slice(&mbr);
    disk[part_lba as usize * 512..].copy_from_slice(&fat_blob);
    std::fs::write(&img, &disk).unwrap();

    let payload = vec![0x5Au8; 12345];
    {
        let (file, ctx, commit) = resolve_partition_rw(&img, Some(1)).unwrap();
        let mut efs =
            open_editable_filesystem(file, ctx.offset, ctx.type_byte, ctx.type_string.as_deref())
                .unwrap();
        let parent = efs.root().unwrap();
        let mut data = &payload[..];
        efs.create_file(
            &parent,
            "BACKUP.BIN",
            &mut data,
            payload.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        efs.sync_metadata().unwrap();
        drop(efs);
        commit.commit().unwrap();
    }
    let disk_final = std::fs::read(&img).unwrap();
    let part_off = part_lba as usize * 512;
    let source_partition = &disk_final[part_off..part_off + part_bytes];

    // Serve it.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serve_root = root.clone();
    std::thread::spawn(move || {
        let _ = serve_on(listener, serve_root, None);
    });
    let conn = RemoteConnection::connect_shared(&addr).unwrap();

    // Pull a raw, sector-by-sector backup over the wire so the captured
    // partition bytes can be compared verbatim against the source.
    let dest = root.join("out");
    std::fs::create_dir_all(&dest).unwrap();
    let display = format!("rb://{addr}/disk.img");
    let config = BackupConfig {
        source_path: std::path::PathBuf::from("/disk.img"),
        destination_dir: dest.clone(),
        backup_name: "remote-backup".to_string(),
        compression: CompressionType::None,
        checksum: ChecksumType::Crc32,
        split_size_mib: None,
        sector_by_sector: true,
        partition_filter: None,
        chd_options: None,
        size_policy: None,
        partition_target_sizes: None,
        shrink_to_minimum: false,
        precomputed_minimum_sizes: None,
        defrag_partition_indices: None,
        defrag_fat: false,
    };
    let progress = Arc::new(Mutex::new(BackupProgress::default()));
    let source = BackupSource::Remote {
        conn: Arc::clone(&conn),
        path: "/disk.img".to_string(),
        display: display.clone(),
        is_device: false,
    };
    run_backup_from(source, config, progress).expect("remote backup must succeed");

    // The backup folder records the remote source and the full disk size.
    let backup_dir = dest.join("remote-backup");
    let metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(backup_dir.join("metadata.json")).unwrap()).unwrap();
    assert_eq!(
        metadata["source_device"], display,
        "metadata records the remote source label"
    );
    assert_eq!(
        metadata["source_size_bytes"].as_u64().unwrap(),
        disk_final.len() as u64,
        "metadata records the full remote image size"
    );

    // The captured partition is byte-exact vs the source partition.
    let captured = std::fs::read(backup_dir.join("partition-0.raw"))
        .expect("partition-0.raw written by the remote backup");
    assert_eq!(
        captured.len(),
        source_partition.len(),
        "captured partition size matches source"
    );
    assert_eq!(
        captured, source_partition,
        "remote-pulled partition must be byte-exact vs the local source"
    );

    // And the MBR sidecar matches sector 0 of the source.
    let mbr_bin = std::fs::read(backup_dir.join("mbr.bin")).expect("mbr.bin written");
    assert_eq!(
        &mbr_bin[..512],
        &disk_final[..512],
        "mbr.bin matches sector 0"
    );
}

/// The physical-device path: `ListDevices` round-trips over the wire, and — when
/// a readable device exists — `OpenDevice` + a ranged read works end-to-end.
/// The device read is best-effort: opening a raw disk usually needs root, so a
/// permission failure is tolerated (the verb plumbing is still proven by the
/// `ListDevices` round-trip and the image-file block-tier tests above).
#[test]
fn list_devices_round_trips_and_open_device_reads() {
    use std::io::{Read, Seek, SeekFrom};

    use rusty_backup::remote::{RemoteBlockReader, RemoteConnection};

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serve_root = root.clone();
    std::thread::spawn(move || {
        let _ = serve_on(listener, serve_root, None);
    });
    let conn = RemoteConnection::connect_shared(&addr).unwrap();

    // 1. ListDevices round-trips: the daemon advertises its own physical disks.
    //    The set can legitimately be empty (CI sandboxes), so only assert it
    //    returns Ok and the entries are well-formed.
    let devices = conn.lock().unwrap().list_devices().unwrap();
    for d in &devices {
        assert!(!d.path.is_empty(), "device path is non-empty");
    }

    // 2. Best-effort: open the first device that reports a non-zero size and
    //    read its first sector over the block tier. Skip on permission errors
    //    (raw-disk read normally needs root) so the test stays green unprivileged.
    if let Some(dev) = devices.iter().find(|d| d.size_bytes >= 512) {
        match RemoteBlockReader::open_device(Arc::clone(&conn), &dev.path) {
            Ok(mut reader) => {
                assert_eq!(
                    reader.len(),
                    dev.size_bytes,
                    "device-backed reader length matches the enumerated size"
                );
                reader.seek(SeekFrom::Start(0)).unwrap();
                let mut sector = [0u8; 512];
                reader
                    .read_exact(&mut sector)
                    .expect("read the device's first sector over the wire");
                // No ground truth to compare against — reaching here proves
                // OpenDevice + ioctl-size + ReadBlock work on a real device.
            }
            Err(e) => {
                eprintln!("open_device skipped (need root for raw disk read): {e:#}");
            }
        }
    } else {
        eprintln!("no readable device enumerated; ListDevices round-trip still proven");
    }

    // 3. Opening a path that isn't an enumerated device is rejected (the sandbox
    //    guarantee: OpenDevice can't be used to read arbitrary files).
    let bogus = conn
        .lock()
        .unwrap()
        .open_device("/etc/hostname")
        .map(|_| ())
        .unwrap_err();
    let msg = format!("{bogus:#}");
    assert!(
        msg.contains("not an enumerated device"),
        "non-device path must be refused, got: {msg}"
    );
}

/// The Backup-tab remote-source core: connect + list devices, then parse a
/// remote source's partition table over the block tier. Exercised against a
/// served image (`is_device=false`) because the device-open path needs root;
/// the partition-load logic is identical either way.
#[test]
fn backup_remote_core_lists_and_loads_partitions() {
    use rusty_backup::model::backup_remote::{connect_and_list_devices, load_remote_source};
    use rusty_backup::partition::mbr::build_minimal_mbr;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let img = root.join("disk.img");

    let part_lba = 2048u32;
    let part_bytes = 8 * 1024 * 1024usize;
    let part_sectors = (part_bytes / 512) as u32;
    let fat_blob = fat::create_blank_fat(part_bytes as u64, Some("BKVOL")).unwrap();
    let mbr = build_minimal_mbr(
        0x0BAD_F00D,
        &[(0x06, part_lba, part_sectors, true)],
        255,
        63,
    );
    let mut disk = vec![0u8; part_lba as usize * 512 + part_bytes];
    disk[..512].copy_from_slice(&mbr);
    disk[part_lba as usize * 512..].copy_from_slice(&fat_blob);
    std::fs::write(&img, &disk).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serve_root = root.clone();
    std::thread::spawn(move || {
        let _ = serve_on(listener, serve_root, None);
    });

    // connect + list devices in one call (devices may be empty in CI).
    let (conn, _devices) = connect_and_list_devices(&addr).unwrap();

    // Parse the served image's partition table over the wire.
    let info = load_remote_source(Arc::clone(&conn), "/disk.img", false).unwrap();
    assert_eq!(info.size, disk.len() as u64, "reported full source size");
    assert!(!info.is_superfloppy, "MBR disk is not a superfloppy");
    assert!(
        info.table_desc.contains("MBR"),
        "table_desc was {:?}",
        info.table_desc
    );
    let real: Vec<_> = info
        .partitions
        .iter()
        .filter(|p| !p.is_extended_container)
        .collect();
    assert_eq!(real.len(), 1, "one FAT partition parsed over the wire");
    assert_eq!(real[0].start_lba, part_lba as u64);
}

/// Remote **CHD** backup: a single-file CHD needs random-access local `File`
/// reads, so the engine materializes the remote disk to a local temp first,
/// then runs the normal local CHD pipeline. Proves the produced `.chd` opens
/// and the partition data round-trips byte-exact — over the wire.
///
/// Smart (defrag-packed) mode, so it also exercises the FAT packer through the
/// full remote→materialize→CHD→pack→round-trip path. The 21000-byte file spans
/// many clusters, which would truncate before the FAT-type-preservation fix.
#[cfg(feature = "chd")]
#[test]
fn run_backup_chd_materializes_remote_and_round_trips() {
    use std::io::{Read, Seek, SeekFrom};
    use std::sync::Mutex;

    use rusty_backup::backup::{
        run_backup_from, BackupConfig, BackupProgress, BackupSource, ChecksumType, CompressionType,
    };
    use rusty_backup::fs::open_filesystem;
    use rusty_backup::partition::mbr::{build_minimal_mbr, Mbr};
    use rusty_backup::rbformats::chd::ChdReader;

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let img = root.join("disk.img");

    let part_lba = 2048u32;
    let part_bytes = 8 * 1024 * 1024usize;
    let part_sectors = (part_bytes / 512) as u32;
    let fat_blob = fat::create_blank_fat(part_bytes as u64, Some("CHDVOL")).unwrap();
    let mbr = build_minimal_mbr(
        0xC0FF_EE00,
        &[(0x06, part_lba, part_sectors, true)],
        255,
        63,
    );
    let mut disk = vec![0u8; part_lba as usize * 512 + part_bytes];
    disk[..512].copy_from_slice(&mbr);
    disk[part_lba as usize * 512..].copy_from_slice(&fat_blob);
    std::fs::write(&img, &disk).unwrap();

    let payload = vec![0x7Eu8; 21000];
    {
        let (file, ctx, commit) = resolve_partition_rw(&img, Some(1)).unwrap();
        let mut efs =
            open_editable_filesystem(file, ctx.offset, ctx.type_byte, ctx.type_string.as_deref())
                .unwrap();
        let parent = efs.root().unwrap();
        let mut data = &payload[..];
        efs.create_file(
            &parent,
            "CHD.BIN",
            &mut data,
            payload.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        efs.sync_metadata().unwrap();
        drop(efs);
        commit.commit().unwrap();
    }

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serve_root = root.clone();
    std::thread::spawn(move || {
        let _ = serve_on(listener, serve_root, None);
    });
    let conn = RemoteConnection::connect_shared(&addr).unwrap();

    let dest = root.join("out");
    std::fs::create_dir_all(&dest).unwrap();
    let config = BackupConfig {
        source_path: std::path::PathBuf::from("/disk.img"),
        destination_dir: dest.clone(),
        backup_name: "remote-chd".to_string(),
        compression: CompressionType::Chd,
        checksum: ChecksumType::Crc32,
        split_size_mib: None,
        // Smart mode: defrag-pack the FAT into the CHD (exercises the packer).
        sector_by_sector: false,
        partition_filter: None,
        chd_options: None,
        size_policy: None,
        partition_target_sizes: None,
        shrink_to_minimum: false,
        precomputed_minimum_sizes: None,
        defrag_partition_indices: None,
        defrag_fat: false,
    };
    let progress = Arc::new(Mutex::new(BackupProgress::default()));
    let source = BackupSource::Remote {
        conn: Arc::clone(&conn),
        path: "/disk.img".to_string(),
        display: format!("rb://{addr}/disk.img"),
        is_device: false,
    };
    run_backup_from(source, config, progress).expect("remote CHD backup must succeed");

    // The single-file CHD lands at <dest>/<name>/<name>.chd; the scratch temp
    // is cleaned up.
    let backup_dir = dest.join("remote-chd");
    let chd_path = backup_dir.join("remote-chd.chd");
    assert!(chd_path.exists(), "single-file CHD written");
    assert!(
        !dest.join(".remote-chd.remote-download.tmp").exists(),
        "the materialize scratch file is cleaned up"
    );

    // Open the produced CHD and round-trip the partition data over it.
    let mut reader = ChdReader::open(&chd_path).expect("open produced CHD");
    let mut sector = [0u8; 512];
    reader.seek(SeekFrom::Start(0)).unwrap();
    reader.read_exact(&mut sector).unwrap();
    let parsed = Mbr::parse(&sector).unwrap();
    let p = parsed
        .entries
        .iter()
        .find(|e| !e.is_empty())
        .expect("partition in the CHD's MBR");
    assert_eq!(p.start_lba, part_lba);
    let mut fs = open_filesystem(reader, p.start_lba as u64 * 512, p.partition_type, None).unwrap();
    let root_e = fs.root().unwrap();
    let blob = fs
        .list_directory(&root_e)
        .unwrap()
        .into_iter()
        .find(|e| e.name == "CHD.BIN")
        .expect("CHD.BIN in the round-tripped CHD");
    let mut got = Vec::new();
    fs.write_file_to(&blob, &mut got).unwrap();
    assert_eq!(
        got, payload,
        "partition data round-trips byte-exact through the remote-materialized CHD"
    );
}

/// Family B binary HANDSHAKE (cb-dos / phase 7a): a JSON-free client connects,
/// sends the raw binary `Hello` (`b"RBK0"` + version + caps, big-endian), and
/// reads back the daemon's binary reply (magic echo + version + caps). Proves
/// the daemon disambiguates a binary Family-B client from a JSON Family-F frame
/// on the same port and round-trips the exact bytes the DJGPP client will send.
/// Also asserts a normal JSON Family-F client still connects on the same daemon.
#[test]
fn family_b_binary_handshake_over_loopback() {
    use std::io::{Read as _, Write as _};
    use std::net::TcpStream;

    use rusty_backup::remote::protocol::{PROTOCOL_VERSION, RB_HELLO_MAGIC, RB_HELLO_MAGIC_BYTES};
    use rusty_backup::remote::RemoteConnection;

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    // A served image, so the JSON-client half of the test has something to open.
    make_fat_image(&root.join("disk.img"), "HSVOL", "H.BIN", 0x33, 1024);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serve_root = root.clone();
    std::thread::spawn(move || {
        let _ = serve_on(listener, serve_root, None);
    });

    // --- binary Family-B client (what cb-dos sends): 8 bytes out, 8 bytes in ---
    let mut sock = TcpStream::connect(&addr).unwrap();
    let mut req = Vec::new();
    req.extend_from_slice(&RB_HELLO_MAGIC_BYTES); // b"RBK0"
    req.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes()); // client version
    req.extend_from_slice(&0u16.to_be_bytes()); // client caps (none yet)
    sock.write_all(&req).unwrap();
    sock.flush().unwrap();

    let mut reply = [0u8; 8];
    sock.read_exact(&mut reply).unwrap();
    assert_eq!(
        &reply[0..4],
        &RB_HELLO_MAGIC_BYTES,
        "daemon echoes the magic so the client can confirm it reached an rb daemon"
    );
    let reply_version = u16::from_be_bytes([reply[4], reply[5]]);
    let reply_caps = u16::from_be_bytes([reply[6], reply[7]]);
    assert_eq!(
        reply_version, PROTOCOL_VERSION,
        "daemon reports its version"
    );
    // The magic round-trips as a u32 too (documents the byte order).
    assert_eq!(
        u32::from_be_bytes([reply[0], reply[1], reply[2], reply[3]]),
        RB_HELLO_MAGIC
    );
    // 7a advertises Family F (the chunk protocol isn't built yet); just assert
    // the daemon answered with a well-formed, non-garbage capability word.
    assert!(reply_caps != 0xFFFF, "capabilities are a real bitfield");

    // --- a JSON Family-F client still connects on the same daemon ---
    let conn = RemoteConnection::connect_shared(&addr).unwrap();
    let devices = conn.lock().unwrap().list_devices();
    assert!(
        devices.is_ok(),
        "JSON Family-F handshake still works alongside the binary one"
    );
}

/// Family B chunk PUT (cb-dos / phase 7b): a JSON-free client does the binary
/// handshake, then streams a native backup folder as members → chunks, and the
/// daemon assembles them into a frozen `.cbk` under the serve root. Emulates the
/// exact bytes `NETPUT.EXE` sends (handshake + `RBKP` PUT header + per-member
/// `RBKM` descriptors + per-chunk `{src_offset,len,crc32}` + stop-and-go acks),
/// then asserts the received container is **byte-identical** to a locally-packed
/// `.cbk` of the same folder and materializes back to the original files.
#[test]
fn family_b_chunk_put_assembles_cbk_over_loopback() {
    use std::io::{Read as _, Write as _};
    use std::net::TcpStream;

    use flate2::write::GzEncoder;
    use flate2::Compression;
    use rusty_backup::rbformats::cbk::{is_cbk, materialize_cbk_to_folder, pack_folder_to_cbk};
    use rusty_backup::remote::protocol::{
        read_put_ack, read_put_result, read_resume_map, write_chunk_header, write_member_header,
        write_put_header, ChunkHeader, CAP_FAMILY_B, PROTOCOL_VERSION, RB_HELLO_MAGIC_BYTES,
    };

    // Build a tiny native-ish backup folder: metadata.json + mbr.bin (raw
    // members the host gzip-wraps) + a real gzip member + its crc32 sidecar.
    let dir = tempfile::tempdir().unwrap();
    let folder = dir.path().join("MYDISK");
    std::fs::create_dir(&folder).unwrap();
    std::fs::write(folder.join("metadata.json"), b"{\n  \"version\": 1\n}\n").unwrap();
    std::fs::write(folder.join("mbr.bin"), vec![0xAAu8; 512]).unwrap();
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(&vec![0u8; 80_000]).unwrap();
    enc.write_all(b"real bytes in the middle of the partition")
        .unwrap();
    enc.write_all(&vec![0u8; 40_000]).unwrap();
    let gz = enc.finish().unwrap();
    std::fs::write(folder.join("partition-0.gz"), &gz).unwrap();
    std::fs::write(folder.join("partition-0.gz.crc32"), b"deadbeef").unwrap();

    // The serve root the daemon writes the assembled .cbk into.
    let root = tempfile::tempdir().unwrap();
    let root = root.path().canonicalize().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serve_root = root.clone();
    std::thread::spawn(move || {
        let _ = serve_on(listener, serve_root, None);
    });

    // --- the bytes NETPUT sends ---
    let mut sock = TcpStream::connect(&addr).unwrap();
    // 1) binary Family-B handshake.
    let mut req = Vec::new();
    req.extend_from_slice(&RB_HELLO_MAGIC_BYTES);
    req.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    req.extend_from_slice(&0u16.to_be_bytes());
    sock.write_all(&req).unwrap();
    sock.flush().unwrap();
    let mut reply = [0u8; 8];
    sock.read_exact(&mut reply).unwrap();
    let reply_caps = u16::from_be_bytes([reply[6], reply[7]]);
    assert!(
        reply_caps & CAP_FAMILY_B != 0,
        "the daemon now advertises Family B (the chunk PUT protocol is built)"
    );

    // 2) the PUT: enumerate the folder and stream each file as one chunk.
    let mut names: Vec<String> = std::fs::read_dir(&folder)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    write_put_header(&mut sock, "MYDISK", 0xDEAD_BEEF, names.len() as u16).unwrap();
    // The daemon replies with the resume map (empty — a fresh transfer).
    let resume = read_resume_map(&mut sock).unwrap();
    assert!(resume.is_empty(), "a fresh PUT has nothing to resume");
    for name in &names {
        let bytes = std::fs::read(folder.join(name)).unwrap();
        let is_gz = name.to_ascii_lowercase().ends_with(".gz");
        // The real producer (CRUSTYBK streaming a disk) sends a partition as
        // several gzip-member span chunks; small raw members ride as one chunk.
        // Split the .gz member into two chunks here to exercise the daemon's
        // multi-chunk concatenation path the same way.
        let chunks: Vec<&[u8]> = if is_gz && bytes.len() > 1 {
            let mid = bytes.len() / 2;
            vec![&bytes[..mid], &bytes[mid..]]
        } else {
            vec![&bytes[..]]
        };
        write_member_header(
            &mut sock,
            if is_gz { 0 } else { 1 },
            name,
            chunks.len() as u32,
        )
        .unwrap();
        let mut off = 0u64;
        for part in &chunks {
            write_chunk_header(
                &mut sock,
                &ChunkHeader {
                    src_offset: off,
                    len: part.len() as u32,
                    crc32: crc32fast::hash(part),
                },
            )
            .unwrap();
            sock.write_all(part).unwrap();
            sock.flush().unwrap();
            assert!(
                read_put_ack(&mut sock).unwrap(),
                "daemon ACKs a {name} chunk"
            );
            off += part.len() as u64;
        }
    }
    // 3) the result frame.
    let (status, cbk_size) = read_put_result(&mut sock).unwrap();
    assert_eq!(status, 0, "PUT succeeded");
    assert!(cbk_size > 0, "the daemon reports the .cbk size");
    drop(sock);

    // --- verify the assembled container ---
    let received = root.join("MYDISK.cbk");
    assert!(received.is_file(), "the daemon wrote MYDISK.cbk under root");
    assert!(is_cbk(&received), "it is a recognizable .cbk container");
    assert_eq!(
        std::fs::metadata(&received).unwrap().len(),
        cbk_size,
        "the reported size matches the file"
    );

    // Byte-identical to a locally-packed .cbk of the same folder (the daemon
    // reuses the frozen pack_folder_to_cbk writer, so the bytes match exactly).
    let local = dir.path().join("local.cbk");
    pack_folder_to_cbk(&folder, &local).unwrap();
    assert_eq!(
        std::fs::read(&received).unwrap(),
        std::fs::read(&local).unwrap(),
        "the PUT-assembled .cbk is byte-identical to a locally-packed one"
    );

    // And it materializes back to the original folder, file-for-file.
    let restored = dir.path().join("restored");
    materialize_cbk_to_folder(&received, &restored).unwrap();
    for name in &names {
        assert_eq!(
            std::fs::read(folder.join(name)).unwrap(),
            std::fs::read(restored.join(name)).unwrap(),
            "member {name} round-trips byte-for-byte through the PUT + .cbk"
        );
    }
}

/// Family B RESUME (cb-dos / phase 7d): a PUT is killed mid-partition, then a
/// reconnect (same name + fingerprint) is told where to resume and finishes the
/// transfer. Proves the daemon's durable journal (fsync-before-record),
/// truncate-to-last-committed, the resume-map reply, and the host filling the
/// partition checksum the rebooted client can't recompute across a resume.
#[test]
fn family_b_chunk_put_resumes_after_drop() {
    use std::io::{Read as _, Write as _};
    use std::net::TcpStream;

    use flate2::write::GzEncoder;
    use flate2::Compression;
    use rusty_backup::rbformats::cbk::materialize_cbk_to_folder;
    use rusty_backup::remote::protocol::{
        read_put_ack, read_put_result, read_resume_map, write_chunk_header, write_member_header,
        write_put_header, ChunkHeader, PROTOCOL_VERSION, RB_HELLO_MAGIC_BYTES,
    };

    const FP: u32 = 0xCAFE_F00D;

    // Do the binary handshake on a fresh socket.
    fn connect_hello(addr: &str) -> TcpStream {
        let mut sock = TcpStream::connect(addr).unwrap();
        let mut req = Vec::new();
        req.extend_from_slice(&RB_HELLO_MAGIC_BYTES);
        req.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
        req.extend_from_slice(&0u16.to_be_bytes());
        sock.write_all(&req).unwrap();
        sock.flush().unwrap();
        let mut reply = [0u8; 8];
        sock.read_exact(&mut reply).unwrap();
        sock
    }
    fn send_gz_chunk(sock: &mut TcpStream, src_offset: u64, payload: &[u8]) {
        write_chunk_header(
            sock,
            &ChunkHeader {
                src_offset,
                len: payload.len() as u32,
                crc32: crc32fast::hash(payload),
            },
        )
        .unwrap();
        sock.write_all(payload).unwrap();
        sock.flush().unwrap();
        assert!(read_put_ack(sock).unwrap(), "daemon ACKs the chunk");
    }
    fn send_raw_member(sock: &mut TcpStream, name: &str, bytes: &[u8]) {
        write_member_header(sock, 1, name, 1).unwrap();
        send_gz_chunk(sock, 0, bytes); // src_offset is ignored for a 1-chunk Raw member
    }

    // The partition is four independent 1 KiB gzip members (the §2c span shape);
    // concatenated they are one valid multi-member partition-0.gz.
    let spans: Vec<Vec<u8>> = (0..4u8)
        .map(|i| {
            let mut e = GzEncoder::new(Vec::new(), Compression::default());
            e.write_all(&vec![b'A' + i; 1000]).unwrap();
            e.finish().unwrap()
        })
        .collect();
    let full_gz: Vec<u8> = spans.iter().flatten().copied().collect();
    let mbr = vec![0x55u8; 512];
    // A realistic metadata.json (cb-dos format) with a placeholder checksum the
    // daemon must overwrite with the real gz CRC at finalize.
    let metadata = r#"{
  "version": 1,
  "created": "2026-06-25T00:00:00Z",
  "source_device": "0x81",
  "source_size_bytes": 17825792,
  "partition_table_type": "MBR",
  "checksum_type": "crc32",
  "compression_type": "gzip",
  "split_size_mib": null,
  "sector_by_sector": false,
  "layout": "per-partition",
  "alignment": { "detected_type": "Modern 1MB boundaries", "first_partition_lba": 2048, "alignment_sectors": 2048, "heads": 64, "sectors_per_track": 32 },
  "partitions": [
    { "index": 0, "type_name": "FAT16 (>32MB)", "partition_type_byte": 6, "start_lba": 2048, "original_size_bytes": 16777216, "imaged_size_bytes": 4000, "compressed_files": ["partition-0.gz"], "checksum": "00000000", "resized": false, "compacted": true, "is_logical": false, "minimum_size_bytes": 4000 }
  ]
}
"#;

    let root_tmp = tempfile::tempdir().unwrap();
    let root = root_tmp.path().canonicalize().unwrap();
    let staging_tmp = tempfile::tempdir().unwrap();
    let staging = staging_tmp.path().to_path_buf();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serve_root = root.clone();
    let serve_staging = staging.clone();
    std::thread::spawn(move || {
        let _ = serve_on(listener, serve_root, Some(serve_staging));
    });

    // --- connection 1: send mbr + the first 2 of 4 partition spans, then drop ---
    {
        let mut sock = connect_hello(&addr);
        write_put_header(&mut sock, "MYDISK", FP, 3).unwrap();
        assert!(
            read_resume_map(&mut sock).unwrap().is_empty(),
            "fresh: nothing to resume"
        );
        send_raw_member(&mut sock, "mbr.bin", &mbr);
        write_member_header(&mut sock, 0, "partition-0.gz", 4).unwrap();
        let mut off = 0u64;
        for span in &spans[..2] {
            send_gz_chunk(&mut sock, off, span);
            off += 1000;
        }
        // Simulate a crash mid-partition: close the socket before chunk 2.
        drop(sock);
    }

    // --- connection 2: reconnect, get told to resume at span 2, finish ---
    let mut sock = connect_hello(&addr);
    write_put_header(&mut sock, "MYDISK", FP, 3).unwrap();
    let resume = read_resume_map(&mut sock).unwrap();
    let part = resume
        .iter()
        .find(|e| e.name == "partition-0.gz")
        .expect("the daemon remembers the in-progress partition");
    assert_eq!(
        part.committed_chunks, 2,
        "two spans were durably committed before the drop"
    );
    // Raw members are re-sent fresh; the partition resumes at the committed span.
    send_raw_member(&mut sock, "mbr.bin", &mbr);
    write_member_header(&mut sock, 0, "partition-0.gz", 4).unwrap();
    for (k, span) in spans
        .iter()
        .enumerate()
        .skip(part.committed_chunks as usize)
    {
        send_gz_chunk(&mut sock, (k as u64) * 1000, span);
    }
    send_raw_member(&mut sock, "metadata.json", metadata.as_bytes());
    let (status, cbk_size) = read_put_result(&mut sock).unwrap();
    assert_eq!(status, 0, "the resumed PUT finished");
    assert!(cbk_size > 0);
    drop(sock);

    // --- verify the assembled container ---
    let received = root.join("MYDISK.cbk");
    assert!(received.is_file(), "the resumed PUT produced MYDISK.cbk");
    let out = root_tmp.path().join("out");
    materialize_cbk_to_folder(&received, &out).unwrap();
    assert_eq!(
        std::fs::read(out.join("partition-0.gz")).unwrap(),
        full_gz,
        "the resumed partition is the full 4-span gz (committed prefix + resumed tail)"
    );
    assert_eq!(std::fs::read(out.join("mbr.bin")).unwrap(), mbr);

    // The daemon filled the real checksum (the rebooted client only sent a
    // placeholder — it can't CRC a member it streamed across two sessions).
    let meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(out.join("metadata.json")).unwrap()).unwrap();
    let got_checksum = meta["partitions"][0]["checksum"].as_str().unwrap();
    assert_eq!(
        got_checksum,
        format!("{:08x}", crc32fast::hash(&full_gz)),
        "metadata checksum was filled with the real partition gz CRC"
    );

    // Staging was cleared on success — a re-backup of the same name starts fresh.
    assert!(
        !staging.join("rb-cbk-MYDISK").exists(),
        "the per-container staging dir is removed after finalize"
    );
}

/// Remote per-partition MINIMUM-SIZE calc over the block tier: drive the
/// `min_size_runner` worker with a `MinSizeSource::Remote { is_device: false }`
/// against a served FAT image and prove it computes an in-place minimum well
/// below the volume size — entirely over ranged reads, no download. This is the
/// runner path the Backup-tab "Calc min" / eager-calc UI uses for a remote
/// source. (The `is_device: true` device path needs root + a real disk, so it's
/// left to interactive hardware verification.)
#[test]
fn remote_min_size_calc_over_block_tier() {
    use rusty_backup::model::min_size_runner::{spawn, MinSizeRequest, MinSizeSource};
    use rusty_backup::remote::RemoteConnection;

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let img = root.join("minsize.img");
    // 8 MiB FAT superfloppy with one small file: the used region (the in-place
    // minimum) is far below the 8 MiB volume size.
    make_fat_image(&img, "MINVOL", "SMALL.BIN", 0x42, 4096);
    let img_len = std::fs::metadata(&img).unwrap().len();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serve_root = root.clone();
    std::thread::spawn(move || {
        let _ = serve_on(listener, serve_root, None);
    });
    let conn = RemoteConnection::connect_shared(&addr).unwrap();

    let status = spawn(MinSizeRequest {
        source: MinSizeSource::Remote {
            conn: Arc::clone(&conn),
            path: "/minsize.img".to_string(),
            is_device: false,
        },
        partition_offset: 0,
        partition_type: 0, // superfloppy auto-detect
        partition_type_string: None,
        partition_size: img_len,
        partition_index: 0,
    });

    // Poll the shared status the GUI would poll, bounded so a hang fails loudly.
    let mut result = None;
    for _ in 0..600 {
        {
            let s = status.lock().unwrap();
            if s.finished {
                assert!(
                    s.error.is_none(),
                    "remote min-size worker errored: {:?}",
                    s.error
                );
                result = Some(s.result);
                break;
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let min = result
        .expect("remote min-size worker finished within the timeout")
        .expect("worker computed an in-place minimum over the wire");
    assert!(
        min > 0 && min < img_len,
        "remote min size {min} should be > 0 and < the {img_len}-byte volume",
    );
}

/// Remote SUPERFLOPPY backup (no partition table) over the block tier: serve a
/// bare FAT image (no MBR/GPT), pull a raw backup of its single offset-0
/// partition, and prove the capture is byte-exact and the metadata records
/// `partition_table_type == "None"`. Proves the `PartitionTable::None` path runs
/// end to end over the wire — the no-table sibling of
/// `run_backup_pulls_remote_image_byte_exact`.
#[test]
fn run_backup_pulls_remote_superfloppy_byte_exact() {
    use std::sync::Mutex;

    use rusty_backup::backup::{
        run_backup_from, BackupConfig, BackupProgress, BackupSource, ChecksumType, CompressionType,
    };

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let img = root.join("floppy.img");
    // A bare FAT superfloppy (no MBR/GPT) with a known multi-cluster file.
    make_fat_image(&img, "SFVOL", "SF.BIN", 0x5A, 9000);
    let disk_final = std::fs::read(&img).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serve_root = root.clone();
    std::thread::spawn(move || {
        let _ = serve_on(listener, serve_root, None);
    });
    let conn = RemoteConnection::connect_shared(&addr).unwrap();

    let dest = root.join("out");
    std::fs::create_dir_all(&dest).unwrap();
    let display = format!("rb://{addr}/floppy.img");
    let config = BackupConfig {
        source_path: std::path::PathBuf::from("/floppy.img"),
        destination_dir: dest.clone(),
        backup_name: "remote-superfloppy".to_string(),
        compression: CompressionType::None,
        checksum: ChecksumType::Crc32,
        split_size_mib: None,
        sector_by_sector: true,
        partition_filter: None,
        chd_options: None,
        size_policy: None,
        partition_target_sizes: None,
        shrink_to_minimum: false,
        precomputed_minimum_sizes: None,
        defrag_partition_indices: None,
        defrag_fat: false,
    };
    let progress = Arc::new(Mutex::new(BackupProgress::default()));
    let source = BackupSource::Remote {
        conn: Arc::clone(&conn),
        path: "/floppy.img".to_string(),
        display: display.clone(),
        is_device: false,
    };
    run_backup_from(source, config, progress).expect("remote superfloppy backup must succeed");

    // Metadata records the no-table layout and the full source size.
    let backup_dir = dest.join("remote-superfloppy");
    let metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(backup_dir.join("metadata.json")).unwrap()).unwrap();
    assert_eq!(
        metadata["partition_table_type"], "None",
        "superfloppy records no partition table"
    );
    assert_eq!(
        metadata["source_size_bytes"].as_u64().unwrap(),
        disk_final.len() as u64,
        "metadata records the full bare-FS image size"
    );

    // The single offset-0 partition captures the whole bare-FS image
    // byte-exact. A superfloppy's raw capture is renamed `.raw` -> `.img` (a
    // universally-mountable flat image), so it lands as `partition-0.img`.
    let captured = std::fs::read(backup_dir.join("partition-0.img"))
        .expect("partition-0.img written by the superfloppy backup");
    assert_eq!(
        captured, disk_final,
        "captured superfloppy is byte-exact vs the source"
    );
}
