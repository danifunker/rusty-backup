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
