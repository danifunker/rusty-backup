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
