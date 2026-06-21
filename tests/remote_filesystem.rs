//! End-to-end test for the Family F read path: a `RemoteFilesystem` browsing
//! and reading a real FAT image over a loopback `rb-cli serve` daemon.
//!
//! Binds a port-0 listener *before* spawning the daemon thread, so the connect
//! lands in the OS listen backlog with no sleep/race, then drives the adapter.
#![cfg(feature = "remote")]

use std::net::TcpListener;

use rusty_backup::cli::resolve::resolve_partition_rw;
use rusty_backup::fs::filesystem::{CreateFileOptions, Filesystem};
use rusty_backup::fs::{fat, open_editable_filesystem};
use rusty_backup::remote::{serve_on, RemoteFilesystem, RemoteHostFilesystem};

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
