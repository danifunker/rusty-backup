//! Fork-preserving single-file export: MacBinary (.bin) and AppleDouble
//! (`._name` sidecar) output from an HFS file carrying a resource fork. Guards
//! `fs::fork_export::export_file_with_fork`, which Commander's copy/export-to-
//! host now uses.

use rusty_backup::cli::resolve::resolve_partition_ro;
use rusty_backup::fs::filesystem::{CreateFileOptions, ResourceForkSource};
use rusty_backup::fs::fork_export::export_file_with_fork;
use rusty_backup::fs::resource_fork::ResourceForkMode;
use rusty_backup::fs::{hfs, open_editable_filesystem, open_filesystem};

/// Build a bare classic-HFS image with one file that has both forks.
fn make_hfs_with_fork(path: &std::path::Path, name: &str, data: &[u8], rsrc: &[u8]) {
    std::fs::write(
        path,
        hfs::create_blank_hfs(4 * 1024 * 1024, 512, "MACVOL").unwrap(),
    )
    .unwrap();
    let (file, ctx, commit) = rusty_backup::cli::resolve::resolve_partition_rw(path, None).unwrap();
    let mut efs =
        open_editable_filesystem(file, ctx.offset, ctx.type_byte, ctx.type_string.as_deref())
            .unwrap();
    let parent = efs.root().unwrap();
    let opts = CreateFileOptions {
        resource_fork: Some(ResourceForkSource::Data(rsrc.to_vec())),
        ..Default::default()
    };
    let mut d = data;
    efs.create_file(&parent, name, &mut d, data.len() as u64, &opts)
        .unwrap();
    efs.sync_metadata().unwrap();
    drop(efs);
    commit.commit().unwrap();
}

#[test]
fn export_macbinary_and_appledouble_preserve_the_resource_fork() {
    let dir = tempfile::tempdir().unwrap();
    let img = dir.path().join("mac.img");
    let data = b"data fork contents for export";
    let rsrc = b"the resource fork payload to preserve";
    make_hfs_with_fork(&img, "FORKED.BIN", data, rsrc);

    let (file, ctx) = resolve_partition_ro(&img, None).unwrap();
    let mut fs =
        open_filesystem(file, ctx.offset, ctx.type_byte, ctx.type_string.as_deref()).unwrap();
    let root = fs.root().unwrap();
    let forked = fs
        .list_directory(&root)
        .unwrap()
        .into_iter()
        .find(|e| e.name.eq_ignore_ascii_case("FORKED.BIN"))
        .expect("forked file present");
    assert_eq!(forked.resource_fork_size, Some(rsrc.len() as u64));

    let out = tempfile::tempdir().unwrap();

    // MacBinary: one self-contained .bin holding both forks (rsrc verbatim).
    let name = rusty_backup::fs::fork_export::safe_name(&forked);
    export_file_with_fork(
        fs.as_mut(),
        &forked,
        out.path(),
        &name,
        ResourceForkMode::MacBinary,
    )
    .unwrap();
    let bin = std::fs::read(out.path().join("FORKED.BIN.bin")).unwrap();
    assert!(
        bin.len() >= 128 + data.len() + rsrc.len(),
        "MacBinary = 128B header + both forks"
    );
    assert!(
        bin.windows(rsrc.len()).any(|w| w == rsrc),
        "resource fork bytes present in the MacBinary"
    );

    // AppleDouble: data fork under the plain name + a `._name` rsrc sidecar.
    export_file_with_fork(
        fs.as_mut(),
        &forked,
        out.path(),
        &name,
        ResourceForkMode::AppleDouble,
    )
    .unwrap();
    assert_eq!(
        std::fs::read(out.path().join("FORKED.BIN")).unwrap(),
        data,
        "AppleDouble writes the data fork under the plain name"
    );
    let ad = std::fs::read(out.path().join("._FORKED.BIN")).unwrap();
    assert!(
        ad.windows(rsrc.len()).any(|w| w == rsrc),
        "resource fork bytes present in the AppleDouble sidecar"
    );
}
