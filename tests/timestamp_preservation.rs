//! Round-trip regression tests for file-mtime preservation.
//!
//! The rule the code enforces:
//!
//! - A **genuinely new** file (rb-cli `put` from stdin, GUI "new blank file")
//!   gets `now` — every driver did this before, and still does when
//!   `CreateFileOptions.unix_times` is `None`.
//! - A **copy / import / extract** preserves the source date end-to-end:
//!   * host file → image (`dir_import`) — host stat mtime lands on disk
//!   * tar entry → image (`tar_import`) — tar Header mtime lands on disk
//!   * image → tar (`tar_export`) — the on-disk mtime lands in the tar Header
//!   * image → host (`fork_export`) — the on-disk mtime lands on the host file
//!
//! One end-to-end test per stage; one "genuinely new stamps now" test per
//! kind so a future refactor can't silently regress the distinction.

use rusty_backup::fs::dir_import::{import_dir, DirImportOptions};
use rusty_backup::fs::efs::{create_blank_efs, EfsFilesystem};
use rusty_backup::fs::entry::FileEntry;
use rusty_backup::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
use rusty_backup::fs::fork_export::export_file_with_fork;
use rusty_backup::fs::resource_fork::ResourceForkMode;
use rusty_backup::fs::tar_export::{export_tar, TarCompression, TarExportOptions};
use rusty_backup::fs::tar_import::{import_tar_into, TarImportOptions};
use rusty_backup::fs::times::UnixTimes;
use std::io::Cursor;

const YEAR_2020: u64 = 1_577_836_800; // 2020-01-01 00:00:00 UTC
const YEAR_2018: u64 = 1_514_764_800; // 2018-01-01 00:00:00 UTC

fn fresh_efs() -> EfsFilesystem<Cursor<Vec<u8>>> {
    let img = create_blank_efs(1024 * 1024, "rb-efs").expect("format 1 MiB EFS");
    EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS")
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn put_bytes(
    fs: &mut EfsFilesystem<Cursor<Vec<u8>>>,
    name: &str,
    data: &[u8],
    times: Option<UnixTimes>,
) -> FileEntry {
    let root = fs.root().unwrap();
    fs.create_file(
        &root,
        name,
        &mut &data[..],
        data.len() as u64,
        &CreateFileOptions {
            unix_times: times,
            ..Default::default()
        },
    )
    .expect("create_file")
}

/// dir_import from a host file with a known mtime lands that mtime on the
/// image inode — the "put --from-dir preserves the source date" contract.
#[test]
fn dir_import_preserves_host_mtime() {
    let tmp = tempfile::tempdir().unwrap();
    let host = tmp.path().join("aged.txt");
    std::fs::write(&host, b"aged content").unwrap();
    // Backdate the host file to a known point in 2018.
    let ft = filetime::FileTime::from_unix_time(YEAR_2018 as i64, 0);
    filetime::set_file_times(&host, ft, ft).unwrap();

    let mut fs = fresh_efs();
    let dest = fs.root().unwrap();
    let stats = import_dir(
        &mut fs,
        &dest,
        tmp.path(),
        &DirImportOptions::default(),
        &|_| {},
    )
    .expect("import_dir");
    assert_eq!(stats.files, 1);

    let listed = fs
        .list_directory(&dest)
        .unwrap()
        .into_iter()
        .find(|e| e.name == "aged.txt")
        .expect("host file imported");
    assert_eq!(
        listed.modified_unix,
        Some(YEAR_2018),
        "the host's mtime must land on the image inode verbatim"
    );
}

/// tar_import from an archive whose entry has a 2020 mtime lands that mtime
/// on the image inode. Same shape as `dir_import` — the mtime source is the
/// tar Header instead of `stat`.
#[test]
fn tar_import_preserves_archive_mtime() {
    // Build a tarball in memory: one file "src.txt" with mtime = 2020-01-01.
    let mut archive_bytes = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut archive_bytes);
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        header.set_uid(0);
        header.set_gid(0);
        header.set_size(3);
        header.set_mtime(YEAR_2020);
        header.set_entry_type(tar::EntryType::Regular);
        header.set_cksum();
        builder
            .append_data(&mut header, "src.txt", &b"abc"[..])
            .unwrap();
        builder.finish().unwrap();
    }

    let mut fs = fresh_efs();
    let dest = fs.root().unwrap();
    let stats = import_tar_into(
        &mut fs,
        &dest,
        Cursor::new(archive_bytes),
        &TarImportOptions::default(),
        &|_| {},
    )
    .expect("import_tar_into");
    assert_eq!(stats.files, 1);

    let listed = fs
        .list_directory(&dest)
        .unwrap()
        .into_iter()
        .find(|e| e.name == "src.txt")
        .expect("tar file imported");
    assert_eq!(
        listed.modified_unix,
        Some(YEAR_2020),
        "the tar Header's mtime must land on the image inode verbatim"
    );
}

/// The other direction: a file created with a preserved mtime, then
/// exported through `tar_export`, must show that mtime in the tar Header
/// (was hard-coded to 0 before this change).
#[test]
fn tar_export_carries_source_mtime() {
    let mut fs = fresh_efs();
    put_bytes(
        &mut fs,
        "dated.txt",
        b"payload",
        Some(UnixTimes::all(YEAR_2020)),
    );

    let mut tar_bytes = Vec::new();
    let root = fs.root().unwrap();
    export_tar(
        &mut fs,
        &root,
        "",
        &mut tar_bytes,
        TarCompression::None,
        &TarExportOptions::default(),
        &|_| {},
    )
    .expect("export_tar");

    // Read the tar back and confirm the file entry carries our mtime.
    let mut ar = tar::Archive::new(&tar_bytes[..]);
    let entry = ar
        .entries()
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| {
            e.path()
                .map(|p| p.file_name().and_then(|n| n.to_str()) == Some("dated.txt"))
                .unwrap_or(false)
        })
        .expect("tar contains dated.txt");
    let mtime = entry.header().mtime().expect("mtime present");
    assert_eq!(
        mtime, YEAR_2020,
        "tar_export must carry the source's modified_unix as the entry's mtime"
    );
}

/// The full round-trip: a host file dated 2018 goes into an EFS image via
/// dir_import, then out again via tar_export — the tar entry carries the
/// original date. This is the whole point of the change: source-of-truth
/// mtime survives every hop.
#[test]
fn host_to_image_to_tar_preserves_mtime_end_to_end() {
    let tmp = tempfile::tempdir().unwrap();
    let host = tmp.path().join("old.txt");
    std::fs::write(&host, b"end-to-end").unwrap();
    let ft = filetime::FileTime::from_unix_time(YEAR_2018 as i64, 0);
    filetime::set_file_times(&host, ft, ft).unwrap();

    let mut fs = fresh_efs();
    let dest = fs.root().unwrap();
    import_dir(
        &mut fs,
        &dest,
        tmp.path(),
        &DirImportOptions::default(),
        &|_| {},
    )
    .expect("import_dir");

    let mut tar_bytes = Vec::new();
    let root = fs.root().unwrap();
    export_tar(
        &mut fs,
        &root,
        "",
        &mut tar_bytes,
        TarCompression::None,
        &TarExportOptions::default(),
        &|_| {},
    )
    .expect("export_tar");

    let mut ar = tar::Archive::new(&tar_bytes[..]);
    let entry = ar
        .entries()
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| {
            e.path()
                .map(|p| p.file_name().and_then(|n| n.to_str()) == Some("old.txt"))
                .unwrap_or(false)
        })
        .expect("tar contains old.txt");
    assert_eq!(
        entry.header().mtime().unwrap(),
        YEAR_2018,
        "host mtime must survive host->efs->tar unchanged"
    );
}

/// The extract half of the round-trip: file with a 2020 mtime on the image
/// is extracted to a host folder — the extracted file carries the same
/// mtime, via filetime::set_file_times.
#[test]
fn image_to_host_extract_preserves_mtime() {
    let mut fs = fresh_efs();
    let root = fs.root().unwrap();
    let entry = put_bytes(
        &mut fs,
        "dated.bin",
        b"host-out",
        Some(UnixTimes::all(YEAR_2020)),
    );

    let tmp = tempfile::tempdir().unwrap();
    // Re-read entry with `list_directory` so `modified_unix` is set the way
    // an extract-in-production reads it (the create_file return path leaves
    // it None, which is fine — real callers list first).
    let listed = fs
        .list_directory(&root)
        .unwrap()
        .into_iter()
        .find(|e| e.name == "dated.bin")
        .unwrap();
    let _ = entry;
    export_file_with_fork(
        &mut fs,
        &listed,
        tmp.path(),
        "dated.bin",
        ResourceForkMode::DataForkOnly,
    )
    .expect("extract");

    let host = tmp.path().join("dated.bin");
    let meta = std::fs::metadata(&host).unwrap();
    let host_mtime = meta
        .modified()
        .unwrap()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert_eq!(
        host_mtime, YEAR_2020,
        "extracted host file must carry the source's modified_unix"
    );
}

/// The other side of the rule: a genuinely new file (unix_times = None)
/// still gets `now`. The distinction between "new" and "copied" must not
/// silently regress into "always preserve" (which would zero-mtime every
/// blank file GUI users create).
#[test]
fn genuinely_new_file_stamps_now() {
    let mut fs = fresh_efs();
    let before = now_secs();
    let entry = put_bytes(&mut fs, "fresh.txt", b"hello", None);
    let after = now_secs();
    let root = fs.root().unwrap();
    let listed = fs
        .list_directory(&root)
        .unwrap()
        .into_iter()
        .find(|e| e.name == "fresh.txt")
        .unwrap();
    let mtime = listed
        .modified_unix
        .expect("modified_unix must be set for a genuinely new file");
    assert!(
        (before..=after + 5).contains(&mtime),
        "genuinely new file mtime must be ~now (before={before}, mtime={mtime}, after={after})"
    );
    let _ = entry;
}

// ---------------------------------------------------------------------------
// Cross-filesystem coverage — every driver that Phase 2..5 taught to honour
// `CreateFileOptions.unix_times` gets one test that puts a 2020-dated byte
// stream through `create_file` and reads the mtime back via `list_directory`.
// The point is not to re-verify each format encoder (those have unit tests in
// `fs::times`) — it's the plumbing between `create_file` and `list_directory`
// that's easy to let drift; one test per driver catches a driver forgetting
// to thread `options.unix_times` through, or forgetting to populate
// `modified_unix` on read.
// ---------------------------------------------------------------------------

use rusty_backup::fs::adfs::{create_blank_adfs, AdfsFilesystem};
use rusty_backup::fs::exfat::{create_blank_exfat, ExfatFilesystem, ExfatFormatTemplate};
use rusty_backup::fs::fat::{create_blank_fat, FatFilesystem};
use rusty_backup::fs::hfs::{create_blank_hfs, HfsFilesystem};
use rusty_backup::fs::hfsplus::{create_blank_hfsplus, HfsPlusFilesystem};
use rusty_backup::fs::hpfs::{create_blank_hpfs, HpfsFilesystem};
use rusty_backup::fs::mfs::{create_blank_mfs, MfsFilesystem};
use rusty_backup::fs::ntfs::NtfsFilesystem;
use rusty_backup::fs::ntfs_format::create_blank_ntfs;
use rusty_backup::fs::os9::{create_blank_os9, Os9Filesystem};
use rusty_backup::fs::prodos::{create_blank_prodos, ProDosFilesystem};
use rusty_backup::fs::ucsd::{create_blank_ucsd, UcsdFilesystem};

/// One-shot helper: create a file on `fs` at `/name` with mtime = YEAR_2020,
/// re-list the root, and return the entry's `modified_unix`.
fn put_and_readback_mtime<F: EditableFilesystem>(fs: &mut F, name: &str) -> Option<u64> {
    let root = fs.root().unwrap();
    fs.create_file(
        &root,
        name,
        &mut &b"hello"[..],
        5,
        &CreateFileOptions {
            unix_times: Some(UnixTimes::mtime_only(YEAR_2020)),
            ..Default::default()
        },
    )
    .expect("create_file");
    fs.sync_metadata().expect("sync_metadata");
    let root = fs.root().unwrap();
    fs.list_directory(&root)
        .unwrap()
        .into_iter()
        .find(|e| e.name == name)
        .expect("listed entry")
        .modified_unix
}

/// The DOS-packed date is 2-second granular; a round-trip loses the odd
/// low bit but the whole-minute value must survive.
fn assert_within_dos_granularity(actual: Option<u64>, expected: u64) {
    let a = actual.expect("modified_unix must be populated");
    assert!(
        a >= expected && a - expected <= 1,
        "expected ~{expected}, got {a} (DOS is 2-second granular)"
    );
}

#[test]
fn fat_preserves_mtime_across_create_and_list() {
    let img = create_blank_fat(4 * 1024 * 1024, Some("TIMES")).unwrap();
    let mut fs = FatFilesystem::open(Cursor::new(img), 0).unwrap();
    assert_within_dos_granularity(put_and_readback_mtime(&mut fs, "TIMES.TXT"), YEAR_2020);
}

#[test]
fn exfat_preserves_mtime_across_create_and_list() {
    let template = ExfatFormatTemplate {
        bytes_per_sector: 512,
        sectors_per_cluster: 8,
        label: Some("TIMES".to_string()),
    };
    let size = 16 * 1024 * 1024u64;
    let mut cur = Cursor::new(Vec::<u8>::new());
    create_blank_exfat(&mut cur, &template, size).unwrap();
    let mut fs = ExfatFilesystem::open(cur, 0).unwrap();
    assert_within_dos_granularity(put_and_readback_mtime(&mut fs, "times.txt"), YEAR_2020);
}

#[test]
fn ntfs_preserves_mtime_across_create_and_list() {
    let mut cur = Cursor::new(Vec::<u8>::new());
    create_blank_ntfs(&mut cur, 16 * 1024 * 1024, 128, Some("TIMES")).unwrap();
    let mut fs = NtfsFilesystem::open(cur, 0).unwrap();
    let mtime = put_and_readback_mtime(&mut fs, "times.txt").expect("modified_unix set");
    assert_eq!(mtime, YEAR_2020, "NTFS FILETIME is second-granular");
}

#[test]
fn hfs_preserves_mtime_across_create_and_list() {
    let img = create_blank_hfs(4 * 1024 * 1024, 512, "TIMES").unwrap();
    let mut fs = HfsFilesystem::open(Cursor::new(img), 0).unwrap();
    let mtime = put_and_readback_mtime(&mut fs, "times.txt").expect("modified_unix set");
    assert_eq!(mtime, YEAR_2020);
}

#[test]
fn hfsplus_preserves_mtime_across_create_and_list() {
    let img = create_blank_hfsplus(16 * 1024 * 1024, 4096, "TIMES", false);
    let mut fs = HfsPlusFilesystem::open(Cursor::new(img), 0).unwrap();
    let mtime = put_and_readback_mtime(&mut fs, "times.txt").expect("modified_unix set");
    assert_eq!(mtime, YEAR_2020);
}

#[test]
fn mfs_preserves_mtime_across_create_and_list() {
    let img = create_blank_mfs(400 * 1024, "TIMES").unwrap();
    let mut fs = MfsFilesystem::open(Cursor::new(img), 0).unwrap();
    let mtime = put_and_readback_mtime(&mut fs, "times.txt").expect("modified_unix set");
    assert_eq!(mtime, YEAR_2020);
}

#[test]
fn prodos_preserves_mtime_across_create_and_list() {
    let img = create_blank_prodos(400 * 1024, "TIMES").unwrap();
    let mut fs = ProDosFilesystem::open(Cursor::new(img), 0).unwrap();
    let mtime = put_and_readback_mtime(&mut fs, "TIMES.TXT").expect("modified_unix set");
    // ProDOS is minute-granular; YEAR_2020 lands on a minute boundary.
    assert_eq!(mtime, YEAR_2020);
}

#[test]
fn hpfs_preserves_mtime_across_create_and_list() {
    let img = create_blank_hpfs(4 * 1024 * 1024, "TIMES").unwrap();
    let mut fs = HpfsFilesystem::open(Cursor::new(img), 0).unwrap();
    let mtime = put_and_readback_mtime(&mut fs, "times.txt").expect("modified_unix set");
    assert_eq!(mtime, YEAR_2020);
}

// Human68k needs a captured `Human68kFormatTemplate` (BPB + reserved region)
// to format, which needs a source volume to hand — the driver's own unit
// tests cover the roundtrip. Extending it into this file would duplicate
// that scaffolding.

#[test]
fn os9_preserves_mtime_across_create_and_list() {
    let img = create_blank_os9("TIMES").unwrap();
    let mut fs = Os9Filesystem::open(Cursor::new(img), 0).unwrap();
    let mtime = put_and_readback_mtime(&mut fs, "times").expect("modified_unix set");
    // OS-9 FD.DAT is minute-granular; YEAR_2020 lands on a minute boundary.
    assert_eq!(mtime, YEAR_2020);
}

#[test]
fn ucsd_preserves_mtime_across_create_and_list() {
    // UCSD's year field is 0..99 -> 1900..1999, so 2020 would clamp to 1999.
    // Test with a UCSD-representable year instead.
    const YEAR_1990: u64 = 631_152_000; // 1990-01-01 00:00:00 UTC
    let img = create_blank_ucsd(400 * 1024, "TIMES").unwrap();
    let mut fs = UcsdFilesystem::open(Cursor::new(img), 0).unwrap();
    let root = fs.root().unwrap();
    fs.create_file(
        &root,
        "TIMES",
        &mut &b"hello"[..],
        5,
        &CreateFileOptions {
            unix_times: Some(UnixTimes::mtime_only(YEAR_1990)),
            ..Default::default()
        },
    )
    .expect("create_file");
    fs.sync_metadata().expect("sync");
    let root = fs.root().unwrap();
    let mtime = fs
        .list_directory(&root)
        .unwrap()
        .into_iter()
        .find(|e| e.name == "TIMES")
        .expect("listed entry")
        .modified_unix
        .expect("modified_unix set");
    // UCSD stores day-granularity only, so 1990-01-01 00:00:00 round-trips exact.
    assert_eq!(mtime, YEAR_1990);
}

#[test]
fn adfs_preserves_mtime_across_create_and_list() {
    let img = create_blank_adfs("TIMES");
    let mut fs = AdfsFilesystem::open(Cursor::new(img), 0).unwrap();
    let mtime = put_and_readback_mtime(&mut fs, "times").expect("modified_unix set");
    // ADFS is centisecond-granular; whole seconds round-trip exactly.
    assert_eq!(mtime, YEAR_2020);
}
