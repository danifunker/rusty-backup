use super::*;
use std::io::Cursor;

fn opts_for(kind: FsKind) -> FormatOptions {
    FormatOptions {
        label: kind.default_label().to_string(),
        cpm_preset: (kind == FsKind::Cpm).then(|| "amstrad_data".to_string()),
        ..FormatOptions::default()
    }
}

/// A size in the kind's range that a test can hold in RAM.
fn middle(kind: FsKind) -> u64 {
    let c = kind.caps();
    if c.fixed_size {
        return 800 * KIB;
    }
    (c.min_size.max(4 * MIB)).min(c.max_size).max(c.min_size)
}

fn format_vec(kind: FsKind, size: u64) -> Result<Vec<u8>> {
    let mut cur = Cursor::new(Vec::new());
    let done = format_into(&mut cur, size, kind, &opts_for(kind))?;
    let mut v = cur.into_inner();
    v.resize(done.len as usize, 0);
    Ok(v)
}

fn check_opens(kind: FsKind, bytes: Vec<u8>) {
    let mut fs = crate::fs::open_filesystem(Cursor::new(bytes), 0, 0, None)
        .unwrap_or_else(|e| panic!("{}: does not open: {e:#}", kind.id()));
    if let Some(r) = fs.fsck() {
        let r = r.unwrap_or_else(|e| panic!("{}: fsck failed: {e:#}", kind.id()));
        assert!(
            r.errors.is_empty(),
            "{}: fsck errors {:?}",
            kind.id(),
            r.errors
        );
    }
}

#[test]
fn ids_round_trip_and_are_unique() {
    for k in FsKind::ALL {
        assert_eq!(FsKind::from_id(k.id()), Some(k));
    }
    let mut ids: Vec<_> = FsKind::ALL.iter().map(|k| k.id()).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), FsKind::ALL.len());
}

#[test]
fn every_kind_formats_at_its_minimum_and_opens() {
    let mut bad = Vec::new();
    for k in FsKind::ALL {
        let c = k.caps();
        let size = if c.fixed_size { 800 * KIB } else { c.min_size };
        let r = std::panic::catch_unwind(|| {
            let bytes = format_vec(k, size).unwrap_or_else(|e| panic!("{e:#}"));
            check_opens(k, bytes);
        });
        if r.is_err() {
            bad.push(format!("{} at {size}", k.id()));
        }
    }
    assert!(bad.is_empty(), "failed at the claimed minimum: {bad:?}");
}

#[test]
fn every_kind_formats_at_a_middle_size_and_opens() {
    for k in FsKind::ALL {
        let size = middle(k);
        let bytes = format_vec(k, size).unwrap_or_else(|e| panic!("{} at {size}: {e:#}", k.id()));
        check_opens(k, bytes);
    }
}

#[test]
fn fixed_geometry_kinds_report_their_size() {
    for k in FsKind::ALL {
        let c = k.caps();
        // A size of 0 means a preset or geometry picks it.
        if c.fixed_size && c.min_size > 0 {
            let bytes = format_vec(k, 800 * KIB).unwrap();
            assert_eq!(bytes.len() as u64, c.min_size, "{}", k.id());
        }
    }
}

#[test]
fn options_that_do_not_apply_are_refused() {
    let o = FormatOptions {
        inodes: Some(10),
        ..FormatOptions::default()
    };
    assert!(check_options(FsKind::Ntfs, &o).is_err());
    assert!(check_options(FsKind::Efs, &o).is_ok());
    let o = FormatOptions {
        bytes_per_inode: Some(8192),
        ..FormatOptions::default()
    };
    assert!(check_options(FsKind::Ufs, &o).is_ok());
    assert!(check_options(FsKind::UfsSun, &o).is_ok());
    assert!(check_options(FsKind::Fat, &o).is_err());
    assert!(check_options(FsKind::Cpm, &FormatOptions::default()).is_err());
}

#[test]
fn window_formats_inside_a_larger_disk() {
    let base = 1 << 20;
    let mut disk = Cursor::new(vec![0u8; 12 << 20]);
    let mut w = Window::new(&mut disk, base, 8 << 20);
    let done = format_into(&mut w, 8 << 20, FsKind::Fat, &opts_for(FsKind::Fat)).unwrap();
    let bytes = disk.into_inner();
    assert!(bytes[..base as usize].iter().all(|&b| b == 0));
    let vol = bytes[base as usize..(base + done.len) as usize].to_vec();
    check_opens(FsKind::Fat, vol);
}

#[test]
fn window_refuses_writes_past_its_end() {
    let mut disk = Cursor::new(vec![0u8; 4096]);
    let mut w = Window::new(&mut disk, 512, 1024);
    w.seek(SeekFrom::Start(1000)).unwrap();
    assert_eq!(w.write(&[1u8; 100]).unwrap(), 24);
    assert!(w.write(&[1u8; 1]).is_err());
    assert!(w.seek(SeekFrom::Current(-2000)).is_err());
}

#[test]
fn os9_names_are_sanitized() {
    assert_eq!(sanitize_os9_volume_name("rusty-backup"), "rustybackup");
    assert_eq!(sanitize_os9_volume_name("9lives"), "OS99lives");
}

/// Each kind at the largest size Build Disk offers, capped (UFS writes its inode tables, which is slow on a hard disk).
#[test]
fn every_kind_formats_at_its_largest_buildable_size() {
    let dir = tempfile::tempdir().unwrap();
    let mut bad = Vec::new();
    for k in FsKind::ALL {
        let c = k.caps();
        if c.fixed_size {
            continue;
        }
        // Windows files are not sparse by default, so a CI runner would reserve all of it.
        let cap = match k {
            _ if cfg!(windows) => GIB,
            FsKind::Ufs | FsKind::Ufs43 | FsKind::UfsSun => GIB,
            _ => 4 * GIB,
        };
        let size = c.max_buildable().min(cap);
        let path = dir.path().join(format!("{}.img", k.id()));
        let r = format_to_file(&path, size, k, &opts_for(k));
        match r {
            Ok(d) if d.len <= size => {
                let f = std::fs::File::open(&path).unwrap();
                if let Err(e) = crate::fs::open_filesystem(f, 0, 0, None) {
                    bad.push(format!("{} at {size}: does not open: {e:#}", k.id()));
                }
            }
            Ok(d) => bad.push(format!("{} at {size}: length {}", k.id(), d.len)),
            Err(e) => bad.push(format!("{} at {size}: {e:#}", k.id())),
        }
        let _ = std::fs::remove_file(&path);
    }
    assert!(bad.is_empty(), "failed at the claimed maximum: {bad:#?}");
}

#[test]
fn every_geometry_formats_and_opens() {
    for k in FsKind::ALL {
        for (id, _) in geometries(k) {
            let opts = FormatOptions {
                geometry: Some(id.to_string()),
                ..opts_for(k)
            };
            let mut cur = Cursor::new(Vec::new());
            let done = format_into(&mut cur, 0, k, &opts)
                .unwrap_or_else(|e| panic!("{} {id}: {e:#}", k.id()));
            let mut v = cur.into_inner();
            v.resize(done.len as usize, 0);
            let r = std::panic::catch_unwind(|| check_opens(k, v));
            assert!(r.is_ok(), "{} {id} fails its check", k.id());
        }
    }
}

/// The streaming rule: only a true floppy format may build in memory.
#[test]
fn only_floppy_sized_formats_build_in_memory() {
    for k in FsKind::ALL {
        let c = k.caps();
        if c.streams {
            continue;
        }
        let mut largest = c.max_size.max(c.min_size);
        for (id, _) in geometries(k) {
            let opts = FormatOptions {
                geometry: Some(id.to_string()),
                ..opts_for(k)
            };
            let mut cur = Cursor::new(Vec::new());
            largest = largest.max(format_into(&mut cur, 0, k, &opts).unwrap().len);
        }
        assert!(
            largest <= IN_RAM_LIMIT,
            "{} builds {largest} bytes in RAM",
            k.id()
        );
    }
}
