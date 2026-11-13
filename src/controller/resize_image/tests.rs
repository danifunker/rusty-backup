use super::*;
use crate::controller::inspect::{
    InspectAction, InspectController, InspectServices, InspectSource, JobRegistry,
};
use crate::model::job::NoNotify;
use crate::partition::type_catalog::TableKind;

use ResizeImageAction as A;

/// Inspect on `path`, as the shells open it, then the wizard on what Inspect hands over.
fn wizard(path: &Path) -> ResizeImageController {
    let mut inspect = InspectController::new(InspectServices {
        jobs: JobRegistry::new(),
        notify: Arc::new(NoNotify),
    });
    inspect.apply(InspectAction::Open(InspectSource::Image {
        path: path.to_path_buf(),
        temp: None,
    }));
    while inspect.loading().is_some() {
        std::thread::sleep(std::time::Duration::from_millis(2));
        inspect.tick();
    }
    let source = inspect
        .resize_image_source()
        .expect("an image file with a table");
    let mut c = ResizeImageController::new(Arc::new(NoNotify));
    c.apply(A::Open(Box::new(source)));
    c.wait_idle();
    c
}

fn sizes(path: &Path) -> Vec<u64> {
    let t = crate::partition::PartitionTable::detect(&mut std::fs::File::open(path).unwrap());
    t.unwrap()
        .partitions()
        .iter()
        .map(|p| p.size_bytes)
        .collect()
}

#[test]
fn the_wizard_grows_a_copy_and_leaves_free_space() {
    let dir = tempfile::tempdir().unwrap();
    let src = crate::controller::test_fixtures::fat_disk(dir.path(), TableKind::Mbr, 64, &[16, 16]);
    let mut c = wizard(&src);
    assert_eq!(c.page(), Page::Size);
    assert_eq!(c.disk_text(), "64M");
    assert_eq!(
        c.rows()[1].title,
        "Partition 2 of 2: Partition 2 (FAT32 (LBA))"
    );
    c.apply(A::SetDiskSize("96M".into()));
    assert_eq!(c.disk_change(), "Grows by 32.0 MiB");
    c.apply(A::Next);
    assert_eq!(c.page(), Page::Check);
    c.wait_idle();
    assert!(c.checks().iter().all(|r| r.verdict == Some(Verdict::Clean)));
    c.apply(A::Next);
    assert_eq!(c.page(), Page::Partitions);
    c.apply(A::SetSizeText {
        index: 1,
        text: "40".into(),
    });
    assert_eq!(c.rows()[1].mode, PartMode::Custom);
    assert_eq!(c.free_at_end(), Some(39 * MIB));
    c.apply(A::Next);
    c.wait_idle();
    assert_eq!(c.page(), Page::Review, "{:?}", c.plan_error());
    let review = c.review().join("\n");
    assert!(review.contains("Copy the image to "), "{review}");
    assert!(review.contains("Disk: 64.0 MiB -> 96.0 MiB"), "{review}");
    assert!(
        review.contains("Free space left at the end: 39.0 MiB"),
        "{review}"
    );
    assert!(
        c.apply(A::Start).is_empty(),
        "a copy starts without a question"
    );
    assert_eq!(c.page(), Page::Work);
    c.wait_idle();
    assert_eq!(c.page(), Page::Done);
    assert_eq!(c.outcome(), Some(&Ok(())));
    assert!(c
        .steps()
        .iter()
        .all(|(_, s)| matches!(s, StepState::Done(_))));
    let out = dir.path().join("disk-resized.img");
    assert_eq!(sizes(&out), [16 * MIB, 40 * MIB]);
    assert_eq!(std::fs::metadata(&out).unwrap().len(), 96 * MIB);
    assert_eq!(
        sizes(&src),
        [16 * MIB, 16 * MIB],
        "the original is untouched"
    );
    assert!(!c.take_disk_changed());
    c.apply(A::OpenResult);
    assert_eq!(c.take_open_request(), Some(out));
}

#[test]
fn fill_gives_one_partition_the_space_the_others_leave() {
    let dir = tempfile::tempdir().unwrap();
    let src = crate::controller::test_fixtures::fat_disk(dir.path(), TableKind::Mbr, 64, &[16, 16]);
    let mut c = wizard(&src);
    c.apply(A::SetDiskSize("96M".into()));
    c.apply(A::SetMode {
        index: 0,
        mode: PartMode::Fill,
    });
    c.apply(A::SetMode {
        index: 1,
        mode: PartMode::Fill,
    });
    assert_eq!(c.rows()[0].mode, PartMode::Keep, "one Fill row at a time");
    // 96 MiB less the 1 MiB head and partition 1's 16 MiB.
    assert_eq!(c.final_sizes().unwrap(), [(0, 16 * MIB), (1, 79 * MIB)]);
    assert_eq!(c.free_at_end(), Some(0));
    let (before, after) = c.bars();
    assert_eq!(before.last().unwrap().kind, SegmentKind::Free);
    assert_eq!(after.len(), 2, "no free space left");
}

#[test]
fn a_layout_that_does_not_fit_or_goes_below_the_minimum_is_named() {
    let dir = tempfile::tempdir().unwrap();
    let src = crate::controller::test_fixtures::fat_disk(dir.path(), TableKind::Gpt, 64, &[16, 16]);
    let mut c = wizard(&src);
    c.apply(A::SetDiskSize("20M".into()));
    let err = c.live_error().unwrap();
    assert!(err.starts_with("The partitions need 33.0 MiB"), "{err}");
    c.apply(A::SetDiskSize("64M".into()));
    assert_eq!(c.live_error(), None);
    let min = c.rows()[1].minimum.expect("the probe read a minimum");
    c.apply(A::SetSizeText {
        index: 1,
        text: stamp_mib(min - MIB),
    });
    assert!(c.live_error().unwrap().contains("can shrink to"));
}

#[test]
fn in_place_asks_first_and_reports_the_disk_changed() {
    let dir = tempfile::tempdir().unwrap();
    let src = crate::controller::test_fixtures::fat_disk(dir.path(), TableKind::Mbr, 64, &[16, 16]);
    let mut c = wizard(&src);
    c.apply(A::SetDiskSize("80M".into()));
    c.apply(A::SetCheck(false));
    c.apply(A::Next);
    assert_eq!(c.page(), Page::Check);
    assert!(c.checks().is_empty(), "no check was started");
    c.apply(A::Next);
    c.apply(A::Next);
    c.wait_idle();
    assert_eq!(c.page(), Page::Review, "{:?}", c.plan_error());
    c.apply(A::SetInPlace(true));
    let fx = c.apply(A::Start);
    assert!(matches!(fx.as_slice(), [Effect::Confirm(_)]));
    assert_eq!(c.page(), Page::Review, "nothing starts before the answer");
    c.apply(A::Confirmed {
        purpose: purpose::IN_PLACE,
        yes: true,
    });
    c.wait_idle();
    assert_eq!(c.outcome(), Some(&Ok(())));
    assert_eq!(std::fs::metadata(&src).unwrap().len(), 80 * MIB);
    assert!(c.take_disk_changed());
}

#[test]
fn the_copy_name_is_numbered_when_taken() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("disk.img");
    assert_eq!(default_copy(&src), dir.path().join("disk-resized.img"));
    std::fs::write(dir.path().join("disk-resized.img"), b"x").unwrap();
    assert_eq!(default_copy(&src), dir.path().join("disk-resized-2.img"));
}

#[test]
fn an_hfs_partition_past_its_limit_is_marked_for_a_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mac.img");
    let disk = crate::controller::test_fixtures::fat_disk(dir.path(), TableKind::Mbr, 64, &[16]);
    std::fs::rename(&disk, &path).unwrap();
    let hfs = fs_mac::hfs::create_blank_hfs(16 * MIB, 512, "Mac").unwrap();
    let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    use std::io::{Seek, SeekFrom, Write};
    f.seek(SeekFrom::Start(MIB)).unwrap();
    f.write_all(&hfs).unwrap();
    drop(f);
    let mut c = wizard(&path);
    assert_eq!(c.rows()[0].hfs_block, Some(512));
    c.apply(A::SetSizeText {
        index: 0,
        text: "30".into(),
    });
    assert_eq!(c.rebuild_block(0), None, "30 MiB fits 512-byte blocks");
    c.apply(A::SetSizeText {
        index: 0,
        text: "40".into(),
    });
    assert_eq!(c.rebuild_block(0), Some(4096));
    assert!(c
        .row_warning(0)
        .unwrap()
        .contains("rebuilt with 4 KiB blocks"));
    c.apply(A::SetCheck(false));
    c.apply(A::Next);
    c.apply(A::Next);
    c.apply(A::Next);
    c.wait_idle();
    assert_eq!(c.page(), Page::Review, "{:?}", c.plan_error());
    assert!(c.review().join("\n").contains("rebuilt with 4 KiB blocks"));
    c.apply(A::Start);
    c.wait_idle();
    assert_eq!(c.outcome(), Some(&Ok(())), "{:?}", c.steps());
}
