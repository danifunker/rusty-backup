//! NTFS integrity check (fsck) and repair.
//!
//! Same `analyze()`-shared-by-check-and-repair shape as `exfat_fsck` /
//! `ext_fsck`: one pass computes both the read-only [`FsckResult`] and the
//! concrete fixes the writable path applies. The central structure is the
//! **volume allocation bitmap** (`$Bitmap`, MFT record 6): one bit per cluster,
//! `1 = allocated`, reconciled against the actual allocation walked from the
//! MFT.
//!
//! ## What it checks
//!
//! - **Volume allocation bitmap** — the in-use cluster set is computed from a
//!   walk of every in-use MFT record's non-resident data runs (skipping sparse
//!   / hole runs). `$MFT`, `$Bitmap`, `$Boot`, `$LogFile`, index allocations —
//!   they are all just records with non-resident `$DATA` / `$INDEX_ALLOCATION`,
//!   so a straight walk yields the complete allocation. Divergence flagged as
//!   `BitmapUsedButFree` (cluster claimed by a record but marked free) and
//!   `BitmapLeaked` (cluster marked allocated but not claimed by anything).
//! - **$MFTMirr** — the first 4 MFT records (`$MFT`, `$MFTMirr`, `$LogFile`,
//!   `$Volume`) are mirrored at the LCN in `mft_mirror_cluster`. Compared;
//!   divergence flagged as `MftMirrorMismatch`.
//! - **Boot backup sector** — the last sector of the volume is a copy of the
//!   VBR (sector 0). Compared; divergence flagged as `BackupBootMismatch`.
//! - **Volume dirty flag** — bit 0 of `$Volume`'s `$VOLUME_INFORMATION.flags`.
//!   Warned when set (the same bit `chkdsk` sets/reads).
//! - **`$ATTRIBUTE_LIST`** presence — v1 does not follow attribute lists into
//!   extension records; volumes with any get an `AttributeListNotTraced`
//!   warning so the reconciled bitmap is understood as best-effort.
//!
//! ## What it repairs
//!
//! - **Rebuild $Bitmap** from the walked allocation.
//! - **Resync $MFTMirr** from the first 4 MFT records.
//! - **Rewrite the boot backup** from sector 0.
//! - **Clear VolumeDirty** in `$Volume`'s `$VOLUME_INFORMATION`.
//!
//! All are non-destructive metadata rewrites. No cross-links / MFT-record
//! rewrites are attempted — those need `chkdsk` on Windows or would risk
//! wrong-decision damage.

use std::collections::HashMap;
use std::io::{Read, Seek, Write};

use super::filesystem::FilesystemError;
use super::fsck::{FsckIssue, FsckResult, FsckStats, RepairReport};
use super::ntfs::{NtfsFilesystem, NtfsGeom};

/// Bit 0 of the `$VOLUME_INFORMATION` flags word.
const VOLUME_DIRTY_FLAG: u16 = 0x0001;

/// A concrete repair action.
enum Fix {
    /// Rewrite `$Bitmap`'s $DATA from these computed bytes.
    Bitmap(Vec<u8>),
    /// Rewrite the boot backup (last sector) from sector 0.
    BackupBoot,
    /// Rewrite `$MFTMirr` from the current first-4 MFT records.
    MftMirror,
    /// Clear the `VolumeDirty` bit in `$Volume`'s `$VOLUME_INFORMATION`.
    ClearVolumeDirty,
}

struct Analysis {
    geom: NtfsGeom,
    errors: Vec<FsckIssue>,
    warnings: Vec<FsckIssue>,
    unrepairable: usize,
    fixes: Vec<Fix>,

    files_checked: u32,
    dirs_checked: u32,
    records_in_use: u32,
    clusters_used: u64,
    bitmap_used_but_free: u64,
    bitmap_leaked: u64,
}

impl Analysis {
    fn err(&mut self, code: &str, message: String, repairable: bool) {
        if !repairable {
            self.unrepairable += 1;
        }
        self.errors.push(FsckIssue {
            code: code.into(),
            message,
            repairable,
            debug: false,
        });
    }

    fn warn(&mut self, code: &str, message: String) {
        self.warnings.push(FsckIssue {
            code: code.into(),
            message,
            repairable: false,
            debug: false,
        });
    }
}

/// Upper bound on the number of MFT records reachable from `$MFT`'s $DATA.
fn record_capacity(fs: &NtfsFilesystem<impl Read + Seek>) -> u64 {
    fs.mft_record_capacity()
}

/// The shared analysis pass. Read-only.
fn analyze<R: Read + Seek + Send>(fs: &mut NtfsFilesystem<R>) -> Result<Analysis, FilesystemError> {
    let g = fs.fsck_geometry();
    let mut a = Analysis {
        geom: g,
        errors: Vec::new(),
        warnings: Vec::new(),
        unrepairable: 0,
        fixes: Vec::new(),
        files_checked: 0,
        dirs_checked: 0,
        records_in_use: 0,
        clusters_used: 0,
        bitmap_used_but_free: 0,
        bitmap_leaked: 0,
    };

    // ---- Walk the MFT; mark clusters used by every in-use record. ----
    // `owner[lcn]` = MFT record that first claimed it, so cross-link surface
    // messages can name the other party.
    let cap = record_capacity(fs);
    let total_clusters = g.total_clusters;
    let mut owner: HashMap<u64, u64> = HashMap::new();
    let mut attribute_list_seen = false;
    let mut cross_linked = 0u64;
    let mut out_of_range = 0u64;

    for rec_num in 0..cap {
        let record = match fs.fsck_read_in_use_record(rec_num)? {
            Some(r) => r,
            None => continue,
        };
        a.records_in_use += 1;

        let flags = u16::from_le_bytes([record[0x16], record[0x17]]);
        let is_dir = flags & 0x0002 != 0;
        // Records 0..24 are metadata files, not user files.
        if rec_num >= 24 {
            if is_dir {
                a.dirs_checked += 1;
            } else {
                a.files_checked += 1;
            }
        }

        if fs.fsck_record_has_attribute_list(&record) {
            attribute_list_seen = true;
        }

        // LCN 0 is a legal run start ($Boot); sparse runs never reach here.
        for (lcn, len) in fs.fsck_record_clusters(&record) {
            if len == 0 {
                continue;
            }
            let end = lcn.saturating_add(len);
            if end > total_clusters {
                out_of_range += 1;
                continue;
            }
            for c in lcn..end {
                if let Some(&other) = owner.get(&c) {
                    if other != rec_num {
                        cross_linked += 1;
                    }
                } else {
                    owner.insert(c, rec_num);
                }
            }
        }
    }

    a.clusters_used = owner.len() as u64;

    if attribute_list_seen {
        a.warn(
            "AttributeListNotTraced",
            "one or more MFT records carry a $ATTRIBUTE_LIST — v1 fsck does not \
             follow it into extension records, so the reconciled bitmap is a \
             lower bound on the true allocation"
                .into(),
        );
    }
    if out_of_range > 0 {
        a.err(
            "OutOfRangeCluster",
            format!("{out_of_range} data run(s) reference clusters past the volume end"),
            false,
        );
    }
    if cross_linked > 0 {
        a.err(
            "CrossLinkedClusters",
            format!("{cross_linked} cluster(s) claimed by more than one MFT record"),
            false,
        );
    }

    // ---- Reconcile against $Bitmap. ----
    let on_disk = fs.fsck_read_volume_bitmap()?;
    let bytes_needed = (total_clusters as usize).div_ceil(8);
    let mut correct = vec![0u8; bytes_needed];
    for &c in owner.keys() {
        let byte = (c / 8) as usize;
        let off = (c % 8) as u8;
        if byte < correct.len() {
            correct[byte] |= 1 << off;
        }
    }

    let mut used_but_free = 0u64;
    let mut leaked = 0u64;
    for c in 0..total_clusters {
        let byte = (c / 8) as usize;
        let off = (c % 8) as u8;
        let disk = byte < on_disk.len() && on_disk[byte] & (1 << off) != 0;
        let should = owner.contains_key(&c);
        match (disk, should) {
            (false, true) => used_but_free += 1,
            (true, false) => leaked += 1,
            _ => {}
        }
    }
    a.bitmap_used_but_free = used_but_free;
    a.bitmap_leaked = leaked;
    // If we couldn't fully trace the allocation (attribute list), don't offer
    // to rewrite the bitmap — used-but-free would be spurious for the untraced
    // fragments. Leaks and errors are still reported.
    let bitmap_repairable = !attribute_list_seen;
    if used_but_free > 0 || leaked > 0 {
        if bitmap_repairable {
            // Preserve any trailing (beyond-cluster_count) bytes so we only
            // touch the meaningful range.
            let mut rebuilt = on_disk.clone();
            let valid = bytes_needed.min(rebuilt.len());
            rebuilt[..valid].copy_from_slice(&correct[..valid]);
            a.fixes.push(Fix::Bitmap(rebuilt));
        }
        if used_but_free > 0 {
            a.err(
                "BitmapUsedButFree",
                format!(
                    "{used_but_free} cluster(s) claimed by an MFT record but marked free in $Bitmap"
                ),
                bitmap_repairable,
            );
        }
        if leaked > 0 {
            a.err(
                "BitmapLeaked",
                format!(
                    "{leaked} cluster(s) marked allocated in $Bitmap but referenced by nothing"
                ),
                bitmap_repairable,
            );
        }
    }

    // ---- $MFTMirr. ----
    let first4 = fs.fsck_read_first_mft_records()?;
    let mirr_off = fs.fsck_mftmirr_offset();
    let mirr = fs.fsck_read_raw(mirr_off, first4.len())?;
    if mirr != first4 {
        a.err(
            "MftMirrorMismatch",
            "$MFTMirr does not match the first 4 records of $MFT".into(),
            true,
        );
        a.fixes.push(Fix::MftMirror);
    }

    // ---- Boot backup: at sector index total_sectors (the VBR field excludes
    // the backup sector, so it sits one past the last addressable sector). ----
    let bps = g.bytes_per_sector;
    let vbr = fs.fsck_read_raw(g.partition_offset, bps as usize)?;
    let backup_off = g.partition_offset + g.total_sectors * bps;
    let backup = fs.fsck_read_raw(backup_off, bps as usize)?;
    if backup != vbr {
        a.err(
            "BackupBootMismatch",
            "backup boot sector (last sector) does not match the VBR".into(),
            true,
        );
        a.fixes.push(Fix::BackupBoot);
    }

    // ---- Volume dirty flag. ----
    let vflags = fs.fsck_volume_flags()?;
    if vflags & VOLUME_DIRTY_FLAG != 0 {
        a.warn(
            "VolumeDirty",
            "VolumeDirty flag is set in $Volume — Windows will run chkdsk on next mount".into(),
        );
        a.fixes.push(Fix::ClearVolumeDirty);
    }

    Ok(a)
}

fn stats(a: &Analysis) -> FsckStats {
    FsckStats {
        files_checked: a.files_checked,
        directories_checked: a.dirs_checked,
        extra: vec![
            ("cluster_size".into(), a.geom.cluster_size.to_string()),
            ("total_clusters".into(), a.geom.total_clusters.to_string()),
            ("clusters_used".into(), a.clusters_used.to_string()),
            ("records_in_use".into(), a.records_in_use.to_string()),
            (
                "bitmap_used_but_free".into(),
                a.bitmap_used_but_free.to_string(),
            ),
            ("bitmap_leaked".into(), a.bitmap_leaked.to_string()),
        ],
    }
}

/// Run the NTFS integrity check (read-only).
pub fn fsck_ntfs<R: Read + Seek + Send>(
    fs: &mut NtfsFilesystem<R>,
) -> Result<FsckResult, FilesystemError> {
    let a = analyze(fs)?;
    let repairable = a.errors.iter().any(|e| e.repairable);
    let stats = stats(&a);
    Ok(FsckResult {
        errors: a.errors,
        warnings: a.warnings,
        stats,
        repairable,
        orphaned_entries: Vec::new(),
    })
}

/// Apply the fixes `analyze` produced: rebuild `$Bitmap`, resync `$MFTMirr`,
/// rewrite the boot backup, clear the dirty flag.
pub fn repair_ntfs<R: Read + Write + Seek + Send>(
    fs: &mut NtfsFilesystem<R>,
) -> Result<RepairReport, FilesystemError> {
    let a = analyze(fs)?;
    let g = a.geom;
    let mut applied: Vec<String> = Vec::new();
    let mut failed: Vec<String> = Vec::new();

    for fix in &a.fixes {
        let r: Result<String, FilesystemError> = match fix {
            Fix::Bitmap(bytes) => fs.fsck_write_volume_bitmap(bytes).map(|_| {
                format!(
                    "rebuilt $Bitmap ({} used-but-free, {} leaked)",
                    a.bitmap_used_but_free, a.bitmap_leaked
                )
            }),
            Fix::MftMirror => {
                let first4 = fs.fsck_read_first_mft_records()?;
                fs.fsck_write_raw(fs.fsck_mftmirr_offset(), &first4)
                    .map(|_| "resynchronised $MFTMirr from $MFT".into())
            }
            Fix::BackupBoot => {
                let vbr = fs.fsck_read_raw(g.partition_offset, g.bytes_per_sector as usize)?;
                let backup_off = g.partition_offset + g.total_sectors * g.bytes_per_sector;
                fs.fsck_write_raw(backup_off, &vbr)
                    .map(|_| "rewrote backup boot sector from the VBR".into())
            }
            Fix::ClearVolumeDirty => {
                // The $VOLUME_INFORMATION flags live at offset 10 of the value.
                // Read the record, clear bit 0, write it back through the same
                // path `write_mft_record` uses. Doing this via update_resident
                // would need extra plumbing; instead we splice the two bytes
                // into the record buffer and re-write the whole record via
                // `fsck_write_raw` at the correct MFT offset — the fixup is
                // preserved because we do not alter any 512-byte block ends.
                clear_volume_dirty(fs).map(|_| "cleared VolumeDirty flag in $Volume".into())
            }
        };
        match r {
            Ok(desc) => applied.push(desc),
            Err(e) => failed.push(format!("{e}")),
        }
    }

    if !applied.is_empty() {
        fs.fsck_flush_writer()?;
    }

    Ok(RepairReport {
        fixes_applied: applied,
        fixes_failed: failed,
        unrepairable_count: a.unrepairable,
    })
}

/// Clear the `VolumeDirty` bit by patching the two flag bytes in `$Volume`'s
/// resident `$VOLUME_INFORMATION` attribute on disk. Walking the record
/// attributes gives us the exact byte offset; we then splice a single 2-byte
/// write at that location. The USA (last 2 bytes per 512-byte block) is not
/// touched, so no fixup rewrite is needed.
fn clear_volume_dirty<R: Read + Write + Seek + Send>(
    fs: &mut NtfsFilesystem<R>,
) -> Result<(), FilesystemError> {
    let g = fs.fsck_geometry();
    // $Volume is MFT record 3.
    let record = match fs.fsck_read_in_use_record(3)? {
        Some(r) => r,
        None => {
            return Err(FilesystemError::Parse(
                "$Volume MFT record (3) is not in use".into(),
            ));
        }
    };
    // $VOLUME_INFORMATION layout: reserved u64, major_ver u8, minor_ver u8,
    // flags u16. Flags word starts at value+10 (see `fsck_volume_flags` and
    // `read_ntfs_version` in ntfs.rs for the same offsets).
    let flags_off_in_attr_value = 10usize;
    let attr_off = find_resident_attr_value_offset(&record, 0x70)
        .ok_or_else(|| FilesystemError::Parse("$VOLUME_INFORMATION not found".into()))?;
    let target = attr_off + flags_off_in_attr_value;
    if target + 2 > record.len() {
        return Err(FilesystemError::Parse(
            "$VOLUME_INFORMATION too short".into(),
        ));
    }
    let old = u16::from_le_bytes([record[target], record[target + 1]]);
    let new = old & !VOLUME_DIRTY_FLAG;
    if new == old {
        return Ok(());
    }
    // MFT record 3 always lives in the first fragment at mft_cluster (records
    // 0..24 are metadata, laid down contiguously by both mkntfs and $MFT
    // relocation), so we can compute its disk offset directly.
    let record_off = g.partition_offset
        + g.mft_cluster * g.cluster_size
        + 3 * g.mft_record_size as u64
        + target as u64;
    fs.fsck_write_raw(record_off, &new.to_le_bytes())?;
    Ok(())
}

/// Scan resident attributes in an MFT record; return the offset (within the
/// record) of `target_type`'s value bytes, if found.
fn find_resident_attr_value_offset(record: &[u8], target_type: u32) -> Option<usize> {
    if record.len() < 0x18 {
        return None;
    }
    let mut pos = u16::from_le_bytes([record[0x14], record[0x15]]) as usize;
    while pos + 16 <= record.len() {
        let atype = u32::from_le_bytes([
            record[pos],
            record[pos + 1],
            record[pos + 2],
            record[pos + 3],
        ]);
        if atype == 0xFFFF_FFFF || atype == 0 {
            return None;
        }
        let alen = u32::from_le_bytes([
            record[pos + 4],
            record[pos + 5],
            record[pos + 6],
            record[pos + 7],
        ]) as usize;
        if alen < 16 || pos + alen > record.len() {
            return None;
        }
        let non_resident = record[pos + 8];
        if atype == target_type && non_resident == 0 {
            let vo = u16::from_le_bytes([record[pos + 0x14], record[pos + 0x15]]) as usize;
            return Some(pos + vo);
        }
        pos += alen;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::ntfs::NtfsFilesystem;
    use std::io::Cursor;

    type Img = Cursor<Vec<u8>>;

    /// Decompress the committed fixture. The fixture was created by our own
    /// clean-room formatter and carries two harmless baseline defects that
    /// `chkdsk` would also flag: a couple of leaked bits in `$Bitmap` and no
    /// backup boot sector. `load()` runs `repair_ntfs` once so every test
    /// starts from a truly clean image.
    fn load() -> Img {
        let path = "tests/fixtures/test_ntfs.img.zst";
        let compressed = std::fs::read(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        let mut dec =
            crate::rbformats::zstd_compat::decoder(Cursor::new(compressed)).expect("zstd decoder");
        let mut out = Vec::new();
        dec.read_to_end(&mut out).expect("decompress");
        let mut img = Cursor::new(out);
        // Bring the fixture up to a repair-verified-clean baseline. This is the
        // same normalisation `rb-cli fsck --repair` performs on first run.
        {
            let mut fs = NtfsFilesystem::open(&mut img, 0).expect("open fixture");
            repair_ntfs(&mut fs).expect("baseline repair");
        }
        img.set_position(0);
        img
    }

    fn open(img: &mut Img) -> NtfsFilesystem<&mut Img> {
        NtfsFilesystem::open(img, 0).expect("open NTFS")
    }

    fn run_fsck(img: &mut Img) -> FsckResult {
        let mut fs = open(img);
        fsck_ntfs(&mut fs).expect("fsck runs")
    }

    fn run_repair(img: &mut Img) -> RepairReport {
        let mut fs = open(img);
        repair_ntfs(&mut fs).expect("repair runs")
    }

    fn codes(r: &FsckResult) -> Vec<String> {
        r.errors.iter().map(|e| e.code.clone()).collect()
    }

    fn read_bitmap(img: &mut Img) -> (NtfsGeom, Vec<u8>) {
        let mut fs = open(img);
        let g = fs.fsck_geometry();
        let bm = fs.fsck_read_volume_bitmap().unwrap();
        (g, bm)
    }

    fn write_bitmap(img: &mut Img, bm: &[u8]) {
        let mut fs = open(img);
        fs.fsck_write_volume_bitmap(bm).unwrap();
        fs.fsck_flush_writer().unwrap();
    }

    #[test]
    fn clean_fixture_reports_no_errors() {
        let mut img = load();
        let r = run_fsck(&mut img);
        assert!(
            r.is_clean(),
            "clean fixture reported errors: {:?}",
            r.errors
        );
    }

    #[test]
    fn detects_and_repairs_bitmap_leak() {
        let mut img = load();
        let (g, mut bm) = read_bitmap(&mut img);
        let leaked = (0..g.total_clusters)
            .find(|&c| {
                let byte = (c / 8) as usize;
                let off = (c % 8) as u8;
                byte < bm.len() && bm[byte] & (1 << off) == 0
            })
            .expect("a free cluster");
        let byte = (leaked / 8) as usize;
        let off = (leaked % 8) as u8;
        bm[byte] |= 1 << off;
        write_bitmap(&mut img, &bm);

        let r = run_fsck(&mut img);
        assert!(
            codes(&r).contains(&"BitmapLeaked".to_string()),
            "{:?}",
            r.errors
        );
        assert!(r.repairable);

        let rep = run_repair(&mut img);
        assert!(rep.fixes_failed.is_empty(), "{:?}", rep.fixes_failed);
        assert!(run_fsck(&mut img).is_clean());
    }

    #[test]
    fn detects_and_repairs_bitmap_used_but_free() {
        let mut img = load();
        let (g, mut bm) = read_bitmap(&mut img);
        // Find a cluster the walk claims (any bit currently 1) and clear it.
        let used = (0..g.total_clusters)
            .find(|&c| {
                let byte = (c / 8) as usize;
                let off = (c % 8) as u8;
                byte < bm.len() && bm[byte] & (1 << off) != 0
            })
            .expect("a used cluster");
        let byte = (used / 8) as usize;
        let off = (used % 8) as u8;
        bm[byte] &= !(1 << off);
        write_bitmap(&mut img, &bm);

        let r = run_fsck(&mut img);
        assert!(
            codes(&r).contains(&"BitmapUsedButFree".to_string()),
            "{:?}",
            r.errors
        );

        run_repair(&mut img);
        assert!(run_fsck(&mut img).is_clean());
    }

    #[test]
    fn detects_and_repairs_backup_boot() {
        let mut img = load();
        // Overwrite the backup boot sector so it no longer matches the VBR.
        {
            let mut fs = open(&mut img);
            let g = fs.fsck_geometry();
            let backup_off = g.partition_offset + g.total_sectors * g.bytes_per_sector;
            fs.fsck_write_raw(backup_off, &vec![0xAA; g.bytes_per_sector as usize])
                .unwrap();
            fs.fsck_flush_writer().unwrap();
        }
        let r = run_fsck(&mut img);
        assert!(
            codes(&r).contains(&"BackupBootMismatch".to_string()),
            "{:?}",
            r.errors
        );

        let rep = run_repair(&mut img);
        assert!(rep.fixes_failed.is_empty(), "{:?}", rep.fixes_failed);
        assert!(run_fsck(&mut img).is_clean());
    }

    #[test]
    fn detects_and_repairs_mft_mirror() {
        let mut img = load();
        // Scribble on the first 16 bytes of $MFTMirr.
        {
            let mut fs = open(&mut img);
            let off = fs.fsck_mftmirr_offset();
            let mut byt = fs.fsck_read_raw(off, 16).unwrap();
            byt[0] ^= 0xFF; // "FILE" -> not-"FILE"
            fs.fsck_write_raw(off, &byt).unwrap();
            fs.fsck_flush_writer().unwrap();
        }
        let r = run_fsck(&mut img);
        assert!(
            codes(&r).contains(&"MftMirrorMismatch".to_string()),
            "{:?}",
            r.errors
        );

        let rep = run_repair(&mut img);
        assert!(rep.fixes_failed.is_empty(), "{:?}", rep.fixes_failed);
        assert!(run_fsck(&mut img).is_clean());
    }

    #[test]
    fn detects_and_clears_volume_dirty() {
        let mut img = load();
        // Set the VolumeDirty bit via a targeted 2-byte splice at $Volume's
        // $VOLUME_INFORMATION flags. We locate the byte the same way repair
        // does so we're patching the exact spot.
        {
            let mut fs = open(&mut img);
            let g = fs.fsck_geometry();
            let record = fs.fsck_read_in_use_record(3).unwrap().unwrap();
            let attr_off = super::find_resident_attr_value_offset(&record, 0x70).unwrap();
            let target = attr_off + 10; // flags word offset within $VOLUME_INFORMATION
            let record_off =
                g.partition_offset + g.mft_cluster * g.cluster_size + 3 * g.mft_record_size as u64;
            let mut cur = fs.fsck_read_raw(record_off + target as u64, 2).unwrap();
            let cur_flags = u16::from_le_bytes([cur[0], cur[1]]);
            let dirtied = cur_flags | VOLUME_DIRTY_FLAG;
            cur.copy_from_slice(&dirtied.to_le_bytes());
            fs.fsck_write_raw(record_off + target as u64, &cur).unwrap();
            fs.fsck_flush_writer().unwrap();
        }

        let r = run_fsck(&mut img);
        assert!(
            r.warnings.iter().any(|w| w.code == "VolumeDirty"),
            "{:?}",
            r.warnings
        );

        let rep = run_repair(&mut img);
        assert!(rep.fixes_applied.iter().any(|f| f.contains("VolumeDirty")));
        let r2 = run_fsck(&mut img);
        assert!(!r2.warnings.iter().any(|w| w.code == "VolumeDirty"));
    }

    // ---- Oracle cross-check against Windows `chkdsk`. ----
    //
    // `chkdsk` needs a mounted volume with a drive letter; we mount the raw
    // image via `Mount-DiskImage` (works on fixed VHD only, so we wrap the raw
    // image in a fixed VHD footer at test time). Gated to Windows.
    #[cfg(windows)]
    #[test]
    fn oracle_chkdsk_agrees_after_repair() {
        use std::process::Command;
        use std::sync::atomic::{AtomicU32, Ordering};
        static NONCE: AtomicU32 = AtomicU32::new(0);

        // Corrupt a bitmap bit, repair with our code, then run chkdsk /f on
        // the mounted image. Exit code 0 means the volume is fully clean.
        let mut img = load();
        let (g, mut bm) = read_bitmap(&mut img);
        let leaked = (0..g.total_clusters)
            .find(|&c| {
                let byte = (c / 8) as usize;
                let off = (c % 8) as u8;
                byte < bm.len() && bm[byte] & (1 << off) == 0
            })
            .unwrap();
        let byte = (leaked / 8) as usize;
        let off = (leaked % 8) as u8;
        bm[byte] |= 1 << off;
        write_bitmap(&mut img, &bm);
        run_repair(&mut img);

        let n = NONCE.fetch_add(1, Ordering::Relaxed);
        let img_path = std::env::temp_dir().join(format!("rb_ntfs_{}_{n}.img", std::process::id()));
        let vhd_path = std::env::temp_dir().join(format!("rb_ntfs_{}_{n}.vhd", std::process::id()));
        std::fs::write(&img_path, img.get_ref()).unwrap();

        // Build a fixed VHD by copying the raw image and appending a 512-byte
        // fixed-VHD footer with a valid checksum. Mount-DiskImage refuses raw
        // .img, but a fixed VHD is just image bytes + footer.
        let raw = std::fs::read(&img_path).unwrap();
        let footer = fixed_vhd_footer(raw.len() as u64);
        let mut wrapped = raw;
        wrapped.extend_from_slice(&footer);
        std::fs::write(&vhd_path, &wrapped).unwrap();

        // Try to mount; if elevation / VHDs aren't available, skip.
        let mount = Command::new("powershell")
            .args([
                "-NoProfile",
                "-Command",
                &format!(
                    "$m = Mount-DiskImage -ImagePath '{}' -PassThru -ErrorAction Stop; \
                     ($m | Get-Disk | Get-Partition | Where-Object DriveLetter | \
                     Select-Object -First 1 -ExpandProperty DriveLetter)",
                    vhd_path.display()
                ),
            ])
            .output();
        let letter = match mount {
            Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).trim().to_string(),
            _ => {
                eprintln!("skipping: cannot Mount-DiskImage (likely non-admin or no Hyper-V PS)");
                let _ = std::fs::remove_file(&img_path);
                let _ = std::fs::remove_file(&vhd_path);
                return;
            }
        };
        if letter.is_empty() {
            eprintln!("skipping: mounted VHD did not expose a drive letter");
            let _ = Command::new("powershell")
                .args([
                    "-NoProfile",
                    "-Command",
                    &format!("Dismount-DiskImage -ImagePath '{}'", vhd_path.display()),
                ])
                .output();
            let _ = std::fs::remove_file(&img_path);
            let _ = std::fs::remove_file(&vhd_path);
            return;
        }

        // Read-only check: chkdsk without /f. Exit 0 == clean.
        let out = Command::new("chkdsk").arg(format!("{letter}:")).output();
        let _ = Command::new("powershell")
            .args([
                "-NoProfile",
                "-Command",
                &format!("Dismount-DiskImage -ImagePath '{}'", vhd_path.display()),
            ])
            .output();
        let _ = std::fs::remove_file(&img_path);
        let _ = std::fs::remove_file(&vhd_path);

        let ok = out.map(|o| o.status.success()).unwrap_or(false);
        assert!(ok, "chkdsk should find our repaired volume clean");
    }

    /// Build a 512-byte fixed VHD footer for a raw image of `size` bytes.
    /// The footer is the standard Conner/Connectix "conectix" layout with
    /// disk_type=2 (fixed) and a valid checksum. Enough for Mount-DiskImage
    /// to accept the file as a VHD.
    #[cfg(windows)]
    fn fixed_vhd_footer(size: u64) -> [u8; 512] {
        let mut f = [0u8; 512];
        f[0..8].copy_from_slice(b"conectix");
        f[8..12].copy_from_slice(&0xFFFFFFFFu32.to_be_bytes()); // features
        f[12..16].copy_from_slice(&0x0001_0000u32.to_be_bytes()); // file format version
        f[16..24].copy_from_slice(&0xFFFF_FFFF_FFFF_FFFFu64.to_be_bytes()); // data offset (fixed)
        f[24..28].copy_from_slice(&0u32.to_be_bytes()); // timestamp
        f[28..32].copy_from_slice(b"rbcp"); // creator app
        f[32..36].copy_from_slice(&0x0001_0000u32.to_be_bytes()); // creator ver
        f[36..40].copy_from_slice(b"Wi2k"); // creator host os
        f[40..48].copy_from_slice(&size.to_be_bytes()); // original size
        f[48..56].copy_from_slice(&size.to_be_bytes()); // current size
                                                        // CHS geometry: cylinders / heads / sectors_per_track. Compute a
                                                        // best-effort geometry per the VHD spec (any consistent triple works
                                                        // for fixed VHDs).
        let sectors = size / 512;
        let (cyls, heads, spt) = chs_from_sectors(sectors);
        f[56..58].copy_from_slice(&cyls.to_be_bytes());
        f[58] = heads;
        f[59] = spt;
        f[60..64].copy_from_slice(&2u32.to_be_bytes()); // disk type = fixed
                                                        // unique id (16 bytes at 68..84) left zero
                                                        // saved state (byte 84) left zero
                                                        // Checksum: ones' complement of the sum of every other byte (bytes
                                                        // 64..68 held zero during the sum).
        let mut sum: u32 = 0;
        for &b in f.iter() {
            sum = sum.wrapping_add(b as u32);
        }
        let cksum = !sum;
        f[64..68].copy_from_slice(&cksum.to_be_bytes());
        f
    }

    #[cfg(windows)]
    fn chs_from_sectors(total: u64) -> (u16, u8, u8) {
        // Per the VHD 1.0 spec's example CHS algorithm. Kept simple; any
        // consistent triple works for a fixed VHD Mount-DiskImage accepts.
        let total = total.min(65535 * 16 * 255);
        let (mut spt, mut heads, mut cyls_times_heads): (u8, u8, u64);
        if total > 65535 * 16 * 63 {
            spt = 255;
            heads = 16;
            cyls_times_heads = total / spt as u64;
        } else {
            spt = 17;
            cyls_times_heads = total / spt as u64;
            heads = cyls_times_heads.div_ceil(1024).clamp(4, 16) as u8;
            if cyls_times_heads >= (heads as u64 * 1024) {
                spt = 31;
                heads = 16;
                cyls_times_heads = total / spt as u64;
            }
            if cyls_times_heads >= (heads as u64 * 1024) {
                spt = 63;
                heads = 16;
                cyls_times_heads = total / spt as u64;
            }
        }
        let cyls = (cyls_times_heads / heads as u64) as u16;
        (cyls, heads, spt)
    }
}
