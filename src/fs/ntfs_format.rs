//! From-scratch NTFS formatter (`create_blank_ntfs`) — the foundation for the
//! defragmenting NTFS clone, the peer of [`crate::fs::exfat::create_blank_exfat`].
//!
//! NTFS is far more intricate than exFAT (a dozen interlocking system files:
//! $MFT, $MFTMirr, $LogFile, $Volume, $AttrDef, root index, $Bitmap, $Boot,
//! $BadClus, $Secure, $UpCase, $Extend). Rather than synthesize every byte from
//! the spec, this reuses the **size-independent** pieces from a known-good
//! `mkntfs` reference image (the `$AttrDef`/`$UpCase` tables, the `$Secure`
//! descriptors, the boot code, the system-record templates, and the root index
//! that links records 0–11) and recomputes only the **size-dependent** layout:
//! the cluster placement, the non-resident `$DATA` runs, `$Bitmap`, the boot
//! geometry, and `$MFTMirr`. The reference blobs live in `ntfs_template/` and
//! were extracted from `mkntfs -F -s 512 -c 4096`; the resulting volumes mount
//! clean under `ntfs-3g`/`ntfsfix` and round-trip files via `ntfscp`/`ntfscat`.
//!
//! Fixed geometry: 512-byte sectors, **4096-byte clusters**, **1024-byte MFT
//! records**. The clone copies files (not raw clusters), so the target cluster
//! size is independent of the source — 4096 is the modern NTFS default and
//! covers volumes up to 16 TB.

use std::io::{Seek, SeekFrom, Write};

use super::filesystem::FilesystemError;

const BPS: u64 = 512;
const SPC: u64 = 8;
const CLUSTER: u64 = BPS * SPC; // 4096
const REC: usize = 1024; // MFT record size

static BOOT: &[u8] = include_bytes!("ntfs_template/boot.bin"); // 8192
static MFT_RECORDS: &[u8] = include_bytes!("ntfs_template/mft_records.bin"); // 16 * 1024
static ATTRDEF: &[u8] = include_bytes!("ntfs_template/attrdef.bin"); // 2560
static UPCASE: &[u8] = include_bytes!("ntfs_template/upcase.bin"); // 131072
static SECURE_SDS: &[u8] = include_bytes!("ntfs_template/secure_sds.bin"); // 262396
static ROOT_SECDESC: &[u8] = include_bytes!("ntfs_template/root_secdesc.bin"); // 4140
static ROOT_INDEX: &[u8] = include_bytes!("ntfs_template/root_index.bin"); // 4096

#[inline]
fn cdiv(a: u64, b: u64) -> u64 {
    a.div_ceil(b)
}
#[inline]
fn ru16(b: &[u8], o: usize) -> u16 {
    u16::from_le_bytes([b[o], b[o + 1]])
}
#[inline]
fn ru32(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}
#[inline]
fn wu16(b: &mut [u8], o: usize, v: u16) {
    b[o..o + 2].copy_from_slice(&v.to_le_bytes());
}
#[inline]
fn wu32(b: &mut [u8], o: usize, v: u32) {
    b[o..o + 4].copy_from_slice(&v.to_le_bytes());
}
#[inline]
fn wu64(b: &mut [u8], o: usize, v: u64) {
    b[o..o + 8].copy_from_slice(&v.to_le_bytes());
}

/// One non-resident-run patch: (attr type, attr name, run length in clusters,
/// LCN (None = sparse), allocated size, real size).
type RunEdit<'a> = (u32, &'a str, u64, Option<u64>, u64, u64);

/// One parsed attribute (raw bytes, kept verbatim unless patched).
struct Attr {
    atype: u32,
    name: String,
    bytes: Vec<u8>,
}

fn record_template(n: usize) -> &'static [u8] {
    &MFT_RECORDS[n * REC..(n + 1) * REC]
}

fn parse_attrs(rec: &[u8]) -> Vec<Attr> {
    let mut out = Vec::new();
    let mut o = ru16(rec, 0x14) as usize;
    while o + 4 <= rec.len() {
        let atype = ru32(rec, o);
        if atype == 0xFFFF_FFFF {
            break;
        }
        let alen = ru32(rec, o + 4) as usize;
        if alen == 0 || o + alen > rec.len() {
            break;
        }
        let nlen = rec[o + 9] as usize;
        let noff = ru16(rec, o + 10) as usize;
        let name = if nlen > 0 {
            let units: Vec<u16> = (0..nlen).map(|i| ru16(rec, o + noff + i * 2)).collect();
            String::from_utf16_lossy(&units)
        } else {
            String::new()
        };
        out.push(Attr {
            atype,
            name,
            bytes: rec[o..o + alen].to_vec(),
        });
        o += alen;
    }
    out
}

/// Encode a single data run. `lcn == None` -> a sparse run (length only).
fn enc_run(length: u64, lcn: Option<u64>) -> Vec<u8> {
    let lb = (64 - length.leading_zeros()).div_ceil(8).max(1) as usize;
    match lcn {
        None => {
            let mut v = vec![lb as u8];
            v.extend_from_slice(&length.to_le_bytes()[..lb]);
            v
        }
        Some(l) => {
            // Signed offset: reserve a byte so the sign bit stays clear.
            let bits = 64 - l.leading_zeros();
            let ob = (((bits + 8) / 8).max(1)) as usize;
            let mut v = vec![(lb | (ob << 4)) as u8];
            v.extend_from_slice(&length.to_le_bytes()[..lb]);
            v.extend_from_slice(&(l as i64).to_le_bytes()[..ob]);
            v
        }
    }
}

/// Rebuild a non-resident attribute's runlist + size fields for a new single
/// contiguous (or sparse) extent.
fn set_nonres_run(
    attr: &mut Vec<u8>,
    length_clusters: u64,
    lcn: Option<u64>,
    alloc: u64,
    real: u64,
) {
    let nlen = attr[9] as usize;
    let run = enc_run(length_clusters, lcn);
    let run_off = (0x40 + nlen * 2 + 7) & !7;
    let mut new = attr[..run_off].to_vec();
    new.extend_from_slice(&run);
    while !new.len().is_multiple_of(8) {
        new.push(0);
    }
    wu16(&mut new, 0x20, run_off as u16); // mapping pairs offset
    wu64(&mut new, 0x10, 0); // start VCN
    wu64(&mut new, 0x18, length_clusters - 1); // last VCN
    wu64(&mut new, 0x28, alloc); // allocated size
    wu64(&mut new, 0x30, real); // real size
    wu64(&mut new, 0x38, real); // initialized size
    let attr_len = new.len() as u32;
    wu32(&mut new, 4, attr_len); // attribute length
    *attr = new;
}

/// Set the resident `$VOLUME_NAME` value (record 3) to `label` (UTF-16).
fn set_volume_name(attr: &mut Vec<u8>, label: &str) {
    let coff = ru16(attr, 0x14) as usize;
    let mut val = Vec::new();
    for u in label.encode_utf16() {
        val.extend_from_slice(&u.to_le_bytes());
    }
    let mut new = attr[..coff].to_vec();
    new.extend_from_slice(&val);
    while !new.len().is_multiple_of(8) {
        new.push(0);
    }
    let val_len = val.len() as u32;
    let attr_len = new.len() as u32;
    wu32(&mut new, 0x10, val_len); // content length
    wu32(&mut new, 4, attr_len); // attribute length
    *attr = new;
}

/// Write the update-sequence (fixup) array: bump the USN and stash the last two
/// bytes of every sector into the array, replacing them with the USN.
fn fixup_write(rec: &mut [u8]) {
    let uoff = ru16(rec, 4) as usize;
    let ucnt = ru16(rec, 6) as usize;
    let mut usn = ru16(rec, uoff).wrapping_add(1);
    if usn == 0 {
        usn = 1;
    }
    wu16(rec, uoff, usn);
    for i in 1..ucnt {
        let tail = i * BPS as usize - 2;
        let last = ru16(rec, tail);
        wu16(rec, uoff + i * 2, last);
        wu16(rec, tail, usn);
    }
}

/// Reassemble a 1024-byte MFT record from a template header plus `attrs`,
/// terminate it, set the size fields, and write the fixup array.
fn build_record(template: &[u8], attrs: &[Attr]) -> [u8; REC] {
    let mut buf = [0u8; REC];
    let header_end = ru16(template, 0x14) as usize;
    buf[..header_end].copy_from_slice(&template[..header_end]);
    let mut o = header_end;
    for a in attrs {
        buf[o..o + a.bytes.len()].copy_from_slice(&a.bytes);
        o += a.bytes.len();
    }
    wu32(&mut buf, o, 0xFFFF_FFFF);
    o += 4;
    let used = (o + 7) & !7;
    wu32(&mut buf, 0x18, used as u32);
    wu32(&mut buf, 0x1C, REC as u32);
    fixup_write(&mut buf);
    buf
}

/// A blank, unused MFT record (sequence 0).
fn blank_record() -> [u8; REC] {
    let mut b = [0u8; REC];
    b[0..4].copy_from_slice(b"FILE");
    let fixup_count = (REC as u64 / BPS + 1) as u16;
    wu16(&mut b, 4, 0x30);
    wu16(&mut b, 6, fixup_count);
    wu16(&mut b, 0x10, 0); // sequence 0 = unused
    let fa = (0x30 + fixup_count as usize * 2 + 7) & !7;
    wu16(&mut b, 0x14, fa as u16);
    wu32(&mut b, 0x18, (fa + 4) as u32);
    wu32(&mut b, 0x1C, REC as u32);
    wu32(&mut b, fa, 0xFFFF_FFFF);
    fixup_write(&mut b);
    b
}

/// Smallest MFT-record count that holds the 24 reserved system records plus
/// `entries` user files/dirs, with headroom.
pub fn ntfs_mft_records_for(entries: u64) -> u64 {
    (24 + entries + entries / 8 + 32).max(64)
}

/// Smallest target size (bytes, 4096-cluster aligned) for a defragmenting clone
/// holding `used_bytes` of file data plus `entries` files/dirs of metadata
/// overhead. Mirrors [`crate::fs::exfat::exfat_min_packed_size`].
pub fn ntfs_min_packed_size(used_bytes: u64, entries: u64) -> u64 {
    let mft_records = ntfs_mft_records_for(entries);
    let mft_c = cdiv(mft_records * REC as u64, CLUSTER);
    let upcase_c = cdiv(UPCASE.len() as u64, CLUSTER);
    let sds_c = cdiv(SECURE_SDS.len() as u64, CLUSTER);
    let log_c = cdiv(2 * 1024 * 1024, CLUSTER);
    // boot + mft-bitmap + mft + mftmirr + logfile + clusterbitmap + attrdef +
    // upcase + sds + root sd + root index + the data itself + margin.
    let data_c = cdiv(used_bytes, CLUSTER);
    let meta_c = 2 + 1 + mft_c + 1 + log_c + 8 + 1 + upcase_c + sds_c + 2 + 1;
    let total_c = meta_c + data_c + data_c / 20 + 64;
    total_c * CLUSTER
}

/// Format a fresh, mountable blank NTFS volume of `target_size` bytes (a
/// multiple of the 4096-byte cluster size) into `target`, with room for at
/// least `mft_records` MFT records and the given volume `label`.
pub fn create_blank_ntfs<W: Write + Seek>(
    target: &mut W,
    target_size: u64,
    mft_records: u64,
    label: Option<&str>,
) -> Result<(), FilesystemError> {
    if !target_size.is_multiple_of(CLUSTER) {
        return Err(FilesystemError::InvalidData(format!(
            "NTFS format: target size {target_size} is not a multiple of the 4096-byte cluster"
        )));
    }
    let tc = target_size / CLUSTER;
    let mft_records = mft_records.max(64);

    // ---- Contiguous layout ----
    let boot_c = cdiv(8192, CLUSTER);
    let mft_c = cdiv(mft_records * REC as u64, CLUSTER);
    let mftmirr_c = cdiv(4 * REC as u64, CLUSTER);
    let log_c = cdiv(2 * 1024 * 1024, CLUSTER);
    let attrdef_c = cdiv(ATTRDEF.len() as u64, CLUSTER);
    let upcase_c = cdiv(UPCASE.len() as u64, CLUSTER);
    let sds_c = cdiv(SECURE_SDS.len() as u64, CLUSTER);
    let rootsd_c = cdiv(ROOT_SECDESC.len() as u64, CLUSTER);
    let rootidx_c = cdiv(ROOT_INDEX.len() as u64, CLUSTER);
    let bm_c = cdiv(cdiv(tc, 8), CLUSTER);

    let mut lcn = boot_c;
    let mftbm_lcn = lcn;
    lcn += 1;
    let mft_lcn = lcn;
    lcn += mft_c;
    let mftmirr_lcn = lcn;
    lcn += mftmirr_c;
    let log_lcn = lcn;
    lcn += log_c;
    let bm_lcn = lcn;
    lcn += bm_c;
    let attrdef_lcn = lcn;
    lcn += attrdef_c;
    let upcase_lcn = lcn;
    lcn += upcase_c;
    let sds_lcn = lcn;
    lcn += sds_c;
    let rootsd_lcn = lcn;
    lcn += rootsd_c;
    let rootidx_lcn = lcn;
    lcn += rootidx_c;
    let end_meta = lcn;
    if end_meta >= tc {
        return Err(FilesystemError::DiskFull(format!(
            "NTFS format: metadata needs {end_meta} clusters but the volume only has {tc}"
        )));
    }

    // ---- Build system records (patch the size-dependent runs) ----
    let patch = |n: usize, edits: &[RunEdit]| -> [u8; REC] {
        let mut attrs = parse_attrs(record_template(n));
        for &(atype, name, lc, lcn_, alloc, real) in edits {
            for a in attrs.iter_mut() {
                if a.atype == atype && a.name == name {
                    set_nonres_run(&mut a.bytes, lc, lcn_, alloc, real);
                }
            }
        }
        build_record(record_template(n), &attrs)
    };

    let bm_real = cdiv(tc, 8);
    let mut recs: Vec<[u8; REC]> = Vec::with_capacity(mft_records as usize);

    let rec0 = patch(
        0,
        &[
            (
                0x80,
                "",
                mft_c,
                Some(mft_lcn),
                mft_c * CLUSTER,
                mft_c * CLUSTER,
            ),
            (0xB0, "", 1, Some(mftbm_lcn), CLUSTER, cdiv(mft_records, 8)),
        ],
    );
    let rec1 = patch(
        1,
        &[(
            0x80,
            "",
            mftmirr_c,
            Some(mftmirr_lcn),
            mftmirr_c * CLUSTER,
            mftmirr_c * CLUSTER,
        )],
    );
    let rec2 = patch(
        2,
        &[(
            0x80,
            "",
            log_c,
            Some(log_lcn),
            log_c * CLUSTER,
            log_c * CLUSTER,
        )],
    );
    // Record 3 ($Volume): keep all-resident, but set the label.
    let rec3 = {
        let mut attrs = parse_attrs(record_template(3));
        if let Some(lbl) = label {
            for a in attrs.iter_mut() {
                if a.atype == 0x60 {
                    set_volume_name(&mut a.bytes, lbl);
                }
            }
        }
        build_record(record_template(3), &attrs)
    };
    let rec4 = patch(
        4,
        &[(
            0x80,
            "",
            attrdef_c,
            Some(attrdef_lcn),
            attrdef_c * CLUSTER,
            ATTRDEF.len() as u64,
        )],
    );
    let rec5 = patch(
        5,
        &[
            (
                0x50,
                "",
                rootsd_c,
                Some(rootsd_lcn),
                rootsd_c * CLUSTER,
                ROOT_SECDESC.len() as u64,
            ),
            (
                0xA0,
                "$I30",
                rootidx_c,
                Some(rootidx_lcn),
                rootidx_c * CLUSTER,
                ROOT_INDEX.len() as u64,
            ),
        ],
    );
    let rec6 = patch(
        6,
        &[(0x80, "", bm_c, Some(bm_lcn), bm_c * CLUSTER, bm_real)],
    );
    let rec7 = patch(7, &[(0x80, "", boot_c, Some(0), boot_c * CLUSTER, 8192)]);
    let rec8 = patch(8, &[(0x80, "$Bad", tc, None, tc * CLUSTER, tc * CLUSTER)]);
    let rec9 = patch(
        9,
        &[(
            0x80,
            "$SDS",
            sds_c,
            Some(sds_lcn),
            sds_c * CLUSTER,
            SECURE_SDS.len() as u64,
        )],
    );
    let rec10 = patch(
        10,
        &[(
            0x80,
            "",
            upcase_c,
            Some(upcase_lcn),
            upcase_c * CLUSTER,
            UPCASE.len() as u64,
        )],
    );
    let rec11 = build_record(record_template(11), &parse_attrs(record_template(11)));

    recs.push(rec0);
    recs.push(rec1);
    recs.push(rec2);
    recs.push(rec3);
    recs.push(rec4);
    recs.push(rec5);
    recs.push(rec6);
    recs.push(rec7);
    recs.push(rec8);
    recs.push(rec9);
    recs.push(rec10);
    recs.push(rec11);
    for n in 12..16 {
        let mut r = [0u8; REC];
        r.copy_from_slice(record_template(n));
        fixup_write(&mut r);
        recs.push(r);
    }
    while (recs.len() as u64) < mft_records {
        recs.push(blank_record());
    }

    // ---- Boot sector (reuse golden boot region, patch geometry) ----
    let mut boot = BOOT.to_vec();
    wu64(&mut boot, 0x28, tc * SPC - 1); // total sectors (last sector = backup boot)
    wu64(&mut boot, 0x30, mft_lcn);
    wu64(&mut boot, 0x38, mftmirr_lcn);

    // ---- Cluster allocation bitmap (bit set = in use) ----
    let mut bm = vec![0u8; (cdiv(tc, 8)) as usize];
    for c in 0..end_meta {
        bm[(c / 8) as usize] |= 1 << (c % 8);
    }
    // Mark the padding bits past the volume end as allocated (NTFS convention).
    for c in tc..(bm.len() as u64 * 8) {
        bm[(c / 8) as usize] |= 1 << (c % 8);
    }

    // ---- MFT record bitmap (records 0-23 reserved) ----
    let mut mftbm = vec![0u8; cdiv(mft_records, 8) as usize];
    for i in 0..24u64 {
        mftbm[(i / 8) as usize] |= 1 << (i % 8);
    }
    for c in mft_records..(mftbm.len() as u64 * 8) {
        mftbm[(c / 8) as usize] |= 1 << (c % 8);
    }

    // ---- Emit ----
    let put = |target: &mut W, lcn0: u64, data: &[u8]| -> Result<(), FilesystemError> {
        target.seek(SeekFrom::Start(lcn0 * CLUSTER))?;
        target.write_all(data)?;
        Ok(())
    };
    // Pre-size so trailing free space reads back as zero.
    target.seek(SeekFrom::Start(target_size - 1))?;
    target.write_all(&[0u8])?;

    put(target, 0, &boot)?;
    put(target, mftbm_lcn, &mftbm)?;
    let mut mft_data = Vec::with_capacity(recs.len() * REC);
    for r in &recs {
        mft_data.extend_from_slice(r);
    }
    put(target, mft_lcn, &mft_data)?;
    let mut mirror = Vec::with_capacity(4 * REC);
    for r in recs.iter().take(4) {
        mirror.extend_from_slice(r);
    }
    put(target, mftmirr_lcn, &mirror)?;
    // $LogFile: 0xFF-filled (ntfs-3g treats it as needing restart and resets it).
    let logfile = vec![0xFFu8; (log_c * CLUSTER) as usize];
    put(target, log_lcn, &logfile)?;
    put(target, bm_lcn, &bm)?;
    put(target, attrdef_lcn, ATTRDEF)?;
    put(target, upcase_lcn, UPCASE)?;
    put(target, sds_lcn, SECURE_SDS)?;
    put(target, rootsd_lcn, ROOT_SECDESC)?;
    put(target, rootidx_lcn, ROOT_INDEX)?;
    // Backup boot sector at the very last sector.
    target.seek(SeekFrom::Start((tc * SPC - 1) * BPS))?;
    target.write_all(&boot[..BPS as usize])?;
    target.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
    use crate::fs::ntfs::NtfsFilesystem;
    use std::io::Cursor;

    #[test]
    fn blank_ntfs_opens_and_is_writable_by_our_reader() {
        let size = 32 * 1024 * 1024u64;
        let mut cur = Cursor::new(Vec::<u8>::new());
        create_blank_ntfs(&mut cur, size, 128, Some("RBNTFS")).expect("format");
        assert_eq!(cur.get_ref().len() as u64, size);

        cur.seek(SeekFrom::Start(0)).unwrap();
        let mut fs = NtfsFilesystem::open(cur, 0).expect("our reader opens the blank");
        assert_eq!(fs.volume_label(), Some("RBNTFS"));
        let root = fs.root().unwrap();
        assert!(
            fs.list_directory(&root).unwrap().is_empty(),
            "fresh volume has an empty root"
        );

        // A multi-cluster (non-resident) file round-trips — the case the clone
        // needs and the old packer corrupted.
        let data = vec![0x33u8; 10_000];
        let mut src = Cursor::new(data.clone());
        fs.create_file(
            &root,
            "data.bin",
            &mut src,
            data.len() as u64,
            &CreateFileOptions::default(),
        )
        .expect("create_file into the blank");
        EditableFilesystem::sync_metadata(&mut fs).unwrap();

        let entries = fs.list_directory(&root).unwrap();
        let f = entries
            .iter()
            .find(|e| e.name == "data.bin")
            .expect("file present after sync");
        assert_eq!(fs.read_file(f, f.size as usize).unwrap(), data);
    }
}
