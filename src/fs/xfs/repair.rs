//! XFS conservative repair (the write side of `docs/xfs_edit_and_repair.md`).
//!
//! This module implements `EditableFilesystem::repair` for XFS. The first
//! slice (**R4**) is the only repair that is fully self-verifiable without
//! the free-space btree rebuild: rewriting a damaged per-AG **superblock
//! replica** from the authoritative primary.
//!
//! Why R4 first: it touches nothing structural — every allocation group
//! begins with a copy of the superblock, and the geometry fields in those
//! copies must match the primary exactly. Corruption there is repaired by a
//! straight field copy, and the fix is provable by re-running the verifier
//! (and, externally, `xfs_repair -n` via `scripts/xfs-oracle.sh`).
//!
//! The editable surface's mutation methods (`create_file`,
//! `create_directory`, `delete_entry`) are **not** implemented yet — they
//! belong to the Track B edit work, which needs the §3 write primitives that
//! repair does not. They return `Unsupported` so the GUI/CLI can gate them.
//!
//! Porting reference: `xfs_repair` @ v3.1.11 `repair/agheader.c`
//! (`secondary_sb_whack`) under `~/efs-xfs-refs/xfsprogs/`.

use std::io::{Read, Seek, SeekFrom, Write};

use byteorder::{BigEndian, ByteOrder};

use super::ag::{XfsAgf, XfsAgi};
use super::sb::XfsSuperblock;
use super::{read_at_aligned, XfsFilesystem};
use crate::fs::entry::FileEntry;
use crate::fs::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, FilesystemError,
};
use crate::fs::fsck::RepairReport;

/// Geometry fields replicated into every AG superblock, as
/// `(start, end, name)` byte ranges within the sb sector. These are exactly
/// the fields the verifier's `check_sb_replica` compares; the inode-pointer
/// fields (rootino/rbmino/rsumino/quota) are intentionally excluded because
/// XFS leaves them NULLFSINO in secondaries.
const GEOM_FIELDS: &[(usize, usize, &str)] = &[
    (4, 8, "blocksize"),
    (8, 16, "dblocks"),
    (84, 88, "agblocks"),
    (88, 92, "agcount"),
    (104, 106, "inodesize"),
];

impl<R: Read + Write + Seek + Send> XfsFilesystem<R> {
    /// R4: rewrite damaged secondary-superblock geometry from the primary.
    ///
    /// Each AG (1..agcount) carries a superblock copy. For every copy whose
    /// replicated geometry fields disagree with the primary, copy those exact
    /// fields back and write the sector. Inode-pointer and counter fields are
    /// left untouched (secondaries legitimately differ there). Per-AG and
    /// independent, so a mid-run write error leaves the volume no worse than
    /// before and is re-detected on the next verifier pass.
    pub(crate) fn run_repair(&mut self) -> Result<RepairReport, FilesystemError> {
        let mut report = RepairReport {
            fixes_applied: Vec::new(),
            fixes_failed: Vec::new(),
            unrepairable_count: 0,
        };

        let sb = self.superblock().clone();
        let sectsize = sb.sectsize as u64;
        let agblocks = sb.agblocks as u64;
        let blocksize = sb.blocksize as u64;

        // Authoritative primary superblock sector (AG 0, sector 0).
        let mut primary = vec![0u8; sectsize as usize];
        read_at_aligned(
            &mut self.reader,
            self.partition_offset,
            sectsize,
            &mut primary,
        )?;

        let mut sec = vec![0u8; sectsize as usize];
        for agno in 1..sb.agcount as u64 {
            let ag_byte = self.partition_offset + agno * agblocks * blocksize;
            if read_at_aligned(&mut self.reader, ag_byte, sectsize, &mut sec).is_err() {
                report
                    .fixes_failed
                    .push(format!("AG {agno}: could not read secondary superblock"));
                continue;
            }

            // Only repair a sector that is still a parseable XFS superblock —
            // pure geometry corruption. A trashed-magic secondary needs full
            // regeneration (a later phase), so flag it unrepairable here.
            if XfsSuperblock::parse(&sec).is_err() {
                report.unrepairable_count += 1;
                report.fixes_failed.push(format!(
                    "AG {agno}: secondary superblock unreadable; needs full regeneration"
                ));
                continue;
            }

            let mut changed: Vec<&str> = Vec::new();
            for &(start, end, name) in GEOM_FIELDS {
                if sec[start..end] != primary[start..end] {
                    sec[start..end].copy_from_slice(&primary[start..end]);
                    changed.push(name);
                }
            }
            if changed.is_empty() {
                continue;
            }

            self.reader.seek(SeekFrom::Start(ag_byte))?;
            match self.reader.write_all(&sec) {
                Ok(()) => report.fixes_applied.push(format!(
                    "AG {agno}: rewrote superblock {} from primary",
                    changed.join(", ")
                )),
                Err(e) => report
                    .fixes_failed
                    .push(format!("AG {agno}: superblock write failed: {e}")),
            }
        }

        self.reader.flush()?;
        Ok(report)
    }

    /// R4b: recompute the AGF/AGI summary counters from the btrees and rewrite
    /// them if the cached values are wrong.
    ///
    /// `agf_freeblks` is recomputed by summing the free-space btree; `agi_count`
    /// and `agi_freecount` from the inode btree. These are cached summaries —
    /// the btrees themselves are authoritative — so a straight overwrite is
    /// safe and self-verifiable (re-run the verifier; xfs_repair -n agrees).
    /// Field offsets within the AGF/AGI sectors per the on-disk layout:
    /// AGF freeblks @ 52..56; AGI count @ 16..20, freecount @ 28..32.
    pub(crate) fn run_repair_summaries(&mut self) -> Result<RepairReport, FilesystemError> {
        let mut report = RepairReport {
            fixes_applied: Vec::new(),
            fixes_failed: Vec::new(),
            unrepairable_count: 0,
        };
        let sb = self.superblock().clone();
        let sectsize = sb.sectsize as u64;
        let agblocks = sb.agblocks as u64;
        let blocksize = sb.blocksize as u64;

        for agno in 0..sb.agcount as u64 {
            let expected_len = if agno == sb.agcount as u64 - 1 {
                sb.dblocks - agno * agblocks
            } else {
                agblocks
            };
            let ag_byte = self.partition_offset + agno * agblocks * blocksize;
            let agf_byte = ag_byte + sectsize;
            let agi_byte = ag_byte + 2 * sectsize;

            // --- AGF freeblks ---
            let mut agf = vec![0u8; sectsize as usize];
            if read_at_aligned(&mut self.reader, agf_byte, sectsize, &mut agf).is_ok() {
                if let Ok(parsed) = XfsAgf::parse(&agf) {
                    match self.freesp_block_total(&sb, agno, parsed.bno_root, expected_len) {
                        Ok(correct) if correct != parsed.freeblks as u64 => {
                            BigEndian::write_u32(&mut agf[52..56], correct as u32);
                            self.write_sector(agf_byte, &agf, &mut report, agno, "AGF freeblks");
                        }
                        Ok(_) => {}
                        // Tree too damaged to walk: the structural rebuild
                        // (R2) runs after R4b and will both rebuild it and set
                        // the correct summary, so this is not a failure — skip
                        // quietly rather than reporting an alarming error.
                        Err(_) => {}
                    }
                }
            }

            // --- AGI count / freecount ---
            let mut agi = vec![0u8; sectsize as usize];
            if read_at_aligned(&mut self.reader, agi_byte, sectsize, &mut agi).is_ok() {
                if let Ok(parsed) = XfsAgi::parse(&agi) {
                    match self.inode_count_freecount(&sb, agno, parsed.root, expected_len) {
                        Ok((count, freecount))
                            if count != parsed.count || freecount != parsed.freecount =>
                        {
                            BigEndian::write_u32(&mut agi[16..20], count);
                            BigEndian::write_u32(&mut agi[28..32], freecount);
                            self.write_sector(agi_byte, &agi, &mut report, agno, "AGI count");
                        }
                        Ok(_) => {}
                        // Tree too damaged to walk: R3 (inobt) runs after R4b
                        // and will rebuild it and set the correct AGI summary,
                        // so skip quietly rather than reporting a failure.
                        Err(_) => {}
                    }
                }
            }
        }
        self.reader.flush()?;
        Ok(report)
    }

    /// Write a full sector at `byte_off`, recording success/failure.
    fn write_sector(
        &mut self,
        byte_off: u64,
        data: &[u8],
        report: &mut RepairReport,
        agno: u64,
        what: &str,
    ) {
        if self.reader.seek(SeekFrom::Start(byte_off)).is_err() {
            report
                .fixes_failed
                .push(format!("AG {agno}: seek for {what} failed"));
            return;
        }
        match self.reader.write_all(data) {
            Ok(()) => report
                .fixes_applied
                .push(format!("AG {agno}: rewrote {what} from btree")),
            Err(e) => report
                .fixes_failed
                .push(format!("AG {agno}: {what} write failed: {e}")),
        }
    }

    /// Sum free blocks across all AGFs → free bytes. Used by `free_space`.
    fn free_bytes(&mut self) -> Result<u64, FilesystemError> {
        let sb = self.superblock().clone();
        let sectsize = sb.sectsize as u64;
        let agblocks = sb.agblocks as u64;
        let blocksize = sb.blocksize as u64;
        // AGF is sector 1 of each AG; read sb+AGF (2 sectors) per AG.
        let span = 2 * sectsize;
        let mut buf = vec![0u8; span as usize];
        let mut free_blocks: u64 = 0;
        for agno in 0..sb.agcount as u64 {
            let ag_byte = self.partition_offset + agno * agblocks * blocksize;
            read_at_aligned(&mut self.reader, ag_byte, span, &mut buf)?;
            let agf = XfsAgf::parse(&buf[sectsize as usize..])?;
            free_blocks += agf.freeblks as u64;
        }
        Ok(free_blocks * blocksize)
    }
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for XfsFilesystem<R> {
    fn create_file(
        &mut self,
        _parent: &FileEntry,
        _name: &str,
        _data: &mut dyn std::io::Read,
        _data_len: u64,
        _options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "XFS file creation is not implemented yet (repair-only editable surface)".into(),
        ))
    }

    fn create_directory(
        &mut self,
        _parent: &FileEntry,
        _name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "XFS directory creation is not implemented yet (repair-only editable surface)".into(),
        ))
    }

    fn delete_entry(
        &mut self,
        _parent: &FileEntry,
        _entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        Err(FilesystemError::Unsupported(
            "XFS deletion is not implemented yet (repair-only editable surface)".into(),
        ))
    }

    fn repair(&mut self) -> Result<RepairReport, FilesystemError> {
        // Order matters: R4 (secondary-superblock geometry) → R4b (AGF/AGI
        // summary counters) → R3 (inobt free-mask/freecount repair) → R5
        // (inode-core counters) → R2 (free-space btree rebuild). R3 runs after
        // R4b so its corrected AGI counters aren't re-clobbered, and before R5
        // so R5 walks the now-correct inobt. R5 only rewrites cached inode
        // counters (di_nblocks/di_nextents), touching no block ownership, so it
        // is independent of R2. R2 runs *last* because its final step resyncs
        // `sb_fdblocks` by summing every AGF's freeblks. Each phase is gated
        // hard and is a no-op on a clean volume.
        let mut report = self.run_repair()?;
        let summaries = self.run_repair_summaries()?;
        report.fixes_applied.extend(summaries.fixes_applied);
        report.fixes_failed.extend(summaries.fixes_failed);
        report.unrepairable_count += summaries.unrepairable_count;
        let inobt = self.run_inobt_repair()?;
        report.fixes_applied.extend(inobt.fixes_applied);
        report.fixes_failed.extend(inobt.fixes_failed);
        report.unrepairable_count += inobt.unrepairable_count;
        let inode_core = self.run_inode_core_repair()?;
        report.fixes_applied.extend(inode_core.fixes_applied);
        report.fixes_failed.extend(inode_core.fixes_failed);
        report.unrepairable_count += inode_core.unrepairable_count;
        let freespace = self.run_freespace_rebuild()?;
        report.fixes_applied.extend(freespace.fixes_applied);
        report.fixes_failed.extend(freespace.fixes_failed);
        report.unrepairable_count += freespace.unrepairable_count;
        // R6 (directory connectivity) runs last: it only rewrites short-form
        // directory inodes (inline data — no block-ownership change), and needs
        // R3's corrected allocated-inode state to tell dangling entries from
        // live ones.
        let dirs = self.run_dir_repair()?;
        report.fixes_applied.extend(dirs.fixes_applied);
        report.fixes_failed.extend(dirs.fixes_failed);
        report.unrepairable_count += dirs.unrepairable_count;
        // R7 (link-count half) runs after R6 so it counts the post-cleanup
        // directory graph. Orphan reconnection (the other half of R7) is not
        // implemented yet — it needs inode-allocation + directory-insertion
        // primitives — so OrphanInode stays reported-but-unreconnected.
        let nlinks = self.run_nlink_repair()?;
        report.fixes_applied.extend(nlinks.fixes_applied);
        report.fixes_failed.extend(nlinks.fixes_failed);
        report.unrepairable_count += nlinks.unrepairable_count;
        Ok(report)
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        // Repair writes through to the device immediately; just flush.
        self.reader.flush()?;
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        self.free_bytes()
    }
}
