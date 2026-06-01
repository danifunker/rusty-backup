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

use super::ag::XfsAgf;
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
        self.run_repair()
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
