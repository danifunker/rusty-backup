//! Bootable X68000 HDD assembly — IPL stubs + boot-block layout.
//!
//! This is the write side of [`crate::partition::x68k`]: how to construct
//! a sector-0 boot block that the Sharp X68000 IPL ROM will recognise as
//! a bootable HDD and chain into.
//!
//! ## Boot-block conventions
//!
//! The Sharp IPL ROM (`x68kd11s/iplrom/iplrom30.s` reference disasm)
//! detects two distinct bootable-HDD shapes. Both leave byte 0 readable
//! as 68000 machine code that the IPL ROM JMPs into after the device-
//! type detection passes:
//!
//! | Variant | Byte-0 shape                              | Table offset | Sector size |
//! |---------|-------------------------------------------|--------------|-------------|
//! | SASI    | `60 00 00 NN` (BRA.W +NN) or `60 FE` (BRA.S self) — no fixed signature; IPL code starts at the BRA target | `0x400` (sector 4 @ 256 B) | 256 B |
//! | SCSI    | `X68SCSI1` + 8-byte geometry descriptor + 40-byte Keisoku Giken ASCII string + IPL code at byte `0x400` | `0x800` (sector 2 @ 1024 B) | 1024 B |
//!
//! Real anchors used to derive these conventions:
//! - `Bomberman.hdf` (SASI, 10 MB, 256-B sectors): byte 0 = `60 00 00 ca`
//!   (BRA.W to byte 0xCA where the IPL menu code lives), table at 0x400.
//! - `SCSI_NetBSD.hds` (SCSI, 996 MB, 1024-B sectors): byte 0 = `X68SCSI1`
//!   signature header, IPL menu block at 0x400, X68K table at 0x800.
//!
//! ## What's in here
//!
//! Phase A: [`IPL_HALT_STUB`] — a 2-byte `BRA.S self` 68k stub. Enough
//! to satisfy the IPL ROM's "byte 0 is executable" check; the chain
//! lands in an infinite loop. Useful as a "minimum viable" HDD shape
//! that proves the IPL accepts our boot block; the user resets to FDD0
//! to actually bring Human68k up.
//!
//! Phase B (TBD): a clean-room IPL menu — ANSI-rendered partition
//! selector with arrow-key navigation, chains to the selected partition's
//! boot sector via the IOCS B_READ call.
//!
//! Phase C (TBD): SCSI variant. Same IPL code body, but byte 0 carries
//! the `X68SCSI1` signature + descriptor, and the IPL menu sits at byte
//! `0x400` instead of byte 0.

use crate::partition::x68k::{
    X68K_ENTRY_SIZE, X68K_MAX_PARTITIONS, X68K_SCSI_SIGNATURE, X68K_TABLE_HEADER_SIZE,
};

/// Size of the boot block that the IPL ROM reads + executes on a SASI HDD,
/// in bytes. Spans byte 0 (IPL code entry) up to and including the X68K
/// partition table at byte `0x400..0x490`, rounded up to a 256-byte sector
/// boundary.
pub const SASI_BOOT_BLOCK_BYTES: usize = 0x500;

/// Size of the boot block on a SCSI HDD (signature header + IPL menu +
/// X68K table at byte `0x800`). 1024-B-sector aligned.
pub const SCSI_BOOT_BLOCK_BYTES: usize = 0xC00;

/// Byte offset where the X68k partition table lives on a SASI HDD.
pub const SASI_TABLE_OFFSET: usize = 0x400;

/// Byte offset where the X68k partition table lives on a SCSI HDD.
pub const SCSI_TABLE_OFFSET: usize = 0x800;

/// Byte offset where the IPL boot code lives on a SCSI HDD (after the
/// 64-byte signature header at byte 0).
pub const SCSI_IPL_OFFSET: usize = 0x400;

/// Minimal IPL "halt" stub — a 2-byte `BRA.S self` instruction that loops
/// forever when chained to. Sufficient for Phase A: the Sharp IPL ROM's
/// byte-0 check sees a valid 68000 BRA opcode, loads + JMPs the boot
/// block, and our code halts. The HDD is recognised as a bootable
/// device, but doesn't actually bring up Human68k — the user resets to
/// FDD0 to do that.
///
/// Hand-encoded:
/// - `60 FE` — `BRA.S (PC - 2)` (i.e. branch back to this instruction)
pub const IPL_HALT_STUB: &[u8] = &[0x60, 0xFE];

/// 8-byte SASI HDD geometry descriptor for the `\x82w68000W` signature.
/// Cribbed from `iplrom30.s:Lff09e8` (the IPL ROM's default empty-SASI
/// descriptor template). Only relevant when emitting the "empty marker"
/// SASI shape (signature at byte 0 with no IPL code) — the self-bootable
/// SASI path skips the signature entirely.
///
/// Layout (per the IPL ROM disasm):
/// ```text
/// 0x00..0x02  $40 $00 — cylinder count high / low ($4000 = 16384)
/// 0x02..0x04  $00 $00 — reserved
/// 0x04..0x06  $fc $00 — head count + sector count ($fc = 252, $00 = ?)
/// 0x06..0x08  $00 $00 — reserved
/// ```
pub const SASI_EMPTY_GEOMETRY_BYTES: [u8; 8] = [0x40, 0x00, 0x00, 0x00, 0xfc, 0x00, 0x00, 0x00];

/// Build a SASI boot block: byte-0 IPL stub + zero padding + X68K
/// partition table at byte `0x400`. The returned buffer is exactly
/// [`SASI_BOOT_BLOCK_BYTES`] long and ready to be written at byte 0
/// of the SASI HDD image.
///
/// `partition_table_bytes` must be the 144-byte block produced by
/// [`crate::partition::x68k::X68kPartitionTable::to_bytes`].
pub fn build_sasi_boot_block(
    partition_table_bytes: &[u8; X68K_TABLE_HEADER_SIZE + X68K_MAX_PARTITIONS * X68K_ENTRY_SIZE],
) -> [u8; SASI_BOOT_BLOCK_BYTES] {
    let mut block = [0u8; SASI_BOOT_BLOCK_BYTES];
    // Byte 0: minimal IPL stub. The IPL ROM JMPs here after reading the
    // boot block; we BRA.S to self and halt.
    block[..IPL_HALT_STUB.len()].copy_from_slice(IPL_HALT_STUB);
    // Byte 0x400: partition table (144 bytes, padded with zeros to the
    // end of the boot block).
    block[SASI_TABLE_OFFSET..SASI_TABLE_OFFSET + partition_table_bytes.len()]
        .copy_from_slice(partition_table_bytes);
    block
}

/// Build a SCSI boot block: `X68SCSI1` signature + descriptor at byte 0,
/// IPL stub at byte `0x400`, X68K partition table at byte `0x800`. The
/// returned buffer is exactly [`SCSI_BOOT_BLOCK_BYTES`] long.
///
/// `descriptor` must be the 8-byte geometry / capacity descriptor that
/// follows the signature. The 40-byte Keisoku Giken ASCII string at
/// byte 0x10 is filled in automatically.
pub fn build_scsi_boot_block(
    descriptor: &[u8; 8],
    partition_table_bytes: &[u8; X68K_TABLE_HEADER_SIZE + X68K_MAX_PARTITIONS * X68K_ENTRY_SIZE],
) -> [u8; SCSI_BOOT_BLOCK_BYTES] {
    let mut block = [0u8; SCSI_BOOT_BLOCK_BYTES];
    // Bytes 0..8: X68SCSI1 magic.
    block[..8].copy_from_slice(X68K_SCSI_SIGNATURE);
    // Bytes 8..16: caller-provided geometry descriptor.
    block[8..16].copy_from_slice(descriptor);
    // Bytes 16..56: Keisoku Giken ASCII string. Real disks (Hudson,
    // BlueSCSI, ZuluSCSI, NetBSD-prepared) all carry this exact text.
    let ksg = b"Human68K SCSI-DISK by Keisoku Giken\0\0\0\0\0";
    block[16..16 + ksg.len()].copy_from_slice(ksg);
    // Byte 0x400: IPL stub. Same minimal halt-loop as SASI for Phase A.
    block[SCSI_IPL_OFFSET..SCSI_IPL_OFFSET + IPL_HALT_STUB.len()].copy_from_slice(IPL_HALT_STUB);
    // Byte 0x800: partition table.
    block[SCSI_TABLE_OFFSET..SCSI_TABLE_OFFSET + partition_table_bytes.len()]
        .copy_from_slice(partition_table_bytes);
    block
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn halt_stub_is_bra_s_self() {
        // 68000: BRA.S with displacement -2 (jumps back to itself).
        // Encoding: opcode 0x60 | (8-bit signed displacement 0xFE).
        // The branch displacement is from PC+2 (i.e. after the BRA word),
        // so $FE = -2 lands back at the BRA instruction itself.
        assert_eq!(IPL_HALT_STUB, &[0x60, 0xFE]);
    }

    #[test]
    fn sasi_block_has_ipl_at_byte_0_and_table_at_0x400() {
        let mut table = [0u8; X68K_TABLE_HEADER_SIZE + X68K_MAX_PARTITIONS * X68K_ENTRY_SIZE];
        // Sprinkle a recognisable byte so we can locate the table in the block.
        table[0] = b'X';
        table[1] = b'6';
        table[2] = b'8';
        table[3] = b'K';
        let block = build_sasi_boot_block(&table);
        assert_eq!(block.len(), SASI_BOOT_BLOCK_BYTES);
        assert_eq!(&block[0..2], IPL_HALT_STUB);
        // Padding between the IPL stub and the table is all zeros.
        assert!(block[2..SASI_TABLE_OFFSET].iter().all(|&b| b == 0));
        // Table magic at byte 0x400.
        assert_eq!(&block[SASI_TABLE_OFFSET..SASI_TABLE_OFFSET + 4], b"X68K");
    }

    #[test]
    fn scsi_block_has_signature_descriptor_and_table() {
        let mut table = [0u8; X68K_TABLE_HEADER_SIZE + X68K_MAX_PARTITIONS * X68K_ENTRY_SIZE];
        table[0..4].copy_from_slice(b"X68K");
        let descriptor = [0x02, 0x00, 0x00, 0x1d, 0xaf, 0xff, 0x01, 0x00];
        let block = build_scsi_boot_block(&descriptor, &table);
        assert_eq!(block.len(), SCSI_BOOT_BLOCK_BYTES);
        // Byte 0: X68SCSI1 magic.
        assert_eq!(&block[0..8], X68K_SCSI_SIGNATURE);
        // Byte 8: descriptor (matches the NetBSD reference fixture).
        assert_eq!(&block[8..16], &descriptor);
        // Byte 16: Keisoku Giken ASCII string.
        assert_eq!(&block[16..51], b"Human68K SCSI-DISK by Keisoku Giken");
        // Byte 0x400: IPL halt stub.
        assert_eq!(&block[SCSI_IPL_OFFSET..SCSI_IPL_OFFSET + 2], IPL_HALT_STUB);
        // Byte 0x800: partition table.
        assert_eq!(&block[SCSI_TABLE_OFFSET..SCSI_TABLE_OFFSET + 4], b"X68K");
    }
}
