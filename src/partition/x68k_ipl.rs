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

/// Phase B IPL "print" stub — clears the screen, prints a status banner
/// telling the user the HDD is from rusty-backup, then halts. The IPL ROM
/// chains in, the user sees the banner and knows their HDD is recognised,
/// then manually resets to FDD0 to boot Human68k.
///
/// Improves over [`IPL_HALT_STUB`] purely on the UX side — same "needs
/// FDD0" outcome, but with visible confirmation that the chain worked.
/// Phase D's `--system-disk` flag adds the actual self-boot.
///
/// 68000 assembly equivalent (clean-room — written from the public
/// IOCS function reference at <https://gamesx.com/wiki/doku.php?id=x68000:trap_codes>,
/// not transcribed from Sharp / Hudson IPL bytes):
///
/// ```text
/// entry:                                        ; byte 0
///     bra.s     fall_through                    ; 60 02 (satisfies IPL ROM's "byte 0 == $60" check)
///     dc.b      "RB"                            ; 0x52 0x42 ("rusty-backup" tag)
/// fall_through:                                 ; byte 4
///     lea.l     msg(pc), a1                     ; 43 FA NN NN (PC-relative load of msg)
///     moveq.l   #$21, d0                        ; 70 21  (IOCS B_PRINT function code)
///     trap      #15                             ; 4E 4F  (IOCS dispatch)
/// halt:                                         ; byte 12
///     bra.s     halt                            ; 60 FE  (infinite loop)
/// msg:                                          ; byte 14
///     dc.b      "\x1B[2J\x1B[10;28HRusty Backup X68000 HDD\r\n"
///     dc.b      "\x1B[12;26HBoot Human68k from FDD0 to mount as C:\r\n"
///     dc.b      0
/// ```
///
/// The IOCS B_PRINT call (function `$21`) takes a null-terminated string
/// pointer in A1 and writes it to the text console; ANSI escape sequences
/// for clear-screen (`ESC [ 2 J`) and cursor positioning (`ESC [ row ; col H`)
/// are interpreted directly by the IPL ROM's console driver.
pub fn build_print_ipl_stub() -> Vec<u8> {
    build_print_ipl_stub_with_partitions(&[])
}

/// Build the Phase B print stub with a partition list baked into the
/// banner text. Each entry in `partitions` is rendered as a row like
/// `"  C: Human68k 99 MiB"` so users with multi-partition HDDs see the
/// actual partition layout when they boot — distinct from a single-
/// partition HDD's "boot from FDD0" footer.
///
/// `partitions` is `(drive_letter, size_mib)` pairs in slot order. Empty
/// slice falls back to the single-partition banner (equivalent to
/// [`build_print_ipl_stub`]).
///
/// The rendered text fits in the boot block (≤ ~900 bytes for SASI,
/// ≤ ~2800 bytes for SCSI), so up to 8 partitions render cleanly.
///
/// 68k code stays the same shape as the single-partition variant — only
/// the baked text changes. No runtime X68K table parsing in 68k, which
/// keeps the verification footprint small (we'd need an interactive
/// MAME oracle to safely debug 68k table-walking code, and we don't
/// have one in headless CI).
pub fn build_print_ipl_stub_with_partitions(partitions: &[(char, u64)]) -> Vec<u8> {
    // Message body — ANSI escape sequences are interpreted by the IPL
    // ROM's console driver, so we get clear-screen + cursor positioning
    // without writing any framebuffer code ourselves.
    let mut msg: Vec<u8> = Vec::new();
    msg.extend_from_slice(b"\x1B[2J"); // clear screen
    msg.extend_from_slice(b"\x1B[3;28H"); // cursor row 3, col 28
    msg.extend_from_slice(b"Rusty Backup X68000 HDD\r\n");

    if partitions.is_empty() {
        // Single-partition / unspecified: legacy banner.
        msg.extend_from_slice(b"\x1B[5;26H"); // cursor row 5, col 26
        msg.extend_from_slice(b"Boot Human68k from FDD0 to mount as C:\r\n");
    } else {
        // Multi-partition: list each slot with its drive letter + size.
        let line = format!(
            "\x1B[5;28HX68K partition table -- {} slots:\r\n",
            partitions.len()
        );
        msg.extend_from_slice(line.as_bytes());
        for (i, (letter, size_mib)) in partitions.iter().enumerate() {
            // Row 7 + i, col 30
            let line = format!(
                "\x1B[{};30H  {}: Human68k {} MiB\r\n",
                7 + i,
                letter,
                size_mib,
            );
            msg.extend_from_slice(line.as_bytes());
        }
        let footer_row = 7 + partitions.len() + 2;
        let line = format!(
            "\x1B[{};26HBoot Human68k from FDD0 to mount as drives.\r\n",
            footer_row,
        );
        msg.extend_from_slice(line.as_bytes());
    }
    msg.push(0); // null terminator for B_PRINT

    // Code prelude — exactly 14 bytes so msg lands at byte 14, which makes
    // the LEA's PC-relative displacement = 14 - (4 + 2) = 8 (PC during the
    // LEA's second word is at byte 6).
    let mut code = vec![
        0x60, 0x02, // bra.s fall_through (jump to byte 4)
        b'R', b'B', // 2-byte "RB" tag (could be version / magic in future)
        0x43, 0xFA, 0x00, 0x08, // lea msg(pc), a1 — disp=8, PC=6, target=14
        0x70, 0x21, // moveq #$21, d0 — IOCS B_PRINT function code
        0x4E, 0x4F, // trap #15 — IOCS dispatch
        0x60, 0xFE, // bra.s halt (loop at byte 12)
    ];
    code.extend_from_slice(&msg);
    code
}

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

/// Which IPL stub variant to write at byte 0 (SASI) / byte `0x400` (SCSI).
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum IplStub {
    /// 2-byte `BRA.S self` halt loop. Smallest possible stub that the IPL
    /// ROM accepts. No visible output — used as the safe default and by
    /// Phase A.
    Halt,
    /// Clear-screen + printed banner via IOCS B_PRINT, then halt.
    /// The boot block builder renders a single-partition default
    /// banner. For a per-partition list, use [`render_ipl_stub_bytes`]
    /// directly with [`build_print_ipl_stub_with_partitions`].
    Print,
}

/// Build the actual byte sequence for an IPL stub. Wrapper around the
/// raw stub builders that lets the caller pass partition info for the
/// Print variant — when `partitions` is empty, falls back to the
/// generic single-partition banner.
pub fn render_ipl_stub_bytes(stub: IplStub, partitions: &[(char, u64)]) -> Vec<u8> {
    match stub {
        IplStub::Halt => IPL_HALT_STUB.to_vec(),
        IplStub::Print => build_print_ipl_stub_with_partitions(partitions),
    }
}

/// Build a SASI boot block: byte-0 IPL stub + zero padding + X68K
/// partition table at byte `0x400`. The returned buffer is exactly
/// [`SASI_BOOT_BLOCK_BYTES`] long and ready to be written at byte 0
/// of the SASI HDD image.
///
/// `partition_table_bytes` must be the 144-byte block produced by
/// [`crate::partition::x68k::X68kPartitionTable::to_bytes`].
pub fn build_sasi_boot_block(
    stub: IplStub,
    partition_table_bytes: &[u8; X68K_TABLE_HEADER_SIZE + X68K_MAX_PARTITIONS * X68K_ENTRY_SIZE],
) -> [u8; SASI_BOOT_BLOCK_BYTES] {
    build_sasi_boot_block_with_stub_bytes(&render_ipl_stub_bytes(stub, &[]), partition_table_bytes)
}

/// Like [`build_sasi_boot_block`], but accepts a pre-rendered IPL stub
/// byte sequence — lets the caller pass partition info into the Print
/// stub via [`render_ipl_stub_bytes`].
pub fn build_sasi_boot_block_with_stub_bytes(
    stub_bytes: &[u8],
    partition_table_bytes: &[u8; X68K_TABLE_HEADER_SIZE + X68K_MAX_PARTITIONS * X68K_ENTRY_SIZE],
) -> [u8; SASI_BOOT_BLOCK_BYTES] {
    let mut block = [0u8; SASI_BOOT_BLOCK_BYTES];
    // Byte 0: IPL stub. The IPL ROM JSRs here after reading the boot
    // block; the chosen stub either halts or prints a banner + halts.
    assert!(
        stub_bytes.len() <= SASI_TABLE_OFFSET,
        "IPL stub ({} bytes) overlaps the SASI partition table at 0x{:x}",
        stub_bytes.len(),
        SASI_TABLE_OFFSET,
    );
    block[..stub_bytes.len()].copy_from_slice(stub_bytes);
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
    stub: IplStub,
    partition_table_bytes: &[u8; X68K_TABLE_HEADER_SIZE + X68K_MAX_PARTITIONS * X68K_ENTRY_SIZE],
) -> [u8; SCSI_BOOT_BLOCK_BYTES] {
    build_scsi_boot_block_with_stub_bytes(
        descriptor,
        &render_ipl_stub_bytes(stub, &[]),
        partition_table_bytes,
    )
}

/// Like [`build_scsi_boot_block`], but accepts a pre-rendered IPL stub
/// byte sequence — lets the caller pass partition info into the Print
/// stub via [`render_ipl_stub_bytes`].
pub fn build_scsi_boot_block_with_stub_bytes(
    descriptor: &[u8; 8],
    stub_bytes: &[u8],
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
    // Byte 0x400: IPL stub. The SCSI variant places the IPL after the
    // signature header rather than at byte 0.
    assert!(
        SCSI_IPL_OFFSET + stub_bytes.len() <= SCSI_TABLE_OFFSET,
        "IPL stub ({} bytes) at 0x{:x} overlaps the SCSI partition table at 0x{:x}",
        stub_bytes.len(),
        SCSI_IPL_OFFSET,
        SCSI_TABLE_OFFSET,
    );
    block[SCSI_IPL_OFFSET..SCSI_IPL_OFFSET + stub_bytes.len()].copy_from_slice(stub_bytes);
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
        let block = build_sasi_boot_block(IplStub::Halt, &table);
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
        let block = build_scsi_boot_block(&descriptor, IplStub::Halt, &table);
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

    #[test]
    fn print_stub_with_partitions_renders_drive_letters_and_sizes() {
        // Multi-partition build: each slot gets a drive letter + size.
        let partitions = [('C', 16u64), ('D', 16), ('E', 16)];
        let stub = build_print_ipl_stub_with_partitions(&partitions);
        // Prelude is still 14 bytes (same shape as single-partition).
        assert_eq!(&stub[..4], &[0x60, 0x02, b'R', b'B']);
        // Banner header should mention X68000 HDD.
        assert!(stub.windows(23).any(|w| w == b"Rusty Backup X68000 HDD"));
        // Each drive letter + size combo should appear in the message.
        assert!(stub.windows(15).any(|w| w == b"C: Human68k 16 "));
        assert!(stub.windows(15).any(|w| w == b"D: Human68k 16 "));
        assert!(stub.windows(15).any(|w| w == b"E: Human68k 16 "));
        // The "3 slots:" header tells the user there are 3 partitions.
        assert!(stub.windows(8).any(|w| w == b"3 slots:"));
    }

    #[test]
    fn print_stub_starts_with_bra_and_satisfies_ipl_check() {
        let stub = build_print_ipl_stub();
        // Byte 0 must be $60 (BRA opcode) — the Sharp IPL ROM checks
        // `cmpi.b #$60, (a0)` at iplrom30.s:4181 before JSRing to our code.
        assert_eq!(stub[0], 0x60);
        // The BRA.S +2 lands at byte 4, where the LEA / MOVEQ / TRAP /
        // BRA.S-halt sequence lives.
        assert_eq!(stub[1], 0x02);
        // LEA msg(pc), a1 at byte 4 with displacement 8 (msg lives at
        // byte 14 = (4 + 2) + 8).
        assert_eq!(&stub[4..8], &[0x43, 0xFA, 0x00, 0x08]);
        // MOVEQ #$21, D0 = IOCS B_PRINT
        assert_eq!(&stub[8..10], &[0x70, 0x21]);
        // TRAP #15 = IOCS dispatch
        assert_eq!(&stub[10..12], &[0x4E, 0x4F]);
        // BRA.S self halt at byte 12
        assert_eq!(&stub[12..14], &[0x60, 0xFE]);
        // Message starts at byte 14 with ANSI clear-screen sequence
        assert_eq!(&stub[14..18], b"\x1B[2J");
        // Message ends with a null terminator (B_PRINT contract)
        assert_eq!(*stub.last().unwrap(), 0);
        // Recognisable banner text appears somewhere in the message body
        assert!(stub.windows(23).any(|w| w == b"Rusty Backup X68000 HDD"));
    }

    #[test]
    fn sasi_block_with_print_stub_keeps_table_at_0x400() {
        // The print stub is ~100 bytes — much larger than the halt stub,
        // but still well under the 0x400 table offset, so the SASI block
        // layout stays sound.
        let mut table = [0u8; X68K_TABLE_HEADER_SIZE + X68K_MAX_PARTITIONS * X68K_ENTRY_SIZE];
        table[0..4].copy_from_slice(b"X68K");
        let block = build_sasi_boot_block(IplStub::Print, &table);
        assert_eq!(block.len(), SASI_BOOT_BLOCK_BYTES);
        assert_eq!(block[0], 0x60); // BRA opcode
        assert_eq!(&block[SASI_TABLE_OFFSET..SASI_TABLE_OFFSET + 4], b"X68K");
    }

    #[test]
    fn scsi_block_with_print_stub_keeps_table_at_0x800() {
        let mut table = [0u8; X68K_TABLE_HEADER_SIZE + X68K_MAX_PARTITIONS * X68K_ENTRY_SIZE];
        table[0..4].copy_from_slice(b"X68K");
        let descriptor = [0x02, 0x00, 0x00, 0x1d, 0xaf, 0xff, 0x01, 0x00];
        let block = build_scsi_boot_block(&descriptor, IplStub::Print, &table);
        // Byte 0 still the SCSI signature
        assert_eq!(&block[0..8], X68K_SCSI_SIGNATURE);
        // IPL at byte 0x400 starts with BRA opcode
        assert_eq!(block[SCSI_IPL_OFFSET], 0x60);
        // Table still at byte 0x800
        assert_eq!(&block[SCSI_TABLE_OFFSET..SCSI_TABLE_OFFSET + 4], b"X68K");
    }
}
