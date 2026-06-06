//! Build a minimal Human68k HDD image for inspection / round-trip
//! testing through our engine. Mirrors the test-helper pattern in
//! `tests/x68000_resize.rs`: X68k SASI partition table at byte 2048 +
//! one Human-tagged FAT12 partition body starting at sector 64,
//! seeded with a handful of plain-text files.
//!
//! Usage:
//!
//! ```text
//! cargo run --example build_x68k_hdd -- <out.vhd> [size_mib]
//! ```
//!
//! Default size is 8 MiB.
//!
//! ## NOT-MiSTer-mountable — known format gaps
//!
//! As of 2026-06-05, the file this example produces is **not
//! mountable by the real MiSTer X68000 core**. Probing real Sharp
//! HDD images (`Bomberman.hdf`, `hd0.hds`) + reading the disassembled
//! IPL ROM (`../x68kd11s/iplrom/iplrom30.s`) showed three concrete
//! divergences from real Sharp SASI/SCSI HDDs that
//! `partition/x68k.rs` doesn't yet implement:
//!
//! 1. **No byte-0 Sharp signature.** Real SASI HDDs start with
//!    `\x82w68000W` at byte 0; real SCSI HDDs start with `X68SCSI1`.
//!    The IPL ROM's signature check is at `iplrom30.s:16420`
//!    (`cmpi.l #$53435349,(-$0014,a0)`). Our file starts with zeros
//!    at byte 0, so the IPL ROM rejects it.
//!
//! 2. **Wrong sector-size assumption.** This example computes the
//!    partition body offset as `start_lba * 512`. Real SASI HDDs use
//!    256-B sectors; real SCSI HDDs use 1024-B sectors. Even if the
//!    signature check passed, the FAT12 BPB would land at the wrong
//!    physical offset.
//!
//! 3. **No IPL menu / boot code.** Real HDDs that boot themselves
//!    start with a 68000 BSR opcode at byte 0 jumping to a partition
//!    selector. Our file has no boot code, so even if Human68k saw
//!    the HDD it'd need a system floppy in FDD0 to boot.
//!
//! See OPEN-WORK §8 "X68000 SASI / SCSI HDD format gaps" for the
//! full reference set (iplrom30.s line numbers, dis68k libfat hints,
//! real-image byte probes) and the proposed fix sequencing. The
//! example stays in tree as a starting scaffold: the partition-table
//! emit + Human68k engine call are correct, only the byte-0
//! signature, sector-size derivation, and IPL boot code are missing.
//!
//! Today the example still serves a purpose host-side:
//! `rb-cli inspect <out.vhd>` and `rb-cli ls <out.vhd>@1` round-trip
//! through the same engine paths that `tests/x68000_resize.rs`
//! exercises, so this is the simplest way to scaffold a synthetic
//! X68K-partition / Human68k disk for ad-hoc debugging.

use std::env;
use std::fs;
use std::io::Cursor;

use byteorder::{BigEndian, ByteOrder};

use rusty_backup::fs::fat::create_blank_fat;
use rusty_backup::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
use rusty_backup::fs::human68k::Human68kFilesystem;
use rusty_backup::partition::x68k::{X68K_MAGIC, X68K_TABLE_OFFSET};

const SECTOR_SIZE: u64 = 512;
const PART_START_SECTOR: u64 = 64;

fn main() {
    let mut args = env::args().skip(1);
    let out_path = args
        .next()
        .expect("usage: build_x68k_hdd <out.vhd> [size_mib]");
    let size_mib: u64 = args
        .next()
        .map(|s| s.parse().expect("size_mib must be integer"))
        .unwrap_or(8);

    // Partition body in bytes.
    let part_bytes_target = size_mib * 1024 * 1024 - (PART_START_SECTOR * SECTOR_SIZE);

    // mkfs the partition as blank FAT12 with X68000-friendly geometry.
    let label = Some("RBHDD");
    let mut body =
        create_blank_fat(part_bytes_target, label).expect("create_blank_fat on partition body");

    // Seed a few text files inside it via the Human68k engine so the
    // user can see them from `dir C:`.
    {
        let mut fs = Human68kFilesystem::open(Cursor::new(&mut body), 0)
            .expect("open blank partition as Human68k");
        let root = fs.root().expect("root");
        let seeds: &[(&str, &[u8])] = &[
            (
                "HELLO.TXT",
                b"hello from rusty-backup\r\n\
                  This file lives on the synthetic X68000 SASI HDD that\r\n\
                  rusty-backup's examples/build_x68k_hdd built.\r\n",
            ),
            (
                "MISTER.TXT",
                b"rusty-backup mister wave-2 manual verification\r\n",
            ),
            (
                "README.TXT",
                b"To verify on the X68000 MiSTer core:\r\n\
                  1. Boot Human68k from FD0 (BLANK_disk_X68000.D88).\r\n\
                  2. At the A> prompt, type: dir C:\r\n\
                  3. You should see HELLO.TXT, MISTER.TXT, README.TXT.\r\n\
                  4. Type: type C:HELLO.TXT  (or 'less', 'cat', etc.)\r\n",
            ),
        ];
        for (name, data) in seeds {
            let mut reader: &[u8] = data;
            fs.create_file(
                &root,
                name,
                &mut reader,
                data.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap_or_else(|e| panic!("create_file {name}: {e}"));
        }
        fs.sync_metadata().expect("sync");
    }

    // Wrap in an X68k SASI HDD: X68k table at byte 2048 + body at sector 64.
    let part_offset = (PART_START_SECTOR * SECTOR_SIZE) as usize;
    let total = part_offset + body.len();
    let mut disk = vec![0u8; total];

    let part_sectors = body.len() as u64 / SECTOR_SIZE;
    let disk_sectors = total as u64 / SECTOR_SIZE;

    let off = X68K_TABLE_OFFSET as usize;
    BigEndian::write_u32(&mut disk[off..off + 4], X68K_MAGIC);
    BigEndian::write_u32(&mut disk[off + 4..off + 8], disk_sectors as u32);
    BigEndian::write_u32(&mut disk[off + 8..off + 12], disk_sectors as u32);

    // Slot 0: name "Human   " (8 ASCII bytes), start_lba, size_sectors.
    let e_off = off + 16;
    disk[e_off..e_off + 8].copy_from_slice(b"Human   ");
    BigEndian::write_u32(&mut disk[e_off + 8..e_off + 12], PART_START_SECTOR as u32);
    BigEndian::write_u32(&mut disk[e_off + 12..e_off + 16], part_sectors as u32);

    disk[part_offset..].copy_from_slice(&body);

    fs::write(&out_path, &disk).unwrap_or_else(|e| panic!("write {out_path}: {e}"));

    println!(
        "wrote {} ({} bytes, {} MiB)",
        out_path,
        disk.len(),
        disk.len() / (1024 * 1024)
    );
    println!(
        "  X68k table @ byte 2048, 1 Human partition @ sector {} ({} sectors, {} MiB)",
        PART_START_SECTOR,
        part_sectors,
        body.len() / (1024 * 1024)
    );
    println!("  seeded: HELLO.TXT, MISTER.TXT, README.TXT");
}
