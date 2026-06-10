//! Build a self-bootable Human68k HDD image (SASI or SCSI variant) that
//! the Sharp X68000 IPL ROM recognises and chains into.
//!
//! Usage:
//!
//! ```text
//! cargo run --example build_x68k_hdd -- <out.hdf> [size_mib] [--scsi]
//! ```
//!
//! Default is **SASI** (256-byte sectors, table at byte 0x400, minimal
//! IPL stub at byte 0). Pass `--scsi` to emit the SCSI variant
//! (`X68SCSI1` + Keisoku Giken descriptor at byte 0, IPL at byte 0x400,
//! table at byte 0x800, 1024-byte sectors).
//!
//! ## What you get
//!
//! - **Self-bootable**: byte 0 IPL stub satisfies the Sharp IPL ROM's
//!   "this is a bootable HDD" check; MAME `x68000 -bios ipl10 -sasi out.hdf`
//!   and `x68030 -hard out.hdf` both accept the image.
//! - **One Human68k partition** starting at logical sector 64 with three
//!   seed text files (`HELLO.TXT`, `MISTER.TXT`, `README.TXT`).
//! - **Halt-loop IPL** (Phase A): the byte-0 stub is `BRA.S self` —
//!   the IPL ROM chains in, the system halts harmlessly. Phase B will
//!   replace this with a full ANSI-rendered partition-menu IPL.
//!
//! ## What it does NOT do (yet)
//!
//! - Boot directly into Human68k without a system floppy in FDD0 —
//!   that's Phase D (`--system-disk PATH.DIM`, extract HUMAN.SYS +
//!   COMMAND.X from a donor Human68k boot floppy).
//! - Multi-partition layouts — Phase B will add `--partitions N`.

use std::env;
use std::fs;
use std::io::Cursor;

use byteorder::{BigEndian, ByteOrder};

use rusty_backup::fs::fat::create_blank_fat;
use rusty_backup::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
use rusty_backup::fs::human68k::Human68kFilesystem;
use rusty_backup::partition::x68k::{
    X68K_ENTRY_SIZE, X68K_FIRST_PARTITION_SECTOR, X68K_MAGIC, X68K_MAX_PARTITIONS,
    X68K_TABLE_HEADER_SIZE,
};
use rusty_backup::partition::x68k_ipl::{
    build_sasi_boot_block, build_scsi_boot_block, SASI_BOOT_BLOCK_BYTES, SCSI_BOOT_BLOCK_BYTES,
};

/// SASI logical sector size (matches real Sharp / Hudson SASI HDDs +
/// `Bomberman.hdf` reference).
const SASI_SECTOR_SIZE: u64 = 256;

/// SCSI logical sector size (matches the `SCSI_NetBSD.hds` /
/// BlueSCSI / ZuluSCSI / Keisoku Giken convention).
const SCSI_SECTOR_SIZE: u64 = 1024;

/// Geometry/capacity descriptor that follows the `X68SCSI1` signature on
/// our generated SCSI HDDs. Bytes lifted from `SCSI_NetBSD.hds` byte
/// `0x08..0x10`. Real Sharp HDDs vary the inner fields per drive; this
/// is the canonical NetBSD-installer value that MAME accepts.
const SCSI_DESCRIPTOR: [u8; 8] = [0x02, 0x00, 0x00, 0x1d, 0xaf, 0xff, 0x01, 0x00];

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Variant {
    Sasi,
    Scsi,
}

impl Variant {
    fn sector_size(self) -> u64 {
        match self {
            Self::Sasi => SASI_SECTOR_SIZE,
            Self::Scsi => SCSI_SECTOR_SIZE,
        }
    }

    fn boot_block_bytes(self) -> usize {
        match self {
            Self::Sasi => SASI_BOOT_BLOCK_BYTES,
            Self::Scsi => SCSI_BOOT_BLOCK_BYTES,
        }
    }
}

fn main() {
    let mut out_path: Option<String> = None;
    let mut size_mib: u64 = 8;
    let mut variant = Variant::Sasi;
    for arg in env::args().skip(1) {
        match arg.as_str() {
            "--scsi" => variant = Variant::Scsi,
            "--sasi" => variant = Variant::Sasi,
            other => {
                if out_path.is_none() {
                    out_path = Some(other.to_string());
                } else if let Ok(n) = other.parse::<u64>() {
                    size_mib = n;
                } else {
                    panic!("unrecognised arg: {other}");
                }
            }
        }
    }
    let out_path = out_path.expect("usage: build_x68k_hdd <out.hdf> [size_mib] [--scsi]");

    let sector_size = variant.sector_size();
    let boot_block_bytes = variant.boot_block_bytes();
    let part_start_byte = u64::from(X68K_FIRST_PARTITION_SECTOR) * sector_size;
    assert!(
        part_start_byte >= boot_block_bytes as u64,
        "partition body would overlap the boot block (start={part_start_byte}, \
         boot block ends at {boot_block_bytes})",
    );

    let total_bytes = size_mib * 1024 * 1024;
    let part_bytes_target = total_bytes - part_start_byte;

    // mkfs the partition as blank FAT12 with X68000-friendly geometry.
    let label = Some("RBHDD");
    let mut body =
        create_blank_fat(part_bytes_target, label).expect("create_blank_fat on partition body");

    // Seed a few text files inside it via the Human68k engine so the
    // user can see them once a Human68k system floppy boots and mounts C:.
    {
        let mut fs = Human68kFilesystem::open(Cursor::new(&mut body), 0)
            .expect("open blank partition as Human68k");
        let root = fs.root().expect("root");
        let seeds: &[(&str, &[u8])] = &[
            (
                "HELLO.TXT",
                b"hello from rusty-backup\r\n\
                  This file lives on the self-bootable X68000 HDD that\r\n\
                  rusty-backup's examples/build_x68k_hdd built.\r\n",
            ),
            (
                "MISTER.TXT",
                b"rusty-backup MiSTer X68000 verification fixture\r\n",
            ),
            (
                "README.TXT",
                b"To verify in MAME or on real / MiSTer hardware:\r\n\
                  1. Boot Human68k from FDD0 (BLANK_disk_X68000.D88).\r\n\
                  2. At the A> prompt, type: dir C:\r\n\
                  3. You should see HELLO.TXT, MISTER.TXT, README.TXT.\r\n\
                  4. Type: type C:HELLO.TXT\r\n",
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

    let disk_sectors = total_bytes / sector_size;
    let part_sectors = body.len() as u64 / sector_size;
    let part_start_sector = u64::from(X68K_FIRST_PARTITION_SECTOR);

    // Build the X68K partition table (144 bytes: 16-byte header + 8 entries).
    // We only populate slot 0; the remaining 7 slots stay zero-filled.
    let mut table = [0u8; X68K_TABLE_HEADER_SIZE + X68K_MAX_PARTITIONS * X68K_ENTRY_SIZE];
    BigEndian::write_u32(&mut table[0..4], X68K_MAGIC);
    BigEndian::write_u32(&mut table[4..8], disk_sectors as u32);
    BigEndian::write_u32(&mut table[8..12], disk_sectors as u32);
    // Bytes 12..16 reserved.
    let e_off = X68K_TABLE_HEADER_SIZE;
    table[e_off..e_off + 8].copy_from_slice(b"Human   ");
    BigEndian::write_u32(&mut table[e_off + 8..e_off + 12], part_start_sector as u32);
    BigEndian::write_u32(&mut table[e_off + 12..e_off + 16], part_sectors as u32);

    // Assemble the disk: boot block + zero padding + partition body.
    let mut disk = vec![0u8; total_bytes as usize];
    let boot_block: Vec<u8> = match variant {
        Variant::Sasi => build_sasi_boot_block(&table).to_vec(),
        Variant::Scsi => build_scsi_boot_block(&SCSI_DESCRIPTOR, &table).to_vec(),
    };
    disk[..boot_block.len()].copy_from_slice(&boot_block);
    let part_off = part_start_byte as usize;
    disk[part_off..part_off + body.len()].copy_from_slice(&body);

    fs::write(&out_path, &disk).unwrap_or_else(|e| panic!("write {out_path}: {e}"));

    let variant_name = match variant {
        Variant::Sasi => "SASI",
        Variant::Scsi => "SCSI",
    };
    println!(
        "wrote {} ({} bytes, {} MiB, {} {}-byte sectors)",
        out_path,
        disk.len(),
        disk.len() / (1024 * 1024),
        disk_sectors,
        sector_size,
    );
    println!(
        "  {variant_name} boot block @ byte 0 ({} bytes), table inside it",
        boot_block.len(),
    );
    println!(
        "  1 Human partition @ sector {} (byte {}, {} sectors, {} MiB)",
        part_start_sector,
        part_off,
        part_sectors,
        body.len() / (1024 * 1024),
    );
    println!("  seeded: HELLO.TXT, MISTER.TXT, README.TXT");
    println!();
    println!("To verify in MAME (WSL):");
    match variant {
        Variant::Sasi => println!(
            "  mame -rompath /mnt/c/Temp/x68000/roms \\\n\
             \x20    -window -nothrottle -seconds_to_run 5 x68000 -sasi {out_path}"
        ),
        Variant::Scsi => println!(
            "  mame -rompath /mnt/c/Temp/x68000/roms \\\n\
             \x20    -window -nothrottle -seconds_to_run 5 x68030 -hard {out_path}"
        ),
    }
}
