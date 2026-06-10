//! Build a self-bootable Human68k HDD image (SASI or SCSI variant) that
//! the Sharp X68000 IPL ROM recognises and chains into.
//!
//! Usage:
//!
//! ```text
//! cargo run --example build_x68k_hdd -- <out.hdf> [size_mib] [--scsi]
//!                                       [--halt-stub|--print-stub]
//!                                       [--system-disk PATH.DIM]
//! ```
//!
//! Default is **SASI** (256-byte sectors, table at byte 0x400, IPL stub
//! at byte 0). Pass `--scsi` to emit the SCSI variant (`X68SCSI1` +
//! Keisoku Giken descriptor at byte 0, IPL at byte 0x400, table at byte
//! 0x800, 1024-byte sectors).
//!
//! Most callers should use the shipping CLI verb at parity instead:
//!
//! ```text
//! rb-cli new-x68k-hdd out.hdf --size 16M [--scsi] [--system-disk PATH.DIM]
//! ```
//!
//! This example exists for ad-hoc Phase A-D verification (it wires the
//! same `partition::x68k_hdd_builder::build_x68k_hdd` builder the CLI
//! uses), and to print a MAME boot-test hint after writing the file.

use std::env;
use std::path::PathBuf;

use rusty_backup::partition::x68k_hdd_builder::{build_x68k_hdd, HddVariant};
use rusty_backup::partition::x68k_ipl::IplStub;

fn main() {
    let mut out_path: Option<String> = None;
    let mut size_mib: u64 = 8;
    let mut variant = HddVariant::Sasi;
    let mut stub = IplStub::Print;
    let mut system_disk: Option<String> = None;
    let mut boot_sector_donor: Option<String> = None;
    let mut partitions: usize = 1;
    let mut args_iter = env::args().skip(1);
    while let Some(arg) = args_iter.next() {
        match arg.as_str() {
            "--scsi" => variant = HddVariant::Scsi,
            "--sasi" => variant = HddVariant::Sasi,
            "--halt-stub" => stub = IplStub::Halt,
            "--print-stub" => stub = IplStub::Print,
            "--system-disk" => {
                system_disk = Some(args_iter.next().expect("--system-disk needs a path"));
            }
            "--boot-sector-donor" => {
                boot_sector_donor =
                    Some(args_iter.next().expect("--boot-sector-donor needs a path"));
            }
            "--partitions" => {
                partitions = args_iter
                    .next()
                    .expect("--partitions needs a number")
                    .parse()
                    .expect("--partitions must be an integer");
            }
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
    let out_path = out_path.expect(
        "usage: build_x68k_hdd <out.hdf> [size_mib] [--scsi] [--halt-stub|--print-stub] \
         [--system-disk PATH.DIM] [--boot-sector-donor PATH.HDS]",
    );
    let out_path = PathBuf::from(out_path);

    let summary = build_x68k_hdd(
        &out_path,
        size_mib,
        variant,
        stub,
        partitions,
        system_disk.as_deref().map(std::path::Path::new),
        boot_sector_donor.as_deref().map(std::path::Path::new),
    )
    .unwrap_or_else(|e| panic!("build failed: {e}"));

    println!(
        "wrote {} ({} bytes, {} MiB, {} {}-byte sectors)",
        out_path.display(),
        summary.total_bytes,
        summary.total_bytes / (1024 * 1024),
        summary.disk_sectors,
        summary.sector_size,
    );
    println!(
        "  {} boot block @ byte 0 ({} bytes), table inside it",
        summary.variant.name(),
        summary.boot_block_bytes,
    );
    println!(
        "  {} Human partition{} starting @ sector {} (byte {}), {} sectors / {} MiB each",
        summary.partition_count,
        if summary.partition_count == 1 {
            ""
        } else {
            "s"
        },
        summary.partition_start_sector,
        summary.partition_start_byte,
        summary.partition_sectors,
        summary.partition_bytes / (1024 * 1024),
    );
    if summary.from_donor {
        println!(
            "  wrote donor: {} files + {} dirs into Human partition",
            summary.files_written, summary.dirs_written
        );
        if summary.boot_sector_donor_applied {
            println!("  -> Sharp partition boot sector overlaid from --boot-sector-donor.");
            println!("     HDD self-boots straight to C:> on every power-on, no FDD0 needed.");
        } else {
            println!("  -> Boot from FDD0 with the same donor floppy once, then run");
            println!("     `A:\\BIN\\SWITCH.X /HD` to install the HDD boot sector.");
            println!("     After that, the HDD self-boots straight to C:>.");
        }
    } else if summary.boot_sector_donor_applied {
        println!("  seeded: HELLO.TXT, MISTER.TXT, README.TXT");
        println!("  WARNING: --boot-sector-donor without --system-disk;");
        println!("           the donor's boot code expects HUMAN.SYS in the partition root.");
    } else {
        println!("  seeded: HELLO.TXT, MISTER.TXT, README.TXT");
    }
    println!();
    println!("To verify in MAME (WSL):");
    match variant {
        HddVariant::Sasi => println!(
            "  mame -rompath /mnt/c/Temp/x68000/roms \\\n\
             \x20    -window -nothrottle -seconds_to_run 5 x68000 -sasi {}",
            out_path.display(),
        ),
        HddVariant::Scsi => println!(
            "  mame -rompath /mnt/c/Temp/x68000/roms \\\n\
             \x20    -window -nothrottle -seconds_to_run 5 x68030 -hard {}",
            out_path.display(),
        ),
    }
}
