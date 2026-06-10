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

use rusty_backup::fs::entry::{EntryType, FileEntry};
use rusty_backup::fs::fat::create_blank_fat;
use rusty_backup::fs::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem,
};
use rusty_backup::fs::human68k::Human68kFilesystem;
use rusty_backup::partition::x68k::{
    X68K_ENTRY_SIZE, X68K_FIRST_PARTITION_SECTOR, X68K_MAGIC, X68K_MAX_PARTITIONS,
    X68K_TABLE_HEADER_SIZE,
};
use rusty_backup::partition::x68k_ipl::{
    build_sasi_boot_block, build_scsi_boot_block, IplStub, SASI_BOOT_BLOCK_BYTES,
    SCSI_BOOT_BLOCK_BYTES,
};
use rusty_backup::rbformats::containers::decode_floppy_container_file;

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

/// Recursively walk a donor Human68k filesystem and collect every file +
/// directory as `(relative_path, kind)` pairs in DFS pre-order. Files
/// come with their full byte contents — donors are typically 1-2 MiB
/// (Human68k 3.x system floppy) so eager-loading is fine.
fn collect_donor_tree(
    donor: &mut Human68kFilesystem<Cursor<&[u8]>>,
) -> Result<Vec<DonorEntry>, String> {
    fn walk(
        fs: &mut Human68kFilesystem<Cursor<&[u8]>>,
        dir: &FileEntry,
        prefix: &[String],
        out: &mut Vec<DonorEntry>,
    ) -> Result<(), String> {
        let entries = fs.list_directory(dir).map_err(|e| format!("list: {e}"))?;
        for entry in entries {
            // Human68k surfaces "." and ".." on subdir listings; skip them.
            if entry.name == "." || entry.name == ".." {
                continue;
            }
            let mut path = prefix.to_vec();
            path.push(entry.name.clone());
            match entry.entry_type {
                EntryType::Directory => {
                    out.push(DonorEntry {
                        path: path.clone(),
                        kind: DonorKind::Directory,
                    });
                    walk(fs, &entry, &path, out)?;
                }
                EntryType::File => {
                    let bytes = fs
                        .read_file(&entry, usize::MAX)
                        .map_err(|e| format!("read {}: {e}", entry.name))?;
                    out.push(DonorEntry {
                        path,
                        kind: DonorKind::File(bytes),
                    });
                }
                _ => { /* symlinks / specials don't exist on Human68k FAT */ }
            }
        }
        Ok(())
    }
    let root = donor.root().map_err(|e| format!("root: {e}"))?;
    let mut out = Vec::new();
    walk(donor, &root, &[], &mut out)?;
    Ok(out)
}

struct DonorEntry {
    /// Path components, e.g. `["BIN", "SWITCH.X"]` for `/BIN/SWITCH.X`.
    path: Vec<String>,
    kind: DonorKind,
}

enum DonorKind {
    File(Vec<u8>),
    Directory,
}

/// Apply a previously-collected donor tree to the output partition's
/// Human68k filesystem, creating directories before the files inside them.
/// Returns `(files_written, dirs_written)`.
fn apply_donor_tree<W: std::io::Read + std::io::Write + std::io::Seek + Send>(
    out_fs: &mut Human68kFilesystem<W>,
    tree: &[DonorEntry],
) -> Result<(usize, usize), String> {
    let mut files = 0;
    let mut dirs = 0;
    // Walk DonorEntry list — DFS pre-order so each entry's parent has
    // already been created by the time we get to it.
    for entry in tree {
        // split_last returns (last_element, rest_slice) — the last
        // component is the new entry's name, everything before it is the
        // parent directory chain we need to walk.
        let (name, parent_components) = match entry.path.split_last() {
            Some(s) => s,
            None => continue,
        };
        // Resolve the parent FileEntry by walking from root.
        let mut parent = out_fs.root().map_err(|e| format!("root: {e}"))?;
        for comp in parent_components {
            let children = out_fs
                .list_directory(&parent)
                .map_err(|e| format!("list {comp}: {e}"))?;
            parent = children
                .into_iter()
                .find(|c| &c.name == comp && c.entry_type == EntryType::Directory)
                .ok_or_else(|| format!("intermediate dir {comp} missing"))?;
        }
        match &entry.kind {
            DonorKind::Directory => {
                out_fs
                    .create_directory(&parent, name, &CreateDirectoryOptions::default())
                    .map_err(|e| format!("mkdir {name}: {e}"))?;
                dirs += 1;
            }
            DonorKind::File(bytes) => {
                let mut reader: &[u8] = bytes;
                out_fs
                    .create_file(
                        &parent,
                        name,
                        &mut reader,
                        bytes.len() as u64,
                        &CreateFileOptions::default(),
                    )
                    .map_err(|e| format!("create {name}: {e}"))?;
                files += 1;
            }
        }
    }
    Ok((files, dirs))
}

fn main() {
    let mut out_path: Option<String> = None;
    let mut size_mib: u64 = 8;
    let mut variant = Variant::Sasi;
    let mut stub = IplStub::Print;
    let mut system_disk: Option<String> = None;
    let mut args_iter = env::args().skip(1);
    while let Some(arg) = args_iter.next() {
        match arg.as_str() {
            "--scsi" => variant = Variant::Scsi,
            "--sasi" => variant = Variant::Sasi,
            "--halt-stub" => stub = IplStub::Halt,
            "--print-stub" => stub = IplStub::Print,
            "--system-disk" => {
                system_disk = Some(args_iter.next().expect("--system-disk needs a path"));
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
         [--system-disk PATH.DIM]",
    );

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

    // If a donor system disk was provided, pre-read its tree so we can
    // close it before opening the output filesystem (avoids double-borrow
    // of the partition body).
    let donor_tree: Option<Vec<DonorEntry>> = system_disk.as_deref().map(|path| {
        // Donors are typically .dim / .D88 / .xdf / .hdm floppy containers
        // — decode via the container layer to get a flat FAT stream. Fall
        // back to raw bytes if the file isn't a recognised container (so a
        // pre-flat .img works too).
        let donor_bytes = match decode_floppy_container_file(std::path::Path::new(path)) {
            Ok((kind, flat)) => {
                println!(
                    "  donor: decoded {} container ({} bytes flat)",
                    kind.display_name(),
                    flat.len()
                );
                flat
            }
            Err(_) => {
                let raw = fs::read(path).unwrap_or_else(|e| panic!("read donor {path}: {e}"));
                println!("  donor: using raw bytes ({} bytes)", raw.len());
                raw
            }
        };
        let donor_slice: &[u8] = &donor_bytes;
        let mut donor_fs = Human68kFilesystem::open(Cursor::new(donor_slice), 0)
            .unwrap_or_else(|e| panic!("open donor as Human68k: {e}"));
        let tree = collect_donor_tree(&mut donor_fs).unwrap_or_else(|e| panic!("walk donor: {e}"));
        println!(
            "  read donor {path}: {} files + {} dirs",
            tree.iter()
                .filter(|e| matches!(e.kind, DonorKind::File(_)))
                .count(),
            tree.iter()
                .filter(|e| matches!(e.kind, DonorKind::Directory))
                .count(),
        );
        tree
    });

    // Populate the partition: either a couple of seed text files (the
    // engine-validation case) or a full clone of the donor system disk
    // (Phase D — `--system-disk` was passed).
    {
        let mut fs = Human68kFilesystem::open(Cursor::new(&mut body), 0)
            .expect("open blank partition as Human68k");
        if let Some(tree) = donor_tree {
            let (files, dirs) =
                apply_donor_tree(&mut fs, &tree).expect("apply donor tree to output partition");
            println!("  wrote donor: {files} files + {dirs} dirs into Human partition");
        } else {
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
        Variant::Sasi => build_sasi_boot_block(stub, &table).to_vec(),
        Variant::Scsi => build_scsi_boot_block(&SCSI_DESCRIPTOR, stub, &table).to_vec(),
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
    if system_disk.is_some() {
        println!("  -> Boot from FDD0 with the same donor floppy once, then run");
        println!("     `A:\\BIN\\SWITCH.X /HD` to install the HDD boot sector.");
        println!("     After that, the HDD self-boots straight to C:>.");
    } else {
        println!("  seeded: HELLO.TXT, MISTER.TXT, README.TXT");
    }
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
