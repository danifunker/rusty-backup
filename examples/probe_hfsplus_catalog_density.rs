//! Diagnostic: walk a wrapped (or flat) HFS+ partition's catalog and report
//! per-leaf record counts + byte usage, flagging any leaf where the merged
//! record set wouldn't fit two 4096-byte nodes (the failure mode that
//! crashed the defrag clone on /dev/disk6). Also runs the same simulation
//! the defrag planner uses, so you can see WHICH leaf would trigger an
//! N-way split.
//!
//! Usage:
//!   sudo ./target/debug/examples/probe_hfsplus_catalog_density /dev/disk6 6
//!   sudo ./target/debug/examples/probe_hfsplus_catalog_density /dev/disk6 7
//!
//! The second arg is the APM partition index (matches the GUI's
//! `Partition N:` numbering).

use std::env;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};

use rusty_backup::fs::hfsplus_wrapper_clone::detect_wrapped_hfsplus;
use rusty_backup::os::SectorAlignedReader;
use rusty_backup::partition::PartitionTable;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: {} <device-or-image> <partition-index>", args[0]);
        std::process::exit(2);
    }
    let path = &args[1];
    let want_idx: usize = args[2].parse()?;

    let file = File::open(path)?;
    let is_raw_device = path.starts_with("/dev/");
    let mut reader: Box<dyn ReadSeek> = if is_raw_device {
        Box::new(SectorAlignedReader::new(file))
    } else {
        Box::new(BufReader::new(file))
    };

    let table = PartitionTable::detect(&mut reader)?;
    let parts = table.partitions();
    println!(
        "disk: {} partition(s) (table={})",
        parts.len(),
        table.type_name()
    );
    let part = parts
        .into_iter()
        .find(|p| p.index == want_idx)
        .ok_or_else(|| format!("no partition with index {want_idx}"))?;
    let partition_offset = part.start_lba * 512;
    println!(
        "partition-{}: type={} offset={} size={}",
        part.index, part.type_name, partition_offset, part.size_bytes,
    );

    // Detect wrapper, resolve inner HFS+ offset.
    let wrapper = detect_wrapped_hfsplus(&mut reader, partition_offset, part.size_bytes);
    let inner_offset = match &wrapper {
        Some(info) => {
            println!(
                "wrapper: al_block_size={} inner_offset={} inner_size={}",
                info.al_block_size, info.inner_offset, info.inner_size
            );
            info.inner_offset
        }
        None => {
            println!("no HFS wrapper — treating as flat HFS+");
            partition_offset
        }
    };

    // Read the HFS+ volume header at inner_offset + 1024.
    let mut vh = [0u8; 512];
    reader.seek(SeekFrom::Start(inner_offset + 1024))?;
    reader.read_exact(&mut vh)?;
    let signature = u16::from_be_bytes([vh[0], vh[1]]);
    let block_size = u32::from_be_bytes([vh[40], vh[41], vh[42], vh[43]]);
    let total_blocks = u32::from_be_bytes([vh[44], vh[45], vh[46], vh[47]]);
    let free_blocks = u32::from_be_bytes([vh[48], vh[49], vh[50], vh[51]]);
    println!(
        "VH: sig={:04x} block_size={} total_blocks={} free_blocks={}",
        signature, block_size, total_blocks, free_blocks
    );

    // catalogFile fork: 80 bytes at VH offset 272 (after attributes, allocation,
    // extents). HFS+ VH layout: catalogFile lives at offset 272..272+80.
    let catalog_logical_size = u64::from_be_bytes([
        vh[272], vh[273], vh[274], vh[275], vh[276], vh[277], vh[278], vh[279],
    ]);
    let catalog_total_blocks = u32::from_be_bytes([vh[284], vh[285], vh[286], vh[287]]);
    println!(
        "catalog fork: logical={} bytes total_blocks={}",
        catalog_logical_size, catalog_total_blocks
    );

    // First 8 extent descriptors of the catalog fork: 8 × 8 bytes at offset 288.
    let mut catalog_bytes: Vec<u8> = Vec::with_capacity(catalog_logical_size as usize);
    let mut remaining = catalog_logical_size;
    for i in 0..8 {
        if remaining == 0 {
            break;
        }
        let off = 288 + i * 8;
        let start_block = u32::from_be_bytes([vh[off], vh[off + 1], vh[off + 2], vh[off + 3]]);
        let blk_count = u32::from_be_bytes([vh[off + 4], vh[off + 5], vh[off + 6], vh[off + 7]]);
        if blk_count == 0 {
            continue;
        }
        let ext_bytes = (blk_count as u64) * (block_size as u64);
        let take = ext_bytes.min(remaining);
        let abs_offset = inner_offset + (start_block as u64) * (block_size as u64);
        let mut buf = vec![0u8; take as usize];
        reader.seek(SeekFrom::Start(abs_offset))?;
        reader.read_exact(&mut buf)?;
        catalog_bytes.extend_from_slice(&buf);
        remaining -= take;
    }
    if remaining > 0 {
        println!(
            "WARNING: catalog has {} bytes in extents-overflow not read (extents-overflow walk not implemented in this probe)",
            remaining
        );
    }
    println!(
        "catalog loaded: {} bytes (in-line extents only)",
        catalog_bytes.len()
    );

    // Parse BTHeader at node 0, offset 14.
    if catalog_bytes.len() < 512 {
        return Err("catalog too small to contain BTHeader".into());
    }
    let node_size = u16::from_be_bytes([catalog_bytes[32], catalog_bytes[33]]) as usize;
    let first_leaf = u32::from_be_bytes([
        catalog_bytes[24],
        catalog_bytes[25],
        catalog_bytes[26],
        catalog_bytes[27],
    ]);
    let leaf_records = u32::from_be_bytes([
        catalog_bytes[20],
        catalog_bytes[21],
        catalog_bytes[22],
        catalog_bytes[23],
    ]);
    println!(
        "BTHeader: node_size={} first_leaf={} leaf_records={}",
        node_size, first_leaf, leaf_records
    );

    // Walk leaves via fwd-link.
    let mut leaf_idx = first_leaf;
    let mut visited = 0usize;
    let payload_limit = node_size.saturating_sub(16);
    println!();
    println!(
        "{:>8} {:>8} {:>10} {:>10}  note",
        "node_idx", "records", "used_b", "longest_r"
    );
    let mut tight_leaves = 0usize;
    while leaf_idx != 0 && visited < 200_000 {
        let off = leaf_idx as usize * node_size;
        if off + node_size > catalog_bytes.len() {
            println!("leaf {leaf_idx}: out of bounds (catalog truncated by extents-overflow)");
            break;
        }
        let node = &catalog_bytes[off..off + node_size];
        let kind = node[8] as i8;
        if kind != -1 {
            println!("leaf {leaf_idx}: kind={} (not leaf) — chain corrupt", kind);
            break;
        }
        let num_records = u16::from_be_bytes([node[10], node[11]]) as usize;
        let next = u32::from_be_bytes([node[0], node[1], node[2], node[3]]);

        let mut used = 14usize + 2; // descriptor + free pointer
        let mut longest = 0usize;
        for i in 0..num_records {
            let opos = node_size - 2 * (i + 1);
            let npos = node_size - 2 * (i + 2);
            let s = u16::from_be_bytes([node[opos], node[opos + 1]]) as usize;
            let e = u16::from_be_bytes([node[npos], node[npos + 1]]) as usize;
            let rec_len = e.saturating_sub(s);
            used += rec_len + 2;
            longest = longest.max(rec_len);
        }
        // Simulate inserting one max-size catalog record (~768 bytes for HFS+
        // long-name file) and check if 2-way split would fit.
        let max_record = 768usize;
        let total_after = used.saturating_sub(2) + max_record + 2; // remove free ptr from used
        let half = total_after.saturating_sub(14) / 2;
        let note = if used > node_size {
            "OVERFLOW (corrupt)".to_string()
        } else if half > payload_limit {
            tight_leaves += 1;
            format!("TIGHT: 2-way split with +{max_record}B record won't fit")
        } else if half > payload_limit * 90 / 100 {
            format!("near limit: 2-way half = {half}/{payload_limit}")
        } else {
            String::new()
        };
        println!("{leaf_idx:>8} {num_records:>8} {used:>10} {longest:>10}  {note}");

        leaf_idx = next;
        visited += 1;
    }
    println!();
    println!(
        "visited {visited} leaves; {tight_leaves} would require N>2-way split on max-record insert",
    );
    Ok(())
}

trait ReadSeek: Read + Seek {}
impl<T: Read + Seek> ReadSeek for T {}
