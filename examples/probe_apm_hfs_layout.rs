//! Per-partition Apple_HFS layout probe.
//!
//! Walks every APM partition on the given device and, for each
//! `Apple_HFS`/`Apple_HFSX` slot, reports whether the body is a native
//! HFS+/HFSX volume or an embedded HFS+ inside a classic HFS wrapper
//! (the `drEmbedSigWord` layout used by Mac OS 8/9 dual-fork volumes).
//!
//! Usage:
//!   sudo cargo run --example probe_apm_hfs_layout -- <device>

use std::env;
use std::fs::File;
use std::io::BufReader;

use rusty_backup::fs;
use rusty_backup::partition::PartitionTable;

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: {} <device>", args[0]);
        std::process::exit(2);
    }
    let path = &args[1];

    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let table = PartitionTable::detect(&mut reader)?;
    let parts = table.partitions();

    println!("Table: {}", table.type_name());
    println!("Partitions: {}", parts.len());

    for part in &parts {
        let type_str = part
            .partition_type_string
            .as_deref()
            .unwrap_or("(no type string)");
        let offset = part.start_lba * 512;
        print!(
            "  partition-{} type=\"{}\" offset={} size={} ",
            part.index, type_str, offset, part.size_bytes,
        );

        // Only Apple_HFS family slots can be HFS / HFS+ / wrapped.
        let is_hfs_family = matches!(type_str, "Apple_HFS" | "Apple_HFSX" | "Apple_HFS+");
        if !is_hfs_family {
            println!("-> not HFS family, skipping");
            continue;
        }

        let f2 = File::open(path)?;
        let mut r2 = BufReader::new(f2);
        let (fs_type, hfsplus_offset) = fs::resolve_apple_hfs(&mut r2, offset);
        println!();
        println!("    resolved fs_type: {fs_type}");
        println!("    hfsplus_offset:   {hfsplus_offset}");
        if fs_type == "hfsplus" {
            if hfsplus_offset == offset {
                println!("    layout: NATIVE HFS+/HFSX  (flat clone)");
            } else {
                let delta = hfsplus_offset - offset;
                println!(
                    "    layout: EMBEDDED HFS+ inside HFS wrapper (offset shift {delta} bytes) (wrapped clone)"
                );
            }
        } else if fs_type == "hfs" {
            println!("    layout: CLASSIC HFS (no HFS+) -- clone NOT supported");
        } else {
            println!("    layout: unrecognised -- skipping");
        }

        // Pre-flight result mirrors what run_backup uses.
        let f3 = File::open(path)?;
        let mut r3 = BufReader::new(f3);
        match fs::can_defrag_clone_hfsplus(&mut r3, offset) {
            Ok(()) => println!("    can_defrag_clone_hfsplus: OK"),
            Err(reason) => println!("    can_defrag_clone_hfsplus: REFUSED -- {reason}"),
        }

        // Detailed wrapper parse for embedded HFS+, plus a clone plan
        // dry-run so we can see the exact alignment math.
        let f4 = File::open(path)?;
        let mut r4 = BufReader::new(f4);
        if let Some(info) =
            fs::hfsplus_wrapper_clone::detect_wrapped_hfsplus(&mut r4, offset, part.size_bytes)
        {
            println!(
                "    wrapper: al_block_size={} drAlBlSt={} drNmAlBlks={} drVBMSt={} embed_start_block={} embed_block_count={}",
                info.al_block_size,
                info.al_block_start_sector,
                info.source_total_blocks,
                info.bitmap_start_sector,
                info.embed_start_block,
                info.embed_block_count,
            );
            let outer_overhead = (info.al_block_start_sector as u64) * 512
                + (info.embed_start_block as u64) * (info.al_block_size as u64)
                + 1024;
            println!("    outer_overhead = {outer_overhead}");
            // Try a plan at the inner volume's defragmented_minimum_size.
            let f5 = File::open(path)?;
            let r5 = BufReader::new(f5);
            if let Some(inner_min) = fs::defragmented_partition_size(
                r5,
                offset,
                part.partition_type_byte,
                type_str.into(),
            ) {
                // defragmented_partition_size already adds wrapper overhead;
                // back it out for the plan dry-run.
                let inner_only = inner_min.saturating_sub(outer_overhead);
                println!("    inner-only defragmented_minimum_size = {inner_only}");
                match fs::hfsplus_wrapper_clone::plan_wrapped_clone(&info, inner_only) {
                    Ok(p) => println!(
                        "    plan_wrapped_clone OK: new_inner_size={} new_embed_block_count={} new_partition_size={}",
                        p.new_inner_size, p.new_embed_block_count, p.new_partition_size,
                    ),
                    Err(e) => println!("    plan_wrapped_clone Err: {e}"),
                }
            }
        }
    }

    Ok(())
}
