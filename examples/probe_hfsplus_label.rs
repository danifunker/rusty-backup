//! Diagnostic for HFS+ volume-label probing on a live partition.
//!
//! Usage:
//!   sudo cargo run --example probe_hfsplus_label -- <device> <partition_offset_bytes>
//!
//! Walks the same path as `probe_hfsplus_volume_label`, but prints every
//! intermediate value (VH fields, catalog header, first-leaf record 0 key)
//! so we can see exactly where the live probe breaks.

use std::env;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};

use byteorder::{BigEndian, ByteOrder};

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: {} <device> <partition_offset_bytes>", args[0]);
        std::process::exit(2);
    }
    let path = &args[1];
    let offset: u64 = args[2].parse().expect("offset must be a u64");

    let f = File::open(path)?;
    let mut r = BufReader::new(f);

    // Detect HFS-wrapped HFS+ (legacy hybrid where the partition starts with a
    // classic HFS MDB at partition_offset+1024 with drEmbedSigWord=0x482B
    // pointing at the inner HFS+ region).
    eprintln!("== reading first sector at {} ==", offset + 1024);
    r.seek(SeekFrom::Start(offset + 1024))?;
    let mut probe = [0u8; 512];
    r.read_exact(&mut probe)?;
    let probe_sig = BigEndian::read_u16(&probe[0..2]);
    eprintln!(
        "  signature: 0x{:04X} ({})",
        probe_sig,
        match probe_sig {
            0x4244 => "BD (classic HFS MDB)",
            0x482B => "H+ (native HFS+)",
            0x4858 => "HX (HFSX)",
            _ => "??",
        }
    );

    let hfsplus_offset = if probe_sig == 0x4244 {
        let embedded_sig = BigEndian::read_u16(&probe[124..126]);
        if embedded_sig != 0x482B {
            eprintln!(
                "  classic HFS without embedded HFS+ (drEmbedSigWord=0x{:04X}); aborting",
                embedded_sig
            );
            return Ok(());
        }
        let block_size_hfs = BigEndian::read_u32(&probe[20..24]) as u64;
        let first_alloc_block = BigEndian::read_u16(&probe[28..30]) as u64;
        let embedded_start = BigEndian::read_u16(&probe[126..128]) as u64;
        let resolved = offset + first_alloc_block * 512 + embedded_start * block_size_hfs;
        eprintln!(
            "  HFS-wrapped HFS+: drAlBlkSiz={}, drAlBlSt={}, drEmbedExtent.startBlock={}",
            block_size_hfs, first_alloc_block, embedded_start
        );
        eprintln!("  embedded HFS+ region starts at byte {}", resolved);
        resolved
    } else if probe_sig == 0x482B || probe_sig == 0x4858 {
        offset
    } else {
        eprintln!("  unrecognized signature; aborting");
        return Ok(());
    };

    eprintln!("== reading VH at {} ==", hfsplus_offset + 1024);
    r.seek(SeekFrom::Start(hfsplus_offset + 1024))?;
    let mut vh = [0u8; 512];
    r.read_exact(&mut vh)?;
    let sig = BigEndian::read_u16(&vh[0..2]);
    eprintln!(
        "  signature: 0x{:04X} ({})",
        sig,
        match sig {
            0x482B => "H+",
            0x4858 => "HX",
            _ => "??",
        }
    );
    let block_size = BigEndian::read_u32(&vh[40..44]);
    let total_blocks = BigEndian::read_u32(&vh[44..48]);
    let free_blocks = BigEndian::read_u32(&vh[48..52]);
    eprintln!(
        "  block_size={}, total_blocks={}, free_blocks={}",
        block_size, total_blocks, free_blocks
    );

    // Catalog fork: 80 bytes at offset 272.
    let cat = &vh[272..352];
    let cat_logical = BigEndian::read_u64(&cat[0..8]);
    let cat_total_blocks = BigEndian::read_u32(&cat[12..16]);
    eprintln!(
        "  catalog_file: logical={} bytes, total_blocks={}",
        cat_logical, cat_total_blocks
    );
    eprintln!("  catalog inline extents:");
    for i in 0..8 {
        let off = 16 + i * 8;
        let start = BigEndian::read_u32(&cat[off..off + 4]);
        let count = BigEndian::read_u32(&cat[off + 4..off + 8]);
        if count == 0 {
            break;
        }
        eprintln!(
            "    [{}] start_block={} count={} (bytes {}..{})",
            i,
            start,
            count,
            hfsplus_offset + start as u64 * block_size as u64,
            hfsplus_offset + (start as u64 + count as u64) * block_size as u64
        );
    }

    // Build a tiny extent walker (inline extents only).
    let extents: Vec<(u32, u32)> = (0..8)
        .map(|i| {
            let off = 16 + i * 8;
            (
                BigEndian::read_u32(&cat[off..off + 4]),
                BigEndian::read_u32(&cat[off + 4..off + 8]),
            )
        })
        .filter(|(_, c)| *c > 0)
        .collect();
    let translate = |fork_off: u64| -> Option<(u64, u64)> {
        let bs = block_size as u64;
        let mut cur = 0u64;
        for (start, count) in &extents {
            let len = *count as u64 * bs;
            if fork_off < cur + len {
                let local = fork_off - cur;
                return Some((hfsplus_offset + *start as u64 * bs + local, len - local));
            }
            cur += len;
        }
        None
    };

    // Read node 0 with a 512-byte probe; the BTHeaderRec lives at bytes 14..44.
    let (n0_dev, n0_avail) = translate(0).expect("translate node 0");
    eprintln!(
        "== reading node 0 header at dev offset {} ({} bytes available) ==",
        n0_dev, n0_avail
    );
    r.seek(SeekFrom::Start(n0_dev))?;
    let mut n0 = [0u8; 4096];
    r.read_exact(&mut n0)?;
    let depth = BigEndian::read_u16(&n0[14..16]);
    let root_node = BigEndian::read_u32(&n0[14 + 2..14 + 6]);
    let leaf_records = BigEndian::read_u32(&n0[14 + 6..14 + 10]);
    let first_leaf = BigEndian::read_u32(&n0[14 + 10..14 + 14]);
    let last_leaf = BigEndian::read_u32(&n0[14 + 14..14 + 18]);
    let node_size = BigEndian::read_u16(&n0[14 + 18..14 + 20]);
    let max_key_len = BigEndian::read_u16(&n0[14 + 20..14 + 22]);
    let total_nodes = BigEndian::read_u32(&n0[14 + 22..14 + 26]);
    let free_nodes = BigEndian::read_u32(&n0[14 + 26..14 + 30]);
    eprintln!(
        "  depth={} root={} first_leaf={} last_leaf={} node_size={} max_key_len={} total_nodes={} free_nodes={} leaf_records={}",
        depth, root_node, first_leaf, last_leaf, node_size, max_key_len, total_nodes, free_nodes, leaf_records
    );

    // Read first leaf node.
    let leaf_off = (first_leaf as u64) * (node_size as u64);
    let (leaf_dev, leaf_avail) = translate(leaf_off).expect("translate first leaf");
    eprintln!(
        "== reading first leaf (node {}) at fork off {} -> dev off {} ({} bytes available) ==",
        first_leaf, leaf_off, leaf_dev, leaf_avail
    );
    if (leaf_avail as usize) < node_size as usize {
        eprintln!(
            "  WARNING: first leaf straddles an extent boundary (avail={} < node_size={}); probe would bail."
        , leaf_avail, node_size);
    }
    r.seek(SeekFrom::Start(leaf_dev))?;
    let mut leaf = vec![0u8; node_size as usize];
    r.read_exact(&mut leaf)?;
    let kind = leaf[8] as i8;
    let height = leaf[9];
    let num_records = BigEndian::read_u16(&leaf[10..12]);
    eprintln!(
        "  kind={} (-1=leaf, 0=index) height={} num_records={}",
        kind, height, num_records
    );

    // Offset table at end of node, MSB-first (offsets are big-endian u16,
    // table grows downward from the very end).
    let n = num_records as usize;
    let read_offset = |idx: usize| -> u16 {
        let pos = node_size as usize - 2 - idx * 2;
        BigEndian::read_u16(&leaf[pos..pos + 2])
    };
    eprintln!("  record offsets:");
    for i in 0..n.min(8) {
        eprintln!("    [{}] = {}", i, read_offset(i));
    }

    // Inspect record 0 in detail.
    let r0 = read_offset(0) as usize;
    let r1 = if n > 1 {
        read_offset(1) as usize
    } else {
        node_size as usize
    };
    eprintln!(
        "  record 0 spans bytes {}..{} (len={})",
        r0,
        r1,
        r1.saturating_sub(r0)
    );
    if r0 + 8 > leaf.len() {
        eprintln!("  record 0 too short to read key header");
        return Ok(());
    }
    let key_len = BigEndian::read_u16(&leaf[r0..r0 + 2]) as usize;
    let parent_id = BigEndian::read_u32(&leaf[r0 + 2..r0 + 6]);
    let name_len = BigEndian::read_u16(&leaf[r0 + 6..r0 + 8]) as usize;
    eprintln!(
        "  key: key_len={} parent_id={} name_len={} (UTF-16 units)",
        key_len, parent_id, name_len
    );

    let body_start = r0 + 8;
    let body_end = body_start + name_len * 2;
    if body_end <= leaf.len() && body_end <= r0 + 2 + key_len {
        let mut units: Vec<u16> = Vec::with_capacity(name_len);
        for i in 0..name_len {
            units.push(BigEndian::read_u16(
                &leaf[body_start + i * 2..body_start + i * 2 + 2],
            ));
        }
        let name = String::from_utf16_lossy(&units);
        eprintln!("  decoded name: {:?}", name);
    } else {
        eprintln!(
            "  key body out of bounds: body_end={} key_max={} leaf_len={}",
            body_end,
            r0 + 2 + key_len,
            leaf.len()
        );
    }

    // Also dump first 64 bytes of record 0 as hex for visual inspection.
    let dump_end = (r0 + 64).min(leaf.len());
    let hex: Vec<String> = leaf[r0..dump_end]
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect();
    eprintln!("  hex (first 64 of rec 0): {}", hex.join(" "));

    Ok(())
}
