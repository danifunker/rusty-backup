//! Dump the shape of a real SFS extent B-tree: root, interior fan-out, leaf
//! fill. Ground truth for the F-009 split work, since the reference C sources
//! are not on this machine.
//!
//! cargo run --release --example probe_sfs_btree -- <image>

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

fn rd_u16(b: &[u8], o: usize) -> u16 {
    u16::from_be_bytes([b[o], b[o + 1]])
}
fn rd_u32(b: &[u8], o: usize) -> u32 {
    u32::from_be_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}

fn main() {
    if std::env::var("PROBE_NODES").is_ok() {
        probe_nodes(&std::env::args().nth(1).expect("usage"));
        return;
    }
    if std::env::var("PROBE_ADMIN").is_ok() {
        probe_admin(&std::env::args().nth(1).expect("usage"));
        return;
    }
    let path = std::env::args().nth(1).expect("usage: probe <image>");
    let mut f = File::open(&path).expect("open");

    // Rootblock is at block 0 of the partition; this fixture is a superfloppy.
    let mut hdr = vec![0u8; 512];
    f.read_exact(&mut hdr).expect("read block 0");
    assert_eq!(
        &hdr[0..4],
        b"SFS\0",
        "not an SFS rootblock: {:?}",
        &hdr[0..4]
    );
    let version = rd_u16(&hdr, 12);
    let blocksize = rd_u32(&hdr, 52);
    let extentbnoderoot = rd_u32(&hdr, 108);
    let objectnoderoot = rd_u32(&hdr, 112);
    println!("version={version} blocksize={blocksize} extentbnoderoot={extentbnoderoot} objectnoderoot={objectnoderoot}");

    let bs = blocksize as usize;
    let read_block = |f: &mut File, blk: u32| -> Vec<u8> {
        let mut buf = vec![0u8; bs];
        f.seek(SeekFrom::Start(blk as u64 * bs as u64)).unwrap();
        f.read_exact(&mut buf).unwrap();
        buf
    };

    // Walk down the left spine, printing each level's node header, and tally
    // fill factors across every node at each level.
    let mut level = 0;
    let mut frontier = vec![extentbnoderoot];
    loop {
        let mut next: Vec<u32> = Vec::new();
        let mut counts: Vec<usize> = Vec::new();
        let mut isleaf_seen = 0u8;
        let mut nodesize_seen = 0usize;
        for &blk in &frontier {
            let buf = read_block(&mut f, blk);
            let id = &buf[0..4];
            let nodecount = rd_u16(&buf, 12) as usize;
            let isleaf = buf[14];
            let nodesize = buf[15] as usize;
            isleaf_seen = isleaf;
            nodesize_seen = nodesize;
            counts.push(nodecount);
            if blk == frontier[0] {
                println!(
                    "level {level}: blk {blk} id={:?} nodecount={nodecount} isleaf={isleaf} nodesize={nodesize} max={}",
                    String::from_utf8_lossy(id),
                    (bs - 16) / nodesize.max(1)
                );
                // First few entries verbatim.
                for i in 0..nodecount.min(4) {
                    let o = 16 + i * nodesize;
                    if isleaf == 0 {
                        println!(
                            "    [{i}] key={} child={}",
                            rd_u32(&buf, o),
                            rd_u32(&buf, o + 4)
                        );
                    } else {
                        println!(
                            "    [{i}] key={} next={} prev={} blocks={}",
                            rd_u32(&buf, o),
                            rd_u32(&buf, o + 4),
                            rd_u32(&buf, o + 8),
                            rd_u16(&buf, o + 12)
                        );
                    }
                }
            }
            if isleaf == 0 {
                for i in 0..nodecount {
                    next.push(rd_u32(&buf, 16 + i * nodesize + 4));
                }
            }
        }
        let total: usize = counts.iter().sum();
        let max = (bs - 16) / nodesize_seen.max(1);
        println!(
            "level {level}: {} node(s), entries min={} max={} avg={:.1} (capacity {max}, {:.0}% full)",
            counts.len(),
            counts.iter().min().copied().unwrap_or(0),
            counts.iter().max().copied().unwrap_or(0),
            total as f64 / counts.len() as f64,
            100.0 * (total as f64 / counts.len() as f64) / max as f64
        );
        // The invariant a split has to preserve: is an interior entry's key the
        // first key of the subtree it points at, and is the leftmost always 0?
        if isleaf_seen == 0 {
            let (mut equal, mut leftmost_zero, mut mismatched) = (0usize, 0usize, Vec::new());
            for &blk in &frontier {
                let buf = read_block(&mut f, blk);
                let nodecount = rd_u16(&buf, 12) as usize;
                for i in 0..nodecount {
                    let o = 16 + i * nodesize_seen;
                    let (key, child) = (rd_u32(&buf, o), rd_u32(&buf, o + 4));
                    let cbuf = read_block(&mut f, child);
                    let cnodesize = cbuf[15] as usize;
                    let cfirst = if rd_u16(&cbuf, 12) > 0 {
                        rd_u32(&cbuf, 16)
                    } else {
                        u32::MAX
                    };
                    if i == 0 && key == 0 {
                        leftmost_zero += 1;
                    } else if key == cfirst {
                        equal += 1;
                    } else if mismatched.len() < 5 {
                        mismatched.push((blk, i, key, child, cfirst, cnodesize));
                    }
                }
            }
            println!(
                "level {level}: leftmost-key-zero={leftmost_zero}, key==child_first={equal}, mismatched={}",
                mismatched.len()
            );
            for m in &mismatched {
                println!(
                    "    MISMATCH blk={} idx={} key={} child={} child_first={} child_nodesize={}",
                    m.0, m.1, m.2, m.3, m.4, m.5
                );
            }
        }
        if isleaf_seen != 0 || next.is_empty() {
            break;
        }
        frontier = next;
        level += 1;
        if level > 8 {
            break;
        }
    }
}

// Appended probe: the AdminSpaceContainer chain, which is where a split has to
// get blocks for new BNDC nodes.
#[allow(dead_code)]
fn probe_admin(path: &str) {
    let mut f = File::open(path).unwrap();
    let mut hdr = vec![0u8; 512];
    f.read_exact(&mut hdr).unwrap();
    let bs = rd_u32(&hdr, 52) as usize;
    let mut blk = rd_u32(&hdr, 100);
    let mut n = 0;
    while blk != 0 && n < 20 {
        let mut buf = vec![0u8; bs];
        f.seek(SeekFrom::Start(blk as u64 * bs as u64)).unwrap();
        f.read_exact(&mut buf).unwrap();
        let id = String::from_utf8_lossy(&buf[0..4]).to_string();
        let next = rd_u32(&buf, 12);
        let prev = rd_u32(&buf, 16);
        // adminspace[] entries start at 28: (space u32, bits u32) pairs.
        let mut free_total = 0;
        let mut entries = 0;
        let mut o = 28;
        while o + 8 <= bs {
            let space = rd_u32(&buf, o);
            let bits = rd_u32(&buf, o + 4);
            if space == 0 {
                break;
            }
            entries += 1;
            free_total += bits.count_zeros();
            if entries <= 3 {
                println!(
                    "    ADMC {blk} entry[{}] space={space} bits={bits:#010x} free={}",
                    entries - 1,
                    bits.count_zeros()
                );
            }
            o += 8;
        }
        println!("ADMC blk={blk} id={id} next={next} prev={prev} entries={entries} free_slots_total={free_total}");
        blk = next;
        n += 1;
    }
}

/// The object-node tree (NDC): fixed fan-out indexed by node number, not a
/// sorted B-tree. `nodes` is how many object-nodes one entry covers; nodes==1
/// is a leaf of 10-byte fsObjectNode records.
#[allow(dead_code)]
fn probe_nodes(path: &str) {
    let mut f = File::open(path).unwrap();
    let mut hdr = vec![0u8; 512];
    f.read_exact(&mut hdr).unwrap();
    let bs = rd_u32(&hdr, 52) as usize;
    // BLCKn = (BLCK << shifts_block32) | flags, shifts_block32 = log2(bs) - 5.
    let shifts = (bs as u32).trailing_zeros().saturating_sub(5);
    let mut blk = rd_u32(&hdr, 112);
    let mut level = 0;
    println!("objectnoderoot={blk} blocksize={bs} shift={shifts}");
    loop {
        let mut buf = vec![0u8; bs];
        f.seek(SeekFrom::Start(blk as u64 * bs as u64)).unwrap();
        f.read_exact(&mut buf).unwrap();
        let nodenumber = rd_u32(&buf, 12);
        let nodes = rd_u32(&buf, 16);
        let stride = if nodes == 1 { 10 } else { 4 };
        let slots = (bs - 20) / stride;
        let mut used = 0;
        let mut first_free = None;
        for i in 0..slots {
            let e = rd_u32(&buf, 20 + i * stride);
            if e != 0 {
                used += 1
            } else if first_free.is_none() {
                first_free = Some(i)
            }
        }
        println!(
            "level {level}: blk={blk} id={} nodenumber={nodenumber} nodes={nodes} stride={stride} slots={slots} used={used} first_free={first_free:?}",
            String::from_utf8_lossy(&buf[0..4])
        );
        if nodes == 1 {
            break;
        }
        // Descend into the first populated child.
        let mut child = 0;
        for i in 0..slots {
            let e = rd_u32(&buf, 20 + i * stride);
            if e != 0 {
                child = e >> shifts;
                break;
            }
        }
        if child == 0 {
            break;
        }
        blk = child;
        level += 1;
        if level > 6 {
            break;
        }
    }
}
