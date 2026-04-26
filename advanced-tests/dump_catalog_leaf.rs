use byteorder::{BigEndian, ByteOrder};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

fn read_at(f: &mut File, off: u64, len: usize) -> Vec<u8> {
    f.seek(SeekFrom::Start(off)).unwrap();
    let mut v = vec![0u8; len];
    f.read_exact(&mut v).unwrap();
    v
}

fn main() {
    let path = std::env::args().nth(1).unwrap();
    let mut f = File::open(&path).unwrap();
    let part = 96u64 * 512;
    let mdb = read_at(&mut f, part + 1024, 162);
    let block_size = BigEndian::read_u32(&mdb[20..24]) as u64;
    let first_alloc = BigEndian::read_u16(&mdb[28..30]) as u64;
    let cat_size = BigEndian::read_u32(&mdb[146..150]) as u64;
    let cat_start_block = BigEndian::read_u16(&mdb[150..152]) as u64;
    let cat_off = part + first_alloc * 512 + cat_start_block * block_size;
    let cat = read_at(&mut f, cat_off, cat_size as usize);

    let depth = BigEndian::read_u16(&cat[14..16]);
    let root = BigEndian::read_u32(&cat[16..20]);
    let leaf_recs = BigEndian::read_u32(&cat[20..24]);
    let first_leaf = BigEndian::read_u32(&cat[24..28]) as usize;
    let last_leaf = BigEndian::read_u32(&cat[28..32]) as usize;
    let node_size = BigEndian::read_u16(&cat[32..34]) as usize;
    let total_nodes = BigEndian::read_u32(&cat[36..40]) as usize;
    println!("Catalog: node_size={node_size} total={total_nodes} depth={depth} root={root} first={first_leaf} last={last_leaf} leaf_recs={leaf_recs}");

    let mut node_idx = first_leaf;
    let mut leaves = 0;
    let mut total_recs = 0u32;
    let mut order_errs = 0u32;
    let mut chain_breaks = 0u32;
    let mut prev_node: Option<usize> = None;
    let mut prev_key_global: Option<(u32, Vec<u8>)> = None;
    while node_idx != 0 {
        let off = node_idx * node_size;
        if off + node_size > cat.len() {
            println!("OOB node {node_idx}");
            break;
        }
        let n = &cat[off..off + node_size];
        let flink = BigEndian::read_u32(&n[0..4]) as usize;
        let blink = BigEndian::read_u32(&n[4..8]) as usize;
        let nrecs = BigEndian::read_u16(&n[10..12]) as usize;
        if let Some(p) = prev_node {
            if blink != p {
                chain_breaks += 1;
                if chain_breaks <= 3 {
                    println!("  chain break node {node_idx} blink={blink} expected={p}");
                }
            }
        }
        for r in 0..nrecs {
            let off_pos = node_size - 2 * (r + 1);
            let rec_off = BigEndian::read_u16(&n[off_pos..off_pos + 2]) as usize;
            let key_len = n[rec_off] as usize;
            if rec_off + 1 + key_len > node_size {
                continue;
            }
            let parent = BigEndian::read_u32(&n[rec_off + 2..rec_off + 6]);
            let name_len = n[rec_off + 6] as usize;
            let name = &n[rec_off + 7..rec_off + 7 + name_len];
            let upper: Vec<u8> = name
                .iter()
                .map(|&b| if b.is_ascii_lowercase() { b - 32 } else { b })
                .collect();
            let this_key = (parent, upper);
            if let Some(pk) = &prev_key_global {
                if *pk >= this_key {
                    order_errs += 1;
                    if order_errs <= 8 {
                        println!("  ORDER ERR node {node_idx} rec {r}: prev=(parent={}, {:?}) >= this=(parent={}, {:?})",
                            pk.0, String::from_utf8_lossy(&pk.1), this_key.0, String::from_utf8_lossy(&this_key.1));
                    }
                }
            }
            prev_key_global = Some(this_key);
        }
        total_recs += nrecs as u32;
        leaves += 1;
        prev_node = Some(node_idx);
        node_idx = flink;
        if leaves > total_nodes {
            println!("loop");
            break;
        }
    }
    println!("leaves={leaves} recs={total_recs} (header {leaf_recs}) order_errs={order_errs} chain_breaks={chain_breaks}");
}
