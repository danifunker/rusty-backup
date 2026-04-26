// Verify every index node: its records are in order, each record's key <= first key of pointed child node.
use byteorder::{BigEndian, ByteOrder};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

fn read_at(f: &mut File, off: u64, len: usize) -> Vec<u8> {
    f.seek(SeekFrom::Start(off)).unwrap();
    let mut v = vec![0u8; len];
    f.read_exact(&mut v).unwrap();
    v
}

fn extract_key(rec: &[u8]) -> (u32, Vec<u8>) {
    let parent = BigEndian::read_u32(&rec[2..6]);
    let nlen = rec[6] as usize;
    let name = &rec[7..7 + nlen];
    let upper: Vec<u8> = name
        .iter()
        .map(|&b| if b.is_ascii_lowercase() { b - 32 } else { b })
        .collect();
    (parent, upper)
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

    let node_size = BigEndian::read_u16(&cat[32..34]) as usize;
    let total_nodes = BigEndian::read_u32(&cat[36..40]) as usize;

    let get_node = |idx: usize| -> &[u8] {
        let off = idx * node_size;
        &cat[off..off + node_size]
    };
    let get_first_key = |idx: usize| -> Option<(u32, Vec<u8>)> {
        let n = get_node(idx);
        let nrecs = BigEndian::read_u16(&n[10..12]) as usize;
        if nrecs == 0 {
            return None;
        }
        // First record offset
        let off_pos = node_size - 2;
        let rec_off = BigEndian::read_u16(&n[off_pos..off_pos + 2]) as usize;
        Some(extract_key(&n[rec_off..]))
    };

    let mut bad_index_keys = 0u32;
    let mut wrong_pointer_first_key = 0u32;
    for idx in 0..total_nodes {
        let n = get_node(idx);
        let kind = n[8] as i8;
        if kind != 0 {
            continue;
        } // 0 = index node in HFS
        let nrecs = BigEndian::read_u16(&n[10..12]) as usize;
        let mut last_key: Option<(u32, Vec<u8>)> = None;
        for r in 0..nrecs {
            let off_pos = node_size - 2 * (r + 1);
            let rec_off = BigEndian::read_u16(&n[off_pos..off_pos + 2]) as usize;
            let key_len = n[rec_off] as usize;
            let key = extract_key(&n[rec_off..]);
            if let Some(prev) = &last_key {
                if *prev >= key {
                    bad_index_keys += 1;
                    if bad_index_keys <= 3 {
                        println!("idx {} rec {}: out-of-order keys", idx, r);
                    }
                }
            }
            // Pointer to child = u32 right after key (key_len rounded to even byte boundary)
            let key_bytes = 1 + key_len; // length byte + key bytes
            let pad = key_bytes & 1;
            let child_off = rec_off + key_bytes + pad;
            let child = BigEndian::read_u32(&n[child_off..child_off + 4]) as usize;
            if let Some(child_first) = get_first_key(child) {
                if child_first != key {
                    wrong_pointer_first_key += 1;
                    if wrong_pointer_first_key <= 5 {
                        println!("idx node {} rec {}: index_key=(p={},{:?}) but child {} first_key=(p={},{:?})",
                            idx, r, key.0, String::from_utf8_lossy(&key.1),
                            child, child_first.0, String::from_utf8_lossy(&child_first.1));
                    }
                }
            }
            last_key = Some(key);
        }
    }
    println!("bad_index_keys: {bad_index_keys}");
    println!("wrong_pointer_first_key: {wrong_pointer_first_key}");
}
