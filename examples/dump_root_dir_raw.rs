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
    let node_size = BigEndian::read_u16(&cat[32..34]) as usize;
    let first_leaf = BigEndian::read_u32(&cat[24..28]) as usize;

    let mut node_idx = first_leaf;
    let mut count = 0;
    while node_idx != 0 && count < 5 {
        let off = node_idx * node_size;
        let n = &cat[off..off + node_size];
        let flink = BigEndian::read_u32(&n[0..4]) as usize;
        let nrecs = BigEndian::read_u16(&n[10..12]) as usize;
        println!("=== Node {} ({} records) ===", node_idx, nrecs);
        for r in 0..nrecs.min(5) {
            let off_pos = node_size - 2 * (r + 1);
            let rec_off = BigEndian::read_u16(&n[off_pos..off_pos + 2]) as usize;
            let key_len = n[rec_off] as usize;
            let parent = BigEndian::read_u32(&n[rec_off + 2..rec_off + 6]);
            let name_len = n[rec_off + 6] as usize;
            let name = &n[rec_off + 7..rec_off + 7 + name_len];
            let key_total = 1 + key_len;
            let pad = key_total & 1;
            let data_off = rec_off + key_total + pad;
            // Compute next record start to know data length
            let next_rec_off = if r + 1 < nrecs {
                let next_pos = node_size - 2 * (r + 2);
                BigEndian::read_u16(&n[next_pos..next_pos + 2]) as usize
            } else {
                node_size - 2 * nrecs
            };
            let data_len = next_rec_off - data_off;
            println!(
                " rec {}: rec_off={} key_len={} parent={} name={:?}",
                r,
                rec_off,
                key_len,
                parent,
                String::from_utf8_lossy(name)
            );
            println!("   key bytes: {:02x?}", &n[rec_off..rec_off + 1 + key_len]);
            println!(
                "   data ({} bytes): {:02x?}",
                data_len,
                &n[data_off..data_off + data_len.min(80)]
            );
        }
        count += 1;
        node_idx = flink;
    }
}
