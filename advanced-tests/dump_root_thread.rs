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

    // Walk leaves looking for records with parent CNID == 2 and empty name (root thread)
    // and with key.parent == 2, and any record where data is for CNID 2.
    let mut node_idx = first_leaf;
    let mut found_root_thread = false;
    let mut found_root_dir = false;
    while node_idx != 0 {
        let off = node_idx * node_size;
        let n = &cat[off..off + node_size];
        let flink = BigEndian::read_u32(&n[0..4]) as usize;
        let nrecs = BigEndian::read_u16(&n[10..12]) as usize;
        for r in 0..nrecs {
            let off_pos = node_size - 2 * (r + 1);
            let rec_off = BigEndian::read_u16(&n[off_pos..off_pos + 2]) as usize;
            let key_len = n[rec_off] as usize;
            let parent = BigEndian::read_u32(&n[rec_off + 2..rec_off + 6]);
            let name_len = n[rec_off + 6] as usize;
            let name = &n[rec_off + 7..rec_off + 7 + name_len];
            // Data follows key, padded to even
            let key_total = 1 + key_len;
            let pad = key_total & 1;
            let data_off = rec_off + key_total + pad;
            let rec_type = n[data_off] as i8;
            // Look for parent=2, name="" (root thread record)
            if parent == 2 && name_len == 0 {
                found_root_thread = true;
                println!(
                    "ROOT THREAD record: rec_type={} (3=dir thread, 4=file thread)",
                    rec_type
                );
                // Thread record: type(1) + reserved(9) + parentID(4) + nameLen(1) + name(31)
                let thread_parent = BigEndian::read_u32(&n[data_off + 10..data_off + 14]);
                let thread_name_len = n[data_off + 14] as usize;
                let thread_name = &n[data_off + 15..data_off + 15 + thread_name_len.min(31)];
                println!("  thread.parent_id (should be 1) = {}", thread_parent);
                println!("  thread.name = {:?}", String::from_utf8_lossy(thread_name));
                println!(
                    "  raw thread bytes (first 48): {:02x?}",
                    &n[data_off..data_off + 48.min(n.len() - data_off)]
                );
            }
            // Look for parent=1, name="<volume name>" — actual root dir record
            if parent == 1 {
                found_root_dir = true;
                println!(
                    "ROOT DIR record: name={:?} rec_type={} (1=dir, 2=file)",
                    String::from_utf8_lossy(name),
                    rec_type
                );
                // Dir record (70B): type(1)+flags(1)+valence(2)+dirID(4)+...
                let valence = BigEndian::read_u16(&n[data_off + 2..data_off + 4]);
                let dir_id = BigEndian::read_u32(&n[data_off + 4..data_off + 8]);
                println!(
                    "  dir_id = {} (should be 2)  valence = {} (children of root)",
                    dir_id, valence
                );
            }
        }
        node_idx = flink;
    }
    if !found_root_thread {
        println!("!!! NO ROOT THREAD RECORD (parent=2, name='') — mount will fail !!!");
    }
    if !found_root_dir {
        println!("!!! NO ROOT DIR RECORD (parent=1) — mount will fail !!!");
    }
}
