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
    let drNxtCNID = BigEndian::read_u32(&mdb[30..34]);
    let drFilCnt = BigEndian::read_u32(&mdb[84..88]);
    let drDirCnt = BigEndian::read_u32(&mdb[88..92]);
    println!(
        "MDB drNxtCNID={} drFilCnt={} drDirCnt={}",
        drNxtCNID, drFilCnt, drDirCnt
    );

    let mut node_idx = first_leaf;
    let mut max_cnid = 0u32;
    let mut file_count = 0u32;
    let mut dir_count = 0u32;
    let mut total_records = 0u32;
    while node_idx != 0 {
        let off = node_idx * node_size;
        let n = &cat[off..off + node_size];
        let flink = BigEndian::read_u32(&n[0..4]) as usize;
        let nrecs = BigEndian::read_u16(&n[10..12]) as usize;
        for r in 0..nrecs {
            let off_pos = node_size - 2 * (r + 1);
            let rec_off = BigEndian::read_u16(&n[off_pos..off_pos + 2]) as usize;
            let key_len = n[rec_off] as usize;
            let key_total = 1 + key_len;
            let pad = key_total & 1;
            let data_off = rec_off + key_total + pad;
            let rec_type = n[data_off] as i8;
            // type: 1=dir, 2=file, 3=dir thread, 4=file thread
            if rec_type == 1 {
                let dir_id = BigEndian::read_u32(&n[data_off + 6..data_off + 10]);
                if dir_id > max_cnid {
                    max_cnid = dir_id;
                }
                if dir_id != 2 {
                    dir_count += 1;
                }
            } else if rec_type == 2 {
                let file_id = BigEndian::read_u32(&n[data_off + 20..data_off + 24]);
                if file_id > max_cnid {
                    max_cnid = file_id;
                }
                file_count += 1;
            }
            total_records += 1;
        }
        node_idx = flink;
    }
    println!(
        "Walked: total_recs={} files={} dirs={} (excluding root)",
        total_records, file_count, dir_count
    );
    println!("max CNID seen in catalog: {}", max_cnid);
    println!(
        "drNxtCNID should be > max_cnid: {} > {} ? {}",
        drNxtCNID,
        max_cnid,
        drNxtCNID > max_cnid
    );
}
