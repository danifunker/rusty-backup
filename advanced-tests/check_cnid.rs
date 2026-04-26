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
    let bs = BigEndian::read_u32(&mdb[20..24]) as u64;
    let fa = BigEndian::read_u16(&mdb[28..30]) as u64;
    let cs = BigEndian::read_u32(&mdb[146..150]) as u64;
    let csb = BigEndian::read_u16(&mdb[150..152]) as u64;
    let nxtCNID = BigEndian::read_u32(&mdb[30..34]);
    let cat = read_at(&mut f, part + fa * 512 + csb * bs, cs as usize);
    let ns = BigEndian::read_u16(&cat[32..34]) as usize;
    let mut node_idx = BigEndian::read_u32(&cat[24..28]) as usize;
    let mut max_cnid = 0u32;
    while node_idx != 0 {
        let off = node_idx * ns;
        let n = &cat[off..off + ns];
        let nrec = BigEndian::read_u16(&n[10..12]) as usize;
        for r in 0..nrec {
            let op = ns - 2 * (r + 1);
            let ro = BigEndian::read_u16(&n[op..op + 2]) as usize;
            let kl = n[ro] as usize;
            let kt = 1 + kl;
            let pad = kt & 1;
            let dof = ro + kt + pad;
            let rt = n[dof] as i8;
            // 1=dir 2=file 3=dir thread 4=file thread
            let cnid = match rt {
                1 => BigEndian::read_u32(&n[dof + 6..dof + 10]), // dir.dirID
                2 => BigEndian::read_u32(&n[dof + 20..dof + 24]), // file.filFlNum
                _ => 0,
            };
            if cnid > max_cnid {
                max_cnid = cnid;
            }
        }
        node_idx = BigEndian::read_u32(&n[0..4]) as usize;
    }
    println!("drNxtCNID = {} (0x{:x})", nxtCNID, nxtCNID);
    println!("max CNID seen in catalog = {} (0x{:x})", max_cnid, max_cnid);
    println!(
        "drNxtCNID > max ? {}  (must be true; equal triggers scavenge)",
        nxtCNID > max_cnid
    );
}
