//! Replay (or with `--zero`, discard; `--check`, inspect) the internal log of a bare XFS image.
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};

use fs_xfs::xfs::log_replay::{check_log, replay_log, zero_log};
use fs_xfs::xfs::sb::XfsSuperblock;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = args.get(1).expect("usage: xfs_log_replay IMAGE [--zero]");
    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut sec = [0u8; 512];
    f.seek(SeekFrom::Start(0)).unwrap();
    f.read_exact(&mut sec).unwrap();
    let sb = XfsSuperblock::parse(&sec).unwrap();
    if args.iter().any(|a| a == "--check") {
        println!("{:?}", check_log(&mut f, 0, &sb).unwrap());
    } else if args.iter().any(|a| a == "--zero") {
        zero_log(&mut f, 0, &sb).unwrap();
        println!("log zeroed");
    } else {
        println!("{:?}", replay_log(&mut f, 0, &sb).unwrap());
    }
}
