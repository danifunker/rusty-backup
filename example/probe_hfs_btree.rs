use byteorder::{BigEndian, ByteOrder};
use rusty_backup::fs::hfs::HfsFilesystem;
use rusty_backup::partition::apm::Apm;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: probe_hfs_btree <path>");
    let mut f = File::open(&path).unwrap();
    let block_size_phys = 512u64;

    let off = match Apm::parse(&mut f) {
        Ok(apm) => {
            println!("APM entries:");
            for e in &apm.entries {
                println!(
                    "  type={:<24} start_block={:>8} block_count={:>10} status=0x{:08x}",
                    e.partition_type, e.start_block, e.block_count, e.status
                );
            }
            let hfs_entry = apm
                .entries
                .iter()
                .find(|e| e.partition_type == "Apple_HFS")
                .expect("no Apple_HFS in APM");
            hfs_entry.start_block as u64 * block_size_phys
        }
        Err(_) => {
            println!("No APM detected; treating image as a bare HFS volume at offset 0.");
            0
        }
    };

    let mut fs = HfsFilesystem::open(File::open(&path).unwrap(), off).unwrap();
    let s = fs.volume_summary();
    println!(
        "\nHFS volume:\n  name={:?}\n  block_size={}\n  total_blocks={}\n  free_blocks={}\n  files={}\n  folders={}\n  catalog_file_size={} ({} KiB)\n  extents_file_size={} ({} KiB)",
        s.volume_name,
        s.block_size,
        s.total_blocks,
        s.free_blocks,
        s.file_count,
        s.folder_count,
        s.catalog_file_size,
        s.catalog_file_size / 1024,
        s.extents_file_size,
        s.extents_file_size / 1024,
    );

    // Read the catalog raw data to count nodes/records.
    // We can't access fs.catalog_data() directly from here (private), so go
    // via list_children traversal. Instead read bytes 0..4 of each candidate
    // node by re-opening and using the public fork-read API isn't available
    // either. Easiest: use volume_summary + raw on-disk peek of the catalog
    // header node.
    drop(fs);
    let mut f = File::open(&path).unwrap();
    f.seek(SeekFrom::Start(off + 1024)).unwrap();
    let mut mdb = [0u8; 512];
    f.read_exact(&mut mdb).unwrap();
    let block_size = BigEndian::read_u32(&mdb[20..24]);
    let first_alloc = BigEndian::read_u16(&mdb[28..30]) as u64;
    // catalog extents at MDB offset 150..162
    let cat_start_blk = BigEndian::read_u16(&mdb[150..152]) as u64;
    let cat_blk_count = BigEndian::read_u16(&mdb[152..154]) as u64;
    let cat_byte_off = off + first_alloc * 512 + cat_start_blk * block_size as u64;
    println!(
        "\nCatalog first extent: start_block={} block_count={} byte_offset={}",
        cat_start_blk, cat_blk_count, cat_byte_off
    );

    f.seek(SeekFrom::Start(cat_byte_off)).unwrap();
    let mut header_node = vec![0u8; 4096];
    f.read_exact(&mut header_node).unwrap();
    // BTHeaderRec layout (HFS classic), record 0 of header node, starts at offset 14:
    //   depth u16, rootNode u32, leafRecords u32, firstLeafNode u32,
    //   lastLeafNode u32, nodeSize u16, maxKeyLength u16,
    //   totalNodes u32, freeNodes u32
    let h = &header_node[14..];
    let depth = BigEndian::read_u16(&h[0..2]);
    let root_node = BigEndian::read_u32(&h[2..6]);
    let leaf_recs = BigEndian::read_u32(&h[6..10]);
    let first_leaf = BigEndian::read_u32(&h[10..14]);
    let last_leaf = BigEndian::read_u32(&h[14..18]);
    let node_size = BigEndian::read_u16(&h[18..20]);
    let total_nodes = BigEndian::read_u32(&h[22..26]);
    let free_nodes = BigEndian::read_u32(&h[26..30]);
    println!(
        "Catalog BTHeaderRec:\n  node_size={}\n  total_nodes={}\n  free_nodes={}\n  used_nodes={}\n  depth={}\n  root_node={}\n  first_leaf={}\n  last_leaf={}\n  leaf_records={}",
        node_size,
        total_nodes,
        free_nodes,
        total_nodes - free_nodes,
        depth,
        root_node,
        first_leaf,
        last_leaf,
        leaf_recs
    );
    println!(
        "  catalog file capacity at this node_size: {} KiB ({} nodes)",
        (total_nodes as u64 * node_size as u64) / 1024,
        total_nodes
    );
}
