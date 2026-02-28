# Filesystem Editing Feature — Implementation Plan

## Context

Rusty Backup currently supports read-only filesystem operations (browsing, extraction, compaction, backup/restore). This plan adds the ability to **add and delete files and folders** on disk images, VHD files, backup archives, and physical devices — across all supported filesystems except ProDOS.

**Supported targets:** Raw images (.img), VHD files, backup archives (CHD/zstd — decompress to temp, edit, recompress), physical devices.

**Implementation order:** FAT → exFAT → NTFS → ext2/3/4 → HFS/HFS+ → btrfs

---

## Phase 0: Core Trait & GUI Scaffolding

### 0.1 — `EditableFilesystem` Trait

**File:** `src/fs/filesystem.rs`

New trait extending `Filesystem`:

```
EditableFilesystem: Filesystem {
    create_file(parent, name, data, data_len, options) -> FileEntry
    create_directory(parent, name, options) -> FileEntry
    delete_entry(entry) -> ()                    // files, empty dirs, symlinks
    delete_recursive(entry) -> ()                // default impl: recurse children then delete_entry
    set_permissions(entry, mode) -> ()           // no-op for FAT/exFAT
    set_type_creator(entry, type_code, creator) -> ()   // HFS/HFS+ only
    write_resource_fork(entry, data, len) -> ()         // HFS/HFS+ only
    sync_metadata() -> ()                        // flush superblock counts, bitmaps, etc.
    free_space() -> u64
}
```

**Options structs:**
- `CreateFileOptions` — mode (default 0o100666), uid, gid, type_code, creator_code, resource_fork
- `CreateDirectoryOptions` — mode (default 0o40777), uid, gid

### 0.2 — Routing Function

**File:** `src/fs/mod.rs`

Add `open_editable_filesystem<R: Read + Write + Seek + Send + 'static>(...)` — same dispatch logic as `open_filesystem` but requires writable reader, returns `Box<dyn EditableFilesystem>`. ProDOS returns `Unsupported` error.

### 0.3 — GUI Edit Mode

**File:** `src/gui/browse_view.rs`

- [ ] Add "Edit Mode" toggle button in BrowseView toolbar
- [ ] When enabled, show action buttons: "Add File...", "Add Folder...", "Delete" on selected entries
- [ ] "Add File" dialog: host file picker, name field (pre-filled from source), optional permissions field (Unix fs), optional type/creator override (HFS)
- [ ] "Add Folder" dialog: name field, optional permissions
- [ ] "Properties" panel for selected entries: editable permissions (ext/btrfs), editable type/creator (HFS/HFS+)
- [ ] Delete confirmation dialog with recursive option for non-empty directories
- [ ] HFS-specific: "Add Resource Fork..." option when adding/editing files
- [ ] Free space indicator in edit mode
- [ ] Disable edit mode for Clonezilla streaming sources

**Opening writable handles:**
- Raw images/VHD: `OpenOptions::new().read(true).write(true)`
- Backup archives: decompress partition to temp file, edit, recompress on save
- Physical devices: same elevation path as restore

**Pattern:** Each edit operation opens a fresh `EditableFilesystem` instance, performs the write, calls `sync_metadata()`, then invalidates the directory cache (consistent with existing BrowseView pattern of creating new fs instances per operation).

### 0.4 — Backup Archive Editing Flow

For CHD/zstd backup archives:
1. Decompress target partition to temporary raw image file
2. Open temp image with `open_editable_filesystem`
3. User performs edits (each edit syncs to temp image)
4. On "Save" or closing edit mode: recompress temp image back to archive format
5. Replace original partition file in backup folder
6. Update metadata.json checksums

---

## Phase 1: FAT12/16/32

**File:** `src/fs/fat.rs`

### Internal helpers to add:
- [ ] `find_free_clusters(count)` — scan FAT for entries == 0x000
- [ ] `allocate_cluster_chain(count)` — chain free clusters, return first cluster
- [ ] `free_cluster_chain(start)` — walk chain setting each FAT entry to 0
- [ ] `write_cluster_data(cluster, data)` — seek to cluster offset, write
- [ ] `generate_short_name(long_name)` — 8.3 name with ~N numeric tail for collisions
- [ ] `write_lfn_entries(parent, long_name, sfn, sfn_checksum)` — create LFN directory entry chain (13 UCS-2 chars per entry, reverse order)
- [ ] `add_directory_entry(parent_cluster, name, attrs, first_cluster, size)` — find free 32-byte slot(s) for SFN + LFN entries; allocate new dir cluster if needed
- [ ] `remove_directory_entry(parent_cluster, entry)` — mark SFN entry byte[0] = 0xE5, mark all preceding LFN entries likewise, then free cluster chain
- [ ] `update_fsinfo(delta_free, delta_used)` — FAT32 only: patch FSInfo sector

### Filesystem considerations:
- **No permissions** — `set_permissions` is no-op
- **Timestamps** — set creation/modification to current time
- **FAT12/16 root directory** — fixed size (`root_entry_count`), error if full
- **LFN** — required for names > 8.3 or containing non-CP437 characters
- **Directory creation** — allocate 1 cluster, write `.` and `..` entries, set directory attribute bit

### Deletion:
- Files: mark dir entries 0xE5, free cluster chain
- Empty directories: same (must contain only `.` and `..`)
- Symlinks: N/A (FAT has no symlinks)

---

## Phase 2: exFAT

**File:** `src/fs/exfat.rs`

### Internal helpers to add:
- [ ] `allocate_clusters(count)` — set bits in allocation bitmap, return first cluster
- [ ] `free_clusters(start, count)` — clear bits in allocation bitmap
- [ ] `create_entry_set(parent, file_85, stream_c0, name_c1s)` — find contiguous free slots in parent directory, write File(0x85) + StreamExtension(0xC0) + FileName(0xC1) entries
- [ ] `delete_entry_set(parent, entry)` — mark File entry as 0x05 (deleted), free clusters
- [ ] `validate_filename_upcase(name)` — validate against upcase table
- [ ] `recalculate_boot_checksum()` — hash 11 VBR sectors, write to sector 12 (128 repeats), update both primary and backup boot regions
- [ ] `compute_entry_set_checksum(entries)` — checksum for directory entry set

### Filesystem considerations:
- **No permissions** — `set_permissions` is no-op
- **Boot checksum** — MUST recalculate after any metadata change (unique to exFAT)
- **Entry set checksums** — each directory entry set has its own checksum
- **No contiguous flag** — for simplicity, always use FAT chain allocation (NoFatChain=0)
- **Timestamps** — set to current time with UTC offset

### Deletion:
- Files: mark entry set byte[0] from 0x85→0x05, free clusters
- Empty directories: same
- Symlinks: N/A

---

## Phase 3: NTFS

**File:** `src/fs/ntfs.rs`

### Internal helpers to add:
- [ ] `allocate_mft_record()` — find free bit in $MFT's $Bitmap attribute, initialize 1024-byte record with "FILE" header and fixup array
- [ ] `free_mft_record(record_num)` — clear $MFT $Bitmap bit, mark record not-in-use
- [ ] `allocate_clusters(count)` — scan volume $Bitmap for free runs, set bits
- [ ] `free_clusters(runs)` — clear bits in volume $Bitmap
- [ ] `write_resident_data(mft_record, attr_type, data)` — store data inline in MFT record (for small files <~700 bytes)
- [ ] `write_nonresident_data(mft_record, attr_type, data)` — encode data runs, allocate clusters, write data
- [ ] `build_standard_information_attr()` — timestamps, permissions
- [ ] `build_filename_attr(parent_ref, name)` — $FILE_NAME attribute
- [ ] `add_index_entry(parent_mft, child_ref, filename_attr)` — insert into directory's $INDEX_ROOT or $INDEX_ALLOCATION B+ tree
- [ ] `remove_index_entry(parent_mft, child_ref)` — remove from B+ tree
- [ ] `inherit_security_descriptor(parent_mft)` — copy or reference parent's $SECURITY_DESCRIPTOR

### Filesystem considerations:
- **Security descriptors** — inherit from parent directory
- **MFT fixup arrays** — must be maintained on every MFT record write
- **B+ tree index** — directory indexes require node splitting on overflow; for MVP, handle insertion into nodes with available space + single-level splits
- **Resident vs non-resident** — small files stored in MFT record; larger files need cluster allocation
- **$BITMAP updates** — both MFT bitmap and volume bitmap
- **Timestamps** — NTFS uses 100ns intervals since 1601-01-01

### Deletion:
- Files: remove index entry from parent, free clusters, free MFT record
- Empty directories: same
- Symlinks: remove index entry, free MFT record (reparse point data freed with record)

---

## Phase 4: ext2/3/4

**File:** `src/fs/ext.rs`

### Internal helpers to add:
- [ ] `allocate_inode(preferred_group)` — find free bit in inode bitmap, initialize inode struct, decrement group descriptor free inode count
- [ ] `free_inode(ino)` — clear inode bitmap bit, zero inode, increment free count
- [ ] `allocate_blocks(count, preferred_group)` — find free bits in block bitmaps, set them, update group descriptor free block counts
- [ ] `free_blocks(block_list)` — clear block bitmap bits, update counts
- [ ] `write_inode(ino, inode_data)` — seek to inode table position, write inode
- [ ] `add_dir_entry(parent_ino, name, child_ino, file_type)` — find space in parent's directory data blocks (split existing entry's rec_len padding), or allocate new block
- [ ] `remove_dir_entry(parent_ino, name)` — merge rec_len with previous entry (or mark inode=0 for first entry)
- [ ] `update_superblock_counts()` — sync free inodes/blocks in superblock + all group descriptors
- [ ] `zero_journal()` — clear journal inode's data blocks (ext3/4 only)
- [ ] `setup_extent(ino, blocks)` — create extent tree entries (ext4) or set direct/indirect block pointers (ext2/3)

### Filesystem considerations:
- **Permissions** — default file mode 0o100666, directory mode 0o40777; user-configurable
- **uid/gid** — default 0 (root); user can override
- **Link counts** — files: i_links_count=1; directories: 2 + number of subdirectories; parent's link count incremented when adding subdirectory
- **Journal (ext3/4)** — zero journal data blocks on first edit to prevent stale replay; on subsequent edits before mount, journal is already zeroed so no action needed. Show info message: "Journal has been reset. First mount may take a moment to reinitialize."
- **Extents vs indirect blocks** — use extent tree for ext4 (check INCOMPAT_EXTENTS flag); use direct/indirect block pointers for ext2/3
- **Block groups** — prefer allocating inodes and blocks in the same group for locality
- **Timestamps** — set to current Unix epoch; use nanosecond fields if inode size >= 256

### Deletion:
- Files: remove dir entry, free all data blocks (walk extents or indirect chains), free inode, decrement parent link count if needed
- Empty directories: same + decrement parent's i_links_count
- Symlinks: remove dir entry, free inode (inline symlinks have no data blocks; long symlinks free the data block)

---

## Phase 5: HFS/HFS+

**Files:** `src/fs/hfs.rs`, `src/fs/hfsplus.rs`

### Internal helpers to add (HFS+):
- [ ] `allocate_blocks(count)` — flip bits in allocation file (bitmap)
- [ ] `free_blocks(start, count)` — clear bits in allocation file
- [ ] `insert_catalog_record(parent_cnid, name, record)` — insert file/folder record into catalog B-tree leaf node; handle node splits (allocate new node, redistribute keys, update parent index node, potentially grow tree height)
- [ ] `delete_catalog_record(parent_cnid, name)` — remove from catalog B-tree leaf; for MVP, mark as deleted without node merging
- [ ] `insert_thread_record(cnid, parent_cnid, name)` — reverse-lookup record in catalog
- [ ] `delete_thread_record(cnid)` — remove thread record
- [ ] `write_data_fork(cnid, data, len)` — allocate extents, write data blocks, update catalog record's data fork descriptor
- [ ] `write_resource_fork(cnid, data, len)` — same for resource fork extents
- [ ] `set_finder_info(cnid, type_code, creator_code)` — patch fdType/fdCreator in catalog file record (offset 48/52 in HFS+ file record)
- [ ] `update_volume_header()` — file count, folder count, free blocks, next catalog node ID (nextCNID)
- [ ] `update_backup_volume_header()` — copy VH to second-to-last 512-byte sector

### Classic HFS (`hfs.rs`):
- [ ] Same operations but with simpler B-tree (512-byte nodes), MDB instead of volume header
- [ ] Mac Roman filename encoding instead of Unicode
- [ ] 16-bit block pointers and extent descriptors

### Type/Creator Code Handling:
- [ ] Load `assets/hfs_file_types.json` at filesystem open time
- [ ] On `create_file`: look up file extension in `extensions` map → set fdType/fdCreator automatically
- [ ] Expose `set_type_creator()` for manual override (both new and existing files)
- [ ] GUI: show type/creator fields in Properties panel, pre-filled from auto-detection, editable

### Resource Fork Handling:
- [ ] `CreateFileOptions::resource_fork` accepts `ResourceForkSource::File(PathBuf)` or `ResourceForkSource::Data(Vec<u8>)`
- [ ] GUI "Add File" dialog: optional "Resource Fork..." file picker (HFS/HFS+ only)
- [ ] GUI Properties panel: "Add/Replace Resource Fork..." for existing files
- [ ] Resource fork stored in separate fork extents in catalog record

### Filesystem considerations:
- **B-tree catalog** — most complex part; node splitting must propagate up the tree; may need to increase tree depth
- **Catalog Node IDs (CNIDs)** — monotonically increasing; use nextCNID from volume header
- **Backup volume header** — must be updated in `sync_metadata()`
- **Encoding** — HFS+ uses UTF-16 decomposed (NFD) for filenames; HFS uses Mac Roman
- **Timestamps** — seconds since 1904-01-01 (Mac epoch)

### Deletion:
- Files: delete catalog record + thread record, free data fork blocks, free resource fork blocks, decrement parent's valence, update volume header file count
- Empty directories: delete catalog record + thread record, decrement parent's valence, update folder count
- Symlinks: delete catalog record + thread record

---

## Phase 6: btrfs

**File:** `src/fs/btrfs.rs`

### Internal helpers to add:
- [ ] `cow_tree_path(leaf_key)` — copy-on-write from leaf to root: allocate new blocks for each modified node, update parent pointers, cascade to superblock root
- [ ] `allocate_extent(size)` — find free space via chunk tree, create extent item
- [ ] `free_extent(logical, size)` — remove extent item
- [ ] `insert_item(tree_root, key, data)` — insert item into B-tree leaf, splitting nodes if needed (with COW)
- [ ] `delete_item(tree_root, key)` — remove item from B-tree leaf (with COW); for MVP, leave freed space in node without merging
- [ ] `create_inode_item(ino, mode, size)` — insert INODE_ITEM into fs tree
- [ ] `insert_dir_item(parent_ino, name, child_ino)` — DIR_ITEM with name hash key
- [ ] `insert_dir_index(parent_ino, index, name, child_ino)` — DIR_INDEX for readdir ordering
- [ ] `insert_extent_data(ino, offset, disk_bytenr, size)` — EXTENT_DATA item for file data
- [ ] `recalc_node_checksum(node_addr)` — CRC32C of node contents (mandatory for every modified node)
- [ ] `increment_generation()` — bump transid in superblock and all modified nodes

### Filesystem considerations:
- **COW semantics** — every tree node modification requires allocating a new block, copying, modifying, and updating the parent (which itself must be COWed) — cascades to root
- **CRC32C checksums** — every tree node has a checksum that must be recalculated after modification
- **Generation numbers** — transid must be consistent across all modified nodes
- **Chunk tree** — maps logical addresses to physical; must be consulted for all block reads/writes
- **Permissions** — default file mode 0o100666, dir mode 0o40777; stored in INODE_ITEM
- **Mark as experimental** — warn user that btrfs editing is experimental due to COW complexity

### Deletion:
- Files: delete DIR_ITEM + DIR_INDEX from parent, delete INODE_ITEM + EXTENT_DATA items, free extents
- Empty directories: same (without EXTENT_DATA)
- Symlinks: delete DIR_ITEM + DIR_INDEX, delete INODE_ITEM + inline extent (symlink target stored inline)

---

## Cross-Cutting Concerns

### Name Validation (per filesystem)
| Filesystem | Max Length | Forbidden Characters | Encoding |
|-----------|-----------|---------------------|----------|
| FAT (SFN) | 8.3 | `"*+,/:;<=>?\[\]|` | CP437 |
| FAT (LFN) | 255 | `"*/:<>?\|` | UCS-2 |
| exFAT | 255 | `"*/:<>?\|` | UTF-16LE |
| NTFS | 255 | `"*/:<>?\|` | UTF-16LE |
| ext2/3/4 | 255 bytes | `/`, NUL | UTF-8 |
| HFS | 31 | `:` | Mac Roman |
| HFS+ | 255 | `:` | UTF-16 NFD |
| btrfs | 255 bytes | `/`, NUL | UTF-8 |

### Duplicate Name Handling
- Check for existing entry with same name before creating
- Return `FilesystemError::AlreadyExists` (new variant) if collision

### Free Space Checks
- Check `free_space()` before every file creation
- Account for metadata overhead (directory entries, inodes, bitmap updates)

### Concurrency
- Disable edit mode during active backup/restore operations
- Only one edit operation at a time (no concurrent writes)

### Error Recovery
- `sync_metadata()` after every operation for crash consistency
- If an operation fails mid-way (e.g., data written but directory not updated), best-effort cleanup: free allocated clusters/blocks

---

## Testing Strategy

### Per-Filesystem Unit Tests
For each filesystem, create small in-memory or temp-file images:
- [ ] Create file → list directory → verify entry exists → read back data matches
- [ ] Create directory → list parent → verify entry exists
- [ ] Delete file → list directory → verify entry gone → verify free space recovered
- [ ] Delete non-empty directory recursively → verify all entries gone
- [ ] Fill disk → verify `free_space() == 0` → verify create_file returns error
- [ ] Name validation: reject forbidden chars, max length, duplicates
- [ ] Permissions: set/get mode bits (ext/btrfs), verify no-op on FAT/exFAT

### Filesystem-Specific Tests
- **FAT:** LFN generation, 8.3 short name collision (~1, ~2...), FSInfo update, FAT12/16 fixed root directory full
- **exFAT:** Boot checksum recalculation, entry set checksum, allocation bitmap
- **NTFS:** Resident vs non-resident data, MFT fixup arrays, security descriptor inheritance
- **ext2/3/4:** Inode/block allocation across groups, journal zeroing, extent tree vs indirect blocks
- **HFS/HFS+:** Type/creator auto-detection from extension, manual override, resource fork write/read, B-tree node split, catalog thread records
- **btrfs:** COW chain verification, CRC32C checksums, generation numbers

### Round-Trip Tests
- Create image → add files via EditableFilesystem → read back via Filesystem (read-only) → verify
- Add files → delete some → verify remaining files intact
- FAT32: verify FSInfo free count matches actual FAT scan

### Integration Tests (where tooling available)
- FAT: verify with `mtools` (mdir, mcopy)
- ext: verify with `e2fsck -fn` (no-repair check)
- Mount edited images in loopback (Linux CI) to validate OS-level compatibility

---

## Implementation Tracking

### Status Key: [ ] Todo  [~] In Progress  [x] Done

- [ ] **Phase 0:** Core trait + GUI scaffolding
  - [ ] 0.1: `EditableFilesystem` trait + option structs in `filesystem.rs`
  - [ ] 0.2: `open_editable_filesystem()` in `mod.rs`
  - [ ] 0.3: Edit mode UI in `browse_view.rs`
  - [ ] 0.4: Backup archive edit flow (decompress/recompress)
- [ ] **Phase 1:** FAT12/16/32 editing
  - [ ] Cluster allocation/deallocation
  - [ ] Directory entry creation (SFN + LFN)
  - [ ] Directory entry deletion
  - [ ] Directory creation (. and .. entries)
  - [ ] FSInfo updates (FAT32)
  - [ ] Unit tests
- [ ] **Phase 2:** exFAT editing
  - [ ] Cluster allocation via bitmap
  - [ ] Entry set creation (0x85 + 0xC0 + 0xC1)
  - [ ] Entry set deletion
  - [ ] Boot checksum recalculation
  - [ ] Unit tests
- [ ] **Phase 3:** NTFS editing
  - [ ] MFT record allocation
  - [ ] Cluster allocation via $Bitmap
  - [ ] Resident/non-resident data writes
  - [ ] Directory index B+ tree insertion/removal
  - [ ] Security descriptor inheritance
  - [ ] Unit tests
- [ ] **Phase 4:** ext2/3/4 editing
  - [ ] Inode allocation/deallocation
  - [ ] Block allocation/deallocation
  - [ ] Directory entry add/remove
  - [ ] Journal zeroing (ext3/4)
  - [ ] Extent tree / indirect block support
  - [ ] Unit tests
- [ ] **Phase 5:** HFS/HFS+ editing
  - [ ] Allocation bitmap management
  - [ ] Catalog B-tree insertion with node splitting
  - [ ] Catalog B-tree deletion
  - [ ] Thread record management
  - [ ] Data fork + resource fork writes
  - [ ] Type/creator from hfs_file_types.json + manual override
  - [ ] Backup volume header update
  - [ ] Classic HFS support
  - [ ] Unit tests
- [ ] **Phase 6:** btrfs editing (experimental)
  - [ ] COW tree path implementation
  - [ ] Extent allocation/deallocation
  - [ ] B-tree item insertion/deletion with COW
  - [ ] CRC32C checksum recalculation
  - [ ] Generation number management
  - [ ] Unit tests
