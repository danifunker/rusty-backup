# Rusty Backup - Development Specification

## Project Overview
Build a cross-platform GUI application in Rust for backing up and restoring vintage computer hard disk images with partition resizing and CHD compression. The name "Rusty Backup" is a play on words - both referencing the Rust programming language and the fact that most vintage computers are quite literally rusty.

## Core Requirements

### Supported Filesystems
- FAT16
- FAT32
- NTFS
- ext2
- ext3
- ext4

**Note:** HFS/HFS+ support is planned for a future phase.

### Supported Partition Schemes
- MBR (Master Boot Record)
- GPT (GUID Partition Table)

### Key Features
1. **Backup Mode**: Read disk/partition → detect filesystem → optionally resize → compress with CHD → save to folder structure
2. **Restore Mode**: Read backup folder → decompress CHD files → write to target disk
3. **Inspect Mode**: Display partition table, filesystem info, used/total space
4. **Resize Logic**: Analyze filesystem, determine minimum size, shrink partition during backup
5. **CHD Compression**: All partition data compressed using CHD format (MAME standard)
6. **Folder-based Storage**: Each backup stored in a folder with JSON metadata, individual partition CHD files, and partition table backups
7. **Data Verification**: User-selectable CRC32 or SHA256 checksums per file
8. **Split Archives**: Optional splitting of large CHD files (default: just under 4GB for FAT32 compatibility)
9. **GUI Interface**: Easy-to-use graphical application with progress indicators and logging
10. **Safety**: Extensive validation, pre-flight disk space checks, bad sector handling

## Technical Architecture

### Project Structure
```
rusty-backup/
├── Cargo.toml
├── README.md
├── LICENSE (GPL-3.0)
├── src/
│   ├── main.rs              # GUI application entry point
│   ├── lib.rs               # Core library interface
│   ├── gui/
│   │   ├── mod.rs
│   │   ├── main_window.rs   # Main application window
│   │   ├── backup_tab.rs    # Backup interface
│   │   ├── restore_tab.rs   # Restore interface with partition layout config
│   │   ├── inspect_tab.rs   # Inspect interface
│   │   └── progress.rs      # Progress indicators and logging
│   ├── partition/
│   │   ├── mod.rs
│   │   ├── mbr.rs           # MBR parsing and export
│   │   └── gpt.rs           # GPT parsing and export
│   ├── filesystem/
│   │   ├── mod.rs           # Filesystem trait
│   │   ├── fat.rs           # FAT16/32 implementation
│   │   ├── ntfs.rs          # NTFS implementation
│   │   └── ext.rs           # ext2/3/4 implementation
│   ├── resize/
│   │   └── mod.rs           # Resize logic per filesystem
│   ├── chd/
│   │   ├── mod.rs           # CHD compression wrapper
│   │   └── split.rs         # CHD file splitting logic
│   ├── backup/
│   │   ├── mod.rs
│   │   ├── metadata.rs      # JSON metadata structures
│   │   ├── format.rs        # Backup folder structure
│   │   └── verify.rs        # Checksum verification
│   └── error.rs             # Error types
└── tests/
    ├── unit/
    └── integration/
```

### Rust Dependencies (Cargo.toml suggestions)
```toml
[package]
name = "rusty-backup"
version = "0.1.0"
edition = "2021"
license = "GPL-3.0"

[dependencies]
# GUI framework
eframe = "0.27"          # egui framework for cross-platform GUI
egui = "0.27"

# Core functionality
anyhow = "1"
thiserror = "1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# Filesystem support
fatfs = "0.3"
gpt = "3"

# CHD compression (libchdr bindings or pure Rust implementation)
# Note: May need to create FFI bindings to libchdr or find/create pure Rust implementation
# libchdr = { git = "https://github.com/rtissera/libchdr" }  # Example

# Checksums
crc32fast = "1"          # CRC32
sha2 = "0.10"            # SHA256

# For progress indication
indicatif = "0.17"

# For byte operations
byteorder = "1"

# Platform-specific disk access
[target.'cfg(unix)'.dependencies]
nix = { version = "0.28", features = ["mount"] }

[target.'cfg(windows)'.dependencies]
winapi = { version = "0.3", features = ["winioctl", "fileapi"] }

[dev-dependencies]
tempfile = "3"
```

## Implementation Phases

### Phase 1: Foundation (Start Here)
1. **GUI skeleton** with egui/eframe:
   - Create main window with tabs (Backup, Restore, Inspect)
   - Setup basic layout and navigation
   - Add logging panel at bottom of window
   - Create progress bar component
   
2. **Partition detection**:
   - Read first 512 bytes, detect MBR vs GPT
   - Parse partition tables, enumerate partitions
   - Display partition info in GUI table

3. **Basic disk I/O**:
   - Safe read/write with error handling
   - Support both block devices and image files
   - Sector-aligned operations (512 bytes or 4KB)
   - Platform-specific device enumeration

### Phase 2: Filesystem Detection and Alignment Analysis
1. **Filesystem trait**:
```rust
pub trait Filesystem {
    fn detect(data: &[u8]) -> bool;
    fn get_info(&self) -> FilesystemInfo;
    fn calculate_min_size(&self) -> Result<u64>;
    fn validate(&self) -> Result<()>;
}

pub struct FilesystemInfo {
    pub fs_type: FilesystemType,
    pub total_size: u64,
    pub used_size: u64,
    pub block_size: u32,
    pub label: Option<String>,
}
```

2. **Partition alignment detection**:
```rust
pub struct PartitionAlignment {
    pub first_lba: u64,
    pub alignment_sectors: u64,
    pub heads: u16,
    pub sectors_per_track: u16,
    pub alignment_type: AlignmentType,
}

pub enum AlignmentType {
    DosTraditional,      // LBA 63, cylinder boundaries (255×63)
    Modern1MB,           // LBA 2048, 1MB boundaries  
    Custom(u64),         // Other detected pattern
    None,                // No detectable pattern
}

fn detect_alignment(partition_table: &PartitionTable) -> PartitionAlignment {
    // 1. Read first partition start LBA
    // 2. Check if subsequent partitions align to common boundaries
    // 3. Extract CHS geometry from MBR partition table entries
    //    - CHS values are in partition table alongside LBA
    //    - Calculate sectors_per_cylinder = heads × sectors_per_track
    // 4. Identify pattern:
    //    - First at LBA 63 + multiples of 16065 → DosTraditional
    //    - First at LBA 2048 + multiples of 2048 → Modern1MB
    //    - Other consistent pattern → Custom
    //    - No pattern → None
}
```

3. **Implement detection logic** for each filesystem type
4. **Read filesystem metadata** (superblocks, allocation tables)

### Phase 3: FAT Implementation (Simplest First)
1. Use `fatfs` crate for FAT16/32
2. Calculate used clusters
3. Determine minimum partition size
4. Implement resize validation (check for files beyond shrink point)

**FAT-specific considerations:**
- Read FAT tables to find allocated clusters
- Calculate minimum size = (highest allocated cluster + overhead) * cluster_size
- Validate no fragmentation beyond shrink point

### Phase 4: Backup Format and Metadata
1. **Folder-based backup structure**:
```
backup-name/
├── metadata.json          # Backup metadata
├── mbr.bin               # Raw MBR (for MBR disks only)
├── mbr.json              # Human-readable MBR info (for MBR disks)
├── gpt.json              # GPT partition table (for GPT disks)
├── partition-0.chd       # Partition 0 compressed data
├── partition-0.chd.sha256  # or .crc32 depending on user choice
├── partition-1.chd       # Partition 1 compressed data
├── partition-1.chd.sha256
└── bad-sectors.json      # List of bad sectors encountered (if any)
```

2. **metadata.json structure**:
```json
{
  "version": "0.1.0",
  "created": "2026-01-29T12:34:56Z",
  "source_device": "/dev/sdb",
  "source_size_bytes": 8589934592,
  "partition_table_type": "MBR",
  "checksum_type": "SHA256",
  "split_size_mib": 4000,
  "partition_alignment": {
    "detected_type": "dos_traditional",
    "first_partition_lba": 63,
    "sectors_per_cylinder": 16065,
    "alignment_sectors": 16065,
    "geometry": {
      "heads": 255,
      "sectors_per_track": 63
    },
    "notes": "Traditional DOS/Windows 95-XP cylinder boundary alignment"
  },
  "partitions": [
    {
      "index": 0,
      "type": "FAT32",
      "original_size_bytes": 4294967296,
      "minimum_size_bytes": 2147483648,
      "compressed_size_bytes": 1073741824,
      "chd_files": ["partition-0.chd"],
      "checksum": "abc123...",
      "label": "DISK_LABEL",
      "resized": true,
      "flexible_restore": true,
      "filesystem_can_expand": true
    }
  ],
  "bad_sectors": []
}
```

**Notes on partition_alignment:**
- `detected_type`: One of "dos_traditional", "modern_1mb", "custom", "none"
- `first_partition_lba`: Where the first partition starts (typically 63 or 2048)
- `sectors_per_cylinder`: For cylinder boundary alignment (typically 16065 = 255×63)
- `alignment_sectors`: The alignment boundary in sectors
- `geometry`: CHS geometry detected from partition table
- `notes`: Human-readable description of detected alignment

3. **MBR export formats**:
   - `mbr.bin`: Raw 512-byte MBR sector
   - `mbr.json`: Structured partition table info:
```json
{
  "boot_signature": "0x55AA",
  "disk_signature": "0x12345678",
  "partitions": [
    {
      "index": 0,
      "bootable": true,
      "type": "0x0C",
      "type_name": "FAT32 LBA",
      "start_sector": 2048,
      "total_sectors": 8388608,
      "start_lba": 2048,
      "size_bytes": 4294967296
    }
  ]
}
```

4. **GPT export format** (`gpt.json`):
```json
{
  "disk_guid": "...",
  "header_lba": 1,
  "backup_lba": 16777215,
  "first_usable_lba": 34,
  "last_usable_lba": 16777182,
  "partitions": [
    {
      "index": 0,
      "type_guid": "...",
      "partition_guid": "...",
      "name": "Data",
      "first_lba": 2048,
      "last_lba": 8390656,
      "attributes": 0,
      "size_bytes": 4294967296
    }
  ]
}
```

### Phase 5: CHD Compression
1. **CHD library integration**:
   - Use libchdr (MAME's CHD library) via FFI or find pure Rust implementation
   - Implement streaming compression to avoid loading entire partition in memory
   - Support CHD v5 format (current MAME standard)

2. **File splitting for large partitions**:
   - Default split size: just under 4GB (4000 MiB) for FAT32 compatibility
   - User-configurable split size in MiB
   - Create sequential CHD files: `partition-0.chd`, `partition-0.001.chd`, etc.
   - Update metadata.json with list of CHD files per partition

3. **Progress indication**:
   - Track bytes processed vs total
   - Display compression ratio in real-time
   - Update GUI progress bar

4. **CHD parameters**:
```rust
pub struct ChdConfig {
    pub hunk_size: u32,        // Compression block size (default: 16KB)
    pub split_size_mib: u32,   // File split size (default: 4000)
}
```

### Phase 6: NTFS and ext2/3/4
1. **NTFS**: 
   - Option 1: Use `ntfs` crate (pure Rust)
   - Option 2: FFI to ntfs-3g (more mature)
   - Parse MFT, calculate used space
   
2. **ext2/3/4**: 
   - Parse superblock manually or use existing library
   - Read block group descriptors
   - Calculate used blocks from block bitmap
   
3. Implement resize calculation for each

### Phase 7: Restore Operation with Flexible Partition Layout

1. **Read backup folder**:
   - Parse `metadata.json`
   - Extract detected partition alignment information
   - Verify all CHD files present
   - Check checksums match

2. **Pre-flight checks**:
   - Verify target disk size ≥ sum of minimum partition sizes
   - Check if target device is mounted (warn if so)
   - Calculate available space

3. **Partition layout configuration**:

**Alignment options (presented to user):**
```
GUI Options:
○ Use original alignment (detected: DOS/Windows, 255 heads × 63 sectors)
○ 1MB boundaries (modern systems)
○ Custom alignment: [____] sectors
```

**Single partition backup:**
```
GUI Options:
- Use entire disk (expand to fill target)
- Use minimum size (from backup metadata)
- Custom size (user-specified, must be ≥ minimum)
- Option: Leave unallocated space for additional partitions
```

**Multi-partition backup:**
```
GUI Options:
- Show each partition with min/original sizes
- User can specify size for each partition
- "Fill remaining" option for last partition
- Smart default: distribute space proportionally to original sizes
- Preview of final layout before confirming
```

4. **User confirmation**:
   - Display warning: "All data will be overwritten on the destination disk. Are you sure you want to proceed?"
   - Show final partition layout preview with alignment info
   - Confirm button to proceed

5. **Restoration process**:
   - Calculate partition boundaries using selected alignment:
     - Apply detected alignment (default)
     - OR apply user-selected alignment override
   - Write partition table with user-specified sizes and proper alignment
   - For each partition:
     - Read CHD file(s) sequentially
     - Stream decompress
     - Write to target partition
     - If partition > minimum size, expand filesystem:
       - FAT16/32: Adjust FAT table entries
       - NTFS: Use ntfsresize or equivalent
       - ext2/3/4: Use resize2fs or equivalent
     - Verify checksum during write
   - Update progress bar throughout

6. **Post-restore verification**:
   - Optional: Re-read and verify checksums
   - Display summary of restored partitions
   - Log any warnings or errors

**Key use case support:**
- 64GB CF with 8GB used → restore to 16GB CF (take entire disk or leave room)
- 4GB CF → 8GB CF (expand to use more space)
- 2GB CF → 512MB CF (if data fits in minimum)
- Multi-partition flexibility for various target disk sizes
- Preserves original alignment by default (DOS 6.22 through Windows XP compatibility)

## Critical Safety Features

### Must-Have Validations
- Check if device is mounted (warn user, allow override)
- **Pre-flight disk space check**: Verify destination folder has adequate space before starting backup
- Verify target disk size is adequate for restore
- Display clear warning for destructive operations
- Calculate and verify checksums (user choice: CRC32 or SHA256) per file
- All logging displayed within GUI application
- Semi-accurate progress bar showing:
  - Current operation (reading/compressing/writing)
  - Bytes processed / total bytes
  - Estimated time remaining
  - Current partition being processed

### Error Handling Strategy
- Use `anyhow::Result` for application errors
- Use `thiserror` for custom error types
- Clear error messages displayed in GUI
- All errors logged to GUI log panel
- Rollback capability where possible

**Bad sector handling**:
- Mark bad sectors when encountered during read
- Skip bad sectors and continue operation
- Notify user via GUI dialog
- Store bad sector list in `bad-sectors.json`:
```json
{
  "bad_sectors": [
    {
      "partition": 0,
      "sector": 12345,
      "lba": 67890,
      "timestamp": "2026-01-29T12:34:56Z"
    }
  ]
}
```
- Include bad sector count in backup metadata
- Warn user during restore if backup contained bad sectors

```rust
#[derive(thiserror::Error, Debug)]
pub enum RustyBackupError {
    #[error("Device is currently mounted: {0}")]
    DeviceMounted(String),
    
    #[error("Insufficient space in destination: need {needed} bytes, have {available} bytes")]
    InsufficientSpace { needed: u64, available: u64 },
    
    #[error("Filesystem validation failed: {0}")]
    InvalidFilesystem(String),
    
    #[error("Bad sector detected at LBA {lba}")]
    BadSector { lba: u64 },
    
    #[error("CHD file not found: {0}")]
    ChdFileMissing(String),
    
    #[error("Checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },
    
    // ... more error types
}
```

## Testing Strategy

### Unit Tests
- Partition table parsing with known-good samples
- Filesystem detection logic
- Compression/decompression round-trips
- Minimum size calculations
- Error handling paths

### Integration Tests
- Create test disk images (small, e.g., 100MB)
- Format with different filesystems using system tools
- Run backup → restore → verify cycle
- Test with fragmented filesystems
- Test across different compression types

### Test Data Sources
- Generate fresh filesystem images for testing
- Include edge cases:
  - Empty partitions
  - Nearly-full partitions (>95% used)
  - Fragmented filesystems
  - Multiple partitions on one disk
  - Different partition table types (MBR vs GPT)

### Cross-Platform Testing
- Test on Linux (primary development target)
- Test on macOS (different device paths)
- Test on Windows (different device access model)

## Development Guidelines

### Code Style
- Minimize comments, focus on clear self-documenting code
- Use `Result<T>` and `?` operator for error propagation
- Prefer standard library where possible
- Keep filesystem modules independent and testable
- Use descriptive variable names
- Separate business logic from GUI code

### GUI Design Principles
- Clean, uncluttered interface
- Clear progress indication for all long-running operations
- In-app logging panel (scrollable, auto-scrolls to bottom)
- Responsive UI (don't block on I/O operations)
- Use async/await or threading for disk operations
- Disable UI controls during operations to prevent conflicts

### Platform Considerations
- Use `std::fs` for file operations (cross-platform)
- Platform-specific device access:
  - Linux: `/dev/sdX`, `/dev/mmcblkX`
  - macOS: `/dev/diskX`
  - Windows: `\\.\PhysicalDriveX`
- Handle permissions (may need root/admin):
  - Check effective UID on Unix
  - Check admin privileges on Windows

**Partition alignment:**
- **Detection-based:** Analyze source disk during backup to detect alignment pattern
- **Supported patterns:**
  - DOS Traditional: LBA 63 start, cylinder boundaries (255 heads × 63 sectors = 16,065 sectors)
  - Modern 1MB: LBA 2048 start, 2048-sector boundaries
  - Custom: Other detected patterns
- **Target systems:** DOS 6.22 through Windows XP (Pentium through Pentium 4 era)
- **Restore default:** Use detected alignment from backup
- **User override:** Allow selection of different alignment during restore
  
```rust
#[cfg(unix)]
fn require_root() -> Result<()> {
    if !nix::unistd::Uid::effective().is_root() {
        anyhow::bail!("This operation requires root privileges");
    }
    Ok(())
}
```

### Performance Considerations
- Read/write in large blocks (64KB-1MB chunks)
- Show progress bars for all operations
- Stream CHD compression (don't load entire partition in RAM)
- Use buffered I/O
- Use async operations for GUI responsiveness
- Run disk operations on separate thread to prevent GUI freezing
- Update progress bar at reasonable intervals (not every byte)

## Open Source Preparation

### License
- GPL-3.0 (GNU General Public License v3.0)

### Documentation Required
- **README.md**: Project overview, build instructions, usage guide with screenshots
- **CONTRIBUTING.md**: How to contribute, code style, testing requirements
- **CHANGELOG.md**: Version history
- **docs/**: Detailed documentation on backup format, CHD integration, filesystem support

### CI/CD Setup
- Will use existing Rust project pipeline (to be integrated later)
- Target platforms: Linux, macOS, Windows
- Automated builds and tests

### Example README Structure
```markdown
# Rusty Backup

Cross-platform GUI tool for backing up and restoring vintage computer disk images. The name is a play on words - both referencing the Rust programming language and the rusty vintage computers we're preserving.

## Features
- Support for FAT16/32, NTFS, ext2/3/4
- Automatic partition resizing to minimum size
- CHD compression (MAME standard)
- User-selectable checksums (CRC32 or SHA256)
- Split archives for FAT32 compatibility
- Bad sector detection and handling
- Easy-to-use GUI with progress tracking

## Installation
[build instructions]

## Usage
[screenshots and guide]

## Supported Systems
[list of vintage computers/OSes]

## Backup Format
[description of folder structure]
```

## Initial Task for Claude Code

**Start with Phase 1 - GUI Foundation:**

1. Create Cargo project structure for `rusty-backup`
2. Implement basic GUI with eframe/egui:
   - Main window with three tabs: Backup, Restore, Inspect
   - Device selection dropdown (enumerate available devices)
   - Destination folder picker
   - Start/Cancel buttons
   - Progress bar component
   - Scrollable log panel at bottom
3. Add basic MBR partition table parsing:
   - Read boot sector
   - Parse partition entries (both LBA and CHS values)
   - Validate partition table
   - Detect partition alignment pattern:
     - Extract first partition LBA
     - Check for cylinder boundary alignment (255×63 pattern)
     - Check for 1MB alignment (2048-sector pattern)
     - Identify alignment type (DOS traditional, modern, custom, or none)
4. Implement Inspect tab:
   - Display device list
   - Show partition table when device selected
   - Display partition info (type, size, filesystem)
   - Display detected alignment information

**Focus areas:**
- Clean separation between GUI and core logic
- Async disk operations to keep GUI responsive
- Comprehensive error handling from the start
- Simple, intuitive user interface
- Accurate alignment detection from partition table

**GUI Layout Suggestion for Backup:**
```
┌────────────────────────────────────────┐
│  Rusty Backup                          │
├────────────────────────────────────────┤
│ [Backup] [Restore] [Inspect]           │
├────────────────────────────────────────┤
│                                        │
│  Source Device: [Dropdown ▼]          │
│  Destination:   [Browse...]            │
│                                        │
│  Options:                              │
│  ☑ Resize partitions                  │
│  ☑ Split archives (4000 MiB)          │
│  Checksum: ○ CRC32  ● SHA256          │
│                                        │
│  [Start Backup]  [Cancel]              │
│                                        │
│  Progress: [████████░░░░] 67%         │
│                                        │
├────────────────────────────────────────┤
│ Log:                                   │
│ 2026-01-29 12:34:56 - Reading MBR...  │
│ 2026-01-29 12:34:57 - Found 2 parts   │
│ 2026-01-29 12:34:58 - Compressing...  │
│ ↓ (scrollable)                        │
└────────────────────────────────────────┘
```

**GUI Layout for Restore (Single Partition):**
```
┌────────────────────────────────────────┐
│  Rusty Backup - Restore                │
├────────────────────────────────────────┤
│  Backup Folder: [Browse...]            │
│  Target Device: [/dev/sdb (16GB) ▼]   │
│                                        │
│  Backup Info:                          │
│  • 1 partition (FAT32)                 │
│  • Original size: 64GB                 │
│  • Minimum needed: 8GB                 │
│  • Actual data: 6.5GB                  │
│  • Original alignment: DOS (255×63)    │
│                                        │
│  Partition Alignment:                  │
│  ● Use original (DOS/Win95-XP)         │
│  ○ 1MB boundaries (modern)             │
│  ○ Custom: [____] sectors              │
│                                        │
│  Partition Layout:                     │
│  ○ Use entire disk (16GB)              │
│  ○ Use minimum size (8GB)              │
│  ● Custom: [10] GB                     │
│  [ ] Leave unallocated space           │
│                                        │
│  [Restore]  [Cancel]                   │
└────────────────────────────────────────┘
```

**GUI Layout for Restore (Multiple Partitions):**
```
┌────────────────────────────────────────┐
│  Rusty Backup - Restore                │
├────────────────────────────────────────┤
│  Target disk: /dev/sdb (32GB)          │
│  Original alignment: DOS (255×63)      │
│                                        │
│  Alignment:                            │
│  ● Use original  ○ 1MB  ○ Custom       │
│                                        │
│  Partition 0 (FAT16):                  │
│  Min: 512MB  Original: 2GB             │
│  Restore as: [2048] MB                 │
│                                        │
│  Partition 1 (FAT32):                  │
│  Min: 4GB    Original: 62GB            │
│  Restore as: [Fill remaining ▼]       │
│              (≈29.5GB available)       │
│                                        │
│  Preview:                              │
│  ┌──────┬─────────────────────────┐   │
│  │ P0   │ 2GB                     │   │
│  ├──────┼─────────────────────────┤   │
│  │ P1   │ 29.5GB                  │   │
│  └──────┴─────────────────────────┘   │
│                                        │
│  [Restore]  [Cancel]                   │
└────────────────────────────────────────┘
```

## Target Use Case Reminder
**Primary users:** Retro computing enthusiasts backing up CF/SD cards from vintage computers (DOS, Windows 9x, early Linux systems) to modern computers for archival and restoration.

**Typical workflow:**
1. Remove CF/SD card from vintage computer
2. Insert into USB reader on modern computer
3. Launch Rusty Backup GUI
4. Select **Backup** tab
5. Choose source device from dropdown
6. Click "Browse" to select destination folder
7. Configure options:
   - Enable "Resize partitions" if desired (shrinks to minimum during backup)
   - Choose checksum type (CRC32 for speed, SHA256 for security)
   - Adjust split size if needed (default 4000 MiB)
8. Click "Start Backup"
9. Monitor progress and log output
10. Store backup folder safely
11. Later, to restore to a different-sized disk:
    - Select **Restore** tab
    - Click "Browse" to select backup folder
    - Choose target device (can be smaller or larger than original)
    - **Configure partition layout:**
      - Single partition: Choose "Use entire disk", "Use minimum", or specify custom size
      - Multiple partitions: Configure each partition size individually
      - Preview final layout
    - Confirm overwrite warning
    - Click "Start Restore"
12. Application automatically expands filesystems to fit configured partition sizes

## Design Decisions (Resolved)

### Image Format
- ✓ **No incremental backups** - Full backups only for v1.0
- ✓ **Version tracking** - Backup version matches software version that created it
- ✓ **Metadata format** - JSON for human readability and easy parsing
- ✓ **Folder structure** - Each backup is a folder, not a single file
- ✓ **Partition table storage** - Separate files (mbr.bin + mbr.json or gpt.json)
- ✓ **CHD compression** - All partition data compressed using CHD format (MAME standard)
- ✓ **No magic blocks** - Human-readable metadata files, not binary headers

### Data Integrity
- ✓ **Checksum algorithm** - User choice: CRC32 (fast) or SHA256 (secure)
- ✓ **Checksum scope** - Per CHD file (allows verification of individual files)
- ✓ **Bad sector handling** - Mark, skip, notify user; store list in bad-sectors.json
- ✓ **Storage limits** - Pre-flight check for destination space, no artificial limit otherwise
- ✓ **File splitting** - Optional, default just under 4GB (4000 MiB) for FAT32 compatibility
- ✓ **Split size** - User configurable in MiB

### Partition Layout Flexibility
- ✓ **Restore-time configuration** - User chooses partition sizes during restore
- ✓ **Single partition options** - Use entire disk, use minimum, or custom size
- ✓ **Multi-partition options** - Configure each partition individually, with "fill remaining" option
- ✓ **Primary use case** - Support migrating from larger to smaller disks (e.g., 64GB → 16GB CF)
- ✓ **Filesystem expansion** - Automatically expand filesystems if restored partition > minimum
- ✓ **Layout preview** - Show user the final partition layout before confirming restore
- ✓ **Partition alignment detection** - Analyze source disk to detect alignment pattern during backup
- ✓ **Alignment preservation** - Default to original detected alignment during restore (DOS/Win95-XP compatible)
- ✓ **Alignment override** - User can choose different alignment (original, 1MB, custom)
- ✓ **Target systems** - DOS 6.22 through Windows XP (Pentium through Pentium 4 era)

### User Experience
- ✓ **Restore confirmation** - Single warning: "All data will be overwritten on destination disk"
- ✓ **Logging** - All logs displayed in GUI application
- ✓ **Progress indication** - Semi-accurate progress bar with operation details
- ✓ **Mount detection** - Quick check, warn user if device is mounted
- ✓ **GUI-based** - Primary interface is graphical, not command-line

### Features Out of Scope (for now)
- ✗ **Incremental backups** - Future feature
- ✗ **Network features** - No network backup/restore
- ✗ **Encryption** - No encryption support
- ✗ **Deduplication** - Future feature consideration
- ✗ **CI/CD** - Will integrate existing pipeline later
- ✗ **Layout-only backup/restore** - Good idea, but deferred to future version

## Resources and References

### Documentation
- [MBR Specification](https://en.wikipedia.org/wiki/Master_boot_record)
- [GPT Specification](https://en.wikipedia.org/wiki/GUID_Partition_Table)
- [FAT Filesystem](https://en.wikipedia.org/wiki/File_Allocation_Table)
- [NTFS Filesystem](https://en.wikipedia.org/wiki/NTFS)
- [ext2/3/4 Filesystem](https://en.wikipedia.org/wiki/Ext4)
- [CHD Format](https://github.com/rtissera/libchdr) - MAME's Compressed Hunks of Data format
- [libchdr Documentation](https://github.com/rtissera/libchdr/blob/master/README.md)

### Similar Tools (for inspiration)
- `dd` - basic disk imaging
- `clonezilla` - partition cloning with GUI
- `partclone` - partition backup
- `ddrescue` - recovery-focused imaging
- MAME - Uses CHD format for disc images (inspiration for our compression)

### Rust Crates
- [eframe](https://docs.rs/eframe/) - egui framework wrapper
- [egui](https://docs.rs/egui/) - Immediate mode GUI
- [fatfs](https://docs.rs/fatfs/) - FAT filesystem
- [gpt](https://docs.rs/gpt/) - GPT parsing
- [indicatif](https://docs.rs/indicatif/) - progress bars
- [serde_json](https://docs.rs/serde_json/) - JSON serialization
- [libchdr](https://github.com/rtissera/libchdr) - CHD compression (will need FFI or Rust bindings)

## Success Criteria

### MVP (Minimum Viable Product)
- [ ] GUI application with tabs (Backup, Restore, Inspect)
- [ ] Device enumeration and selection
- [ ] Reads MBR and GPT partition tables
- [ ] Detects partition alignment pattern from source disk
- [ ] Exports partition tables (mbr.bin + mbr.json or gpt.json)
- [ ] Exports detected alignment information in metadata.json
- [ ] Detects FAT16/FAT32 filesystems
- [ ] Creates folder-based backups with CHD compression
- [ ] Restores backups to disk with flexible partition layout configuration
- [ ] Single partition: options for entire disk, minimum size, or custom
- [ ] Multi-partition: per-partition size configuration with preview
- [ ] Alignment options during restore: original (default), 1MB, or custom
- [ ] Filesystem expansion during restore (FAT/NTFS/ext)
- [ ] User-selectable checksums (CRC32 or SHA256) per file
- [ ] Pre-flight disk space validation
- [ ] Progress bar and logging in GUI
- [ ] Bad sector detection and reporting
- [ ] Split file support (configurable, default 4000 MiB)
- [ ] Works on Mac

### Version 1.0
- [ ] All listed filesystems supported (FAT, NTFS, ext2/3/4)
- [ ] Partition resizing during backup
- [ ] Cross-platform (Linux, macOS, Windows)
- [ ] Comprehensive test suite
- [ ] Complete documentation with screenshots
- [ ] GPL-3.0 license with proper headers
- [ ] Support for migrating between different-sized disks (primary use case validated)

### Future Enhancements
- [ ] HFS/HFS+ support
- [ ] Layout-only backup/restore feature
- [ ] Incremental backup support
- [ ] Deduplication across backups
- [ ] Batch operations (backup multiple devices)
- [ ] Backup scheduling/automation
- [ ] Additional alignment pattern detection (non-standard geometries)