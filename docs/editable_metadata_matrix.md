# Per-Filesystem Editable-Metadata Matrix

Which optional [`EditableFilesystem`](../src/fs/filesystem.rs) metadata setters
each filesystem implements, versus the trait default of returning
`FilesystemError::Unsupported`. This is the reference for "which metadata
editors can a UI surface for this filesystem" — the Commander Mode **File Info**
window (and the classic browse view) gate their editor rows on exactly these
capabilities.

How to read it: a filesystem that *overrides* a setter supports editing that
field; one that doesn't inherits the default `Unsupported` and a caller must
gray the control out (or branch on the returned error). The structural
mutations every editable filesystem implements — `create_file`,
`create_directory`, `delete_entry`, `delete_recursive`, `sync_metadata` — are
not shown; they're required, not optional.

26 filesystems implement `EditableFilesystem`: ADFS, AFFS, Apple DOS 3.3, Atari
DOS, Alto BFS, CBM DOS, CP/M, Acorn DFS, DragonDOS, EFS, exFAT, ext, FAT, HFS,
HFS+, Human68k, MFS, NTFS, OS-9, PFS3, ProDOS, QDOS, RS-DOS, SFS, UFS, XFS.
Two later additions are covered below as well: **QNX 2** (`fs::qnx2`, ICON /
LEXICON floppies and hard disks) and **Sun QIC tapes** (`fs::sun2tape_edit::TapeEditor`,
editing the tar, `.tar.Z` and UFS miniroot files inside a SUN2TAPE `.qic`). This
list predates several other drivers and is not exhaustive.

## Rename — universal

`rename(parent, entry, new_name)` is implemented **in place on all 26** editable
filesystems (it keeps the entry's identity — start cluster / inode / CNID /
objectnode — and changes only the parent listing's name). Sun QIC tapes are the
exception to "in place": a rename there rebuilds the tape like every other tape
edit. Browse-only
filesystems (e.g. btrfs) are not in the list above and surface `Unsupported`.

## Optional metadata setters

Every filesystem not listed in a column below returns `Unsupported` for that
setter. Only the filesystems that override at least one optional setter have
a row; the rest support `rename` (above) and nothing else here.

| Filesystem | Type/Creator | Dates | Finder info | Resource fork | Bless folder | Volume name | Permissions | ProDOS type | ProDOS access | Symlink | Hardlink |
|------------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **HFS**    | Yes | Yes | Yes | Yes | Yes | Yes | —   | —   | —   | —   | —   |
| **HFS+**   | Yes | —   | —   | Yes | Yes | —   | —   | —   | —   | —   | Yes |
| **MFS**    | Yes | —   | —   | —   | —   | —   | —   | —   | —   | —   | —   |
| **ProDOS** | —   | —   | —   | —   | —   | —   | —   | Yes | Yes | —   | —   |
| **ext**    | —   | —   | —   | —   | —   | —   | Yes | —   | —   | —   | —   |
| **PFS3**   | —   | —   | —   | —   | —   | —   | —   | —   | —   | Yes | Yes |
| **QNX 2**  | —   | —   | —   | —   | —   | —   | Yes | —   | —   | —   | —   |
| **Sun QIC tape** | — | — | —   | —   | —   | —   | Yes | —   | —   | —   | —   |

Trait method behind each column:

| Column | `EditableFilesystem` method |
|--------|-----------------------------|
| Type/Creator   | `set_type_creator(entry, type_code, creator_code)` |
| Dates          | `set_dates(entry, create, modify, backup)` (three u32 Mac timestamps) |
| Finder info    | `set_finder_info(entry, finfo: [u8; 16], fxinfo: [u8; 16])` |
| Resource fork  | `write_resource_fork(entry, data)` |
| Bless folder   | `set_blessed_folder(entry)` |
| Volume name    | `set_volume_name(new_name)` |
| Permissions    | `set_permissions(entry, mode: u32)` |
| ProDOS type    | `set_prodos_type(entry, type_byte: u8, aux_type: u16)` |
| ProDOS access  | `set_prodos_access(entry, access: u8)` |
| Symlink        | `create_symlink(parent, name, target)` |
| Hardlink       | `create_hardlink(parent, name, target)` |

### Notes / caveats

- **HFS** has the richest metadata surface: 4-byte OSType/creator, the three
  catalog dates (created / modified / backup), the full 16-byte Finder info,
  resource-fork write, folder blessing (boot folder), and volume rename.
- **HFS+** supports type/creator, resource fork, and blessing, plus real
  **hardlinks** (`iNodeXXXX` indirect-node records under the private metadata
  directory). It does **not** implement `set_dates`, `set_finder_info`, or
  `set_volume_name` today — those return `Unsupported` even though HFS does.
  Closing that gap is a natural follow-up.
- **MFS** (the 1984 flat Macintosh filesystem) supports only `set_type_creator`.
- **ProDOS** supports its native `file_type` / `aux_type` and the access byte;
  it has no Mac type/creator.
- **ext** supports POSIX permission bits (`set_permissions`, preserving the
  `S_IFMT` file-type bits and changing only the low 12 mode bits).
- **PFS3** is the only filesystem with symlink *and* hardlink creation.
- **QNX 2** (ICON / LEXICON) supports create, mkdir, rename, delete (as QNX
  `frel`), `set_permissions` and `set_owner`. Permissions map Unix mode bits
  onto the ICON attribute letters (`rwaem` for files, `rcbm` for directories)
  for owner / group / other; `set_owner` stores the uid as the member and the
  gid as the group (each 0-255), shown on read as `group.member`. The ICON
  clock's dates are read (and written on create) but there is no `set_dates`;
  the 4-bit year stops at 1995, so newer dates store as 31-Dec-95. Writes were
  checked with the real QNX 4.10.01 `chkfsys` in icon-emu.
- **Sun QIC tapes** (SUN2TAPE `.qic`) support add, mkdir, rename, delete,
  `set_permissions` and `set_owner` inside a tape file that holds a tar, a
  compress(1)-ed tar (`.tar.Z`) or a UFS miniroot. A tape has no free space,
  so every edit session rebuilds the whole tape beside the original and keeps
  the original as `<name>.bak` (then `.bak.1`, ...; never overwritten).
  Untouched members and tape files stay byte-identical; tape files themselves
  are not added, removed or renamed. Not yet verified on Sun hardware.
- **Owner** (`set_owner(entry, uid, gid)`) has no column above; besides QNX 2
  and Sun tapes it is overridden by several Unix-style drivers.
- **Amiga protection bits / file comment** (AFFS, PFS3, SFS) are **create-time
  only** — they ride in on `CreateFileOptions` (`amiga_protection` at offset
  0x140, `amiga_comment` BSTR at 0x148) and surface read-only on `FileEntry`.
  There is no post-hoc `set_amiga_protection` setter, so they are not in the
  matrix above; editing them in place would be new engine work.

## Commander File Info — current vs. available

The Commander Mode File Info window currently exposes editors for:

- **HFS / HFS+** type/creator (`render_hfs_type_row`)
- **HFS** dates (`render_hfs_dates_row`)
- **ProDOS** type (`render_prodos_type_row`)
- **ext** permissions (`render_ext_permissions_row`)

Engine support exists but is **not yet surfaced** in that window for: HFS
Finder info and volume name; HFS/HFS+ resource-fork replacement and folder
blessing; HFS+ hardlinks; MFS type/creator; ProDOS access; PFS3 symlinks /
hardlinks. These are the backlog for the per-FS metadata-editor expansion
(`docs/commander_mode.md` §10.2).
