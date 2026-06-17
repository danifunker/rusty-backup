# SGI/IRIX EFS HDD synthesis — RESOLVED (real-IRIX validated)

`rb-cli new-sgi-hdd` synthesizes a dvh-wrapped EFS IRIX hard disk.

## RESOLVED 2026-06-17 — mounts + reads on real IRIX 5.3

A disk built by the fixed `new-sgi-hdd` (`/Users/dani/irix-toolbox.img`,
100 MiB) was attached to the IRIS emulator (IRIX 5.3, IP22) and **IRIX
auto-mounted it** at `/disk2p0` — `ls -la /disk2p0` listed `HELLO.TXT` and
`cat /disk2p0/HELLO.TXT` printed the file. Both blockers below are confirmed
fixed end-to-end; `fx` accepted the sgilabel (no geometry complaint) and the
single-cylinder-group EFS mounted fine, so the multi-CG work under "Remaining"
turned out to be unnecessary for mounting. Everything from here down is the
investigation record.

## How it was cracked: both blockers, from the oracle disk

The user supplied the oracle the prior handoff asked for — an IRIX-formatted
1 GB SCSI disk at **`~/scsi3.raw`** (disk 0,3, mkfs_efs'd by IRIX, and it even
contains IRIX's own writes: a `FromIRIX/` dir + `FromIrixText.txt`). Dumping it
resolved **both** open unknowns, and the fixes are shipped on `commander-mode`:

1. **dvh geometry (the blocker)** — the emulated SGI drive reports
   **heads=16, sectors/track=63, cyls=ceil(total_sectors/1008)** via SCSI MODE
   SENSE (source of truth: `~/repos/iris/src/scsi.rs:701-705`). We were writing
   `secs=128`, so `fx` rejected the sgilabel ("disagrees with existing volume
   header parameters") and IRIX never reached the EFS. **Fixed:**
   `DEFAULT_SECTORS_PER_TRACK` 128 → **63** in `sgi_hdd_builder.rs`; cylinder is
   now 16×63 = 1008 sectors. A generated disk's dvh geometry now equals the
   drive's exactly (C×H×S == disk sectors).
2. **EFS superblock `fs_checksum` (was the last unknown)** — **cracked** via
   Aaru's `EFS/Checksum.cs` ("new"/IRIX-3.3+ algorithm) and validated byte-exact
   against the oracle (`0xfa73610a`). Algorithm: XOR each **big-endian 16-bit**
   word of the superblock over offsets `0..88` into a u32, **rotate-left-1**
   after each word. Implemented as `efs::efs_superblock_checksum` +
   `EfsSuperblock::recompute_checksum`; pinned by the
   `efs_checksum_matches_real_irix_disk` unit test.
3. Other superblock fixes from the oracle: **`fs_bmblock` 2 → 0** (old-magic
   leaves it computed — bitmap is at block 2), and **`fs_tfree`/`fs_tinode`**
   now computed (were 0). `write_superblock_pair` recomputes the checksum on
   every mutation, so edited/resized EFS volumes stay valid too.

**Our EFS reader was already correct** — it reads `~/scsi3.raw`'s real
multi-cylinder-group volume (ncg=51) cleanly, listing `lost+found`, `FromIRIX`,
`FromIrixText.txt`. The bug was purely on the synthesis side.

**Caveat in the oracle:** `scsi3.raw`'s **dvh at sector 0 is zeroed** (the
emulator never flushed the first ~4032 sectors to the backing file, though it
flushed all the EFS data). So the oracle gave us the EFS layout + checksum but
*not* the dvh bytes — the drive geometry came from the emulator source instead.

**Confirmed working** on IRIX 5.3 (auto-mounted, file read — see RESOLVED block
above). Single-CG (ncg=1) mounted fine, so the multi-CG work under "Remaining"
is optional (only matters for inode count / authenticity, not mounting).

---

## (historical) original handoff follows

## What shipped (branch `commander-mode`)

- `4c41662` engine: `sgi.rs` models `device_parameters` (vh_dp geometry, byte-exact
  round-trip; also fixed a latent editor geometry-wipe); `partition/sgi_hdd_builder.rs`
  (`build_sgi_efs_hdd`); `efs.rs` scales `firstcg` so EFS can exceed ~32 MiB.
- `8258ed0` CLI: flat verb `new-sgi-hdd` + `inspect` full 16-slot dvh dump +
  `tests/cli_sgi_hdd.rs` e2e + docs.
- `5569c89` `fs/efs.rs`: `create_blank_efs` now writes `.`/`..` into the root dir
  (real IRIX EFS requires them; our reader was lenient and hid the gap).

## The open problem

```
rb-cli new-sgi-hdd /Users/dani/irix-toolbox.img --size 100M --name TOOLBOX
rb-cli put /Users/dani/irix-toolbox.img@1 ./HELLO.TXT /HELLO.TXT
```
Attached to IRIS (BlueSCSI-style SCSI HDD), IRIX appears to mount it but the file
doesn't show under `/disk2`. User created a file+dir from inside IRIX as an
oracle — **but those writes never reached the host image** (IRIS buffers writes;
`/Users/dani/irix-toolbox.img` is still byte-identical to rb-cli's output).

**Leading hypothesis (user's):** `/disk2` is a stock empty directory on the IRIX
root fs, not the disk's mountpoint — i.e. the disk was never actually mounted
there. Must confirm with an explicit manual mount before concluding anything.

## Confirmed correct (verified against the real fixtures)

- **dvh**: magic `0be5a941`, volume-header checksum zero-sums, geometry
  (100c×16h×128s×512), slots 8 VOLHDR / 10 VOLUME(whole disk) / 0 EFS. Round-trips
  through `src/partition/sgi.rs` and matches `tests/fixtures/sgi/irix_volhdr.bin`.
- **EFS directory format**: byte-identical to the real `efs_small.img` fixture —
  dirent = `inode(be32) + namelen(u8) + name`, `.`/`..` at offsets 506/498 (both
  inode 2), `EFS_DIRBLK_MAGIC=0xbeef`, inode/extent encoding. Our reader + fsck
  read the real fixture fine.
- rb-cli round-trip (put/ls/fsck/get/cmp) is clean.

## Known DIFFERENCES from the real fixture — now mostly RESOLVED

| field        | real         | ours (before) | status |
|--------------|--------------|---------------|--------|
| `fs_checksum`| computed     | `0`           | **FIXED** — algo cracked (see below), now stamped + validated |
| `fs_tfree`   | real count   | `0`           | **FIXED** — computed in `create_blank_efs` |
| `fs_tinode`  | real count   | `0`           | **FIXED** — computed (`cgisize*4*ncg - 3`) |
| `fs_bmblock` | `0`          | `2`           | **FIXED** — now 0 (old-magic computed; bitmap stays at block 2) |
| `ncg`/`heads`| 51 / 10      | 1 / 1         | **STILL single-CG** — legal EFS, but see "Remaining" |

### The EFS superblock checksum algorithm (cracked + validated)

From Aaru `Aaru.Filesystems/EFS/Checksum.cs` `ComputeChecksum` (the "new",
IRIX-3.3+ variant, used by both old- and new-magic volumes). Validated against
`~/scsi3.raw` (`0xfa73610a`):

```
c = 0u32
for off in (0, 2, 4, ... 86):              # 44 big-endian 16-bit words, bytes 0..88
    word = (sb[off] << 8) | sb[off+1]
    c ^= word
    c = c.rotate_left(1)                   # 32-bit rotate (carry = old bit 31)
# c is fs_checksum; the field at 88..92 is excluded from the loop
```

(The "old"/pre-3.3 variant *shifts* instead of rotating — `c <<= 1` — and did
**not** match our old-magic oracle. Use the rotate version.) Ours:
`efs::efs_superblock_checksum` / `EfsSuperblock::recompute_checksum`.

### The real EFS multi-CG layout (from `~/scsi3.raw`, a 1 GB disk)

```
fs_size=2092584 firstcg=513 cgfsize=41021 cgisize=1051
sectors=63 heads=10 ncg=51 magic=0x00072959 bmblock=0
bmsize=261573 tfree=2038446 tinode=214397 replsb=2092607 lastialloc=28
```
Layout (partition-relative blocks): block 0 reserved, block 1 = superblock,
blocks 2..512 = bitmap (511 blocks), block 513 (`firstcg`) = CG0
[inodes 1051 blocks, then data], CG1 at 513+41021, ... ×51; `fs_size` =
`firstcg + ncg*cgfsize`. Root inode 2 @ block `firstcg`, off 256; root dirblock
= `firstcg + cgisize` = 1564 (matches our single-CG `root_dirblock` formula).
Inode addressing (Linux `efs/inode.c`, mirrored by our `inode_byte_offset`):
`block = firstcg + cgfsize*(inode_index/cgisize) + inode_index%cgisize`,
4 inodes/block.

## FINDINGS from IRIS (fx 5.3) — 2026-06-17

Tested on the IRIS emulator (drive identifies as `SGI / IRIS EMUL DISK / 1.0`,
`fx version 5.3, Oct 18 1994` → IRIX 5.3 era). Disk attached as SCSI 0,2.

`fx` on our disk (dksc(0,2)):
```
Scsi drive type == SGI    IRIS EMUL DISK  1.0
            warning: disagrees with existing volume header parameters
fx: Warning: no sgilabel on disk
fx: Warning: can't read sgilabel on disk
creating new sgilabel
fx: Error: Invalid argument: can't set volume header in driver
```
Disk Manager shows our disk as "104 MB / IRIX Disk / Inactive" but "This disk is
not recognized or is part of a logical volume" (only **Initialize** offered).

**Root cause: the dvh `device_parameters` geometry disagrees with the geometry
the emulated drive reports, so `fx` rejects our sgilabel — IRIX never reaches the
EFS.** This is the blocker. (The "can't set volume header in driver: Invalid
argument" is a *secondary* effect of trying to re-`fx` our wrong-geometry label;
on a truly blank disk `fx` says "invalid label, ignored" and proceeds fine.)

Reference: `mkfs_efs` log from IRIX formatting a blank ~1 GB emulator disk (0,3):
```
blocks=2092608 inodes=214404
sectors=63 cgfsize=41021
cgalign=1 ialign=1 ncg=51
firstcg=513 cgisize=1051
bitmap blocks=511
```
So: drive uses **sectors/track = 63** (we use 128 in the dvh dp); IRIX EFS is
**multi-cylinder-group** (ncg=51 for 1 GB; ours is ncg=1, a single CG). Both must
match. Note `firstcg=513`, `cgisize=1051` per CG — very different from our
single-CG `firstcg=51`.

### Two layered fixes needed
1. **dvh geometry** (blocker): `device_parameters` must match the emulated
   drive's real CHS (the drive reports sectors=63; need its heads/cyls too).
   Until then `fx` rejects the sgilabel and the disk is "not recognized".
2. **EFS multi-CG layout** (next): replicate IRIX's `mkfs_efs` cylinder-group
   layout (ncg>1, firstcg/cgfsize/cgisize per the formula) instead of our
   single-CG hack. `create_blank_efs` + the EFS reader's CG addressing.

### The oracle to get
An **IRIX-formatted disk's host image** (the blank 0,3 IRIX just formatted to
1.02 GB EFS, or the system disk 0,1). It reveals the exact dvh geometry the
emulator wants, the dvh checksum algorithm, AND IRIX's EFS multi-CG layout —
everything needed to replicate. Find where the emulator stores its disk files
and dump it with the script below. Fallback: `prtvtoc /dev/rdsk/dks0d3vh` on the
formatted blank prints the drive's cylinders/heads/sectors.

## Multi-CG formatter — DONE (2026-06-17)

`create_blank_efs` now emits a **uniform multi-cylinder-group** layout (the
`mkfs_efs` shape) instead of a single CG, so the inode count scales with the
volume instead of being capped at one group's worth (~512 on a 100 MB disk).
Design:

- One inode per `EFS_BYTES_PER_INODE` (4 KiB): `cgisize = cgfsize*512/(NBPI*4)`,
  which makes total inodes `= data_bytes / 4096` independent of `ncg`.
- `ncg = ceil(usable / EFS_CG_TARGET_BLOCKS)` with a ~16 MiB target CG, so each
  `cgfsize ≤ target` and `cgisize` stays well inside its u16 field at any size.
- Uniform CGs: `fs_size = firstcg + ncg*cgfsize`; bitmap spans the whole volume
  at block 2; the replica SB lives in the trailing zone at block V-1; root dir
  is the first data block of CG 0.

Verified: a 100 MB disk → ncg=7, 25 060 inodes (was 512); 64 MB unit test
round-trips a file across CGs; the 48 MB fsck test now exercises 3 CGs and stays
clean. Applies to both `new-sgi-hdd` and bare `new --fs efs`. Reader + in-place
resize already handled uniform multi-CG, so only the formatter changed.

Future option (not needed for mounting): a `--bytes-per-inode` / `--inodes` CLI
knob to tune density per disk.

## Validation done (rb-cli side, no emulator)

- `cargo test --lib` (1997 pass) incl. new `efs_checksum_matches_real_irix_disk`
  and `create_blank_efs_has_valid_checksum_and_bmblock`; `cargo test --test
  cli_sgi_hdd`; `cargo clippy --all-targets -D warnings` clean.
- A generated 100M disk: dvh geometry 204c×16h×63s == disk sectors, dvh checksum
  zero-sums, EFS checksum recomputes valid, `bmblock=0`, `firstcg+ncg*cgfsize ==
  fs_size`. Our reader reads `~/scsi3.raw`'s real multi-CG EFS.
- Bare `new --fs efs` still uses `firstcg=18` (layout guardrail) and now also
  carries a valid checksum.

To dump+check a freshly generated disk, see the validate script approach in this
session's history, or reuse the superblock script below (`BASE = efs_first*512`).

## Next steps — in priority order

### 0. Real-IRIX mount test (USER — the actual remaining work)
Regenerate with the fixed binary, attach to IRIS, and confirm `fx` no longer
reports "disagrees with existing volume header parameters" and `mount -t efs`
shows the put file. If it mounts: done. If not, capture the `fx`/`mount`/`fsck`
errors and proceed to the diagnostics below.

### 1. Confirm whether/where it actually mounts (do this FIRST)
In the IRIX shell:
```sh
hinv -c disk                          # which SCSI unit is our disk?
ls -l /dev/dsk | grep dks             # device nodes (find dks0d<N>s0)
prtvtoc /dev/rdsk/dks0d<N>vh          # does IRIX see our dvh partition table?
mkdir -p /mnt2
mount -t efs /dev/dsk/dks0d<N>s0 /mnt2 ; echo "mount exit=$?"
ls -la /mnt2                          # do we see HELLO.TXT?
```
- `mount exit != 0` → it never mounts; the error string says what IRIX rejects.
- mounts but `/mnt2` empty → readdir/inode issue (dig there).
- `/disk2` was probably a red herring (stock empty dir).

### 2. Run IRIX's own EFS fsck (the gold diagnostic)
```sh
fsck -t efs /dev/rdsk/dks0d<N>s0
```
IRIX fsck names malformed fields directly (bad checksum / free count / inode / etc).

### 3. Capture IRIX's own writes (the format oracle)
Get IRIX's file+dir onto the host image: in IRIX `umount /mnt2; sync`, then cleanly
**halt IRIX and quit IRIS** so it flushes the backing file. Then re-dump
`/Users/dani/irix-toolbox.img` (script below). If IRIX's entries appear, we can read
the **authoritative dirent format**, the **checksum IRIX computed** (cracks the algo),
and **tfree/tinode**. If the host file is still unchanged, IRIS uses a working copy —
find its path / use the emulator's "export disk" option and analyze that.

### 4. Fix `create_blank_efs` / builder to match
Likely fixes once IRIX tells us what it wants: compute `fs_checksum`, set
`fs_tfree`/`fs_tinode`, set `fs_bmblock=0`, and/or emit a standard multi-CG geometry.

## Reusable: dump an EFS superblock + root directory

```python
# python3 - <<'PY'   (set PATH and BASE; BASE = partition_start_sector*512;
#                     our disk's EFS is at sector 4096 -> BASE=4096*512; a bare
#                     superfloppy/fixture is BASE=0)
import struct
PATH='/Users/dani/irix-toolbox.img'; BASE=4096*512
d=open(PATH,'rb').read(); sb=d[BASE+512:BASE+512+92]
u32=lambda o:struct.unpack('>I',sb[o:o+4])[0]; u16=lambda o:struct.unpack('>H',sb[o:o+2])[0]
print("sb: fs_size=%d firstcg=%d cgfsize=%d cgisize=%d sectors=%d heads=%d ncg=%d"%(u32(0),u32(4),u32(8),u16(12),u16(14),u16(16),u16(18)))
print("    dirty=%d fs_time=%d magic=0x%08x bmsize=%d tfree=%d tinode=%d bmblock=%d lastialloc=%d checksum=0x%08x"%(u16(20),u32(24),u32(28),u32(44),u32(48),u32(52),u32(56),u32(64),u32(88)))
firstcg=u32(4); ino=BASE+firstcg*512+2*128; inode=d[ino:ino+128]
nex=struct.unpack('>H',inode[28:30])[0]
print("root inode2: nlink=%d size=%d numextents=%d"%(struct.unpack('>H',inode[2:4])[0],struct.unpack('>I',inode[8:12])[0],nex))
for i in range(nex):
    ex=inode[32+i*8:40+i*8]; bn=(ex[1]<<16)|(ex[2]<<8)|ex[3]; ln=ex[4]
    for b in range(ln):
        db=d[BASE+(bn+b)*512:BASE+(bn+b)*512+512]; slots=db[3]
        print(" dirblk@%d magic=0x%04x firstused=%d slots=%d"%(bn+b,struct.unpack('>H',db[0:2])[0],db[2],slots))
        for s in range(slots):
            raw=db[4+s]
            if raw==0: continue
            off=raw<<1; nl=db[off+4]
            print("   slot%d off=%d inode=%d name=%r"%(s,off,struct.unpack('>I',db[off:off+4])[0],db[off+5:off+5+nl]))
# PY
```
Decompress the real EFS oracle: `zstd -dkf tests/fixtures/sgi/efs_small.img.zst -o /tmp/efs_real.img`
(its EFS is a bare superfloppy → use `BASE=0`).

## Key files / commands

- Engine: `src/partition/sgi_hdd_builder.rs`, `src/partition/sgi.rs`,
  `src/fs/efs.rs` (`create_blank_efs` ≈ L2301, superblock fields + root dir),
  `src/fs/efs_fsck.rs`.
- CLI: `src/cli/verbs/new_sgi_hdd.rs`, `src/cli/verbs/inspect.rs` (SGI dump).
- Test: `tests/cli_sgi_hdd.rs`. Build: `cargo build --bin rb-cli` → `target/debug/rb-cli`.
- Fixtures (oracles): `tests/fixtures/sgi/irix_volhdr.bin` (real dvh),
  `tests/fixtures/sgi/efs_small.img.zst` (real EFS).
- **Oracle disk**: `~/scsi3.raw` (1 GB IRIX-formatted SCSI 0,3; EFS partition at
  sector 4032; dvh sector 0 is zeroed/unflushed; has IRIX's own writes).
- External refs: drive geometry `~/repos/iris/src/scsi.rs` (`exec_mode_sense_6`,
  heads=16/spt=63); EFS checksum `~/repos/Aaru/Aaru.Filesystems/EFS/Checksum.cs`;
  EFS layout `~/efs-xfs-refs/efs-linux-5.15/efs/{super,inode}.c`.
- Disk under test (user): `/Users/dani/irix-toolbox.img` — regenerate with the
  fixed binary before re-testing (the old one had the wrong geometry/checksum).
- Gates: `cargo clippy --all-targets -- -D warnings`, `cargo test --lib`, `cargo test --test cli_sgi_hdd`.

## Resume prompt (paste into a fresh session)

> Resume: making `rb-cli new-sgi-hdd` (dvh + EFS IRIX hard disk) actually mount +
> show files on the IRIS emulator. Read `docs/sgi_efs_hdd_irix_debug.md` first —
> the "STATUS 2026-06-17 (cont)" block at the top is authoritative. Both prior
> blockers are FIXED on `commander-mode`: the dvh geometry now matches the
> emulated drive (heads=16, secs=63, cyls=ceil(N/1008), from `iris/src/scsi.rs`),
> and the EFS `fs_checksum` algorithm was cracked (Aaru new-algo: XOR BE16 words
> 0..88 + rotate-left-1) and validated against the oracle `~/scsi3.raw`
> (`0xfa73610a`); we also set `bmblock=0` and compute `tfree`/`tinode`. Lib +
> e2e + clippy all green; a generated disk's dvh/EFS structure validates. The
> ONLY remaining work is the **real-IRIX mount test on the emulator** (regenerate,
> attach, `fx` should accept the label, `mount -t efs` should show the file). If
> it still won't mount, the prime suspect is our **single-cylinder-group** layout
> vs IRIX's multi-CG — replicate `mkfs_efs`'s CG sizing from `~/scsi3.raw` (see
> "Remaining"). Don't regress bare `new --fs efs` (`firstcg` stays 18 — verified).
