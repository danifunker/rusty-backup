# SGI/IRIX EFS HDD synthesis — IRIX-mount debugging (resume)

`rb-cli new-sgi-hdd` synthesizes a dvh-wrapped EFS IRIX hard disk. The rb-cli
side is **done and validated** (create → inspect → put → fsck → get → cmp all
pass, in CI). The **open problem** is the real-IRIX validation: a disk built by
`new-sgi-hdd` does not yet visibly work when attached to the IRIS emulator. This
doc is the handoff to continue that investigation.

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

## Known DIFFERENCES from the real fixture (suspects, not yet ruled in/out)

Compared our superblock to `efs_small.img` (decompress: see script below):

| field        | real fixture | ours        | note |
|--------------|--------------|-------------|------|
| `fs_checksum`| `0xde58a0b8` | `0`         | **couldn't reverse-engineer the algo** from the fixture alone (XOR/ADD × rotate × word-count brute force: no match). Linux efs driver does NOT validate it; IRIX *might*. |
| `fs_tfree`   | real count   | `0`         | we never compute free blocks |
| `fs_tinode`  | real count   | `0`         | we never compute free inodes |
| `fs_bmblock` | `0`          | `2`         | `fs_bmblock` is "s2 only"; old-magic EFS (ours, `0x00072959`) computes the bitmap location, so real disks leave it 0 |
| `ncg`/`heads`| 78 / 10      | 1 / 1       | ours is a single cylinder group, internally consistent but non-standard; IRIX EFS may assume the standard multi-CG layout |

`root nlink/size/numextents` also differ but are *correct* for our (tiny) content.

## Next steps — in priority order

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
- Disk under test: `/Users/dani/irix-toolbox.img` (100 MiB; EFS partition at sector 4096).
- Gates: `cargo clippy --all-targets -- -D warnings`, `cargo test --lib`, `cargo test --test cli_sgi_hdd`.

## Resume prompt (paste into a fresh session)

> Resume: making `rb-cli new-sgi-hdd` (dvh + EFS IRIX hard disk) actually mount +
> show files on the IRIS emulator / real IRIX. Read `docs/sgi_efs_hdd_irix_debug.md`
> first — it's authoritative. The rb-cli side is done + committed (`4c41662`,
> `8258ed0`, `5569c89`) and passes its own create→put→fsck→get→cmp e2e; the open
> issue is that a synthesized disk doesn't visibly work under IRIX. Our EFS now
> matches the real `efs_small.img` fixture's directory format byte-for-byte
> (incl. `.`/`..`), so the remaining suspects are superblock fields IRIX may
> validate: `fs_checksum`=0 (algorithm not yet reverse-engineered), `fs_tfree`/
> `fs_tinode`=0, `fs_bmblock`=2 (should be 0 for old-magic EFS), and our
> non-standard single-cylinder-group geometry. NEXT: use the IRIX diagnostics in
> the doc (manual `mount -t efs`, `prtvtoc`, and especially IRIX's own
> `fsck -t efs`) and/or capture IRIX's own writes by flushing the emulator to the
> host image, then dump it (script in the doc) to read the authoritative format +
> crack the checksum, and fix `create_blank_efs` / `sgi_hdd_builder` to match.
> Don't regress the bare `new --fs efs` path (small EFS volumes must stay
> byte-identical: `firstcg` stays 18). Disk under test: `/Users/dani/irix-toolbox.img`.
