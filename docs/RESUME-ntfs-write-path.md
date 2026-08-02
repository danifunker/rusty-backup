# Resume: the NTFS write path (and what else is still open)

Written 2026-08-02, superseding `RESUME-ntfs-index-root.md`. Branch
`ppc-macos-work`. Everything marked *measured* was observed against real Windows
7 in the QEMU rig, not inferred from code.

Read the rig section before touching anything — several of these bugs were
mis-diagnosed twice because the evidence destroys itself.

---

## Landed and verified this session (do not re-investigate)

| commit | fix | verified how |
|---|---|---|
| `31c0c0c` | restore: `SectorAlignedWriter` overflowed on a partial chunk | zstd restore of a 129 MB stream on Win7; payload md5 identical |
| `95f960c` | NTFS: inherit the ACL instead of writing a fabricated one | Windows read the file; Security tab populated; `icacls /reset` clean |
| `3c1d44f` | NTFS: **sign-safe data-run lengths**; real timestamps | 31 MB file now readable *and* executable on Win7 |
| `2700551` | NTFS: parent reference carries the parent's sequence number | measured before/after |
| `5ab770a` | NTFS: `$INDEX_ROOT` named `$I30`; `--format` downgrade now warns | measured |

Files created into an **existing Windows directory** work end to end — a 31 MB
executable written by us runs on Win7. The broken cases are all below.

---

## ISSUE 1 — Windows deletes entries we splice into a resident `$INDEX_ROOT`

**Severity: high.** This is why `restore` could not read a backup folder we had
staged with `mkdir` + `put`.

### Symptom (measured)

An entry we insert shows in `rb-cli ls`, and **Windows removes it when it mounts
the volume** — not merely hides it. On a volume our own formatter wrote, `mydir`
and `rootfile.txt` were absent from the image afterwards, and Windows had added
its own `$RECYCLE.BIN` and `bootsqm.dat`. `chkdsk` then reports "found no
problems" because nothing wrong is left to find.

### The split — the most useful fact

| entry inserted into | result |
|---|---|
| existing **`$INDEX_ALLOCATION`** (Windows-made dir: Desktop, `C:\` root) | **survives** — readable, executable |
| resident **`$INDEX_ROOT`** (any dir *we* create; root of a volume *we* format) | **pruned on mount** |

### The isolating experiment (already run — this is the sharpest statement)

`rootfile.txt` was inserted into the **formatter's own root index**, which is
built by `root_index_root()` (`src/fs/ntfs_format.rs:457`) and is definitely
well-formed — Windows mounts that volume, reads its root, and writes
`bootsqm.dat` into it. That entry was **still pruned**.

So the bug is in **`try_insert_into_index_root`**
(`src/fs/ntfs.rs:2417`), *not* in how we construct an index root.
`try_insert_into_index_allocation` (`src/fs/ntfs.rs:2542`) appears fine.

### Ruled out — each compared byte-for-byte against a Windows-written peer

- MFT record header: flags `0x0003`, `used`, `alloc`, first-attribute offset,
  self index at `0x2C`, update-sequence fixups (USN at `0x30`, count 3, both
  sector tails equal the USN, originals saved in the USA).
- `$INDEX_ROOT` header: indexed attr `0x30`, collation 1.
- Index node header: entries offset 16, `index_used` / `index_allocated`
  self-consistent with the attribute value length.
- Index entry header: `length` (8-byte aligned), `keyLength` == `66 + 2*nameLen`,
  `flags`, padding, end sentinel `0x02`.
- Target file reference sequence == the target record's own sequence.
- Splice bookkeeping: `try_insert_into_index_root` **does** update node
  `index_used`, node `index_allocated`, the `$INDEX_ROOT` value length, the
  attribute length, and the record's `used_size` at `0x18`.
- Collation order — reproduces with a **single** entry, so ordering cannot be it.
- Parent reference sequence (was 0, fixed in `2700551`) — did not stop it.
- `$I30` attribute name (was absent, fixed in `5ab770a`) — did not stop it.

### Candidates not yet checked

1. **A directory's own `$FILE_NAME` `allocatedSize` / `realSize`.** Windows keeps
   both 0 for a directory. Ours are written by `build_file_name_attr` with
   `size = 0` for directories, so probably fine — but the *index entry copy* in
   the parent has never been checked against this.
2. **Whether a directory needs `$INDEX_ALLOCATION` + `$BITMAP` present even when
   empty.** Windows-made directories that have ever held entries carry both.
   Our created directories carry neither. This is my leading candidate — and it
   overlaps with ISSUE 2, so implementing index-block allocation may fix both.
3. **`$LogFile` state.** We never touch it. NTFS self-healing may distrust a
   volume whose log does not corroborate recent index changes.
4. **`$Secure` / `$UpCase` collation dependence** — the index is collated with
   `COLLATION_FILE_NAME`, which uses `$UpCase`. If our inserted key's ordering
   disagrees with what Windows computes from `$UpCase`, a B-tree lookup can miss
   even when a linear walk finds it. Unlikely with one entry, but untested.

---

## ISSUE 2 — a directory we create fills up at ~6 entries

`insert_index_entry` (`src/fs/ntfs.rs:2385`) tries `$INDEX_ROOT`, then existing
`$INDEX_ALLOCATION` INDX blocks, then gives up:

```
error: create_file: disk full: directory index full, no room in existing nodes
```

There is no path that **allocates a new INDX block** and promotes the resident
index root to a large index. A 1024-byte MFT record holds roughly six ~104-byte
entries alongside `$STANDARD_INFORMATION` + `$FILE_NAME`, hence the limit.

Fixing this properly means: allocate a cluster-aligned INDX block, write its
`INDX` header + fixups, move the root's entries into it, rewrite `$INDEX_ROOT`
as a *large* index (node flags bit 0 set, single end entry with a sub-node VCN —
`root_index_root()` at `ntfs_format.rs:457` already shows the exact shape), and
add `$INDEX_ALLOCATION` (non-resident) plus `$BITMAP`. The formatter already
builds all three for `$Extend`, so the byte layouts can be copied from there
rather than re-derived.

---

## ISSUE 3 — `build_empty_index_root` hardcodes index geometry

`src/fs/ntfs.rs:1744`:

```rust
data[8..12].copy_from_slice(&4096u32.to_le_bytes()); // index alloc size
data[12] = 1;                                        // clusters per index record
```

The formatter derives both (`idxroot_clusters_per_block`,
`src/fs/ntfs_format.rs:229`): when `index_record_size >= cluster_size` the field
is *clusters per block*, otherwise it is a sector count.

On the 64 MB test volume our own formatter makes — **cluster = 512** — the
correct value is `4096/512 = 8`, and we write `1`. On the Win7 C: volume
(cluster 4096) `1` happens to be right, which is why this did not show up there.

Real defect. Not the cause of ISSUE 1 (pruning happens on C: too, where the
hardcoded value is correct), but it must be fixed before ISSUE 1 conclusions on
small-cluster volumes mean anything.

---

## ISSUE 4 — `--format` is not honoured for partition-table-less sources

`src/backup/mod.rs:1446` forces `CompressionType::None` for a superfloppy. This
is **deliberate** ("a compressed superfloppy output needs restore-path work"),
not an accident. As of `5ab770a` it warns instead of silently writing a `.img`
and recording `compression_type: "none"`.

Making it actually honour zstd/gzip/lz4 for superfloppies is open work, and the
restore side is the part that needs doing.

---

## ISSUE 5 — stale precomputed minimum across devices

`src/backup/sizes.rs:234`. From a real user log: a 64 GB card was scanned
(minimum 27.2 GiB), the card was swapped for a 32 GB one behind the same
`\\.\PhysicalDrive3`, and the next backup logged

```
Compact analysis (partition-0): ... data=3.4 GiB ...
Partition-0: reusing precomputed defragmented minimum 27.2 GiB (skipped volume walk)
```

`precomputed_min` is keyed on **partition index only**, with no device identity
or size check. The imaged data was correct (smart sizing used 3.4 GiB) but
`mbr-min.bin` records 27.2 GiB, so a `--size minimum` restore from that backup
produces a wrongly-sized partition. Not data loss; still wrong.

Fix: key the cache on something device-identifying (path + source size + a
partition fingerprint), or invalidate it whenever the scanned geometry differs.

---

## The structural problem behind most of this

`src/fs/ntfs_format.rs` and the create path in `src/fs/ntfs.rs` build the same
on-disk structures independently, and have now disagreed **six** times:

1. version-aware `$STANDARD_INFORMATION` (`9017f92`)
2. inherited `security_id` (`9017f92`)
3. MFT self-index at `0x2C` (`2c8fb2e`)
4. unique attribute instance ids (`2c8fb2e`)
5. sign-safe data-run lengths (`3c1d44f`) — the formatter's `enc_run`
   (`ntfs_format.rs:266`) even documents the rule the create path violated
6. the `$I30` attribute name (`5ab770a`) — again, the formatter had it right

Every one was found by diffing our bytes against a Windows-written peer. They
should share one record/attribute builder; until they do, expect a seventh.

---

## The rig — how to reproduce any of this

```sh
# small NTFS volume, wrapped in a hand-built MBR so Windows gives it a letter
rb-cli new volume ntfs nt.img --size 64M
rb-cli mkdir nt.img /mydir
rb-cli put   nt.img f.txt /mydir/inner.txt
# prepend an MBR: type 0x07, start LBA 2048, size len(vol)/512, 0x55AA at 510
```

Boot Win7 with it as a **second** disk so `chkdsk D: /f` can run live (elevated):

```sh
qemu-system-x86_64 -m 2048 -smp 2 \
  -drive file=sys.qcow2,if=none,id=d0,cache=writeback \
  -device ich9-ahci,id=ahci -device ide-hd,drive=d0,bus=ahci.0 \
  -drive file=ntdisk.img,format=raw,if=none,id=d1 \
  -device ide-hd,drive=d1,bus=ahci.1 \
  -monitor unix:mon.sock,server,nowait -vga std -display none \
  -netdev user,id=n0 -device e1000,netdev=n0
```

`shot.py` (screendump -> PNG) and `type.py` (sendkey) drive it over the monitor
socket. `qcow.py` / `qread.py` read qcow2 and parse MFT records on the host with
no root and no conversion — `qread.attributes()` walks a record, `qread.fixup()`
applies the update sequence.

### Traps that cost real time here

- **Analyse only images Windows has never mounted.** Mounting sets VolumeDirty,
  self-heals, and *deletes the evidence* — a corrupt 31 MB file was gone before I
  could dump its record, and a later `chkdsk` then reported a clean volume.
- **`dir X >nul && echo ok || echo BROKEN` lies.** `dir` exits 1 on an *empty*
  directory ("File Not Found"). That produced a bogus "directories are
  untraversable" conclusion. Run `dir` plainly and read the message.
- **`copy FILE NUL`** distinguishes "cannot read" from "cannot execute".
- **Non-elevated writes to `C:\` are virtualised** to
  `C:\Users\<u>\AppData\Local\VirtualStore\`. A restore that "produced no output"
  had actually written 210 MB there.
- **Never `pkill -f` / `pgrep -f` a pattern that appears in your own command
  line** — it kills the shell (exit 144). This bit three times. Use
  `ps -eo pid,comm | awk '$2=="qemu-system-x86"{print $1}'`.
- **Check the binary you are testing.** Four separate stale-artifact traps this
  session: `rb-cli-dev` on the G5, the loose `rb-cli.exe` on the VM Desktop, a
  day-old `target/release/rb-cli`, and a CI zip that predated its own fix.
  Confirm with `--version` before drawing conclusions.
- CI zips are **nested** (`.zip` containing a `.zip`); unzipping once and running
  `objdump` on the missing path yields a silent false negative.

---

## Other open items (not NTFS)

- **Win7 TUI rendering.** Could not reproduce on `2026-08-02-04-48`: renders and
  redraws correctly at 80x25 and maximised. Needs a screenshot plus console font
  and window size from the reporter before it is worth chasing further.
- **Text encoding audit** — see `docs/RESUME-text-encoding-audit.md`. Two jobs:
  ~325 non-ASCII characters in our own UI/log strings, and per-filesystem
  filename charsets (114 `from_utf8_lossy` uses under `src/fs/`; Amiga/HPFS/CP-M
  are wrong).
- **`rb-cli get` path lookup is case-sensitive on NTFS.** `KernelBase.dll` works,
  `KERNELBASE.dll` does not. NTFS is case-insensitive; this is a real usability
  bug and probably a one-line collation change in the lookup.
- **PowerPC**: ship `rb-cli-ppc-g3.tar.gz` only — measured ~50% *faster* than the
  G5-targeted build on a G5. `PPC_CPU=750 PPC_TUNE=970` is untried if anyone
  wants to chase G5 scheduling.
