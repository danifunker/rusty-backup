# Resume: the NTFS write path (and what else is still open)

Rewritten 2026-08-02 after the write path was closed out; supersedes the
earlier revision of this file and `RESUME-ntfs-index-root.md`. Branch
`ppc-macos-work`. Everything marked *measured* was observed against real
Windows 7 in the QEMU rig, not inferred from code.

**The NTFS write path now works end to end on real Windows.** Read the rig
section before touching anything — several of the bugs below were mis-diagnosed
twice because the evidence destroys itself.

---

## The state of it (measured on Win7, 2026-08-02)

A 64 MB volume our formatter wrote, populated entirely by `rb-cli`
(`mkdir`, `put`, 45-entry directory, three-level nesting, a 31 MB
executable), mounted as `D:` on Windows 7:

| check | result |
|---|---|
| every entry we wrote still present after mount | **yes** — nothing pruned |
| `chkdsk D:` | **"Windows has checked the file system and found no problems"** (294 index entries) |
| `d:\mydir\tool.exe --version` | **runs**, prints its version |
| `icacls d:\mydir\tool.exe` | Administrators:(F) SYSTEM:(F) Authenticated Users:(M) Users:(RX), `(OI)(CI)` inheritance |
| Windows writes into a 45-entry directory *we* built | **succeeds**, 45 -> 46 files |
| we read Windows' file back afterwards | **yes**, byte-exact |
| 31 MB payload md5 after the round trip | identical |

That last pair is the strongest statement available: Windows accepted a
large ($INDEX_ALLOCATION-backed) directory index we constructed, inserted
its own entry into it, and we parsed the result back.

---

## Landed and verified (do not re-investigate)

| commit | fix | verified how |
|---|---|---|
| `31c0c0c` | restore: `SectorAlignedWriter` overflowed on a partial chunk | zstd restore of a 129 MB stream on Win7; payload md5 identical |
| `95f960c` | NTFS: inherit the ACL instead of writing a fabricated one | Windows read the file; Security tab populated |
| `3c1d44f` | NTFS: **sign-safe data-run lengths**; real timestamps | 31 MB file readable *and* executable on Win7 |
| `2700551` | NTFS: parent reference carries the parent's sequence number | measured before/after |
| `5ab770a` | NTFS: `$INDEX_ROOT` named `$I30`; `--format` downgrade now warns | measured |
| `9dcc9dc` | NTFS: **real B-tree directory indexes** + index geometry from the volume + case-folded lookup | unit tests across geometries; Win7 mount |
| `1c712a0` | NTFS: LCN 0 is not "sparse"; backup boot checked at its real sector | `fsck` on a fresh volume is now clean |
| `7311f59` | NTFS: only claim the DOS namespace for genuine 8.3 names | measured |
| `1a4808a` | NTFS: inherit the parent's real ACL, correctly framed DACL | `icacls` + execute + Windows-writes-into-our-dir |

### What `9dcc9dc` actually changed

The old create path could only splice an entry into a node that already had
room. That produced both of the headline bugs:

- **`insert_index_entry`** now promotes a full resident `$INDEX_ROOT` to a
  large index (entries move into a fresh INDX block; `$INDEX_ALLOCATION` +
  `$BITMAP` attributes appended), descends the tree in collation order
  instead of picking any node with room, and splits full leaves upward,
  pushing the median separator into the parent.
- **`remove_index_entry`** replaces a removed separator with its in-order
  predecessor (or drops an empty single-leaf subtree).
- Index geometry (`index_record_size`, clusters-per-index-block) is read
  from BPB 0x44 and the directory's own `$INDEX_ROOT`, never hardcoded.
- `$FILE_NAME` carries the indexed flag; `free_mft_record` bumps the record
  sequence and reuse preserves it, so an index entry can never carry a
  stale reference.

The regression net is in `src/fs/ntfs.rs`'s test module:
`directory_grows_past_resident_root_across_geometry` (512/1024/4096-byte
clusters), `root_directory_takes_a_hundred_files_alongside_metafiles`,
`deleting_separator_entries_keeps_the_tree_sound`,
`rename_moves_entries_between_index_nodes`,
`reused_mft_record_bumps_sequence_and_index_entry_matches`, plus
`verify_directory_index`, a recursive checker asserting sorted order, key
bounds per subtree, `$BITMAP` agreement, and that every index entry points
at an in-use record with a matching sequence number.

### The ACL bug worth remembering

Created files were getting `$Secure` id `0x100` — the *metafile* descriptor,
granting only SYSTEM and Administrators plain **Read**. Hence "Access is
denied" when running our executable, and Windows being unable to write into
a directory we made. Files now inherit the parent's own
`$SECURITY_DESCRIPTOR` (the formatter's root carries the standard permissive
data-volume ACL), repacked compactly because mkntfs pads the root's DACL to
4 KiB.

The repack has to frame the ACL header exactly — revision(1) sbz1(1)
size(2) count(2) sbz2(2). Copying 4 source bytes instead of 2 shifted every
field, and the DACL read back as 184 zero-length ACEs. **Windows accepted
that silently and treated it as granting nothing** — no chkdsk complaint, no
error, just mysterious access-denied. If permissions ever look wrong again,
dump the DACL and check that its ACEs exactly fill the declared size;
`dacl_extent` in the tests does this.

---

## Still open

### ISSUE 4 — `--format` for partition-table-less sources — **fixed**

A superfloppy now honours every codec (`zstd` / `gzip` / `lz4` / `vhd` /
`chd` / `raw`); a 64 MiB ext4 superfloppy backs up to ~47 KB with zstd and
restores byte-identical.

The restore path was never at fault. Backup forced `CompressionType::None`
*and* recorded `compression_type: "none"` in metadata unconditionally — the
force made the lie harmless, and honouring the format made the two diverge,
so restore (which dispatches on `metadata.compression_type`) wrote the
compressed member to the target verbatim. Metadata now records the codec
actually written. `tests/superfloppy_compression.rs` pins it.

Still open, and unchanged by that fix: a superfloppy restore ignores a
`--target-size` larger than the source and lands the original size. Raw
behaves the same way, so it is a separate gap, not a regression.

### ISSUE 5 — stale precomputed minimum across devices

`src/backup/sizes.rs:234`. From a real user log: a 64 GB card was scanned
(minimum 27.2 GiB), the card was swapped for a 32 GB one behind the same
`\\.\PhysicalDrive3`, and the next backup logged

```
Compact analysis (partition-0): ... data=3.4 GiB ...
Partition-0: reusing precomputed defragmented minimum 27.2 GiB (skipped volume walk)
```

`precomputed_min` is keyed on **partition index only**, with no device
identity or size check. The imaged data was correct (smart sizing used
3.4 GiB) but `mbr-min.bin` records 27.2 GiB, so a `--size minimum` restore
from that backup produces a wrongly-sized partition. Not data loss; still
wrong. Fix: key the cache on something device-identifying (path + source
size + a partition fingerprint), or invalidate whenever the scanned geometry
differs.

### NTFS work not needed yet, but known missing

- **`$LogFile` is never written.** Windows replays/resets it on mount and has
  not objected, but a torn write mid-`put` has no journal to recover from.
- **No `$Secure` authoring.** We inherit an SD attribute rather than adding a
  descriptor to `$SDS` and referencing it by id. Inline SDs are legal and
  Windows honours them; ids would be tidier and cheaper per file.
- **`$ATTRIBUTE_LIST` is not produced**, so a single file cannot outgrow one
  MFT record. Not reachable through the CLI surface today.
- **The defragmenting clone** (`src/fs/ntfs_clone.rs`) still does not replay
  reparse points, named streams, or per-file security descriptors beyond the
  inherited default — documented in its module header.

---

## The structural problem behind most of this

`src/fs/ntfs_format.rs` and the create path in `src/fs/ntfs.rs` build the
same on-disk structures independently, and have now disagreed **eight**
times:

1. version-aware `$STANDARD_INFORMATION` (`9017f92`)
2. inherited `security_id` (`9017f92`)
3. MFT self-index at `0x2C` (`2c8fb2e`)
4. unique attribute instance ids (`2c8fb2e`)
5. sign-safe data-run lengths (`3c1d44f`) — the formatter's `enc_run`
   (`ntfs_format.rs:266`) even documents the rule the create path violated
6. the `$I30` attribute name (`5ab770a`) — again, the formatter had it right
7. index-block geometry: hardcoded 4096/1 vs. the formatter's derived
   `idxroot_clusters_per_block` (`9dcc9dc`)
8. the `$FILE_NAME` indexed flag, set by `resident_attr` but not by
   `build_resident_attr` (`9dcc9dc`)

Every one was found by diffing our bytes against a Windows-written peer.
They should share one record/attribute builder; until they do, expect a
ninth.

---

## The rig — how to reproduce any of this

```sh
# small NTFS volume, populated entirely through the CLI
rb-cli new volume ntfs ntvol.img --size 64M --name RBTEST
rb-cli put   ntvol.img rootfile.txt /rootfile.txt
rb-cli mkdir ntvol.img /mydir
rb-cli put   ntvol.img tool.exe /mydir/tool.exe
# then prepend an MBR so Windows gives it a letter:
# type 0x07, start LBA 2048, size = len(vol)/512, 0x55AA at 510
```

Boot Win7 with it as a **second** disk so `chkdsk D:` can run live:

```sh
qemu-system-x86_64 -m 2048 -smp 2 \
  -drive file=sys.qcow2,if=none,id=d0,cache=writeback \
  -device ich9-ahci,id=ahci -device ide-hd,drive=d0,bus=ahci.0 \
  -drive file=ntdisk.img,format=raw,if=none,id=d1 \
  -device ide-hd,drive=d1,bus=ahci.1 \
  -monitor unix:mon.sock,server,nowait -vga std -display none \
  -netdev user,id=n0 -device e1000,netdev=n0
```

The Win7 image is `~/Win7/8D5CAE93-....qcow2`; **snapshot it first**
(`qemu-img snapshot -c <tag>`). `shot.py` (screendump -> PNG via PIL) and
`type.py` (sendkey, with `{win}` `{csenter}` `{alty}` `{esc}` tokens) drive
it over the monitor socket.

### Free oracles — try these before booting anything

- **`ntfs-3g` tools are installed and need no root**: `ntfsls -R vol.img`,
  `ntfscat vol.img /path/file | md5sum`, `ntfsinfo`. They caught nothing
  Windows didn't, but they run in a second and are a real independent parser.
- **`rb-cli fsck`** on the image. On a fresh volume it must be silent; any
  output is a regression.
- A **python MFT dump** on the host (fixups are 3 lines) beats guessing.
  Parse the record, list attribute types, print `$STANDARD_INFORMATION`'s
  security id and any `$SECURITY_DESCRIPTOR`. This is how the ACL bug was
  localised, after the VM only said "Access is denied".

### Traps that cost real time here

- **Analyse only images Windows has never mounted.** Mounting sets
  VolumeDirty, self-heals, and *deletes the evidence*.
- **`dir X >nul && echo ok || echo BROKEN` lies.** `dir` exits 1 on an
  *empty* directory ("File Not Found"). Run `dir` plainly and read it.
- **`copy FILE NUL`** distinguishes "cannot read" from "cannot execute".
- **Non-elevated writes to `C:\` are virtualised** to
  `C:\Users\<u>\AppData\Local\VirtualStore\`.
- **Never `pkill -f` / `pgrep -f` a pattern that appears in your own command
  line** — it kills the shell (exit 144). Use
  `ps -eo pid,comm | awk '$2=="qemu-system-x86"{print $1}'`.
- **One VM at a time.** A second qemu on the same qcow2 dies with `Failed to
  get "write" lock`, and — worse — keystrokes meant for it go to whichever
  VM owns `mon.sock`. Shut the old one down and confirm the process is gone
  before booting the next.
- **Never regenerate the test disk while a VM has it open.** The guest is
  writing to the file you just replaced.
- **`screendump` is asynchronous**: PIL will read a truncated PPM if you
  convert immediately. Retry the open until it parses (`shot.py` does).
- **UAC often lands late** after `{csenter}`, and a duplicate consent prompt
  can queue up behind it. Screenshot before assuming a command ran.
- **Check the binary you are testing.** `--version` before drawing
  conclusions. CI zips are **nested** (`.zip` containing a `.zip`).

---

## Other open items (not NTFS)

- **Win7 TUI rendering.** Could not reproduce on `2026-08-02-04-48`.
  Needs a screenshot plus console font and window size from the reporter.
- **Text encoding audit** — see `docs/RESUME-text-encoding-audit.md`.
  ~325 non-ASCII characters in our own UI/log strings, and per-filesystem
  filename charsets (114 `from_utf8_lossy` uses under `src/fs/`;
  Amiga/HPFS/CP-M are wrong).
- **PowerPC**: ship `rb-cli-ppc-g3.tar.gz` only — measured ~50% *faster*
  than the G5-targeted build on a G5. `PPC_CPU=750 PPC_TUNE=970` is untried.
