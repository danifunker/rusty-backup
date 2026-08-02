# Windows prunes entries we splice into a resident $INDEX_ROOT

Open as of 2026-08-02. Everything below is measured in the Win7 QEMU rig against
real Windows 7, not inferred.

## The symptom

A file or directory we add to an index shows up in `rb-cli ls`, and **Windows
deletes it when it mounts the volume**. Not "invisible" — gone. On a volume our
own formatter wrote, `mydir` and `rootfile.txt` were absent afterwards and
Windows had added its own `$RECYCLE.BIN` / `bootsqm.dat`. `chkdsk` then reports
"found no problems", because nothing wrong is left to find.

## The split — this is the useful part

| where the entry goes | result |
|---|---|
| existing **`$INDEX_ALLOCATION`** (a Windows-made dir like the Desktop, or C:\ root) | **survives**, readable, executable |
| resident **`$INDEX_ROOT`** (any directory *we* created; the root of a volume *we* formatted) | **pruned on mount** |

So `try_insert_into_index_root` is the suspect path;
`try_insert_into_index_allocation` appears fine. Directory *creation* itself is
fine — an empty directory we create shows as `<DIR>` in Windows and `dir` on it
succeeds. It only breaks once an entry is spliced in.

**Careful with the test.** `dir C:\d1 >nul && echo ok || echo BROKEN` reports
BROKEN for an *empty* directory, because `dir` exits 1 with "File Not Found".
That misled the first pass into "directories are untraversable" — they are not.
Run `dir` plainly and read the message.

## Ruled out (all measured against a Windows-written peer)

- MFT record header: flags `0x0003`, `used`, `alloc`, first-attr offset, self
  index at `0x2C`, update-sequence fixups — all match Windows.
- `$INDEX_ROOT` header: indexed attr `0x30`, collation 1, alloc size 4096,
  clusters-per-index 1 — match.
- Index node header: entries offset 16, `totalSize`/`allocSize` self-consistent
  with the attribute value length — match.
- Index entry header: `length`, `keyLength` (= 66 + 2·nameLen), `flags`, padding,
  end sentinel `0x02` — match.
- Target file reference sequence: matches the target record's own sequence.
- The splice bookkeeping in `try_insert_into_index_root` updates node
  `entries_size`, node `allocated_size`, the `$INDEX_ROOT` value length, the
  attribute length **and** the record's `used_size` at `0x18`.
- Collation order — the failure reproduces with a *single* entry, so ordering
  cannot be it.
- **Parent reference sequence number** — was genuinely wrong (always 0) and is
  now fixed (`2700551`), but fixing it did **not** stop the pruning.
- **The `$I30` attribute name** — `build_resident_attr` had no name support at
  all, so every directory we created carried a *nameless* `$INDEX_ROOT` while
  `ntfs_format.rs` correctly writes `resident_attr(0x90, "$I30", ...)`. Fixed,
  and it did **not** stop the pruning either. Note `rootfile.txt` is a plain file
  in the volume root, whose index the formatter already named correctly, and it
  is pruned too — so the name was never the whole story.

## Where to look next

Something about the resident-index shape that Windows validates and we don't
reproduce. Candidates not yet checked:

- Whether Windows requires a directory's `$FILE_NAME` `allocatedSize`/`realSize`
  to be 0, and what ours carry.
- Whether a directory needs `$INDEX_ALLOCATION` + `$BITMAP` present even when
  empty.
- What `$LogFile` state Windows expects; self-healing may distrust a volume whose
  log we never touch.

## Method notes

- Do structural analysis on an image Windows has **never mounted**. It sets
  VolumeDirty, self-heals, and destroys the evidence.
- A 64 MB NTFS volume wrapped in a hand-built MBR, attached as a **second** QEMU
  disk, gets a drive letter and lets `chkdsk D:` run live (elevated) — far faster
  than round-tripping the 6 GB system image.
- `qcow.py` / `qread.py` in the session scratchpad read qcow2 and parse MFT
  records without root or conversion.

## Related

- `--format` on a superfloppy source: the raw override is **deliberate**
  (`src/backup/mod.rs`, "a compressed superfloppy output needs restore-path
  work"), not a bug — but it used to be silent, so asking for zstd and getting a
  `.img` read as one. It now warns. Making it actually honour the format is still
  open work.
- A directory we create fills up at ~6 entries (`directory index full, no room in
  existing nodes`) because the index never spills to `$INDEX_ALLOCATION`. Fixing
  the pruning above does not fix this; it needs real index-node allocation.
