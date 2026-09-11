# Backup and restore, by partition scheme

The rule that decides every shape below: **a CHD holds a whole disk.**
`backup --format chd` writes exactly one `<name>.chd` whose logical bytes are
the disk, or it refuses. It never writes `partition-N.chd`. A partitionless
volume (a floppy, a BasiliskII `.hfv`, a bare Amiga `.hdf`) is the one case
where the partition *is* the disk, so its CHD is the volume, sized to the
source, with no table and no sidecar.

Two layouts exist. **Per-partition** (`zstd`, `gzip`, `lz4`, `raw`, `vhd`)
stores one body per partition plus a table sidecar, and restore rebuilds the
table from the sidecar. **Single-file CHD** stores the disk image itself, and
restore is a byte copy unless a size changes.

## The disk-label schemes

Sun, NeXT, SGI volume header, SGI disk label, Amiga RDB, Atari AHDI and Sharp
X68k are backed up **whole**: everything before the first partition (label
copies, the RDSK/PART/FSHD/LSEG chain, the IPL and its table, boot blocks)
rides verbatim inside the CHD. Nothing on the restore side has to understand
the label to put the disk back.

| Scheme | Per-partition layouts | Resize at backup time | Resize on restore | Unit the patcher rounds to |
|---|---|---|---|---|
| Sun | refused | refused | slices rewritten, XOR checksum restamped | cylinder (`ntrks * nsect`) |
| NeXT | refused | refused | all four label copies rewritten | `d_secsize` (1024) past the front porch |
| SGI volume header | refused | refused | slots rewritten, checksum recomputed | 512-byte block |
| SGI disk label | refused | refused | eight slots rewritten in the label's own word order | 512-byte block |
| Amiga RDB | refused | refused | RDSK + PART blocks overlaid, driver chain untouched | cylinder, per partition |
| Atari AHDI | refused | refused | root-sector entries rewritten, 0x1234 word-sum restamped | 512-byte sector |
| Sharp X68k | allowed (`zstd` etc.) | refused | table entries rewritten, sector-size aware | logical sector (256 / 512 / 1024) |

The per-partition layouts are refused for the label schemes because their
sidecar is a *parsed* table, and re-serializing it loses what the head
carries (the RDB driver chain and bad-block list most visibly). X68k is the
exception: its per-partition restore rebuilds the table from `x68k.json` and
zero-fills the IPL region, which is fine for a MiSTer data disk and wrong for
a real SCSI disk whose `X68SCSI1` signature selects the sector size. Use CHD
for a faithful copy.

**Resize on restore** is `partition::restore_patch`. The restore reads the
head region out of the CHD, rewrites the start/size fields and checksums in
those bytes, and copies the bodies to wherever the label now says. Because
each scheme counts in its own unit, the patcher may round a partition up and
shift the ones after it; it returns the layout it actually wrote, and the body
copy and filesystem resize follow that. A filesystem `resize_filesystem_for`
cannot shrink (UFS on a real Sun or NeXT disk) is refused before anything is
written. The head is written *after* the bodies, so a label that lives inside
its first slice (SunOS at cylinder 0) still lands.

**Resize at backup time** stays refused for these schemes: the backup is the
faithful whole-disk image, and the resize happens on the way out.

## What cannot be backed up

- **`.dsd`** (double-sided Acorn DFS): two sides track-interleaved in one
  file, which the reader de-interleaves into two volumes. No restore could put
  them back, so `backup` refuses it in every format. Copy the file; `ls`,
  `get`, `put` and `convert` keep working on it.
- **An X68k SASI disk with a partition off a 512-byte boundary** is refused
  for CHD only; the per-partition layouts still work.

## Compacted bodies in a single-file CHD

A packed FAT/NTFS/exFAT body sits shrunk inside its full partition in the CHD
(the tail is zeros, which the CHD compresses away). An as-is restore grows the
volume back to its partition, on every table, so the two layouts restore the
same disk. For NTFS that includes the backup boot sector the packed stream
stops short of. `--sector-by-sector` copies the source bytes instead, and
restores byte-identical.
