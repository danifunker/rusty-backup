# Convert tab

One tab, in every shell (egui, TUI, GTK), that changes the format of one image or every image in a
folder. It replaces the header menu's Bulk Convert and Inspect's Convert Floppy Container, and the
Optical tab's conversion form. Agreed with the user on 2026-11-07; this is slice 7c of
[gtk4_shell_plan.md](gtk4_shell_plan.md).

## What stays where

- **Convert** changes container formats, shrinks hard disks to their minimum (plus headroom), and
  refits floppies to another standard size.
- **Inspect** keeps browsing, per-partition export, custom per-partition sizes and HFV export. Its
  "Convert..." button opens the Convert tab with the image loaded.
- **Optical** keeps browsing, extracting, CD audio and ripping drives. Its "Convert..." button
  opens the Convert tab with the disc loaded.
- **Restore** keeps backup folders: Convert does not read them.

## Sources and kinds

The source is one file or one folder. A folder is walked one level deep, or through every
subfolder when "Include subfolders" is on; the output folder copies the subfolder layout.
Dotfiles are skipped, and a `.bin` beside a `.cue` of the same name is read through the `.cue`.

Each file is one of three kinds, read from its contents first and its extension second
(`model::convert::classify`):

| Kind | Read from |
|---|---|
| Disc | ISO 9660 / High Sierra volume descriptor, a cue sheet, a CD or DVD CHD |
| Floppy | a floppy container's magic (WOZ, MOOF, DC42, D88, DIM), a 2MG of floppy size, or a raw image of a standard floppy size |
| Hard disk | everything else the engine opens |

When the contents and the extension disagree, the contents win and Review says so. `.adz`, `.hdz`
and `.gz` are unpacked on the way in.

## Targets

One target per kind. Only the formats that fit the kind are offered:

| Kind | Formats |
|---|---|
| Hard disk | Raw, VHD, Dynamic VHD, QCOW2, VMDK (flat, sparse), 2MG, CHD |
| Floppy | Raw, 2MG, WOZ, MOOF, DiskCopy 4.2, XDF, HDM, DIM, D88 |
| Disc | CD CHD, DVD CHD, BIN/CUE, ISO |

A format that needs a feature this build lacks stays in the list, greyed out, with the reason:
"This copy was built without CHD support." (or "without optical disc support"). A file a format
cannot take (a 1.44 MB disk to WOZ) is shown in Review with the reason and is not converted.

Each kind keeps its own output extension, editable, defaulting to the format's.

## Hard disk sizing

- **Keep size** (default): a plain format change.
- **Minimum**: every partition shrinks to its filesystem's minimum, and the disk with it.
- **Minimum + extra**: the minimum plus an amount on **every** partition, in MB or as a percentage
  of that partition's minimum.

A partition is never made larger than it was, nor larger than its filesystem or format allows
(HFS's 65535-block ceiling, for one); either cap is logged. A filesystem that cannot shrink keeps
its size, logged. An image with no partition table keeps its size.

Some filesystems can only be measured by reading the whole partition
(`fs::is_expensive_minimum`). "Measure slow filesystems too" is off by default; its help lists the
slow ones from that function, so it stays current. With it off, those partitions keep their size.

## Floppy size

- **Same size** (default): a plain container change.
- **Refit to** a standard size in the source's family (PC: 360K, 720K, 1.2M, 1.44M, 2.88M; Mac:
  800K, 1.44M; Amiga: 880K, 1.76M). A blank volume of the same filesystem is formatted at the new
  size, every file is copied across, and a PC disk's boot code is carried over with the new BPB.

A disk whose files do not fit is skipped and logged; nothing is truncated. A disk with no
filesystem the engine can copy from (copy-protected, raw tracks) is refused with that reason.

## Verify

Off by default. Same size: both images are read back as plain sectors and compared. Resized or
refitted: each output volume's file tree (names and sizes) is compared against the source's, and
the filesystem check runs where one exists. A mismatch fails that file and is logged.

## Output and clashes

A single file goes to a save-as path; a folder to an output folder, keeping each file's stem with
the new extension. Existing outputs ask Skip / Overwrite / Back, as Bulk Convert did. The output
may not be the source.

## Command line

`rb-cli convert <file|folder> <dest>` takes the same choices: `--format` (hard disk), `--floppy-to`,
`--disc-to`, `--recursive`, `--size keep|minimum|minimum+N[MB|%]`, `--measure-slow`,
`--floppy-size same|360K|720K|...`, `--verify`, `--extension` and `--overwrite`.
`rb-cli floppy convert` and `rb-cli optical convert` stay as they are.

## Layers

- `model::convert`: classify, the target tables, sizing, refit, verify and the per-file runner.
- `controller::convert`: the tab's state, actions and effects; replaces `bulk_convert` and
  `floppy_convert`.
- Shells: `src/gui/convert_tab.rs`, the TUI's Convert screen, `frontends/gtk4/src/convert/`.
