# FS-UAE emulator oracle

Boots a real AmigaOS against an artifact and reports what it makes of it. The
first oracle here that is not a command-line tool.

## How the verdict gets out

FS-UAE mounts a **host directory** as an Amiga volume:

    hard_drive_2 = <results dir>
    hard_drive_2_label = RESULTS

so the guest writes `RESULTS:info.txt` and the host reads it. No screen
scraping, no OCR, no control-frame image diff — the difference between this and
the Snow testrunner approach, and the reason FS-UAE is the emulator oracle of
record.

`done.txt` is a sentinel: the host polls for it, then terminates the emulator.
AmigaDOS has no clean "quit the emulator" verb, so a sentinel plus a timeout is
the honest mechanism. Absence of the sentinel means the guest never got there,
which is a different result from a bad volume and is reported as such.

## Layout

    hard_drive_0 = <profile>/System   bootable Workbench (a host directory)
    hard_drive_1 = <artifact>         the thing under test, mounts as DH1:
    hard_drive_2 = <results dir>      verdict channel

The Workbench profile is a **copy** of Amiga Forever's
`Amiga Files/Shared/dir/System`, because the probe has to be inserted into its
`S/Startup-Sequence`. Never edit the Amiga Forever install in place.

Kickstarts come from the licensed Amiga Forever set. Filesystem handlers for
PFS3 and SFS are staged at `rb-fixtures/oracle-assets/amiga`, extracted from
the SFS reference fixture's own `L:` — neither Amiga Forever nor AmigaVision
ships an SFS handler.

## Always run a control

A bad mount and a bad image look identical from the host. Every conclusion here
must be paired with a known-good volume through the *same* config: R-020 was
only credible because `Mister-3-2.hdf` mounted as `Read/Write Amiga32` under
byte-identical settings while our own AFFS output did not.
