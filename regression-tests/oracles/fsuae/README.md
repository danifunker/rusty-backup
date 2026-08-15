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

## Status, 2026-08-14 — working; it answered R-020

`affs_mount.py` runs end to end. Kickstart 3.1 on an A1200 mounts our AFFS
volume `Read/Write` under its own name and `List DH1:` reads the contents
back, which closes R-020. Run it as:

    python regression-tests/oracles/fsuae/affs_mount.py <image>

    0  mounted        1  not mounted (a verdict)
    3  no sentinel — harness failure, NOT a verdict        2  setup error

### Always pair a pass with a negative control

A harness that reports success for anything is indistinguishable from a good
volume. 2 MB of `os.urandom` through the identical config yields no DH1: unit
and `Can't examine "DH1:"`, exit 1 — so the oracle discriminates.

### The trap: LF endings, written as bytes

The guest script must be LF-terminated and written with `write_bytes`. Python's
`write_text` applies the platform newline on Windows; AmigaDOS scripts are
LF-only, so the guest read `Info >RESULTS:info.txt` and tried to create a
file whose name ended in a carriage return — illegal on the host directory that
DH2 maps to. Every redirect failed, `FAILAT 21` correctly stopped the script
aborting, and the run booted and silently produced nothing. It looks exactly
like an emulator that will not boot. It is not.

### The other thing that cost time

The probe *replaces* the fixture's `S/Startup-Sequence` rather than appending to
it. Workbench 1.3's own startup — `Mount NEWCON:`, `Resident`, then Amiga
Forever's `Execute S:AFShared-Startup` — never reaches an appended probe under
Kickstart 3.1. All this test needs is a shell, `Info`, `List` and `Echo`.
