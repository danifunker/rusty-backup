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

## Status, 2026-08-14 — `affs_mount.py` runs, the guest does not report

`affs_mount.py` is the script the notes above describe: it unpacks the
Workbench fixture to a host directory, writes the probe, generates the config,
launches FS-UAE, polls for the sentinel and parses `Info`. Everything up to the
guest works, verified against FS-UAE's own log:

    hard drive mount: .../System        device DH0, boot priority 0
    hardfile          .../under-test.hdf device DH1, boot priority 0
    hard drive mount: .../results       device DH2, boot priority 0
    FS: mounted virtual unit DH0
    FS: mounted virtual unit DH2
    Mounting uaehf.device 1 (0) (size=2097152)

So the ROM loads, all three drives attach and the artifact is presented to
Kickstart 3.1. **What does not happen is the guest executing
`S/Startup-Sequence`** — `RESULTS:` stays empty, no `done.txt` appears, and the
script correctly reports exit 3 ("harness result, not a verdict") rather than
inventing a verdict about the volume.

Two things already ruled out:

* **Not the fixture's startup script.** The first attempt appended the probe to
  the Workbench 1.3 `Startup-Sequence`, whose Amiga Forever chain
  (`Mount NEWCON:`, `Execute S:AFShared-Startup`, …) never reached it. The
  probe now *replaces* that file with five lines, and the replacement is
  confirmed on disk. No change.
* **Not missing commands.** `c/Info`, `c/List` and `c/Echo` are all present in
  the unpacked tree.

**The next step needs a screen, not another blind run.** Launch the generated
config by hand and watch what the Amiga actually does — whether it sits on the
Kickstart insert-disk hand, throws a Guru, or boots and fails at the redirect:

    regression-tests/scratch/fsuae/probe.fs-uae

Each of those points somewhere different, and one look settles which. Until
then this oracle answers "harness not ready", and **R-020 stays open on its
original evidence** — nothing here has yet put a real Kickstart's opinion on
one of our volumes.
