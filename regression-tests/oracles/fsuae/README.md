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
    99 no sentinel — harness failure, NOT a verdict       2  setup error
    77 fs-uae or a Kickstart is not on this host — skipped, not a verdict

77 and 99 are the harness-wide reserved codes `verify` maps to
skip-unavailable and error; every other code is a verdict on the volume.

### One run at a time

`--workdir` defaults to a single fixed path, so two of these running at once
share a boot volume, a config and a results directory and trample each other.
The symptom is a Python traceback or a missing sentinel on runs that pass in
isolation. Within one `verify` the artifacts go through sequentially and are
fine; the trap is starting a second `verify` while one is still going.

### Never let it write into the artifact tree

`verify` runs a check with the *artifact's* directory as cwd. Both defaults
here were repo-relative, so run that way they resolved inside
`artifacts/<os>/<format>/` and built the boot volume into the artifact tree.
They are anchored to the repo now (`REPO` in the script). Any new path default
must be absolute for the same reason.

### Always pair a pass with a negative control

A harness that reports success for anything is indistinguishable from a good
volume. 2 MB of `os.urandom` through the identical config yields no DH1: unit
and `Can't examine "DH1:"`, exit 1 — so the oracle discriminates.

### The trap: LF endings, written as bytes

The guest script must be LF-terminated and written with `write_bytes`. Python's
`write_text` applies the platform newline on Windows; AmigaDOS scripts are
LF-only, so the guest read `Info >RESULTS:info.txt
` and tried to create a
file whose name ended in a carriage return — illegal on the host directory that
DH2 maps to. Every redirect failed, `FAILAT 21` correctly stopped the script
aborting, and the run booted and silently produced nothing. It looks exactly
like an emulator that will not boot. It is not.

### The other thing that cost time

The probe *replaces* the fixture's `S/Startup-Sequence` rather than appending to
it. Workbench 1.3's own startup — `Mount NEWCON:`, `Resident`, then Amiga
Forever's `Execute S:AFShared-Startup` — never reaches an appended probe under
Kickstart 3.1. All this test needs is a shell, `Info`, `List` and `Echo`.

## Status, 2026-08-17 — `sfs_mount.py` written; the mount is not solved yet

`sfs_mount.py` is the SFS counterpart, built for F-009. It stages the handler
into the guest's `L:`, generates a MountList whose geometry covers the image
exactly, mounts with a 3.x `Mount` lifted out of the SFS reference fixture's
own `C:` (Workbench 1.3's `Mount` builds a device node the handler will not
start from), and separates its outcomes the way the AFFS oracle does.

It does not yet return a verdict. Every SFS run reports `absent` with
`Can't examine "SFSTEST:": not enough memory available`.

**A retracted claim, recorded because the retraction is the lesson.** This
section first said the FS-UAE environment had regressed, on the evidence that
`affs_mount.py` no longer reached its sentinel either. That was a bad control:
it was handed the bootable Workbench fixture *as the artifact*, so DH0 and DH1
were the same bootable volume and the guest never ran the probe. Pointed at an
ordinary AFFS volume the same afternoon, the AFFS oracle mounts it
`Read/Write rusty-backup` exactly as it always did. The environment is fine and
the SFS mount is a real, separate problem. **A control has to differ from the
thing it controls for**; one that shares the artifact's boot priority tests
nothing.

Both staged assets are corpus, not repo. The handler ships with the fixture
set; the `Mount` is read out of the SFS reference volume with our own tool:

    rb-cli get <sfs-fixture> /C/Mount regression-tests/fixtures/oracle-assets/amiga/Mount-3x

Routes eliminated for the SFS mount, so nobody repeats them:

- `Mount SFSTEST: from <file>` — 1.3's `Mount` predates the `from` keyword.
- `uaehf.device` unit 0 — FS-UAE's own log settles it: DH0/DH1/DH2 become
  `uaehf0/1/2` whether they are directories or hardfiles, so the artifact is
  **unit 1**. An earlier guess at unit 0 was wrong.
- `hard_drive_1_file_system` in the FS-UAE config — changes nothing.
- `uae_hardfile2` with the handler in WinUAE's filesystem field — FS-UAE
  normalises the option and strips that field back to empty
  (`...,512,0,,uae` in the log), so the handler never reaches the core.
- `List` before the sentinel — on an unmounted volume it blocks on AmigaDOS's
  "please insert volume" requester until the timeout, indistinguishable from a
  guest that never booted. The sentinel is written before it now.

**Next, and why it is WinUAE.** The one field that would attach the handler to
the unit is `hardfile2`'s filesystem slot, and FS-UAE is what strips it. WinUAE
takes that config directly, is already in `oracles.toml`, and EMULATORS.md
names it as the intended oracle for PFS3/SFS/RDB boot checks. It is installed
here. The MiSTer Amiga core is the other route and is real hardware rather than
emulation, which `local.toml` already records as authoritative for Amiga
filesystems; it needs scheduled time on the board.
