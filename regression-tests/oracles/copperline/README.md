# Copperline emulator oracle

The first emulator oracle `verify` can actually run.

FS-UAE and WinUAE need a preconfigured guest — a Workbench profile, a licensed
Kickstart, a probe spliced into `S/Startup-Sequence`, and a sentinel file plus
a timeout because AmigaDOS has no "quit the emulator" verb. Copperline is
headless, unthrottled, deterministic, takes its whole machine from a config
file and exits on its own at a stated emulated timestamp. That is the whole
reason this exists.

## What it checks

`attach_check.py <image> [dostype-hex]`

Copperline classifies an image *before any ROM runs*: an image with its own
RDSK is used as-is, and a bare partition hardfile is wrapped in a synthesized
RDB carrying **the dostype read out of the image's own boot block**. The check
asserts that classification happened and that the dostype is the one we wrote.

So this is a second implementation reading our container and boot block. It is
**not** a filesystem verdict — nothing mounts the volume — and it is scored
`smoke` in `oracles.toml` to say so.

    exit 0  recognised, dostype matches
    exit 1  not recognised, or a different dostype   (a verdict)
    exit 2  no Copperline binary                     (not a verdict)

It boots Copperline's own AROS ROM, so no host needs a licensed Kickstart.

## It has a negative control, because the first version did not

Written the obvious way, the check passed 2 MB of `urandom`: Copperline
attaches an image it cannot classify as a plain raw disk, silently, and
"no complaint" was being read as success. A harness that approves anything
cannot tell a good volume from noise.

The fix is that recognition must be *positive* — the bare-partition wrap line
with the expected dostype — so the absence of a signal is a failure. Noise now
fails, and so does a correct volume checked against the wrong dostype. Any
change here should be re-run against both.

For the same reason only **bare** volumes are covered. A whole-disk RDB image
produces no log line to key on, and neither does noise, so there is nothing to
tell them apart; claiming a pass on that absence is exactly the bug above.

## Making it authoritative

Mounting the volume needs a guest OS: a Kickstart ROM and a Workbench, neither
redistributable, so that artifact is local corpus rather than repo. The shape
is a multi-partition RDB — bootable Workbench on DH0, the volume under test on
DH1 — with the filesystem handlers embedded in the RDB's FileSystemHeader
chain (`rb-cli new hd rdb --filesystem`). That boots to a Workbench desktop
under Copperline today; what is not yet solved is reading a verdict back out
of the guest. Copperline routes Paula's serial to stdout, which is the obvious
channel and needs no results volume.
