# Amiga work — resume here

Written 2026-08-18 at a task switch. Everything below is either committed or
reproducible from committed code; nothing depends on a live session.

## The prompt

Paste this to pick the work back up:

> Continue the SFS mount oracle in rusty-backup. Read `docs/AMIGA-RESUME.md`
> first — it has the plan, the geometry, and six gotchas already paid for.
> Short version: boot a copy of the `fs.sfs.workbench-dh0.hd.img` fixture (a
> real bootable AmigaOS system carrying the genuine SFS handler in `/L`) as DH0
> with our `new volume sfs` output as DH1, using 16x63 geometry and
> `new hd rdb --filesystem` to embed the handler. If Workbench appears, our
> FSHD chain works; the `DH1:` line is then a verdict on our SFS writer. Model
> it on `regression-tests/oracles/copperline/affs_mount.py`, which already does
> the same thing for AFFS end to end.

## The goal

`fs.affs` has an authoritative mount verdict, in
`regression-tests/oracles/copperline/affs_mount.py`. Do the same for `fs.sfs`,
then `fs.pfs3`. The blocker is that Kickstart has FFS in ROM but no
SFS handler, so an SFS check has to supply one — and until that is proven to
load, a `Not a DOS disk` result says nothing about our bytes.

## The plan, and why it is the right one

Boot **a real Amiga system that already has SFS installed**, then browse our
volume as the secondary disk. The corpus fixture
`fs.sfs.workbench-dh0.hd.img` is exactly that: a complete bootable AmigaOS
system carved from a real 128 GB Amiga disk, carrying the genuine handler.

    /L/SmartFileSystemFixed   97984   <- Amiga-authored, not ours
    /L/FastFileSystem         30532
    /S/  WHDLoad-Startup, PreVP-Startup-sequence
    /    Utilities WBStartup Expansion Prefs Storage System Tools Devs

**Booting it is self-validating.** Kickstart cannot read an SFS volume until
the handler is loaded, and the only place it can come from is the RDB's
FileSystemHeader chain — the one `new hd rdb --filesystem` writes (c5fc075). So
if that disk reaches Workbench at all, our FSHD chain provably works, and the
`DH1:` line is then a clean verdict on our SFS *writer*. One boot separates
handler-loading from bytes, which nothing so far has managed.

## Disk shape

    DH0   copy of fs.sfs.workbench-dh0.hd.img (1021104 blocks), bootable,
          its S/Startup-Sequence REPLACED by the probe
    DH1   our `new volume sfs` output, a multiple of 1008 blocks (516096 bytes)
    FSHD  the genuine SmartFileSystemFixed, carved out of DH0's own /L

Geometry is the default **16 heads x 63 sectors = 1008 blocks**, because
1021104 = 2^4 * 3^2 * 7 * 1013 and 1008 divides it exactly (1013 cylinders).
That also leaves a 1008-block RDB reserve, ample for the ~200-block handler
chain. This is why the earlier attempt failed: pairing the SFS fixture with the
*AFFS* Workbench volume gives gcd(4040, 1021104) = 8 blocks, far too small to
hold the chain.

The fixture is read-only corpus, so this works on a ~500 MB copy.

## Steps

1. Copy the fixture to scratch. Carve nothing — it is already a bare volume.
2. Extract the handler: `rb-cli get <copy> /L/SmartFileSystemFixed <path>`
   (also staged in the corpus at
   `regression-tests/fixtures/oracle-assets/amiga/SmartFileSystemFixed`).
3. Build our test volume: `rb-cli new volume sfs --size 8257536` (16 cylinders)
   and put a file in it, so a mounted volume can be shown to list.
4. `rb-cli new hd rdb --size <sum+slack> --heads 16 --sectors 63
   --partition '522805248:SFS\0:DH0' --partition '8257536:SFS\0:DH1'
   --fill 1=<copy> --fill 2=<ours>
   --filesystem 'SFS\0=<handler>' <disk>`
5. `rb-cli partmap set-bootable <disk> 1 --bootable`  — required; see below.
6. Replace DH0's startup, exactly as the AFFS oracle does:
   `rb-cli put <disk>@1 <probe> /s/Startup-Sequence --force`
   The probe body and the SER: MountList entry are in that oracle; reuse
   them verbatim. The 3.x `Mount` is staged in the corpus at
   `regression-tests/fixtures/oracle-assets/amiga/Mount-3x`.
7. Boot headless and read the verdict off stdout:
   `copperline --config <cfg> --noaudio --screenshot-after 40 <png>`

If Workbench appears, the FSHD chain works. If `DH1:` then reports
`Read/Write`, our SFS writer is good and the check can be wired into
`oracles.toml` the way the AFFS one is (`strength = "authoritative"`).

## Gotchas, all paid for already

- **Profile and ROM must match.** An A1200 Kickstart under an A4000 profile
  boots to a bare shell and runs no startup — indistinguishable from a volume
  that will not mount. Use `profile = "A1200"` with the A1200 ROM.
- **Replace the Startup-Sequence, never append.** A real system's own startup
  never reaches an appended probe. This fixture's pulls in WHDLoad/PreVP
  machinery, so expect to replace it outright.
- **Copperline's host-directory mounts boot but never run S/Startup-Sequence.**
  The boot volume must be a real RDB partition. Their in-memory `dirfs`
  builder; not fixable from here.
- **Each volume must exactly fill its partition** — AFFS stores no size (R-042)
  and SFS records its own, but a mismatch bit us both ways. Size partitions to
  volumes, not the reverse.
- **`new hd rdb` marks nothing bootable.** `pb_Flags` stays 0, so the ROM has
  nothing to boot. Call `partmap set-bootable` until that is fixed.
- **MSYS path mangling.** `rb-cli` Amiga paths need `MSYS2_ARG_CONV_EXCL='*'`
  so `/S` stays `/S`; Copperline needs real Windows paths (`C:/...`) and must
  NOT have that variable set, or `/c/...` reaches it unconverted and it fails
  with `os error 3`.
- **One oracle run at a time.** The FS-UAE oracle shares a fixed workdir; two
  concurrent `verify` runs trample each other. Copperline's is under
  `scratch/copperline` and has the same shape.

## Defects found on the way here, none fixed

- `new hd rdb` never sets the RDB bootable flag on any partition.
- `rb-cli resize` is a silent no-op growing AFFS: the file grows, the volume
  does not, and nothing is logged. R-042's mistake from the other direction.
- `produce.toml` emits no `fs.pfs3` or `fs.sfs` artifact, so even a working
  check would have nothing to verify. Fix this before wiring the claim.
- `fs.pfs3` has three oracle claims and zero runnable checks; `fs.sfs` has one
  claim and no check.

## State at the switch

Committed on `feature/f-008-backup-containers`, unpushed:

    04867aa  a real AFFS mount verdict from Copperline, over Paula serial
    65e7e83  the FS-UAE check can run, and a check can say "not a verdict"
    e29ed80  Copperline as an oracle `verify` can actually run
    c4e5c1a  record R-042, and a case that would have caught it
    c5fc075  embed filesystem handlers in the RDB — `new hd rdb --filesystem`
    b341a5f  an AFFS partition that is not last on its disk can now be opened

`rb-regress verify` on this host: pass 21, FAIL 4, error 2. The four FAILs are
stale linux/macOS artifacts that the old smoke check used to pass; the two
errors are the FS-UAE oracle correctly declining to give a verdict on them.
Re-run `rb-regress produce` on those hosts to clear them.

F-009 itself — the SFS extent b-tree splits — is still not started. The format
groundwork stands: interior nodes are 8-byte (key, child) pairs, the leftmost
separator is always 0, and separators satisfy `key <= subtree_min`, so deletion
needs no parent maintenance. `examples/probe_sfs_btree.rs` (untracked) dumps
all of it.
