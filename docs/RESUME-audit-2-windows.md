# Resume prompt: audit sweep, leg 2 of 3 (Windows)

Run after `docs/RESUME-audit-1-linux.md`. Pull `bugfixer1` first.

## Prompt

You are continuing the 2026-09-01 codebase audit on branch `bugfixer1` on
the Windows development machine. Leg 1 finished the Linux-verifiable items;
this leg does the Windows-only findings and verifies, against Windows itself,
the NTFS / exFAT / FAT fixes that shipped on 2026-09-01 and 2026-09-02.
One finding per commit; the pre-commit hook (fmt + clippy -D warnings) is on;
run `cargo test --lib` before each commit. CONTRIBUTING.md rules apply:
two-line comments, ASCII-only UI strings, no threads spawned from `gui/`,
engine code stays on Rust 1.73.

### 0. First: confirm the build no longer takes the machine down

Debug builds were OOM-killing sessions on both machines. The fix (a687f64)
is `debug = "line-tables-only"` in Cargo.toml and `jobs = 4` in
`.cargo/config.toml`; it is verified on Linux only. On this machine:

1. Delete `target\debug` (every artifact in it predates the profile change).
2. Add the repo's `target` folder to Defender's real-time exclusions; scanning
   tens of GB of artifact writes is the same I/O stall the bug is made of.
3. Run `cargo test --no-run` and watch committed memory in Task Manager.
4. Record the peak and the lib-test binary size in the "Still open" list of
   `docs/build-memory-crashes.md`, and commit that.

If it still dies, the doc's "If it still happens" section lists the next
levers; try `split-debuginfo` is not one of them on MSVC (packed only).

### 1. Windows-only findings (R14, R15, plus the Win7 trap)

- R14: `os/windows.rs::request_elevation` relaunches without argv, so a
  file-association double-click that needs elevation loses the file.
  Forward the arguments.
- R15: `os/windows.rs` locks only lettered volumes and enumerates
  `PhysicalDrive0..15` only. Enumerate through the drive count and lock
  mount-point-less volumes too.
- Known Win7 trap (see the memory note if you have it, otherwise the
  commit history for "DeviceIoControl"): a `DeviceIoControl` with NULL
  `lpBytesReturned` and NULL `lpOverlapped` access-violates on Windows 7
  only. Grep every call and make sure each passes a real `lpBytesReturned`.

### 2. Verify the filesystem fixes against Windows

Build images with rb-cli, attach them with Disk Management (Action > Attach
VHD; `rb-cli new hd` writes fixed VHDs) or mount the raw image with any
tool you trust, then let Windows judge:

- D12 (251b211, NTFS rename replaces every name): put a file with a long
  name onto an NTFS image with Windows first so it carries a DOS alias,
  rename it with `rb-cli mv`, attach, run `chkdsk X:` (read-only) and
  `dir /x`. Expect a clean chkdsk and one short name.
- D8 / D10 (9e083f5): delete a hard-linked file with rb-cli, chkdsk clean;
  backup boot sector read by chkdsk after a resize.
- D1 / D5 / D7 / D9 (e008eff, e5266da): exFAT files Windows wrote as
  NoFatChain survive rb-cli edits; a directory grown past one cluster lists
  every file in Explorer; a resize past the bitmap keeps the up-case table.
- D13 (eaa6f3d): reconstruct a Ghost image with prefix-sharing long names,
  attach, `dir /x` shows distinct 8.3 aliases.
- D2 (33115a8): restore an NTFS partition to a new LBA and boot it, or at
  least confirm sector 6 still holds NTFS boot code, not a FAT backup BPB.

Record each result (pass / fail plus the chkdsk output) in
`docs/Regression_Bugs.md` next to its ID.

### 3. Optional: the wire changes across platforms

Run `rb-cli serve` on the Linux box and use this machine as the client to
exercise R12 (an existing remote image is refused without `--yes`), R13 and
R18 (declared-versus-received sizes). The Windows client and Linux daemon
should agree byte for byte with `rb-cli cp`.

### Close-out

Update the status line below, commit, push, then hand over to
`docs/RESUME-audit-3-macos.md`.

Status: not started.
