# Solaris 9 SPARC: what is still open

Tracking file for the Solaris 9 port. Everything here is known-incomplete or known-wrong, as
distinct from untested -- where something has not been verified, that is said outright.

Working today: cross build through mrustc, the release pipeline leg, disk-image operations,
the TUI, and read-only device enumeration. See `docs/build-sol9-mrustc.md` for the build and
`docs/solaris-raw-devices.md` for how devices are handled.

## Raw devices

- [ ] **Writing to a raw device is refused.** `open_target_for_writing_inner` in
      `src/os/mod.rs` still falls into the catch-all arm for Solaris and bails. Enumeration
      and reading are done; writing needs a device that is not the root disk to develop
      against, and Solaris raw nodes reject unaligned I/O with `EINVAL`, so
      `SectorAlignedWriter` has to be exercised properly rather than assumed.
- [ ] **Reading an explicit device path is untested.** `open_source_for_reading` is not
      gated for Solaris -- it falls through to a plain `File::open` -- so naming a
      `/dev/rdsk/...` path should work, but no read of a real disk has been done through it.
- [ ] **A CD-ROM reports `removable: no`.** `DKIOCREMOVABLE` either fails or returns 0 on the
      BlueSCSI emulation in the test machine, and `is_removable` treats a failed ioctl as
      "not removable". Whether that is the emulator or the ioctl has not been established.
      This is a known-wrong value, not a verified one.
- [ ] **Free space is not reported.** The third catch-all arm in `src/os/mod.rs` returns
      `None` for Solaris, so `MountedPartition::total_space` and `available_space` are always
      zero. `statvfs(2)` is the obvious fix.
- [ ] **Mounted-partition safety is untested.** Solaris will not let a mounted slice be
      written through its raw node; `device_safety.rs` should refuse first, with a clear
      message, rather than letting the write fail late.

## USB mass storage (beta)

- [ ] **`scsa2usb` wedges on non-compliant devices**, blocking in `open(2)` and ignoring
      `O_NONBLOCK`, leaving processes that `kill -9` cannot clear. Enumeration already routes
      around this by never opening a device, but any *use* of such a disk will hang.
      `scripts/solaris-usb-unblock.sh` diagnoses it and prints Oracle's remedy.
- [ ] **The `reduced-cmd-support` override is untried.** The documented fix for
      non-compliant devices has not been applied on the test machine, so we do not know
      whether it recovers a stick that is already wedged, or only prevents it.
- [ ] **No USB device has been read or written end to end.** Every USB test so far has been
      against one stick that wedges, so the USB path is entirely unproven.

## Solaris 10

- [ ] **Deliberately not built.** Support was implemented and then removed: Solaris
      guarantees forward binary compatibility, so the Solaris 9 artifact is expected to run
      on 10 and 11. **This has never been tested** -- the test machine was reinstalled to 9.
      Copying `rb-cli-sol9` onto a Solaris 10 box settles whether a second target is needed
      at all. The mrustc target (`sparcv9-sun-solaris`, GCC 11) exists if it is.

## Build and pipeline

- [ ] **minicargo races on build scripts.** It can schedule one crate's build script twice
      concurrently -- visible as `serde_core (build)` listed twice in its own progress line
      -- and the second worker then execs the binary while the first is still linking it,
      before the linker has set the executable bit. It surfaces as `Unable to run process
      ... Permission denied` partway through the graph, and is intermittent: the same
      commit and seed failed once and passed on re-run. The CI job retries the stage once,
      which works because the build is incremental, but the race is upstream in minicargo
      and that retry is a mitigation rather than a fix.
      **2026-09-12:** the retry used to fail on a *different* error than the one it was
      retrying. The losing worker leaves the build-script marker behind with an empty
      `OUT_DIR`, so the second attempt trusted the marker, skipped re-running the script
      and died with `Unable to open .../private.rs`. The retry now clears
      `$SOL9_OUT/{,host/}build_*` first, so the scripts genuinely re-run. Upstream, the
      two minicargo commits on the `ppc-build-2026-09` branch (`0d3211be`, `debcce0e`)
      address the scheduling side, but CI builds from a prebuilt seed that predates them,
      so a reseeded toolchain is what would actually retire this item.
- [ ] **The seed pins an mrustc commit implicitly.** `scripts/pack-sol9-seed.sh` packs
      whatever `bin/mrustc` and the stdlib outputs happen to be, with nothing recording which
      commit built them. Stamping that into the seed would make a stale one obvious.
- [ ] **`docs/cli-reference.md` has no `mv` entry.** Unrelated to Solaris, but found here:
      the verb shipped without the reference being updated.

## Upstream mrustc

- [ ] **The C-calls-Rust direction of the FFI enum fix is unfixed.** libgcc's unwinder calls
      `rust_eh_personality` with a plain int while mrustc declares it as a struct, so on
      64-bit big-endian that boundary still mismatches. Pre-existing upstream behaviour,
      documented in the commit rather than silently left; fixing it needs a conversion at
      function entry. See thepowersgang/mrustc#428.
