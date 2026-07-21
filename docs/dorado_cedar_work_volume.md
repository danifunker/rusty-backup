# Reproducible Cedar/Dorado work volume

`scripts/build-cedar-work-volume.sh` creates the baseline volume for Cedar
bring-up in the sibling Dorado checkout.  It is intentionally a clean work
volume rather than a repacked kitchen-sink disk.

## Build

From this Rusty Backup checkout:

```sh
scripts/build-cedar-work-volume.sh ../../CedarDisk/CedarDorado-work.pdi
```

The script uses the matching `Dorado.germ-6.1.6` and
`BasicCedarDorado.boot!22` from the sibling repository by default.  Override
either only when deliberately testing a matched alternative:

```sh
CEDAR_GERM=/path/to/germ CEDAR_BOOT=/path/to/boot \
  scripts/build-cedar-work-volume.sh /tmp/cedar-work.pdi CedarWork
```

It performs this exact sequence:

1. `pilot_probe new 65535 cedar CedarWork` creates a 65,450-page logical
   Cedar volume in a 65,535-record PDI.
2. `install-boot germ` installs FileID 2 and updates physical-root
   `bootingInfo[germ]` and its page-label boot chain.
3. `install-boot bootfile` installs FileID 3 similarly.
4. `set-dir` creates an empty Cedar `client` B-tree with reserved B-tree pages
   so Cedar may allocate local names.
5. `verify` checks VAM-versus-label allocation, both boot chains, source
   hashes, and PDI writer round-trip.  When available, Dorado's independent
   `pdidump` then checks the physical-volume root and VAM references.

The output has approximately 64,309 free pages after its boot payloads and
empty name directory.  It should be the starting point for every experiment:
first cold-boot, log in, and prove Cedar can create a local file; only then add
one package group at a time.

## Acceptance command

The generated image is accepted only after the emulator can use it:

```sh
cd ../../dorado
./build/dorado-sdl --boot-reason disk --no-alto-boot \
  --eb ../chm/dorado/CedarDorado.eb!6 \
  --germ ../chm/cedar/germ-alt/Dorado.germ-6.1.6 \
  --pilot-disk ../CedarDisk/CedarDorado-work.pdi
```

The first GUI/login image is not enough.  The test passes when the Guest login
can create or open a local file without `FS.Error: No more free pages/names on
a local volume`.

### Current observed result

On 2026-07-11, a fresh `CedarDorado-work.pdi` built by the script passed
`pilot_probe verify` and Dorado `pdidump`, then cold-booted to Cedar 6.1's
SimpleTerminal `Please login ... / Name:` screen at 720,000,000 emulator
cycles (28,538 display-list pixels).  This proves the generated volume is a
valid graphical-login boot volume.  Guest login plus successful local-file
creation is still the required next acceptance result.

Do not reclaim space from an installed image by deleting an arbitrary FileID
range.  Pilot FileID ordering does not encode Cedar package dependencies.

## Fast debugging snapshot

Once the Guest/Loader state is stable, save a fresh snapshot (do not reuse an
old snapshot after changing the emulator ABI):

```sh
cd ../../dorado
./build/dorado --boot-reason disk --no-alto-boot \
  --eb ../chm/dorado/CedarDorado.eb!6 \
  --germ ../chm/cedar/germ-alt/Dorado.germ-6.1.6 \
  --pilot-disk ../CedarDisk/CedarDorado-work.pdi \
  --ftp-root ../chm/cedar/stp-root \
  --type-at 760000000 --type 'Guest\n\n' --cycles 1350000000 \
  --snapshot-out /tmp/cedar-work-loader.snap
```

Restore with the PDI and FTP root explicitly supplied again; this keeps the
host-side media and server configuration current after snapshot restore:

```sh
./build/dorado --snapshot-in /tmp/cedar-work-loader.snap \
  --pilot-disk ../CedarDisk/CedarDorado-work.pdi \
  --ftp-root ../chm/cedar/stp-root --cycles 10000000
```

This was verified on 2026-07-11: the restore returned to the LoaderDriver
state with 30,592 rendered pixels after 10M additional cycles.
