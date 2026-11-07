# CD-ROM building: mastering discs from content

Split out of [build_disk_plan.md](build_disk_plan.md) on 2026-11-05 to run later; it is
not scheduled. The user wanted the quick wins (archive expansion, G14 there) first. Read
CONTRIBUTING.md § Architecture policy (MVC) before starting; it is binding.

Today `rb-cli optical new sgi-efs | mac-hfs | mac-hfsplus | next-ufs` builds discs in one
step from a host folder (`--from-dir`, `--size auto`), and `fs::dir_import` does the archive
expansion. This plan adds an optical-disc kind to the Build Disk wizard, an ISO 9660 writer,
and CD audio.

## Gaps

| # | Gap | Where | Size |
|---|---|---|:-:|
| G15 | **CD audio mastering**: audio-only and mixed-mode discs from WAV / AIFF / FLAC, to BIN/CUE or CHD-CD. | a crate + `src/model` | M-L |
| G12 | **ISO 9660 mastering**: our own streaming writer (see "ISO 9660 writer" below), Level 1 / 2 / 3, Joliet, Rock Ridge, then Apple extensions and an HFS hybrid. | a crate | L |

## Optical discs take their content at creation

A hard disk is **created, then populated**: the build formats blank partitions (or pours in a
source image), and files go in afterwards through Inspect / Commander, or through a row's
host-folder content (phase 9). A CD is **mastered**: what goes on it is chosen first and the
disc is sized to fit, because the real medium is written once. So **optical disc** is its own
kind with its own steps, not a partitioned disk with a CD geometry:

| # | Step | What it asks | Next is blocked when |
|---|---|---|---|
| 1 | Kind | optical disc | - |
| 2 | Disc format | Mac HFS, Mac HFS+, SGI EFS, NeXT UFS today; ISO 9660 (+ Joliet / Rock Ridge, + HFS hybrid) when G12 lands; format options here (HFS block size, UFS bytes per inode) | - |
| 3 | Content | the **disc label** (validated live against the format's rules: HFS 27 bytes, ISO 9660 levels, ...), one or more host folders, and the **expansion options** below | no content chosen; a label that breaks the format's rules |
| 4 | Output | `.iso`, or CHD (CD) / BIN+CUE from phase 6; never a physical disk | no path |
| 5 | Review | the fitted size, and whether it fits a 74 / 80-minute CD or needs a DVD | - |
| 6 | Build | progress; then "Open in Inspect" | (running) |

**Expansion on a disc (survey 2026-11-05).** `fs::dir_import` is shared by all three CD
builders. Today it does the following:

| Input | Today | Notes |
|---|---|---|
| tar and its compressed forms (`.tar`, `.tgz`, `.tar.gz` / `.bz2` / `.xz` / `.zst`, `.gtar`, ...), IRIX `.tardist` | yes, `--expand-archives` (SGI, Mac, NeXT) | one folder per archive, or `--flatten-folders` into the root (SGI, NeXT) |
| gzip without a tar inside | yes, `--expand-gunzip` on every CD format and `import` | every format since 2026-11-05 |
| StuffIt `.sit` / `.sea`, Compact Pro `.cpt`, BinHex `.hqx` (and `.sit.hqx`), MAR `.mar` | yes, `--expand-archives` | through `arc-mac`; forks land as forks on HFS / HFS+, as AppleDouble on the others |
| AppleDouble sidecars (`._x`, `__MACOSX`) | yes, `--include-appledouble` | merged into the file's forks on HFS |
| ZIP (`.zip`; macOS `__MACOSX` / `._name` forks joined; Info-ZIP MacZip through `arc-mac`) | yes, `--expand-archives` (2026-11-05) | `fs_tar::zip_import`, streamed member by member |
| MacBinary `.bin` and AppleSingle as single files | yes, always on HFS / HFS+ targets (AppleSingle since 2026-11-05) | `fs_mac::resource_fork::detect_resource_fork`; other targets keep the file as it is |
| Disk images (`.dsk`, `.img`, DiskCopy, `.dmg`, `.hfv`, ...) | yes, `--expand-images` (2026-11-05) | `fs::image_import`, through the Commander's descend code; one folder per image, a subfolder per partition; an image with nothing browsable is copied as a file |
| LhA / LZH (Amiga, Japanese PC) | **no** | needs a decoder: M |
| 7z, RAR | no | not planned (dependencies, licensing) |

In the wizard these are checkboxes on the Content step, each listing the types it covers:
expand archives, strip gzip only, decode Mac single-file encodings, merge AppleDouble,
flatten into the root, plus the SGI and NeXT permission option. Each format offers only the
options that make sense for it: forks matter on HFS / HFS+, and "flatten" is for `inst`
trees. The ZIP, MacBinary / AppleSingle and disk-image rows were **G14** in the Build Disk plan, done 2026-11-05.

## CD audio mastering (asked 2026-11-05)

Audio tracks are possible only on CD output. They need a track-aware container (BIN/CUE, or
CHD for CD), never `.iso`, so the step appears only when the Output step can hold tracks.
This is **G15**, after phase 6 (containers):

- **Disc shapes:** audio-only (Red Book) and mixed mode (data track 1 plus audio tracks,
  the common game / multimedia shape). Enhanced CD / CD Extra (audio first, data in a
  second session) needs multisession and comes later, if at all.
- **Inputs:** WAV and AIFF (parsers we write: S), FLAC (a pure-Rust decoder that builds on
  Rust 1.73: to check). 44.1 kHz 16-bit stereo, or resampled with a warning. Track order,
  2-second pregaps, optional CD-Text titles.
- **Steps:** a **Tracks** step after Content (an ordered list of tracks with their lengths),
  and Review shows the total minutes against 74 / 80.
- **Checking:** our own CD-DA reader (`optical::cd_audio`) reads the tracks back, and each
  track is compared byte for byte with its PCM; MAME / an emulator reads the CHD.

The CLI keeps `optical new <format> --from-dir` (with `--size auto`) and gains the same
through `rb-cli build`. A blank disc stays possible from the CLI only, for `import` / `put`
workflows that already use it.

Rules:

- **Back never loses entries.** Changing Kind or scheme keeps what still applies (rows survive
  a scheme change when their sizes still place; types are re-proposed per scheme).
- The GUI shows the steps as a list on the left; any step already reached can be clicked.
  Steps that don't apply to the chosen kind are hidden, not greyed out.
- `next_blocked()` is the same text the GUI shows under a disabled Next and the TUI shows in
  its status line.
- Every step's widgets get ids under `build_disk.*` for the scenario runner.
- Phases fill the steps in over time: phase 2 has Kind = partitioned disk only, with rows
  holding empty or image content; phase 4 adds formatted rows and step 4; phase 5 the floppy
  and volume kinds; phase 6 the containers in step 5; phase 9 the presets and host folders.

## ISO 9660 writer (G12; surveyed 2026-11-05)

### Existing crates

[`hadris-iso`](https://docs.rs/crate/hadris-iso/latest) 2.5.0 (MIT) writes ISO 9660 with
Joliet, Rock Ridge and El Torito, but it does not fit our builds:

- **Rust:** edition 2024, `rust-version = 1.88`. Our CD builders are not behind the `optical`
  feature, so they ship in the Rust 1.73 builds (Windows 7, macOS 10.7, PowerPC, Solaris 9).
- **Proc-macros:** `hadris-macros` and `bytemuck` derive, which mrustc cannot build.
- **Gaps:** its Rock Ridge write uses fixed defaults (mode 0755 / 0644, uid / gid 0); host
  permissions are not carried. There is no HFS hybrid. Output goes to a `Cursor` in its
  examples, so streaming to a large sink is unclear.

`hadris-cd` (UDF bridge) has the same constraints. The other ISO crates only read.

### Decision

**Write our own**, in a crate, streaming to `Write + Seek`, on Rust 1.73, with no
proc-macros. Reading is already done by `opticaldiscs` (ISO 9660, Joliet, Rock Ridge, High
Sierra), so every write round-trips through our own reader too. The home is a new pure crate
(`iso-master`, MIT, no `drives` / audio deps), so the 1.73 and mrustc builds can enable it.
`opticaldiscs` reads, `iso-master` writes.

### What it writes, in order

| Step | Standard | What it is for | Size |
|---|---|---|---|
| 1 | ISO 9660 Level 1 / 2 / 3 | 8.3 names (L1), 31-character names (L2), files over 4 GiB in multiple extents (L3). Path tables (L and M), directory records, PVD and terminator, deep-directory rules | M |
| 2 | Joliet (SVD, UCS-2) | long Unicode names (64 characters) for Windows | S |
| 3 | Rock Ridge (RRIP 1.12 over SUSP) | Unix: POSIX mode, uid / gid, nlink (`PX`), long names (`NM`), symlinks (`SL`), timestamps (`TF`), device nodes (`PN`), deep-directory relocation (`CL` / `PL` / `RE`), taken from the host files or from a source filesystem | M |
| 4 | Apple ISO 9660 extensions (`AA` / `BA` SUSP) | type / creator and Finder flags on a plain ISO, for classic Mac OS | S |
| 5 | HFS hybrid | one disc that mounts as ISO on a PC and HFS on a Mac, with file data shared between both. Needs the HFS writer to place extents at given sectors | L |
| 6 | El Torito | boot record and catalog. The boot image comes from a validated donor, as in `docs/bootable_disks_backlog.md`; we don't ship boot images | S |
| later | UDF, CD-ROM XA (mode 2) | UDF for DVD-sized trees, XA for VCD / PlayStation (needs 2352-byte BIN/CUE) | L / M |
| - | Amiga `AS` extension, High Sierra | Amiga protection bits (S, if asked); High Sierra is pre-ISO, read-only is enough | - |

### Oracles

Already installed here: `isoinfo -d -l -R -J`, `isovfy`, and `xorriso -indev X -check_md5 /
-find` read and check what we write, and `genisoimage` / `xorriso -as mkisofs` build reference
images from the same tree. The goal is matching directory trees and metadata, not
byte-identical images. A control (a damaged path table) must fail `isovfy` before the oracle
is trusted. Real OS reads: Windows 7 VM (Joliet), the FreeBSD VM (Rock Ridge), classic Mac
OS in an emulator (Apple extensions, hybrid).
