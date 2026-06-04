# MiSTer On-Hardware Deployment & Verification Plan

A reproducible runbook for validating every Rusty Backup MiSTer-core
filesystem on real MiSTer hardware. The plan covers Wave-1 cores
(already shipped and unit-test-verified), Wave-2 cores (in progress),
and Wave-3 cores (long tail). Each entry is structured so the user can
work through it one core at a time, in any order, without re-deriving
the procedure.

**Status:** authored 2026-06-03 alongside the Wave-2 kick-off. Execution
is intentionally deferred — engine + write-path + rb-cli round-trip
already prove byte-level correctness; this runbook is the final
"the real core actually boots the image" cross-check.

---

## 1. Test environment

### 1.1 MiSTer hardware + media

- MiSTer FPGA board (DE10-Nano + IO board recommended; bare DE10-Nano
  also fine for read-only checks).
- USB keyboard for in-core navigation.
- SD card ≥ 16 GB, formatted ExFAT, with the latest MiSTer release
  installed (https://github.com/MiSTer-devel/Distribution_MiSTer).
- HDMI display capable of 720p (most MiSTer cores).
- Optional: USB drive for parallel storage when SD-card swap latency
  matters.

### 1.2 Networking + file transfer

The runbook assumes one of these channels for moving rb-cli-built
images onto the MiSTer:

- **SCP via MiSTer's SSH** (preferred for iterative testing) — MiSTer
  ships with `dropbear`; default user `root` / password `1`. From the
  host:
  ```bash
  scp out.img root@mister.local:/media/fat/games/AtariST/
  ```
- **CIFS mount** — edit `/media/fat/cifs.sh` to mount a network share
  at `/media/fat/cifs/`. Slower but survives reboots.
- **Physical SD swap** — pull the card, mount on the host, copy the
  image into `/games/<core>/`. Slowest; use only for one-shot checks.

`/media/fat/` is the SD card root inside MiSTer's Linux userland. The
`/games/<core>/` directories below it are what the per-core file
picker scans.

### 1.3 rb-cli build for the test host

The runbook assumes the WSL Ubuntu-24.04 build is in PATH:

```bash
cd /home/dani/repos/rusty-backup
cargo build --release --bin rb-cli
export PATH="$PATH:$(pwd)/target/release"
```

All commands below use `rb-cli` directly; the Windows-side cargo build
also works if `cargo run --release --bin rb-cli --` is substituted.

### 1.4 Reference-tool inventory

Per the WSL toolchain verified 2026-06-03 in OPEN-WORK §8, these
tools cover the engine-level cross-checks before each MiSTer-side run:

| Tool | Apt package | Filesystems |
|---|---|---|
| `hmsa` | `hatari` | AtariST MSA |
| `mtools` (mcopy/mdir) | `mtools` | FAT12/16/32 (AtariST + MacPlus FAT + MSX + DOS-era PC) |
| `cpmtools` (cpmls/cpmcp) | `cpmtools` | CP/M (Amstrad/PCW/Einstein/SVI328/Altair/MultiComp/ZX+3) |
| `c1541` | `vice` | CBM DOS (C64/C128/C16/VIC20) |
| `hmount`/`hcopy` | `hfsutils` | classic HFS (MacPlus) |
| `hpmount`/`hpcopy` | `hfsplus` | HFS+ (MacPlus on newer systems) |
| `fls`/`icat` | `sleuthkit` | general forensics, ext-tree dump |

No apt tool exists for MFS / DOS 3.3 / Human68k / ADFS / QDOS /
OS-9 RBF / TRS-80 / Sedoric / N88-BASIC / SharpMZ / EOS / TI-99 /
ANDOS / MicroDOS / Specialist-MX. Those engines verify only against
our own decoder + the MiSTer-core mount check.

### 1.5 Shared content seed

Every test plants the same fixed content shape so the verification
steps can be a one-line checksum:

```
hello.txt   "Hello from rb-cli on <core>!"                 (size varies per core)
sub/        directory
sub/nested  "nested via <core>"                            (variable)
```

For 8.3-name filesystems (FAT12, CP/M, DOS 3.3, GEMDOS):
```
HELLO.TXT    "Hello from rb-cli on <core>!"
SUB          (directory or 8.3-named single file when subdirs unsupported)
SUB/NESTED   "nested via <core>"
```

Where the FS is flat (MFS, CBM, CP/M-no-subdirs, DOS 3.3) the
`sub/nested` slot becomes a second top-level file `NESTED` with the
same content.

---

## 2. Shared verification recipe

Each per-core entry below references one of three workflows:

### 2.1 Workflow A — Read-only mount + checksum

For cores that only run their disks read-only (ROM-only / readonly core):

1. Build a host-side seed image with rb-cli, hash it.
2. Copy to MiSTer SD card.
3. Boot the core, mount the image via the in-core file picker.
4. Confirm the seed files appear in the core's directory view.
5. (If the core has a "type a file" path — e.g. DOS `TYPE`, BASIC
   `LOAD`/`LIST`, etc.) read back the seeded content and visually
   compare with the source bytes.
6. Result: PASS if mounts + lists + read; FAIL otherwise.

### 2.2 Workflow B — In-core write + host-side validation

For cores whose OS can write to disk:

1. Build seed image via rb-cli; record SHA256.
2. Copy to MiSTer SD card.
3. Boot core, mount image.
4. From the core's OS, create a file with known content (e.g.
   `SAVE` from BASIC, `COPY` from CP/M, GEMDOS file-save dialog).
5. Power down cleanly. Wait for the LED to stop blinking before
   pulling the SD card.
6. On the host: `rb-cli ls` the image — confirm the new file is
   visible. `rb-cli get` it and byte-compare against the known
   content.
7. Result: PASS if rb-cli sees the new file with correct content.

### 2.3 Workflow C — Host writes + in-core mount + read

The opposite direction — the most common rb-cli use case:

1. Build a seed image **with rb-cli's write path** (e.g.
   `rb-cli put img.dsk hello.txt /HELLO.TXT`).
2. Copy to MiSTer; mount in the core.
3. From the core's OS, list and read the file. Validate the bytes
   match.
4. Result: PASS if rb-cli-written files are visible to the core OS.

**Every core test plan executes A + C minimum.** Workflow B is added
for cores with a robust OS-level write path. Cores whose OS is
read-only or whose write path is too fragile to test reliably skip B.

---

## 3. Per-core test matrix

Order follows the implementation waves: Wave 1 (shipped) → Wave 2
(now) → Wave 3 (later). Within each wave, ordering is by leverage.

### 3.1 AtariST (Wave 1 — shipped)

| Field | Value |
|---|---|
| SD path | `/media/fat/games/AtariST/` |
| Floppy format | `.st` (raw FAT12, 720K or 360K) and `.msa` (compressed) |
| HDD format | `.vhd` (AHDI partition table, FAT12/16 GEM/BGM partitions) |
| ROMs | `/games/AtariST/system/tos.img` (TOS 1.04 / 2.06; user-supplied) |
| Workflows | A + B + C |
| Reference tool | `hmsa` (encode/decode); mtools (per-partition FAT) |

**Test recipe:**

```bash
# 1) Floppy: hmsa-encoded MSA round-trip
cp tests/fixtures/test_atarist_floppy.msa.zst /tmp/
zstd -d /tmp/test_atarist_floppy.msa.zst -o /tmp/atst.msa
scp /tmp/atst.msa root@mister.local:/media/fat/games/AtariST/

# On MiSTer: boot AtariST core, mount /games/AtariST/atst.msa as A:.
# Verify GEMDOS lists HELLO.TXT and PROG/NESTED.TXT.

# 2) HDD: AHDI fixture write-verified via Workflow C
zstd -d tests/fixtures/test_atarist_ahdi.img.zst -o /tmp/atst.vhd
rb-cli put /tmp/atst.vhd@1 host_file.txt /HOSTNEW.TXT  # add to GEM partition
scp /tmp/atst.vhd root@mister.local:/media/fat/games/AtariST/

# On MiSTer: boot AtariST core, mount /games/AtariST/atst.vhd as C:.
# Verify HOSTNEW.TXT appears in GEMDOS directory.

# 3) Workflow B: from GEMDOS, COPY a file to A:, power down.
# Pull SD card, run:
rb-cli ls /tmp/atst.msa /             # expect the new file
rb-cli get /tmp/atst.msa /NEW_FILE.TXT /tmp/out.txt
# Byte-compare /tmp/out.txt against the GEMDOS source content.
```

**Pass criteria:**
- A: GEMDOS lists `HELLO.TXT` + `PROG/NESTED.TXT` after mount.
- C: GEMDOS lists `HOSTNEW.TXT` (the rb-cli-added file).
- B: rb-cli sees the GEMDOS-saved file with correct bytes.

### 3.2 MacPlus (Wave 1 — shipped)

| Field | Value |
|---|---|
| SD path | `/media/fat/games/MacPlus/` |
| Floppy format | `.dsk` (DiskCopy 4.2 wrapping HFS or MFS) |
| HDD format | `.vhd` (HFS volume) |
| ROMs | `/games/MacPlus/boot.rom` (Mac Plus 128K ROM — user-supplied) |
| Workflows | A + C (B is brittle on classic Mac OS without System Disk in same drive) |
| Reference tool | `hmount`/`hcopy` for HFS; nothing for MFS (use BasiliskII per §7) |

**Test recipe:**

```bash
# MFS floppy: synthetic-fixture round-trip
cargo test --lib mfs::tests::build_test_mfs   # produces a sample volume
# Or use a real System 1.0/2.0 floppy from macintoshrepository.org.
scp my_mfs.dsk root@mister.local:/media/fat/games/MacPlus/

# On MiSTer: boot MacPlus, mount the floppy. Confirm Finder shows the
# seeded files; double-click to view contents (if launchable) or
# inspect via ResEdit if available.

# HFS HDD: build via rb-cli + verify in Finder
rb-cli new bench.vhd --size 4M --name BenchHFS --fs hfs
rb-cli put bench.vhd host_hello.txt /Hello.txt
scp bench.vhd root@mister.local:/media/fat/games/MacPlus/
# In Finder: mount, confirm Hello.txt is visible.
```

**Pass criteria:**
- A: MFS floppy: Finder shows seed files; one open succeeds.
- A: HFS HDD: Finder mounts the drive, lists Hello.txt.
- C: rb-cli put files are visible to Finder.
- B (deferred per §7): boot a real System floppy in BasiliskII for
  the user-side reference cross-check that no Linux tool gives us.

### 3.3 Apple-II (Wave 1 — shipped)

| Field | Value |
|---|---|
| SD path | `/media/fat/games/Apple-II/` |
| Floppy format | `.dsk` / `.do` (DOS-order) / `.po` (ProDOS-order) / `.nib` (raw nibble) |
| HDD format | `.hdv` (ProDOS-only) |
| ROMs | `/games/Apple-II/apple_ii.rom` |
| Workflows | A + B + C |
| Reference tool | `a2kit` (cargo install a2kit) for cross-checks per §7 |

**Test recipe:**

```bash
# DOS 3.3 floppy: build, mount, verify
rb-cli new dos33.dsk --size 140K --fs apple_dos --vol 254
rb-cli put dos33.dsk host_hello.txt /HELLO  # type T (text)
scp dos33.dsk root@mister.local:/media/fat/games/Apple-II/

# On MiSTer: boot Apple-II core, type `CATALOG` to list disk.
# Then `READ HELLO` or `PRINT CHR$(4)"OPEN HELLO":INPUT$:CLOSE` etc.

# Workflow B: from Applesoft, `SAVE NEWPROG` after a few BASIC lines.
# Power down, pull SD, run:
rb-cli ls dos33.dsk /                # expect NEWPROG
rb-cli get dos33.dsk /NEWPROG /tmp/out
hexdump -C /tmp/out | head -3        # compare against expected BASIC tokens
```

**Pass criteria:**
- A: `CATALOG` lists `HELLO`.
- B: rb-cli sees Applesoft's `SAVE`-d program with correct token bytes.
- C: rb-cli-written file appears in `CATALOG` and reads via DOS `READ`.

### 3.4 X68000 (Wave 2)

| Field | Value |
|---|---|
| SD path | `/media/fat/games/X68000/` |
| Floppy format | `.d88` (multi-track), `.dim` (raw), `.xdf` (sector dump) |
| HDD format | `.hdf` (SASI HDD, Human68k partition scheme) |
| ROMs | `/games/X68000/X68000_IPLROM.dat` + `/games/X68000/X68000_FONT.dat` |
| Workflows | A + B + C |
| Reference tool | None on Linux apt; use `xdf2d88` / `xm6g` (Windows) for cross-check |

**Test recipe:**

```bash
# Floppy: build a 1.2 MB FDD with Shift-JIS filename
rb-cli new sample.d88 --size 1.2M --fs human68k --label "SAMPLE"
rb-cli put sample.d88 host.txt /HELLO.TXT       # ASCII path
rb-cli put sample.d88 host.txt /日本語.TXT      # Shift-JIS path (engine must transcode)
scp sample.d88 root@mister.local:/media/fat/games/X68000/

# On MiSTer: boot X68000 core, type `dir A:` in Human68k.
# Verify HELLO.TXT and the Shift-JIS name appear.

# HDD: build a SASI image with two partitions
rb-cli new x68k.hdf --size 32M --fs human68k --partitions 2
# (see Human68k partition scheme docs for the on-disk layout)
```

**Pass criteria:**
- A: Human68k `dir` lists `HELLO.TXT` and the Shift-JIS-encoded name.
- C: rb-cli-written files are visible to Human68k's `TYPE` command.

### 3.5 Archie / Acorn Archimedes (Wave 2)

| Field | Value |
|---|---|
| SD path | `/media/fat/games/Archie/` |
| Floppy format | `.adf` (800 KB E-format ADFS) |
| HDD format | `.hdf` (HDFS partition + ADFS; sometimes wrapped in a 512 B Arculator header) |
| ROMs | `/games/Archie/riscos.rom` (RISC OS 3.11 or 3.71) |
| Workflows | A + C |
| Reference tool | OpenAcornExplorer (Windows), `arcback` (Linux build-from-source) |

**Test recipe:**

```bash
rb-cli new arc.adf --size 800K --fs adfs --name BenchADFS
rb-cli put arc.adf host.txt /HELLO
scp arc.adf root@mister.local:/media/fat/games/Archie/

# On MiSTer: boot Archie, mount arc.adf as :0.
# At RISC OS, `*CAT :0` should list HELLO.
```

**Pass criteria:**
- A: `*CAT :0` lists HELLO.
- C: rb-cli-written file is openable via `*Type HELLO`.

### 3.6 QL (Wave 2)

| Field | Value |
|---|---|
| SD path | `/media/fat/games/QL/` |
| Floppy format | `.mdv` (microdrive cartridge image, ~100 KB) |
| HDD format | `.win` / `QXL.WIN` (container; large) |
| ROMs | `/games/QL/JSx.ROM` |
| Workflows | A + C |
| Reference tool | `qltools` (build from source per OPEN-WORK §8) |

**Test recipe:**

```bash
rb-cli new bench.mdv --size 100K --fs qdos --name BENCH
rb-cli put bench.mdv host.txt mdv1_HELLO
scp bench.mdv root@mister.local:/media/fat/games/QL/

# On MiSTer: boot QL, type `DIR mdv1_` — verify HELLO appears.
```

**Pass criteria:**
- A: SuperBasic `DIR mdv1_` lists HELLO.
- C: `LOAD mdv1_HELLO` or `EXEC_W mdv1_HELLO` succeeds with the
  expected content (for executable files) or `LIST` shows the
  expected lines for BASIC.

### 3.7 Altair8800 + CP/M cores (Wave 2 + spillover into Wave 3)

| Field | Value |
|---|---|
| SD path | `/media/fat/games/Altair8800/` (and per-CP/M-core dirs) |
| Floppy format | `.dsk` (varies by core: EDSK for Amstrad / PCW / Einstein; raw .dsk for Altair / SVI328 / MultiComp / ZX+3) |
| HDD format | Altair only: raw CF image (`.dsk` extension common too) |
| ROMs | per-core BIOS (e.g. Altair MITS-monitor `/games/Altair8800/8K BASIC.bin`) |
| Workflows | A + B + C — strongest write path of any Wave-2 core |
| Reference tool | `cpmtools` (cpmls / cpmcp / mkfs.cpm) — apt-installed in WSL |

**Test recipe (single-DPB example — Amstrad CPC 6128 system disk):**

```bash
# Build CP/M disk with cpmtools DPB shape
cpmtools_diskdefs=~/cpmtools_diskdefs    # path to cpmtools' diskdef file
mkfs.cpm -f amstrad_pcw -b boot.bin cpm.dsk
cpmcp -f amstrad_pcw cpm.dsk host_hello.txt 0:HELLO.TXT

# Now build the same disk via rb-cli and confirm cpmtools sees it:
rb-cli new rb_cpm.dsk --size 720K --fs cpm --dpb amstrad_pcw
rb-cli put rb_cpm.dsk host_hello.txt /HELLO.TXT
cpmls -f amstrad_pcw rb_cpm.dsk            # cross-check via cpmtools

scp rb_cpm.dsk root@mister.local:/media/fat/games/AmstradPCW/

# On MiSTer: boot AmstradPCW core; type `DIR A:` at CP/M prompt.
# Verify HELLO.TXT appears. `TYPE HELLO.TXT` to read back.

# Workflow B: at CP/M `> SAVE 1 NEW.COM` after typing some bytes.
# Power down, pull SD, on host:
cpmls -f amstrad_pcw rb_cpm.dsk            # expect NEW.COM
rb-cli ls rb_cpm.dsk /                     # same expectation via rb-cli
```

**Pass criteria:**
- A: CP/M `DIR` lists `HELLO.TXT`.
- B: cpmtools + rb-cli both see CP/M-side `SAVE` output.
- C: CP/M `TYPE` of rb-cli-put file matches host bytes.

**Per-core DPB:**

| Core | DPB preset name | Geometry notes |
|---|---|---|
| Amstrad CPC 6128 (system) | `amstrad_sys` | 9 spt × 40 trk × 2 sides × 512 B, off=2 |
| Amstrad CPC 6128 (data)   | `amstrad_data` | 9 spt × 40 trk × 2 sides × 512 B, off=0 |
| Amstrad PCW 8256/8512     | `amstrad_pcw` | 9 spt × 40 trk × 1 side × 512 B, off=1 (Format A — the TOSEC default; cross-checked against PAW + cpmtools `pcw` 2026-06-04). Format B (706 KB DS 80-trk) would land as `amstrad_pcw_b`. |
| Tatung Einstein           | `einstein` | 10 spt × 80 trk × 1 side × 512 B |
| SVI-328                   | `svi328_cpm` | 17 spt × 40 trk × 2 sides × 256 B |
| Altair8800 CF             | `altair_cf` | raw IDE block device, dpb varies by config |
| MultiComp                 | `multicomp` | EDSK or raw |
| ZX-Spectrum +3DOS         | `zxplus3` | 9 spt × 40 trk × 1 side × 512 B (= Amstrad data shape) |

### 3.8 BK0011M (Wave 2)

| Field | Value |
|---|---|
| SD path | `/media/fat/games/BK0011M/` |
| Floppy format | `.dsk` |
| HDD format | `.vhd` (ANDOS / CSIDOS filesystem) |
| ROMs | `/games/BK0011M/disk_bk11.rom` (built into the core mostly) |
| Workflows | A + C |
| Reference tool | None on Linux apt; verify against `bkd` Russian-hobby emulator |

(Niche; deferred until reach.)

---

## 4. Wave-3 floppy-only cores

Each gets a slot in the matrix once the engine ships. The procedure is
uniform: rb-cli build → SCP → mount → in-core verify. Per-core notes:

| Core | SD path | Format | Workflow |
|---|---|---|---|
| C64 / C128 / C16 / VIC20 | `/games/C64/` etc. | `.d64`/`.d71`/`.d81` (CBM DOS) | A + C; `c1541` for cross-check |
| PET2001 | `/games/PET/` | `.d80`/`.d82` | A only (cbm crate doesn't write D80/D82) |
| BBCMicro / AcornElectron | `/games/BBCMicro/` | `.ssd`/`.dsd` (Acorn DFS) | A + C |
| AcornAtom | `/games/AcornAtom/` | `.atm` | A only |
| CoCo2 / CoCo3 | `/games/CoCo2/` | `.dsk` (JV1; RS-DOS or OS-9 RBF) | A + C |
| TRS-80 | `/games/TRS-80/` | `.dsk` (JV1; TRSDOS / LDOS / NEWDOS) | A only initially |
| Atari800 | `/games/Atari800/` | `.atr` (Atari DOS 2.x) | A + C |
| Oric | `/games/Oric/` | `.dsk` (EDSK; Sedoric / Oric DOS) | A + C |
| PC88 | `/games/PC88/` | `.d88` (N88-BASIC Disk BASIC) | A only initially |
| SAM-Coupe | `/games/SAM-Coupe/` | `.mgt`/`.dsk` (SAM DOS / MasterDOS) | A + C |
| SharpMZ | `/games/SharpMZ/` | `.m12`/`.2d` (Sharp MZ FD) | A only |
| ColecoAdam | `/games/ColecoAdam/` | `.dsk` (EOS, RO in core) | A only |
| TI-99_4A | `/games/TI-99_4A/` | `.dsk` (TI floppy FS) | A + C |
| Vector-06C | `/games/Vector-06C/` | `.fdd`/`.edd` (MicroDOS) | A only |
| Specialist | `/games/Specialist/` | `.odi` (Specialist-MX FS) | A only |

---

## 5. Failure-mode triage

When a test fails, classify the failure to direct the fix:

| Symptom | Likely root cause | Where to fix |
|---|---|---|
| File-picker doesn't list our image | Wrong extension / container wrap | `src/rbformats/containers/<fmt>.rs` extension dispatch |
| File-picker lists but mount fails | On-disk layout mismatch with the core's expectation | engine `src/fs/<fs>.rs` (probably allocation / header bytes) |
| Mount OK, files invisible | Directory traversal mismatch | engine `list_directory` / catalog walker |
| Files visible, can't read | Data-fork allocation chain wrong | engine `read_file` / chain walker |
| Read OK, content garbled | Encoding or transcoding (text mode, Shift-JIS, ProDOS-vs-DOS sector order) | engine read path / container decode |
| Reads OK, in-core save fails | EditableFilesystem write primitive | engine `create_file` |
| Save OK, host can't re-read | sync_metadata not flushed / on-disk write mismatch | engine `sync_metadata` |

---

## 6. Reporting back

After each test run, append one line to `docs/mister-deployment-test-log.md`
(create on first use) with:

```
YYYY-MM-DD <core> <workflow> <PASS/FAIL> [note]
```

Failures get a short note (one sentence). Passes are silent. The log
is the audit trail when the user asks "did I ever verify X on real
hardware?".

---

## 7. Out of scope

The plan deliberately does **not** cover:

- **Core development.** Each MiSTer core is its own project; we test
  with the released RBF, we don't fork it.
- **ROM acquisition.** Where a core needs a system ROM, the user
  supplies it (most are copyrighted). The plan assumes the ROM is
  already in place.
- **Network mounts as the primary path.** CIFS is documented but
  SCP is the recommended channel; CIFS shows up because some
  users insist on it.
- **Image format conversion outside our supported set.** WOZ / IPF /
  IMD / TD0 imports are tracked under §3.2 of the MiSTer plan and
  remain port-from-source per OPEN-WORK §1.
