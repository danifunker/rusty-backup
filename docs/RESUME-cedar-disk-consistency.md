# RESUME — Cedar disk "Root page is inconsistent" (make rusty-backup author a Cedar-mountable volume)

Paste-and-go checkpoint. Self-contained: goal, repos, the automated test rig,
the full chain of findings (what's ruled out), the current narrowed conclusion +
the open contradiction, and the precise next step.

---

## >>> RESOLVED (2026-06-21): "Root page is inconsistent" was an EMULATOR bug <<<

The open contradiction below is **answered**: it was an **emulator disk-addressing
gap**, NOT a rusty-backup `pilot.rs` bug. rusty-backup's volume is correct.

**What the runtime trace showed.** Cedar's File package reads the LV root via
`subVolumes.first.address` = `pvPage` = **84**, then converts that flat page to a
real `DiskFace.DiskAddress`. The Pilot-on-Trident geometry is **5 heads x 28
addressed sectors/track**, and Pilot's virtual->real mapping **ignores the head**,
so VDA = `cylinder*28 + sector`. Page 84 = `cyl 3, sector 0` → the IOCB carries
disk-address words `[cyl=3, head/sector=0]` (raw `[3,0]`). The emulator's PDI
shortcut (`machine_germ_complete_disk_iocb`) decoded the address with the **germ's
flat rule** `disk_page = low + high` = **3**, so it served physical page 3 (zeros)
instead of page 84 (the real LV root). Cedar saw `seal=0 ≠ LRSeal` →
`rootStatus=inconsistent` → restricted Iago. (PV root reads at page 0 = `cyl 0`
worked by luck under both rules.)

**The fix** (in the emulator, `~/repos/xerox-dorado-dani/dorado/src/machine.c`,
uncommitted on branch `cedar-boot-continue`): in `machine_germ_complete_disk_iocb`,
for **Cedar's interrupt-driven IOCBs** (`csb_interrupt_mask != 0`) decode the SA
IOCB disk address as CHS → flat VDA: `disk_page = cylinder*DORADO_PILOT_SECTORS_PER_TRACK
(=28) + (head/sector_word & 0xFF)`. The **germ's polled IOCBs** (`mask == 0`) keep
the old flat `low+high` + GERMDATA-streaming path untouched (the germ addresses the
PDI by flat VDAs from the boot file's run table; only Cedar's File package uses real
CHS). Added `#define DORADO_PILOT_SECTORS_PER_TRACK 28u`.

**Verified:** with the fix Cedar now issues `raw=[3,0]` → `page=0o124` (84) and reads
the **real LV root** (seal `131313`/version `5`), continuing into the volume (reads
page 85 = VAM header). The "Root page is inconsistent" / "No System Volume" message
is GONE. Full emulator `make test` suite still passes (PDI + disk + CPU + baseboard).

### FIX #2 (also DONE this session) — VAM `File.FP.da` was 0 in `pilot.rs`

With FIX #1 the root mounts, but Cedar then took the `rootStatus=ok` path and
**stalled blank** (no display list, no further disk I/O after root+VAM ~cyc 584M, no
fault). Traced to a SECOND, independent **rusty-backup bug**: `install_vam` left
`rootFile[VAM].fp.da = 0`. `ReadVAM` (`LogicalVolumeImpl.mesa`) does
`File.Open[volume, rootFile[VAM].fp]`; `FileImpl.DoOpen` reads the file's leader at
`logicalRun = [first: file.fp.da, …]` and `File.DA == VolumeFormat.LogicalPage`. So
`fp.da` MUST be the VAM leader's **logical page** (= `header_lp` = 1). With `fp.da=0`
Cedar read logical page 0 (the LV root, physical 84) as the "VAM leader" → garbage
run table.

**Fix** (`src/fs/alto/pilot.rs`, `install_vam`): `wrlong(d, ROOTFILE_VAM_WORD+2,
header_lp)` (set `fp.da`, words +2..+3). Verified on the rig (patch the existing
fixture's VDA-84 `fp.da` via `/tmp/patch_vamda.py`, since `pilot_probe add` does NOT
rewrite `rootFile[VAM]` and the tracked fixture/`.gz` were authored with the old
writer): Cedar's second post-root read moved from `page=0o124` (84, the root) to
**`page=0o125` (85, the real VAM leader)** `raw=[3,1]`, count=2 reading the leader +
its 1 data page (86) — i.e. Cedar now reads the **complete VAM**. `cargo test --lib
pilot` (incl. `blank_volume_round_trips`) and emulator `make test` both green.

### NEW BLOCKER #3 (the next real step — past the VAM, in Login/FS/online)

Even with the complete VAM read, Cedar **still stalls** the same way: blank, no
display list, no disk I/O after the VAM (~cyc 584M), no fault. Per
`cyan/cedar6.1/iago/IagoMainImpl.mesa!1` (~line 186-223):
```
rootStatus ← CheckForDisk[]                            -- ok (reads root + now the VAM)
ignoreDisk ← bootedWithN OR (rootStatus#ok)            -- FALSE
IagoCommands.Login[[ignoreDiskEntirely: ignoreDisk]]   -- a REAL disk-using login
... SELECT TRUE FROM ignoreDisk => "...restricted command set"; ENDCASE => NULL
```
A/B (`build/dorado-nofix`): no-fix takes `ignoreDisk=TRUE` → `Login[…:TRUE]` (skips
disk) → restricted Iago **renders (28466 px)**. With both fixes, `ignoreDisk=FALSE`
→ `Login[…:FALSE]` enters a disk-using path and Cedar goes quiet at ~584M **before
issuing any FS reads and before the full-Iago command-loop render**.

**Pinned deeper:** `IagoCommands.Login` (`IagoCommands2Impl.mesa!1`) is just a wrapper
that calls `UserCredentials.Login[startInteraction: TurnOnProc, …, options]` where
`TurnOnProc` prints `"\nPlease login ...\n"`. The fixed run's screen is BLANK — i.e.
"Please login ..." is **never rendered** — so Cedar stalls **inside
`UserCredentials.Login` (with `ignoreDiskEntirely=FALSE`) BEFORE `TurnOnProc`/before
the print**, before any FS disk read.

**>>> VERDICT: blocker #3 is EMULATOR-side (incomplete Pilot timer/RTC emulation). <<<**
`DORADO_FINAL_DEBUG=1 DORADO_MACHINE_PCHIST=1` on the stalled run shows the CPU spinning
in a tight loop (task-0 hot PCs `0o234`/`0o264`/`0o600`/`0o700`, 8-10M hits each) — NOT
at `0o3445`. `RM[0]`(NWW)=`000000`: the Pilot timer interrupt channel
(`PILOT_TIMER_CHAN_MASK=0100000`, NWW sign bit) is **never raised**; `EVENFIELD`(display
retrace) fires 15282× but the Pilot interval timer fires **0×**. Root cause: the emulator
does NOT faithfully drive the RTC/interval-timer interrupt — `machine_pilot_timer_channel`
(machine.c:303) is a SHIM that injects the timer channel ONLY after the CPU parks at the
early-boot busy-wait `pre_pc==0o3445` (its own comment: "until that task is faithful
enough for Cedar/Pilot, inject … only after Pilot has parked in BusyWait"). The
mounted-volume login path parks/spins at a DIFFERENT PC, so the shim never arms, the
timer-wake of the login process never happens, and Cedar spins forever. (Pilot wakes
timed `WAIT`s via this channel; `RTClock=006110` vs `WakeupTime=177770` at the stall — the
clock never advances to the wakeup because no timer interrupt drives the scheduler.) The
no-fix path renders precisely because it DOES park at `0o3445`, so the shim services it.

**Fix lives in the emulator** (`machine.c`): make `machine_pilot_timer_channel` raise the
timer channel on a real cadence (PILOT_TIMER_INTERVAL_CYCLES) regardless of PC once
`germ_data_done` / the world is up — not gated on `pre_pc==0o3445` — OR drive it from a
faithful RTC/junk-task. Risk: arming it too early/at the wrong cadence can derail the germ
boot (why it was gated); validate the germ + Cedar-login + Alto gates after. rusty-backup's
volume is sound through root+VAM; any further rusty-backup bug is hidden BEHIND this hang
until the timer is faithful (re-test the disk-read trace after fixing the timer).

**Decisive next step (next session):**
1. Confirm `vamStatus` ended `ok` (separate from `rootStatus`; `rebuild_vam` bitmap
   must be valid — `set bit = in use`, LSB-first). The complete-VAM read happened, so
   this is likely fine, but verify.
2. Dig into `UserCredentials.Login` (find its impl in `~/PARC-Stuff/cyan/cedar6.1`,
   pkg `UserCredentials`/`GVBasics`/`Credentials`) — what does it touch FIRST (before
   the "Please login" print) when `ignoreDiskEntirely=FALSE`? Candidates: reading a
   credentials/registry file via `FS`, `BasicTime`/clock (`TimeNotKnown` is caught in
   IagoMain), or a process/condition `WAIT`.
3. Investigate the dead Pilot timer: is the emulator supposed to tick a Pilot
   interval/real-time clock that the disk-using login waits on? (0 ticks to 1B.)
4. Then `FS`/credentials (page 2) + **client name directory** (`rootFile[client]`,
   FileID 6) — smoke has it, fixture doesn't, both stall identically, so it's BEFORE
   the client dir.

**State of the test volumes:** the `pilot.rs` fix makes all NEW volumes correct, but
the tracked `CedarDisk/CedarDorado-boot.pdi.gz` fixture + `/tmp/cedar-smoke.pdi` were
authored with the old writer (`fp.da=0`). Use `/tmp/cedar-smoke-vamfix.pdi` (patched)
for the rig, or re-author the fixture with the fixed writer. **Rebuild gotcha:** after
`git stash`-based no-fix builds, `make` may skip recompiling `machine.c` (mtime) and
leave `build/dorado` STALE — `touch dorado/src/machine.c && make`, and confirm with
`cmp build/dorado build/dorado-nofix`.

Everything below is the ORIGINAL pre-resolution checkpoint (kept for grounding).

---

## Goal (the bigger arc)

Build a **full Cedar/Dorado disk with software** via **Route 2**: rusty-backup
authors a Cedar **PDI** containing the BasicCedar boot world **+** Cedar `.bcd`
packages installed as named files; the running Cedar's **loader** runs them off
the disk (`Run X.bcd`, or auto-run via `User.Profile`). Cedar uses **dynamic
loading** — "full Cedar" = the small base world (BasicCedar = 26 modules) **plus**
`.bcd` files on the volume, **not** a monolithic build. So Route 2 is **disk
authoring + Cedar's own loader**, NOT reimplementing the Mesa binder.

We ran the **one-package smoke test** (BasicCedar + `Chat.bcd` + `User.Profile`)
to prove Cedar can find+run a `.bcd` from a rusty-backup-authored disk. It found
the blocker below.

## THE BLOCKER (what the smoke test found)

The disk boots to **interactive Iago** (keyboard works, added files don't break
boot), but after login Cedar prints:

```
Root page is inconsistent (wrong version?).  Invoking Iago, with restricted command set
```

→ Cedar's File package **refuses to mount the volume** → "No System Volume" →
**restricted Iago** → `List Names` / `Run … BCD` are **not available** ("command
not found"). So the disk-authoring route is blocked until rusty-backup writes a
volume Cedar 6.1 accepts.

**This is PRE-EXISTING** in rusty-backup's base Cedar writer (confirmed: the
*unmodified* `CedarDorado-boot.pdi` shows the exact same message). It is the
long-flagged "Cedar volume writer validated round-trip only, never against real
Cedar" gap — now confirmed by ground truth. Adding `Chat`/`User.Profile` is
innocent.

---

## Repos, branches, key files

- **Emulator (C):** `~/repos/xerox-dorado-dani`, branch **`cedar-boot-continue`**
  (off updated `main` = `e91adf5`; the *old* `new-disk-things` branch was 129
  commits behind and its WF/RF/TIsId fix is already upstream as `b7e803d`).
  Build dir `dorado/`; `make` → `build/dorado` (non-SDL, renders headless), `make sdl`
  → `build/dorado-sdl` (window). Needs SDL2 (`/opt/homebrew/lib`).
- **rusty-backup (Rust, the fix target):** `~/repos/rusty-backup`.
  - Cedar/Pilot writer: **`src/fs/alto/pilot.rs`** (86 KB). PDI format: `src/fs/alto/pdi.rs`.
  - CLI: `examples/pilot_probe.rs` → `cargo run --quiet --example pilot_probe -- <verb>`.
- **Cedar 6.1 source:** `~/PARC-Stuff/cyan/cedar6.1/` (Tioga = binary bytes; read
  with `LC_ALL=C tr -c '[:print:]\n' ' ' < FILE | grep …`). The **nucleus FS impl
  source** (what computes `rootStatus`) is in **`~/PARC-Stuff/indigo/nucleus/file/`**.
- **Disk inputs:** germ `chm/cedar/germ-alt/Dorado.germ-6.1.6`; microcode
  `chm/dorado/CedarDorado.eb!6`; boot fixture `CedarDisk/CedarDorado-boot.pdi.gz`
  (tracked; `gunzip -k` → `CedarDisk/CedarDorado-boot.pdi`, git-ignored).
- **Smoke-test bits:** `Chat.bcd` = `~/PARC-Stuff/cyan/cedarchest6.1/chat/Chat.bcd!2`;
  `User.Profile` = `~/PARC-Stuff/cyan/cedar6.1/top/User.Profile!1`.

---

## THE AUTOMATED TEST RIG (the tooling breakthrough — use this)

The **non-SDL** `dorado` renders the Cedar login (~28k display-list px at ~1.2B
cyc), AND supports `--type` (keystroke injection) + `--out` (PGM). So the whole
boot→login→screenshot loop is **headless, deterministic, ~2 min/iteration, no
window, no hand-driving**:

```sh
cd ~/repos/xerox-dorado-dani/dorado && make
EB='../chm/dorado/CedarDorado.eb!6'; GERM='../chm/cedar/germ-alt/Dorado.germ-6.1.6'
PDI=/tmp/cedar-smoke.pdi   # or ../CedarDisk/CedarDorado-boot.pdi
./build/dorado --boot-reason disk --no-alto-boot --eb "$EB" --germ "$GERM" --pilot-disk "$PDI" \
  --type $'Guest\n\n' --type-at 1100000000 --cycles 2200000000 --out /tmp/v.pgm
sips -s format png /tmp/v.pgm --out /tmp/v.png      # macOS PGM->PNG
# then Read /tmp/v.png to see the verdict (full Iago vs "Root page is inconsistent")
```

Notes on driving the login:
- The Cedar login prompt appears ~800M–1B cyc; type at `1.1B`, screenshot at `2.2B`.
- `--type` maps chars→keys; **`'.'` is UNMAPPED** (only `a-z 0-9 space \n/\r`). Type
  `Guest` (not `Guest.pa`) — the registry default **`pa`** appends, giving `Guest.pa`.
  `\n`/`\r` → RETURN; Grapevine is down so any password (empty `\n`) proceeds.
- Pass real CR via `$'...'` (zsh ANSI-C quoting), not `"…\n"`.
- `--speed N` on `dorado-sdl` is **cycles-per-redraw** (NOT a throttle); for the
  window use `make run-cedar`. `SDL_VIDEODRIVER=dummy` forces headless on the SDL build.

**Confirm it's the disk, not Ethernet:** the boot uses `--boot-reason disk` +
`--pilot-disk` with **no `--eftp`** and **no `--germ-netboot-bfn`**; `DORADO_DISK_IOCB_TRACE=1`
shows real PDI page reads, `DORADO_EFTP_TRACE=1` shows zero EFTP traffic.

### Re-author the smoke PDI (if `/tmp/cedar-smoke.pdi` is gone)
```sh
cd ~/repos/rusty-backup
gunzip -kf ~/repos/xerox-dorado-dani/CedarDisk/CedarDorado-boot.pdi.gz
cp ~/repos/xerox-dorado-dani/CedarDisk/CedarDorado-boot.pdi /tmp/cedar-smoke.pdi
cargo run --quiet --example pilot_probe -- add /tmp/cedar-smoke.pdi ~/PARC-Stuff/cyan/cedarchest6.1/chat/Chat.bcd!2          # -> FileID 4
cargo run --quiet --example pilot_probe -- add /tmp/cedar-smoke.pdi ~/PARC-Stuff/cyan/cedar6.1/top/User.Profile!1            # -> FileID 5
cargo run --quiet --example pilot_probe -- set-dir /tmp/cedar-smoke.pdi "Chat.bcd=4" "User.Profile=5"
cargo run --quiet --example pilot_probe -- probe /tmp/cedar-smoke.pdi      # shows Chat.bcd + User.Profile named
```
`add`/`set-dir` keep the germ + boot file byte-intact (verified via `boot-info` +
`extract-boot bootfile` cmp). So Phase-1 authoring works; the blocker is the
volume root, below.

---

## The diagnosis chain (Cedar source-grounded)

`rootStatus=inconsistent` traces through (paths under `~/PARC-Stuff`):

1. **`cyan/cedar6.1/iago/IagoMainImpl.mesa`**: `rootStatus ← CheckForDisk[]`;
   `ignoreDisk ← bootedWithN OR (rootStatus#ok)`; `inconsistent => "Root page is
   inconsistent (wrong version?)"` → registers the **restricted** command set
   (no `ListNames`/`RunDiagnosticBCD`). `CheckForDisk: rt ← File.LogicalInfo[v].rootStatus`.
2. **`indigo/nucleus/file/VolumeRootImpl.mesa!8`** (and `LogicalVolumeImpl.mesa!13`)
   `ReadRootPage`:
   ```
   volume.rootStatus ← RootTransfer[volume, read];
   IF ok AND (root.seal#LRSeal OR root.version#LRCurrentVersion) THEN inconsistent;
   IF ok AND root.type#cedar THEN nonCedarVolume;
   ... ReadVAM[volume] sets volume.vamStatus (separate from rootStatus) ...
   ```
3. **`RootTransfer`**: a Disk read of the LV root (`subVolumes.first.address`, the
   subvolume's first physical page) with command `[header:verify, label:verify,
   data:read]`, expected label = `LogRootLabel[volume.id]` =
   `[fileID: abs[AbsID[id]], filePage: 0, attributes: logicalRoot]`. Returns
   `FileInternal.TranslateStatus[disk status]`.
4. **`indigo/nucleus/heads/DiskFace.mesa!3`**: `Label = [fileID(0):FileID,
   filePage(5):INT, attributes(7):Attributes, dontCare(8)]`; `FileID` is an
   **OVERLAID** union (no tag): `rel => relID(4)+fill`, `abs => absID(5)`.
5. **`indigo/nucleus/file/VolumeFormat.mesa!10`** (== cyan/cedar6.1 versions):
   `LRSeal=131313B`, `LRCurrentVersion=5`; `LogicalRoot` has `rootFile (125B)`
   = word **85** (6 words/entry), `lastFileID (375B)` = word **253**,
   `coCedar (374B)` = 252, `checksum (377B)` = 255.

## RULED OUT — rusty-backup's LV root page (VDA 84) is structurally correct

Dumped the actual bytes (tool below); all match the Cedar-nucleus spec:
- `seal[0]=131313` ✓, `version[1]=5` ✓ (Cedar 6.1 uses `LRCurrentVersion=5` — Iago's
  "wrong version?" is just a guess).
- `rootFile[VAM]` (word **127** = 85+7×6) populated: `fp.id=1, page=1` ✓;
  `lastFileID[253]` set (3 in fixture, 6 in smoke) ✓. **Earlier "rootFile all zero"
  was a dump bug** (octal vs decimal offsets) — it is NOT zero.
- `checksum[255]` is **self-consistent** with `pilot_checksum` ✓.
- LV-root **label** (20 B at VDA 84): `fileID=[2,0,0,0,0]`, `filePage=0`,
  `attributes=5` — matches `DiskFace.Label`; `abs[AbsID[2]]` overlays as `[2,0,0,0,0]`.
- `type=3` (cedar) ✓ (so not `nonCedarVolume`); `coCedar[252]=0`.

### Dump tool (decimal offsets — IMPORTANT)
```python  # /tmp/pdiroot2.py  — run: python3 /tmp/pdiroot2.py <file.pdi>
import sys; b=bytearray(open(sys.argv[1],'rb').read())
def be16(o): return (b[o]<<8)|b[o+1]
record=be16(22)+be16(24)+be16(26); off=512+84*record+be16(22)+be16(24)  # VDA 84 data
def W(i): o=off+i*2; return (b[o]<<8)|b[o+1]
def L(i): return W(i)|(W(i+1)<<16)
print("seal",oct(W(0)),"ver",W(1),"type",W(28),"coCedar",W(252),"lastFileID",L(253),"cksum",oct(W(255)))
for s in range(10):                       # rootFile[] base word 85, 6 words/entry
    w=85+s*6
    if L(w) or L(w+2) or L(w+4): print(f"rootFile[{s}] @w{w}: id={L(w)} da={L(w+2)} page={L(w+4)}")
```
(VDA 84 = subvolume's first physical page = LV root; probe shows `[0] lvPage=0
pvPage=84`. Geometry on this disk: `label_bytes=20, data_bytes=512, record=532`.)

## KEY EMULATOR FINDING (a real gap, worth fixing regardless)

The emulator's **PDI disk path does NOT verify labels**. `machine.c:692-718`
(`dorado_pdi_*`): on a read it **copies the on-disk label into Cedar's buffer**
and unconditionally sets `SA_IOCB_HEADERSTATUS = LABELSTATUS = DATASTATUS = 0`
(OK). `disk.c` has no label-verify either. The PDI shortcut serves **all** reads
once `pilot_pdi_loaded && germ_data_done` (machine.c:624) — i.e. Cedar's
post-boot reads too (unless `--disk-real`). So `[label:verify]`/`[header:verify]`
reads **always "pass."**

## THE OPEN CONTRADICTION (resolve this first next session)

Emulator always reports disk-read **OK**, and the root data has **correct
seal/version** → `RootTransfer` should return `ok` → `rootStatus` should be
**`ok`**. **Yet Cedar reports `inconsistent`.** So the truth is in a layer not yet
pinned. Hypotheses, in order:

1. **Runtime root-read page/data mismatch.** The post-boot LV-root read may hit a
   different page, or get different data, than VDA-84-on-disk. → **TRACE IT.**
2. **Cedar 6.1 `FileImpl.bcd` differs from the indigo/nucleus *source*** I read
   (source is very close but not guaranteed identical to the 6.1 binary).
3. Something feeds `rootStatus` beyond `ReadRootPage` (`File.LogicalInfo` impl in
   `LogicalVolumeImpl.mesa`/`FileImpl.mesa` — re-read the actual `LogicalInfo`
   return, and whether a physical-volume/online check also gates it).

## THE DECISIVE NEXT STEP

**Runtime-trace the post-boot root read** with the rig:
```sh
DORADO_DISK_IOCB_TRACE=1 ./build/dorado --boot-reason disk --no-alto-boot --eb "$EB" --germ "$GERM" \
  --pilot-disk /tmp/cedar-smoke.pdi --type $'Guest\n\n' --type-at 1100000000 --cycles 2200000000 \
  --out /tmp/v.pgm 2>&1 | grep -iE "IOCB|page=0o124|label|status"
```
Watch the read of **page 84 (=0o124)** *after* boot: which page, what data words
(seal at word 0?), what label/status Cedar gets. That empirically resolves the
contradiction — and likely exposes whether it's an emulator addressing/verify gap
(fixable in the emulator) or a genuine rusty-backup root/VAM-file mismatch
(fixable in `pilot.rs`).

If it's rusty-backup: the fix is in **`pilot.rs`'s Cedar-nucleus volume writer**
(constants `LR_SEAL=0o131313`, `LR_VERSION=5`, `ROOTFILE_VAM_WORD=127`,
`ROOTFILE_CLIENT_WORD=133`; `install_vam` ~line 597 builds the VAM file + sets
`rootFile[VAM]`+`lastFileID`; `Label` struct ~line 238; `set_page_checksum`/
`pilot_checksum`). Then re-run the rig to validate (full Iago, then `ListNames`
should show `Chat.bcd`, then `RunDiagnosticBCD` → `Chat.bcd`).

## After consistency: scaling Route 2

Once a single `.bcd` loads from disk, scale to all 303 `cedarchest6.1` packages.
The one real new piece of code is **upgrading `pilot.rs`'s name-directory (client
dir) writer from single-root-leaf → multi-page B-tree** (it currently *errors past
~1022 words / one leaf* — see `pilot.rs:1332-1386`). Then a package installer
(loop the chest, `add` each `.bcd` + `.load`, build a `User.Profile`). Spec:
`CedarDisk/PARC_PILOT_FORMAT.md`.

## Side note (separate thread, already DONE this session, on the emulator)

The germ-6.1 dispatch-113 `ControlFault` was fixed upstream (`b7e803d`:
`TIsId/RIsId` + `WF←A/RF←A` ShC) — I independently re-derived the same fix on the
stale `new-disk-things` branch (now redundant). Current emulator `main` boots
Cedar to the login and the Mesa NetExec to a prompt. Not part of this thread.
